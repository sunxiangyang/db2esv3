package com.lhcz.db2es.core;

import com.lhcz.db2es.config.AppConfig;
import com.lhcz.db2es.model.SyncData;
import com.lhcz.db2es.util.JsonUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.BlockingQueue;

/**
 * 数据库读取任务 (生产者)
 * 负责从数据库查询数据，转换格式，并放入缓冲队列。
 * 具备断点续传和自动重连机制。
 */
public class JdbcSource implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(JdbcSource.class);

    private final HikariDataSource ds;
    private final AppConfig.TaskConfig task;
    private final BlockingQueue<SyncData> queue;
    private volatile boolean running = true;
    private final CheckpointManager checkpointManager;

    // 新增：回溯检查的时间间隔 (60秒)
    private static final long REWIND_INTERVAL_MS = 60000L;
    private static final long REWIND_OFFSET = 10000L;

    public JdbcSource(HikariDataSource ds, AppConfig.TaskConfig task, BlockingQueue<SyncData> queue, CheckpointManager cm) {
        this.ds = ds;
        this.task = task;
        this.queue = queue;
        this.checkpointManager = cm;
    }

    @Override
    public void run() {
        // 1. 获取起始进度 (优先读取断点文件，没有则使用配置的 startId)
        long currentId = checkpointManager.getStartId(task.tableName(), task.startId());
        int pageSize = 5000; // 每次查询条数，建议 2000-5000
        long lastRewindTime = System.currentTimeMillis(); // 记录上次回溯时间

        log.info("任务 [{}] 启动，从 ID: {} 开始同步", task.tableName(), currentId);

        // 2. 主循环：只要 running 为 true，就一直运行
        // 将 try-catch 放进循环内部，确保发生异常（如断网）后能重试，而不是直接退出线程
        while (running) {
            try {
                // --- 🟢 新增逻辑：定期执行回溯校验 (解决并发写入丢数据问题) ---
                if (System.currentTimeMillis() - lastRewindTime > REWIND_INTERVAL_MS) {
                    performRewindCheck(currentId);
                    lastRewindTime = System.currentTimeMillis();
                }
                // -------------------------------------------------------

                // 构造 SQL：必须按 idColumn 排序以保证不漏数据
                // 示例: SELECT * FROM user WHERE id > ? ORDER BY id ASC LIMIT ?
                String sql = String.format("SELECT %s FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT ?",
                        task.columns(), task.tableName(), task.idColumn(), task.idColumn());

                int fetchCount = 0;
                long startTime = System.currentTimeMillis();

                // 3. 获取连接与执行查询
                // 使用 try-with-resources 自动关闭 Connection 和 PreparedStatement
                try (Connection conn = ds.getConnection();
                     PreparedStatement ps = conn.prepareStatement(sql)) {

                    ps.setLong(1, currentId);
                    ps.setInt(2, pageSize);

                    // 调试时可开启：打印具体执行的 SQL
                    String debugSql = sql.replaceFirst("\\?", String.valueOf(currentId))
                            .replaceFirst("\\?", String.valueOf(pageSize));

                    log.info("[SQL] {}", debugSql);


                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            // A. 获取分页游标值 (用于进度记录，必须是数字)
                            String cursorVal = rs.getString(task.idColumn());

                            // B. 获取业务去重主键 (用于 ES _id)
                            // 如果没配置 pkColumn，则默认使用 idColumn
                            String pkColName = (task.pkColumn() != null && !task.pkColumn().isBlank())
                                    ? task.pkColumn() : task.idColumn();
                            String esIdVal = rs.getString(pkColName);

                            // C. 转换为 JSON
                            String json = JsonUtil.resultSetToJson(rs);

                            // D. 放入队列 (如果队列满，这里会阻塞等待 Sink 消费，实现背压)
                            // 🟢 修改：构造 SyncData 时传入 isRepair=false
                            queue.put(new SyncData(Long.parseLong(cursorVal), null, esIdVal, json, false));

                            // 更新内存中的进度
                            currentId = Long.parseLong(cursorVal);
                            fetchCount++;
                        }
                    }
                }

                // 4. 根据读取结果决定下一步
                if (fetchCount == 0) {
                    // 没有新数据，休眠 2 秒避免空转
                    Thread.sleep(2000);
                } else {
                    long cost = System.currentTimeMillis() - startTime;
                    log.info("任务 [{}] 读取 {} 条数据，耗时 {}ms，当前进度 ID: {}",
                            task.tableName(), fetchCount, cost, currentId);
                }

            } catch (InterruptedException e) {
                log.info("任务 [{}] 被中断，正在停止...", task.tableName());
                Thread.currentThread().interrupt();
                break; // 退出循环
            } catch (Exception e) {
                // 5. 异常处理 (第三道防线)
                // 无论是 SQL 错误还是网络中断，都会捕获到这里
                log.error("任务 [{}] 发生异常 (可能是数据库断连): {}, 5秒后重试...",
                        task.tableName(), e.getMessage());

                // 发生错误时强制休眠，防止死循环刷日志导致 CPU 飙升
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    break;
                }
                // 循环继续，下一次会自动尝试 ds.getConnection() 重新建立连接
            }
        }

        log.info("👋 任务 [{}] 线程已结束", task.tableName());
    }

    /**
     * 执行回溯校验：读取 [currentId - 10000, currentId] 范围的数据
     */
    private void performRewindCheck(long currentMaxId) {
        long startId = Math.max(0, currentMaxId - REWIND_OFFSET);
        log.info("🔄 正在执行回溯校验: 表[{}] 范围 [{} - {}]", task.tableName(), startId, currentMaxId);

        // 查询范围数据的 SQL (不需要排序，只要把数据捞出来即可)
        String sql = String.format("SELECT %s FROM %s WHERE %s > ? AND %s <= ?",
                task.columns(), task.tableName(), task.idColumn(), task.idColumn());

        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setLong(1, startId);
            ps.setLong(2, currentMaxId);

            int count = 0;
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String cursorVal = rs.getString(task.idColumn());
                    String pkColName = (task.pkColumn() != null && !task.pkColumn().isBlank()) ? task.pkColumn() : task.idColumn();
                    String esIdVal = rs.getString(pkColName);
                    String json = JsonUtil.resultSetToJson(rs);

                    // 🟢 关键：标记 isRepair=true，告诉 Sink 不要更新 Checkpoint
                    queue.put(new SyncData(Long.parseLong(cursorVal), null, esIdVal, json, true));
                    count++;
                }
            }
            log.info("🔄 回溯校验完成: 表[{}] 推送了 {} 条历史数据进行二次验证", task.tableName(), count);
        } catch (Exception e) {
            log.error("⚠️ 回溯校验失败 (不影响主流程): {}", e.getMessage());
        }
    }

    public void stop() {
        this.running = false;
    }
}
