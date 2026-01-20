package com.lhcz.db2es.core;

import com.lhcz.db2es.config.AppConfig;
import com.lhcz.db2es.model.SyncData;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 核心流水线控制器
 */
public class Pipeline {
    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

    private final AppConfig config;
    // 使用虚拟线程 (Java 21)
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    // 管理器组件
    private final CheckpointManager checkpointManager = new CheckpointManager();
    private final DeadLetterQueueManager deadLetterQueueManager = new DeadLetterQueueManager(); // 补录管理器

    // 🟢 新增：保存任务引用以便 WebConsole 监控
    private final List<JdbcSource> sources = new ArrayList<>();
    private final List<EsSink> sinks = new ArrayList<>();

    public Pipeline(AppConfig config) {
        this.config = config;
    }

    public void start() {
        log.info(" 正在初始化数据库连接池 (HikariCP)...");
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.db().url());
        hikariConfig.setUsername(config.db().user());
        hikariConfig.setPassword(config.db().password());

        // 🟢 核心修复：应用稳健的连接池参数
        long maxLifetime = (config.db().maxLifetimeMs() != null) ? config.db().maxLifetimeMs() : 600000L; // 默认10分钟
        long idleTimeout = (config.db().idleTimeoutMs() != null) ? config.db().idleTimeoutMs() : 300000L; // 默认5分钟
        int minIdle = (config.db().minIdle() != null) ? config.db().minIdle() : 2;
        int maxPoolSize = (config.db().maxPoolSize() != null) ? config.db().maxPoolSize() : 10;

        log.info("连接池配置: MaxLifetime={}ms, IdleTimeout={}ms, PoolSize={}", maxLifetime, idleTimeout, maxPoolSize);

        hikariConfig.setMaxLifetime(maxLifetime);
        hikariConfig.setIdleTimeout(idleTimeout);
        hikariConfig.setMinimumIdle(minIdle);
        hikariConfig.setMaximumPoolSize(maxPoolSize);

        // 开启 TCP KeepAlive 防止防火墙静默切断连接
        hikariConfig.addDataSourceProperty("socketTimeout", "30000");
        hikariConfig.addDataSourceProperty("tcpKeepAlive", "true");

        HikariDataSource ds = new HikariDataSource(hikariConfig);

        for (AppConfig.TaskConfig task : config.tasks()) {
            // 有界队列实现背压
            BlockingQueue<SyncData> channel = new LinkedBlockingQueue<>(5000);

            JdbcSource source = new JdbcSource(ds, task, channel, checkpointManager);
            EsSink sink = new EsSink(channel, config.es(), task, checkpointManager, deadLetterQueueManager);

            // 🟢 收集引用
            sources.add(source);
            sinks.add(sink);

            log.info("启动任务线程: 表[{}] -> 索引[{}]", task.tableName(), task.esIndex());
            executor.submit(source);
            executor.submit(sink);
        }

        // 🟢 启动 Web 控制台 (如果配置了端口)
        if (config.web() != null && config.web().port() != null) {
            WebConsole webConsole = new WebConsole(config.web().port(), sources, sinks);
            webConsole.start();
        }
    }

    public void await() {
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
