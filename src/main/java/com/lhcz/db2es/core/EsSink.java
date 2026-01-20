package com.lhcz.db2es.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lhcz.db2es.config.AppConfig;
import com.lhcz.db2es.model.SyncData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ES 写入消费者
 */
public class EsSink implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(EsSink.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final BlockingQueue<SyncData> queue;
    private final AppConfig.EsConfig esConfig;
    private final AppConfig.TaskConfig taskConfig;
    private final HttpClient httpClient;
    private volatile boolean running = true;
    private final CheckpointManager checkpointManager;
    private final DeadLetterQueueManager deadLetterQueueManager;

    private static final DateTimeFormatter FMT_MONTH = DateTimeFormatter.ofPattern("yyyy_MM");
    private static final DateTimeFormatter FMT_DAY = DateTimeFormatter.ofPattern("yyyy_MM_dd");

    // 🟢 新增：统计指标 (用于 Web 监控)
    private final AtomicLong totalCreated = new AtomicLong(0);
    private final AtomicLong totalUpdated = new AtomicLong(0);
    private final AtomicLong totalFailed = new AtomicLong(0);

    public EsSink(BlockingQueue<SyncData> queue, AppConfig.EsConfig esConfig, AppConfig.TaskConfig taskConfig, CheckpointManager cm, DeadLetterQueueManager dlq) {
        this.queue = queue;
        this.esConfig = esConfig;
        this.taskConfig = taskConfig;
        this.checkpointManager = cm;
        this.deadLetterQueueManager = dlq;
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    // 🟢 新增：Getter 方法供 WebConsole 使用
    public long getTotalCreated() { return totalCreated.get(); }
    public long getTotalUpdated() { return totalUpdated.get(); }
    public long getTotalFailed() { return totalFailed.get(); }
    public AppConfig.TaskConfig getTaskConfig() { return taskConfig; }

    @Override
    public void run() {
        List<SyncData> buffer = new ArrayList<>(esConfig.batchSize());
        long lastFlushTime = System.currentTimeMillis();

        try {
            while (running) {
                SyncData data = queue.poll(100, TimeUnit.MILLISECONDS);
                if (data != null) buffer.add(data);

                boolean sizeTrigger = buffer.size() >= esConfig.batchSize();
                boolean timeTrigger = !buffer.isEmpty() && (System.currentTimeMillis() - lastFlushTime > esConfig.flushIntervalMs());

                if (sizeTrigger || timeTrigger) {
                    flush(buffer);
                    buffer.clear();
                    lastFlushTime = System.currentTimeMillis();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String resolveIndexName(String template) {
        if (template == null || !template.contains("#(")) return template;
        LocalDate now = LocalDate.now();
        String result = template.replace("#(dtmon)", now.format(FMT_MONTH));
        result = result.replace("#(dtday)", now.format(FMT_DAY));
        return result;
    }

    private void flush(List<SyncData> batch) {
        if (batch.isEmpty()) return;

        String realIndex = resolveIndexName(taskConfig.esIndex());
        String realType = (taskConfig.esType() != null && !taskConfig.esType().isBlank()) ? taskConfig.esType() : "_doc";

        StringBuilder bulkBody = new StringBuilder();
        // 检查本批次是否包含正常数据 (用于决定是否更新 Checkpoint)
        SyncData lastNormalData = null;

        // 🟢 新增：记录本批次中最大的修复ID
        long maxRepairId = -1;
        int repairCount = 0;

        for (SyncData item : batch) {
            bulkBody.append(String.format("{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n",
                    realIndex, realType, item.esIdVal()));
            bulkBody.append(item.jsonBody()).append("\n");

            if (!item.isRepair()) {
                lastNormalData = item;
            } else {
                repairCount++;
                // 追踪最大的修复ID
                if (item.idCursorVal() > maxRepairId) {
                    maxRepairId = item.idCursorVal();
                }
            }
        }

        // 构建 Auth
        String authHeader = null;
        if (esConfig.user() != null && !esConfig.user().isBlank()) {
            String auth = esConfig.user() + ":" + esConfig.password();
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            authHeader = "Basic " + encodedAuth;
        }

        int retries = 0;
        String lastErrorReason = "";

        while (retries < 3) {
            try {
                HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                        .uri(URI.create(esConfig.url() + "/_bulk"))
                        .header("Content-Type", "application/json");

                if (authHeader != null) reqBuilder.header("Authorization", authHeader);

                HttpRequest request = reqBuilder.POST(HttpRequest.BodyPublishers.ofString(bulkBody.toString())).build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    String body = response.body();
                    // 逻辑错误检查
                    if (body.contains("\"errors\":true")) {
                        String logicError = parsePartialError(body);
                        log.error("❌ [{}] 写入拒绝 (逻辑错误)! 原因: {}", taskConfig.tableName(), logicError);
                        // 逻辑错误重试无效，直接存入死信队列
                        deadLetterQueueManager.save(taskConfig.tableName(), batch, "Logic_" + logicError);
                        totalFailed.addAndGet(batch.size()); // 统计失败
                        return; // 本批次结束，不抛异常，避免阻塞流水线
                    }

                    // 🟢 新增：解析响应统计 Create/Update 数量
                    int created = 0;
                    int updated = 0;
                    try {
                        JsonNode root = mapper.readTree(body);
                        JsonNode items = root.path("items");
                        if (items.isArray()) {
                            for (JsonNode item : items) {
                                // 响应项通常是 {"index": {"_index":..., "result": "created", ...}}
                                // 我们取第一个字段的值即可 (index/create/update)
                                if (item.isObject() && item.fields().hasNext()) {
                                    JsonNode resultNode = item.fields().next().getValue();
                                    String resultStatus = resultNode.path("result").asText();
                                    if ("created".equals(resultStatus)) {
                                        created++;
                                    } else if ("updated".equals(resultStatus)) {
                                        updated++;
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn("⚠️ 统计 ES 响应结果时出错: {}", e.getMessage());
                    }

                    // 🟢 更新全局统计
                    totalCreated.addAndGet(created);
                    totalUpdated.addAndGet(updated);

                    // 🟢 修改：根据数据类型输出不同日志并控制 Checkpoint
                    if (repairCount == batch.size()) {
                        // 全是修复数据
                        log.info("✅ [回溯验证] 成功将 {} 条历史数据再次写入 ES (Create:{}, Update:{})", 
                                repairCount, created, updated);
                    } else {
                        // 包含正常数据
                        log.info("✅ 成功写入 [{}] -> ES [{}] ({} 条, 含 {} 条修复) [Create:{}, Update:{}]",
                                taskConfig.tableName(), realIndex, batch.size(), repairCount, created, updated);
                    }

                    // 🟢 关键：只有存在正常增量数据时，才更新 Checkpoint
                    // 防止回溯的历史旧 ID 覆盖了当前的最新进度
                    if (lastNormalData != null) {
                        long lastIdCursor = lastNormalData.idCursorVal();
                        String lastTimestampCursor = lastNormalData.timestampCursorVal();
                        checkpointManager.save(taskConfig.tableName(), new CheckpointManager.Checkpoint(lastIdCursor, lastTimestampCursor));
                    }

                    // 🟢 2. 处理回溯修复进度
                    // 如果本批次包含修复数据，将其中最大的ID保存到 checkpoint 文件
                    if (maxRepairId > 0) {
                        checkpointManager.saveRewind(taskConfig.tableName(), maxRepairId);
                    }

                    return;
                } else {
                    lastErrorReason = "HTTP_" + response.statusCode();
                    log.warn("⚠️ ES 返回状态码: {}, 内容: {}", response.statusCode(), response.body());
                }
            } catch (Exception e) {
                lastErrorReason = "Exception_" + e.getClass().getSimpleName();
                log.warn("⚠️ [{}] 写入异常，正在重试 {}/3 ... {}", taskConfig.tableName(), retries + 1, e.getMessage());
            }

            retries++;
            try { Thread.sleep(1000L * retries); } catch (InterruptedException ignored) {}
        }

        log.error("❌ [{}] 重试耗尽，写入失败! 转存补录队列。原因: {}", taskConfig.tableName(), lastErrorReason);
        deadLetterQueueManager.save(taskConfig.tableName(), batch, lastErrorReason);
        totalFailed.addAndGet(batch.size()); // 统计失败
    }

    private String parsePartialError(String responseBody) {
        try {
            JsonNode root = mapper.readTree(responseBody);
            if (root.path("errors").asBoolean()) {
                JsonNode items = root.path("items");
                if (items.isArray() && items.size() > 0) {
                    for (JsonNode item : items) {
                        JsonNode indexObj = item.path("index");
                        if (indexObj.has("error")) {
                            return indexObj.path("error").path("reason").asText();
                        }
                    }
                }
            }
        } catch (Exception ignored) {}
        return "Unknown_Error";
    }

    public void stop() { this.running = false; }
}
