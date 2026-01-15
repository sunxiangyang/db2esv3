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
        String realIndex = resolveIndexName(taskConfig.esIndex());
        String realType = (taskConfig.esType() != null && !taskConfig.esType().isBlank()) ? taskConfig.esType() : "_doc";

        StringBuilder bulkBody = new StringBuilder();
        for (SyncData item : batch) {
            bulkBody.append(String.format("{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n",
                    realIndex, realType, item.esIdVal()));
            bulkBody.append(item.jsonBody()).append("\n");
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
                        return; // 本批次结束，不抛异常，避免阻塞流水线
                    }

                    log.info("✅ 成功写入 [{}] -> ES [{}] ({} 条)", taskConfig.tableName(), realIndex, batch.size());

                    if (!batch.isEmpty()) {
                        String lastCursor = batch.get(batch.size() - 1).cursorVal();
                        checkpointManager.save(taskConfig.tableName(), lastCursor);
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