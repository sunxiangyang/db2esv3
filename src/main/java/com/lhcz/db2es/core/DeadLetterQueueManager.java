package com.lhcz.db2es.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lhcz.db2es.model.SyncData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 死信队列管理器 (数据补录)
 * 作用：当 ES 写入失败时，将数据保存到本地文件，防止丢失。
 */
public class DeadLetterQueueManager {
    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueManager.class);
    private static final String FAIL_DIR = "failed_data";
    private final ObjectMapper mapper = new ObjectMapper();

    public DeadLetterQueueManager() {
        File dir = new File(FAIL_DIR);
        if (!dir.exists()) {
            if (dir.mkdirs()) {
                log.info("📂 已创建补录数据目录: {}", dir.getAbsolutePath());
            }
        }
    }

    /**
     * 保存失败批次到磁盘
     */
    public void save(String tableName, List<SyncData> batch, String reason) {
        if (batch == null || batch.isEmpty()) return;

        // 生成文件名: failed_表名_时间_原因.json
        String timeStr = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String safeReason = reason.replaceAll("[^a-zA-Z0-9]", "_");
        if (safeReason.length() > 30) safeReason = safeReason.substring(0, 30);

        String fileName = String.format("%s/failed_%s_%s_%s.json", FAIL_DIR, tableName, timeStr, safeReason);

        try {
            File file = new File(fileName);
            mapper.writerWithDefaultPrettyPrinter().writeValue(file, batch);
            log.error("💾 [补录保存] 写入失败的数据已保存到文件! 路径: {}, 原因: {}", fileName, reason);
        } catch (IOException e) {
            log.error("🚨 [严重错误] 无法保存失败数据! 数据可能永久丢失! 表: {}", tableName, e);
        }
    }
}