package com.lhcz.db2es.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Properties;

public class CheckpointManager {
    private static final Logger log = LoggerFactory.getLogger(CheckpointManager.class);
    private static final String FILE_NAME = "checkpoint.properties";
    private final Properties props = new Properties();

    // 🟢 新增：定义 Checkpoint 数据结构，供 EsSink 使用
    public record Checkpoint(long idVal, String timestampVal) {}

    // 🟢 新增：每日统计数据结构
    public record DailyStats(long created, long updated, long failed, String date) {}

    public CheckpointManager() {
        load();
    }

    private void load() {
        try (FileInputStream in = new FileInputStream(FILE_NAME)) {
            props.load(in);
            log.info("已加载历史进度文件: {}", props);
        } catch (IOException e) {
            log.info("未找到进度文件，将使用默认配置启动");
        }
    }

    public long getStartId(String tableName, long configStartId) {
        String val = props.getProperty(tableName);
        if (val != null && !val.isBlank()) {
            return Long.parseLong(val);
        }
        return configStartId;
    }

    // 🟢 新增：获取回溯起始 ID
    public long getRewindId(String tableName, long defaultVal) {
        String val = props.getProperty(tableName + ".rewind");
        if (val != null && !val.isBlank()) {
            return Long.parseLong(val);
        }
        return defaultVal;
    }

    // 🟢 新增：获取每日统计 (带日期检查，跨天自动归零)
    public DailyStats getDailyStats(String tableName) {
        String savedDate = props.getProperty(tableName + ".stats.date");
        String today = LocalDate.now().toString();

        // 如果日期不一致（或者是新的一天），返回归零的统计
        if (savedDate == null || !savedDate.equals(today)) {
            return new DailyStats(0, 0, 0, today);
        }

        long created = Long.parseLong(props.getProperty(tableName + ".stats.created", "0"));
        long updated = Long.parseLong(props.getProperty(tableName + ".stats.updated", "0"));
        long failed = Long.parseLong(props.getProperty(tableName + ".stats.failed", "0"));

        return new DailyStats(created, updated, failed, today);
    }

    public synchronized void save(String tableName, Checkpoint checkpoint) {
        props.setProperty(tableName, String.valueOf(checkpoint.idVal));
        saveToFile();
    }

    // 🟢 新增：单独保存回溯进度
    public synchronized void saveRewind(String tableName, long rewindId) {
        props.setProperty(tableName + ".rewind", String.valueOf(rewindId));
        saveToFile();
    }

    // 🟢 新增：保存每日统计
    public synchronized void saveDailyStats(String tableName, DailyStats stats) {
        props.setProperty(tableName + ".stats.date", stats.date());
        props.setProperty(tableName + ".stats.created", String.valueOf(stats.created()));
        props.setProperty(tableName + ".stats.updated", String.valueOf(stats.updated()));
        props.setProperty(tableName + ".stats.failed", String.valueOf(stats.failed()));
        saveToFile();
    }

    private void saveToFile() {
        try (FileOutputStream out = new FileOutputStream(FILE_NAME)) {
            props.store(out, "Db2Es 数据同步进度");
        } catch (IOException e) {
            log.error("保存进度失败!", e);
        }
    }
}
