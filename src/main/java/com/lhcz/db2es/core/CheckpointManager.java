package com.lhcz.db2es.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class CheckpointManager {
    private static final Logger log = LoggerFactory.getLogger(CheckpointManager.class);
    private static final String FILE_NAME = "checkpoint.properties";
    private final Properties props = new Properties();

    // 🟢 新增：定义 Checkpoint 数据结构，供 EsSink 使用
    public record Checkpoint(long idVal, String timestampVal) {}

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

    public synchronized void save(String tableName, Checkpoint checkpoint) {
        props.setProperty(tableName, String.valueOf(checkpoint.idVal));
        saveToFile();
    }

    // 🟢 新增：单独保存回溯进度
    public synchronized void saveRewind(String tableName, long rewindId) {
        props.setProperty(tableName + ".rewind", String.valueOf(rewindId));
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
