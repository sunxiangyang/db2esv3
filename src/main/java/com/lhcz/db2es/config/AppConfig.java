package com.lhcz.db2es.config;

import java.util.List;

/**
 * 应用配置记录类
 */
public record AppConfig(DbConfig db, EsConfig es, WebConfig web, List<TaskConfig> tasks) {

    // 🟢 修改：增加了连接池相关配置
    public record DbConfig(
            String url,
            String user,
            String password,
            Integer maxLifetimeMs,  // 最大存活时间
            Integer idleTimeoutMs,  // 空闲回收时间
            Integer minIdle,        // 最小空闲连接数
            Integer maxPoolSize     // 最大连接数
    ) {}

    public record EsConfig(String url, String user, String password, int batchSize, int flushIntervalMs) {}

    // 🟢 新增：Web 控制台配置
    public record WebConfig(Integer port) {}

    public record TaskConfig(
            String tableName,
            String idColumn,
            String pkColumn,
            String columns,
            String esIndex,
            String esType,
            long startId
    ) {}
}
