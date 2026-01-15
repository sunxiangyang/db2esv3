package com.lhcz.db2es.model;

/**
 * 数据传输对象
 * @param cursorVal 用于断点续传的游标值 (来自 idColumn，必须是递增数字)
 * @param esIdVal   用于 ES 去重的唯一ID (来自 pkColumn，可以是UUID等任意唯一值)
 * @param jsonBody  转换好的JSON字符串
 */
public record SyncData(String cursorVal, String esIdVal, String jsonBody) {}