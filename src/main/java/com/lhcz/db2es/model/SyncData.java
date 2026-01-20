package com.lhcz.db2es.model;

/**
 * 数据传输对象
 * @param idCursorVal      用于断点续传的ID游标值 (来自 idColumn，必须是递增数字)
 * @param timestampCursorVal 用于断点续传的时间戳游标值 (来自 timestampColumn)
 * @param esIdVal          用于 ES 去重的唯一ID (来自 pkColumn，可以是UUID等任意唯一值)
 * @param jsonBody         转换好的JSON字符串
 * @param isRepair         是否为回溯修复数据 (true=不更新进度, false=正常更新进度)
 */
public record SyncData(long idCursorVal, String timestampCursorVal, String esIdVal, String jsonBody, boolean isRepair) {}
