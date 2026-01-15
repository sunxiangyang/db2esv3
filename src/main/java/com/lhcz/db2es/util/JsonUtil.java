package com.lhcz.db2es.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class JsonUtil {
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        // 🔴 关键修复：配置日期格式
        // 禁用默认的时间戳格式，改为使用字符串格式 "yyyy-MM-dd HH:mm:ss"
        // 这能兼容 ES 中常见的 date 类型 Mapping
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 使用系统默认时区 (通常与数据库和服务器一致)
        sdf.setTimeZone(TimeZone.getDefault());
        mapper.setDateFormat(sdf);
    }

    public static String resultSetToJson(ResultSet rs) {
        try {
            ObjectNode node = mapper.createObjectNode();
            ResultSetMetaData meta = rs.getMetaData();
            int count = meta.getColumnCount();
            for (int i = 1; i <= count; i++) {
                String name = meta.getColumnLabel(i);
                Object value = rs.getObject(i);
                if (value != null) {
                    node.putPOJO(name, value);
                }
            }
            return mapper.writeValueAsString(node);
        } catch (Exception e) {
            throw new RuntimeException("JSON conversion failed", e);
        }
    }
}