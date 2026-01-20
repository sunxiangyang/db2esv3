package com.lhcz.db2es.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 简易 Web 管理控制台
 * 提供任务状态监控页面
 */
public class WebConsole {
    private static final Logger log = LoggerFactory.getLogger(WebConsole.class);
    private final int port;
    private final List<JdbcSource> sources;
    private final List<EsSink> sinks;
    private HttpServer server;
    private final ObjectMapper mapper = new ObjectMapper();

    public WebConsole(int port, List<JdbcSource> sources, List<EsSink> sinks) {
        this.port = port;
        this.sources = sources;
        this.sinks = sinks;
    }

    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/", new DashboardHandler());
            server.createContext("/api/status", new StatusHandler());
            server.setExecutor(null); // creates a default executor
            server.start();
            log.info("🌐 Web 管理控制台已启动: http://localhost:{}", port);
        } catch (IOException e) {
            log.error("❌ Web 控制台启动失败", e);
        }
    }

    private class DashboardHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            String html = """
                <!DOCTYPE html>
                <html lang="zh-CN">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Db2Es 任务监控</title>
                    <style>
                        body {
                            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                            background-color: #f3f4f6;
                            margin: 0;
                            padding: 20px;
                            color: #1f2937;
                        }
                        .container {
                            max-width: 1200px;
                            margin: 0 auto;
                        }
                        .header {
                            display: flex;
                            justify-content: space-between;
                            align-items: center;
                            margin-bottom: 20px;
                        }
                        h1 {
                            font-size: 1.5rem;
                            font-weight: bold;
                            margin: 0;
                        }
                        .refresh-hint {
                            font-size: 0.875rem;
                            color: #6b7280;
                        }
                        .card {
                            background: white;
                            border-radius: 8px;
                            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
                            overflow: hidden;
                        }
                        table {
                            width: 100%;
                            border-collapse: collapse;
                            text-align: left;
                        }
                        th {
                            background-color: #f9fafb;
                            color: #4b5563;
                            font-weight: 600;
                            text-transform: uppercase;
                            font-size: 0.75rem;
                            padding: 12px 24px;
                            border-bottom: 2px solid #e5e7eb;
                        }
                        td {
                            padding: 12px 24px;
                            border-bottom: 1px solid #e5e7eb;
                            font-size: 0.875rem;
                        }
                        tr:last-child td {
                            border-bottom: none;
                        }
                        .badge {
                            display: inline-block;
                            padding: 2px 8px;
                            font-size: 0.75rem;
                            font-weight: 600;
                            border-radius: 9999px;
                            background-color: #dbeafe;
                            color: #1e40af;
                        }
                        .text-green { color: #059669; font-weight: bold; }
                        .text-yellow { color: #d97706; font-weight: bold; }
                        .text-red { color: #dc2626; font-weight: bold; }
                        .font-bold { font-weight: bold; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">
                            <h1>Db2Es 数据同步监控</h1>
                            <span class="refresh-hint">自动刷新中...</span>
                        </div>
                        
                        <div class="card">
                            <table>
                                <thead>
                                    <tr>
                                        <th>表名 (Table)</th>
                                        <th>索引 (Index)</th>
                                        <th>当前 ID 进度</th>
                                        <th>当日创建 (Created)</th>
                                        <th>当日更新 (Updated)</th>
                                        <th>当日失败 (Failed)</th>
                                    </tr>
                                </thead>
                                <tbody id="task-list">
                                    <!-- 数据将通过 JS 插入 -->
                                </tbody>
                            </table>
                        </div>
                    </div>

                    <script>
                        function fetchStatus() {
                            fetch('/api/status')
                                .then(response => response.json())
                                .then(data => {
                                    const tbody = document.getElementById('task-list');
                                    tbody.innerHTML = '';
                                    data.forEach(task => {
                                        const row = `
                                            <tr>
                                                <td>
                                                    <span class="font-bold">${task.tableName}</span>
                                                </td>
                                                <td>
                                                    ${task.esIndex}
                                                </td>
                                                <td>
                                                    <span class="badge">${task.currentId}</span>
                                                </td>
                                                <td>
                                                    <span class="text-green">+${task.totalCreated}</span>
                                                </td>
                                                <td>
                                                    <span class="text-yellow">~${task.totalUpdated}</span>
                                                </td>
                                                <td>
                                                    <span class="text-red">${task.totalFailed}</span>
                                                </td>
                                            </tr>
                                        `;
                                        tbody.innerHTML += row;
                                    });
                                })
                                .catch(err => console.error('Error fetching status:', err));
                        }

                        // 初始加载并每 3 秒刷新一次
                        fetchStatus();
                        setInterval(fetchStatus, 3000);
                    </script>
                </body>
                </html>
            """;

            t.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            t.sendResponseHeaders(200, html.getBytes(StandardCharsets.UTF_8).length);
            OutputStream os = t.getResponseBody();
            os.write(html.getBytes(StandardCharsets.UTF_8));
            os.close();
        }
    }

    private class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            List<Map<String, Object>> statusList = new ArrayList<>();

            // 假设 sources 和 sinks 是按顺序对应的
            for (int i = 0; i < sources.size(); i++) {
                JdbcSource source = sources.get(i);
                EsSink sink = sinks.get(i);

                Map<String, Object> status = new HashMap<>();
                status.put("tableName", source.getTaskConfig().tableName());
                status.put("esIndex", source.getTaskConfig().esIndex());
                status.put("currentId", source.getCurrentId());
                status.put("totalCreated", sink.getTotalCreated());
                status.put("totalUpdated", sink.getTotalUpdated());
                status.put("totalFailed", sink.getTotalFailed());
                statusList.add(status);
            }

            String json = mapper.writeValueAsString(statusList);
            t.getResponseHeaders().set("Content-Type", "application/json");
            t.sendResponseHeaders(200, json.getBytes(StandardCharsets.UTF_8).length);
            OutputStream os = t.getResponseBody();
            os.write(json.getBytes(StandardCharsets.UTF_8));
            os.close();
        }
    }
}
