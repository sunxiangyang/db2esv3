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
                    <script src="https://cdn.tailwindcss.com"></script>
                </head>
                <body class="bg-gray-100 p-8">
                    <div class="max-w-6xl mx-auto">
                        <div class="flex justify-between items-center mb-8">
                            <h1 class="text-3xl font-bold text-gray-800">Db2Es 数据同步监控</h1>
                            <span class="text-sm text-gray-500">自动刷新中...</span>
                        </div>
                        
                        <div class="bg-white shadow-md rounded-lg overflow-hidden">
                            <table class="min-w-full leading-normal">
                                <thead>
                                    <tr>
                                        <th class="px-5 py-3 border-b-2 border-gray-200 bg-gray-100 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">表名 (Table)</th>
                                        <th class="px-5 py-3 border-b-2 border-gray-200 bg-gray-100 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">索引 (Index)</th>
                                        <th class="px-5 py-3 border-b-2 border-gray-200 bg-gray-100 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">当前 ID 进度</th>
                                        <th class="px-5 py-3 border-b-2 border-gray-200 bg-gray-100 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">当日创建 (Created)</th>
                                        <th class="px-5 py-3 border-b-2 border-gray-200 bg-gray-100 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">当日更新 (Updated)</th>
                                        <th class="px-5 py-3 border-b-2 border-gray-200 bg-gray-100 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">当日失败 (Failed)</th>
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
                                                <td class="px-5 py-5 border-b border-gray-200 bg-white text-sm">
                                                    <div class="flex items-center">
                                                        <div class="ml-3">
                                                            <p class="text-gray-900 whitespace-no-wrap font-bold">${task.tableName}</p>
                                                        </div>
                                                    </div>
                                                </td>
                                                <td class="px-5 py-5 border-b border-gray-200 bg-white text-sm">
                                                    <p class="text-gray-900 whitespace-no-wrap">${task.esIndex}</p>
                                                </td>
                                                <td class="px-5 py-5 border-b border-gray-200 bg-white text-sm">
                                                    <span class="relative inline-block px-3 py-1 font-semibold text-blue-900 leading-tight">
                                                        <span aria-hidden="true" class="absolute inset-0 bg-blue-200 opacity-50 rounded-full"></span>
                                                        <span class="relative">${task.currentId}</span>
                                                    </span>
                                                </td>
                                                <td class="px-5 py-5 border-b border-gray-200 bg-white text-sm">
                                                    <p class="text-green-600 whitespace-no-wrap font-bold">+${task.totalCreated}</p>
                                                </td>
                                                <td class="px-5 py-5 border-b border-gray-200 bg-white text-sm">
                                                    <p class="text-yellow-600 whitespace-no-wrap font-bold">~${task.totalUpdated}</p>
                                                </td>
                                                <td class="px-5 py-5 border-b border-gray-200 bg-white text-sm">
                                                    <p class="text-red-600 whitespace-no-wrap font-bold">${task.totalFailed}</p>
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
