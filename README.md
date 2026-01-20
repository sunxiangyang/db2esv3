
# Db2Es: 数据库到Elasticsearch实时同步工具

Db2Es 是一个基于 Java 21 虚拟线程构建的高性能、高可用的数据库到 Elasticsearch 实时增量同步工具。它旨在提供一个稳定可靠的解决方案，将关系型数据库（如 MySQL、PostgreSQL）中的数据实时同步到                                         
Elasticsearch，以支持搜索、分析等业务场景。

## ✨ 主要特性

*   **实时增量同步**：基于 ID 游标进行增量数据抓取，支持断点续传。
*   **断点续传**：自动记录同步进度到 `checkpoint.properties` 文件，确保程序重启后能从上次中断的地方继续同步，避免数据重复或丢失。
*   **死信队列 (Dead-Letter Queue)**：当数据写入 Elasticsearch 失败时，自动将失败的数据保存到本地 `failed_data` 目录下的 JSON 文件中，防止数据永久丢失，便于后续人工排查补录。
*   **Java 21 虚拟线程 (Virtual Threads)**：充分利用 Java 21 的虚拟线程特性，实现高并发、低开销的数据抓取和写入，提升整体吞吐量。
*   **并发回溯校验**：引入独特的回溯校验机制，定期检查指定 ID 范围内的历史数据，以填补分布式环境或并发写入可能造成的少量数据空洞，保证数据最终一致性。
*   **可配置的数据库连接池**：使用 HikariCP 连接池，提供灵活的连接池参数配置，确保数据库连接的稳定性和高效性。
*   **动态 Elasticsearch 索引**：支持基于日期（按月或按天）的动态索引名称生成，方便管理时序数据。
*   **YAML 配置**：所有配置通过 `application.yaml` 文件管理，结构清晰，易于理解和修改。
*   **简洁高效**：核心逻辑精炼，无额外复杂依赖，专注于核心同步任务。


## 🏛️ 高层设计

Db2Es 采用经典的生产者-消费者模型：

*   **简洁高效**：核心逻辑精炼，无额外复杂依赖，专注于核心同步任务。

1.  **`JdbcSource` (生产者)**：
    *   负责从配置的数据库表中读取数据。
    *   利用 `checkpoint.properties` 中的 ID 游标实现断点续传。
    *   定期执行并发回溯校验，抓取指定范围的历史数据。
    *   将读取到的数据转换为 `SyncData` 对象，并放入一个有界 `BlockingQueue` 中。
    *   当队列满时，自动阻塞，实现背压机制，防止数据溢出和内存压力。

2.  **`BlockingQueue<SyncData>` (缓冲区)**：
    *   一个有界队列，作为生产者和消费者之间的数据传输通道，实现流量控制。

3.  **`EsSink` (消费者)**：
    *   从队列中获取 `SyncData` 对象。
    *   将数据批量写入 Elasticsearch。
    *   处理 ES 写入响应，如果成功，则更新主同步进度 (`checkpoint.properties`)。
    *   如果写入失败，将失败数据转存到死信队列 (`DeadLetterQueueManager`)。
    *   针对回溯数据，不会更新主同步进度，只会更新回溯进度。

4.  **`CheckpointManager`**：
    *   管理 `checkpoint.properties` 文件，负责读写主同步进度和回溯进度。

5.  **`DeadLetterQueueManager`**：
    *   负责将写入 ES 失败的数据保存到本地文件，以便后续处理。

## 🚀 快速开始

### 前提条件

*   Java Development Kit (JDK) 21 或更高版本
*   Apache Maven 3.x
*   一个可用的关系型数据库实例 (例如 MySQL)
*   一个可用的 Elasticsearch 实例

### 1. 配置 `application.yaml`

在项目根目录或 `config/` 目录下创建 `application.yaml` 文件，并根据您的环境进行配置。

```yaml
db:
  url: "jdbc:mysql://localhost:3306/your_database?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai"
  user: "your_db_user"
  password: "your_db_password"                                                                                                                                                                                                     
  maxLifetimeMs: 600000 # 连接最大存活时间 (毫秒)，默认 600000ms (10分钟)                                                                                                                                                          
  idleTimeoutMs: 300000 # 连接空闲超时时间 (毫秒)，默认 300000ms (5分钟)                                                                                                                                                           
  minIdle: 2            # 最小空闲连接数，默认 2                                                                                                                                                                                   
  maxPoolSize: 10       # 最大连接数，默认 10

es:
  url: "http://localhost:9200"
  user: "elastic" # 如果 ES 需要认证
  password: "your_es_password" # 如果 ES 需要认证
  batchSize: 1000       # 每次批量写入 ES 的数据条数
  flushIntervalMs: 5000 # 强制刷新到 ES 的最大等待时间 (毫秒)

tasks:
  - tableName: "your_table_name_1" # 数据库表名
    idColumn: "id"                 # 增量同步的 ID 列，必须是递增的数字类型
    pkColumn: "uuid"               # ES 的 _id 字段，推荐使用 UUID 或其他唯一业务主键，如不填则默认使用 idColumn
    columns: "id, uuid, name, create_time" # 要查询的列，逗号分隔，推荐只选择需要的列                                                                                                                                              
    esIndex: "your_index_name_1_#(dtmon)" # ES 索引名称，支持动态日期占位符 # (dtmon) -> yyyy_MM, # (dtday) -> yyyy_MM_dd                                                                                                          
    esType: "_doc"                 # ES 类型，默认为 _doc
    startId: 0                     # 首次启动时的起始 ID，此值只在 checkpoint.properties 不存在时生效

  - tableName: "your_table_name_2"
    idColumn: "id"
    pkColumn: "id"
    columns: "*"                   # 查询所有列                                                                                                                                                                                    
    esIndex: "your_index_name_2_data"                                                                                                                                                                                              
    esType: "_doc"                                                                                                                                                                                                                 
    startId: 0                                                                                                                                                                                                                     
                                                                                                                                                                                                                                   

2. 构建项目

在项目根目录下执行 Maven 命令：

                                                                                                                                                                                                                                   
mvn clean package                                                                                                                                                                                                                  
                                                                                                                                                                                                                                   

这将生成一个包含所有依赖的可执行 JAR 包，位于 target/ 目录下。

3. 运行程序

                                                                                                                                                                                                                                   
java -jar target/db2es-*.jar                                                                                                                                                                                                       
                                                                                                                                                                                                                                   

替换 db2es-*.jar 为实际生成的文件名，例如 db2es-1.0.0-SNAPSHOT.jar。


📊 进度管理 (Checkpoint)

 • 程序启动时，会尝试读取根目录下的 checkpoint.properties 文件来获取上次的同步进度。
 • 如果文件不存在，则使用 application.yaml 中配置的 startId 作为初始值。
 • 程序会定期将最新的同步进度（主增量进度和回溯进度）保存到 checkpoint.properties 文件中。
 • 示例 checkpoint.properties 内容：
   
   v_car_pass=58656
   v_person_pass=46137256
   # 回溯进度
   v_car_pass.rewind=50000
   v_person_pass.rewind=40000000
   


🚨 死信队列 (Dead-Letter Queue)

 • 当数据写入 Elasticsearch 失败（例如 ES 不可用、数据格式错误等）时，该批次数据不会被丢弃。
 • DeadLetterQueueManager 会将这些失败的数据保存到程序根目录下的 failed_data 目录中，文件名为 failed_表名_时间_原因.json。
 • 这些文件可用于后续分析和手动补录，确保数据零丢失。


📝 日志

项目使用 logback 进行日志输出，日志配置文件为 src/main/resources/logback.xml。 默认情况下，日志会输出到控制台，并分为 info.log 和 error.log 文件存储在程序根目录的 logs 文件夹下。

───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
感谢您使用 Db2Es！如果您有任何问题或建议，欢迎提出。

