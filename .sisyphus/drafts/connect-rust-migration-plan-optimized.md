# Kafka Connect模块Rust迁移方案（优化版 - API契约前置）

## TL;DR

> **项目目标**: 将Apache Kafka Connect模块从Java迁移到Rust，实现1:1功能映射，确保无遗漏
> **代码规模**: 629个Java文件，64,761行主代码，199个测试文件，~33,200行测试代码
> **输出目录**: `connect-rust-new/`
> **核心策略**: **API契约前置识别 + 阶段1生成所有crate骨架 + 后续阶段完全并行**
> **并发度提升**: 预计提升60-70%整体效率

---

## 一、优化策略说明

### 1.1 原计划的问题

**串行依赖问题**：
- 各模块的实现存在串行依赖
- 需要等待上游模块完全实现后才能开始下游模块
- 无法充分利用并发度

**示例**：
```
原计划流程：
阶段1: kafka-clients-trait → kafka-clients-mock (2任务并行)
阶段2: connect-api (等待阶段1完成)
阶段3: connect-transforms, basic-auth, mirror-client (等待阶段2完成)
阶段4: connect-runtime-core (等待阶段3完成)
...
```

### 1.2 优化后的策略

**核心思想**：**API契约前置识别 + 骨架生成 + 完全并行实现**

```
优化后流程：
阶段0: API接口识别与依赖分析 (7任务并行)
  ↓
阶段1: 所有crate骨架 + API契约生成 (10任务并行)
  ↓
阶段2-8: 各模块完全并行实现 (7个模块可同时开发)
  ↓
阶段9: 测试迁移与端到端验证
```

### 1.3 优化优势

| 维度 | 原计划 | 优化后 | 提升 |
|------|--------|--------|------|
| **阶段1并发度** | 2任务 | 10任务 | **5倍** |
| **模块实现并发度** | 串行/部分并行 | 完全并行 | **极大提升** |
| **API契约确定时机** | 分散在各阶段 | 阶段1完成 | **提前锁定** |
| **团队协作灵活性** | 低（依赖串行） | 高（完全独立） | **极大提升** |
| **总体效率** | 基准 | +60-70% | **显著提升** |

---

## 二、项目概述

### 2.1 迁移范围

**包含模块**:
- `connect/api` - Connect API核心接口定义（102文件，13,601行）
- `connect/runtime` - Connect运行时实现（388文件，25,920行）
- `connect/transforms` - 数据转换实现（47文件，8,687行）
- `connect/mirror` - MirrorMaker2实现（62文件，12,935行）
- `connect/mirror-client` - Mirror客户端库（13文件，1,632行）
- `connect/basic-auth-extension` - 基本基础认证扩展（5文件，881行）
- `connect/test-plugins` - 测试插件（12文件，1,135行）

**排除模块**:
- `connect/file` - 文件连接器（用户明确排除）
- `connect/json` - JSON连接器（用户明确排除）

### 2.2 核心要求

1. **1:1映射**: 函数和类名严格对应，代码细节遵从kafka，禁止遗漏
2. **编译和UT通过**: 每个任务完成后必须`cargo check`或`cargo build`成功
3. **禁止空实现**: 禁止生成仅包含注释、空函数、默认值或TODO的代码
4. **二次检查**: 每个任务完成后检查代码生成无遗漏，代码行数偏差不大
5. **独立项目**: 大项目拆成多个独立项目，每个可独立编译和测试
6. **依赖Mock**: 外部依赖（kafka-client）用独立crate的mock实现
7. **无遗漏**: 包括测试代码迁移，补充端到端MM2测试
8. **充分并发**: 大阶段内拆分子任务，无依赖关系可并行开发
9. **上下文压缩**: 循环任务中上下文超限时自动compact
10. **业界实践**: 探索优秀实践确保高质量

### 2.3 优化要求

1. **API契约前置**: 阶段0识别所有模块的对外API接口
2. **骨架生成**: 阶段1生成所有crate的trait定义和类型声明
3. **完全并行**: 阶段2-8的模块可以同时开发
4. **独立验证**: 每个模块可以独立编译和测试
5. **细粒度任务拆解**: 每个阶段包含多个可独立编译和测试的子任务
6. **每个任务独立验证**: 每个任务完成后立即编译和测试，不积压到最后
7. **难点模块细致拆分**: runtime、mirror等复杂模块拆解为更小的子任务
8. **二次检查AGENTS.md**: 每个任务执行后检查是否与AGAGENTS.md一致，不一致及时修改
9. **不着急下一个任务**: 每个任务完成后等待验证通过再继续
10. **每个任务都设计UT**: 每个任务都有对应的单元测试
11. **每个阶段完成后自检**: 每个阶段完成后针对针对AGENTS.md要求进行自检

---

## 三、Connect对Kafka其他模块的依赖分析

### 3.1 核心发现

**Connect模块的主代码仅依赖kafka-clients**，其他kafka模块的依赖仅存在于测试代码中。

### 3.2 主代码依赖（编译时+运行时）

| Connect模块 | 依赖的Kafka模块 | 依赖类型 |
|------------|----------------|---------|
| connect/api | kafka-clients | api依赖 |
| connect/runtime | kafka-clients | api依赖 |
| connect/transforms | kafka-clients | 通过connect/api间接依赖 |
| connect/mirror | kafka-clients | api依赖 |
| connect/mirror-client | kafka-clients | implementation依赖 |
| connect/basic-auth-extension | kafka-clients | 通过connect/api间接依赖 |
| connect/test-plugins | kafka-clients + kafka-server-common | api + implementation依赖 |

### 3.3 测试代码依赖（仅测试时）

| Connect模块 | 依赖的Kafka模块 | 使用范围 |
|------------|----------------|---------|
| connect/runtime | kafka-core, kafka-server, kafka-metadata, kafka-server-common, kafka-raft, kafka-storage, kafka-group-coordinator, kafka-test-common | 集成测试 |
| connect/file | kafka-core, kafka-server-common, kafka-test-common | 集成测试 |
| connect/mirror | kafka-core, kafka-raft, kafka-server, kafka-server-common, kafka-test-common | 集成测试 |

### 3.4 依赖汇总表

| Kafka模块 | 主代码依赖 | 测试代码依赖 | 使用范围 |
|-----------|-----------|-------------|---------|
| **kafka-clients** | ✅ 所有connect子模块 | ✅ 所有测试 | 编译时+运行时 |
| **kafka-core** | ❌ | ✅ runtime, file, mirror测试 | 仅测试时 |
| **kafka-server** | ❌ | ✅ runtime, mirror测试 | 仅测试时 |
| **kafka-metadata** | ❌ | ✅ runtime测试 | 仅测试时 |
| **kafka-server-common** | ✅ test-plugins | ✅ runtime, file, mirror测试 | 编译时+测试时 |
| **kafka-raft** | ❌ | ✅ runtime, mirror测试 | 仅测试时 |
| **kafka-storage** | ❌ | ✅ runtime测试 | 仅测试时 |
| **kafka-group-coordinator** | ❌ | ✅ runtime测试 | 仅测试时 |
| **kafka-test-common** | ❌ | ✅ runtime, file, mirror测试 | 仅测试时 |

### 3.5 关键结论

1. **主代码仅依赖kafka-clients**，这是一个非常好的解耦设计
2. **其他kafka模块的依赖仅存在于测试代码中**，不影响生产运行
3. **connect:test-plugins依赖server-common**，但这是测试工具，不影响生产代码
4. **迁移到Rust的主要挑战**在于测试基础设施，而非核心功能
5. **可以采用渐进式迁移策略**，先迁移主代码，再处理测试

---

## 四、Kafka-Client接口API契约细化

### 4.1 Producer接口使用

**核心方法签名**：
```java
// 异步发送记录（带回调）
Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

// 刷新待发送的记录
void flush();

// 关闭producer
void close(Duration timeout);

// 事务相关方法（仅Exactly-Once使用）
void initTransactions(boolean keepPreparedTxn);
void beginTransaction();
void commitTransaction();
void abortTransaction();
```

**ProducerRecord字段**：
- `topic` - 目标主题
- `partition` - 分区号（可选）
- `key` - 键（字节数组）
- `value` - 值（字节数组）
- `headers` - 记录头
- `timestamp` - 时间戳

**Callback接口**：
```java
interface Callback {
    void onCompletion(RecordMetadata metadata, Exception exception);
}
```

**RecordMetadata字段**：
- `topic()` - 主题名
- `partition()` - 分区号
- `offset()` - 偏移量
- `timestamp()` - 时间戳
- `serializedKeySize()` - 键大小
- `serializedValueSize()` - 值大小

**使用频率统计**：
- `send()` - 极高（每次发送记录）
- `flush()` - 高（KafkaBasedLog、Worker关闭时）
- `close()` - 高（资源清理）
- 事务方法 - 中（仅Exactly-Once语义）

### 4.2 Consumer接口使用

**核心方法签名**：
```java
// 订阅主题（带rebalance监听器）
void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);
void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

// 手动分配分区
void assign(Collection<TopicPartition> partitions);

// 轮询记录
ConsumerRecords<K, V> poll(Duration timeout);

// 提交偏移量
void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);
void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

// 位置控制
void seek(TopicPartition partition, long offset);
void seekToBeginning(Collection<TopicPartition> partitions);

// 唤醒consumer
void wakeup();

// 获取信息
Set<TopicPartition> assignment();
long position(TopicPartition partition);
Map<TopicPartition, Long> endOffsets(Set<TopicPartition> partitions);
List<PartitionInfo> partitionsFor(String topic);
```

**ConsumerRecord字段**：
- `topic()` - 主题名
- `partition()` - 分区号
- `offset()` - 偏移量
- `key()` - 键（字节数组）
- `value()` - 值（字节数组）
- `headers()` - 记录头
- `timestamp()` - 时间戳
- `timestampType()` - 时间戳类型

**ConsumerRebalanceListener接口**：
```java
interface ConsumerRebalanceListener {
    void onPartitionsAssigned(Collection<TopicPartition> partitions);
    void onPartitionsRevoked(Collection<TopicPartition> partitions);
    void onPartitionsLost(Collection<TopicPartition> partitions);
}
```

**OffsetAndMetadata字段**：
- `offset()` - 偏移量
- `metadata()` - 元数据（字符串）

**使用频率统计**：
- `poll()` - 极高（持续消费）
- `subscribe()/assign()` - 高（初始化时）
- `commitSync()/commitAsync()` - 高（定期提交）
- `seek()` - 中（初始化、重放）
- `wakeup()` - 中（停止、暂停）
- `assignment()/position()` - 中（状态查询）

### 4.3 AdminClient接口使用

**核心方法签名**：
```java
// 创建主题
CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options);

// 描述主题
DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options);

// 描述配置
DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options);

// 获取结束偏移量
ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> offsetSpecs, ListOffsetsOptions options);

// 列出主题
ListTopicsResult listTopics(ListTopicsOptions options);
```

**NewTopic字段**：
- `name()` - 主题名
- `numPartitions()` - 分区数
- `replicationFactor()` - 副本因子
- `configs()` - 配置映射

**TopicDescription字段**：
- `name()` - 主题名
- `partitions()` - 分区信息列表
- `isInternal()` - 是否内部主题

**Config字段**：
- `entries()` - 配置项列表

**使用频率统计**：
- `createTopics()` - 高（自动创建主题）
- `describeTopics()` - 高（检查主题是否存在）
- `describeConfigs()` - 中（验证配置）
- `endOffsets()` - 中（KafkaBasedLog）
- `listTopics()` - 低（MirrorClient）

### 4.4 序列化接口使用

**Serializer接口**：
```java
interface Serializer<T> {
    byte[] serialize(String topic, T data);
    void configure(Map<String, ?> configs);
    void close();
}
```

**Deserializer接口**：
```java
interface Deserializer<T> {
    T deserialize(String topic, byte[] data);
    void configure(Map<String, ?> configs);
    void close();
}
```

**常用实现**：
- `ByteArraySerializer` - 字节数组序列化
- `ByteArrayDeserializer` - 字节数组反序列化

**使用频率统计**：
- Connect模块主要使用`ByteArraySerializer/Deserializer`
- 自定义序列化器由用户connector提供

### 4.5 配置类使用

**ProducerConfig常用配置**：
- `BOOTSTRAP_SERVERS_CONFIG` - "bootstrap.servers"
- `ACKS_CONFIG` - "acks"（通常设为"all"）
- `RETRIES_CONFIG` - "retries"
- `MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION` - "max.in.flight.requests.per.connection"
- `KEY_SERIALIZER_CLASS_CONFIG` - "key.serializer"
- `VALUE_SERIALIZER_CLASS_CONFIG` - "value.serializer"
- `CLIENT_ID_CONFIG` - "client.id"

**ConsumerConfig常用配置**：
- `BOOTSTRAP_SERVERS_CONFIG` - "bootstrap.servers"
- `GROUP_ID_CONFIG` - "group.id"
- `ENABLE_AUTO_COMMIT_CONFIG` - "enable.auto.commit"（通常设为false）
- `AUTO_OFFSET_RESET_CONFIG` - "auto.offset.reset"（通常设为"earliest"）
- `KEY_DESERIALIZER_CLASS_CONFIG` - "key.deserializer"
- `VALUE_DESERIALIZER_CLASS_CONFIG` - "value.deserializer"
- `ISOLATION_LEVEL_CONFIG` - "isolation.level"（Exactly-Once时使用）

**CommonClientConfigs常用配置**：
- `BOOTSTRAP_SERVERS_CONFIG` - "bootstrap.servers"
- `CLIENT_ID_CONFIG` - "client.id"
- `METRICS_RECORDING_LEVEL_CONFIG` - "metrics.recording.level"

### 4.6 其他关键类

**TopicPartition**：
```java
// 表示主题分区的不可变类
- `topic()` - 主题名
- `partition()` - 分区号
```

**OffsetAndMetadata**：
```java
// 表示偏移量和元数据
- `offset()` - 偏移量
- `metadata()` - 元数据字符串
```

**ConsumerGroupMetadata**：
```java
// 表示消费者组元数据
- `groupId()` - 消费者组ID
- `memberId()` - 成员ID
```

**ClusterMetadata**：
```java
// 表示集群元数据
- `clusterId()` - 集群ID
- `controller()` - 控制器地址
- `nodes()` - 节点列表
```

### 4.7 推荐的Rust Trait设计

```rust
// Producer Trait
pub trait KafkaProducer {
    fn send(&self, record: ProducerRecord, callback: Option<Box<dyn FnOnce(RecordMetadata, Option<Error>)>>) -> Box<dyn Future<RecordMetadata>>;
    fn flush(&self);
    fn close(&self, timeout: Duration);

    // 事务相关（可选）
    fn init_transactions(&self, keep_prepared_txn: bool);
    fn begin_transaction(&self);
    fn commit_transaction(&self);
    fn abort_transaction(&self);
}

// Consumer Trait
pub trait KafkaConsumer {
    fn subscribe(&self, topics: Vec<String>, listener: Option<Box<dyn ConsumerRebalanceListener>>);
    fn subscribe_pattern(&self, pattern: Regex, listener: Option<Box<dyn ConsumerRebalanceListener>>);
    fn assign(&self, partitions: Vec<TopicPartition>);

    fn poll(&self, timeout: Duration) -> ConsumerRecords;

    fn commit_sync(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>);
    fn commit_async(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>, callback: Box<dyn FnOnce(Option<Error>)>);

    fn seek(&self, partition: TopicPartition, offset: i64);
    fn seek_to_beginning(&self, partitions: Vec<TopicPartition>);

    fn wakeup(&self);
    fn assignment(&self) -> HashSet<TopicPartition>;
    fn position(&self, partition: TopicPartition) -> i64;
    fn end_offsets(&self, partitions: HashSet<TopicPartition>) -> HashMap<TopicPartition, i64>;
}

// AdminClient Trait
pub trait KafkaAdmin {
    fn create_topics(&self, topics: Vec<NewTopic>, options: CreateTopicsOptions) -> CreateTopicsResult;
    fn describe_topics(&self, topic_names: Vec<String>, options: DescribeTopicsOptions) -> DescribeTopicsResult;
    fn describe_configs(&self, resources: Vec<ConfigResource>, options: DescribeConfigsOptions) -> DescribeConfigsResult;
    fn list_offsets(&self, offset_specs: HashMap<TopicPartition, OffsetSpec>, options: ListOffsetsOptions) -> ListOffsetsResult;
    fn list_topics(&self, options: ListTopicsOptions) -> ListTopicsResult;
}

// Serializer/Deserializer Traits
pub trait Serializer<T> {
    fn serialize(&self, topic: &str, data: &T) -> Vec<u8>;
    fn configure(&self, configs: HashMap<String, String>);
    fn close(&self);
}

pub trait Deserializer<T> {
    fn deserialize(&self, topic: &str, data: &[u8]) -> T;
    fn configure(&self, configs: HashMap<String, String>);
    fn close(&self);
}
```

### 4.8 可能的简化建议

**可以简化或延后实现的方法**：

1. **Producer事务方法**：
   - 仅Exactly-Once语义需要
   - 可以先实现基本版本，事务作为可选feature

2. **AdminClient的高级操作**：
   - `listTopics()` - 仅MirrorClient使用
   - `describeConfigs()` - 仅验证配置时使用
   - 可以先实现核心的`createTopics()`和`describeTopics()`

3. **Consumer的复杂操作**：
   - `subscribe(Pattern)` - 正则订阅较少使用
   - `seek(TopicPartition, OffsetAndMetadata)` - 使用较少
   - 可以先实现基于Collection的subscribe和基于long的seek

4. **Metrics相关方法**：
   - `metrics()`, `partitionsFor()` - 主要用于监控
   - 可以延后实现

**必须实现的核心方法**：

1. **Producer**：
   - `send()` - 必须
   - `flush()` - 必须
   - `close()` - 必须

2. **Consumer**：
   - `subscribe()` / `assign()` - 必须
   - `poll()` - 必须
   - `commitSync()` / `commit()` - 必须
   - `seek()` / `seekToBeginning()` - 必须
   - `wakeup()` - 必须
   - `assignment()` / `position()` - 必须

3. **AdminClient**：
   - `createTopics()` - 必须（自动创建主题）
   - `describeTopics()` - 必须（检查主题存在）
   - `endOffsets()` - 必须（KafkaBasedLog）

4. **序列化**：
   - `ByteArraySerializer` / `ByteArrayDeserializer` - 必须

---

## 五、推荐的Crate拆分方案

### 5.1 方案：完全独立编译（推荐）

```
connect-rust-new/
├── kafka-clients-trait/          # Kafka客户端trait定义（独立）
│   ├── 依赖: 无
│   └── 代码量: ~500行
│
├── kafka-clients-mock/           # Kafka客户端mock实现（独立）
│   ├── 依赖: kafka-clients-trait
│   └── 代码量: ~2000行
│
├── connect-api/                  # Connect API核心接口（独立）
│   ├── 依赖: kafka-clients-trait
│   └── 代码量: ~13K行
│
├── connect-transforms/           # 数据转换（独立）
│   ├── 依赖: connect-api
│   └── 代码量: ~8K行
│
├── connect-runtime-core/         # 运行时核心（独立）
│   ├── 依赖: connect-api, connect-transforms, kafka-clients-trait
│   └── 代码量: ~25K行
│
├── connect-runtime-distributed/  # 分布式组件（独立）
│   ├── 依赖: connect-runtime-core
│   └── 代码量: ~5K行
│
├── connect-mirror-client/        # Mirror客户端（独立）
│   ├── 依赖: kafka-clients-trait
│   └── 代码量: ~1.5K行
│
├── connect-mirror/               # MirrorMaker2（独立）
│   ├── 依赖: connect-runtime-core, connect-mirror-client
│   └── 代码量: ~12K行
│
└── connect-basic-auth-extension/ # 基本认证扩展（独立）
    ├── 依赖: connect-api
    └── 代码量: ~800行
```

### 5.2 编译顺序（优化后）

**阶段0 - API接口识别与依赖分析（新增）**:
- 0.1 分析connect/api模块的public接口
- 0.2 分析connect/runtime模块的public接口
- 0.3 分析connect/transforms模块的public接口
- 0.4 分析connect/mirror和mirror-client的public接口
- 0.5 分析connect/basic-auth-extension的public接口
- 0.6 生成模块依赖关系图
- 0.7 生成API契约文档

**阶段1 - 基础设施与API骨架生成（优化 - 完全并行）**:
- 1.1 创建workspace和所有crate的Cargo.toml
- 1.2.1 实现kafka-clients-trait crate（trait定义）
- 1.2.2 实现kafka-clients-mock crate（trait实现）
- 1.3 生成connect-api的trait和类型定义（不包含实现）
- 1.4 生成connect-transforms的trait定义（不包含实现）
- 1.5 生成connect-runtime-core的trait定义（不包含实现）
- 1.6 生成connect-runtime-distributed的trait定义（不包含实现）
- 1.7 生成connect-mirror-client的trait定义（不包含实现）
- 1.8 生成connect-mirror的trait定义（不包含实现）
- 1.9 生成connect-basic-auth-extension的trait定义（不包含实现）

**阶段2 - API层实现（优化 - 可与其他阶段并行）**:
- 2.1 实现connect-api模块（基于阶段1的trait定义）

**阶段3 - 功能层（优化 - 完全并行）**:
- 3.1 实现connect-transforms模块
- 3.2 实现connect-basic-auth-extension模块
- 3.3 实现connect-mirror-client模块

**阶段4 - 核心层（优化 - 可与其他阶段并行）**:
- 4.1 实现connect-runtime-core模块

**阶段5 - 分布式层（优化 - 可与其他阶段并行）**:
- 5.1 实现connect-runtime-distributed模块

**阶段6 - 应用层（优化 - 可与其他阶段并行）**:
- 6.1 实现connect-mirror模块

**阶段7 - 测试迁移（优化 - 可并行）**:
- 7.1 迁移单元测试
- 7.2 迁移集成测试
- 7.3 实现端到端MM2测试

---

## 六、Java到Rust转换难点分析

### 6.1 高优先级难点（必须解决）

#### 1. 动态类加载和反射使用（严重程度：高）

**问题**:
- Connect依赖Java的类加载器机制实现插件隔离
- 使用反射在运行时动态发现和实例化Connector、Converter、Transformation
- 支持版本化插件加载

**推荐解决方案**:
- 使用`libloading` crate实现动态库加载
- 使用`abi`和`serde`定义插件ABI接口
- 使用`dlopen`/`dlsym`加载符号
- 实现插件注册表（类似ServiceLoader）
- **可能需要放弃的功能**: 运行时动态发现插件（需要编译时注册）

#### 2. 异常处理模式（严重程度：高）

**问题**:
- Java使用checked exception，方法签名必须声明throws
- 大量自定义异常：ConnectException、DataException、ConfigException

**推荐解决方案**:
- 使用`Result<T, E>`替代checked exception
- 使用`anyhow::Error`或`thiserror`定义错误类型
- 使用`?`运算符处理可恢复错误
- 使用`Box<dyn std::error::Error>`处理多态错误

#### 3. 并发模型差异（严重程度：高）

**问题**:
- Java使用`ExecutorService`线程池模型
- 使用`Future`、`CompletableFuture`进行异步编程
- Worker为每个Task分配独立线程

**推荐解决方案**:
- 使用`tokio`运行时和异步任务
- 使用`tokio::task::spawn`替代线程池
- 使用`tokio::sync::Mutex`替代`synchronized`
- **架构调整**: 从"每任务一线程"改为"每任务一async task"

#### 4. SPI机制（严重程度：高）

**问题**:
- 使用`java.util.ServiceLoader`机制发现插件
- 插件需要在`META-INF/services/`目录下注册

**推荐解决方案**:
- 使用`libloading`的`dlopen`加载动态库
- 定义插件注册函数：`extern "C" fn register_plugin() -> Box<dyn Plugin>`
- 使用`lazy_static`或`once_cell`维护插件注册表
- **可能需要放弃的功能**: 运行时插件发现（改为编译时链接）

### 6.2 中优先级难点

#### 5. 泛型和类型擦除（严重程度：中）

**问题**:
- Transformation使用递归泛型`R extends ConnectRecord<R>>`
- 大量使用`Map<String, ?>`通配符类型

**推荐解决方案**:
- 使用Rust泛型（monomorphization，类型信息保留）
- 使用`enum`或`Box<dyn Trait>`处理运行时多态

#### 6. 序列化框架差异（严重程度：中）

**问题**:
- 依赖Jackson框架进行JSON序列化
- 实现了Connect Schema到JSON Schema的转换

**推荐解决方案**:
- 使用`serde_json` crate替代Jackson
- 使用`serde::Serialize`、`serde::Deserialize` trait

#### 7. 生命周期和所有权（严重程度：中）

**问题**:
- 使用`Closeable`接口和try-with-resources
- 资源释放可能抛出IOException

**推荐解决方案**:
- 使用`Drop` trait实现RAII
- 使用`scopeguard` crate实现defer模式

### 6.3 低优先级难点

#### 8. 集合类型转换（严重程度：低）

**问题**:
- Java使用`HashMap`、`ArrayList`、`HashSet`
- 并发使用`ConcurrentHashMap`

**推荐解决方案**:
- 直接映射：`HashMap<K,V>` → `std::collections::HashMap<K,V>`
- `ArrayList<T>` → `Vec<T>`
- `ConcurrentHashMap` → `tokio::sync::RwLock<HashMap<K,V>>`

#### 9. 闭包和lambda（严重程度：低）

**问题**:
- 使用`Function<T,R>`、`Supplier<T>`、`Consumer<T>`等函数式接口

**推荐解决方案**:
- 直接使用Rust闭包：`|arg| { ... }`
- 使用`Fn` trait对象替代Function接口

---

## 七、细粒度任务拆解（优化版）

### 阶段0：API接口识别与依赖分析（优先级：最高，新增）

#### 任务0.1：分析connect/api模块的public接口

**目标**: 识别connect/api模块的所有对外API接口

**子任务**:
0.1.1 扫描所有public接口和类
0.1.2 提取核心方法签名
0.1.3 识别对外依赖
0.1.4 识别被依赖者

**验证每个子任务**:
- 扫描完成
- 接口清单完整

**验证整个任务**:
- 生成connect-api的API清单文档
- 包含所有public接口和类

**输出**:
- connect-api的API清单
- 方法签名列表
- 依赖关系

#### 任务0.2：分析connect/runtime模块的public接口

**目标**: 识别connect/runtime模块的所有对外API接口

**子任务**:
0.2.1 扫描所有public接口和类
0.2.2 提取核心方法签名
0.2.3 识别对外依赖
0.2.4 识别被依赖者

**验证每个子任务**:
- 扫描完成
- 接口清单完整

**验证整个任务**:
- 生成connect-runtime的API清单文档
- 包含所有public接口和类

**输出**:
- connect-runtime的API清单
- 方法签名列表
- 依赖关系

#### 任务0.3：分析connect/transforms模块的public接口

**目标**: 识别connect/transforms模块的所有对外API接口

**子任务**:
0.3.1 扫描所有public转换器类
0.3.2 提取配置参数和apply方法签名
0.3.3 识别对外依赖
0.3.4 识别被依赖者

**验证每个子任务**:
- 扫描完成
- 转换器清单完整

**验证整个任务**:
- 生成connect-transforms的API清单文档
- 包含所有public转换器类

**输出**:
- connect-transforms的API清单
- 转换器配置和签名
- 依赖关系

#### 任务0.4：分析connect/mirror和mirror-client的public接口

**目标**: 识别mirror和mirror-client模块的所有对外API接口

**子任务**:
0.4.1 扫描mirror-client的所有public接口和类
0.4.2 扫描mirror的所有public接口和类
0.4.3 提取核心方法签名
0.4.4 识别模块间依赖关系

**验证每个子任务**:
- 扫描完成
- 接口清单完整

**验证整个任务**:
- 生成mirror和mirror-client的API清单文档
- 包含所有public接口和类

**输出**:
- mirror和mirror-client的API清单
- 方法签名列表
- 模块间依赖关系

#### 任务0.5：分析connect/basic-auth-extension的public接口

**目标**: 识别connect/basic-auth-extension模块的所有对外API接口

**子任务**:
0.5.1 扫描所有public接口和类
0.5.2 提取核心方法签名
0.5.3 识别对外依赖

**验证每个子任务**:
- 扫描完成
- 接口清单完整

**验证整个任务**:
- 生成connect-basic-auth-extension的API清单文档
- 包含所有public接口和类

**输出**:
- connect-basic-auth-extension的API清单
- 方法签名列表
- 依赖关系

#### 任务0.6：生成模块依赖关系图

**目标**: 生成所有模块的依赖关系图

**子任务**:
0.6.1 汇总所有模块的对外依赖
0.6.2 汇总所有模块的被依赖者
0.6.3 生成依赖关系矩阵
0.6.4 识别可并行开发的任务组

**验证每个子任务**:
- 依赖关系完整
- 矩阵正确

**验证整个任务**:
- 生成模块依赖关系图
- 标注可并行的任务组

**输出**:
- 模块依赖关系图
- 可并行开发的任务组

#### 任务0.7：生成API契约文档

**目标**: 生成所有模块的API契约文档

**子任务**:
0.7.1 汇总所有模块的API清单
0.7.2 生成API契约文档
0.7.3 验证API契约的完整性

**验证每个子任务**:
- API清单完整
- 契约文档正确

**验证整个任务**:
- 生成完整的API契约文档
- 文档包含所有模块的对外API

**输出**:
- API契约文档（.sisyphus/drafts/api-contract.md）

---

### 阶段1：基础设施与API骨架生成（优先级：最高，优化）

#### 任务1.1：创建所有Cargo Crate项目结构

**目标**: 创建workspace和所有crate的基础结构

**子任务**:
1.1.1 创建workspace根目录和Cargo.toml
1.1.2 创建kafka-clients-trait crate
1.1.3 创建kafka-clients-mock crate
1.1.4 创建connect-api crate
1.1.5 创建connect-transforms crate
1.1.6 创建connect-runtime-core crate
1.1.7 创建connect-runtime-distributed crate
1.1.8 创建connect-mirror-client crate
1.1.9 创建connect-mirror crate
1.1.10 创建connect-basic-auth-extension crate

**验证每个子任务**:
- `cargo check` 成功
- crate结构正确
- 依赖关系正确

**验证整个任务**:
- `cargo build --workspace` 成功
- 所有crate编译通过

#### 任务1.2：实现kafka-clients-trait crate（trait定义）

**目标**: 定义Kafka客户端核心trait接口

**子任务**:
1.2.1 定义Producer trait
   - `send()` - 发送消息
   - `flush()` - 刷新缓冲区
   - `close()` - 关闭producer

1.2.2 定义Consumer trait
   - `poll()` - 拉取消息
   - `assign()` - 分配分区
   - `seek` - 定位到指定offset
   - `commit()` - 提交offset
   - `close()` - 关闭consumer

1.2.3 定义AdminClient trait
   - `create_topics()` - 创建主题
   - `delete_topics()` - 删除主题
   - `list_topics()` - 列出主题
   - `describe_configs()` - 描述配置



1.2.4 定义ConfigDef trait
   - 配置定义接口

1.2.5 定义序列化接口
   - Serializer trait
   - Deserializer trait

**验证每个子任务**:
- `cargo check` 成功
- trait编译通过
- 代码行数检查（~500行）

**验证整个任务**:
- `cargo build --release` 成功
- 所有trait编译通过

**单元测试设计**:
- 创建trait的mock测试
- 验证trait方法签名正确

#### 任务1.3：实现kafka-clients-mock crate（trait实现）

**目标**: 实现基于内存队列的mock实现

**子任务**:
1.3.1 实现MockProducer
   - 内存队列存储消息
   - 支持预制数据

1.3.2 实现MockConsumer
   - 从内存队列消费
   - 支持offset管理

1.3.3 实现MockAdminClient
   - 内存存储主题元数据
   - 支持主题CRUD操作

1.3.4 实现MockConfigDef
   - 配置验证mock

1.3.5 实现序列化mock
   - 简单的序列化/反序列化

**验证每个子任务**:
- `cargo check` 成功
- mock编译通过
- 代码行数检查（~2000行）

**验证整个任务**:
- `cargo build --release` 成功
- 所有mock编译通过

**单元测试设计**:
- 无需测试（内存版mock，保证编译通过即可）

#### 任务1.4：生成connect-api的trait和类型定义（不包含实现）

**目标**: 生成connect-api的trait定义和类型声明

**子任务**:
1.4.1 定义错误类型系统
   - ConnectError枚举
   - RetriableError trait

1.4.2 定义数据模型
   - Schema trait和ConnectSchema
   - Struct和Field
   - ConnectRecord trait
   - SourceRecord
   - SinkRecord
   - SchemaAndValue

1.4.3 定义核心trait
   - Versioned trait
   - Connector trait
   - Task trait
   - SourceConnector trait
   - SourceTask trait
   - SinkConnector trait
   - SinkTask trait
   - Converter trait
   - HeaderConverter trait
   - Transformation trait

1.4.4 定义配置系统
   - ConfigDef
   - ConfigValue
   - Configurable trait

**验证每个子任务**:
- `cargo check` 成功
- trait和类型定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-api的trait和类型定义编译通过

**单元测试设计**:
- 创建trait的mock测试
- 验证trait方法签名正确

#### 任务1.5：生成connect-transforms的trait定义（不包含实现）

**目标**: 生成connect-transforms的trait定义

**子任务**:
1.5.1 定义Transformation trait
   - `apply()` 方法
   - `config()` 方法
   - `close()` 方法

1.5.2 定义所有内置转换器的trait定义
   - Filter
   - InsertHeader
   - DropHeaders
   - RegexRouter
   - TimestampRouter
   - HoistField
   - ValueToKey
   - ReplaceField
   - MaskField
   - SetSchemaMetadata
   - InsertField
   - HeaderFrom
   - ExtractField
   - Flatten
   - Cast
   - TimestampConverter

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-transforms的trait定义编译通过

**单元测试设计**:
- 创建trait的mock测试
- 验证trait方法签名正确

#### 任务1.6：生成connect-runtime-core的trait定义（不包含实现）

**目标**: 生成connect-runtime-core的trait定义

**子任务**:
1.6.1 定义Worker trait
   - `start()` 方法
   - `stop()` 方法
   - `awaitStop()` 方法

1.6.2 定义Herder trait
   - `start()` 方法
   - `stop()` 方法
   - `connect()` 方法
   - `deleteConnector()` 方法

1.6.3 定义StandaloneHerder trait
   - 继承Herder trait

1.6.4 定义DistributedHerder trait
   - 继承Herder trait

1.6.5 定义WorkerConnector trait
   - `start()` 方法
   - `stop()` 方法

1.6.6 定义WorkerTask trait
   - `start()` 方法
   - `stop()` 方法

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-runtime-core的trait定义编译通过

**单元测试设计**:
- 创建trait的mock测试
- 验证trait方法签名正确

#### 任务1.7：生成connect-runtime-distributed的trait定义（不包含实现）

**目标**: 生成connect-runtime-distributed的trait定义

**子任务**:
1.7.1 定义ConnectProtocol trait
   - `sendRequest()` 方法
   - `receiveResponse()` 方法

1.7.2 定义WorkerCoordinator trait
   - `join()` 方法
   - `leave()` 方法

1.7.3 定义EagerAssignor trait
   - `assign()` 方法

1.7.4 定义IncrementalCooperativeAssignor trait
   - `assign()` 方法

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-runtime-distributed的trait定义编译通过

**单元测试设计**:
- 创建trait的mock测试
- 验证trait方法签名正确

#### 任务1.8：生成connect-mirror-client的trait定义（不包含实现）

**目标**: 生成connect-mirror-client的trait定义

**子任务**:
1.8.1 定义ReplicationPolicy trait
   - `topic()` 方法
   - `partition()` 方法

1.8.2 定义MirrorClient trait
   - `connect()` 方法
   - `disconnect()` 方法

1.8.3 定义Heartbeat trait
   - `send()` 方法

1.8.4 定义Checkpoint trait
   - `record()` 方法

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-mirror-client的trait定义编译通过

**单元测试设计**:
- 创建trait的mock测试
- 验证trait方法签名正确

#### 任务1.9：生成connect-mirror的trait定义（不包含实现）

**目标**: 生成connect-mirror的trait定义

**子任务**:
1.9.1 定义MirrorMaker2 trait
   - `start()` 方法
   - `stop()` 方法

1.9.2 定义MirrorSourceTask trait
   - `poll()` 方法

1.9.3 定义MirrorSinkTask trait
   - `put()` 方法

1.9.4 定义MirrorCheckpointTask trait
   - `poll()` 方法

1.9.5 定义MirrorHeartbeatTask trait
   - `poll()` 方法

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-mirror的trait定义编译通过

**单元测试设计**:
- 创建trait的mock测试
- 验证trait方法签名正确

#### 任务1.10：生成connect-basic-auth-extension的trait定义（不包含实现）

**目标**: 生成connect-basic-auth-extension的trait定义

**子任务**:
1.10.1 定义BasicAuthExtension trait
   - `authenticate()` 方法

1.10.2 定义RestAuthenticator trait
   - `authenticate()` 方法

**验证每个子任务**:
- `cargo check` 成功
- trait定义编译通过

**验证整个任务**:
- `cargo build --release` 成功
- connect-basic-auth-extension的trait定义编译通过

**单元测试设计**:
- 创建trait的mock测试
- 验证trait方法签名正确

---

### 阶段2：Connect API层实现（优先级：高，优化 - 可并行）

#### 任务2.1：实现错误类型系统

**子任务**:
2.1.1 定义ConnectError枚举
   - Generic
   - Data
   - AlreadyExists
   - NotFound
   - IllegalWorkerState
   - SchemaBuilder
   - SchemaProjector

2.1.2 定义RetriableError trait
   - `is_retriable()` 方法

2.1.3 使用thiserror实现错误层次

**验证每个子任务**:
- `cargo check` 成功
- 错误类型编译通过

**验证整个任务**:
- `cargo build --release` 成功
- 错误类型编译通过

**单元测试设计**:
- 测试错误类型创建
- 测试RetriableError trait
- 测试错误层次结构

#### 任务2.2：实现数据模型

**子任务**:
2.2.1 实现Schema trait和ConnectSchema
   - 类型枚举（INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES, ARRAY, MAP, STRUCT）
   - Schema trait定义
   - ConnectSchema实现

2.2.2 实现Struct和Field
   - Struct结构体
   - Field结构体
   - 类型化访问方法

2.2.3 实现ConnectRecord trait
   - 泛型trait定义
   - 基础字段访问

2.2.4 实现SourceRecord
   - 继承ConnectRecord
   - sourcePartition和sourceOffset字段

2.2.5 实现SinkRecord
   - 继承ConnectRecord
   - kafkaOffset等字段

2.2.6 实现SchemaAndValue
   - Schema和Value的组合

**验证每个子任务**:
- `cargo check` 成功
- 数据模型编译通过

**验证整个任务**:
- `cargo build --release` 成功
- 数据模型编译通过

**单元测试设计**:
-测试Schema创建和验证
- 测试Struct字段访问
- 测试ConnectRecord字段访问
- 测试SourceRecord和SinkRecord

#### 任务2.3：实现核心trait

**子任务**:
2.3.1 实现Versioned trait
   - `version()` 方法

2.3.2 实现Connector trait
   - `initialize()`
   - `start()`
   - `stop()`
   - `task_class()`
   - `task_configs()`
   - `validate()`
   - `config()`

2.3.3 实现Task trait
   - `version()`
   - `start()`
   - `stop()`

2.3.4 实现SourceConnector trait
   - 继承Connector
   - `exactly_once_support()`
   - `can_define_transaction_boundaries()`
   - `alter_offsets()`

2.3.5 实现SourceTask trait
   - 继承Task
   - `initialize()`
   - `poll()`
   - `commit()`
   - `commit_record()`

2.3.6 实现SinkConnector trait
   - 继承Connector
   - `alter_offsets()`

2.3.7 实现SinkTask trait
   - 继承Task
   - `initialize()`
   - `put()`
   - `flush()`
   - `pre_commit()`
   - `open()`
   - `close()`

2.3.8 实现Converter trait
   - `configure()`
   - `from_connect_data()`
   - `to_connect_data()`
   - `config()`
   - `close()`

2.3.9 实现HeaderConverter trait
   - `to_connect_header()`
   - `from_connect_header()`
   - `config()`

2.3.10 实现Transformation trait
   - 泛型trait
   - `apply()`
   - `config()`
   - `close()()`

**验证每个子任务**:
- `cargo check` 成功
- 所有trait编译通过

**验证整个任务**:
- `cargo build --release` 成功
- 所有trait编译通过

**单元测试设计**:
- 创建trait的mock测试
- 验证trait方法签名正确

#### 任务2.4：实现配置系统

**子任务**:
2.4.1 实现ConfigDef
   - 配置定义结构

2.4.2 实现ConfigValue
   - 配置值结构

2.4.3 实现Configurable trait
   - `configure()` 方法

**验证每个子任务**:
- `cargo check` 成功
- 配置系统编译通过

**验证整个任务**:
- `cargo build --release` 成功
- 配置系统编译通过

**单元测试设计**:
- 测试配置验证
- 测试配置值转换
- 测试Configurable trait

---

### 阶段3：Transforms模块（优先级：高，优化 - 可并行）

#### 任务3.1：实现基础设施（可并行）

**子任务**:
3.1.1 实现SimpleConfig
3.1.2 实现Requirements工具类
3.1.3 实现SchemaUtil工具类
3.1.4 实现RegexValidator
3.1.5 实现NonEmptyListValidator

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 工具类测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 工具类测试通过

**单元测试设计**:
- 测试SimpleConfig
- 测试Requirements验证
- 测试SchemaUtil
- 测试RegexValidator
- 测试NonEmptyListValidator

#### 任务3.2：实现字段路径模块（可并行）

**子任务**:
3.2.1 实现FieldSyntaxVersion枚举
3.2.2 实现SingleFieldPath

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 字段路径测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 字段路径测试通过

**单元测试设计**:
- 测试FieldSyntaxVersion
- 测试SingleFieldPath解析

#### 任务3.3：实现简单转换器（可并行）

**子任务**:
3.3.1 实现Filter
3.3.2 实现InsertHeader
3.3.3 实现DropHeaders
3.3.4 实现RegexRouter
3.3.5 实现TimestampRouter
3.3.6 实现HoistField
3.3.7 实现ValueToKey

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 简单转换器测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 简单转换器测试通过

**单元测试设计**:
- 每个转换器都有对应的测试
- 测试转换逻辑
- 测试配置验证

#### 任务3.4：实现中等复杂度转换器（可并行）

**子任务**:
3.4.1 实现ReplaceField
3.4.2 实现MaskField
3.4.3 实现SetSchemaMetadata
3.4.4 实现InsertField
3.4.5 实现HeaderFrom
3.4.6 实现ExtractField
3.4.7 实现Flatten

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 中等转换器测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 中等转换器测试通过

**单元测试设计**:
- 每个转换器都有对应的测试
- 测试转换逻辑
- 测试边界情况

#### 任务3.5：实现复杂转换器（串行）

**子任务**:
3.5.1 实现Cast（类型转换）
3.5.2 实现TimestampConverter（时间戳转换）

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 复杂转换器测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 复杂转换器测试通过

**单元测试设计**:
- 测试所有类型转换
- 测试时间戳格式转换
- 测试边界情况

#### 任务3.6：实现谓词模块（可并行）

**子任务**:
3.6.1 实现RecordIsTombstone
3.6.2 实现HasHeaderKey
3.6.3 实现TopicNameMatches

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 谓词测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 谓词测试通过

**单元测试设计**:
- 测试谓词逻辑
- 测试谓词组合

---

### 阶段4：Runtime核心（优先级：高，优化 - 可并行）

#### 任务4.1：实现基础配置和状态管理（可并行）

**子任务**:
4.1.1 实现WorkerConfig
4.1.2 实现ConnectorConfig
4.1.3 实现TaskConfig
4.1.4 实现StateTracker
4.1.5 实现AbstractStatus
4.1.6 实现ConnectorStatus
4.1.7 实现TaskStatus

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 配置和状态测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 配置和状态测试通过

**单元测试设计**:
- 测试配置验证
- 测试状态转换
- 测试状态持久化

#### 任务4.2：实现指标系统（可并行）

**子任务**:
4.2.1 实现ConnectMetrics
4.2.2 实现ConnectMetricsRegistry
4.2.3 实现WorkerMetricsGroup

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 指标测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 指标测试通过

**单元测试设计**:
- 测试指标创建
- 测试指标注册
- 测试指标报告

#### 任务4.3：实现插件隔离层（可并行）

**子任务**:
4.3.1 实现PluginClassLoader
4.3.2 实现DelegatingClassLoader
4.3.3 实现PluginScanner
4.3.4 实现ReflectionScanner
4.3.5 实现ServiceLoaderScanner
4.3.6 实现Plugins
4.3.7 实现PluginUtils

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 插件系统测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 插件系统测试通过

**单元测试设计**:
- 测试插件加载
- 测试插件发现
- 测试插件隔离

#### 任务4.4：实现错误处理层（可并行）

**子任务**:
4.4.1 实现ErrorReporter
4.4.2 实现LogReporter
4.4.3 实现ProcessingContext
4.4.4 实现DeadLetterQueueReporter
4.4.5 实现WorkerErrantRecordReporter
4.4.6 实现RetryWithToleranceOperator
4.4.7 实现ErrorHandlingMetrics

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 错误处理测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 错误处理测试通过

**单元测试设计**:
- 测试错误报告
- 测试重试逻辑
- 测试死信队列

#### 任务4.5：实现Worker核心（串行）

**子任务**:
4.5.1 实现WorkerConnector
4.5.2 实现WorkerTask
4.5.3 实现WorkerTaskContext
4.5.4 实现WorkerSinkTask
4.5.5 实现WorkerSinkTaskContext
4.5.6 实现WorkerSourceTask
4.5.7 实现WorkerSourceTaskContext
4.5.8 实现AbstractWorkerSourceTask
4.5.9 实现ExactlyOnceWorkerSourceTask
4.5.10 实现Worker

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Worker核心测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` Worker核心测试通过

**单元测试设计**:
- 测试WorkerConnector
- 测试WorkerTask
- 测试WorkerSourceTask
- 测试WorkerSinkTask
- 测试Worker

#### 任务4.6：实现Herder层（可并行）

**子任务**:
4.6.1 实现Herder接口
4.6.2 实现AbstractHerder
4.6.3 实现StandaloneHerder
4.6.4 实现StandaloneConfig
4.6.5 实现HealthCheckThread

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Herder测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` Herder测试通过

**单元测试设计**:
- 测试AbstractHerder
- 测试StandaloneHerder
- 测试Herder生命周期

#### 任务4.7：实现REST API层（可并行）

**子任务**:
4.7.1 实现REST实体（19个文件）
4.7.2 实现RestServer
4.7.3 实现RestServerConfig
4.7.4 实现RestClient
4.7.5 实现REST资源（6个文件）
4.7.6 实现REST错误处理

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` REST API测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` REST API测试通过

**单元测试设计**:
- 测试REST实体
- 测试RestServer
- 测试REST资源

---

### 阶段5：Runtime分布式（优先级：中，优化 - 可并行）

#### 任务5.1：实现分布式协调基础（可并行）

**子任务**:
5.1.1 实现ConnectProtocol
5.1.2 实现WorkerCoordinator
5.1.3 实现WorkerGroupMember

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 分布式基础测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 分布式基础测试通过

**单元测试设计**:
- 测试ConnectProtocol
- 测试WorkerCoordinator
- 测试组协调

#### 任务5.2：实现分布式分配器（可并行）

**子任务**:
5.2.1 实现EagerAssignor
5.2.2 实现IncrementalCooperativeAssignor
5.2.3 实现ExtendedAssignment

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 分配器测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 分配器测试通过

**单元测试设计**:
- 测试EagerAssignor
- 测试IncrementalCooperativeAssignor
- 测试分配策略

#### 任务5.3：实现DistributedHerder（串行）

**子任务**:
5.3.1 实现DistributedConfig
5.3.2 实现DistributedHerder

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` DistributedHerder测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` DistributedHerder测试通过

**单元测试设计**:
- 测试DistributedHerder
- 测试分布式协调
- 测试故障转移

---

### 阶段6：Mirror客户端（优先级：中，优化 - 可并行）

#### 任务6.1：实现mirror-client模块（可并行）

**子任务**:
6.1.1 实现ReplicationPolicy接口
6.1.2 实现DefaultReplicationPolicy
6.1.3 实现IdentityReplicationPolicy
6.1.4 实现Heartbeat
6.1.5 实现Checkpoint
6.1.6 实现SourceAndTarget
6.1.7 实现MirrorClientConfig
6.1.8 实现RemoteClusterUtils
6.1.9 实现MirrorClient

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` mirror-client测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` mirror-client测试通过

**单元测试设计**:
- 测试ReplicationPolicy
- 测试MirrorClient
- 测试RemoteClusterUtils

---

### 阶段7：MirrorMaker2（优先级：中，优化 - 可并行）

#### 任务7.1：实现基础工具和配置（可并行）

**子任务**:
7.1.1 实现MirrorUtils
7.1.2 实现Scheduler
7.1.3 实现MirrorConnectorConfig
7.1.4 实现TopicFilter
7.1.5 实现GroupFilter
7.1.6 实现ConfigPropertyFilter
7.1.7 实现DefaultTopicFilter
7.1.8 实现DefaultGroupFilter
7.1.9 实现DefaultConfigPropertyFilter

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 工具和配置测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 工具和配置测试通过

**单元测试设计**:
- 测试MirrorUtils
- 测试Scheduler
- 测试各种Filter

#### 任务7.2：实现Offset同步子系统（可并行）

**子任务**:
7.2.1 实现OffsetSync
7.2.2 实现OffsetSyncStore
7.2.3 实现OffsetSyncWriter

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Offset同步测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` Offset同步测试通过

**单元测试设计**:
- 测试OffsetSync
- 测试OffsetSyncStore
- 测试OffsetSyncWriter

#### 任务7.3：实现Checkpoint子系统（可并行）

**子任务**:
7.3.1 实现CheckpointStore
7.3.2 实现MirrorCheckpointConfig
7.3.3 实现MirrorCheckpointTaskConfig
7.3.4 实现MirrorCheckpointMetrics
7.3.5 实现MirrorCheckpointTask
7.3.6 实现MirrorCheckpointConnector

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Checkpoint测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` Checkpoint测试通过

**单元测试设计**:
- 测试CheckpointStore
- 测试MirrorCheckpointTask
- 测试MirrorCheckpointConnector

#### 任务7.4：实现Heartbeat子系统（可并行）

**子任务**:
7.4.1 实现MirrorHeartbeatConfig
7.4.2 实现MirrorHeartbeatTask
7.4.3 实现MirrorHeartbeatConnector

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Heartbeat测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` Heartbeat测试通过

**单元测试设计**:
- 测试MirrorHeartbeatTask
- 测试MirrorHeartbeatConnector

#### 任务7.5：实现Source子系统（串行）

**子任务**:
7.5.1 实现MirrorSourceConfig
7.5.2 实现MirrorSourceTaskConfig
7.5.3 实现MirrorSourceMetrics
7.5.4 实现MirrorSourceTask
7.5.5 实现MirrorSourceConnector

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` Source测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` Source测试通过

**单元测试设计**:
- 测试MirrorSourceTask
- 测试MirrorSourceConnector
- 测试Source复制逻辑

#### 任务7.6：实现格式化和REST（可并行）

**子任务**:
7.6.1 实现CheckpointFormatter
7.6.2 实现HeartbeatFormatter
7.6.3 实现OffsetSyncFormatter
7.6.4 实现MirrorRestServer
7.6.5 实现InternalMirrorResource

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 格式化和REST测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 格式化和REST测试通过

**单元测试设计**:
- 测试各种Formatter
- 测试MirrorRestServer

#### 任务7.7：实现MirrorMaker主程序（串行）

**子任务**:
7.7.1 实现MirrorMakerConfig
7.7.2 实现MirrorHerder
7.7.3 实现MirrorMaker

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` MirrorMaker测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` MirrorMaker测试通过

**单元测试设计**:
- 测试MirrorMakerConfig
- 测试MirrorMaker

---

### 阶段8：Basic Auth Extension（优先级：低，优化 - 可并行）

#### 任务8.1：实现基本认证扩展（可并行）

**子任务**:
8.1.1 实现BasicAuthExtension
8.1.2 实现REST认证

**验证每个子任务**:
- `cargo check` 成功
- `cargo test` 认证测试通过

**验证整个任务**:
- `cargo build --release` 成功
- `cargo test` 认证测试通过

**单元测试设计**:
- 测试BasicAuthExtension
- 测试REST认证

---

### 阶段9：测试迁移（优先级：高，优化 - 可并行）

#### 任务9.1：迁移单元测试（可并行）

**子任务**:
9.1.1 迁移api模块单元测试
9.1.2 迁移transforms模块单元测试
9.1.3 迁移runtime模块单元测试
9.1.4 迁移mirror模块单元测试
9.1.5 迁移mirror-client模块单元测试

**验证每个子任务**:
- `cargo test` 相关测试通过
- 测试覆盖率>80%

**验证整个任务**:
- `cargo test --workspace` 所有单元测试通过
- 测试覆盖率报告

#### 任务9.2：迁移集成测试（可并行）

**子任务**:
9.2.1 实现EmbeddedConnectCluster
9.2.2 迁移runtime集成测试
9.2.3 迁移mirror集成测试

**验证每个子任务**:
- `cargo test --test integration_test` 所有集成测试通过

**验证整个任务**:
- 所有集成测试通过
- 测试覆盖率报告

#### 任务9.3：实现端到端MM2测试（串行）

**子任务**:
9.3.1 实现基础复制测试
9.3.2 实现Offset同步测试
9.3.3 实现Checkpoint测试
9.3.4 实现故障转移测试
9.3.5 实现Exactly-Once测试

**验证每个子任务**:
- 所有端到端测试通过
- 测试覆盖率报告

**验证整个任务**:
- 所有端到端测试通过
- 测试覆盖率报告

---

## 八、二次检查清单

### 8.1 每个任务完成后检查

- [ ] `cargo build --release` 成功
- [ ] `cargo test` 相关测试通过
- [ ] 代码行数与Java版本对比（偏差<20%）
- [ ] 文件数量与Java版本一致
- [ ] 无空实现或TODO代码
- [ ] 无仅包含注释的函数
- [ ] 函数和类名与Java版本1:1对应
- [ ] 代码细节遵从kafka（可靠性细节）

### 8.2 AGENTS.md一致性检查

- [ ] 生成的代码遵守AGENTS.md编码规约
- [ ] 无生成仅包含注释的代码
- [ ] 无生成空函数或默认值
- [ ] 无生成TODO的代码
- [ ] 每个子任务编码后通过`cargo check`或`cargo build`
- [ ] 每个子任务测试运行通过
- [ ] 二次检查确保代码生成无遗漏
- [ ] 代码行数偏差不大
- [ ] 不对已生成的代码大量删减

### 8.3 每个阶段完成后自检

- [ ] 阶段所有任务都已完成
- [ ] 阶段所有测试都通过
- [ ] 阶段代码行数与Java版本对比（偏差<20%）
- [ ] 阶段文件数量与Java版本一致
- [ ] 阶段无空实现或TODO代码
- [ ] 阶段函数和类名与Java版本1:1对应
- [ ] 阶段代码细节遵从kafka（可靠性细节）
- [ ] 阶段AGENTS.md一致性检查通过

---

## 九、成功标准

### 9.1 编译标准
- 所有crate `cargo build --release` 成功
- 无编译警告（或只有可接受的警告）
- 所有依赖正确解析

### 9.2 测试标准
- 所有单元测试通过
- 所有集成测试通过
- 端到端MM2测试通过
- 测试覆盖率>80%

### 9.3 功能标准
- 所有Java功能1:1映射
- 无遗漏的函数或类
- 代码细节遵从kafka（可靠性细节）

### 9.4 质量标准
- 无空实现或TODO代码
- 代码行数与Java版本偏差<20%
- 文件数量与Java版本一致

---

## 十、实施建议

### 10.1 团队准备
- Rust培训（所有权、借用、生命周期）
- 熟悉tokio异步编程
- 熟悉mockall测试框架

### 10.2 开发环境
- Rust 1.70+（最新稳定版）
- VS Code + rust-analyzer
- cargo-nextest（快速测试）
- cargo-tarpaulin（覆盖率）

### 10.3 CI/CD配置
- 每个PR运行`cargo build --release`
- 每个PR运行`cargo test --workspace`
- 每个PR生成覆盖率报告
- 定期运行性能基准测试

### 10.4 代码规范
- 使用`rustfmt`格式化
- 使用`clippy`进行lint检查
- 遵循Rust命名约定

---

## 十一、优化策略总结

### 11.1 核心优化点

| 优化项 | 原计划 | 优化后 | 提升效果 |
|--------|--------|--------|----------|
| **阶段0** | 无 | API接口识别与依赖分析 | 新增 |
| **阶段1并发度** | 2任务 | 10任务 | **5倍** |
| **阶段2-8** | 串行/部分并行 | 完全并行 | **极大提升** |
| **API契约确定** | 分散在各阶段 | 阶段1完成 | **提前锁定** |
| **团队协作** | 低（依赖串行） | 高（完全独立） | **极大提升** |
| **总体效率** | 基准 | +60-70% | **显著提升** |

### 11.2 关键优势

1. **API契约前置**：阶段0识别所有模块的对外API接口
2. **骨架生成**：阶段1生成所有crate的trait定义和类型声明
3. **完全并行**：阶段2-8的模块可以同时开发
4. **独立验证**：每个模块可以独立编译和测试
5. **灵活调度**：可以根据团队规模动态调整并行度

### 11.3 实施建议

1. **阶段0必须完成**：确保API接口识别完整
2. **阶段1必须完成**：确保所有crate骨架生成成功
3. **阶段2-8可并行**：根据团队规模决定并行度
4. **每个阶段独立验证**：每个阶段完成后编译和测试
5. **灵活调整**：可以根据实际情况调整并行度

---

## 十二、下一步行动

1. **审阅本方案**: 确认优化策略和任务拆解
2. **开始阶段0任务0.1**: 分析connect/api模块的public接口
3. **逐步推进**: 按任务顺序完成迁移
4. **持续验证**: 每个任务完成后编译和测试

---

**附录**:
- 附录A: Java到Rust类型映射表
- 附录B: 核心trait设计草案
- 附录C: 测试迁移检查清单
- 附录D: 性能基准测试计划
