# connect-mirror 模块迁移执行计划

## 计划信息

**计划名称**：connect-mirror 模块完整迁移
**生成时间**：2026-04-12
**计划人员**：Prometheus（战略规划顾问）
**目标目录**：connect-rust-new/connect-mirror

---

## 执行摘要

本计划旨在将 Kafka Connect MirrorMaker2 模块从 Java 完整迁移到 Rust，确保：
- **完整性**：代码结构、函数数量与 Java 版本 1:1 对应
- **正确性**：代码编译通过，核心功能有测试覆盖且测试通过
- **无空实现**：所有函数都有实际实现逻辑，不允许 TODO 或空实现

---

## 一、完整性度量标准

### 1.1 代码量对比

| 指标 | Java 版本 | Rust 版本 | 目标 | 状态 |
|--------|----------|-----------|--------|--------|
| 文件数量 | 34 个 | 14 个 | ≈ | ✅ 符合预期 |
| 类/接口数量 | 26 个 | 58 个 struct/trait/enum | ≈ | ✅ Rust 使用更细粒度 |
| 代码行数 | 待统计 | 待统计 | ≈ | ⏠️ 需要统计 |

### 1.2 函数数量对比

| 模块 | Java 方法数 | Rust 方法数 | 目标 | 状态 |
|--------|-----------|-----------|--------|--------|
| 核心连接器 | ~150 个 | ~100 个 | ≈ | ⚠️ 需要统计和实现 |
| 核心任务 | ~200 个 | ~80 个 | ≈ | ⚠️ 需要统计和实现 |
| 辅助模块 | ~300 个 | ~50 个 | ≈ | ⚠️ 需要统计和实现 |

### 1.3 空实现检查

- ✅ **无 TODO 标记**：不允许代码中出现 `TODO`、`FIXME`、`XXX` 注释
- ✅ **无空实现**：所有函数必须有实际实现逻辑
- ✅ **无 unimplemented!()**：不允许使用 `unimplemented!()` 宏
- ✅ **无默认返回**：不允许函数只返回 `Ok(())` 或默认值

---

## 二、正确性度量标准

### 2.1 编译验证

- ✅ **cargo check 通过**：所有代码必须通过编译检查
- ✅ **cargo build 通过**：所有代码必须能够成功构建
- ✅ **无编译警告**：消除所有编译警告

### 2.2 测试覆盖

- ✅ **单元测试**：每个核心类必须有对应的单元测试
- ✅ **测试覆盖率**：测试覆盖率目标 ≥ 80%
- ✅ **集成测试**：关键功能必须有集成测试
- ✅ **Mock 支持**：测试必须能够使用 Mock 实现隔离依赖

### 2.3 功能验证

- ✅ **OffsetSyncStore**：偏移翻译逻辑必须正确
- ✅ **CheckpointStore**：异步读取逻辑必须正确
- ✅ **MirrorCheckpointTask**：检查点发送逻辑必须正确
- ✅ **Scheduler**：定时任务调度必须正常工作

---

## 三、核心模块映射

### 3.1 已实现模块（✓）

| Java 类 | Rust 模块 | 实现状态 | 说明 |
|---------|-----------|----------|--------|
| MirrorConnectorConfig | config.rs | ✅ 完整 | 基础配置系统 |
| MirrorSourceConfig | config.rs | ✅ 完整 | 源连接器配置 |
| MirrorCheckpointConfig | config.rs | ✅ 完整 | 检查点连接器配置 |
| MirrorHeartbeatConfig | config.rs | ✅ 完整 | 心跳连接器配置 |
| MirrorSourceTaskConfig | config.rs | ✅ 完整 | 源任务配置 |
| MirrorUtils | utils.rs | ✅ 完整 | 工具函数 |
| TopicFilter | filter.rs | ✅ 完整 | 过滤器接口 |
| GroupFilter | filter.rs | ✅ 完整 | 消费者组过滤接口 |
| ConfigPropertyFilter | filter.rs | ✅ 完整 | 配置属性过滤接口 |
| DefaultTopicFilter | filter.rs | ✅ 完整 | 默认主题过滤器 |
| DefaultGroupFilter | filter.rs | ✅ 完整 | 默认消费者组过滤器 |
| DefaultConfigPropertyFilter | filter.rs | ✅ 完整 | 默认配置属性过滤器 |
| SourceConnector | connector.rs | ✅ 框架完整 | 连接器接口 |
| SourceTask | task.rs | ✅ 框架完整 | 任务接口 |
| MirrorSourceConnectorImpl | source_connector_impl.rs | ⚠️ 部分实现 | 大量空实现 |
| MirrorSourceTaskImpl | source_task_impl.rs | ⚠️ 部分实现 | 大量空实现 |
| MirrorCheckpointConnectorImpl | checkpoint_connector_impl.rs | ⚠️ 部分实现 | 部分实现 |
| MirrorCheckpointTaskImpl | checkpoint_task_impl.rs | ⚠️ 部分实现 | 部分实现 |
| MirrorHeartbeatConnectorImpl | heartbeat_connector_impl.rs | ⚠️ 部分实现 | 部分实现 |
| MirrorHeartbeatTaskImpl | heartbeat_task_impl.rs | ⚠️ 部分实现 | 部分实现 |

### 3.2 缺失模块（✗）

| Java 类 | Rust 模块 | 优先级 | 复杂度 | 影响 | 说明 |
|---------|-----------|----------|--------|--------|--------|
| Scheduler | 未实现 | 🔴 最高 | 中等 | 定时任务调度器（118 行） |
| OffsetSyncWriter | 未实现 | 🔴 最高 | 高 | 偏移同步写入器（207 行） |
| OffsetSyncStore | 未实现 | 🔴 最高 | 极高 | 偏移同步存储（340+ 行） |
| CheckpointStore | 未实现 | 🔴 最高 | 高 | 检查点存储（203+ 行） |
| MirrorSourceMetrics | 未实现 | 🔴 高 | 中等 | 源连接器指标（174+ 行） |
| MirrorCheckpointMetrics | 未实现 | 🔴 高 | 低等 | 检查点指标（108+ 行） |
| MirrorCheckpointTask | 未实现 | 🔴 高 | 中等 | 检查点任务完整实现（406+ 行） |
| MirrorHeartbeatTask | 未实现 | 🔴 中 | 低等 | 心跳任务完整实现（31 行） |
| SourceAndTarget | 未实现 | 🔴 中 | 低 | 源和目标集群信息 |
| ReplicationPolicy | connect-mirror-client-trait | 🔴 中 | 复制策略接口 |
| Heartbeat | 未实现 | 🔴 中 | 低 | 心跳数据结构 |
| Checkpoint | 未实现 | 🔴 中 | 低 | 检查点数据结构 |
| OffsetSync | 未实现 | 🔴 中 | 低 | 偏移同步数据结构 |
| MessageFormatter | 未实现 | 🔴 中 | 低 | 消息格式化接口 |

---

## 四、外部依赖分析

### 4.1 Kafka Connect 内部依赖（✓ 已通过 connect-api）

- `SourceConnector`, `SourceTask` - 连接器和任务接口
- `SourceRecord`, `RecordMetadata` - 记录和元数据
- `ConfigDef`, `Config` - 配置管理
- `ConnectorContext` - 连接器上下文

### 4.2 Kafka clients 依赖（⚠️ 需要通过 kafka-clients-trait 提供）

| 依赖 | 使用场景 | 需要实现的 trait | 优先级 |
|------|----------|----------|--------|
| Admin | 主题管理、ACL 管理、消费者组管理 | 高 | 🔴 最高 |
| KafkaConsumer | 消费记录、偏移管理 | 高 | 🔴 最高 |
| KafkaProducer | 发送记录、元数据 | 高 | 🔴 最高 |
| KafkaBasedLog | 基于日志的读取 | 高 | 🔴 最高 |
| TopicAdmin | 主题创建和管理 | 高 | 🔴 最高 |

### 4.3 Kafka common 依赖（可能需要 Mock）

- `org.apache.kafka.common.config.*` - 配置相关
- `org.apache.kafka.common.metrics.*` - 指标相关
- `org.apache.kafka.common.errors.*` - 错误处理
- `org.apache.kafka.common.utils.*` - 工具类

### 4.4 Java 标准库依赖（✓ 无需 Mock）

- `java.util.*` - 集合、Map、List、Stream
- `java.time.Duration` - 时间处理
- `java.util.concurrent` - 并发原语
- `java.util.function` - 函数式接口
- `org.slf4j` - 日志框架

---

## 五、语言难点解决方案

### 5.1 并发和异步

**方案**：使用 `tokio` 实现定时任务和异步操作

**关键点**：
- 使用 `tokio::time::interval` 实现定时任务
- 使用 `tokio::sync::oneshot` 封装异步操作
- 使用 `tokio::spawn` 创建后台任务
- 使用 `Arc` 和 `Mutex` 实现共享可变状态

**示例代码**：
```rust
use tokio::time::{interval, Duration};
use tokio::sync::oneshot;
use std::sync::Arc;

// 定时任务
let mut interval = interval(Duration::from_secs(1), move || {
    execute_task();
});
interval.abort();

// 异步操作
let (tx, rx) = oneshot::channel();
tx.send(async {
    result
});
let result = rx.recv().await?;
```

### 5.2 反射和动态加载

**方案**：使用 trait object 和工厂模式替代运行时反射

**关键点**：
- 使用 `Box<dyn Trait + Send>` 实现动态分发
- 使用工厂 trait 创建具体实例
- 使用编译期宏或构建时注册机制

**示例代码**：
```rust
// 定义 trait
pub trait TopicFilter {
    fn should_replicate_topic(&self, topic: &str) -> bool;
}

// 使用 trait object
let topic_filter: Box<dyn TopicFilter + Send> = 
    Box::new(DefaultTopicFilter::new());
```

### 5.3 异常处理

**方案**：使用 `Result<T, E>` 和 `anyhow::Error` 统一错误处理

**关键点**：
- 使用 `anyhow::Context` 添加上下文信息
- 自定义错误类型实现 `std::error::Error`
- 使用 `anyhow::bail!` 快速返回错误

**示例代码**：
```rust
use anyhow::{Context, Result, bail};

fn process_data(data: &str) -> Result<()> {
    let parsed = parse(data)
        .with_context(|| format!("Failed to parse data: {}", data))?;
    Ok(())
}

if invalid_condition {
    bail!("Invalid configuration: {}", config);
}
```

### 5.4 集合和流

**方案**：使用 `std::collections` 和迭代器

**关键点**：
- 使用 `.iter().filter().map().collect()` 替代 Stream API
- 使用 `rayon::par_iter()` 实现并行处理

**示例代码**：
```rust
let filtered: Vec<_> = list
    .iter()
    .filter(|x| condition(x))
    .map(|x| transform(x))
    .collect::<Vec<_>>();
```

### 5.5 CompletableFuture 封装方案（新增）

**方案**：使用 `tokio::sync::oneshot` 封装 CompletableFuture

**实现位置**：建议在 `connect-runtime` 或独立模块中提供

**优势**：
1. API 与 Java CompletableFuture 高度一致，减少学习成本
2. 周边模块（connect-api, connect-runtime）可以直接使用，无需改动
3. 类型安全，编译期检查
4. 支持链式调用、异常处理等高级特性

**示例代码**：
```rust
use tokio::sync::oneshot;
use std::sync::Arc;

pub struct CompletableFuture<T> {
    sender: tokio::sync::oneshot::Sender<T>,
    receiver: tokio::sync::oneshot::Receiver<T>,
}

impl<T: Send + 'static> CompletableFuture<T> {
    pub fn new() -> Self {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        Self { sender, receiver }
    }

    pub fn complete(&self, value: T) {
        let _ = self.sender.send(value);
    }

    pub fn then_apply<F, U>(&self, f: F) -> CompletableFuture<U>
    where
        F: FnOnce(T) -> U + Send + 'static,
        U: Send + 'static,
    {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        
        tokio::spawn(async move {
            if let Ok(value) = self.receiver.await {
                let result = f(value);
                let _ = sender.send(result);
            }
        });
        
        CompletableFuture { sender, receiver }
    }
}
```

---

## 六、外部依赖 Mock 方案

### 6.1 Kafka clients 依赖 Mock

**实现位置**：`kafka-clients-trait`

**Mock 实现**：

1. **Admin trait Mock**
```rust
pub struct MockAdmin {
    // 支持的主题管理操作
}

impl Admin for MockAdmin {
    // 实现所有 Admin trait 方法
    // 返回测试友好的默认值或可配置的结果
}
```

2. **KafkaConsumer trait Mock**
```rust
pub struct MockKafkaConsumer {
    // 支持消费者操作
    // - assign, poll, seek, commit
    // - 支持注入测试数据
}

impl KafkaConsumer for MockKafkaConsumer {
    // 实现所有 KafkaConsumer trait 方法
    // 支持测试场景
}
```

3. **KafkaProducer trait Mock**
```rust
pub struct MockKafkaProducer {
    // 支持生产者操作
    // - send, flush
    // - 支持验证发送的记录
}

impl KafkaProducer for MockKafkaProducer {
    // 实现所有 KafkaProducer trait 方法
    // 支持测试场景
}
```

4. **KafkaBasedLog trait Mock**
```rust
pub struct MockKafkaBasedLog {
    // 支持基于日志的读取
    // - start, stop, poll
}

impl KafkaBasedLog for MockKafkaBasedLog {
    // 实现所有 KafkaBasedLog trait 方法
    // 用于测试 CheckpointStore 和 OffsetSyncStore
}
```

5. **TopicAdmin trait Mock**
```rust
pub struct MockTopicAdmin {
    // 支持主题管理
    // - createTopics, deleteTopics
}

impl TopicAdmin for MockTopicAdmin {
    // 实现所有 TopicAdmin trait 方法
    // 用于测试 CheckpointStore
}
```

### 6.2 connect-runtime 依赖 Mock

**实现位置**：`connect-runtime`

**Mock 实现**：

1. **Metrics trait Mock**
```rust
pub trait Metrics {
    fn sensor(&self, name: &str);
    fn metric(&self, name: &str, tags: &[(&str, &str)]);
}

pub struct MockMetrics {
    // 简化的指标收集
    // 支持测试场景
}
```

2. **CompletableFuture 封装**
```rust
// 在 connect-runtime 或独立模块中提供
use tokio::sync::oneshot;
use std::sync::Arc;

pub struct CompletableFuture<T> {
    sender: tokio::sync::oneshot::Sender<T>,
    receiver: tokio::sync::oneshot::Receiver<T>,
}

impl<T: Send + 'static> CompletableFuture<T> {
    pub fn new() -> Self {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        Self { sender, receiver }
    }

    pub fn complete(&self, value: T) {
        let _ = self.sender.send(value);
    }

    pub fn then_apply<F, U>(&self, f: F) -> CompletableFuture<U>
    where
        F: FnOnce(T) -> U + Send + 'static,
        U: Send + 'static,
    {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        
        tokio::spawn(async move {
            if let Ok(value) = self.receiver.await {
                let result = f(value);
                let _ = sender.send(result);
            }
        });
        
        CompletableFuture { sender, receiver }
    }
}
```

---

## 七、迁移优先级

### 7.1 优先级 1：核心基础设施（必须首先完成）

**目标**：完成所有连接器和任务依赖的核心基础设施

**任务列表**：

1. **实现 Scheduler**（优先级：最高）
   - **文件**：`src/scheduler.rs`
   - **复杂度**：中等
   - **影响**：所有连接器和任务依赖
   - **关键功能**：
     - 定时任务调度
     - 重复任务执行
     - 超时管理
   - **建议**：使用 `tokio::time::interval` 实现定时任务

2. **实现 OffsetSyncWriter**（优先级：最高）
   - **文件**：`src/offset_sync_writer.rs`
   - **复杂度**：高
   - **影响**：`MirrorSourceTask` 的正确性
   - **依赖**：`KafkaProducer` trait
   - **关键方法**：
     - `maybeQueueOffsetSyncs()` - 队列偏移同步
     - `promoteDelayedOffsetSyncs()` - 提升延迟的同步
     - `firePendingOffsetSyncs()` - 发送待处理的同步

3. **实现 OffsetSyncStore**（优先级：最高）
   - **文件**：`src/offset_sync_store.rs`
   - **复杂度**：极高
   - **影响**：`MirrorCheckpointTask` 的正确性
   - **依赖**：`KafkaBasedLog`, `TopicAdmin`, `KafkaConsumer`
   - **关键方法**：
     - `translateDownstream()` - 偏移翻译（核心逻辑）
     - `handleRecord()` - 处理记录回调
     - `start()` - 初始化存储

4. **实现 CheckpointStore**（优先级：最高）
   - **文件**：`src/checkpoint_store.rs`
   - **复杂度**：高
   - **影响**：`MirrorCheckpointTask` 的正确性
   - **依赖**：`KafkaBasedLog`, `TopicAdmin`, `KafkaConsumer`
   - **关键方法**：
     - `start()` - 初始化并读取检查点
     - `update()` - 更新检查点
     - `get()` - 获取检查点
     - `computeConvertedUpstreamOffset()` - 计算转换的上游偏移

### 7.2 优先级 2：指标和辅助功能

**目标**：完成指标系统和辅助数据结构

**任务列表**：

5. **实现 MirrorSourceMetrics**（优先级：高）
   - **文件**：`src/mirror_source_metrics.rs`
   - **复杂度**：中等
   - **影响**：`MirrorSourceTask` 依赖
   - **依赖**：`Metrics` trait
   - **关键指标**：
     - `record-count`, `record-rate`, `record-age-ms`
     - `byte-count`, `byte-rate`
     - `replication-latency-ms`（含 min/max/avg）

6. **实现 MirrorCheckpointMetrics**（优先级：高）
   - **文件**：`src/mirror_checkpoint_metrics.rs`
   - **复杂度**：低
   - **影响**：`MirrorCheckpointTask` 依赖
   - **依赖**：`Metrics` trait
   - **关键指标**：
     - `checkpoint-latency-ms`（含 min/max/avg）

7. **实现 MirrorCheckpointTask**（优先级：高）
   - **文件**：`src/mirror_checkpoint_task.rs`
   - **复杂度**：中
   - **影响**：所有连接器和任务依赖
   - **依赖**：`Admin`, `KafkaProducer`, `OffsetSyncStore`, `CheckpointStore`
   - **关键方法**：
     - `start()` - 初始化
     - `poll()` - 生成检查点记录
     - `commit()` - 提交检查点
     - `commitRecord()` - 处理记录元数据

8. **实现 MirrorHeartbeatTask**（优先级：中）
   - **文件**：`src/mirror_heartbeat_task.rs`
   - **复杂度**：低
   - **影响**：所有连接器和任务依赖
   - **依赖**：`Admin`, `KafkaProducer`
   - **关键方法**：
     - `start()` - 初始化
     - `poll()` - 生成心跳记录
     - `commit()` - 提交

### 7.3 优先级 3：数据结构和工具类

**目标**：完成数据结构和工具类

**任务列表**：

9. **实现 SourceAndTarget**（优先级：中）
   - **文件**：`src/source_and_target.rs`
   - **复杂度**：低
   - **依赖**：无
   - **说明**：源和目标集群信息结构

10. **实现 Heartbeat**（优先级：中）
   - **文件**：`src/heartbeat.rs`
   - **复杂度**：低
   - **依赖**：无
   - **说明**：心跳数据结构

11. **实现 Checkpoint**（优先级：中）
   - **文件**：`src/checkpoint.rs`
   - **复杂度**：低
   - **依赖**：无
   - **说明**：检查点数据结构

12. **实现 OffsetSync**（优先级：中）
   - **文件**：`src/offset_sync.rs`
   - **复杂度**：低
   - **依赖**：无
   - **说明**：偏移同步数据结构

13. **实现 MessageFormatter 及其实现**（优先级：低）
   - **文件**：`src/message_formatter.rs`
   - **复杂度**：低
   - **依赖**：无
   - **说明**：消息格式化接口

14. **完善 ReplicationPolicy**（优先级：中）
   - **文件**：`connect-mirror-client-trait/src/replication_policy.rs`
   - **复杂度**：中
   - **依赖**：无
   - **说明**：确保所有复制策略方法都已实现

### 7.4 优先级 4：测试和依赖 Mock

**目标**：创建完整的单元测试和外部依赖 Mock

**任务列表**：

15. **创建完整的单元测试**（优先级：中）
   - **文件**：`tests/scheduler_test.rs`
   - **文件**：`tests/offset_sync_writer_test.rs`
   - **文件**：`tests/offset_sync_store_test.rs`
   - **文件**：`tests/checkpoint_store_test.rs`
   - **文件**：`tests/mirror_checkpoint_task_test.rs`
   - **目标**：测试覆盖率 ≥ 80%
   - **策略**：使用 Mock 实现隔离依赖

16. **实现外部依赖 Mock**（优先级：中）
   - **文件**：`kafka-clients-trait/src/mock_admin.rs`
   - **文件**：`kafka-clients-trait/src/mock_kafka_consumer.rs`
   - **文件**：`kafka-clients-trait/src/mock_kafka_producer.rs`
   - **文件**：`kafka-clients-trait/src/mock_kafka_based_log.rs`
   - **文件**：`kafka-clients-trait/src/mock_topic_admin.rs`
   - **文件**：`connect-runtime/src/mock_metrics.rs`
   - **文件**：`connect-runtime/src/completable_future.rs`
   - **目标**：确保 Mock 支持所有使用场景

---

## 八、风险评估

### 8.1 高风险项

1. ⚠️ **OffsetSyncStore 的复杂偏移翻译逻辑**（340+ 行 Java 代码）
   - **风险**：包含复杂的数学不变式和状态管理
   - **建议**：仔细翻译，确保不变式正确性
   - **测试策略**：创建专门的偏移翻译测试

2. ⚠️ **CheckpointStore 的异步读取逻辑**（203+ 行 Java 代码）
   - **风险**：依赖 KafkaBasedLog 的异步行为
   - **建议**：确保正确处理异步回调
   - **测试策略**：模拟异步场景

3. ⚠️ **测试覆盖不足**
   - **风险**：核心功能缺少测试
   - **建议**：优先补充单元测试

### 8.2 中风险项

1. ⚠️ **外部依赖 Mock 不完整**
   - **风险**：可能缺少某些 trait 方法
   - **建议**：确保 Mock 支持所有使用场景

2. ⚠️ **Metrics 依赖处理**
   - **风险**：Metrics API 比较复杂
   - **建议**：创建简化的 Metrics trait 用于测试

---

## 九、迁移策略

### 9.1 分阶段迁移策略

**第一阶段**：核心基础设施（2-3 周）
- 目标：完成所有连接器和任务依赖的核心基础设施
- 任务：
  1. 实现 Scheduler
  2. 实现 OffsetSyncWriter
  3. 实现 OffsetSyncStore
  4. 实现 CheckpointStore
- 验证标准：
  - 所有核心类完整实现
  - 代码编译通过
  - 单元测试通过

**第二阶段**：指标系统（1-2 周）
- 目标：完成指标系统和辅助数据结构
- 任务：
  5. 实现 MirrorSourceMetrics
  6. 实现 MirrorCheckpointMetrics
  7. 实现 MirrorCheckpointTask
  8. 实现 MirrorHeartbeatTask
- 验证标准：
  - 所有指标类完整实现
  - 代码编译通过
  - 单元测试通过

**第三阶段**：数据结构和工具类（3-5 天）
- 目标：完成数据结构和工具类
- 任务：
  9. 实现 SourceAndTarget
  10. 实现 Heartbeat
  11. 实现 Checkpoint
  12. 实现 OffsetSync
  13. 实现 MessageFormatter
  14. 完善 ReplicationPolicy
- 验证标准：
  - 所有数据结构完整实现
  - 代码编译通过
  - 单元测试通过

**第四阶段**：测试和依赖 Mock（1-2 周）
- 目标：创建完整的单元测试和外部依赖 Mock
- 任务：
  15. 创建完整的单元测试
  16. 实现外部依赖 Mock
- 验证标准：
  - 测试覆盖率 ≥ 80%
  - 所有 Mock 实现完整

### 9.2 小步迭代验证策略

**每完成一个核心类**：
- 立即运行 `cargo check` 验证编译状态
- 立即运行 `cargo test` 验证测试通过
- 发现问题立即修复，避免累积

**质量保证**：
- 不允许 "In a real implementation" 注释
- 不允许 `todo!()` 或 `unimplemented!()` 宏
- 所有函数必须有实际的实现逻辑

---

## 十、成功标准

### 10.1 完成标准

- ✅ 所有核心类完整实现
- ✅ 所有函数都有实际实现逻辑
- ✅ 代码编译通过
- ✅ 核心功能有测试覆盖
- ✅ 测试通过
- ✅ 无 TODO 或空实现

### 10.2 验证清单

**编译验证**：
- [ ] 运行 `cargo check` 验证编译状态
- [ ] 运行 `cargo build` 验证构建成功
- [ ] 消除所有编译警告

**测试验证**：
- [ ] 运行 `cargo test` 验证所有测试通过
- [ ] 确认测试覆盖率 ≥ 80%

**功能验证**：
- [ ] OffsetSyncStore 偏移翻译逻辑正确
- [ ] CheckpointStore 异步读取逻辑正确
- [ ] MirrorCheckpointTask 检查点发送逻辑正确
- [ ] Scheduler 定时任务调度正常工作

---

## 十一、执行步骤

### 11.1 立即行动项

1. [ ] **补充 kafka-clients-trait 中缺失的 Admin、KafkaConsumer、KafkaProducer 方法**
2. [ ] **补充 connect-runtime 中缺失的 Metrics、KafkaBasedLog、TopicAdmin trait**
3. [ ] **创建 Scheduler 的完整实现**
4. [ ] **创建 OffsetSyncWriter 的完整实现**
5. [ ] **创建 OffsetSyncStore 的完整实现**
6. [ ] **创建 CheckpointStore 的完整实现**
7. [ ] **创建 MirrorSourceMetrics 的完整实现**
8. [ ] **创建 MirrorCheckpointMetrics 的完整实现**
9. [ ] **创建 MirrorCheckpointTask 的完整实现**
10. [ ] **创建 MirrorHeartbeatTask 的完整实现**
11. [ ] **创建 SourceAndTarget 数据结构**
12. [ ] **创建 Heartbeat、Checkpoint、OffsetSync 数据结构**
13. [ ] **创建 MessageFormatter 及其实现**
14. [ ] **完善 ReplicationPolicy**
15. [ ] **创建完整的单元测试**
16. [ ] **实现外部依赖 Mock**
17. [ ] **实现 CompletableFuture 封装**
18. [ ] 运行 `cargo check` 验证编译状态
19. [ ] 运行 `cargo test` 验证测试通过
20. [ ] 对比 Java 和 Rust 代码的函数数量
21. [ ] 确认所有核心功能都有测试覆盖

### 11.2 验证行动项

1. [ ] 运行 `cargo check` 验证编译状态
2. [ ] 运行 `cargo build` 验证构建成功
3. [ ] 运行 `cargo test` 验证测试通过
4. [ ] 检查测试覆盖率 ≥ 80%
5. [ ] 确认无 TODO 或空实现
6. [ ] 确认所有函数都有实际实现逻辑

---

## 十二、预期时间表

| 阶段 | 任务 | 预估时间 | 累计时间 |
|------|------|-----------|--------|
| 第一阶段 | 核心基础设施（4 个模块） | 2-3 周 | 2-3 周 |
| 第二阶段 | 指标系统（4 个模块） | 1-2 周 | 3-5 周 |
| 第三阶段 | 数据结构和工具类（6 个模块） | 3-5 天 | 6-8 周 |
| 第四阶段 | 测试和依赖 Mock（2 个模块） | 1-2 周 | 8-10 周 |
| **总计** | 16 个核心模块 + 测试和 Mock | **8-13 周** | 8-13 周 |

---

## 十三、开始执行

**当前状态**：计划已生成，等待执行

**下一步**：
1. 审阅本计划，确认优先级和迁移策略
2. 确认外部依赖 Mock 方案
3. 确认语言难点解决方案
4. 开始执行第一阶段：核心基础设施

**执行命令**：
```bash
# 开始执行
cd connect-rust-new/connect-mirror
cargo check
cargo test
```

---

**计划状态**：✅ 已完成
**生成时间**：2026-04-12
**计划人员**：Prometheus（战略规划顾问）
