# Kafka Connect Rust 重写提案

## 概述

将 Apache Kafka Connect Connect 组件从 Java 完整迁移到 Rust，保持 100% 的功能对等性和可靠性细节。

### 迁移范围

```
┌─────────────────────────────────────────────────────┐
│              迁移范围统计                                  │
├─────────────────────────────────────────────────────┤
│                                                             │
│  主代码                                                     │
│  ├── connect/api:        79 文件,  8,278 行           │
│  ├── connect/runtime:     191 文件, 40,426 行           │
│  ├── connect/mirror:       34 文件,  6,059 行           │
│  ├── connect/mirror-client: 10 文件,  ~1,500 行         │
│  ├── connect/json:        ~10 文件                        │
│  ├── connect/transforms:   26 文件,  ~2,500 行          │
│  ├── connect/file:        4 文件,   ~800 行            │
│  ├── connect/test-plugins: 11 文件                        │
│  ├── connect/basic-auth-extension: 5 文件, ~200 行            │
│  └── 总计:                 ~370 文件, ~60,200 行        │
│                                                             │
│  测试代码                                                   │
│  ├── connect/api:        22 测试文件                       │
│ ├── connect/runtime:     119 测试文件                      │
│ ├── connect/mirror:      28 测试文件                      │
│ ├── connect/mirror-client: 3 测试文件                       │
│ ├── connect/json:        ~2 测试文件                      │
│ ├── connect/transforms:   21 测试文件                     │
│ ├── connect/file:        6 测试文件                      │
│  ├── connect/basic-auth-extension: 2 测试文件                    │
│  └── 总计:                 203 测试文件, ~81,000 行    │
│                                                             │
│  公共 API                                                   │
│  ├── API 模块:            68 个公共类型, 443 个方法     │
│  ├── Runtime 模块:        240 个公共类型, 2,351 个方法  │
│  └── 总计:                308 个公共类型, 2,794 个方法  │
│                                                             │
│  代码复杂度指标                                             │
│  ├── 并发相关代码:        443 处                         │
│  ├── 异常处理代码:        1,686 处                       │
│  ├── 空值检查代码:        616 处                         │
│  ├── 重试和超时代码:      327 处                         │
│  ├── Builder 模式类:       7+ 个                          │
│  ├── 状态机枚举:          7+ 个                          │
│  └── 回调/监听器接口:     5+ 个                          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 技术方案

### 1. 插件隔离系统

**方案：编译时注册 + Trait 对象**

使用 `lazy_static` + `HashMap` 实现插件注册表，零运行时开销，类型安全。

```rust
// 插件注册表
lazy_static! {
    static ref SOURCE_CONNECTOR_REGISTRY: RwLock<HashMap<String, Box<dyn Fn() -> Box<dyn SourceConnector>>>> = 
        RwLock::new(HashMap::new());
    static ref SINK_CONNECTOR_REGISTRY: RwLock<HashMap<String, Box<dyn Fn() -> Box<dyn SinkConnector>>>> = 
        RwLock::new(HashMap::new());
}

// 插件注册宏
macro_rules! register_source_connector {
    ($name:expr, $connector_type:ty) => {
        #[ctor]
        fn register_connector() {
            let mut registry = SOURCE_CONNECTOR_REGISTRY.write().unwrap();
            registry.insert($name.to_string(), Box::new(|| Box::new($connector_type::new()) as Box<dyn SourceConnector>));
        }
    }
}

// 运行时创建插件
fn create_connector(name: &str) -> Result<Box<dyn SourceConnector>> {
    let registry = SOURCE_CONNECTOR_REGISTRY.read().unwrap();
    registry.get(name)
        .ok_or_else(|| Error::ConnectorNotFound(name.to_string()))?
        .call()
}
```

### 2. 反射和动态实例化

**方案：插件注册表（lazy_static + HashMap）**

插件使用 `register_plugin!` 宏编译时注册，配置驱动创建实例。

### 3. 异步运行时

**方案：tokio**

所有 I/O 操作使用 tokio 异步运行时。

### 4. REST API

**方案：actix-web**

使用 actix-web 实现 REST API，支持所有端点。

### 5. JSON 处理

**方案：serde_json**

所有 JSON 序列化/反序列化使用 serde_json。

### 6. Kafka 客户端 Mock

**方案：独立 crate（kafka-clients-mock）**

实现内存 Mock，支持预置数据和查询，完整实现 Producer/Consumer/Admin 接口。

## 迁移策略

### 1:1 翻译原则

1. **命名转换**：Java 驼峰命名 → Rust 蛇蛇命名
   - `Connector` → `connector`
   - `taskConfigs` → `task_configs`
   - `MAX_TASKS_CONFIG` → `max_tasks_config`

2. **类型映射**：
   - Java `interface` → Rust `trait`
   - Java `abstract class` → Rust `trait` + `struct` 实现
   - Java `enum` → Rust `enum`
   - Java 泛型 `<T>` → Rust 泛型 `<T>`

3. **并发模式映射**：
   - `synchronized` → `Arc<Mutex<>>` 或 `Arc<RwLock<>>`
   - `volatile` → `Arc<Atomic<>>` 或 `Arc<AtomicCell<>>`
   - `ConcurrentHashMap` → `Arc<DashMap<>>` 或 `tokio::sync::RwLock<HashMap<>>`

4. **异常处理映射**：
   - Checked exceptions → `Result<T, E>`
   - Unchecked exceptions → `panic!` 或 `Result<T, E>`
   - `try-catch-finally` → `match` 或 `?` 操作符

5. **Builder 模式**：
   - Java Builder 模式 → Rust Builder 模式
   - 保持 1:1 对应

### 可靠性细节迁移

**关键可靠性组件：**

1. **RetryWithToleranceOperator**（364 行）
   - 重试逻辑
   - 指数退避
   - 容错类型（NONE, ALL）
   - 超时控制

2. **异常层次结构**
   - `ConnectException`（基类）
   - `RetriableException`（可重试）
   - `DataException`（数据错误）
   - `AlreadyExistsException`
   - `NotFoundException`
   - `IllegalWorkerStateException`

3. **校验机制**
   - `validate()` 方法
   - `ConfigDef` 验证
   - 偏移验证

4. **超时和死信检测**
   - `timeout` 参数
   - `deadline` 检查
   - 心跳机制

5. **幂等性保证**
   - 幂等操作
   - 基移提交幂等性
   - 连接器重启幂等性

## 模块拆分

### Workspace 结构

```
kafka-connect-rust/
├── Cargo.toml              (workspace)
├── Cargo.lock
├── connect-api/             (对应 connect/api)
│   ├── src/
│   │   ├── connector.rs
│   │   ├── source.rs
│   │   ├── sink.rs
│   │   ├── data.rs
│   │   ├── storage.rs
│   │   ├── header.rs
│   │   ├── health.rs
│   │   ├── rest.rs
│   │   ├── transforms.rs
│   │   └── errors.rs
│   └── tests/
│
├── connect-runtime/          (对应 connect/runtime)
│   ├── src/
│   │   ├── cli.rs
│   │   ├── runtime.rs
│   │   ├── distributed.rs
│   │   ├── standalone.rs
│   │   ├── errors.rs
│   │   ├── health.rs
│   │   ├── isolation.rs
│   │   ├── rest.rs
│   │   ├── storage.rs
│   │   └── util.rs
│   └── tests/
│
├── connect-json/            (对应 connect/json)
├── connect-transforms/       (对应 connect/transforms)
├── connect-file/            (对应 connect/file)
├── connect-mirror/           (对应 connect/mirror)
├── connect-mirror-client/    (对应 connect/mirror-client)
├── connect-test-plugins/      (对应 connect/test-plugins)
│   ├── src/
│   │   ├── mock_connector.rs
│   │   ├── mock_sink.rs
│   │   ├── mock_source.rs
│   │   ├── verifiable_sink.rs
│   │   ├── verifiable_source.rs
│   │   └── schema_source.rs
│   └── tests/
│
├── kafka-clients-mock/       (新增：Kafka 客户端 Mock)
│   ├── src/
│   │   ├── producer.rs
│   │   ├── consumer.rs
│   │   └── admin.rs
│   └── tests/
│
└── examples/                (示例代码)
```

### 依赖层次

```
connect-runtime
  ├── connect-api
  ├── connect-json
  ├── connect-transforms
  └── kafka-clients-mock

connect-mirror
  ├── connect-runtime
  └── connect-mirror-client

connect-file
  └── connect-api

connect-transforms
  └── connect-api

connect-test-plugins
  └── connect-api
```

## 迁移阶段

### 阶段阶段规划

每个阶段都要求 `cargo check` 和 `cargo test` 通过。

```
┌─────────────────────────────────────────────────────────────┐
│              迁移阶段和依赖关系                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  阶段 1: 基础设施（无外部依赖）                            │
│  ┌─────────────────────────────────────────────┐   │
│  │ 1.1 创建 workspace 和基本结构                        │   │
│  │ 1.2 kafka-clients-mock (基础 Mock 框架)            │   │
│  │     • 定义 Producer/Consumer/Admin trait              │   │
│  │     • 实现内存存储                                   │   │
│  │     • 编写基础测试                                   │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 2: connect-api（核心数据模型和接口）                    │
│  ┌─────────────────────────────────────────────┐   │
│  │ 2.1 errors.rs (异常类型)                            │   │
│  │ 2.2 data.rs (Schema, Struct, Values)                 │   │
│  │ 2.3 header.rs (Headers, Header)                       │   │
│  │ 2.4 storage.rs (Converter 接口)                      │   │
│  │ 2.5 connector.rs (Connector, Task 基类)               │   │
│  │ 2.6 source.rs (SourceConnector, SourceTask)            │   │
│  │ 2.7 sink.rs (SinkConnector, SinkTask)                  │   │
│  │ 2.8 health.rs (健康状态接口)                         │   │
│  │ 2.9 rest.rs (REST 扩展接口)                         │   │
│  │ 2.10 transforms.rs (Transformation, Predicate)          │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 3: connect-json（JSON 转换器）                        │
│  ┌─────────────────────────────────────────────┐   │
│  │ 3.1 实现 JsonConverter                                │   │
│  │ 3.2 编写单元测试                                   │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 4: connect-test-plugins（测试插件）                     │
│  ┌─────────────────────────────────────────────┐   │
│  │ 4.1 MockConnector, MockSinkConnector, MockSourceConnector│   │
│  │ 4.2 VerifiableSinkConnector, VerifiableSourceConnector  │   │
│  │ 4.3 SchemaSourceConnector                           │   │
│  │ 4.4 编写测试验证插件功能                             │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 5: connect-runtime 核心组件（依赖 connect-api）           │
│  ┌─────────────────────────────────────────────┐   │
│  │ 5.1 isolation.rs (插件系统)                          │   │
│  │     • 插件注册表                                     │   │
│  │     • 插件发现和实例化                               │   │
│  │ 5.2 storage.rs (BackingStore 实现)                    │   │
│  │ 5.3 util.rs (TopicAdmin, KafkaBasedLog)              │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 6: connect-runtime Worker 和 Herder                    │
│  ┌─────────────────────────────────────────────┐   │
│  │ 6.1 Worker (连接器和任务容器)                        │   │
│  │ 6.2 AbstractHerder (Herder 基类)                   │   │
│  │ 6.3 StandaloneHerder                                 │   │
│  │ 6.4 WorkerConnector, WorkerTask                       │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 7: connect-runtime 分布式组件                          │
│  ┌─────────────────────────────────────────────┐   │
│  │ 7.1 DistributedHerder                               │   │
│  │ 7.2 WorkerCoordinator                               │   │
│  │ 7.3 ConnectAssignor                                 │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 8: connect-runtime REST API                          │
│  ┌─────────────────────────────────────────────┐   │
│  │ 8.1 RestServer (actix-web/axum)                     │   │
│  │ 8.2 REST Resources (Connectors, Plugins, Logging)      │   │
│  │ 8.3 REST Entities                                   │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 9: CLI 入口点                                        │
│  ┌─────────────────────────────────────────────┐   │
│  │ 9.1 ConnectStandalone                               │   │
│  │ 9.2 ConnectDistributed                              │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 10: connect-transforms（内置转换器）                   │
│  ┌─────────────────────────────────────────────┐   │
│  │ 10.1 实现 26 个内置转换器                         │   │
│  │     • Cast, DropHeaders, ExtractField, Flatten         │   │
│  │     • HeaderFrom, HoistField, InsertField          │   │
│  │     • InsertHeader, MaskField, ReplaceField          │   │
│  │     • SetSchemaMetadata, TimestampConverter         │   │
│  │     • TimestampRouter, RegexRouter                    │   │
│  │ 10.2 实现 4 个谓词                                 │   │
│  │     • HasHeaderKey, RecordIsTombstone             │   │
│  │     • TopicNameMatches                              │   │
│  │ 10.3 编写单元测试                                   │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 11: connect-file（文件连接器）                         │
│  ┌─────────────────────────────────────────────┐   │
│  │ 11.1 FileStreamSinkConnector/Task                       │   │
│  │ 11.2 FileStreamSourceConnector/Task                       │   │
│  │ 11.3 编写单元测试                                   │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 12: connect-mirror-client（MirrorMaker2 客户端）        │
│  ┌─────────────────────────────────────────────┐   │
│  │ 12.1 MirrorClient 及相关类                          │   │
│  │ 12.2 ReplicationPolicy 实现                          │   │
│  │ 12.3 编写单元测试                                   │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 13: connect-mirror（MirrorMaker2 连接器）               │
│  ┌─────────────────────────────────────────────┐   │
│  │ 13.1 MirrorSourceConnector/Task                       │   │
│  │ 13.2 MirrorHeartbeatConnector/Task                    │   │
│  │ 13.3 MirrorCheckpointConnector/Task                     │   │
│  │ 13.4 MirrorMaker 工具类                             │   │
│  │ 13.5 编写单元测试                                   │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
│  阶段 14: 集成测试和端到端测试                            │
│  ┌─────────────────────────────────────────────┐   │
│  │ 14.1 实现集成测试框架                             │   │
│  │ 14.2 迁移所有原始集成测试（201 个测试文件）       │   │
│  │ 14.3 实现 MM2 端到端测试                         │   │
│  │     ✅ cargo check && cargo test                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 测试策略

### 测试层次

```
┌─────────────────────────────────────────────────────┐
│              测试层次结构                                 │
├─────────────────────────────────────────────────────┤
│                                                             │
│  1. 单元测试 (Unit Tests)                                   │
│     • 每个模块的独立测试                                   │
│     • 使用 Mock 数据                                         │
│     • cargo test --lib                                       │
│                                                             │
│  2. 集成测试 (Integration Tests)                            │
│     • 使用 connect-test-plugins                                │
│     • 测试插件加载、实例化、执行                              │
│     • 测试 Worker、Herder 组件                               │
│     • cargo test --test '*_integration_test'                    │
│                                                             │
│  3. 组件测试 (Component Tests)                               │
│     • 测试 REST API                                          │
│     • 测试分布式协调                                         │
│     • 测试偏移管理                                           │
│     • cargo test --test '*_component_test'                      │
│                                                             │
│  4. 端到端测试 (End-to-End Tests)                           │
│     • 完整的 Connect 流程                                     │
│     • 使用 Mock Kafka 集群                                   │
│     • 验证数据正确性                                         │
│     • cargo test --test '*_e2e_test'                         │
│                                                             │
│  5. MM2 场景测试 (MirrorMaker2.0 Tests)                   │
│     • 测试集群间镜像复制                                     │
│     • 测试故障转移和恢复                                     │
│     • 测试偏移同步                                           │
│     • cargo test --test '*_mm2_test'                         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 原始测试迁移

**迁移所有 201 个测试文件（81,050 行测试代码）：**

1. **connect/api 测试**（22 个文件）
   - `ConnectorTest.java`
   - `ConnectSchemaTest.java`
   - `SchemaBuilderTest.java`
   - `StructTest.java`
   - `ValuesTest.java`
   - 等

2. **connect/runtime 测试**（119 个文件）
   - `WorkerTest.java`（3,210 行）
   - `DistributedHerderTest.java`（4,429 行）
   - `AbstractHerderTest.java`（1,407 行）
   - `WorkerSinkTaskTest.java`（2,034 行）
   - `WorkerSourceTaskTest.java`（1,027 行）
   - 等

3. **connect/mirror 测试**（28 个文件）
   - `MirrorConnectorsIntegrationBaseTest.java`（1,501 行）
   - `MirrorSourceConnectorTest.java`（768 行）
   - `MirrorSourceTaskTest.java`（355 行）
   - 等

4. **connect/transforms 测试**（21 个文件）
   - `CastTest.java`
   - `FlattenTest.java`
   - `TimestampConverterTest.java`
   - 等

5. **connect/file 测试**（6 个文件）
   - `FileStreamSinkConnectorTest.java`
   - `FileStreamSourceConnectorTest.java`
   - 等

6. **connect/mirror-client 测试**（3 个文件）
   - `MirrorClientTest.java`
   - `ReplicationPolicyTest.java`
   - 等

### 额外测试设计

**MM2 场景端到端测试：**

```rust
// connect-mirror/tests/mm2_e2e_test.rs
use kafka_connect_runtime::*;
use kafka_connect_mock::*;
use kafka_connect_mirror::*;

#[tokio::test]
async fn test_mm2_replication_between_clusters() {
    // 创建两个 Mock Kafka 集群
    let source_cluster = MockKafkaCluster::new("source-cluster");
    let target_cluster = MockKafkaCluster::new("target-cluster");
    
    // 创建 MM2 配置
    let mm2_config = MirrorMakerConfig {
        source_bootstrap_servers: source_cluster.bootstrap_servers(),
        target_bootstrap_servers: target_cluster.bootstrap_servers(),
        topics: vec!["test-topic".to_string()],
        replication_policy: ReplicationPolicy::Identity,
    };
    
    // 启动 MM2
    let mut mm2 = MirrorMaker::new(mm2_config).await.unwrap();
    mm2.start().await.unwrap();
    
    // 在源集群生产数据
    source_cluster.produce("test-topic", vec![
        b"message1".to_vec(),
        b"message2".to_vec(),
        b"message3".to_vec(),
    ]).await;
    
    // 等待复制完成
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // 验证目标集群数据
    let target_messages = target_cluster.consume("test-topic").await;
    assert_eq!(target_messages.len(), 3);
    assert_eq!(target_messages[0], b"message1".to_vec());
    assert_eq!(target_messages[1], b"message2".to_vec());
    assert_eq!(target_messages[2], b"message3".to_vec());
    
    mm2.stop().await.unwrap();
}

#[tokio::test]
async fn test_mm2_failover() {
    // 测试故障转移
    let source_cluster = MockKafkaCluster::new("source-cluster");
    let backup_cluster = MockKafkaCluster::new("backup-cluster");
    let target_cluster = MockKafkaCluster::new("target-cluster");
    
    let mm2_config = MirrorMakerConfig {
        source_bootstrap_servers: source_cluster.bootstrap_servers(),
        backup_bootstrap_servers: Some(backup_cluster.bootstrap_servers()),
        target_bootstrap_servers: target_cluster.bootstrap_servers(),
        topics: vec!["test-topic".to_string()],
        replication_policy: ReplicationPolicy::Identity,
    };
    
    let mut mm2 = MirrorMaker::new(mm2_config).await.unwrap();
    mm2.start().await.unwrap();
    
    // 生产数据
    source_cluster.produce("test-topic", vec![b"message1".to_vec()]).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // 模拟源集群故障
    source_cluster.simulate_failure().await;
    
    // 验证备份集群接管
    tokio::time::sleep(Duration::from_secs(3)).await;
    let target_messages = target_cluster.consume("test-topic").await;
    assert_eq!(target_messages.len(), 1);
    
    mm2.stop().await.unwrap();
}
```

## 可靠性细节迁移清单

### 1. 异常处理

**异常层次结构（1:1 迁移）：**

```rust
// connect-api/src/errors.rs

/// ConnectException 是 Kafka Connect 生成的顶级异常类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectException {
    message: String,
    cause: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl ConnectException {
    pub fn new(message: impl Into<String>) -> Self {
        ConnectException {
            message: message.into(),
            cause: None,
        }
    }
    
    pub fn with_cause(message: impl Into<String>, cause: Box<dyn std::error::Error + Send + Sync>) -> Self {
        ConnectException {
            message: message.into(),
            cause: Some(cause),
        }
    }
}

impl std::error::Error for ConnectException {
    fn source(&self) -> Option<&(dyn std::::error::Error + 'static)> {
        self.cause.as_ref().map(|e| e.as_ref())
    }
    
    fn description(&self) -> &str {
        &self.message
    }
}

/// 表示操作可以重试的异常
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetriableException {
    message: String,
    cause: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl RetriableException {
    pub fn new(message: impl Into<String>) -> Self {
        RetriableException {
            message: message.into(),
            cause: None,
        }
    }
}

impl std::error::Error for RetriableException {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_ref().map(|e| e.as_ref())
    }
    
    fn description(&self) -> &str {
        &self.message
    }
}

/// 数据异常
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataException {
    message: String,
    cause: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl DataException {
    pub fn new(message: impl Into<String>) -> Self {
        DataException {
            message: message.into(),
            cause: None,
        }
    }
}

impl std::error::Error for DataException {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_ref().map(|e| e.as_ref())
    }
    
    fn description(&self) -> &str {
        &self.message
    }
}

/// 已存在异常
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlreadyExistsException {
    message: String,
}

impl AlreadyExistsException {
    pub fn new(message: impl Into<String>) -> Self {
        AlreadyExistsException {
            message: message.into(),
        }
    }
}

impl std::error::Error for AlreadyExistsException {
    fn description(&self) -> &str {
        &self.message
    }
}

/// 未找到异常
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotFoundException {
    message: String,
}

impl NotFoundException {
    pub fn new(message: impl Into<String>) -> Self {
        NotFoundException {
            message: message.into(),
        }
    }
}

impl std::error::Error for NotFoundException {
    fn description(&self) -> &str {
        &self.message
    }
}

/// 非法 Worker 状态异常
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IllegalWorkerStateException {
    message: String,
}

impl IllegalWorkerStateException {
    pub fn new(message: impl Into<String>) -> Self {
        IllegalWorkerStateException {
            message: message.into(),
        }
    }
}

impl std::error::Error for IllegalWorkerStateException {
    fn description(&self) -> &str {
        &self.message
    }
}
```

### 2. 重试和容错

**RetryWithToleranceOperator（1:1 迁移）：**

```rust
// connect-runtime/src/errors/retry_with_tolerance_operator.rs

use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::time::{sleep, Duration, Instant};

/// 容错类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToleranceType {
    /// 不容错任何错误
    None,
    /// 容错所有错误
    All,
}

/// 操作阶段
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Transformation,
    HeaderConverter,
    KeyConverter,
    ValueConverter,
    SinkTask,
    SourceTask,
}

/// 尝试并容错的操作
pub trait Operation<V>: Send + Sync {
    fn call(&self) -> Result<V;
}

/// 重试和容错操作符
pub struct RetryWithToleranceOperator<T> {
    error_retry_timeout: Duration,
    error_max_delay_in_millis: u64,
    error_tolerance_type: ToleranceType,
    total_failures: Arc<Mutex<u64>>,
    stopping: Arc<Mutex<bool>>,
    stop_requested: Arc<Notify>,
}

impl<T> RetryWithToleranceOperator<T> {
    pub fn new(
        error_retry_timeout: Duration,
        error_max_delay_in_millis: u64,
        error_tolerance_type: ToleranceType,
    ) -> Self {
        RetryWithToleranceOperator {
            error_retry_timeout,
            error_max_delay_in_millis,
            error_tolerance_type,
            total_failures: Arc::new(Mutex::new(0)),
            stopping: Arc::new(Mutex::new(false)),
            stop_requested: Arc::new(Notify::new()),
        }
    }
    
    /// 执行操作，处理可重试和容错异常
    pub async fn execute(
        &self,
        context: &mut ProcessingContext<T>,
        operation: Box<dyn Operation<V> + Send + Sync>,
        stage: Stage,
    ) -> Result<V, ConnectException> {
        let mut attempt = 0;
        let start_time = Instant::now();
        let deadline = if self.error_retry_timeout.as_millis() > 0 {
            Some(start_time + self.error_retry_timeout)
        } else {
            None
        };
        
        loop {
            attempt += 1;
            context.set_attempt(attempt);
            
            match operation.call() {
                Ok(result) => {
                    return Ok(result);
                }
                Err(e) => {
                    // 检查是否是可重试异常
                    if !self.is_retriable(&e) {
                        return Err(ConnectException::with_cause(
                            "Non-retriable exception",
                            Box::new(e),
                        ));
                    }
                    
                    // 记录失败
                    {
                        let mut failures = self.total_failures.lock().await;
                        *failures += 1;
                    }
                    
                    // 检查是否超时
                    if let Some(deadline) = deadline {
                        if Instant::now() >= deadline {
                            return Err(ConnectException::with_cause(
                                "Retry timeout exceeded",
                                Box::new(e),
                            ));
                        }
                    }
                    
                    // 检查是否已停止
                    if *self.stopping.lock().await {
                        return Err(ConnectException::with_cause(
                            "Operation stopped",
                            Box::new(e),
                        ));
                    }
                    
                    // 指数退避
                    let delay = self.calculate_backoff(attempt, deadline);
                    sleep(delay).await;
                }
            }
        }
    }
    
    /// 检查异常是否可重试
    fn is_retriable(&self, error: &dyn std::error::Error) -> bool {
        // 检查是否是 RetriableException
        error.downcast_ref::<RetriableException>().is_some()
    }
    
    /// 计算退避时间
    fn calculate_backoff(&self, attempt: u32, deadline: Option<Instant>) -> Duration {
        let num_retry = attempt.saturating_sub(1);
        let mut delay = 300u64 << num_retry; // RETRIES_DELAY_MIN_MS = 300
        
        if delay > self.error_max_delay_in_millis {
            delay = self.error_max_delay_in_millis;
        }
        
        // 检查是否超过 deadline
        if let Some(deadline) = deadline {
            let current_time = Instant::now();
            if delay.as_millis() + current_time.elapsed().as_millis() > deadline.elapsed().as_millis() {
                delay = deadline.saturating_duration_since(current_time).as_millis().max(0);
            }
            delay = delay.min(deadline.saturating_duration_since(current_time).as_millis());
        }
        
        Duration::from_millis(delay)
    }
    
    /// 停止重试
    pub async fn stop(&self) {
        *self.stopping.lock().await = true;
        self.stop_requested.notify_one();
    }
}

/// 处理上下文
pub struct ProcessingContext<T> {
    stage: Option<Stage>,
    attempt: u32,
    error: Option<Box<dyn std::error::Error + Send + Sync>>,
    failed: bool,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> ProcessingContext<T> {
    pub fn new() -> Self {
        ProcessingContext {
            stage: None,
            attempt: 0,
            error: None,
            failed: false,
            _phantom: std::marker::PhantomData,
        }
    }
    
    pub fn set_stage(&mut self, stage: Stage) {
        self.stage = Some(stage);
    }
    
    pub fn set_attempt(&mut self, attempt: u32) {
        self.attempt = attempt;
    }
    
    pub fn set_error(&mut self, error: Box<dyn std::error::Error + Send + Sync>) {
        self.error = Some(error);
        self.failed = true;
    }
    
    pub fn failed(&self) -> bool {
        self.failed
    }
}
```

### 3. 并发安全

**并发模式映射：**

```rust
// Java synchronized → Rust Arc<Mutex<>> 或 Arc<RwLock<>>

use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct Worker {
    connectors: Arc<Mutex<HashMap<String, WorkerConnector>>>,
    tasks: Arc<RwLock<HashMap<ConnectorTaskId, WorkerTask>>>,
    metrics: Arc<Mutex<ConnectMetrics>>,
}

// Java volatile → Rust Arc<Atomic<>> 或 Arc<AtomicCell<>>

use std::sync::atomic::{AtomicBool, AtomicU64};

pub struct RetryWithToleranceOperator<T> {
    stopping: Arc<AtomicBool>,
    total_failures: Arc<AtomicU64>,
}

// Java ConcurrentHashMap → Rust Arc<DashMap<>> 或 tokio::sync::RwLock<HashMap<>>

use dashmap::DashMap;

pub struct PluginRegistry {
    source_connectors: Arc<DashMap<String, Box<dyn Fn() -> Box<dyn SourceConnector>>>>,
    sink_connectors: Arc<DashMap<String, Box<dyn Fn() -> Box<dyn SinkConnector>>>>,
}
```

### 4. 超时控制

**超时和死信检测：**

```rust
use tokio::time::{sleep, timeout, Duration, Instant};

pub struct WorkerTask {
    shutdown_timeout: Duration,
    stopping: Arc<AtomicBool>,
}

impl WorkerTask {
    pub async fn await_stop(&self) -> Result<(), ConnectException> {
        let start_time = Instant::now();
        
        loop {
            if *self.stopping.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(());
            }
            
            let elapsed = start_time.elapsed();
            if elapsed >= self.shutdown_timeout {
                return Err(ConnectException::new("Shutdown timeout exceeded"));
            }
            
            sleep(Duration::from_millis(100)).await;
        }
    }
    
    pub async fn execute_with_timeout<F, R>(
        &self,
        operation: F,
        timeout: Duration,
    ) -> Result<R, ConnectException>
    where
        F: FnOnce() -> Result<R, ConnectException>,
    {
        match timeout(timeout, operation).await {
            Ok(result) => result,
            Err(_) => Err(ConnectException::new("Operation timeout")),
        }
    }
}
```

## 成功标准

1. **编译通过**：所有模块 `cargo check` 通过
2. **测试通过**：所有测试 `cargo test` 通过
3. **功能对等**：与 Java 版本功能 100% 对等
4. **性能相当**：性能不低于 Java 版本的 80%
5. **可靠性保证**：所有可靠性细节完整迁移

## 风险和缓解

| 风险 | 缓解措施 |
|------|----------|
| 大规模代码迁移 | 分阶段小步迭代，每阶段验证 |
| 并发模式转换 | 使用 Rust 最佳实践，充分测试 |
| 异常处理转换 | 建立清晰的映射规则，逐行验证 |
| 性能差异 | 性能测试，必要时优化 |
| 测试覆盖不足 | 迁移所有原始测试，新增额外测试 |

## 下一步

1. 审查提案
2. 开始阶段 1：基础设施
3. 逐步完成所有阶段
