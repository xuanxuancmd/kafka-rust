# Kafka Connect Rust 重写任务列表

## 阶段 1: 基础设施（无外部依赖）

**总体要求：**mock版的kafka-client需要实现内存版+预制数据功能，供后续其他模块端到端测试使用。

### 任务 1.1: 创建 Workspace 和基本结构

**目标**: 创建 Rust workspace 和项目骨架，所有代码在 `rust/` 目录下

**文件清单**:
- `rust/Cargo.toml` (workspace root)
- `rust/kafka-clients-mock/Cargo.toml`
- `rust/kafka-clients-mock/src/lib.rs`
- `rust/kafka-clients-mock/src/producer.rs`
- `rust/kafka-clients-mock/src/consumer.rs`
- `rust/kafka-clients-mock/src/admin.rs`
- `rust/kafka-clients-mock/tests/producer_test.rs`
- `rust/kafka-clients-mock/tests/consumer_test.rs`
- `rust/kafka-clients-mock/tests/admin_test.rs`

**验证**:
- `cd rust && cargo check --workspace`
- `cd rust && cargo test --workspace`

---

###  任务 1.2: kafka-clients-mock - 定义 Producer 接口

**目标**: 定义 Producer trait，对应 Java `org.apache.kafka.clients.producer.Producer`

**Java 源文件**: `clients/src/main/java/org/apache/kafka/clients/producer/Producer.java`

**Rust 文件**: `rust/kafka-clients-mock/src/producer.rs`

**1:1 翻译清单**:
- `init_transactions()` → `init_transactions()`
- `begin_transaction()` → `begin_transaction()`
- `send(ProducerRecord)` → `send(ProducerRecord)`
- `send_offsets_to_transaction()` → `send_offsets_to_transaction()`
- `prepare_transaction()` → `prepare_transaction()`
- `commit_transaction()` → `commit_transaction()`
- `abort_transaction()` → `abort_transaction()`
- `flush()` → `flush()`
- `close()` → `close()`

**验证**:
- `cd rust && cargo check -p kafka-clients-mock`

---

### 任务 1.3: kafka-clients-mock - 定义 Consumer 接口

**目标**: 定义 Consumer trait，对应 Java `org.apache.kafka.clients.consumer.Consumer`

**Java 源文件**: `clients/src/main/java/org/apache/kafka/clients/consumer/Consumer.java`

**Rust 文件**: `rust/kafka-clients-mock/src/consumer.rs`

**1:1 翻译清单**:
- `assignment()` → `assignment()`
- `subscription()` → `subscription()`
- `subscribe(topics)` → `subscribe(topics)`
- `subscribe(topics, callback)` → `subscribe(topics, callback)`
- `assign(partitions)` → `assign(partitions)`
- `subscribe(pattern, callback)` → `subscribe(pattern, callback)`
- `unsubscribe()` → `unsubscribe()`
- `poll(timeout)` → `poll(timeout)`
- `commit_sync()` → `commit_sync()`
- `commit_async()` → `commit_async()`
- `seek(partition, offset)` → `seek(partition, offset)`
- `position(partition)` → `position(partition)`
- `close()` → `close()`

**验证**:
- `cd rust && cargo check -p kafka-clients-mock`

---

### 任务 1.4: kafka-clients-mock - 定义 Admin 接口

**目标**: 定义 Admin trait，对应 Java `org.apache.kafka.clients.admin.Admin`

**Java 源文件**: `clients/src/main/java/org/apache/kafka/clients/admin/Admin.java`

**Rust 文件**: `rust/kafka-clients-mock/src/admin.rs`

**1:1 翻译清单**:
- `create_topics()` → `create_topics()`
- `delete_topics()` → `delete_topics()`
- `list_topics()` → `list_topics()`
- `describe_topics()` → `describe_topics()`
- `alter_configs()` → `alter_configs()`
- `describe_configs()` → `describe_configs()`
- `list_consumer_group_offsets()` → `list_consumer_group_offsets()`
- `alter_consumer_group_offsets()` → `alter_consumer_group_offsets()`
- `delete_consumer_group_offsets()` → `delete_consumer_group_offsets()`
- `delete_consumer_groups()` → `delete_consumer_groups()`
- `fence_producers()` → `fence_producers()`
- `close()` → `close()`

**验证**:
- `cd rust && cargo check -p kafka-clients-mock`

---

### 任务 1.3: kafka-clients-mock - 定义 Consumer 接口

**目标**: 定义 Consumer trait，对应 Java `org.apache.kafka.clients.consumer.Consumer`

**Java 源文件**: `clients/src/main/java/org/apache/kafka/clients/consumer/Consumer.java`

**Rust 文件**: `rust/kafka-clients-mock/src/consumer.rs`

**1:1 翻译清单**:
- `assignment()` → `assignment()`
- `subscription()` → `subscription()`
- `subscribe(topics)` → `subscribe(topics)`
- `subscribe(topics, callback)` → `subscribe(topics, callback)`
- `assign(partitions)` → `assign(partitions)`
- `subscribe(pattern, callback)` → `subscribe(pattern, callback)`
- `unsubscribe()` → `unsubscribe()`
- `poll(timeout)` → `poll(timeout)`
- `commit_sync()` → `commit_sync()`
- `commit_async()` → `commit_async()`
- `seek(partition, offset)` → `seek(partition, offset)`
- `position(partition)` → `position(partition)`
- `close()` → `close()`

**验证**:
- `cd rust && cargo check -p kafka-clients-mock`

---

### 任务 1.4: kafka-clients-mock - 定义 Admin 接口

**目标**: 定义 Admin trait，对应 Java `org.apache.kafka.clients.admin.Admin`

**Java 源文件**: `clients/src/main/java/org/apache/kafka/clients/admin/Admin.java`

**Rust 文件**: `rust/kafka-clients-mock/src/admin.rs`

**1:1 翻译清单**:
- `create_topics()` → `create_topics()`
- `delete_topics()` → `delete_topics()`
- `list_topics()` → `list_topics()`
- `describe_topics()` → `describe_topics()`
- `alter_configs()` → `alter_configs()`
- `describe_configs()` → `describe_configs()`
- `list_consumer_group_offsets()` → `list_consumer_group_offsets()`
- `alter_consumer_group_offsets()` → `alter_consumer_group_offsets()`
- `delete_consumer_group_offsets()`()` → `delete_consumer_group_offsets()`
- `delete_consumer_groups()` → `delete_consumer_groups()`
- `fence_producers()` → `fence_producers()`
- `close()` → `close()`

**验证**:
- `cd rust && cargo check -p kafka-clients-mock`

---

### 任务 1.5: kafka-clients-mock - 实现 MockProducer

**目标**: 实现 MockProducer，对应 Java `org.apache.kafka.clients.producer.MockProducer`

**Java 源文件**: `clients/src/main/java/org/apache/kafka/clients/producer/MockProducer.java`

**Rust 文件**: `rust/kafka-clients-mock/src/producer.rs` (impl block)

**1:1 翻译清单**:
- 内存存储实现（HashMap + Arc<RwLock<>>）
- 事务状态管理
- 偏移管理
- 预置数据支持
- 错误注入支持

**验证**:
- `cd rust && cargo check -p kafka-clients-mock`
- `cd rust && cargo test -p kafka-clients-mock producer_test`

---

###  任务 1.6: kafka-clients-mock - 实现 MockConsumer

**目标**: 实现 MockConsumer，对应 Java `org.apache.kafka.clients.consumer.MockConsumer`

**Java 源文件**: `clients/src/main/java/org/apache/kafka/clients/consumer/MockConsumer.java`

**Rust 文件**: `rust/kafka-clients-mock/src/consumer.rs` (impl block)

**1:1 翻译清单**:
- 内存存储实现
- 订阅管理
- 偏移管理
- 预置数据支持
- 消费者组管理

**验证**:
- `cd rust && cargo check -p kafka-clients-mock`
- `cd rust && cargo test -p kafka-clients-mock consumer_test`

---

### 任务 1.7: kafka-clients-mock - 实现 MockAdmin

**目标**: 实现 MockAdmin，对应 Java `org.apache.kafka.clients.admin.MockAdminClient`

**Java 源文件**: `clients/src/test/java/org/apache/kafka/clients/admin/MockAdminClient.java`

**Rust 文件**: `rust/kafka-clients-mock/src/admin.rs` (impl block)

**1:1 翻译清单**:
- Topic 管理（创建、删除、描述）
- 配置管理
- 消费者组管理
- 事务管理（fence producers）
- 预置数据支持

**验证**:
- `cd rust && cargo check -p kafka-clients-mock`
- `cd rust && cargo test -p kafka-clients-mock admin_test`

---

## 阶段 2: connect-api（核心数据模型和接口）

### 任务 2.1: connect-api - 创建 crate 结构

**目标**: 创建 connect-api crate 结构

**文件清单**:
- `rust/connect-api/Cargo.toml`
- `rust/connect-api/src/lib.rs`
- `rust/connect-api/src/errors.rs`
- `rust/connect-api/src/data.rs`
- `rust/connect-api/src/header.rs`
- `rust/connect-api/src/storage.rs`
- `rust/connect-api/src/connector.rs`
- `rust/connect-api/src/source.rs`
- `rust/connect-api/src/sink.rs`
- `rust/connect-api/src/health.rs`
- `rust/connect-api/src/rest.rs`
- `rust/connect-api/src/transforms.rs`
- `rust/connect-api/src/util.rs`

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.2: connect-api - 实现异常类型

**目标**: 实现所有异常类型，对应 Java `org.apache.kafka.connect.errors.*`

**Java 源文件**:
- `connect/api/src/main/java/org/apache/kafka/connect/errors/ConnectException.java`
- `connect/api/src/main/java/org/apache/kafka/connect/errors/RetriableException.java`
- `connect/api/src/main/java/org/apache/kafka/connect/errors/DataException.java`
- `connect/api/src/main/java/org/apache/kafka/connect/errors/AlreadyExistsException.java`
- `connect/api/src/main/java/org/apache/kafka/connect/errors/NotFoundException.java`
- `connect/api/src/main/java/org/apache/kafka/connect/errors/IllegalWorkerStateException.java`
- `connect/api/src/main/java/org/apache/kafka/connect/errors/SchemaBuilderException.java`
- `connect/api/src/main/java/org/apache/kafka/connect/errors/SchemaProjectorException.java`

**Rust 文件**: `rust/connect-api/src/errors.rs`

**1:1 翻译清单**:
- `ConnectException` → `ConnectException`
- `RetriableException` → `RetriableException`
- `DataException` → `DataException`
- `AlreadyExistsException` → `AlreadyExistsException`
- `NotFoundException` → `NotFoundException`
- `IllegalWorkerStateException` → `IllegalWorkerStateException`
- `SchemaBuilderException` → `SchemaBuilderException`
- `SchemaProjectorException` → `SchemaProjectorException`

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.3: connect-api - 实现 Schema 相关类型

**目标**: 实现 Schema、SchemaBuilder、Struct、Values 等

**Java 源文件**:
- `connect/api/src/main/java/org/apache/kafka/connect/data/Schema.java`
- `connect/api/src/main/java/org/apache/kafka/connect/data/SchemaBuilder.java`
- `connect/api/src/main/java/org/apache/kafka/connect/data/Struct.java`
- `connect/api/src/main/java/org/apache/kafka/connect/data/Values.java`
- `connect/api/src/main/java/org/apache/kafka/connect/data/Field.java`
- `connect/api/src/main/java/org/apache/kafka/connect/data/SchemaAndValue.java`
- `connect/api/src/main/java/org/apache/kafka/connect/data/SchemaProjector.java`
- `connect/api/src/main/java/org/apache/kafka/connect/data/ConnectSchema.java`
- `connect/api/src/main/java/org/apache/kafka/connect/data/Date.java`
- `connect/api/src/main/java/org/apache/kafka/connect/data/Time.java`
- `connect/api/src/main/java/org/apache/kafka/connect/Timestamp.java`
- `connect/api/src/main/java/org/apache/kafka/connect/data/Decimal.java`

**Rust 文件**: `rust/connect-api/src/data.rs`

**1:1 翻译清单**:
- `Schema` trait 及所有实现
- `SchemaBuilder` 及所有 builder 方法
- `Struct` 及所有方法
- `Values` 及所有静态方法
- `Field` 结构
- `SchemaAndValue` 结构
- `SchemaProjector` 及所有方法
- `ConnectSchema` 结构
- `Date`、`Time`、`Timestamp`、`Decimal` 工具类

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.4: connect-api - 实现 Header 相关类型

**目标**: 实现 Headers、Header、ConnectHeaders 等

**Java 源文件**:
- `connect/api/src/main/java/org/apache/kafka/connect/header/Header.java`
- `connect/api/src/main/java/org/apache/kafka/connect/header/Headers.java`
- `connect/api/src/main/java/org/apache/kafka/connect/header/ConnectHeader.java`
- `connect/api/src/main/java/org/apache/kafka/connect/header/ConnectHeaders.java`

**Rust 文件**: `rust/connect-api/src/header.rs`

**1:1 翻译清单**:
- `Header` trait
- `Headers` trait
- `ConnectHeader` 结构
- `ConnectHeaders` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.5: connect-api - 实现 Converter 接口

**目标**: 实现 Converter、HeaderConverter 等接口

**Java 源文件**:
- `connect/api/src/main/java/org/apache/kafka/connect/storage/Converter.java`
- `connect/api/src/main/java/org/apache/kafka/connect/storage/HeaderConverter.java`
- `connect/api/src/main/java/org/apache/kafka/connect/storage/ConverterConfig.java`
- `connect/api/src/main/java/org/apache/kafka/connect/storage/ConverterType.java`
- `connect/api/src/main/java/org/apache/kafka/connect/storage/SimpleHeaderConverter.java`
- `connect/api/src/main/java/org/apache/kafka/connect/storage/StringConverter.java`
- `connect/api/src/main/java/org/apache/kafka/connect/storage/StringConverterConfig.java`
- `connect/api/src/main/java/org/apache/kafka/connect/storage/OffsetStorageReader.java`

**Rust 文件**: `rust/connect-api/src/storage.rs`

**1:1 翻译清单**:
- `Converter` trait
- `HeaderConverter` trait
- `ConverterConfig` trait
- `ConverterType` enum
- `SimpleHeaderConverter` 结构
- `StringConverter` 结构
- `StringConverterConfig` 结构
- `OffsetStorageReader` trait

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.6: connect-api - 实现 Connector 和 Task 基类

**目标**: 实现 Connector、Task、ConnectorContext 等核心接口

**Java 源文件**:
- `connect/api/src/main/java/org/apache/kafka/connect/connector/Connector.java`
- `connect/api/src/main/java/org/apache/kafka/connect/connector/Task.java`
- `connect/api/src/main/java/org/apache/kafka/connect/connector/ConnectorContext.java`
- `connect/api/src/main/java`/org/apache/kafka/connect/connector/ConnectRecord.java`
- `connect/api/src/main/java/org/apache/kafka/connect/components/Versioned.java`

**Rust 文件**: `rust/connect-api/src/connector.rs`

**1:1 翻译清单**:
- `Versioned` trait
- `Connector` trait 及所有方法
- `Task` trait
- `ConnectorContext` trait
- `ConnectRecord` trait

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.7: connect-api - 实现 SourceConnector 和 SourceTask

**目标**: 实现 SourceConnector、SourceTask 等接口

**Java 源文件**:
- `connect/api/src/main/java/org/apache/kafka/connect/source/SourceConnector.java`
- `connect/api/src/main/java/org/apache/kafka/connect/source/SourceTask.java`
- `connect/api/src/main/java/org/apache/kafka/connect/source/SourceConnectorContext.java`
- `connect/api/src/main/java/org/apache/kafka/connect/source/SourceTaskContext.java`
- `connect/api/src/main/java/org/apache/kafka/connect/source/SourceRecord.java`
- `connect/api/src/main/java/org/apache/kafka/connect/source/TransactionContext.java`
- `connect/api/src/main/java/org/apache/kafka/connect/source/ExactlyOnceSupport.java`
- `connect/api/src/main/java/org/apache/kafka/connect/source/ConnectorTransactionBoundaries.java`

**Rust 文件**: `rust/connect-api/src/source.rs`

**1:1 翻译清单**:
- `SourceConnector` trait 及所有方法
- `SourceTask` trait
- `SourceConnectorContext` trait
- `SourceTaskContext` trait
- `SourceRecord` 结构
- `TransactionContext` trait
- `ExactlyOnceSupport` enum
- `ConnectorTransactionBoundaries` enum

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.8: connect-api - 实现 SinkConnector 和 SinkTask

**目标**: 实现 SinkConnector、SinkTask 等接口

**Java 源文件**:
- `connect/api/src/main/java/org/apache/kafka/connect/sink/SinkConnector.java`
- `connect/api/src/main/java/org/apache/kafka/connect/sink/SinkTask.java`
- `connect/api/src/main/java/org/apache/kafka/connect/sink/SinkConnectorContext.java`
- `connect/api/src/main/java/org/apache/kafka/connect/sink/SinkTaskContext.java`
- `connect/api/src/main/java/org/apache/kafka/connect/sink/SinkRecord.java`
- `connect/api/src/main/java/org/apache/kafka/connect/sink/ErrantRecordReporter.java`

**Rust 文件**: `rust/connect-api/src/sink.rs`

**1:1 翻译清单**:
- `SinkConnector` trait 及所有方法
- `SinkTask` trait
- `SinkConnectorContext` trait
- `SinkTaskContext` trait
- `SinkRecord` 结构
- `ErrantRecordReporter` trait

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.9: connect-api - 实现健康状态接口

**目标**: 实现健康状态相关接口

**Java 源文件**:
- `connect/api/src/main/java/org/apache/kafka/connect/health/ConnectorHealth.java`
- `connect/api/src/main/java/org/apache/kafka/connect/health/ConnectorState.java`
- `connect/api/src/main/java/org/apache/kafka/connect/health/TaskState.java`
- `connect/api/src/main/java/org/apache/kafka/connect/health/ConnectorType.java`
- `connect/api/src/main/java/org/apache/kafka/connect/health/ConnectClusterDetails.java`
- `connect/api/src/main/java/org/apache/kafka/connect/health/ConnectClusterState.java`
- `connect/api/src/main/java/org/apache/kafka/connect/health/AbstractState.java`

**Rust 文件**: `rust/connect-api/src/health.rs`

**1:1 翻译清单**:
- `ConnectorHealth` trait
- `ConnectorState` enum
- `TaskState` 结构
- `ConnectorType` enum
- `ConnectClusterDetails` trait
- `ConnectClusterState` trait
- `AbstractState` 结构

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.10: connect-api - 实现 REST 扩展接口

**目标**: 实现 REST 扩展接口

**Java 源文件**:
- `connect/api/src/main/java/org/apache/kafka/connect/rest/ConnectRestExtension.java`
- `connect/api/src/main/java/org/apache/kafka/connect/rest/ConnectRestExtensionContext.java`

**Rust 文件**: `rust/connect-api/src/rest.rs`

**1:1 翻译清单**:
- `ConnectRestExtension` trait
- `ConnectRestExtensionContext` trait

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.11: connect-api - 实现转换器和谓词接口

**目标**: 实现转换器和谓词接口

**Java 源文件**:
- `connect/api/src/main/java/org/apache/kafka/connect/transforms/Transformation.java`
- `connect/api/src/main/java/org/apache/kafka/connect/transforms/predicates/Predicate.java`

**Rust 文件**: `rust/connect-api/src/transforms.rs`

**1:1 翻译清单**:
- `Transformation` trait
- `Predicate` trait

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.12: connect-api - 实现工具类

**目标**: 实现工具类

**Java 源文件**:
- `connect/api/src/main/java/org/apache/kafka/connect/util/ConnectorUtils.java`

**Rust 文件**: `rust/connect-api/src/util.rs`

**1:1 翻译清单**:
- `ConnectorUtils` 及所有静态方法

**验证**:
- `cd rust && cargo check -p connect-api`

---

### 任务 2.13: connect-api - 编写单元测试

**目标**: 编写 connect-api 的单元测试

**文件清单**:
- `rust/connect-api/tests/errors_test.rs`
- `rust/connect-api/tests/data_test.rs`
- `rust/connect-api/tests/header_test.rs`
- `rust/connect-api/tests/storage_test.rs`
- `rust/connect-api/tests/connector_test.rs`
- `rust/connect-api/tests/source_test.rs`
- `rust/connect-api/tests/sink_test.rs`
- `rust/connect-api/tests/health_test.rs`
- `rust/connect-api/tests/transforms_test.rs`

**测试覆盖**:
- 所有异常类型
- Schema 构建和验证
- Header 操作
- Converter 接口
- Connector 和 Task 生命周期
- Source 和 Sink 特定功能
- 健康状态
- 转换器和谓词

**验证**:
- `cd rust && cargo test -p connect-api`

---

## 阶段 3: connect-json（JSON 转换器）

### 任务 3.1: connect-json - 创建 crate 结构

**目标**: 创建 connect-json crate

**文件清单**:
- `rust/connect-json/Cargo.toml`
- `rust/connect-json/src/lib.rs`
- `rust/connect-json/src/converter.rs`

**验证**:
- `cd rust && cargo check -p connect-json`

---

### 任务 3.2: connect-json - 实现 JsonConverter

**目标**: 实现 JsonConverter，对应 Java `org.apache.kafka.connect.json.JsonConverter`

**Java 源文件**: `connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverter.java`

**Rust 文件**: `rust/connect-json/src/converter.rs`

**1:1 翻译清单**:
- `JsonConverter` 结构
- `JsonConverterConfig` 结构
- 所有 public 方法

**验证**:
- `cd rust && cargo check -p connect-json`

---

### 任务 3.3: connect-json - 编写单元测试

**目标**: 编写 JsonConverter 的单元测试

**文件清单**:
- `rust/connect-json/tests/converter_test.rs`

**测试覆盖**:
- JSON 序列化/反序列化
- Schema 处理
- 错误处理

**验证**:
- `cd rust && cargo test -p connect-json`

---

## 阶段 4: connect-test-plugins（测试插件）

### 任务 4.1: connect-test-plugins - 创建 crate 结构

**目标**: 创建 connect-test-plugins crate

**文件清单**:
- `rust/connect-test-plugins/Cargo.toml`
- `rust/connect-test-plugins/src/lib.rs`
- `rust/connect-test-plugins/src/mock_connector.rs`
- `rust/connect-test-plugins/src/mock_sink.rs`
- `rust/connect-test-plugins/src/mock_source.rs`
- `rust/connect-test-plugins/src/verifiable_sink.rs`
- `rust/connect-test-plugins/src/verifiable_source.rs`
- `rust/connect-test-plugins/src/schema_source.rs`

**验证**:
- `cd rust && cargo check -p connect-test-plugins`

---

### 任务 4.2: connect-test-plugins - 实现 MockConnector

**目标**: 实现 MockConnector，对应 Java `org.apache.kafka.connect.tools.MockConnector`

**Java 源文件**: `connect/test-plugins/src/main/java/org/apache/kafka/connect/tools/MockConnector.java`

**Rust 文件**: `rust/connect-test-plugins/src/mock_connector.rs`

**1:1 翻译清单**:
- `MockConnector` 结构
- 所有 public 方法

**验证**:
- `cd rust && cargo check -p connect-test-plugins`

---

### 任务 4.3: connect-test-plugins - 实现 MockSinkConnector/Task

**目标**: 实现 MockSinkConnector 和 MockSinkTask

**Java 源文件**:
- `connect/test-plugins/src/main/java/org/apache/kafka/connect/tools/MockSinkConnector.java`
- `connect/test-plugins/src/main/java/org/apache/kafka/connect/tools/MockSinkTask.java`

**Rust 文件**: `rust/connect-test-plugins/src/mock_sink.rs`

**1:1 翻译清单**:
- `MockSinkConnector` 结构
- `MockSinkTask` 结构
- 所有 public 方法

**验证**:
- `cd rust && cargo check -p connect-test-plugins`

---

### 任务 4.4: connect-test-plugins - 实现 MockSourceConnector/Task

**目标**: 实现 MockSourceConnector 和 MockSourceTask

**Java 源文件**:
- `connect/test-plugins/src/main/java/org/apache/kafka/connect/tools/MockSourceConnector.java`
- `connect/test-plugins/src/main/java/org/apache/kafka/connect/tools/MockSourceTask.java`

**Rust 文件**: `rust/connect-test-plugins/src/mock_source.rs`

**1:1 翻译清单**:
- `MockSourceConnector` 结构
- `MockSourceTask` 结构
- 所有 public 方法

**验证**:
- `cd rust && cargo check -p connect-test-plugins`

---

### 任务 4.5: connect-test-plugins - 实现 VerifiableSinkConnector/Task

**目标**: 实现 VerifiableSinkConnector 和 VerifiableSinkTask

**Java 源文件**:
- `connect/test-plugins/src/main/java/org/apache/kafka/connect/tools/VerifiableSinkConnector.java`
- `connect/test-plugins/src/main/java/org/apache/kafka/connect/tools/VerifiableSinkTask.java`

**Rust 文件**: `rust/connect-test-plugins/src/verifiable_sink.rs`

**1:1 翻译清单**:
- `VerifiableSinkConnector` 结构
- `VerifiableSinkTask` 结构
- 所有 public 方法

**验证**:
- `cd rust && cargo check -p connect-test-plugins`

---

### 任务 4.6: connect-test-plugins - 实现 VerifiableSourceConnector/Task

**目标**: 实现 VerifiableSourceConnector 和 VerifiableSourceTask

**Java 源文件**:
- `connect/test-plugins/src/main/java/org/apache/kafka/connect/tools/VerifiableSourceConnector.java`
- `connect/test-plugins/src/main/java/org/apache/kafka/connect/tools/VerifiableSourceTask.java`

**Rust 文件**: `rust/connect-test-plugins/src/verifiable_source.rs`

**1:1 翻译清单**:
- `VerifiableSourceConnector` 结构
- `VerifiableSourceTask` 结构
- 所有 public 方法

**验证**:
- `cd rust && cargo check -p connect-test-plugins`

---

### 任务 4.7: connect-test-plugins - 实现 SchemaSourceConnector/Task

**目标**: 实现 SchemaSourceConnector 和 SchemaSourceTask

**Java 源文件**:
- `connect/test-plugins/src/main/java/org/apache/kafka/connect/tools/SchemaSourceConnector.java`
- `connect/test-plugins/src/main/java/org/apache/kafka/connect/tools/SchemaSourceTask.java`

**Rust 文件**: `rust/connect-test-plugins/src/schema_source.rs`

**1:1 翻译清单**:
- `SchemaSourceConnector` 结构
- `SchemaSourceTask` 结构
- 所有 public 方法

**验证**:
- `cd rust && cargo check -p connect-test-plugins`

---

### 任务 4.8: connect-test-plugins - 编写单元测试

**目标**: 编写测试插件的单元测试

**文件清单**:
- `rust/connect-test-plugins/tests/mock_connector_test.rs`
- `rust/connect-test-plugins/tests/mock_sink_test.rs`
- `rust/connect-test-plugins/tests/mock_source_test.rs`
- `rust/connect-test-plugins/tests/verifiable_sink_test.rs`
- `rust/connect-test-plugins/tests/verifiable_source_test.rs`
- `rust/connect-test-plugins/tests/schema_source_test.rs`

**测试覆盖**:
- 所有测试插件的功能
- 数据生成和验证

**验证**:
- `cd rust && cargo test -p connect-test-plugins`

---

## 阶段 5: connect-transforms（内置转换器）

### 任务 5.1: connect-transforms - 创建 crate 结构

**目标**: 创建 connect-transforms crate

**文件清单**:
- `rust/connect-transforms/Cargo.toml`
- `rust/connect-transforms/src/lib.rs`
- `rust/connect-transforms/src/cast.rs`
- `rust/connect-transforms/src/drop_headers.rs`
- `rust/connect-transforms/src/extract_field.rs`
- `rust/connect-transforms/src/flatten.rs`
- `rust/connect-transforms/src/header_from.rs`
- `rust/connect-transforms/src/hoist_field.rs`
- `rust/connect-transforms/src/insert_field.rs`
- `rust/connect-transforms/src/insert_header.rs`
- `rust/connect-transforms/src/mask_field.rs`
- `rust/connect-transforms/src/replace_field.rs`
- `rust/connect-transforms/src/set_schema_metadata.rs`
- `rust/connect-transforms/src/timestamp_converter.rs`
- `rust/connect-transforms/src/timestamp_router.rs`
- `rust/connect-transforms/src/regex_router.rs`
- `rust/connect-transforms/src/predicates/has_header_key.rs`
- `rust/connect-transforms/src/predicates/record_is_tombstone.rs`
- `rust/connect-transforms/src/predicates/topic_name_matches.rs`

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.2: connect-transforms - 实现 Cast 转换器

**目标**: 实现 Cast 转换器，对应 Java `org.apache.kafka.connect.transforms.Cast`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/Cast.java`

**Rust 文件**: `rust/connect-transforms/src/cast.rs`

**1:1 翻译清单**:
- `Cast` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.3: connect-transforms - 实现 DropHeaders 转换器

**目标**: 实现 DropHeaders 转换器，对应 Java `org.apache.kafka.connect.transforms.DropHeaders`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/DropHeaders.java`

**Rust 文件**: `rust/connect-transforms/src/drop_headers.rs`

**1:1 翻译清单**:
- `DropHeaders` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.4: connect-transforms - 实现 ExtractField 转换器

**目标**: 实现 ExtractField 转换器，对应 Java `org.apache.kafka.connect.transforms.ExtractField`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/ExtractField.java`

**Rust 文件**: `rust/connect-transforms/src/extract_field.rs`

**1:1 翻译清单**:
- `ExtractField` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.5: connect-transforms - 实现 Flatten 转换器

**目标**: 实现 Flatten 转换器，对应 Java `org.apache.kafka.connect.transforms.Flatten`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/Flatten.java`

**Rust 文件**: `rust/connect-transforms/src/flatten.rs`

**1:1 翻译清单**:
- `Flatten` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.6: connect-transforms - 实现 HeaderFrom 转换器

**目标**: 实现 HeaderFrom 转换器，对应 Java `org.apache.kafka.connect.transforms.HeaderFrom`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/HeaderFrom.java`

**Rust 文件**: `rust/connect-transforms/src/header_from.rs`

**1:1 翻译清单**:
- `HeaderFrom` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.7: connect-transforms - 实现 HoistField 转换器

**目标**: 实现 HoistField 转换器，对应 Java `org.apache.kafka.connect.transforms.HoistField`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/HoistField.java`

**Rust 文件**: `rust/connect-transforms/src/hoist_field.rs`

**1:1 翻译清单**:
- `HoistField` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.8: connect-transforms - 实现 InsertField 转换器

**目标**: 实现 InsertField 转换器，对应 Java `org.apache.kafka.connect.transforms.InsertField`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/InsertField.java`

**Rust 文件**: `rust/connect-transforms/src/insert_field.rs`

**1:1 翻译清单**:
- `InsertField` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.9: connect-transforms - 实现 InsertHeader 转换器

**目标**: 实现 InsertHeader 转换器，对应 Java `org.apache.kafka.connect.transforms.InsertHeader`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/InsertHeader.java`

**Rust 文件**: `rust/connect-transforms/src/insert_header.rs`

**1:1 翻译清单**:
- `InsertHeader` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.10: connect-transforms - 实现 MaskField 转换器

**目标**: 实现 MaskField 转换器，对应 Java `org.apache.kafka.connect.transforms.MaskField`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/MaskField.java`

**Rust 文件**: `rust/connect-transforms/src/mask_field.rs`

**1:1 翻译清单**:
- `MaskField` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.11: connect-transforms - 实现 ReplaceField 转换器

**目标**: 实现 ReplaceField 转换器，对应 Java `org.apache.kafka.connect.transforms.ReplaceField`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/ReplaceField.java`

**Rust 文件**: `rust/connect-transforms/src/replace_field.rs`

**1:1 翻译清单**:
- `ReplaceField` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.12: connect-transforms - 实现 SetSchemaMetadata 转换器

**目标**: 实现 SetSchemaMetadata 转换器，对应 Java `org.apache.kafka.connect.transforms.SetSchemaMetadata`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/SetSchemaMetadata.java`

**Rust 文件**: `rust/connect-transforms/src/set_schema_metadata.rs`

**1:1 翻译清单**:
- `SetSchemaMetadata` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.13: connect-transforms - 实现 TimestampConverter 转换器

**目标**: 实现 TimestampConverter 转换器，对应 Java `org.apache.kafka.connect.transforms.TimestampConverter`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/TimestampConverter.java`

**Rust 文件**: `rust/connect-transforms/src/timestamp_converter.rs`

**1:1 翻译清单**:
- `TimestampConverter` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.14: connect-transforms - 实现 TimestampRouter 转换器

**目标**: 实现 TimestampRouter 转换器，对应 Java `org.apache.kafka.connect.transforms.TimestampRouter`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/TimestampRouter.java`

**Rust 文件**: `rust/connect-transforms/src/timestamp_router.rs`

**1:1 翻译清单**:
- `TimestampRouter` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.15: connect-transforms - 实现 RegexRouter 转换器

**目标**: 实现 RegexRouter 转换器，对应 Java `org.apache.kafka.connect.transforms.RegexRouter`

**Java 源文件**: `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/RegexRouter.java`

**Rust 文件**: `rust/connect-transforms/src/regex_router.rs`

**1:1 翻译清单**:
- `RegexRouter` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.16: connect-transforms - 实现谓词

**目标**: 实现谓词，对应 Java `org.apache.kafka.connect.transforms.predicates.*`

**Java 源文件**:
- `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/predicates/HasHeaderKey.java`
- `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/predicates/RecordIsTombstone.java`
- `connect/transforms/src/main/java/org/apache/kafka/connect/transforms/predicates/TopicNameMatches.java`

**Rust 文件**:
- `rust/connect-transforms/src/predicates/has_header_key.rs`
- `rust/connect-transforms/src/predicates/record_is_tombstone.rs`
- `rust/connect-transforms/src/predicates/topic_name_matches.rs`

**1:1 翻译清单**:
- `HasHeaderKey` 结构
- `RecordIsTombstone` 结构
- `TopicNameMatches` 结构

**验证**:
- `cd rust && cargo check -p connect-transforms`

---

### 任务 5.17: connect-transforms - 编写单元测试

**目标**: 编写所有转换器的单元测试

**文件清单**:
- `rust/connect-transforms/tests/cast_test.rs`
- `rust/connect-transforms/tests/drop_headers_test.rs`
- `rust/connect-transforms/tests/extract_field_test.rs`
- `rust/connect-transforms/tests/flatten_test.rs`
- `rust/connect-transforms/tests/header_from_test.rs`
- `rust/connect-transforms/tests/hoist_field_test.rs`
- `rust/connect-transforms/tests/insert_field_test.rs`
- `rust/connect-transforms/tests/insert_header_test.rs`
- `rust/connect-transforms/tests/mask_field_test.rs`
- `rust/connect-transforms/tests/replace_field_test.rs`
- `rust/connect-transforms/tests/set_schema_metadata_test.rs`
- `rust/connect-transforms/tests/timestamp_converter_test.rs`
- `rust/connect-transforms/tests/timestamp_router_test.rs`
- `rust/connect-transforms/tests/regex_router_test.rs`
- `rust/connect-transforms/tests/predicates_test.rs`

**测试覆盖**:
- 所有 26 个转换器
- 所有 4 个谓词
- 数据转换逻辑
- 错误处理

**验证**:
- `cd rust && cargo test -p connect-transforms`

---

## 阶段 6: connect-basic-auth-extension（Basic Auth 扩展）

### 任务 6.1: connect-basic-auth-extension - 创建 crate 结构

**目标**: 创建 connect-basic-auth-extension crate

**文件清单**:
- `rust/connect-basic-auth-extension/Cargo.toml`
- `rust/connect-basic-auth-extension/src/lib.rs`
- `rust/connect-basic-auth-extension/src/basic_auth_extension.rs`
- `rust/connect-basic-auth-extension/src/basic_auth_config.rs`
- `rust/connect-basic-auth-extension/src/jaas_config.rs`

**验证**:
- `cd rust && cargo check -p connect-basic-auth-extension`

---

### 任务 6.2: connect-basic-auth-extension - 实现 BasicAuthExtension

**目标**: 实现 BasicAuthExtension，对应 Java `org.apache.kafka.connect.rest.basic.auth.extension.BasicAuthSecurityRestExtension`

**Java 源文件**: `connect/basic-auth-extension/src/main/java/org/apache/kafka/connect/rest/basic/auth/extension/BasicAuthSecurityRestExtension.java`

**Rust 文件**: `rust/connect-basic-auth-extension/src/basic_auth_extension.rs`

**1:1 翻译清单**:
- `BasicAuthSecurityRestExtension` 结构及所有方法
- `JaasBasicAuthFilter` 结构及所有方法
- HTTP 认证中间件（actix-web）

**安全加固**:
- 密码哈希存储（使用 bcrypt）
- 速率限制（防止暴力破解）
- 安全的密码比较（防止时序攻击）
- 配置验证（防止弱密码）
- 日志脱敏（防止密码泄露）

**验证**:
- `cd rust && cargo check -p connect-basic-auth-extension`

---

### 任务 6.3: connect-basic-auth-extension - 实现 BasicAuthConfig

**目标**: 实现 BasicAuthConfig，对应 Java `org.apache.kafka.connect.rest.basic.auth.extension.BasicAuthConfig`

**Java 源文件**: `connect/basic-auth-extension/src/main/java/org/apache/kafka/connect/rest/basic/auth/extension/BasicAuthConfig.java`

**Rust 文件**: `rust/connect-basic-auth-extension/src/basic_auth_config.rs`

**1:1 翻译清单**:
- `BasicAuthConfig` 结构及所有方法

**安全加固**:
- 配置验证
- 敏感信息保护

**验证**:
- `cd rust && cargo check -p connect-basic-auth-extension`

---

### 任务 6.4: connect-basic-auth-extension - 实现 JAAS 配置支持

**目标**: 实现 JAAS 配置支持，对应 Java JAAS 配置

**Java 源文件**: `connect/basic-auth-extension/src/main/java/org/apache/kafka/connect/rest/basic/auth/extension/JaasContext.java`

**Rust 文件**: `rust/connect-basic-auth-extension/src/jaas_config.rs`

**1:1 翻译清单**:
- `JaasContext` 结构
- `JaasConfig` 结构
- JAAS 配置解析

**安全加固**:
- 安全的配置解析
- 路径遍历防护
- 文件权限检查

**验证**:
- `cd rust && cargo check -p connect-basic-auth-extension`

---

### 任务 6.5: connect-basic-auth-extension - 编写单元测试

**目标**: 编写 Basic Auth 扩展的单元测试

**文件清单**:
- `rust/connect-basic-auth-extension/tests/basic_auth_extension_test.rs`
- `rust/connect-basic-auth-extension/tests/basic_auth_config_test.rs`
- `rust/connect-basic-auth-extension/tests/jaas_config_test.rs`
- `rust/connect-basic-auth-extension/tests/security_test.rs`

**测试覆盖**:
- 认证逻辑
- 配置验证
- 错误处理
- 安全加固测试：
  - 密码哈希验证
  - 速率限制测试
  - 时序攻击防护测试
  - 弱密码检测测试
  - 日志脱敏测试

**验证**:
- `cd rust && cargo test -p connect-basic-auth-extension`

---

## 阶段 7: connect-runtime 核心组件（依赖 connect-api）

### 任务 7.1: connect-runtime - 创建 crate 结构

**目标**: 创建 connect-runtime crate 结构

**文件清单**:
- `rust/connect-runtime/Cargo.toml`
- `rust/connect-runtime/src/lib.rs`
- `rust/connect-runtime/src/cli.rs`
- `rust/connect-runtime/src/runtime.rs`
- `rust/connect-runtime/src/distributed.rs`
- `rust/connect-runtime/src/standalone.rs`
- `rust/connect-runtime/src/errors.rs`
- `rust/connect-runtime/src/health.rs`
- `rust/connect-runtime/src/isolation.rs`
- `rust/connect-runtime/src/rest.rs`
- `rust/connect-runtime/src/storage.rs`
- `rust/connect-runtime/src/util.rs`

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 7.2: connect-runtime - 实现错误处理和重试机制

**目标**: 实现错误处理和重试机制，对应 Java `org.apache.kafka.connect.runtime.errors.*`

**Java 源文件**:
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/errors/RetryWithToleranceOperator.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/errors/ToleranceType.java`
- `connect/runtime`/src/main/java/org/apache/kafka/connect/runtime/errors/ErrorReporter.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/errors/ProcessingContext.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/errors/Stage.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/errors/DeadLetterQueueReporter.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/errors/LogReporter.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/errors/WorkerErrantRecordReporter.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/errors/ErrorHandlingMetrics.java`

**Rust 文件**: `rust/connect-runtime/src/errors.rs`

**1:1 翻译清单**:
- `RetryWithToleranceOperator` 结构及所有方法（364 行）
- `ToleranceType` enum
- `ErrorReporter` trait
- `ProcessingContext` 结构
- `Stage` enum
- `DeadLetterQueueReporter` 结构
- `LogReporter` 结构
- `WorkerErrantRecordReporter` 结构
- `ErrorHandlingMetrics` 结构

**可靠性细节**:
- 重试逻辑（指数退避）
- 容错类型（NONE, ALL）
- 超时控制
- 错误报告
- 死信队列

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 7.3: connect-runtime - 实现配置类

**目标**: 实现配置类，对应 Java `org.apache.kafka.connect.runtime.*Config`

**Java 源文件**:
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerConfig.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ConnectorConfig.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/TaskConfig.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/SourceConnectorConfig.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/SinkConnectorConfig.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/DistributedConfig.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/standalone/StandaloneConfig.java`

**Rust 文件**: `rust/connect-runtime/src/config.rs`

**1:1 翻译清单**:
- `WorkerConfig` 结构及所有方法
- `ConnectorConfig` 结构及所有方法
- `TaskConfig` 结构
- `SourceConnectorConfig` 结构
- `SinkConnectorConfig` 结构
- `DistributedConfig` 结构
- `StandaloneConfig` 结构

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 7.4: connect-runtime - 实现插件系统

**目标**: 实现插件系统，对应 Java `org.apache.kafka.connect.runtime.isolation.*`

**Java 源文件**:
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/Plugins.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/PluginDesc.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/PluginType.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/PluginScanner.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/PluginScanResult.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/ReflectionScanner.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/ServiceLoaderScanner.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/PluginClassLoader.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/DelegatingClassLoader.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/ClassLoaderFactory.java`

**Rust 文件**: `rust/connect-runtime/src/isolation.rs`

**1:1 翻译清单**:
- `Plugins` 结构及所有方法
- `PluginDesc` 结构
- `PluginType` enum
- 插件注册表（lazy_static + HashMap）
- 插件发现和实例化
- ConnectRestExtension 动态加载支持

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 7.5: connect-runtime - 实现 BackingStore

**目标**: 实现 BackingStore，对应 Java `org.apache.kafka.connect.storage.*`

**Java 源文件**:
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/ConfigBackingStore.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/StatusBackingStore.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/OffsetBackingStore.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/MemoryConfigBackingStore.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/MemoryStatusBackingStore.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/MemoryOffsetBackingStore.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/KafkaConfigBackingStore.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/KafkaStatusBackingStore.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/KafkaOffsetBackingStore.java`
- `connect/runtime/src`/main/java/org/apache/kafka/connect/storage/ConnectorOffsetBackingStore.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/OffsetStorageWriter.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/OffsetStorageReaderImpl.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/ClusterConfigState.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/storage/AppliedConnectorConfig.java`

**Rust 文件**: `rust/connect-runtime/src/storage.rs`

**1:1 翻译清单**:
- `ConfigBackingStore` trait
- `StatusBackingStore` trait
- `OffsetBackingStore` trait
- `MemoryConfigBackingStore` 结构
- `MemoryStatusBackingStore` 结构
- `MemoryOffsetBackingStore` 结构
- `KafkaConfigBackingStore` 结构
- `KafkaStatusBackingStore` 结构
- `KafkaOffsetBackingStore` 结构
- `ConnectorOffsetBackingStore` 结构
- `OffsetStorageWriter` 结构
- `OffsetStorageReaderImpl` 结构
- `ClusterConfigState` 结构
- `AppliedConnectorConfig` 结构

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 7.6: connect-runtime - 实现工具类

**目标**: 实现工具类，对应 Java `org.apache.kafka.connect.util.*`

**Java 源文件**:
- `connect/runtime/src/main/java/org/apache/kafka/connect/util/TopicAdmin.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/util/KafkaBasedLog.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/util/SharedTopicAdmin`-.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/util/ConnectUtils.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/util/SinkUtils.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/util/RetryUtil.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/util/ConnectorTaskId.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/util/Callback.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/util/FutureCallback.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/util/ConvertingFutureCallback.java`

**Rust 文件**: `rust/connect-runtime/src/util.rs`

**1:1 翻译清单**:
- `TopicAdmin` 结构及所有方法
- `KafkaBasedLog` 结构
- `SharedTopicAdmin` 结构
- `ConnectUtils` 结构及所有静态方法
- `SinkUtils` 结构
- `RetryUtil` 结构
- `ConnectorTaskId` 结构
- `Callback` trait
- `FutureCallback` 结构
- `ConvertingFutureCallback` 结构

**可靠性细节**:
- Topic 创建和验证
- 重试和超时
- 回调和 Future 处理

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 7.7: connect-runtime - 编写单元测试

**目标**: 编写 connect-runtime 核心组件的单元测试

**文件清单**:
- `rust/connect-runtime/tests/errors_test.rs`
- `rust/connect-runtime/tests/config_test.rs`
- `rust/connect-runtime/tests/isolation_test.rs`
- `rust/connect-runtime/tests/storage_test.rs`
- `rust/connect-runtime/tests/util_test.rs`

**测试覆盖**:
- 重试和容错机制
- 配置验证
- 插件加载和注册
- BackingStore 操作
- 工具类功能

**验证**:
- `cd rust && cargo test -p connect-runtime`

---

## 阶段 8: connect-runtime Worker 和 Herder

### 任务 8.1: connect-runtime - 实现 Worker

**目标**: 实现 Worker，对应 Java `org.apache.kafka.connect.runtime.Worker`

**Java 源文件**: `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/Worker.java`

**Rust 文件**: `rust/connect-runtime/src/worker.rs`

**1:1 翻译清单**:
- `Worker` 结构及所有 56 个 public/protected 方法（2,415 行）
- `TaskBuilder` 及子类
- `SinkTaskBuilder`
- `SourceTaskBuilder`
- `ExactlyOnceSourceTaskBuilder`

**可靠性细节**:
- 并发安全（Arc<Mutex<>>）
- 连接器和任务生命周期管理
- Kafka 客户端创建和管理
- 指标收集
- 错误处理

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 8.2: connect-runtime - 实现 WorkerTask

**目标**: 实现 WorkerTask，对应 Java `org.apache.kafka.connect.runtime.WorkerTask`

**Java 源文件**: `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerTask.java`

**Rust 文件**: `rust/connect-runtime/src/worker_task.rs`

**1:1 翻译清单**:
- `WorkerTask` 结构及所有方法
- 超时控制
- 停止机制

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 8.3: connect-runtime - 实现 WorkerConnector

**目标**: 实现 WorkerConnector，对应 Java `org.apache.kafka.connect.runtime.WorkerConnector`

**Java 源文件**: `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerConnector.java`

**Rust 文件**: `rust/connect-runtime/src/worker_connector.rs`

**1:1 翻译清单**:
- `WorkerConnector` 结构及所有方法
- 连接器生命周期管理

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 8.4: connect-runtime - 实现 WorkerSinkTask

**目标**: 实现 WorkerSinkTask，对应 Java `org.apache.kafka.connect.runtime.WorkerSinkTask`

**Java 源文件**: `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSinkTask.java`

**Rust 文件**: `rust/connect-runtime/src/worker_sink_task.rs`

**1:1 翻译清单**:
- `WorkerSinkTask` 结构及所有方法（2,034 行）
- 消费和提交逻辑
- 转换链处理
- 错误处理

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 8.5: connect-runtime - 实现 WorkerSourceTask

**目标**: 实现 WorkerSourceTask，对应 Java `org.apache.kafka.connect.runtime.WorkerSourceTask`

**Java 源文件**:
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSourceTask.java`
- `connect/runtime/src/main`/java/org/apache/kafka/connect/runtime/AbstractWorkerSourceTask.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ExactlyOnceWorkerSourceTask.java`

**Rust 文件**: `rust/connect-runtime/src/worker_source_task.rs`

**1:1 翻译清单**:
- `WorkerSourceTask` 结构
- `AbstractWorkerSourceTask` 结构
- `ExactlyOnceWorkerSourceTask` 结构
- 所有 public/protected 方法

**可靠性细节**:
- 事务处理
- 偏移提交
- 错误重试
- Topic 自动创建

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 8.6: connect-runtime - 实现 AbstractHerder

**目标**: 实现 AbstractHerder，对应 Java `org.apache.kafka.connect.runtime.AbstractHerder`

**Java 源文件**: `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/AbstractHerder.java`

**Rust 文件**: `rust/connect-runtime/src/abstract_herder.rs`

**1:1 翻译清单**:
- `AbstractHerder` 结构及及所有方法
- 连接器和任务管理
- 状态跟踪

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 8.7: connect-runtime - 实现 StandaloneHerder

**目标**: 实现 StandaloneHerder，对应 Java `org.apache.kafka.connect.runtime.standalone.StandaloneHerder`

**Java 源文件**: `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/standalone/StandaloneHerder.java`

**Rust 文件**: `rust/connect-runtime/src/standalone_herder.rs`

**1:1 翻译清单**:
- `StandaloneHerder` 结构及所有方法
- 内存配置存储
- 文件偏移存储

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 8.8: connect-runtime - 编写单元测试

**目标**: 编写 Worker 和 Herder 的单元测试

**文件清单**:
- `rust/connect-runtime/tests/worker_test.rs`
- `rust/connect-runtime/tests/worker_task_test.rs`
- `rust/connect-runtime/tests/worker_connector_test.rs`
- `rust/connect-runtime/tests/worker_sink_task_test.rs`
- `rust`/connect-runtime/tests/worker_source_task_test.rs`
- `rust/connect-runtime/tests/abstract_herder_test.rs`
- `rust/connect-runtime/tests/standalone_herder_test.rs`

**测试覆盖**:
- Worker 生命周期
- 任务启动和停止
- 连接器管理
- 数据处理流程
- 错误处理

**验证**:
- `cd rust && cargo test -p connect-runtime`

---

## 阶段 9: connect-runtime 分布式组件

### 任务 9.1: connect-runtime - 实现 DistributedHerder

**目标**: 实现 DistributedHerder，对应 Java `org.apache.kafka.connect.runtime.distributed.DistributedHerder`

**Java 源文件**: `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/DistributedHerder.java`

**Rust 文件**: `rust/connect-runtime/src/distributed_herder.rs`

**1:1 翻译清单**:
- `DistributedHerder` 结构及所有 40+ 个 public/protected 方法（~2,000 行）
- 集群协调
- 领导选举
- 配置同步

**可靠性细节**:
- 分布式协调
- 故障和恢复
- 配置一致性
- 重平衡

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 9.2: connect-runtime - 实现 WorkerCoordinator

**目标**: 实现 WorkerCoordinator，对应 Java `org.apache.kafka.connect.runtime.distributed.WorkerCoordinator`

**Java 源文件**: `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/WorkerCoordinator.java`

**Rust 文件**: `rust/connect-runtime/src/worker_coordinator.rs`

**1:1 翻译清单**:
- `WorkerCoordinator` 结构及所有方法
- 消费者组协调
- 重平衡监听

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 9.3: connect-runtime - 实现 ConnectAssignor

**目标**: 实现 ConnectAssignor，对应 Java `org.apache.kafka.connect.runtime.distributed.ConnectAssignor`

**Java 源文件**:
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/ConnectAssignor.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/EagerAssignor.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/IncrementalCooperativeAssignor.java`

**Rust 文件**: `rust/connect-runtime/src/connect_assignor.rs`

**1:1 翻译清单**:
- `ConnectAssignor` trait
- `EagerAssignor` 结构
- `IncrementalCooperativeAssignor` 结构
- 任务分配算法

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 9.4: connect-runtime - 实现分布式协议

**目标**: 实现分布式协议，对应 Java `org.apache.kafka.connect.runtime.distributed.*`

**Java 源文件**:
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/ConnectProtocol.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/ConnectProtocolCompatibility.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/WorkerGroupMember.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/WorkerRebalanceListener.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/ExtendedAssignment.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/ExtendedWorkerState.java`

**Rust 文件**: `rust/connect-runtime/src/distributed_protocol.rs`

**1:1 翻译清单**:
- `ConnectProtocol` 结构
- `ConnectProtocolCompatibility` enum
- `WorkerGroupMember` 结构
- `WorkerRebalanceListener` trait
- `ExtendedAssignment` 结构
- `ExtendedWorkerState` 结构

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 9.5: connect-runtime - 编写单元测试

**目标**: 编写分布式组件的单元测试

**文件清单**:
- `rust/connect-runtime/tests/distributed_herder_test.rs`
- `rust/connect-runtime/tests/worker_coordinator_test.rs`
- `rust/connect-runtime/tests/connect_assignor_test.rs`
- `rust/connect-runtime/tests/distributed_protocol_test.rs`

**测试覆盖**:
- 分布式协调
- 任务分配
- 重平衡
- 协议兼容性

**验证**:
- `cd rust && cargo test -p connect-runtime`

---

## 阶段 10: connect-runtime REST API

### 任务 10.1: connect-runtime - 实现 RestServer

**目标**: 实现 RestServer，对应 Java `org.apache.kafka.connect.runtime.rest.RestServer`

**Java 源文件**:
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/RestServer.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/ConnectRestServer.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/RestServerConfig.java`

**Rust 文件**: `rust/connect-runtime/src/rest_server.rs`

**1:1 翻译清单**:
- `RestServer` 结构
- `ConnectRestServer` 结构
- `RestServerConfig` 结构
- HTTP 服务器管理（actix-web）
- ConnectRestExtension 动态加载和注册

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 10.2: connect-runtime - 实现 REST 实体类

**目标**: 实现 REST 实体类，对应 Java `org.apache.kafka.connect.runtime.rest.entities.*`

**Java 源文件**:
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ConnectorInfo.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ConnectorStateInfo.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ConnectorOffsets.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ConnectorType.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/CreateConnectorRequest.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ConfigInfo.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ConfigInfos.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/Config`KeyInfo.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ConfigValueInfo.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/TaskInfo.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/PluginInfo.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ServerInfo.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/WorkerStatus.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ActiveTopicsInfo.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ErrorMessage.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/LoggerLevel.java`

**Rust 文件**: `rust/connect-runtime/src/rest_entities.rs`

**1:1 翻译清单**:
- 所有 REST 实体结构
- JSON 序列化支持（serde_json）

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 10.3: connect-runtime - 实现 REST Resources

**目标**: 实现 REST Resources，对应 Java `org.apache.kafka.connect.runtime.rest.resources.*`

**Java 源文件**:
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/resources/RootResource.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/resources/ConnectorsResource.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/resources/ConnectorPluginsResource.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/resources/InternalConnectResource.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/resources/InternalClusterResource.java`
- `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/resources/LoggingResource.java`

**Rust 文件**: `rust/connect-runtime/src/rest_resources.rs`

**1:1 翻译清单**:
- `RootResource` 及所有端点
- `ConnectorsResource` 及所有端点
- `ConnectorPluginsResource` 及所有端点
- `InternalConnectResource` 及所有端点
- `InternalClusterResource` 及所有端点
- `LoggingResource` 及所有端点

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 10.4: connect-runtime - 实现 RestClient

**目标**: 实现 RestClient，对应 Java `org.apache.kafka.connect.runtime.rest.RestClient`

**Java 源文件**: `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/RestClient.java`

**Rust 文件**: `rust/connect-runtime/src/rest_client.rs`

**1:1 翻译清单**:
- `RestClient` 结构及所有方法
- HTTP 客户端（reqwest）
- SSL 支持
- 请求签名验证

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 10.5: connect-runtime - 编写单元测试

**目标**: 编写 REST API 的单元测试

**文件清单**:
- `rust/connect-runtime/tests/rest_server_test.rs`
- `rust/connect-runtime/tests/rest_entities_test.rs`
- `rust/connect-runtime/tests/rest_resources_test.rs`
- `rust/connect-runtime/tests/rest_client_test.rs`

**测试覆盖**:
- REST 端点
- 实体序列化
- 错误处理

**验证**:
- `cd rust && cargo test -p connect-runtime`

---

## 阶段 11: CLI 入口点

### 任务 11.1: connect-runtime - 实现 ConnectStandalone

**目标**: 实现 ConnectStandalone，对应 Java `org.apache.kafka.connect.cli.ConnectStandalone`

**Java 源文件**: `connect/runtime/src/main/java/org/apache/kafka/connect/cli/ConnectStandalone.java`

**Rust 文件**: `rust/connect-runtime/src/connect_standalone.rs`

**1:1 翻译清单**:
- `ConnectStandalone` 结构及所有方法
- 命令行参数解析
- 启动和停止

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 11.2: connect-runtime - 实现 ConnectDistributed

**目标**: 实现 ConnectDistributed，对应 Java `org.apache.kafka.connect.cli.ConnectDistributed`

**Java 源文件**: `connect/runtime/src/main/java/org/apache/kafka/connect/cli/ConnectDistributed.java`

**Rust 文件**: `rust/connect-runtime/src/connect_distributed.rs`

**1:1 翻译清单**:
- `ConnectDistributed` 结构及所有方法
- 命令行参数解析
- 启动和停止

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 11.3: connect-runtime - 编写单元测试

**目标**: 编写 CLI 的单元测试

**文件清单**:
- `rust/connect-runtime/tests/connect_standalone_test.rs`
- `rust/connect-runtime/tests/connect_distributed_test.rs`

**测试覆盖**:
- 命令行参数解析
- 启动和停止流程

**验证**:
- `cd rust && cargo test -p connect-runtime`

---

## 阶段 12: connect-mirror-client（MirrorMaker2 客户端）

### 任务 12.1: connect-mirror-client - 创建 crate 结构

**目标**: 创建 connect-mirror-client crate

**文件清单**:
- `rust/connect-mirror-client/Cargo.toml`
- `rust/connect-mirror-client/src/lib.rs`
- `rust/connect-mirror-client/src/mirror_client.rs`
- `rust/connect-mirror-client/src/mirror_client_config.rs`
- `rust/connect-mirror-client/src/replication_policy.rs`
- `rust/connect-mirror-client/src/default_replication_policy.rs`
- `rust/connect-mirror-client/src/identity_replication_policy.rs`
- `rust/connect-mirror-client/src/remote_cluster_utils.rs`
- `rust/connect-mirror-client/src/source_and_target.rs`

**验证**:
- `cd rust && cargo check -p connect-mirror-client`

---

### 任务 12.2: connect-mirror-client - 实现 MirrorClient

**目标**: 实现 MirrorClient，对应 Java `org.apache.kafka.connect.mirror.MirrorClient`

**Java 源文件**: `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/MirrorClient.java`

**Rust 文件**: `rust/connect-mirror-client/src/mirror_client.rs`

**1:1 翻译清单**:
- `MirrorClient` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-mirror-client`

---

### 任务 12.3: connect-mirror-client - 实现 MirrorClientConfig

**目标**: 实现 MirrorClientConfig，对应 Java `org.apache.kafka.connect.mirror.MirrorClientConfig`

**Java 源文件**: `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/MirrorClientConfig.java`

**Rust 文件**: `rust/connect-mirror-client/src/mirror_client_config.rs`

**1:1 翻译清单**:
- `MirrorClientConfig` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-mirror-client`

---

### 任务 12.4: connect-mirror-client - 实现 ReplicationPolicy

**目标**: 实现 ReplicationPolicy，对应 Java `org.apache.kafka.connect.mirror.ReplicationPolicy`

**Java 源文件**: `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/ReplicationPolicy.java`

**Rust 文件**: `rust/connect-mirror-client/src/replication_policy.rs`

**1:1 翻译清单**:
- `ReplicationPolicy` trait

**验证**:
- `cd rust && cargo check -p connect-mirror-client`

---

### 任务 12.5: connect-mirror-client - 实现 DefaultReplicationPolicy

**目标**: 实现 DefaultReplicationPolicy，对应 Java `org.apache.kafka.connect.mirror.DefaultReplicationPolicy`

**Java 源文件**: `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/DefaultReplicationPolicy.java`

**Rust 文件**: `rust/connect-mirror-client/src/default_replication_policy.rs`

**1:1 翻译清单**:
- `DefaultReplicationPolicy` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-mirror-client`

---

### 任务 12.6: connect-mirror-client - 实现 IdentityReplicationPolicy

**目标**: 实现 IdentityReplicationPolicy，对应 Java `org.apache.kafka.connect.mirror.IdentityReplicationPolicy`

**Java 源文件**: `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/IdentityReplicationPolicy.java`

**Rust 文件**: `rust/connect-mirror-client/src/identity_replication_policy.rs`

**1:1 翻译清单**:
- `IdentityReplicationPolicy` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-mirror-client`

---

### 任务 12.7: connect-mirror-client - 实现 RemoteClusterUtils

**目标**: 实现 RemoteClusterUtils，对应 Java `org.apache.kafka.connect.mirror.RemoteClusterUtils`

**Java 源文件**: `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/RemoteClusterUtils.java`

**Rust 文件**: `rust/connect-mirror-client/src/remote_cluster_utils.rs`

**1:1 翻译清单**:
- `RemoteClusterUtils` 结构及所有方法

**验证**:
- `cd rust && cargo check -p` connect-mirror-client`

---

### 任务 12.8: connect-mirror-client - 实现 SourceAndTarget

**目标**: 实现 SourceAndTarget，对应 Java `org.apache.kafka.connect.mirror.SourceAndTarget`

**Java 源文件**: `connect/mirror-client/src/main/java/org/apache/kafka/connect/mirror/SourceAndTarget.java`

**Rust 文件**: `rust/connect-mirror-client/src/source_and_target.rs`

**1:1 翻译清单**:
- `SourceAndTarget` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-mirror-client`

---

### 任务 12.9: connect-mirror-client - 编写单元测试

**目标**: 编写 MirrorMaker2 客户端的单元测试

**文件清单**:
- `rust/connect-mirror-client/tests/mirror_client_test.rs`
- `rust/connect-mirror-client/tests/mirror_client_config_test.rs`
- `rust/connect-mirror-client/tests/replication_policy_test.rs`
- `rust/connect-mirror-client/tests/default_replication_policy_test.rs`
- `rust/connect-mirror-client/tests/identity_replication_policy_test.rs`
- `rust/connect-mirror-client/tests/remote_cluster_utils_test.rs`
- `rust/connect-mirror-client/tests/source_and_target_test.rs`

**测试覆盖**:
- 所有 10 个文件
- 复制策略
- Topic 命名规则
- 错误处理

**验证**:
- `cd rust && cargo test -p connect-mirror-client`

---

## 阶段 13: connect-mirror（MirrorMaker2 连接器）

### 任务 13.1: connect-mirror - 创建 crate 结构

**目标**: 创建 connect-mirror crate

**文件清单**:
- `rust/connect-mirror/Cargo.toml`
- `rust/connect-mirror/src/lib.rs`
- `rust/connect-mirror/src/checkpoint.rs`
- `rust/connect-mirror/src/checkpoint_store.rs`
- `rust/connect-mirror/src/heartbeat.rs`
- `rust/connect-mirror/src/mirror_checkpoint_config.rs`
- `rust/connect-mirror/src/mirror_checkpoint_connector.rs`
- `rust/connect-mirror/src/mirror_checkpoint_task.rs`
- `rust/connect-mirror/src/mirror_checkpoint_metrics.rs`
- `rust/connect-mirror/src/mirror_connector_config.rs`
- `rust/connect-mirror/src/mirror_heartbeat_config.rs`
- `rust/connect-mirror/src/mirror_heartbeat_connector.rs`
- `rust/connect-mirror/src/mirror_heartbeat_task.rs`
- `rust/connect-mirror/src/mirror_maker.rs`
- `rust/connect-mirror/src/mirror_maker_config.rs`
- `rust/connect-mirror/src/mirror_source_config.rs`
- `rust/connect-mirror/src/mirror_source_connector.rs`
- `rust/connect-mirror/src/mirror_source_metrics.rs`
- `rust/connect-mirror/src/mirror_source_task.rs`
- `rust/connect-mirror/src/mirror_source_task_config.rs`
- `rust/connect-mirror/src/mirror_utils.rs`
- `rust/connect-mirror/src/offset_sync.rs`
- `rust/connect-mirror/src/offset_sync_store.rs`
- `rust/connect-mirror/src/offset_sync_writer.rs`

**验证**:
- `cd rust && cargo check -p connect-mirror`

---

### 任务 13.2: connect-mirror - 实现 Checkpoint 相关

**目标**: 实现 Checkpoint 相关类

**Java 源文件**:
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/Checkpoint.java`
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/CheckpointStore.java`

**Rust 文件**:
- `rust/connect-mirror/src/checkpoint.rs`
- `rust/connect-mirror/src/checkpoint_store.rs`

**1:1 翻译清单**:
- `Checkpoint` 结构
- `CheckpointStore` trait

**验证**:
- `cd rust && cargo check -p connect-mirror`

---

### 任务 13.3: connect-mirror - 实现 Heartbeat 相关

**目标**: 实现 Heartbeat 相关类

**Java 源文件**:
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/Heartbeat.java`

**Rust 文件**: `rust/connect-mirror/src/heartbeat.rs`

**1:1 翻译清单**:
- `Heartbeat` 结构

**验证**:
- `cd rust && cargo check -p connect-mirror`

---

### 任务 13.4: connect-mirror - 实现 MirrorCheckpointConnector/Task

**目标**: 实现 MirrorCheckpointConnector 和 MirrorCheckpointTask

**Java 源文件**:
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorCheckpointConnector.java`
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorCheckpointTask.java`

**Rust 文件**:
- `rust/connect-mirror/src/mirror_checkpoint_connector.rs`
- `rust/connect-mirror/src/mirror_checkpoint_task.rs`

**1:1 翻译清单**:
- `MirrorCheckpointConnector` 结构及所有方法
- `MirrorCheckpointTask` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-mirror`

---

### 任务 13.5: connect-mirror - 实现 MirrorHeartbeatConnector/Task

**目标**: 实现 MirrorHeartbeatConnector 和 MirrorHeartbeatTask

**Java 源文件**:
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorHeartbeatConnector.java`
- `connect/mirror/src/main/java/org`/apache/kafka/connect/mirror/MirrorHeartbeatTask.java`

**Rust 文件**:
- `rust/connect-mirror/src/mirror_heartbeat_connector.rs`
- `rust/connect-mirror/src/mirror_heartbeat_task.rs`

**1:1 翻译清单**:
- `MirrorHeartbeatConnector` 结构及所有方法
- `MirrorHeartbeatTask` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-mirror`

---

### 任务 13.6: connect-mirror - 实现 MirrorSourceConnector/Task

**目标**: 实现 MirrorSourceConnector 和 MirrorSourceTask

**Java 源文件**:
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceConnector.java`
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java`

**Rust 文件**:
- `rust/connect-mirror/src/mirror_source_connector.rs`
- `rust/connect-mirror/src/mirror_source_connector_task.rs`

**`1:1 翻译清单**:
- `MirrorSourceConnector` 结构及所有方法
- `MirrorSourceTask` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-mirror`

---

### 任务 13.7: connect-mirror - 实现 MirrorMaker

**目标**: 实现 MirrorMaker，对应 Java `org.apache.kafka.connect.mirror.MirrorMaker`

**Java 源文件**:
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorMaker.java`

**Rust 文件**: `rust/connect-mirror/src/mirror_maker.rs`

**1:1 翻译清单**:
- `MirrorMaker` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-mirror`

---

### 任务 13.8: connect-mirror - 实现 MirrorMakerConfig

**目标**: 实现 MirrorMakerConfig，对应 Java `org.apache.kafka.connect.mirror.MirrorMakerConfig`

**Java 源文件**:
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorMakerConfig.java`

**Rust 文件**: `rust/connect-mirror/src/mirror_maker_config.rs`

**1:1 翻译清单**:
- `MirrorMakerConfig` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-mirror`

---

### 任务 13.9: connect-mirror - 实现 MirrorSourceConfig

**目标**: 实现 MirrorSourceConfig，对应 Java `org.apache.kafka.connect.mirror.MirrorSourceConfig`

**Java 源文件**:
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceConfig.java`

**Rust 文件**: `rust/connect-mirror/src/mirror_source_config.rs`

**1:1 翻译清单**:
- `MirrorSourceConfig` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-mirror`

---

### 任务 13.10: connect-mirror - 实现 OffsetSync 相关

**目标**: 实现 OffsetSync 相关类

**Java 源文件**:
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/OffsetSync.java`
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/OffsetSyncStore.java`
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/OffsetSyncWriter.java`

**Rust 文件**:
- `rust/connect-mirror/src/offset_sync.rs`
- `rust/connect-mirror/src/offset_sync_store.rs`
- `rust/connect-mirror/src/offset_sync_writer.rs`

**1:1 翻译清单**:
- `OffsetSync` 结构
- `OffsetSyncStore` trait
- `OffsetSyncWriter` 结构

**验证**:
- `cd rust && cargo check -p connect-mirror`

---

### 任务 13.11: connect-mirror - 编写单元测试

**目标**: 编写 MirrorMaker2 连接器的单元测试

**文件清单**:
- `rust/connect-mirror/tests/checkpoint_test.rs`
- `rust/connect-mirror/tests/heartbeat_test.rs`
- `rust/connect-mirror/tests/mirror_checkpoint_connector_test.rs`
- `rust/connect-mirror/tests/mirror_checkpoint_task_test.rs`
- `rust/connect-mirror/tests/mirror_heartbeat_connector_test.rs`
- `rust/connect-mirror/tests/mirror_heartbeat_task_test.rs`
- `rust/connect-mirror/tests/mirror_source_connector_test.rs`
- `rust/connect-mirror/tests/mirror_source_task_test.rs`
- `rust/connect-mirror/tests/mirror_maker_test.rs`
-`rust/connect-mirror/tests/mirror_maker_config_test.rs`
- `rust/connect-mirror/tests/mirror_source_config_test.rs`
- `rust/connect-mirror/tests/offset_sync_test.rs`

**测试覆盖**:
- 所有 34 个文件
- Checkpoint 管理
- Heartbeat 机制
- Offset 同步
- Topic 复制
- 错误处理

**验证**:
- `cd rust && cargo test -p connect-mirror`

---

## 阶段 14: connect-file（文件连接器）

### 任务 14.1: connect-file - 创建 crate 结构

**目标**: 创建 connect-file crate

**文件清单**:
- `rust/connect-file/Cargo.toml`
- `rust/connect-file/src/lib.rs`
- `rust/connect-file/src/file_stream_sink_connector.rs`
- `rust/connect-file/src/file_stream_sink_task.rs`
- `rust/connect-file/src/file_stream_source_connector.rs`
- `rust/connect-file/src/file_stream_source_task.rs`

**验证**:
- `cd rust && cargo check -p connect-file`

---

### 任务 14.2: connect-file - 实现 FileStreamSinkConnector/Task

**目标**: 实现 FileStreamSinkConnector 和 FileStreamSinkTask

**Java 源文件**:
- `connect/file/src/main/java/org/apache/kafka/connect/file/FileStreamSinkConnector.java`
- `connect/file/src/main/java/org/apache/kafka/connect/file/FileStreamSinkTask.java`

**Rust 文件**:
- `rust/connect-file/src/file_stream_sink_connector.rs`
- `rust/connect-file/src/file_stream_sink_task.rs`

**1:1 翻译清单**:
- `FileStreamSinkConnector` 结构及所有方法
- `FileStreamSinkTask` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-file`

---

### 任务 14.3: connect-file - 实现 FileStreamSourceConnector/Task

**目标**: 实现 FileStreamSourceConnector 和 FileStreamSourceTask

**Java 源文件**:
- `connect/file/src/main/java/org/apache/kafka/connect/file/FileStreamSourceConnector.java`
- `connect/file/src/main/java/org/apache/kafka/connect/file/FileStreamSourceTask.java`

**Rust 文件**:
- `rust/connect-file/src/file_stream_source_connector.rs`
- `rust/connect-file/src/file_stream_source_task.rs`

**1:1 翻译清单**:
- `FileStreamSourceConnector` 结构及所有方法
- `FileStreamSourceTask` 结构及所有方法

**验证**:
- `cd rust && cargo check -p connect-file`

---

### 任务 14.4: connect-file - 编写单元测试

**`目标**: 编写文件连接器的单元测试

**文件清单**:
- `rust/connect-file/tests/file_stream_sink_connector_test.rs`
- `rust/connect-file/tests/file_stream_sink_task_test.rs`
- `rust/connect-file/tests/file_stream_source_connector_test.rs`
- `rust/connect-file/tests/file_stream_source_task_test.rs`

**测试覆盖**:
- 所有 4 个文件连接器
- 文件读写逻辑
- 错误处理

**验证**:
- `cd rust && cargo test -p connect-file`

---

## 阶段 15: 集成测试和端到端测试

### 任务 15.1: connect-runtime - 实现集成测试框架

**目标**: 实现集成测试框架，对应 Java `org.apache.kafka.connect.util.clusters.*`

**Java 源文件**:
- `connect/runtime/src/test/java/org/apache/kafka/connect/util/clusters/EmbeddedConnect.java`
- `connect/runtime/src/test/java/org/apache/kafka/connect/util/clusters/EmbeddedConnectCluster.java`
- `connect/runtime/src/test/java/org/apache/kafka/connect/util/clusters/EmbeddedKafkaCluster.java`
- `connect/runtime/src/test/java/org/apache/kafka/connect/util/clusters/EmbeddedConnectStandalone.java`

**Rust 文件**: `rust/connect-runtime/tests/integration/embedded_connect.rs`

**1:1 翻译清单**:
- `EmbeddedConnect` 结构
- `EmbeddedConnectCluster` 结构
- `EmbeddedKafkaCluster` 结构
- `EmbeddedConnectStandalone` 结构

**验证**:
- `cd rust && cargo check -p connect-runtime`

---

### 任务 15.2: connect-runtime - 迁移原始集成测试

**目标**: 迁移所有原始集成测试（201 个测试文件）

**Java 测试文件**:
- `connect/runtime/src/test/java/org/apache/kafka/connect/integration/*.java`
- `connect/api/src/test/java/org/apache/kafka/connect/*.java`

**Rust 文件**: `rust/connect-runtime/tests/integration/*.rs`

**测试覆盖**:
- 所有原始集成测试
- 使用 Mock Kafka 集群
- 使用测试插件

**验证**:
- `cd rust && cargo test -p connect-runtime`

---

### 任务 15.3: connect-runtime - 实现 MM2 端到端测试（无认证）

**目标**: 实现 MM2 端到端测试（无认证场景），对应 Java `org.apache.kafka.connect.mirror.integration.*`

**Java 测试文件**:
- `connect/mirror/src/test/java/org/apache/kafka/connect/mirror/integration/MirrorConnectorsIntegrationBaseTest.java`
- `connect/mirror/src/test/java/org/apache/kafka/connect/mirror/integration/IdentityReplicationIntegrationTest.java`

**Rust 文件**: `rust/connect-runtime/tests/e2e/mm2_test.rs`

**测试覆盖**:
- MM2 集群间镜像复制
- 故障转移和恢复
- 偏移同步

**验证**:
- `cd rust && cargo test -p connect-runtime`

---

### 任务 15.4: connect-runtime - 实现 MM2 端到端测试（带 Basic Auth）

**目标**: 实现 MM2 端到端测试（带 Basic Auth 认证场景）

**Rust 文件**: `rust/connect-runtime/tests/e2e/mm2_auth_test.rs`

**测试覆盖**:
- MM2 集群间镜像复制（带认证）
- Basic Auth 认证流程
- 认证失败场景
- 速率限制测试
- 时序攻击防护测试
- 弱密码检测测试
- 日志脱敏测试

**测试策略**:
1. 启动 Mock Kafka 集群
2. 配置 MirrorMaker2 启用 BasicAuthSecurityRestExtension
3. 配置用户名和密码（使用 bcrypt 哈希）
4. 测试正确的认证流程
5. 测试错误的认证流程
6. 测试速率限制
7. 测试弱密码拒绝
8. 验证日志中密码已脱敏

**验证**:
- `cd rust && cargo test -p connect-runtime`

---

### 任务 15.5: connect-runtime - 实现 MM2 端到端测试（带 SSL）

**目标**: 实现 MM2 端到端测试（带 SSL 场景），对应 Java `org.apache.kafka.connect.mirror.integration.*`

**Java 测试文件**:
- `connect/mirror/src/test/java/org/apache/kafka/connect/mirror/integration/MirrorConnectorsIntegrationSSLTest.java`

**Rust 文件**: `rust/connect-runtime/tests/e2e/mm2_ssl_test.rs`

**测试覆盖**:
- MM2 集群间镜像复制（带 SSL）
- SSL/TLS 配置
- 证书验证
- 加密和认证

**验证**:
- `cd rust && cargo test -p connect-runtime`

---

### 任务 15.6: connect-runtime - 实现 MM2 端到端测试（带事务）

**目标**: 实现 MM2 端到端测试（带事务场景），对应 Java `org.apache.kafka.connect.mirror.integration.*`

**Java 测试文件**:
- `connect/mirror/src/test/java/org/apache/kafka/connect/mirror/integration/MirrorConnectorsIntegrationTransactionsTest.java`

**Rust 文件**: `rust/connect-runtime/tests/e2e/mm2_transactions_test.rs`

**测试覆盖**:
- MM2 集群间镜像复制（带事务）
- Exactly-Once 语义
- 事务提交和回滚
- 偏移同步（带事务）

**验证**:
- `cd rust && cargo test -p connect-runtime`

---

### 任务 15.7: connect-runtime - 补充额外单元测试

**目标**: 为 connect-runtime 补充额外的单元测试

**文件清单**:
- `rust/connect-runtime/tests/worker_integration_test.rs`
- `rust/connect-runtime/tests/herder_integration_test.rs`
- `rust/connect-runtime/tests/distributed_integration_test.rs`
- `rust/connect-runtime/tests/rest_integration_test.rs`

**测试覆盖**:
- Worker 集成测试
- Herder 集成测试
- 分布式协调测试
- REST API 集成测试
- 错误场景测试
- 边界条件测试

**验证**:
- `cd rust && cargo test -p connect-runtime`

---

### 任务 15.8: connect-mirror - 补充额外单元测试

**目标**: 为 connect-mirror 补充额外的单元测试

**文件清单**:
- `rust/connect-mirror/tests/checkpoint_integration_test.rs`
- `rust/connect-mirror/tests/heartbeat_integration_test.rs`
- `rust/connect-mirror/tests/offset_sync_integration_test.rs`
- `rust/connect-mirror/tests/mirror_integration_test.rs`

**测试覆盖**:
- Checkpoint 集成测试
- Heartbeat 集成测试
- Offset 同步集成测试
- Mirror 集成测试
- 错误场景测试
- 边界条件测试

**验证**:
- `cd rust && cargo test -p connect-mirror`

---

## 总结

**总任务数**: 200+ 个任务

**验证标准**:
1. 每个任务完成后 `cd rust && cargo check` 通过
2. 每个阶段完成后 `cd rust && cargo test` 通过
3. 所有代码 1:1 翻译，无遗漏
4. 所有可靠性细节完整迁移
5. 所有原始测试迁移
6. 额外测试覆盖

**成功标准**:
- 所有模块编译通过
- 所有测试通过
- 功能与 Java 版本 100% 对等
- 性能不低于 Java 版本的 80%

**依赖关系**:
- 阶段 1（kafka-clients-mock）：无依赖
- 阶段 2（connect-api）：无依赖
- 阶段 3（connect-json）：依赖阶段 2
- 阶段 4（connect-test-plugins）：依赖阶段 2
- 阶段 5（connect-transforms）：依赖阶段 2
- 阶段 6（connect-basic-auth-extension）：依赖阶段 2
- 阶段 7-11（connect-runtime）：依赖阶段 2, 3, 5, 6
- 阶段 12（connect-mirror-client）：依赖阶段 2
- 阶段 13（connect-mirror）：依赖阶段 7-11, 12
- 阶段 14（connect-file）：依赖阶段 2
- 阶段 15（集成测试）：依赖所有之前阶段

**安全加固说明**:
- connect-basic-auth-extension 在阶段 6 实现，包含完整的安全加固
- MM2 端到端测试在阶段 15 分为多个子任务：
  - 任务 15.3：无认证测试
  - 任务 15.4：带 Basic Auth 认证测试（验证安全加固）
  - 任务 15.5：带 SSL 测试
  - 任务 15.6：带事务测试
- 这种设计确保安全加固在集成测试之前完成，并通过专门的测试验证
