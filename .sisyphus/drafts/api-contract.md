# Kafka Connect模块API接口契约文档

> **生成时间**: 2026-03-29
> **用途**: 阶段0的输出，用于阶段1生成所有crate的API骨架

---

## 一、模块依赖关系总览

```
connect/api (核心接口层)
  ↓ 被依赖
connect/runtime (运行时核心层)
  ↓ 被依赖
connect/transforms (数据转换层)
connect/mirror (镜像复制层)
connect/mirror-client (镜像客户端层)
connect/basic-auth-extension (认证扩展层)
```

---

## 二、connect/api模块 - 核心接口契约

### 2.1 Connector相关接口

#### Connector (抽象类)
```rust
// Java: public abstract class Connector implements Versioned
pub trait Connector: Versioned {
    fn initialize(&mut self, ctx: Box<dyn ConnectorContext>);
    fn initialize_with_task_configs(&mut self, ctx: Box<dyn ConnectorContext>, task_configs: Vec<HashMap<String, String>>);
    fn context(&self) -> Option<Box<dyn ConnectorContext>>;
    fn start(&mut self, props: HashMap<String, String>);
    fn reconfigure(&mut self, props: HashMap<String, String>);
    fn task_class(&self) -> Box<dyn Any>;
    fn task_configs(&self, max_tasks: i32) -> Vec<HashMap<String, String>>;
    fn stop(&mut self);
    fn validate(&self, connector_configs: HashMap<String, String>) -> Config;
    fn config(&self) -> ConfigDef;
}
```

#### SourceConnector (抽象类)
```rust
// Java: public abstract class SourceConnector extends Connector
pub trait SourceConnector: Connector {
    fn exactly_once_support(&self, connector_config: HashMap<String, String>) -> ExactlyOnceSupport;
    fn can_define_transaction_boundaries(&self, connector_config: HashMap<String, String>) -> ConnectorTransactionBoundaries;
    fn alter_offsets(&self, connector_config: HashMap<String, String>, offsets: HashMap<HashMap<String, Box<dyn Any>>, HashMap<String, Box<dyn Any>>>) -> bool;
}
```

#### SinkConnector (抽象类)
```rust
// Java: public abstract class SinkConnector extends Connector
pub trait SinkConnector: Connector {
    fn alter_offsets(&self, connector_config: HashMap<String, String>, offsets: HashMap<TopicPartition, i64>) -> bool;
}
```

### 2.2 Task相关接口

#### Task (接口)
```rust
// Java: public interface Task
pub trait Task: Versioned {
    fn start(&mut self, props: HashMap<String, String>);
    fn stop(&mut self);
}
```

#### SourceTask (抽象类)
```rust
// Java: public abstract class SourceTask implements Task
pub trait SourceTask: Task {
    fn initialize(&mut self, context: Box<dyn SourceTaskContext>);
    fn start(&mut self, props: HashMap<String, String>);
    fn poll(&mut self) -> Result<Vec<SourceRecord>, Box<dyn Error>>;
    fn commit(&mut self) -> Result<(), Box<dyn Error>>;
    fn stop(&mut self);
    fn commit_record(&mut self, record: SourceRecord, metadata: RecordMetadata) -> Result<(), Box<dyn Error>>;
}
```

#### SinkTask (抽象类)
```rust
// Java: public abstract class SinkTask implements Task
pub trait SinkTask: Task {
    fn initialize(&mut self, context: Box<dyn SinkTaskContext>);
    fn start(&mut self, props: HashMap<String, String>);
    fn put(&mut self, records: Vec<SinkRecord>) -> Result<(), Box<dyn Error>>;
    fn flush(&mut self, current_offsets: HashMap<TopicPartition, OffsetAndMetadata>) -> Result<(), Box<dyn Error>>;
    fn pre_commit(&mut self, current_offsets: HashMap<TopicPartition, OffsetAndMetadata>) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Box<dyn Error>>;
    fn open(&mut self, partitions: Vec<TopicPartition>) -> Result<(), Box<dyn Error>>;
    fn close(&mut self, partitions: Vec<TopicPartition>) -> Result<(), Box<dyn Error>>;
    fn stop(&mut self);
}
```

### 2.3 数据转换接口

#### Converter (接口)
```rust
// Java: public interface Converter extends Closeable
pub trait Converter {
    fn configure(&mut self, configs: HashMap<String, Box<dyn Any>>, is_key: bool);
    fn from_connect_data(&self, topic: String, schema: Schema, value: Box<dyn Any>) -> Result<Vec<u8>, Box<dyn Error>>;
    fn from_connect_data_with_headers(&self, topic: String, headers: Headers, schema: Schema, value: Box<dyn Any>) -> Result<Vec<u8>, Box<dyn Error>>;
    fn to_connect_data(&self, topic: String, value: Vec<u8>) -> Result<SchemaAndValue, Box<dyn Error>>;
    fn to_connect_data_with_headers(&self, topic: String, headers: Headers, value: Vec<u8>) -> Result<SchemaAndValue, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self) -> Result<(), Box<dyn Error>>;
}
```

#### HeaderConverter (接口)
```rust
// Java: public interface HeaderConverter extends Configurable, Closeable
pub trait HeaderConverter: Configurable {
    fn to_connect_header(&self, topic: String, header_key: String, value: Vec<u8>) -> Result<SchemaAndValue, Box<dyn Error>>;
    fn from_connect_header(&self, topic: String, header_key: String, schema: Schema, value: Box<dyn Any>) -> Result<Vec<u8>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
}
```

#### Transformation (接口)
```rust
// Java: public interface Transformation<R extends ConnectRecord<R>> extends Configurable, Closeable
pub trait Transformation<R: ConnectRecord>: Configurable {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### Predicate (接口)
```rust
// Java: public interface Predicate<R extends ConnectRecord<R>> extends Configurable, AutoCloseable
pub trait Predicate<R: ConnectRecord>: Configurable {
    fn config(&self) -> ConfigDef;
    fn test(&self, record: R) -> bool;
    fn close(&mut self);
}
```

### 2.4 数据模型接口

#### Schema (接口)
```rust
// Java: public interface Schema
pub enum SchemaType {
    INT8, INT16, INT32, INT64,
    FLOAT32, FLOAT64,
    BOOLEAN, STRING, BYTES,
    ARRAY, MAP, STRUCT,
}

pub trait Schema {
    fn type(&self) -> SchemaType;
    fn is_optional(&self) -> bool;
    fn default_value(&self) -> Option<Box<dyn Any>>;
    fn name(&self) -> Option<String>;
    fn version(&self) -> Option<i32>;
    fn doc(&self) -> Option<String>;
    fn parameters(&self) -> HashMap<String, String>;
    fn key_schema(&self) -> Option<Box<dyn Schema>>;
    fn value_schema(&self) -> Option<Box<dyn Schema>>;
    fn fields(&self) -> Vec<Field>;
    fn field(&self, field_name: String) -> Option<Field>;
    fn schema(&self) -> Option<Box<dyn Schema>>;
}
```

#### SchemaBuilder (类)
```rust
// Java: public class SchemaBuilder implements Schema
pub struct SchemaBuilder {
    // 内部字段
    optional: bool,
    required: bool,
    default_value: Option<Box<dyn Any>>,
    name: Option<String>,
    version: Option<i32>,
    doc: Option<String>,
    parameters: HashMap<String, String>,
    schema_type: SchemaType,
    key_schema: Option<Box<dyn Schema>>,
    value_schema: Option<Box<dyn Schema>>,
    fields: Vec<Field>,
}

impl SchemaBuilder {
    // 静态工厂方法
    pub fn int8() -> Self;
    pub fn int16() -> Self;
    pub fn int32() -> Self;
    pub fn int64() -> Self;
    pub fn float32() -> Self;
    pub fn float64() -> Self;
    pub fn bool() -> Self;
    pub fn string() -> Self;
    pub fn bytes() -> Self;
    pub fn array(value_schema: Box<dyn Schema>) -> Self;
    pub fn map(key_schema: Box<dyn Schema>, value_schema: Box<dyn Schema>) -> Self;
    pub fn struct() -> Self;

    // Builder方法
    pub fn optional(mut self) -> Self;
    pub fn required(mut self) -> Self;
    pub fn default_value(mut self, value: Box<dyn Any>) -> Self;
    pub fn name(mut self, name: String) -> Self;
    pub fn version(mut self, version: i32) -> Self;
    pub fn doc(mut self, doc: String) -> Self;
    pub fn parameter(mut self, property_name: String, property_value: String) -> Self;
    pub fn parameters(mut self, props: HashMap<String, String>) -> Self;
    pub fn field(mut self, field_name: String, field_schema: Box<dyn Schema>) -> Self;
    pub fn build(self) -> Result<Box<dyn Schema>, Box<dyn Error>>;
}
```

#### Struct (类)
```rust
// Java: public class Struct
pub struct Struct {
    schema: Box<dyn Schema>,
    data: HashMap<String, Box<dyn Any>>,
}

impl Struct {
    pub fn new(schema: Box<dyn Schema>) -> Self;
    pub fn schema(&self) -> Box<dyn Schema>;
    pub fn get(&self, field_name: String) -> Option<Box<dyn Any>>;
    pub fn get_field(&self, field: Field) -> Option<Box<dyn Any>>;
    pub fn get_int8(&self, field_name: String) -> Result<i8, Box<dyn Error>>;
    pub fn get_int16(&self, field_name: String) -> Result<i16, Box<dyn Error>>;
    pub fn get_int32(&self, field_name: String) -> Result<i32, Box<dyn Error>>;
    pub fn get_int64(&self, field_name: String) -> Result<i64, Box<dyn Error>>;
    pub fn get_float32(&self, field_name: String) -> Result<f32, Box<dyn Error>>;
    pub fn get_float64(&self, field_name: String) -> Result<f64, Box<dyn Error>>;
    pub fn get_boolean(&self, field_name: String) -> Result<bool, Box<dyn Error>>;
    pub fn get_string(&self, field_name: String) -> Result<String, Box<dyn Error>>;
    pub fn get_bytes(&self, field_name: String) -> Result<Vec<u8>, Box<dyn Error>>;
    pub fn get_array<T>(&self, field_name: String) -> Result<Vec<T>, Box<dyn Error>>;
    pub fn get_map<K, V>(&self, field_name: String) -> Result<HashMap<K, V>, Box<dyn Error>>;
    pub fn get_struct(&self, field_name: String) -> Result<Struct, Box<dyn Error>>;
    pub fn put(&mut self, field_name: String, value: Box<dyn Any>) -> Result<(), Box<dyn Error>>;
    pub fn put_field(&mut self, field: Field, value: Box<dyn Any>) -> Result<(), Box<dyn Error>>;
    pub fn validate(&self) -> Result<(), Box<dyn Error>>;
}
```

### 2.5 记录类

#### ConnectRecord (抽象类)
```rust
// Java: public abstract class ConnectRecord<R extends ConnectRecord<R>>
pub trait ConnectRecord<R: ConnectRecord> {
    fn topic(&self) -> String;
    fn kafka_partition(&self) -> Option<i32>;
    fn key(&self) -> Option<Box<dyn Any>>;
    fn key_schema(&self) -> Option<Box<dyn Schema>>;
    fn value(&self) -> Option<Box<dyn Any>>;
    fn value_schema(&self) -> Option<Box<dyn Schema>>;
    fn timestamp(&self) -> Option<i64>;
    fn headers(&self) -> Headers;

    fn new_record(&self, topic: String, kafka_partition: Option<i32>, key_schema: Option<Box<dyn Schema>>, key: Option<Box<dyn Any>>, value_schema: Option<Box<dyn Schema>>, value: Option<Box<dyn Any>>, timestamp: Option<i64>) -> R;
    fn new_record_with_headers(&self, topic: String, kafka_partition: Option<i32>, key_schema: Option<Box<dyn Schema>>, key: Option<Box<dyn Any>>, value_schema: Option<Box<dyn Schema>>, value: Option<Box<dyn Any>>, timestamp: Option<i64>, headers: Headers) -> R;
}
```

#### SourceRecord (类)
```rust
// Java: public class SourceRecord extends ConnectRecord<SourceRecord>
pub struct SourceRecord {
    // 继承ConnectRecord的字段
    topic: String,
    kafka_partition: Option<i32>,
    key_schema: Option<Box<dyn Schema>>,
    key: Option<Box<dyn Any>>,
    value_schema: Option<Box<dyn Schema>>,
    value: Option<Box<dyn Any>>,
    timestamp: Option<i64>,
    headers: Headers,

    // SourceRecord特有字段
    source_partition: HashMap<String, Box<dyn Any>>,
    source_offset: HashMap<String, Box<dyn Any>>,
}

impl ConnectRecord<SourceRecord> for SourceRecord {
    // 实现ConnectRecord trait的所有方法
}
```

#### SinkRecord (类)
```rust
// Java: public class SinkRecord extends ConnectRecord<SinkRecord>
pub struct SinkRecord {
    // 继承ConnectRecord的字段
    topic: String,
    kafka_partition: Option<i32>,
    key_schema: Option<Box<dyn Schema>>,
    key: Option<Box<dyn Any>>,
    value_schema: Option<Box<dyn Schema>>,
    value: Option<Box<dyn Any>>,
    timestamp: Option<i64>,
    headers: Headers,

    // SinkRecord特有字段
    kafka_offset: i64,
    timestamp_type: TimestampType,
    original_topic: Option<String>,
    original_kafka_partition: Option<i32>,
    original_kafka_offset: Option<i64>,
}

impl ConnectRecord<SinkRecord> for SinkRecord {
    // 实现ConnectRecord trait的所有方法
}
```

### 2.6 Header相关接口

#### Headers (接口)
```rust
// Java: public interface Headers extends Iterable<Header>
pub trait Headers: IntoIterator<Item = Header> {
    fn size(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn all_with_name(&self, key: String) -> Vec<Header>;
    fn last_with_name(&self, key: String) -> Option<Header>;
    fn add(&mut self, header: Header);
    fn add_schema_and_value(&mut self, key: String, schema_and_value: SchemaAndValue);
    fn add(&mut self, key: String, value: Box<dyn Any>, schema: Box<dyn Schema>);
    fn add_string(&mut self, key: String, value: String);
    fn add_boolean(&mut self, key: String, value: bool);
    fn add_byte(&mut self, key: String, value: i8);
    fn add_short(&mut self, key: String, value: i16);
    fn add_int(&mut self, key: String, value: i32);
    fn add_long(&mut self, key: String, value: i64);
    fn add_float(&mut self, key: String, value: f32);
    fn add_double(&mut self, key: String, value: f64);
    fn add_bytes(&mut self, key: String, value: Vec<u8>);
    fn add_list<T>(&mut self, key: String, value: Vec<T>, schema: Box<dyn Schema>);
    fn add_map<K, V>(&mut self, key: String, value: HashMap<K, V>, schema: Box<dyn Schema>);
    fn add_struct(&mut self, key: String, value: Struct);
    fn add_decimal(&mut self, key: String, value: BigDecimal);
    fn add_date(&mut self, key: String, value: Date);
    fn add_time(&mut self, key: String, value: Date);
    fn add_timestamp(&mut self, key: String, value: Date);
    fn remove(&mut self, key: String);
    fn retain_latest(&mut self, key: String);
    fn retain_latest_all(&mut self);
    fn clear(&mut self);
    fn duplicate(&self) -> Box<dyn Headers>;
}
```

#### Header (接口)
```rust
// Java: public interface Header
pub trait Header {
    fn key(&self) -> String;
    fn schema(&self) -> Schema;
    fn value(&self) -> Box<dyn Any>;
    fn with(&self, schema: Schema, value: Box<dyn Any>) -> Box<dyn Header>;
    fn rename(&self, key: String) -> Box<dyn Header>;
}
```

### 2.7 Context接口

#### ConnectorContext (接口)
```rust
// Java: public interface ConnectorContext
pub trait ConnectorContext {
    fn request_task_reconfiguration(&self);
    fn raise_error(&self, e: Box<dyn Error>);
    fn plugin_metrics(&self) -> PluginMetrics;
}
```

#### SourceTaskContext (接口)
```rust
// Java: public interface SourceTaskContext
pub trait SourceTaskContext {
    fn configs(&self) -> HashMap<String, String>;
    fn offset_storage_reader(&self) -> Box<dyn OffsetStorageReader>;
    fn transaction_context(&self) -> Box<dyn TransactionContext>;
    fn plugin_metrics(&self) -> PluginMetrics;
}
```

#### SinkTaskContext (接口)
```rust
// Java: public interface SinkTaskContext
pub trait SinkTaskContext {
    fn configs(&self) -> HashMap<String, String>;
    fn offset(&self, tp: TopicPartition, offset: i64);
    fn offsets(&self, offsets: HashMap<TopicPartition, i64>);
    fn timeout(&self, timeout_ms: i64);
    fn assignment(&self) -> HashSet<TopicPartition>;
    fn pause(&self, partitions: Vec<TopicPartition>);
    fn resume(&self, partitions: Vec<TopicPartition>);
    fn request_commit(&self);
    fn errant_record_reporter(&self) -> Box<dyn ErrantRecordReporter>;
    fn plugin_metrics(&self) -> PluginMetrics;
}
```

#### OffsetStorageReader (接口)
```rust
// Java: public interface OffsetStorageReader
pub trait OffsetStorageReader {
    fn offset<T>(&self, partition: HashMap<String, T>) -> Result<HashMap<String, Box<dyn Any>>, Box<dyn Error>>;
    fn offsets<T>(&self, partitions: Vec<HashMap<String, T>>) -> Result<HashMap<HashMap<String, T>, HashMap<String, Box<dyn Any>>>, Box<dyn Error>>;
}
```

### 2.8 辅助接口和枚举

#### Versioned (接口)
```rust
// Java: public interface Versioned
pub trait Versioned {
    fn version(&self) -> String;
}
```

#### ExactlyOnceSupport (枚举)
```rust
// Java: public enum ExactlyOnceSupport
pub enum ExactlyOnceSupport {
    Supported,
    Unsupported,
}
```

#### ConnectorTransactionBoundaries (枚举)
```rust
// Java: public enum ConnectorTransactionBoundaries
pub enum ConnectorTransactionBoundaries {
    Supported,
    Unsupported,
}
```

---

## 三、connect/runtime模块 - 运行时核心契约

### 3.1 Worker类
```rust
// Java: public class Worker
pub struct Worker {
    // 内部字段省略
}

impl Worker {
    pub fn start(&mut self) -> Result<(), Box<dyn Error>>;
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    pub fn start_connector(&mut self, conn_name: String, conn_props: HashMap<String, String>, ctx: Box<dyn CloseableConnectorContext>, status_listener: Box<dyn ConnectorStatusListener>, initial_state: TargetState, on_state_change: Box<dyn Callback<TargetState>>) -> Result<(), Box<dyn Error>>;
    pub fn is_sink_connector(&self, conn_name: String) -> bool;
    pub fn connector_task_configs(&self, conn_name: String, conn_config: ConnectorConfig) -> Vec<HashMap<String, String>>;
    pub fn stop_and_await_connectors(&mut self) -> Result<(), Box<dyn Error>>;
    pub fn stop_and_await_connector(&mut self, conn_name: String) -> Result<(), Box<dyn Error>>;

    pub fn connector_names(&self) -> Vec<String>;
    pub fn is_running(&self, conn_name: String) -> bool;
    pub fn connector_version(&self, conn_name: String) -> Option<String>;
    pub fn task_version(&self, task_id: ConnectorTaskId) -> Option<String>;

    pub fn start_sink_task(&mut self, ...) -> Result<(), Box<dyn Error>>;
    pub fn start_source_task(&mut self, ...) -> Result<(), Box<dyn Error>>;
    pub fn start_exactly_once_source_task(&mut self, ...) -> Result<(), Box<dyn Error>>;

    pub fn fence_zombies(&mut self, conn_name: String, num_tasks: i32, conn_props: HashMap<String, String>) -> Result<(), Box<dyn Error>>;
    pub fn stop_and_await_tasks(&mut self) -> Result<(), Box<dyn Error>>;
    pub fn stop_and_await_task(&mut self, task_id: ConnectorTaskId) -> Result<(), Box<dyn Error>>;

    pub fn task_ids(&self) -> Vec<ConnectorTaskId>;
    pub fn config(&self) -> WorkerConfig;
    pub fn config_transformer(&self) -> Box<dyn WorkerConfigTransformer>;
    pub fn metrics(&self) -> Box<dyn WorkerMetrics>;

    pub fn set_target_state(&mut self, conn_name: String, state: TargetState, state_change_callback: Box<dyn Callback<TargetState>>) -> Result<(), Box<dyn Error>>;
    pub fn connector_offsets(&self, conn_name: String, conn_config: HashMap<String, String>, cb: Box<dyn Callback<ConnectorOffsets>>) -> Result<(), Box<dyn Error>>;
    pub fn modify_connector_offsets(&self, conn_name: String, conn_config: HashMap<String, String>, offsets: HashMap<HashMap<String, Box<dyn Any>>, HashMap<String, Box<dyn Any>>>, cb: Box<dyn Callback<Message>>) -> Result<(), Box<dyn Error>>;
}
```

### 3.2 Herder接口
```rust
// Java: public interface Herder
pub trait Herder {
    fn start(&mut self) -> Result<(), Box<dyn Error>>;
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;
    fn is_ready(&self) -> bool;
    fn health_check(&self, callback: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>>;

    fn connectors(&self, callback: Box<dyn Callback<Vec<String>>>) -> Result<(), Box<dyn Error>>;
    fn connector_info(&self, conn_name: String, callback: Box<dyn Callback<ConnectorInfo>>) -> Result<(), Box<dyn Error>>;
    fn connector_config(&self, conn_name: String, callback: Box<dyn Callback<HashMap<String, String>>>) -> Result<(), Box<dyn Error>>;

    fn put_connector_config(&self, conn_name: String, config: HashMap<String, String>, allow_replace: bool, callback: Box<dyn Callback<Created<ConnectorInfo>>>) -> Result<(), Box<dyn Error>>;
    fn put_connector_config_with_target_state(&self, conn_name: String, config: HashMap<String, String>, target_state: TargetState, allow_replace: bool, callback: Box<dyn Callback<Created<ConnectorInfo>>>) -> Result<(), Box<dyn Error>>;
    fn patch_connector_config(&self, conn_name: String, config_patch: HashMap<String, String>, callback: Box<dyn Callback<Created<ConnectorInfo>>>) -> Result<(), Box<dyn Error>>;
    fn delete_connector_config(&self, conn_name: String, callback: Box<dyn Callback<Created<ConnectorInfo>>>) -> Result<(), Box<dyn Error>>;

    fn request_task_reconfiguration(&self, conn_name: String) -> Result<(), Box<dyn Error>>;
    fn task_configs(&self, conn_name: String, callback: Box<dyn Callback<Vec<TaskInfo>>>) -> Result<(), Box<dyn Error>>;
    fn put_task_configs(&self, conn_name: String, configs: Vec<HashMap<String, String>>, callback: Box<dyn Callback<()>>, request_signature: InternalRequestSignature) -> Result<(), Box<dyn Error>>;
    fn fence_zombie_source_tasks(&self, conn_name: String, callback: Box<dyn Callback<()>>, request_signature: InternalRequestSignature) -> Result<(), Box<dyn Error>>;

    fn connectors_sync(&self) -> Vec<String>;
    fn connector_info_sync(&self, conn_name: String) -> Option<ConnectorInfo>;
    fn connector_status(&self, conn_name: String) -> Option<ConnectorState>;
    fn connector_active_topics(&self, conn_name: String) -> Vec<String>;
    fn reset_connector_active_topics(&self, conn_name: String);

    fn status_backing_store(&self) -> Box<dyn StatusBackingStore>;
    fn task_status(&self, id: ConnectorTaskId) -> Option<TaskState>;

    fn validate_connector_config(&self, connector_config: HashMap<String, String>, callback: Box<dyn Callback<ConfigInfos>>) -> Result<(), Box<dyn Error>>;

    fn restart_task(&self, id: ConnectorTaskId, cb: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>>;
    fn restart_connector(&self, conn_name: String, cb: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>>;
    fn restart_connector_with_delay(&self, delay_ms: i64, conn_name: String, cb: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>>;
    fn restart_connector_and_tasks(&self, request: RestartRequest, cb: Box<dyn Callback<ConnectorStateInfo>>) -> Result<(), Box<dyn Error>>;

    fn stop_connector(&self, connector: String, cb: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>>;
    fn pause_connector(&self, connector: String) -> Result<(), Box<dyn Error>>;
    fn resume_connector(&self, connector: String) -> Result<(), Box<dyn Error>>;

    fn plugins(&self) -> Box<dyn Plugins>;
    fn kafka_cluster_id(&self) -> Option<String>;
    fn connector_plugin_config(&self, plugin_name: String) -> Option<HashMap<String, String>>;
    fn connector_plugin_config_with_version(&self, plugin_name: String, version: VersionRange) -> Option<HashMap<String, String>>;

    fn connector_offsets(&self, conn_name: String, cb: Box<dyn Callback<ConnectorOffsets>>) -> Result<(), Box<dyn Error>>;
    fn alter_connector_offsets(&self, conn_name: String, offsets: HashMap<HashMap<String, Box<dyn Any>>, HashMap<String, Box<dyn Any>>>, cb: Box<dyn Callback<Message>>) -> Result<(), Box<dyn Error>>;
    fn reset_connector_offsets(&self, conn_name: String, cb: Box<dyn Callback<Message>>) -> Result<(), Box<dyn Error>>;

    fn logger_level(&self, logger: String) -> Option<String>;
    fn all_logger_levels(&self) -> HashMap<String, String>;
    fn set_worker_logger_level(&self, namespace: String, level: String) -> Result<(), Box<dyn Error>>;
    fn set_cluster_logger_level(&self, namespace: String, level: String) -> Result<(), Box<dyn Error>>;

    fn connect_metrics(&self) -> Box<dyn ConnectMetrics>;
}
```

### 3.3 StandaloneHerder类
```rust
// Java: public class StandaloneHerder extends AbstractHerder
pub struct StandaloneHerder {
    // 继承AbstractHerder的字段
}

impl StandaloneHerder {
    pub fn start(&mut self) -> Result<(), Box<dyn Error>>;
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>>;
    pub fn ready(&mut self);
    pub fn health_check(&self, cb: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>>;
    pub fn generation(&self) -> i32; // 单机模式返回0
}
```

### 3.4 DistributedHerder类
```rust
// Java: public class DistributedHerder extends AbstractHerder
pub struct DistributedHerder {
    // 继承AbstractHerder的字段
}

impl DistributedHerder {
    pub fn start(&mut self) -> Result<(), Box<dyn Error>>; // 提交到executor
    pub fn run(&mut self) -> Result<(), Box<dyn Error>>; // 主运行循环
    pub fn halt(&mut self) -> Result<(), Box<dyn Error>>; // 有序关闭
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>>;
    pub fn tick(&mut self) -> Result<(), Box<dyn Error>>; // 主循环tick方法
    pub fn generation(&self) -> i32; // 返回当前generation
}
```

### 3.5 Storage接口

#### StatusBackingStore (接口)
```rust
// Java: public interface StatusBackingStore
pub trait StatusBackingStore {
    fn start(&mut self) -> Result<(), Box<dyn Error>>;
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    fn put(&mut self, status: ConnectorStatus) -> Result<(), Box<dyn Error>>;
    fn put_safe(&mut self, status: ConnectorStatus) -> Result<(), Box<dyn Error>>;
    fn get(&self, connector: String) -> Option<ConnectorStatus>;
    fn get_all(&self, connector: String) -> Vec<TaskStatus>;

    fn put_task(&mut self, status: TaskStatus) -> Result<(), Box<dyn Error>>;
    fn put_task_safe(&mut self, status: TaskStatus) -> Result<(), Box<dyn Error>>;
    fn get_task(&self, id: ConnectorTaskId) -> Option<TaskStatus>;

    fn put_topic(&mut self, status: TopicStatus) -> Result<(), Box<dyn Error>>;
    fn get_topic(&self, connector: String, topic: String) -> Option<TopicStatus>;
    fn get_all_topics(&self, connector: String) -> Vec<TopicStatus>;
    fn delete_topic(&mut self, connector: String, topic: String) -> Result<(), Box<dyn Error>>;
}
```

#### ConfigBackingStore (接口)
```rust
// Java: public interface ConfigBackingStore
pub trait ConfigBackingStore {
    fn start(&mut self) -> Result<(), Box<dyn Error>>;
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    fn snapshot(&self) -> HashMap<String, HashMap<String, String>>;
    fn contains(&self, connector: String) -> bool;
    fn put_connector_config(&mut self, connector: String, properties: HashMap<String, String>, target_state: TargetState) -> Result<(), Box<dyn Error>>;
    fn remove_connector_config(&mut self, connector: String) -> Result<(), Box<dyn Error>>;

    fn put_task_configs(&mut self, connector: String, configs: Vec<HashMap<String, String>>) -> Result<(), Box<dyn Error>>;
    fn remove_task_configs(&mut self, connector: String) -> Result<(), Box<dyn Error>>;

    fn put_target_state(&mut self, connector: String, state: TargetState) -> Result<(), Box<dyn Error>>;
    fn claim_write_privileges(&mut self) -> Result<(), Box<dyn Error>>;
}
```

#### OffsetBackingStore (接口)
```rust
// Java: public interface OffsetBackingStore
pub trait OffsetBackingStore {
    fn start(&mut self) -> Result<(), Box<dyn Error>>;
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;
    fn configure(&mut self, configs: HashMap<String, Box<dyn Any>>) -> Result<(), Box<dyn Error>>;

    fn get<T>(&self, partition: HashMap<String, T>, callback: Box<dyn Callback<HashMap<String, Box<dyn Any>>>>) -> Result<(), Box<dyn Error>>;
    fn put(&mut self, offsets: HashMap<HashMap<String, Box<dyn Any>>, HashMap<String, Box<dyn Any>>>, callback: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>>;
    fn connector_partitions(&self, connector: String) -> Vec<TopicPartition>;
}
```

---

## 四、connect/transforms模块 - 数据转换契约

### 4.1 Transformation接口 (已在connect/api中定义)

### 4.2 内置Transformation实现

#### ValueToKey
```rust
// Java: public class ValueToKey<R extends ConnectRecord<R>> implements Transformation<R>
pub struct ValueToKey<R: ConnectRecord> {
    // 配置参数
    fields: Vec<String>,
    replace_null_with_default: bool,
}

impl<R: ConnectRecord> Transformation<R> for ValueToKey<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### TimestampRouter
```rust
// Java: public class TimestampRouter<R extends ConnectRecord<R>> implements Transformation<R>
pub struct TimestampRouter<R: ConnectRecord> {
    // 配置参数
    topic_format: String, // 默认"${topic}-${timestamp}"
    timestamp_format: String, // 默认"yyyyMMdd"
}

impl<R: ConnectRecord> Transformation<R> for TimestampRouter<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### TimestampConverter
```rust
// Java: public abstract class TimestampConverter<R extends ConnectRecord<R>> implements Transformation<R>
pub struct TimestampConverter<R: ConnectRecord> {
    // 配置参数
    field: String, // 默认"" (空表示整个值)
    target_type: String, // "string", "unix", "Date", "Time", "Timestamp"
    format: String, // SimpleDateFormat兼容格式
    unix_precision: String, // "seconds", "milliseconds", "microseconds", "nanoseconds"
    replace_null_with_default: bool,
}

impl<R: ConnectRecord> Transformation<R> for TimestampConverter<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### ReplaceField
```rust
// Java: public abstract class ReplaceField<R extends ConnectRecord<R>> implements Transformation<R>
pub struct ReplaceField<R: ConnectRecord> {
    // 配置参数
    exclude: Vec<String>,
    include: Vec<String>,
    renames: HashMap<String, String>, // "foo:bar"格式
    replace_null_with_default: bool,
}

impl<R: ConnectRecord> Transformation<R> for ReplaceField<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### MaskField
```rust
// Java: public abstract class MaskField<R extends ConnectRecord<R>> implements Transformation<R>
pub struct MaskField<R: ConnectRecord> {
    // 配置参数
    fields: Vec<String>,
    replacement: Option<String>, // 自定义替换值
    replace_null_with_default: bool,
}

impl<R: ConnectRecord> Transformation<R> for MaskField<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### InsertHeader
```rust
// Java: public class InsertHeader<R extends ConnectRecord<R>> implements Transformation<R>
pub struct InsertHeader<R: ConnectRecord> {
    // 配置参数
    header: String,
    value_literal: Option<String>,
}

impl<R: ConnectRecord> Transformation<R> for InsertHeader<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### InsertField
```rust
// Java: public abstract class InsertField<R extends ConnectRecord<R>> implements Transformation<R>
pub struct InsertField<R: ConnectRecord> {
    // 配置参数
    topic_field: Option<String>,
    partition_field: Option<String>,
    offset_field: Option<String>,
    timestamp_field: Option<String>,
    static_field: Option<String>,
    static_value: Option<String>,
    replace_null_with_default: bool,
}

impl<R: ConnectRecord> Transformation<R> for InsertField<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn fn close(&mut self);
}
```

#### HoistField
```rust
// Java: public abstract class HoistField<R extends ConnectRecord<R>> implements Transformation<R>
pub struct HoistField<R: ConnectRecord> {
    // 配置参数
    field: String, // 要创建的单个字段名
}

impl<R: ConnectRecord> Transformation<R> for HoistField<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### HeaderFrom
```rust
// Java: public abstract class HeaderFrom<R extends ConnectRecord<R>> implements Transformation<R>
pub struct HeaderFrom<R: ConnectRecord> {
    // 配置参数
    fields: Vec<String>,
    headers: Vec<String>,
    operation: String, // "move"或"copy"
    replace_null_with_default: bool,
}

impl<R: ConnectRecord> Transformation<R> for HeaderFrom<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### Flatten
```rust
// Java: public abstract class Flatten<R extends ConnectRecord<R>> implements Transformation<R>
pub struct Flatten<R: ConnectRecord> {
    // 配置参数
    delimiter: String, // 默认"."
}

impl<R: ConnectRecord> Transformation<R> for Flatten<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### Filter
```rust
// Java: public class Filter<R extends ConnectRecord<R>>>> implements Transformation<R>
pub struct Filter<R: ConnectRecord> {
    // 无配置参数
}

impl<R: ConnectRecord> Transformation<R> for Filter<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>; // 总是返回None（过滤所有记录）
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### ExtractField
```rust
// Java: public abstract class ExtractField<R extends ConnectRecord<R>> implements Transformation<R>
pub struct ExtractField<R: ConnectRecord> {
    // 配置参数
    field: String,
    replace_null_with_default: bool,
}

impl<R: ConnectRecord> Transformation<R> for ExtractField<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### DropHeaders
```rust
// Java: public class DropHeaders<R extends ConnectRecord<R>> implements Transformation<R>
pub struct DropHeaders<R: ConnectRecord> {
    // 配置参数
    headers: Vec<String>, // 要删除的header名列表
}

impl<R: ConnectRecord> Transformation<R> for DropHeaders<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

#### Cast
```rust
// Java: public abstract class Cast<R extends ConnectRecord<R>> implements Transformation<R>
pub struct Cast<R: ConnectRecord> {
    // 配置参数
    spec: Vec<String>, // "field:type"格式，如"foo:int32"
    replace_null_with_default: bool,
}

impl<R: ConnectRecord> Transformation<R> for Cast<R> {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
    fn close(&mut self);
}
```

### 4.3 Predicate接口 (已在connect/api中定义)

### 4.4 内置Predicate实现

#### TopicNameMatches
```rust
// Java: public class TopicNameMatches<R extends ConnectRecord<R>> implements Predicate<R>
pub struct TopicNameMatches<R: ConnectRecord> {
    // 配置参数
    pattern: String, // Java正则表达式
}

impl<R: ConnectRecord> Predicate<R> for TopicNameMatches<R> {
    fn config(&self) -> ConfigDef;
    fn test(&self, record: R) -> bool;
    fn close(&mut self);
}
```

#### HasHeaderKey
```rust
// Java: public class HasHeaderKey<R extends ConnectRecord<R>> implements Predicate<R>
pub struct HasHeaderKey<R: ConnectRecord> {
    // 配置参数
    name: String, // header名称
}

impl<R: ConnectRecord> Predicate<R> for HasHeaderKey<R> {
    fn config(&self) -> ConfigDef;
    fn test(&self, record: R) -> bool;
    fn close(&mut self);
}
```

#### RecordIsTombstone
```rust
// Java: public class RecordIsTombstone<R extends ConnectRecord<R>> implements Predicate<R>
pub struct RecordIsTombstone<R: ConnectRecord> {
    // 无配置参数
}

impl<R: ConnectRecord> Predicate<R> for RecordIsTombstone<R> {
    fn config(&self) -> ConfigDef;
    fn test(&self, record: R) -> bool; // 检查record.value() == None
    fn close(&mut self);
}
```

---

## 五、connect/mirror-client模块 - 镜像客户端契约

### 5.1 MirrorClient类
```rust
// Java: public class MirrorClient
pub struct MirrorClient {
    // 内部字段省略
}

impl MirrorClient {
    pub fn replication_hops(&self, upstream_cluster_alias: String) -> Result<i32, Box<dyn Error>>;
    pub fn heartbeat_topics(&self) -> Result<Vec<String>, Box<dyn Error>>;
    pub fn checkpoint_topics(&self) -> Result<Vec<String>, Box<dyn Error>>;
    pub fn upstream_clusters(&self) -> Result<Vec<String>, Box<dyn Error>>;
    pub fn remote_topics(&self) -> Result<Vec<String>, Box<dyn Error>>;
    pub fn remote_consumer_offsets(&self) -> Result<HashMap<String, HashMap<String, i64>>, Box<dyn Error>>;
    pub fn replication_policy(&self) -> Box<dyn ReplicationPolicy>;
}
```

### 5.2 ReplicationPolicy接口
```rust
// Java: public interface ReplicationPolicy
pub trait ReplicationPolicy {
    fn format_remote_topic(&self, topic: String) -> String;
    fn topic_source(&self, topic: String) -> String;
    fn upstream_topic(&self, topic: String) -> String;
    fn original_topic(&self, topic: String) -> String;

    fn heartbeats_topic(&self) -> String;
    fn offset_syncs_topic(&self) -> String;
    fn checkpoints_topic(&self) -> String;

    fn is_internal_topic(&self, topic: String) -> bool;
}
```

### 5.3 DefaultReplicationPolicy
```rust
// Java: public class DefaultReplicationPolicy implements ReplicationPolicy
pub struct DefaultReplicationPolicy {
    separator: String, // 默认'.'
}

impl ReplicationPolicy for DefaultReplicationPolicy {
    fn format_remote_topic(&self, topic: String) -> String; // 在远程主题名称前添加源集群别名（如：us-west.topic）
    fn topic_source(&self, topic: String) -> String;
    fn upstream_topic(&self, topic: String) -> String;
    fn original_topic(&self, topic: String) -> String;

    fn heartbeats_topic(&self) -> String;
    fn offset_syncs_topic(&self) -> String;
    fn checkpoints_topic(&self) -> String;

    fn is_internal_topic(&self, topic: String) -> bool;
}
```

### 5.4 IdentityReplicationPolicy
```rust
// Java: public class IdentityReplicationPolicy implements ReplicationPolicy
pub struct IdentityReplicationPolicy {
    // 无额外字段
}

impl ReplicationPolicy for IdentityReplicationPolicy {
    fn format_remote_topic(&self, topic: String) -> String; // 不重命名远程主题
    fn topic_source(&self, topic: String) -> String;
    fn upstream_topic(&self, topic: String) -> String;
    fn original_topic(&self, topic: String) -> String;

    fn heartbeats_topic(&self) -> String;
    fn offset_syncs_topic(&self) -> String;
    fn checkpoints_topic(&self) -> String;

    fn is_internal_topic(&self, topic: String) -> bool;
}
```

### 5.5 数据模型类

#### Checkpoint
```rust
// Java: public class Checkpoint
pub struct Checkpoint {
    consumer_group_id: String,
    topic_partition: TopicPartition,
    upstream_offset: i64,
    downstream_offset: i64,
    metadata: Option<String>,
}

impl Checkpoint {
    // 序列化/反序列化方法
    pub fn serialize(&self) -> Result<Vec<u8>, Box<dyn Error>>;
    pub fn deserialize(data: Vec<u8>) -> Result<Self, Box<dyn Error>>;
}
```

#### Heartbeat
```rust
// Java: public class Heartbeat
pub struct: Heartbeat {
    source_cluster_alias: String,
    target_cluster_alias: String,
    timestamp: i64,
}

impl Heartbeat {
    // 序列化/反序列化方法
    pub fn serialize(&self) -> Result<Vec<u8>, Box<dyn Error>>;
    pub fn deserialize(data: Vec<u8>) -> Result<Self, Box<dyn Error>>;
}
```

#### SourceAndTarget
```rust
// Java: public class SourceAndTarget
pub struct SourceAnd {
    source_cluster_alias: String,
    target_cluster_alias: String,
}

impl SourceAndTarget {
    // 不可变数据类
}
```

---

## 六、connect/mirror模块 - 镜像复制契约

### 6.1 MirrorMaker类
```rust
// Java: public class MirrorMaker
pub struct MirrorMaker {
    // 内部字段省略
}

impl MirrorMaker {
    pub fn start(&mut self) -> Result<(), Box<dyn Error>>;
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>>;
    pub fn await_stop(&mut self) -> Result<(), Box<dyn Error>>;

    pub fn connector_status(&self, conn_name: String) -> Option<ConnectorState>;
    pub fn task_configs(&self, conn_name: String) -> Option<Vec<TaskConfig>>;

    pub fn main(&self) -> Result<(), Box<dyn Error>>; // 命令行入口
}
```

### 6.2 MirrorSourceConnector类
```rust
// Java: public class MirrorSourceConnector extends SourceConnector
pub struct MirrorSourceConnector {
    // 内部字段省略
}

impl SourceConnector for MirrorSourceConnector {
    fn task_class(&self) -> Box<dyn Any>;
    fn task_configs(&self, max_tasks: i32) -> Vec<HashMap<String, String>>;

    // 其他SourceConnector方法
}

impl MirrorSourceConnector {
    pub fn start(&mut self) -> Result<(), Box<dyn Error>>;
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    pub fn sync_topic_acls(&self) -> Result<(), Box<dyn Error>>;
    pub fn sync_topic_configs(&self) -> Result<(), Box<dyn Error>>;
    pub fn refresh_topic_partitions(&self) -> Result<(), Box<dyn Error>>;
    pub fn compute_and_create_topic_partitions(&self) -> Result<(), Box<dyn Error>>;
    pub fn create_new_topics(&self) -> Result<(), Box<dyn Error>>;
    pub fn create_new_partitions(&self) -> Result<(), Box<dyn Error>>;
}
```

### 6.3 MirrorCheckpointConnector类
```rust
// Java: public class MirrorCheckpointConnector extends SourceConnector
pub struct MirrorCheckpointConnector {
    // 内部字段省略
}

impl SourceConnector for MirrorCheckpointConnector {
    fn task_class(&self) -> Box<dyn Any>;
    fn task_configs(&self, max_tasks: i32) -> Vec<HashMap<String, String>>;

    // 其他SourceConnector方法
}

impl MirrorCheckpointConnector {
    pub fn start(&mut self) -> Result<(), Box<dyn Error>>;
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    pub fn refresh_consumer_groups(&self) -> Result<(), Box<dyn Error>>;
    pub fn find_consumer_groups(&self) -> Result<Vec<String>, Box<dyn Error>>;
}
```

### 6.4 MirrorHeartbeatConnector类
```rust
// Java: public class MirrorHeartbeatConnector extends SourceConnector
pub struct MirrorHeartbeatConnector {
    // 内部字段省略
}

impl SourceConnector for MirrorHeartbeatConnector {
    fn task_class(&self) -> Box<dyn Any>;
    fn task_configs(&self, max_tasks: i32) -> Vec<HashMap<String, String>>;

    // 其他SourceConnector方法
}

impl MirrorHeartbeatConnector {
    pub fn start(&mut self) -> Result<(), Box<dyn Error>>;
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>>;
}
```

### 6.5 MirrorSourceTask类
```rust
// Java: public class MirrorSourceTask extends SourceTask
pub struct MirrorSourceTask {
    // 内部字段省略
}

impl SourceTask for MirrorSourceTask {
    fn poll(&mut self) -> Result<Vec<SourceRecord>, Box<dyn Error>>;
    fn commit(&mut self) -> Result<(), Box<dyn Error>>;
    fn commit_record(&mut self, record: SourceRecord, metadata: RecordMetadata) -> Result<(), Box<dyn Error>>;

    // 其他SourceTask方法
}

impl MirrorSourceTask {
    pub fn convert_record(&self, consumer_record: ConsumerRecord) -> Result<SourceRecord, Box<dyn Error>>;
}
```

### 6.6 MirrorCheckpointTask类
```rust
// Java: public class MirrorCheckpointTask extends SourceTask
pub struct MirrorCheckpointTask {
    // 内部字段省略
}

impl SourceTask for MirrorCheckpointTask {
    fn poll(&mut self) -> Result<Vec<SourceRecord>, Box<dyn Error>>;

    // 其他SourceTask方法
}

impl MirrorCheckpointTask {
    pub fn source_records_for_group(&self, group: String) -> Result<Vec<SourceRecord>, Box<dyn Error>>;
    pub fn checkpoints_for_group(&self, group: String) -> Result<Vec<Checkpoint>, Box<dyn Error>>;
    pub fn sync_group_offset(&self, group: String) -> Result<(), Box<dyn Error>>;
}
```

### 6.7 MirrorHeartbeatTask类
```rust
// Java: public class MirrorHeartbeatTask extends SourceTask
pub struct MirrorHeartbeatTask {
    // 内部字段省略
}

impl SourceTask for MirrorHeartbeatTask {
    fn poll(&mut self) -> Result<Vec<SourceRecord>, Box<dyn Error>>;

    // 其他SourceTask方法
}
```

### 6.8 过滤器接口

#### TopicFilter
```rust
// Java: public interface TopicFilter
pub trait TopicFilter {
    fn should_replicate_topic(&self, topic: String) -> bool;
}
```

#### GroupFilter
```rust
// Java: public interface GroupFilter
pub trait GroupFilter {
    fn should_replicate_group(&self, group: String) -> bool;
}
```

---

## 七、connect/basic-auth-extension模块 - 认证扩展契约

### 7.1 BasicAuthExtension类
```rust
// Java: public class BasicAuthExtension implements ConnectRestExtension
pub struct BasicAuthExtension {
    // 内部字段省略
}

impl ConnectRestExtension for BasicAuthExtension {
    fn register(&mut self, rest_plugin_context: Box<dyn ConnectRestExtensionContext>) -> Result<(), Box<dyn Error>>;
}

impl Configurable for BasicAuthExtension {
    fn configure(&mut self, configs: HashMap<String, Box<dyn Any>>) -> Result<(), Box<dyn Error>>;
}

impl Versioned for BasicAuthExtension {
    fn version(&self) -> String;
}

impl Closeable for BasicAuthExtension {
    fn close(&mut self) -> Result<(), Box<dyn Error>>;
}
```

---

## 八、kafka-clients-trait模块 - Kafka客户端契约

### 8.1 Producer trait
```rust
// Java: org.apache.kafka.clients.producer.Producer
pub trait KafkaProducer {
    fn send(&self, record: ProducerRecord, callback: Option<Box<dyn FnOnce(RecordMetadata, Option<Box<dyn Error>>)>>) -> Box<dyn Future<RecordMetadata>>;
    fn flush(&self);
    fn close(&self, timeout: Duration);

    // 事务相关（可选）
    fn init_transactions(&self, keep_prepared_txn: bool);
    fn begin_transaction(&self);
    fn commit_transaction(&self);
    fn abort_transaction(&self);
}
```

### 8.2 Consumer trait
```rust
// Java: org.apache.kafka.clients.consumer.KafkaConsumer
pub trait KafkaConsumer {
    fn subscribe(&self, topics: Vec<String>, listener: Option<Box<dyn ConsumerRebalanceListener>>);
    fn subscribe_pattern(&self, pattern: Regex, listener: Option<Box<dyn ConsumerRebalanceListener>>);
    fn assign(&self, partitions: Vec<TopicPartition>);

    fn poll(&self, timeout: Duration) -> ConsumerRecords;

    fn commit_sync(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>);
    fn commit_async(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>, callback: Box<dyn FnOnce(Option<Box<dyn Error>>)>>);

    fn seek(&self, partition: TopicPartition, offset: i64);
    fn seek_to_beginning(&self, partitions: Vec<TopicPartition>);

    fn wakeup(&self);
    fn assignment(&self) -> HashSet<TopicPartition>;
    fn position(&self, partition: TopicPartition) -> i64;
    fn end_offsets(&self, partitions: HashSet<TopicPartition>) -> HashMap<TopicPartition, i64>;
}
```

### 8.3 AdminClient trait
```rust
// Java: org.apache.kafka.clients.admin.Admin
pub trait KafkaAdmin {
    fn create_topics(&self, topics: Vec<NewTopic>, options: CreateTopicsOptions) -> CreateTopicsResult;
    fn describe_topics(&self, topic_names: Vec<String>, options: DescribeTopicsOptions) -> DescribeTopicsResult;
    fn describe_configs(&self, resources: Vec<ConfigResource>, options: DescribeConfigsOptions) -> DescribeConfigsResult;
    fn list_offsets(&self, offset_specs: HashMap<TopicPartition, OffsetSpec>, options: ListOffsetsOptions) -> ListOffsetsResult;
    fn list_topics(&self, options: ListTopicsOptions) -> ListTopicsResult;
;
}
```

### 8.4 序列化trait
```rust
// Java: org.apache.kafka.common.serialization.Serializer<T>
pub trait Serializer<T> {
    fn serialize(&self, topic: &str, data: &T) -> Result<Vec<u8>, Box<dyn Error>>;
    fn configure(&self, configs: HashMap<String, String>);
    fn close(&self);
}

// Java: org.apache.kafka.common.serialization.Deserializer<T>
pub trait Deserializer<T> {
    fn deserialize(&self, topic: &str, data: &[u8]) -> Result<T, Box<dyn Error>>;
    fn configure(&self, configs: HashMap<String, String>);
    fn close(&self);
}
```

---

## 九、kafka-clients-mock模块 - Mock实现契约

### 9.1 MockProducer
```rust
// 实现KafkaProducer trait
pub struct MockProducer {
    // 内存队列存储消息
    message_queue: Arc<Mutex<VecDeque<ProducerRecord>>>,
    // 支持预制数据
    prepopulated_data: Arc<Mutex<HashMap<String, Vec<ConsumerRecord>>>>,
}

impl KafkaProducer for MockProducer {
    fn send(&self, record: ProducerRecord, callback: Option<Box<dyn FnOnce(RecordMetadata, Option<Box<dyn Error>>)>>) -> Box<dyn Future<RecordMetadata>>;
    fn flush(&self);
    fn close(&self, timeout: Duration);

    // 事务方法（简化实现）
    fn init_transactions(&self, keep_prepared_txn: bool);
    fn begin_transaction(&self);
    fn commit_transaction(&self);
    fn abort_transaction(&self);
}
```

### 9.2 MockConsumer
```rust
// 实现KafkaConsumer trait
pub struct MockConsumer {
    // 从内存队列消费
    message_queue: Arc<Mutex<VecDeque<ConsumerRecord>>>,
    // 支持offset管理
    offsets: Arc<Mutex<HashMap<TopicPartition, i64>>>,
}

impl KafkaConsumer for MockConsumer {
    fn subscribe(&self, topics: Vec<String>, listener: Option<Box<dyn ConsumerRebalanceListener>>);
    fn subscribe_pattern(&self, pattern: Regex, listener: Option<Box<dyn ConsumerRebalanceListener>>);
    fn assign(&self, partitions: Vec<TopicPartition>);

    fn poll(&self, timeout: Duration) -> ConsumerRecords;
    fn commit_sync(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>);
    fn commit_async(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>, callback: Box<dyn FnOnce(Option<Box<dyn Error>>)>>);

    fn seek(&self, partition: TopicPartition, offset: i64);
    fn seek_to_beginning(&self, partitions: Vec<TopicPartition>);

    fn wakeup(&self);
    fn assignment(&self) -> HashSet<TopicPartition>;
    fn position(&self, partition: TopicPartition) -> i64;
    fn end_offsets(&self, partitions: HashSet<TopicPartition>) -> HashMap<TopicPartition, i64>;
}
```

### 9.3 MockAdminClient
```rust
// 实现KafkaAdmin trait
pub struct MockAdminClient {
    // 内存存储主题元数据
    topics: Arc<Mutex<HashMap<String, TopicDescription>>>,
    // 支持主题CRUD操作
}

impl KafkaAdmin for MockAdminClient {
    fn create_topics(&self, topics: Vec<NewTopic>, options: CreateTopicsOptions) -> CreateTopicsResult;
    fn describe_topics(&self, topic_names: Vec<String>, options: DescribeTopicsOptions) -> DescribeTopicsResult;
    fn describe_configs(&self, resources: Vec<ConfigResource>, options: DescribeConfigsOptions) -> DescribeConfigsResult;
    fn list_offsets(&self, offset_specs: HashMap<TopicPartition, OffsetSpec>, options: ListOffsetsOptions) -> ListOffsetsResult;
    fn list_topics(&self, options: ListTopicsOptions) -> ListTopicsResult;
}
```

---

## 十、模块依赖关系矩阵

| 模块 | 依赖的模块 | 被依赖的模块 | 可并行开发 |
|------|-----------|-----------|-----------|
| **kafka-clients-trait** | 无 | kafka-clients-mock, connect-api, connect-runtime-core, connect-mirror-client | ✅ 阶段1 |
| **kafka-clients-mock** | kafka-clients-trait | connect-runtime-core (测试用） | ✅ 阶段1 |
| **connect-api** | kafka-clients-trait | connect-transforms, connect-runtime-core, connect-mirror, connect-mirror-client, connect-basic-auth-extension | ✅ 阶段1 |
| **connect-transforms** | connect-api | connect-runtime-core | ✅ 阶段1 |
| **connect-runtime-core** | connect-api, connect-transforms, kafka-clients-trait | connect-runtime-distributed, connect-mirror | ❌ 阶段2 |
| **connect-runtime-distributed** | connect-runtime-core | connect-mirror | ❌ 阶段3 |
| **connect-mirror-client** | kafka-clients-trait | connect-mirror | ✅ 阶段1 |
| **connect-mirror** | connect-runtime-core, connect-mirror-client | 无 | ❌ 阶段4 |
| **connect-basic-auth-extension** | connect-api | connect-runtime-core | ✅ 阶段1 |

---

## 十一、可并行开发的任务组

### 阶段1 - 完全并行（10个任务）
```
任务组A (无依赖）:
├── kafka-clients-trait
├── kafka-clients-mock
├── connect-api
├── connect-transforms
├── connect-mirror-client
└── connect-basic-auth-extension

所有6个crate可以同时开发，互不阻塞！
```

### 阶段2 - 串行（1个任务）
```
任务组B (依赖connect-api, connect-transforms, kafka-clients-trait):
└── connect-runtime-core

必须等待connect-api和connect-transforms完成
```

### 阶段3 - 串行（1个任务）
```
任务组C (依赖connect-runtime-core):
└── connect-runtime-distributed

必须等待connect-runtime-core完成
```

### 阶段4 - 串行（1个任务）
```
任务组D (依赖connect-runtime-core, connect-mirror-client):
└── connect-mirror

必须等待connect-runtime-core和connect-mirror-client完成
```

---

## 十二、关键设计决策

### 12.1 泛型处理
- Java使用`R extends ConnectRecord<R>>`递归泛型
- Rust使用`R: ConnectRecord`约束，避免递归泛型
- 使用`Box<dyn Any>`处理运行时多态

### 12.2 错误处理
- Java使用checked exception
- Rust使用`Result<T, Box<dyn Error>>`
- 使用`anyhow`或`thiserror`定义错误类型

### 12.3 异步模型
- Java使用`ExecutorService`线程池
- Rust使用`tokio`异步运行时
- 使用`tokio::task::spawn`替代线程池

### 12.4 生命周期管理
- Java使用`Closeable`接口
- Rust使用`Drop` trait实现RAII

### 12.5 插件系统
- Java使用`ServiceLoader`和反射
- Rust使用`libloading`和`abi`
- 插件注册函数：`extern "C" fn register_plugin() -> Box<dyn Plugin>`

---

## 十三、下一步行动

1. **审阅本契约文档**: 确认所有API接口定义正确
2. **开始阶段1**: 生成所有crate的API骨架
3. **并行开发**: 根据可并行任务组进行开发
4. **持续验证**: 每个crate完成后编译和测试

---

**文档结束**
