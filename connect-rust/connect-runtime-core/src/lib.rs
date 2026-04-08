//! Kafka Connect Runtime Core
//!
//! This crate provides core runtime functionality for Kafka Connect.

pub mod config;
pub mod embedded_cluster;
pub mod errors;
pub mod herder;
pub mod isolation;
pub mod kafka_config_store;
pub mod kafka_offset_store;
pub mod kafka_status_store;
pub mod metrics;
pub mod plugin_macro;
pub mod plugin_registry;
pub mod storage;
pub mod worker;

// Re-export main types from worker module
pub use worker::{
    Callback as WorkerCallback, CloseableConnectorContext,
    ConnectorConfig as WorkerConnectorConfig, ConnectorOffsets,
    ConnectorState as WorkerConnectorState, ConnectorStatus as WorkerConnectorStatus,
    ConnectorStatusListener, ConnectorTaskId as WorkerConnectorTaskId, Message as WorkerMessage,
    TargetState as WorkerTargetState, Worker, WorkerConfig as WorkerWorkerConfig,
    WorkerConfigTransformer, WorkerMetrics as WorkerWorkerMetrics,
};

// Re-export main types from herder module
pub use herder::{
    Callback as HerderCallback, ConnectorStateInfo, Created, DistributedHerder, Herder,
    InternalRequestSignature, Message as HerderMessage, PluginInfo, Plugins, RestartRequest,
    StandaloneHerder, TaskState as HerderTaskState, VersionRange,
};

// Re-export main types from storage module
pub use storage::{
    Callback as StorageCallback, ConfigBackingStore, ConnectorState as StorageConnectorState,
    ConnectorStatus as StorageConnectorStatus, ConnectorTaskId as StorageConnectorTaskId,
    OffsetBackingStore, StatusBackingStore, TargetState as StorageTargetState,
    TaskState as StorageTaskState, TaskStatus, TopicState, TopicStatus,
};

// Re-export main types from kafka_offset_store module module
pub use kafka_offset_store::{Closeable as OffsetStoreCloseable, KafkaOffsetBackingStore};

// Re-export main types from kafka_status_store module module
pub use kafka_status_store::KafkaStatusBackingStore;

// Re-export main types from kafka_config_store module module
pub use connect_api::Closeable as ConfigStoreCloseable;
pub use kafka_config_store::KafkaConfigBackingStore;

// Re-export main types from config module
pub use config::{
    ConfigMerger, ConfigProvider, ConfigTransformer, ConfigValidationError, ConfigValidator,
    ConnectorConfig as ConfigConnectorConfig, DefaultConfigMerger, TaskConfig,
    WorkerConfig as ConfigWorkerConfig,
};

// Re-export main types from metrics module
pub use metrics::{
    ConnectMetrics as MetricsConnectMetrics, ConnectorMetrics, DefaultMetricsReporter,
    MetricsReporter, TaskMetrics, WorkerMetrics as MetricsWorkerMetrics,
};

// Re-export main types from isolation module
pub use isolation::{
    IsolationLevel, IsolationPolicy, NoIsolationPolicy, ProcessIsolationPolicy,
    ThreadIsolationPolicy,
};

// Re-export main types from errors module
pub use errors::{ConnectRuntimeError, ConnectRuntimeResult, ErrorCode};
