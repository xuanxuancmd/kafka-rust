//! Connect-specific trait definitions for Kafka Connect runtime
//!
//! This module provides trait interfaces for Connect runtime components.

use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

/// Trait for Kafka-based log storage
///
/// This trait provides a shared, compacted log of records stored in Kafka.
/// It allows clients to consume and write to this log, and manages underlying
/// Kafka producer and consumer interactions.
pub trait KafkaBasedLog<K, V> {
    /// Start log processing
    ///
    /// Starts log processing, which includes creating the producer and consumer,
    /// assigning partitions, seeking to the beginning of the log, and starting
    /// the background thread.
    fn start(&mut self) -> Result<(), Box<dyn Error>>;

    /// Start log processing with error reporting control
    ///
    /// An overloaded version of start() that allows specifying whether errors
    /// should be reported to the callback.
    fn start_with_error_reporting(
        &mut self,
        report_errors_to_callback: bool,
    ) -> Result<(), Box<dyn Error>>;

    /// Stop log processing
    ///
    /// Stops log processing, signals the background thread to terminate,
    /// joins the thread, and closes the internal producer and consumer.
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    /// Read to the end of the log with callback
    ///
    /// Flushes any outstanding writes and then reads to the current end of the log,
    /// invoking the provided callback once the end is reached.
    /// This method is asynchronous.
    fn read_to_end(&self, callback: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>>;

    /// Flush pending writes
    ///
    /// Flushes the underlying producer to ensure all pending writes are sent.
    fn flush(&self) -> Result<(), Box<dyn Error>>;

    /// Send a record to the log
    ///
    /// Sends a record asynchronously to the configured topic without using a
    /// producer callback. This method is for backward compatibility.
    fn send(&self, key: K, value: V) -> Result<(), Box<dyn Error>>;

    /// Send a record to the log and return a future
    ///
    /// Sends a record asynchronously to the configured topic and returns a
    /// Future for the RecordMetadata.
    fn send_with_future(
        &self,
        key: K,
        value: V,
    ) -> Result<Box<dyn Future<RecordMetadata>>, Box<dyn Error>>;

    /// Send a record with callback
    ///
    /// Sends a record asynchronously with a producer callback.
    fn send_with_callback(
        &self,
        key: K,
        value: V,
        callback: Box<dyn ProducerCallback>,
    ) -> Result<(), Box<dyn Error>>;

    /// Send a record with receipt
    ///
    /// Sends a record and returns a Future for the RecordMetadata.
    fn send_with_receipt(
        &self,
        key: K,
        value: V,
    ) -> Result<Box<dyn Future<RecordMetadata>>, Box<dyn Error>>;

    /// Send a record with receipt and callback
    ///
    /// Sends a record with a producer callback and returns a Future for the RecordMetadata.
    fn send_with_receipt_and_callback(
        &self,
        key: K,
        value: V,
        callback: Box<dyn ProducerCallback>,
    ) -> Result<Box<dyn Future<RecordMetadata>>, Box<dyn Error>>;

    /// Get partition count
    ///
    /// Returns the number of partitions in the topic.
    fn partition_count(&self) -> i32;

    /// Check if stopped
    ///
    /// Returns true if the log has been stopped.
    fn is_stopped(&self) -> bool;
}

/// Callback trait for async operations
pub trait Callback<T> {
    /// Called when operation completes
    fn call(&self, result: T);
}

/// Future trait for async operations
pub trait Future<T> {
    /// Get the result, blocking if necessary
    fn get(&self) -> Result<T, Box<dyn Error>>;

    /// Check if the operation is complete
    fn is_done(&self) -> bool;

    /// Cancel the operation
    fn cancel(&self, may_interrupt_if_running: bool) -> bool;
}

/// Record metadata
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// Topic name
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Offset
    pub offset: i64,
    /// Timestamp
    pub timestamp: Option<i64>,
    /// Serialized key size
    pub serialized_key_size: i32,
    /// Serialized value size
    pub serialized_value_size: i32,
}

/// Producer callback trait
pub trait ProducerCallback {
    /// Called when the metadata is available
    fn on_completion(&self, metadata: Option<RecordMetadata>, exception: Option<Box<dyn Error>>);
}

/// Time trait for time-related operations
pub trait Time {
    /// Get current time in milliseconds
    fn milliseconds(&self) -> i64;

    /// Get current time in nanoseconds
    fn nanoseconds(&self) -> i64;

    /// Sleep for the specified duration
    fn sleep(&self, duration: Duration);

    /// Get current time as high-resolution timestamp
    fn hi_res_clock_ms(&self) -> i64;
}

/// Topic admin trait
pub trait TopicAdmin {
    /// Create a topic
    fn create_topic(
        &self,
        name: String,
        num_partitions: i32,
        replication_factor: i16,
        config: HashMap<String, String>,
    ) -> Result<(), Box<dyn Error>>;

    /// Delete a topic
    fn delete_topic(&self, name: String) -> Result<(), Box<dyn Error>>;

    /// Check if a topic exists
    fn topic_exists(&self, name: String) -> Result<bool, Box<dyn Error>>;

    /// Get topic metadata
    fn get_topic_metadata(&self, name: String) -> Result<TopicMetadata, Box<dyn Error>>;
}

/// Topic metadata
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    /// Topic name
    pub name: String,
    /// Number of partitions
    pub num_partitions: i32,
    /// Replication factor
    pub replication_factor: i16,
    /// Is internal topic
    pub is_internal: bool,
}

/// Consumer record
#[derive(Debug, Clone)]
pub struct ConsumerRecord<K, V> {
    /// Topic name
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Offset
    pub offset: i64,
    /// Key
    pub key: Option<K>,
    /// Value
    pub value: Option<V>,
    /// Timestamp
    pub timestamp: Option<i64>,
    /// Timestamp type
    pub timestamp_type: Option<TimestampType>,
}

/// Timestamp type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    /// No timestamp
    NoTimestampType,
    /// Create time
    CreateTime,
    /// Log append time
    LogAppendTime,
}

/// Supplier trait for lazy initialization
pub trait Supplier<T> {
    /// Get the supplied value
    fn get(&self) -> T;
}

/// Consumer trait for processing elements
pub trait Consumer<T> {
    /// Accept a value
    fn accept(&self, value: T);
}

/// Predicate trait for filtering
pub trait Predicate<T> {
    /// Test if -> value matches predicate
    fn test(&self, value: &T) -> bool;
}

/// Session key for REST request verification
///
/// Represents a session key generated by cluster leader when
/// ConnectProtocolCompatibility.SESSIONED is used, and it's used to
/// verify internal REST requests between workers.
#[derive(Debug, Clone)]
pub struct SessionKey {
    /// The actual secret key (Base64 encoded when serialized)
    pub key: String,
    /// Timestamp when session key was created
    pub creation_timestamp: i64,
}

impl SessionKey {
    /// Create a new session key
    pub fn new(key: String, creation_timestamp: i64) -> Self {
        Self {
            key,
            creation_timestamp,
        }
    }
}

/// Restart request for connector and tasks
///
/// Encapsulates information about a request to restart a connector
/// and/or its tasks.
#[derive(Debug, Clone)]
pub struct RestartRequest {
    /// Name of the connector to be restarted
    pub connector_name: String,
    /// Whether only failed tasks/connectors should be restarted
    pub only_failed: bool,
    /// Whether tasks associated with connector should also be restarted
    pub include_tasks: bool,
}

impl RestartRequest {
    /// Create a new restart request
    pub fn new(connector_name: String, only_failed: bool, include_tasks: bool) -> Self {
        Self {
            connector_name,
            only_failed,
            include_tasks,
        }
    }
}

/// Target state for a connector
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetState {
    Started,
    Stopped,
    Paused,
    Destroyed,
}

/// Connector state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorState {
    Uninitialized,
    Running,
    Paused,
    Stopped,
    Failed,
    Destroyed,
}

/// Task state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Running,
    Paused,
    Stopped,
    Failed,
    Destroyed,
}

/// Update listener trait
///
/// Trait for listening to updates from config backing store.
pub trait UpdateListener {
    /// Called when config is updated
    fn on_config_update(&self);

    /// Called when connector config is updated
    fn on_connector_config_update(&self, connector: String);

    /// Called when task config is updated
    fn on_task_config_update(&self, connector: String, task_id: i32);
}

/// Source task offset committer trait
///
/// Trait for committing offsets from source tasks.
pub trait SourceTaskOffsetCommitter {
    /// Commit offsets for a task
    fn commit_offsets(
        &self,
        task_id: String,
        offsets: HashMap<String, i64>,
    ) -> Result<(), Box<dyn Error>>;

    /// Get current offsets for a task
    fn get_offsets(&self, task_id: String) -> Result<HashMap<String, i64>, Box<dyn Error>>;
}

/// Connector client config override policy trait
///
/// Defines what client configurations can be overridden by connector.
pub trait ConnectorClientConfigOverridePolicy {
    /// Check if a config key can be overridden
    fn can_override(&self, config_key: &str) -> bool;

    /// Get policy name
    fn policy_name(&self) -> String;
}

/// Herder trait
///
/// Main interface for managing connectors and tasks in a Connect cluster.
pub trait Herder {
    /// Start the herder
    fn start(&mut self) -> Result<(), Box<dyn Error>>;

    /// Stop the herder
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    /// Create a connector
    fn create_connector(
        &mut self,
        name: String,
        config: HashMap<String, String>,
    ) -> Result<String, Box<dyn Error>>;

    /// Delete a connector
    fn delete_connector(&mut self, name: String) -> Result<(), Box<dyn Error>>;

    /// Get connector info
    fn connector_info(&self, name: String) -> Result<ConnectorInfo, Box<dyn Error>>;

    /// Get all connector names
    fn connector_names(&self) -> Vec<String>;

    /// Restart a connector
    fn restart_connector(&mut self, name: String) -> Result<(), Box<dyn Error>>;
}

/// Connector info
#[derive(Debug, Clone)]
pub struct ConnectorInfo {
    /// Connector name
    pub name: String,
    /// Connector config
    pub config: HashMap<String, String>,
    /// Connector state
    pub state: ConnectorState,
    /// Connector type
    pub connector_type: ConnectorType,
}

/// Connector type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorType {
    Source,
    Sink,
}

/// Plugins trait
///
/// Manages loading and instantiation of Connect plugins.
pub trait Plugins {
    /// Get plugin info
    fn get_plugin_info(&self, plugin_name: String) -> Result<PluginInfo, Box<dyn Error>>;

    /// Get all plugin names
    fn plugin_names(&self) -> Vec<String>;

    /// Reload plugins
    fn reload_plugins(&mut self) -> Result<(), Box<dyn Error>>;
}

/// Plugin info
#[derive(Debug, Clone)]
pub struct PluginInfo {
    /// Plugin name
    pub name: String,
    /// Plugin class name
    pub class_name: String,
    /// Plugin type
    pub plugin_type: PluginType,
    /// Plugin version
    pub version: String,
}

/// Plugin type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginType {
    Connector,
    Converter,
    Transformation,
}

/// Connect metrics trait
///
/// Provides metrics for the Connect worker.
pub trait ConnectMetrics {
    /// Get metrics snapshot
    fn snapshot(&self) -> HashMap<String, f64>;

    /// Get connector metrics
    fn connector_metrics(&self, connector: String) -> ConnectorMetrics;

    /// Get task metrics
    fn task_metrics(&self, connector: String, task_id: i32) -> TaskMetrics;
}

/// Connector metrics
#[derive(Debug, Clone)]
pub struct ConnectorMetrics {
    /// Connector name
    pub connector_name: String,
    /// Connector state
    pub state: String,
    /// Worker ID
    pub worker_id: String,
    /// Metrics
    pub metrics: HashMap<String, f64>,
}

/// Task metrics
#[derive(Debug, Clone)]
pub struct TaskMetrics {
    /// Connector name
    pub connector_name: String,
    /// Task ID
    pub task_id: i32,
    /// Task state
    pub state: String,
    /// Worker ID
    pub worker_id: String,
    /// Metrics
    pub metrics: HashMap<String, f64>,
}

/// Worker metrics group trait
///
/// Provides metrics specific to worker.
pub trait WorkerMetricsGroup {
    /// Get worker metrics
    fn metrics(&self) -> HashMap<String, f64>;

    /// Update metric value
    fn update_metric(&mut self, name: String, value: f64);

    /// Increment metric value
    fn increment_metric(&mut self, name: String, delta: f64);
}

/// Connector status metrics group trait
///
/// Provides metrics for connector statuses.
pub trait ConnectorStatusMetricsGroup {
    /// Get connector status metrics
    fn connector_status_metrics(&self, connector: String) -> HashMap<String, f64>;

    /// Update connector status
    fn update_connector_status(&mut self, connector: String, status: ConnectorState);

    /// Get all connector statuses
    fn all_connector_statuses(&self) -> HashMap<String, ConnectorState>;
}

/// Admin factory trait
///
/// Factory function to create Admin clients.
pub trait AdminFactory {
    /// Create an admin client
    fn create_admin(&self, config: HashMap<String, String>) -> Result<(), Box<dyn Error>>;
}

/// Cluster config state
///
/// Represents the current configuration state of the cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfigState {
    /// Connector configurations
    pub connector_configs: HashMap<String, HashMap<String, String>>,
    /// Task configurations
    pub task_configs: HashMap<String, Vec<HashMap<String, String>>>,
    /// Target states
    pub target_states: HashMap<String, TargetState>,
    /// Session key
    pub session_key: Option<SessionKey>,
}

impl ClusterConfigState {
    /// Create a new cluster config state
    pub fn new() -> Self {
        Self {
            connector_configs: HashMap::new(),
            task_configs: HashMap::new(),
            target_states: HashMap::new(),
            session_key: None,
        }
    }

    /// Get connector config
    pub fn get_connector_config(&self, connector: &str) -> Option<&HashMap<String, String>> {
        self.connector_configs.get(connector)
    }

    /// Get task configs
    pub fn get_task_configs(&self, connector: &str) -> Option<&Vec<HashMap<String, String>>> {
        self.task_configs.get(connector)
    }

    /// Get target state
    pub fn get_target_state(&self, connector: &str) -> Option<TargetState> {
        self.target_states.get(connector).copied()
    }
}
