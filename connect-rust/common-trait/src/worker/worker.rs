// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Worker trait for Kafka Connect runtime.
//!
//! The Worker is the core component of Kafka Connect that runs connectors and tasks.
//! It manages the lifecycle of connectors and tasks, handles configuration,
//! and coordinates with the herder for cluster management.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.Worker` in Java.

use crate::errors::ConnectError;
use crate::worker::{Callback, ConnectorOffsets, ConnectorTaskId, Message, TargetState};
use std::collections::{HashMap, HashSet};
use std::future::Future;

/// Connector context for connector runtime.
/// Corresponds to `org.apache.kafka.connect.runtime.CloseableConnectorContext` in Java.
pub trait ConnectorContext: Send + Sync {
    /// Requests a reconfiguration of the connector.
    fn request_reconfigure(&self);

    /// Requests a task reconfiguration for the connector.
    fn request_task_reconfiguration(&self);
}

/// Worker config trait for worker configuration.
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerConfig` in Java.
pub trait WorkerConfig: Send + Sync {
    /// Returns the worker ID.
    fn worker_id(&self) -> &str;

    /// Returns the bootstrap servers.
    fn bootstrap_servers(&self) -> &str;

    /// Returns the offsets topic.
    fn offsets_topic(&self) -> &str;

    /// Returns the Kafka cluster ID.
    fn kafka_cluster_id(&self) -> &str;

    /// Returns whether topic creation is enabled.
    fn topic_creation_enable(&self) -> bool;

    /// Returns whether exactly-once source support is enabled.
    fn exactly_once_source_enabled(&self) -> bool;

    /// Returns whether connector offsets topics are permitted.
    fn connector_offsets_topics_permitted(&self) -> bool;

    /// Returns the group ID for this worker.
    fn group_id(&self) -> &str;
}

/// Worker config transformer trait.
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerConfigTransformer` in Java.
pub trait WorkerConfigTransformer: Send + Sync {
    /// Transforms the given configuration.
    fn transform(&self, config: HashMap<String, String>) -> HashMap<String, String>;

    /// Closes the transformer.
    fn close(&self);
}

/// Herder trait for worker-herder coordination.
/// Corresponds to `org.apache.kafka.connect.runtime.Herder` in Java.
pub trait Herder: Send + Sync {
    /// Returns the status backing store.
    fn status_backing_store(&self) -> &dyn StatusBackingStore;

    /// Returns the task status for the given task ID.
    fn task_status(&self, id: &ConnectorTaskId) -> String;
}

/// Status backing store trait.
/// Corresponds to `org.apache.kafka.connect.storage.StatusBackingStore` in Java.
pub trait StatusBackingStore: Send + Sync {}

/// Converter trait for key/value conversion.
/// Corresponds to `org.apache.kafka.connect.storage.Converter` in Java.
pub trait Converter: Send + Sync {
    /// Converts from connect data to bytes.
    fn from_connect_data(
        &self,
        topic: &str,
        schema: Option<&dyn Schema>,
        value: &dyn Value,
    ) -> Vec<u8>;

    /// Converts from bytes to connect data.
    fn to_connect_data(&self, topic: &str, bytes: &[u8]) -> SchemaAndValue;
}

/// Schema trait.
pub trait Schema: Send + Sync {}

/// Value trait.
pub trait Value: Send + Sync {}

/// Schema and value pair.
pub struct SchemaAndValue {
    schema: Option<Box<dyn Schema>>,
    value: Box<dyn Value>,
}

/// Plugins trait for plugin management.
/// Corresponds to `org.apache.kafka.connect.runtime.isolation.Plugins` in Java.
pub trait Plugins: Send + Sync {
    /// Returns a new connector instance.
    fn new_connector(
        &self,
        connector_class: &str,
    ) -> Result<Box<dyn ConnectorInstance>, ConnectError>;

    /// Returns a new task instance.
    fn new_task(&self, task_class: &str) -> Result<Box<dyn TaskInstance>, ConnectError>;

    /// Returns a new converter instance.
    fn new_converter(
        &self,
        config: &dyn WorkerConfig,
        config_key: &str,
    ) -> Option<Box<dyn Converter>>;

    /// Returns a new header converter instance.
    fn new_header_converter(
        &self,
        config: &dyn WorkerConfig,
        config_key: &str,
    ) -> Option<Box<dyn HeaderConverter>>;

    /// Returns the class loader for a connector.
    fn connector_loader(&self, connector_class: &str) -> Box<dyn ClassLoader>;
}

/// Header converter trait.
pub trait HeaderConverter: Send + Sync {}

/// Connector instance trait.
pub trait ConnectorInstance: Send + Sync {
    /// Returns the connector version.
    fn version(&self) -> &str;

    /// Returns the connector class name.
    fn class_name(&self) -> &str;
}

/// Task instance trait.
pub trait TaskInstance: Send + Sync {
    /// Returns the task version.
    fn version(&self) -> &str;

    /// Returns the task class name.
    fn class_name(&self) -> &str;
}

/// Class loader trait.
pub trait ClassLoader: Send + Sync {}

/// Connect metrics trait.
/// Corresponds to `org.apache.kafka.connect.runtime.ConnectMetrics` in Java.
pub trait ConnectMetrics: Send + Sync {
    /// Returns the metrics registry.
    fn registry(&self) -> &dyn MetricsRegistry;

    /// Stops the metrics.
    fn stop(&self);

    /// Wraps a converter with metrics.
    fn wrap_converter(
        &self,
        converter: Box<dyn Converter>,
        id: &ConnectorTaskId,
        is_key: bool,
    ) -> Box<dyn Converter>;

    /// Wraps a header converter with metrics.
    fn wrap_header_converter(
        &self,
        converter: Box<dyn HeaderConverter>,
        id: &ConnectorTaskId,
    ) -> Box<dyn HeaderConverter>;
}

/// Metrics registry trait.
pub trait MetricsRegistry: Send + Sync {
    /// Returns the worker group name.
    fn worker_group_name(&self) -> &str;

    /// Returns the connector tag name.
    fn connector_tag_name(&self) -> &str;
}

/// Connector config trait.
/// Corresponds to `org.apache.kafka.connect.runtime.ConnectorConfig` in Java.
pub trait ConnectorConfig: Send + Sync {
    /// Returns the max tasks count.
    fn tasks_max(&self) -> i32;

    /// Returns the originals as strings.
    fn originals_strings(&self) -> HashMap<String, String>;

    /// Returns the originals as a map.
    fn originals(&self) -> HashMap<String, serde_json::Value>;

    /// Returns the error retry timeout.
    fn error_retry_timeout(&self) -> i64;

    /// Returns the error max delay in milliseconds.
    fn error_max_delay_in_millis(&self) -> i64;

    /// Returns the error tolerance type.
    fn error_tolerance_type(&self) -> ErrorToleranceType;

    /// Returns whether to enforce max tasks.
    fn enforce_tasks_max(&self) -> bool;
}

/// Error tolerance type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorToleranceType {
    None,
    All,
}

/// Cluster config state trait.
/// Corresponds to `org.apache.kafka.connect.storage.ClusterConfigState` in Java.
pub trait ClusterConfigState: Send + Sync {
    /// Returns the task count for a connector.
    fn task_count(&self, connector: &str) -> i32;
}

/// Offset backing store trait.
/// Corresponds to `org.apache.kafka.connect.storage.OffsetBackingStore` in Java.
pub trait OffsetBackingStore: Send + Sync {
    /// Starts the offset backing store.
    fn start(&self);

    /// Stops the offset backing store.
    fn stop(&self);

    /// Configures the offset backing store.
    fn configure(&self, config: &dyn WorkerConfig);
}

/// Connector offset backing store trait.
/// Corresponds to `org.apache.kafka.connect.storage.ConnectorOffsetBackingStore` in Java.
pub trait ConnectorOffsetBackingStore: OffsetBackingStore {
    /// Returns the connector partitions.
    fn connector_partitions(&self, connector: &str) -> HashSet<HashMap<String, serde_json::Value>>;
}

/// Offset storage reader trait.
/// Corresponds to `org.apache.kafka.connect.storage.OffsetStorageReader` in Java.
pub trait OffsetStorageReader: Send + Sync {
    /// Returns the offsets for the given partitions.
    fn offsets(
        &self,
        partitions: HashSet<HashMap<String, serde_json::Value>>,
    ) -> HashMap<HashMap<String, serde_json::Value>, Option<HashMap<String, serde_json::Value>>>;
}

/// Admin client trait.
pub trait AdminClient: Send + Sync {
    /// Lists consumer group offsets.
    fn list_consumer_group_offsets(&self, group_id: &str) -> HashMap<String, serde_json::Value>;

    /// Alters consumer group offsets.
    fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: HashMap<String, serde_json::Value>,
    ) -> Result<(), ConnectError>;

    /// Deletes consumer group offsets.
    fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: HashSet<String>,
    ) -> Result<(), ConnectError>;

    /// Deletes consumer groups.
    fn delete_consumer_groups(&self, group_ids: HashSet<String>) -> Result<(), ConnectError>;

    /// Fences producers.
    fn fence_producers(&self, transactional_ids: Vec<String>) -> Result<(), ConnectError>;
}

/// Worker trait for Kafka Connect runtime.
///
/// The Worker is responsible for:
/// - Starting and stopping connectors
/// - Starting and stopping tasks (source and sink)
/// - Managing connector and task configurations
/// - Handling connector/task lifecycle events
/// - Managing offsets for source connectors
/// - Coordinating with the herder for cluster management
///
/// This trait corresponds to `org.apache.kafka.connect.runtime.Worker` in Java.
pub trait Worker: Send + Sync {
    // ===== Lifecycle methods =====

    /// Starts the worker.
    /// This initializes the global offset backing store and source task offset committer.
    fn start(&self);

    /// Stops the worker.
    /// This gracefully shuts down connectors and tasks, and closes resources.
    fn stop(&self);

    // ===== Configuration methods =====

    /// Returns the worker configuration.
    fn config(&self) -> &dyn WorkerConfig;

    /// Returns the worker config transformer.
    fn config_transformer(&self) -> &dyn WorkerConfigTransformer;

    // ===== Connector lifecycle methods =====

    /// Starts a connector managed by this worker.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    /// * `conn_props` - The connector properties
    /// * `ctx` - The connector runtime context
    /// * `status_listener` - Listener for connector status transitions
    /// * `initial_state` - The initial state of the connector
    /// * `on_connector_state_change` - Callback invoked when state change completes
    fn start_connector(
        &self,
        conn_name: &str,
        conn_props: HashMap<String, String>,
        ctx: &dyn ConnectorContext,
        status_listener: &dyn crate::worker::ConnectorStatusListener,
        initial_state: TargetState,
        on_connector_state_change: &dyn Callback<TargetState>,
    );

    /// Returns true if the connector is a sink connector.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    fn is_sink_connector(&self, conn_name: &str) -> bool;

    /// Gets a list of updated task properties for the tasks of this connector.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    /// * `conn_config` - The connector configuration
    fn connector_task_configs(
        &self,
        conn_name: &str,
        conn_config: &dyn ConnectorConfig,
    ) -> Vec<HashMap<String, String>>;

    /// Stops asynchronously all the worker's connectors and awaits their termination.
    fn stop_and_await_connectors(&self);

    /// Stops asynchronously a collection of connectors and awaits their termination.
    ///
    /// # Arguments
    /// * `ids` - The collection of connector names to stop
    fn stop_and_await_connectors_batch(&self, ids: Vec<String>);

    /// Stops a connector and awaits its termination.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    fn stop_and_await_connector(&self, conn_name: &str);

    /// Stops a connector without awaiting termination.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    fn stop_connector(&self, conn_name: &str);

    /// Awaits stop on all connectors.
    fn await_stop_on_all_connectors(&self);

    // ===== Connector status/query methods =====

    /// Gets the IDs of the connectors currently running in this worker.
    fn connector_names(&self) -> HashSet<String>;

    /// Returns true if a connector with the given name is running.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    fn is_running(&self, conn_name: &str) -> bool;

    /// Returns the version of the connector.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    fn connector_version(&self, conn_name: &str) -> Option<String>;

    /// Gets the connector instance for the given connector name.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    fn get_connector(&self, conn_name: &str) -> Option<&dyn ConnectorInstance>;

    /// Reconfigures a connector with new properties.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    /// * `new_props` - The new connector properties
    fn reconfigure_connector(&self, conn_name: &str, new_props: HashMap<String, String>);

    /// Returns true if a connector is currently stopping.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    fn is_stopping(&self, conn_name: &str) -> bool;

    /// Returns the current state of a connector.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    fn state(&self, conn_name: &str) -> TargetState;

    // ===== Task lifecycle methods =====

    /// Starts a sink task managed by this worker.
    ///
    /// # Arguments
    /// * `id` - The task ID
    /// * `config_state` - The cluster config state
    /// * `conn_props` - The connector properties
    /// * `task_props` - The task properties
    /// * `status_listener` - Listener for task status transitions
    /// * `initial_state` - The initial state
    fn start_sink_task(
        &self,
        id: &ConnectorTaskId,
        config_state: &dyn ClusterConfigState,
        conn_props: HashMap<String, String>,
        task_props: HashMap<String, String>,
        status_listener: &dyn crate::worker::TaskStatusListener,
        initial_state: TargetState,
    ) -> bool;

    /// Starts a source task managed by this worker.
    ///
    /// # Arguments
    /// * `id` - The task ID
    /// * `config_state` - The cluster config state
    /// * `conn_props` - The connector properties
    /// * `task_props` - The task properties
    /// * `status_listener` - Listener for task status transitions
    /// * `initial_state` - The initial state
    fn start_source_task(
        &self,
        id: &ConnectorTaskId,
        config_state: &dyn ClusterConfigState,
        conn_props: HashMap<String, String>,
        task_props: HashMap<String, String>,
        status_listener: &dyn crate::worker::TaskStatusListener,
        initial_state: TargetState,
    ) -> bool;

    /// Starts an exactly-once source task managed by this worker.
    ///
    /// # Arguments
    /// * `id` - The task ID
    /// * `config_state` - The cluster config state
    /// * `conn_props` - The connector properties
    /// * `task_props` - The task properties
    /// * `status_listener` - Listener for task status transitions
    /// * `initial_state` - The initial state
    /// * `pre_producer_check` - Preflight check before producer initialization
    /// * `post_producer_check` - Preflight check after producer initialization
    fn start_exactly_once_source_task(
        &self,
        id: &ConnectorTaskId,
        config_state: &dyn ClusterConfigState,
        conn_props: HashMap<String, String>,
        task_props: HashMap<String, String>,
        status_listener: &dyn crate::worker::TaskStatusListener,
        initial_state: TargetState,
        pre_producer_check: Option<Box<dyn Fn() + Send + Sync>>,
        post_producer_check: Option<Box<dyn Fn() + Send + Sync>>,
    ) -> bool;

    /// Stops asynchronously all the worker's tasks and awaits their termination.
    fn stop_and_await_tasks(&self);

    /// Stops asynchronously a collection of tasks and awaits their termination.
    ///
    /// # Arguments
    /// * `ids` - The collection of task IDs to stop
    fn stop_and_await_tasks_batch(&self, ids: Vec<ConnectorTaskId>);

    /// Stops a task and awaits its termination.
    ///
    /// # Arguments
    /// * `task_id` - The task ID
    fn stop_and_await_task(&self, task_id: &ConnectorTaskId);

    /// Stops a task without awaiting termination.
    ///
    /// # Arguments
    /// * `task_id` - The task ID
    fn stop_task(&self, task_id: &ConnectorTaskId);

    /// Starts a task managed by this worker.
    ///
    /// This is a general task startup method that delegates to
    /// start_sink_task or start_source_task based on connector type.
    ///
    /// # Arguments
    /// * `id` - The task ID
    /// * `config_state` - The cluster config state
    /// * `conn_props` - The connector properties
    /// * `task_props` - The task properties
    /// * `status_listener` - Listener for task status transitions
    /// * `initial_state` - The initial state
    fn start_task(
        &self,
        id: &ConnectorTaskId,
        config_state: &dyn ClusterConfigState,
        conn_props: HashMap<String, String>,
        task_props: HashMap<String, String>,
        status_listener: &dyn crate::worker::TaskStatusListener,
        initial_state: TargetState,
    ) -> bool;

    /// Awaits stop on all tasks.
    fn await_stop_on_all_tasks(&self);

    // ===== Task status/query methods =====

    /// Gets the IDs of the tasks currently running in this worker.
    fn task_ids(&self) -> HashSet<ConnectorTaskId>;

    /// Returns the version of the task.
    ///
    /// # Arguments
    /// * `task_id` - The task ID
    fn task_version(&self, task_id: &ConnectorTaskId) -> Option<String>;

    // ===== State transition methods =====

    /// Sets the target state for a connector and its tasks.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    /// * `state` - The target state
    /// * `state_change_callback` - Callback invoked when state change completes
    fn set_target_state(
        &self,
        conn_name: &str,
        state: TargetState,
        state_change_callback: &dyn Callback<TargetState>,
    );

    // ===== Offset management methods =====

    /// Gets the current offsets for a connector.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    /// * `connector_config` - The connector configuration
    /// * `cb` - Callback invoked when offset retrieval completes
    fn connector_offsets(
        &self,
        conn_name: &str,
        connector_config: HashMap<String, String>,
        cb: &dyn Callback<ConnectorOffsets>,
    );

    /// Modifies (alter/reset) a connector's offsets.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    /// * `connector_config` - The connector configuration
    /// * `offsets` - The offsets to write (None for reset)
    /// * `cb` - Callback invoked when modification completes
    fn modify_connector_offsets(
        &self,
        conn_name: &str,
        connector_config: HashMap<String, String>,
        offsets: Option<
            HashMap<HashMap<String, serde_json::Value>, HashMap<String, serde_json::Value>>,
        >,
        cb: &dyn Callback<Message>,
    );

    // ===== Zombie fencing methods =====

    /// Fences out zombie producers for a source connector.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    /// * `num_tasks` - The number of tasks to fence
    /// * `conn_props` - The connector properties
    fn fence_zombies(
        &self,
        conn_name: &str,
        num_tasks: i32,
        conn_props: HashMap<String, String>,
    ) -> impl Future<Output = Result<(), ConnectError>> + Send;

    // ===== Worker properties =====

    /// Returns the worker ID.
    fn worker_id(&self) -> &str;

    /// Returns the internal value converter.
    fn internal_value_converter(&self) -> &dyn Converter;

    /// Returns the plugins instance.
    fn plugins(&self) -> &dyn Plugins;

    /// Returns the connect metrics instance.
    fn metrics(&self) -> &dyn ConnectMetrics;

    /// Returns whether topic creation is enabled.
    fn is_topic_creation_enabled(&self) -> bool;

    /// Returns the herder instance.
    fn herder(&self) -> Option<&dyn Herder>;

    // ===== Configuration generation methods =====

    /// Returns the producer configs for a connector.
    fn producer_configs(
        &self,
        conn_name: &str,
        default_client_id: &str,
        conn_config: &dyn ConnectorConfig,
    ) -> HashMap<String, serde_json::Value>;

    /// Returns the consumer configs for a connector.
    fn consumer_configs(
        &self,
        conn_name: &str,
        default_client_id: &str,
        conn_config: &dyn ConnectorConfig,
    ) -> HashMap<String, serde_json::Value>;

    /// Returns the admin configs for a connector.
    fn admin_configs(
        &self,
        conn_name: &str,
        default_client_id: &str,
        conn_config: &dyn ConnectorConfig,
    ) -> HashMap<String, serde_json::Value>;

    // ===== Offset store factory methods =====

    /// Creates an offset backing store for a regular source connector.
    fn offset_store_for_regular_source_connector(
        &self,
        conn_name: &str,
        conn_config: &dyn ConnectorConfig,
        producer: Option<&dyn Producer>,
    ) -> Box<dyn ConnectorOffsetBackingStore>;

    /// Creates an offset backing store for an exactly-once source connector.
    fn offset_store_for_exactly_once_source_connector(
        &self,
        conn_name: &str,
        conn_config: &dyn ConnectorConfig,
        producer: Option<&dyn Producer>,
    ) -> Box<dyn ConnectorOffsetBackingStore>;

    /// Creates an offset backing store for a regular source task.
    fn offset_store_for_regular_source_task(
        &self,
        id: &ConnectorTaskId,
        conn_config: &dyn ConnectorConfig,
        producer: &dyn Producer,
    ) -> Box<dyn ConnectorOffsetBackingStore>;

    /// Creates an offset backing store for an exactly-once source task.
    fn offset_store_for_exactly_once_source_task(
        &self,
        id: &ConnectorTaskId,
        conn_config: &dyn ConnectorConfig,
        producer: &dyn Producer,
    ) -> Box<dyn ConnectorOffsetBackingStore>;

    // ===== Error handling methods =====

    /// Returns the error handling metrics for a task.
    fn error_handling_metrics(&self, id: &ConnectorTaskId) -> &dyn ErrorHandlingMetrics;

    // ===== Metrics group methods =====

    /// Returns the connector status metrics group.
    fn connector_status_metrics_group(&self) -> &dyn ConnectorStatusMetricsGroup;

    /// Returns the worker metrics group.
    fn worker_metrics_group(&self) -> &dyn WorkerMetricsGroup;

    // ===== Sink connector offset methods =====

    /// Gets the current consumer group offsets for a sink connector.
    fn sink_connector_offsets(
        &self,
        conn_name: &str,
        connector: &dyn ConnectorInstance,
        connector_config: HashMap<String, String>,
        cb: &dyn Callback<ConnectorOffsets>,
    );

    /// Gets the current offsets for a source connector.
    fn source_connector_offsets(
        &self,
        conn_name: &str,
        offset_store: &dyn ConnectorOffsetBackingStore,
        offset_reader: &dyn OffsetStorageReader,
        cb: &dyn Callback<ConnectorOffsets>,
    );

    // ===== Admin client factory methods =====

    /// Creates an admin client for a connector.
    fn create_admin_client(
        &self,
        conn_name: &str,
        conn_config: &dyn ConnectorConfig,
    ) -> Box<dyn AdminClient>;
}

/// Producer trait for Kafka producer.
pub trait Producer: Send + Sync {
    /// Initializes transactions.
    fn init_transactions(&self);

    /// Begins a transaction.
    fn begin_transaction(&self);

    /// Commits a transaction.
    fn commit_transaction(&self);

    /// Aborts a transaction.
    fn abort_transaction(&self);
}

/// Error handling metrics trait.
/// Corresponds to `org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics` in Java.
pub trait ErrorHandlingMetrics: Send + Sync {
    /// Records a failure.
    fn record_failure(&self);

    /// Records a success.
    fn record_success(&self);

    /// Returns the number of failures.
    fn failures(&self) -> i64;

    /// Returns the number of successes.
    fn successes(&self) -> i64;
}

/// Connector status metrics group trait.
/// Corresponds to `org.apache.kafka.connect.runtime.Worker.ConnectorStatusMetricsGroup` in Java.
pub trait ConnectorStatusMetricsGroup: Send + Sync {
    /// Records a task added event.
    fn record_task_added(&self, connector_task_id: &ConnectorTaskId);

    /// Records a task removed event.
    fn record_task_removed(&self, connector_task_id: &ConnectorTaskId);

    /// Returns the metric group for a connector.
    fn metric_group(&self, connector_id: &str) -> Option<&dyn MetricGroup>;

    /// Closes the metrics group.
    fn close(&self);
}

/// Worker metrics group trait.
/// Corresponds to `org.apache.kafka.connect.runtime.Worker.WorkerMetricsGroup` in Java.
pub trait WorkerMetricsGroup: Send + Sync {
    /// Returns the number of connectors.
    fn connector_count(&self) -> i64;

    /// Returns the number of tasks.
    fn task_count(&self) -> i64;

    /// Closes the metrics group.
    fn close(&self);
}

/// Metric group trait for metrics grouping.
pub trait MetricGroup: Send + Sync {
    /// Closes the metric group.
    fn close(&self);
}

/// Static helper functions for the Worker.
pub struct WorkerUtils;

impl WorkerUtils {
    /// Returns the transactional ID for a task.
    ///
    /// # Arguments
    /// * `group_id` - The group ID
    /// * `connector` - The connector name
    /// * `task_id` - The task ID number
    pub fn task_transactional_id(group_id: &str, connector: &str, task_id: i32) -> String {
        format!("{}-{}-{}", group_id, connector, task_id)
    }
}
