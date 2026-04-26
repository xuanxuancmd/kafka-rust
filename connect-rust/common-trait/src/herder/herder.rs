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

//! Herder trait definition for Kafka Connect runtime.
//!
//! The Herder is the primary interface for managing connectors and tasks
//! within a Kafka Connect cluster. It serves as the coordinator between
//! the REST API and the Worker.

use serde_json::Value;
use std::collections::HashMap;

use crate::config::ConfigValue;

/// Callback trait for async operations.
///
/// Used for all async operations in Herder that return results via callbacks.
pub trait Callback<T> {
    /// Called when the operation completes successfully.
    fn on_completion(&self, result: T);

    /// Called when the operation fails with an error.
    fn on_error(&self, error: String);
}

/// Represents a created resource with its info.
#[derive(Debug, Clone)]
pub struct Created<T> {
    /// Whether the resource was newly created (vs updated).
    pub created: bool,
    /// The resource info.
    pub info: T,
}

/// Connector task identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectorTaskId {
    /// Connector name.
    pub connector: String,
    /// Task ID (0-indexed).
    pub task: u32,
}

impl ConnectorTaskId {
    /// Create a new ConnectorTaskId.
    pub fn new(connector: String, task: u32) -> Self {
        ConnectorTaskId { connector, task }
    }

    /// Get the connector name.
    pub fn connector(&self) -> &str {
        &self.connector
    }

    /// Get the task ID.
    pub fn task(&self) -> u32 {
        self.task
    }
}

/// Information about a connector.
#[derive(Debug, Clone)]
pub struct ConnectorInfo {
    /// Connector name.
    pub name: String,
    /// Connector configuration.
    pub config: HashMap<String, String>,
    /// Task configurations.
    pub tasks: Vec<TaskInfo>,
}

/// Information about a task.
#[derive(Debug, Clone)]
pub struct TaskInfo {
    /// Task identifier.
    pub id: ConnectorTaskId,
    /// Task configuration.
    pub config: HashMap<String, String>,
}

/// State of a connector or task.
/// Corresponds to org.apache.kafka.connect.runtime.AbstractStatus.State in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorState {
    /// Starting state.
    Starting,
    /// Running state.
    Running,
    /// Paused state.
    Paused,
    /// Stopped state.
    Stopped,
    /// Failed state.
    Failed,
    /// Unassigned state.
    Unassigned,
    /// Destroyed state.
    Destroyed,
    /// Restarting state - used during restart planning.
    Restarting,
}

/// Target state for a connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetState {
    /// Started state - connector should run.
    Started,
    /// Paused state - connector should pause.
    Paused,
    /// Stopped state - connector should stop.
    Stopped,
}

/// State info for a connector including its tasks.
#[derive(Debug, Clone)]
pub struct ConnectorStateInfo {
    /// Connector name.
    pub name: String,
    /// Connector state.
    pub connector: ConnectorState,
    /// Task ID and state pairs.
    pub tasks: Vec<TaskStateInfo>,
}

/// State info for a single task.
#[derive(Debug, Clone)]
pub struct TaskStateInfo {
    /// Task identifier.
    pub id: ConnectorTaskId,
    /// Task state.
    pub state: ConnectorState,
    /// Worker ID that owns this task.
    pub worker_id: String,
    /// Error trace if failed.
    pub trace: Option<String>,
}

/// Active topics info for a connector.
#[derive(Debug, Clone)]
pub struct ActiveTopicsInfo {
    /// Connector name.
    pub connector: String,
    /// Set of active topics.
    pub topics: Vec<String>,
}

/// Configuration validation result.
#[derive(Debug, Clone)]
pub struct ConfigInfos {
    /// Connector name/type.
    pub name: String,
    /// Error count.
    pub error_count: u32,
    /// Group count.
    pub group_count: u32,
    /// Configuration key info list.
    pub configs: Vec<ConfigInfo>,
}

/// Single configuration info.
#[derive(Debug, Clone)]
pub struct ConfigInfo {
    /// Configuration definition.
    pub definition: ConfigKeyInfo,
    /// Current value.
    pub value: ConfigValue,
    /// Errors for this config.
    pub errors: Vec<String>,
}

/// Configuration key definition.
#[derive(Debug, Clone)]
pub struct ConfigKeyInfo {
    /// Configuration name.
    pub name: String,
    /// Configuration type.
    pub config_type: String,
    /// Default value.
    pub default_value: Option<String>,
    /// Is required.
    pub required: bool,
    /// Importance level.
    pub importance: String,
    /// Documentation.
    pub documentation: String,
    /// Group name.
    pub group: Option<String>,
    /// Order in group.
    pub order_in_group: Option<u32>,
    /// Width for display.
    pub width: String,
    /// Display name.
    pub display_name: String,
    /// Dependents.
    pub dependents: Vec<String>,
    /// Validators.
    pub validators: Vec<String>,
}

/// Connector offsets information.
#[derive(Debug, Clone)]
pub struct ConnectorOffsets {
    /// Offets keyed by partition.
    pub offsets: HashMap<Value, Value>,
}

/// Message for offset alteration result.
#[derive(Debug, Clone)]
pub struct Message {
    /// Message code.
    pub code: i32,
    /// Message text.
    pub message: String,
}

/// Logger level information.
#[derive(Debug, Clone)]
pub struct LoggerLevel {
    /// Logger name.
    pub logger: String,
    /// Current level.
    pub level: String,
    /// Effective level.
    pub effective_level: String,
}

/// Internal request signature for secure operations.
#[derive(Debug, Clone)]
pub struct InternalRequestSignature {
    /// Signature key.
    pub key: String,
    /// Signature value.
    pub signature: String,
}

/// Herder request handle for tracking async operations.
pub trait HerderRequest {
    /// Cancel the request.
    fn cancel(&self);

    /// Check if request is completed.
    fn is_completed(&self) -> bool;
}

/// Status backing store trait.
pub trait StatusBackingStore {
    /// Get connector status.
    fn get_connector_state(&self, connector: &str) -> Option<ConnectorState>;

    /// Get task status.
    fn get_task_state(&self, id: &ConnectorTaskId) -> Option<TaskStateInfo>;

    /// Put connector status.
    fn put_connector_state(&self, connector: &str, state: ConnectorState);

    /// Put task status.
    fn put_task_state(&self, id: &ConnectorTaskId, state: TaskStateInfo);
}

/// Plugins trait for plugin management.
///
/// This trait defines plugin discovery and retrieval operations.
/// Full implementation resides in connect-runtime/src/isolation/plugins.rs.
pub trait Plugins {
    /// Get all connector plugin class names.
    fn connector_plugins(&self) -> Vec<String>;

    /// Get all converter plugin class names.
    fn converter_plugins(&self) -> Vec<String>;

    /// Get all transformation plugin class names.
    fn transformation_plugins(&self) -> Vec<String>;

    /// Get connector plugin descriptor by class name.
    fn connector_plugin_desc(&self, class_name: &str) -> Option<PluginDesc>;

    /// Get converter plugin descriptor by class name.
    fn converter_plugin_desc(&self, class_name: &str) -> Option<PluginDesc>;
}

/// Plugin descriptor containing metadata about a plugin.
#[derive(Debug, Clone)]
pub struct PluginDesc {
    /// Plugin class name.
    pub class_name: String,
    /// Plugin type (connector, converter, transformation, etc.).
    pub plugin_type: PluginType,
    /// Plugin version.
    pub version: String,
    /// Plugin documentation.
    pub documentation: String,
}

/// Type of plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginType {
    /// Source connector plugin.
    Source,
    /// Sink connector plugin.
    Sink,
    /// Converter plugin.
    Converter,
    /// Transformation plugin.
    Transformation,
    /// Header converter plugin.
    HeaderConverter,
}

/// Version range for plugin compatibility.
///
/// This struct represents version constraints for plugin loading.
/// Parsing logic will be implemented in common-trait/src/util/version_range.rs.
#[derive(Debug, Clone)]
pub struct VersionRange {
    /// Lower bound version.
    pub lower: Option<String>,
    /// Upper bound version.
    pub upper: Option<String>,
    /// Is lower bound inclusive.
    pub lower_inclusive: bool,
    /// Is upper bound inclusive.
    pub upper_inclusive: bool,
}

impl VersionRange {
    /// Create a new version range.
    pub fn new(
        lower: Option<String>,
        upper: Option<String>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    ) -> Self {
        Self {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        }
    }

    /// Check if this range has any restrictions defined.
    pub fn has_restrictions(&self) -> bool {
        self.lower.is_some() || self.upper.is_some()
    }
}

/// Config reload action enum.
///
/// Specifies what action to take when a connector config is reloaded.
/// Corresponds to `Herder.ConfigReloadAction` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigReloadAction {
    /// No action - just update the config.
    None,
    /// Restart the connector after config update.
    Restart,
}

/// Restart request for connector and tasks.
///
/// Specifies which connector to restart and whether to include tasks.
/// Corresponds to `org.apache.kafka.connect.runtime.RestartRequest` in Java.
/// Full implementation resides in connect-runtime/src/runtime/restart_request.rs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartRequest {
    /// Connector name.
    pub connector: String,
    /// Whether to restart only failed instances.
    pub only_failed: bool,
    /// Whether to include tasks in the restart.
    pub include_tasks: bool,
}

impl RestartRequest {
    /// Create a new RestartRequest.
    pub fn new(connector: String, only_failed: bool, include_tasks: bool) -> Self {
        RestartRequest {
            connector,
            only_failed,
            include_tasks,
        }
    }

    /// Get the connector name.
    pub fn connector(&self) -> &str {
        &self.connector
    }

    /// Check if only failed instances should be restarted.
    pub fn only_failed(&self) -> bool {
        self.only_failed
    }

    /// Check if tasks should be included in restart.
    pub fn include_tasks(&self) -> bool {
        self.include_tasks
    }
}

/// Connect metrics trait for metrics access.
///
/// Provides access to Connect metrics for monitoring.
/// Full implementation resides in common-trait/src/worker/worker.rs.
pub trait ConnectMetrics: Send + Sync {
    /// Returns the metrics registry.
    fn registry(&self) -> &dyn MetricsRegistry;

    /// Stops the metrics.
    fn stop(&self);
}

/// Metrics registry trait.
pub trait MetricsRegistry: Send + Sync {
    /// Returns the worker group name.
    fn worker_group_name(&self) -> &str;
}

/// Herder trait - the primary interface for managing connectors and tasks.
///
/// This trait defines all operations for managing Kafka Connect connectors,
/// tasks, and cluster state. Implementations include StandaloneHerder and
/// DistributedHerder.
///
/// Reference: Apache Kafka Herder.java (~30 methods, 397 lines)
pub trait Herder {
    // === Lifecycle methods ===

    /// Start the herder.
    fn start(&mut self);

    /// Stop the herder.
    fn stop(&mut self);

    /// Check if the herder is ready to serve requests.
    fn is_ready(&self) -> bool;

    /// Perform a health check.
    fn health_check(&self, callback: Box<dyn Callback<()>>);

    // === Connector enumeration (async) ===

    /// Get list of active connectors (async).
    fn connectors_async(&self, callback: Box<dyn Callback<Vec<String>>>);

    /// Get connector info (async).
    fn connector_info_async(&self, conn_name: &str, callback: Box<dyn Callback<ConnectorInfo>>);

    /// Get connector config (async).
    fn connector_config_async(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<HashMap<String, String>>>,
    );

    // === Connector config management ===

    /// Create or update connector config.
    fn put_connector_config(
        &self,
        conn_name: &str,
        config: HashMap<String, String>,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    );

    /// Create or update connector config with target state.
    fn put_connector_config_with_state(
        &self,
        conn_name: &str,
        config: HashMap<String, String>,
        target_state: TargetState,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    );

    /// Patch connector config (partial update).
    fn patch_connector_config(
        &self,
        conn_name: &str,
        config_patch: HashMap<String, String>,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    );

    /// Delete connector config.
    fn delete_connector_config(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    );

    // === Task reconfiguration ===

    /// Request task reconfiguration for a connector.
    fn request_task_reconfiguration(&self, conn_name: &str);

    /// Get task configs (async).
    fn task_configs_async(&self, conn_name: &str, callback: Box<dyn Callback<Vec<TaskInfo>>>);

    /// Put task configs (internal operation).
    fn put_task_configs(
        &self,
        conn_name: &str,
        configs: Vec<HashMap<String, String>>,
        callback: Box<dyn Callback<()>>,
        request_signature: InternalRequestSignature,
    );

    /// Fence zombie source tasks.
    fn fence_zombie_source_tasks(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<()>>,
        request_signature: InternalRequestSignature,
    );

    // === Connector enumeration (sync) ===

    /// Get list of active connectors (sync).
    fn connectors_sync(&self) -> Vec<String>;

    /// Get connector info (sync).
    fn connector_info_sync(&self, conn_name: &str) -> ConnectorInfo;

    /// Get connector status.
    fn connector_status(&self, conn_name: &str) -> ConnectorStateInfo;

    /// Get connector active topics.
    fn connector_active_topics(&self, conn_name: &str) -> ActiveTopicsInfo;

    /// Reset connector active topics tracking.
    fn reset_connector_active_topics(&self, conn_name: &str);

    // === Status backing store ===

    /// Get the status backing store.
    fn status_backing_store(&self) -> Box<dyn StatusBackingStore>;

    /// Get task status.
    fn task_status(&self, id: &ConnectorTaskId) -> TaskStateInfo;

    // === Config validation ===

    /// Validate connector config (async).
    fn validate_connector_config_async(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn Callback<ConfigInfos>>,
    );

    /// Validate connector config with logging option.
    fn validate_connector_config_async_with_log(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn Callback<ConfigInfos>>,
        do_log: bool,
    );

    // === Task/Connector restart ===

    /// Restart a task.
    fn restart_task(&self, id: &ConnectorTaskId, callback: Box<dyn Callback<()>>);

    /// Restart a connector (immediate).
    fn restart_connector_async(&self, conn_name: &str, callback: Box<dyn Callback<()>>);

    /// Restart a connector with delay.
    fn restart_connector_delayed(
        &self,
        delay_ms: u64,
        conn_name: &str,
        callback: Box<dyn Callback<()>>,
    ) -> Box<dyn HerderRequest>;

    // === Connector pause/resume ===

    /// Pause a connector and its tasks.
    fn pause_connector(&self, connector: &str);

    /// Resume a paused connector.
    fn resume_connector(&self, connector: &str);

    /// Stop a connector asynchronously with callback.
    /// Corresponds to `stopConnector(String, Callback<Void>)` in Java.
    fn stop_connector_async(&self, connector: &str, callback: Box<dyn Callback<()>>);

    // === Connector restart with tasks ===

    /// Restart a connector and optionally its tasks.
    /// Corresponds to `restartConnectorAndTasks(RestartRequest, Callback<ConnectorStateInfo>)` in Java.
    fn restart_connector_and_tasks(
        &self,
        request: &RestartRequest,
        callback: Box<dyn Callback<ConnectorStateInfo>>,
    );

    // === Plugin management ===

    /// Get the plugins handle.
    fn plugins(&self) -> Box<dyn Plugins>;

    /// Get Kafka cluster ID.
    fn kafka_cluster_id(&self) -> String;

    /// Get connector plugin config keys.
    fn connector_plugin_config(&self, plugin_name: &str) -> Vec<ConfigKeyInfo>;

    /// Get connector plugin config keys with version range.
    fn connector_plugin_config_with_version(
        &self,
        plugin_name: &str,
        version: VersionRange,
    ) -> Vec<ConfigKeyInfo>;

    // === Offset management ===

    /// Get connector offsets (async).
    fn connector_offsets_async(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<ConnectorOffsets>>,
    );

    /// Alter connector offsets.
    fn alter_connector_offsets(
        &self,
        conn_name: &str,
        offsets: HashMap<HashMap<String, Value>, HashMap<String, Value>>,
        callback: Box<dyn Callback<Message>>,
    );

    /// Reset connector offsets.
    /// Corresponds to `resetConnectorOffsets(String, Callback<Message>)` in Java.
    fn reset_connector_offsets(&self, conn_name: &str, callback: Box<dyn Callback<Message>>);

    // === Logger management ===

    /// Get logger level.
    fn logger_level(&self, logger: &str) -> LoggerLevel;

    /// Get all logger levels.
    /// Corresponds to `allLoggerLevels()` in Java.
    fn all_logger_levels(&self) -> HashMap<String, LoggerLevel>;

    /// Set worker logger level.
    fn set_worker_logger_level(&self, namespace: &str, level: &str) -> Vec<String>;

    /// Set cluster logger level.
    /// Corresponds to `setClusterLoggerLevel(String, String)` in Java.
    fn set_cluster_logger_level(&self, namespace: &str, level: &str);

    // === Metrics access ===

    /// Get the ConnectMetrics from the worker for this herder.
    /// Corresponds to `connectMetrics()` in Java.
    fn connect_metrics(&self) -> Box<dyn ConnectMetrics>;
}
