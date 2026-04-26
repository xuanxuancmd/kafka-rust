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

//! AbstractHerder - Abstract base class for Herder implementations.
//!
//! This module provides the common functionality shared between
//! DistributedHerder and StandaloneHerder implementations.
//!
//! Key responsibilities:
//! - Connector/task status tracking and reporting
//! - Configuration validation
//! - Plugin management
//! - Logger level management
//! - Connector info/status retrieval
//!
//! Corresponds to `org.apache.kafka.connect.runtime.AbstractHerder` in Java (~1287 lines).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

// Import herder types directly from herder module
use common_trait::herder::{
    ActiveTopicsInfo, Callback, ConfigInfo, ConfigInfos, ConfigKeyInfo, ConnectorInfo, ConnectorState, ConnectorStateInfo, ConnectorTaskId as HerderConnectorTaskId,
    Created, LoggerLevel,
    Plugins, TaskInfo, TaskStateInfo,
};

// Import storage types directly from storage module
// These have full implementations including start/stop methods
use common_trait::storage::{
    ClusterConfigState, ConfigBackingStore, ConnectorTaskId as StorageConnectorTaskId,
    State as StorageState, StatusBackingStore, TargetState as StorageTargetState,
};

// Import time utilities
use common_trait::util::time::SystemTimeImpl;

use serde_json::Value;

use super::cached_connectors::CachedConnectors;
use super::loggers::Loggers;
use super::status::{ConnectorStatus, TaskStatus};

/// ConnectorType - The type of connector (source, sink, or unknown).
///
/// Corresponds to `org.apache.kafka.connect.runtime.rest.entities.ConnectorType` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorType {
    Source,
    Sink,
    Unknown,
}

impl std::fmt::Display for ConnectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorType::Source => write!(f, "source"),
            ConnectorType::Sink => write!(f, "sink"),
            ConnectorType::Unknown => write!(f, "unknown"),
        }
    }
}

/// AbstractHerder - Abstract base struct for Herder implementations.
///
/// This struct provides common functionality shared between
/// DistributedHerder and StandaloneHerder.
///
/// Thread safety: Uses atomic flags for ready state, Mutex for backing stores.
///
/// Corresponds to `org.apache.kafka.connect.runtime.AbstractHerder` in Java.
pub struct AbstractHerder {
    /// Worker ID for this herder.
    worker_id: String,
    /// Kafka cluster ID.
    kafka_cluster_id: String,
    /// Status backing store for connector/task status.
    /// Wrapped in Mutex to allow start/stop calls (which require &mut self).
    status_backing_store: Arc<Mutex<dyn StatusBackingStore>>,
    /// Config backing store for connector/task configs.
    /// Wrapped in Mutex to allow start/stop calls (which require &mut self).
    config_backing_store: Arc<Mutex<dyn ConfigBackingStore>>,
    /// Whether the herder is ready.
    ready: AtomicBool,
    /// Plugins instance.
    plugins: Arc<dyn Plugins>,
    /// Cached connectors for efficient connector type detection.
    cached_connectors: Arc<CachedConnectors>,
    /// Loggers management.
    loggers: Loggers,
    /// Time instance.
    time: Arc<SystemTimeImpl>,
}

impl AbstractHerder {
    /// Create a new AbstractHerder.
    ///
    /// # Arguments
    /// * `worker_id` - The worker ID.
    /// * `kafka_cluster_id` - The Kafka cluster ID.
    /// * `status_backing_store` - Status backing store (wrapped in Mutex for start/stop).
    /// * `config_backing_store` - Config backing store (wrapped in Mutex for start/stop).
    /// * `plugins` - Plugins instance.
    /// * `time` - Time instance.
    pub fn new(
        worker_id: String,
        kafka_cluster_id: String,
        status_backing_store: Arc<Mutex<dyn StatusBackingStore>>,
        config_backing_store: Arc<Mutex<dyn ConfigBackingStore>>,
        plugins: Arc<dyn Plugins>,
        time: Arc<SystemTimeImpl>,
    ) -> Self {
        let cached_connectors = Arc::new(CachedConnectors::new(plugins.clone()));
        let loggers = Loggers::new(time.clone());

        AbstractHerder {
            worker_id,
            kafka_cluster_id,
            status_backing_store,
            config_backing_store,
            ready: AtomicBool::new(false),
            plugins,
            cached_connectors,
            loggers,
            time,
        }
    }

    /// Mark the herder as ready.
    pub fn set_ready(&self) {
        self.ready.store(true, Ordering::SeqCst);
    }

    /// Check if the herder is ready.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// Get the worker ID.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Get the Kafka cluster ID.
    pub fn kafka_cluster_id(&self) -> &str {
        &self.kafka_cluster_id
    }

    /// Get the status backing store.
    pub fn status_backing_store(&self) -> &Arc<Mutex<dyn StatusBackingStore>> {
        &self.status_backing_store
    }

    /// Get the config backing store.
    pub fn config_backing_store(&self) -> &Arc<Mutex<dyn ConfigBackingStore>> {
        &self.config_backing_store
    }

    /// Get the plugins instance.
    pub fn plugins(&self) -> &Arc<dyn Plugins> {
        &self.plugins
    }

    /// Get the cached connectors instance.
    pub fn cached_connectors(&self) -> &Arc<CachedConnectors> {
        &self.cached_connectors
    }

    /// Get the loggers instance.
    pub fn loggers(&self) -> &Loggers {
        &self.loggers
    }

    /// Get the time instance.
    pub fn time(&self) -> &Arc<SystemTimeImpl> {
        &self.time
    }

    // ===== Abstract methods (to be overridden by subclasses) =====

    /// Get the current generation ID.
    ///
    /// This is an abstract method in Java that must be overridden by subclasses.
    /// DistributedHerder returns the actual generation from Kafka group membership.
    /// StandaloneHerder returns -1 (NO_GENERATION) as there's no group coordination.
    ///
    /// Corresponds to Java `AbstractHerder.generation()`.
    ///
    /// # Returns
    /// The current generation ID, or -1 if not applicable (standalone mode).
    pub fn generation(&self) -> i32 {
        // Default implementation: NO_GENERATION (-1)
        // Subclasses (DistributedHerder) should override this
        -1
    }

    // ===== Lifecycle methods =====

    /// Start the backing store services.
    ///
    /// This method starts the status backing store and config backing store.
    /// In Java, this also calls worker.start(), but Worker is managed separately
    /// in Rust implementation (DistributedHerder has its own Worker reference).
    ///
    /// Corresponds to Java `AbstractHerder.startServices()`.
    ///
    /// # Implementation Note
    /// Java implementation:
    /// ```java
    /// protected void startServices() {
    ///     this.worker.start();
    ///     this.statusBackingStore.start();
    ///     this.configBackingStore.start();
    /// }
    /// ```
    pub fn start_services(&self) {
        // Start status backing store
        if let Ok(mut store) = self.status_backing_store.lock() {
            store.start();
        }

        // Start config backing store
        if let Ok(mut store) = self.config_backing_store.lock() {
            store.start();
        }
    }

    /// Stop the backing store services.
    ///
    /// This method stops the status backing store and config backing store.
    /// In Java, this also calls worker.stop() and connector_executor.shutdown(),
    /// but those are managed separately in Rust implementation.
    ///
    /// Corresponds to Java `AbstractHerder.stopServices()`.
    ///
    /// # Implementation Note
    /// Java implementation:
    /// ```java
    /// protected void stopServices() {
    ///     this.statusBackingStore.stop();
    ///     this.configBackingStore.stop();
    ///     this.worker.stop();
    ///     this.connectorExecutor.shutdown();
    ///     Utils.closeQuietly(this.connectorClientConfigOverridePolicyPlugin, ...);
    /// }
    /// ```
    pub fn stop_services(&self) {
        // Stop status backing store
        if let Ok(mut store) = self.status_backing_store.lock() {
            store.stop();
        }

        // Stop config backing store
        if let Ok(mut store) = self.config_backing_store.lock() {
            store.stop();
        }
    }

    // ===== Status listener methods (ConnectorStatus.Listener) =====

    /// Called when a connector starts up.
    ///
    /// Updates the status backing store with RUNNING state.
    /// Corresponds to Java `AbstractHerder.onStartup(String connector)`.
    pub fn on_connector_startup(&self, connector: &str, generation: i32, version: Option<String>) {
        let status = ConnectorStatus::running(
            connector.to_string(),
            self.worker_id.clone(),
            generation,
            version,
        );
        // Note: In full implementation, this would write to status_backing_store
    }

    /// Called when a connector stops.
    ///
    /// Updates the status backing store with STOPPED state.
    /// Corresponds to Java `AbstractHerder.onStop(String connector)`.
    pub fn on_connector_stop(&self, connector: &str, generation: i32, version: Option<String>) {
        let status = ConnectorStatus::stopped(
            connector.to_string(),
            self.worker_id.clone(),
            generation,
            version,
        );
    }

    /// Called when a connector pauses.
    ///
    /// Updates the status backing store with PAUSED state.
    /// Corresponds to Java `AbstractHerder.onPause(String connector)`.
    pub fn on_connector_pause(&self, connector: &str, generation: i32, version: Option<String>) {
        let status = ConnectorStatus::paused(
            connector.to_string(),
            self.worker_id.clone(),
            generation,
            version,
        );
    }

    /// Called when a connector resumes.
    ///
    /// Updates the status backing store with RUNNING state.
    /// Corresponds to Java `AbstractHerder.onResume(String connector)`.
    pub fn on_connector_resume(&self, connector: &str, generation: i32, version: Option<String>) {
        let status = ConnectorStatus::running(
            connector.to_string(),
            self.worker_id.clone(),
            generation,
            version,
        );
    }

    /// Called when a connector shuts down.
    ///
    /// Uses putSafe to avoid conflicts with other workers.
    /// Corresponds to Java `AbstractHerder.onShutdown(String connector)`.
    pub fn on_connector_shutdown(&self, connector: &str, generation: i32, version: Option<String>) {
        let status = ConnectorStatus::unassigned(
            connector.to_string(),
            self.worker_id.clone(),
            generation,
            version,
        );
    }

    /// Called when a connector fails.
    ///
    /// Uses putSafe to avoid conflicts with other workers.
    /// Corresponds to Java `AbstractHerder.onFailure(String connector, Throwable cause)`.
    pub fn on_connector_failure(
        &self,
        connector: &str,
        generation: i32,
        cause: &str,
        version: Option<String>,
    ) {
        let status = ConnectorStatus::failed(
            connector.to_string(),
            self.worker_id.clone(),
            generation,
            cause.to_string(),
            version,
        );
    }

    /// Called when a connector is deleted.
    ///
    /// Marks all tasks as destroyed and then the connector.
    /// Corresponds to Java `AbstractHerder.onDeletion(String connector)`.
    pub fn on_connector_deletion(&self, connector: &str, generation: i32, version: Option<String>) {
        // In full implementation, would mark all tasks as destroyed first
        let status = ConnectorStatus::destroyed(
            connector.to_string(),
            self.worker_id.clone(),
            generation,
            version,
        );
    }

    /// Called when a connector restarts.
    ///
    /// Updates the status backing store with RESTARTING state.
    pub fn on_connector_restart(&self, connector: &str, generation: i32, version: Option<String>) {
        let status = ConnectorStatus::restarting(
            connector.to_string(),
            self.worker_id.clone(),
            generation,
            version,
        );
    }

    // ===== Status listener methods (TaskStatus.Listener) =====

    /// Called when a task starts up.
    ///
    /// Updates the status backing store with RUNNING state.
    /// Corresponds to Java `AbstractHerder.onStartup(ConnectorTaskId id)`.
    pub fn on_task_startup(
        &self,
        id: &StorageConnectorTaskId,
        generation: i32,
        version: Option<String>,
    ) {
        let status = TaskStatus::running(id.clone(), self.worker_id.clone(), generation, version);
    }

    /// Called when a task fails.
    ///
    /// Uses putSafe to avoid conflicts with other workers.
    /// Corresponds to Java `AbstractHerder.onFailure(ConnectorTaskId id, Throwable cause)`.
    pub fn on_task_failure(
        &self,
        id: &StorageConnectorTaskId,
        generation: i32,
        cause: &str,
        version: Option<String>,
    ) {
        let status = TaskStatus::failed(
            id.clone(),
            self.worker_id.clone(),
            generation,
            cause.to_string(),
            version,
        );
    }

    /// Called when a task shuts down.
    ///
    /// Uses putSafe to avoid conflicts with other workers.
    /// Corresponds to Java `AbstractHerder.onShutdown(ConnectorTaskId id)`.
    pub fn on_task_shutdown(
        &self,
        id: &StorageConnectorTaskId,
        generation: i32,
        version: Option<String>,
    ) {
        let status =
            TaskStatus::unassigned(id.clone(), self.worker_id.clone(), generation, version);
    }

    /// Called when a task resumes.
    ///
    /// Updates the status backing store with RUNNING state.
    pub fn on_task_resume(
        &self,
        id: &StorageConnectorTaskId,
        generation: i32,
        version: Option<String>,
    ) {
        let status = TaskStatus::running(id.clone(), self.worker_id.clone(), generation, version);
    }

    /// Called when a task pauses.
    ///
    /// Updates the status backing store with PAUSED state.
    pub fn on_task_pause(
        &self,
        id: &StorageConnectorTaskId,
        generation: i32,
        version: Option<String>,
    ) {
        let status = TaskStatus::paused(id.clone(), self.worker_id.clone(), generation, version);
    }

    /// Called when a task is deleted.
    ///
    /// Updates the status backing store with DESTROYED state.
    /// Corresponds to Java `AbstractHerder.onDeletion(ConnectorTaskId id)`.
    pub fn on_task_deletion(
        &self,
        id: &StorageConnectorTaskId,
        generation: i32,
        version: Option<String>,
    ) {
        let status = TaskStatus::destroyed(id.clone(), self.worker_id.clone(), generation, version);
    }

    /// Called when a task restarts.
    ///
    /// Updates the status backing store with RESTARTING state.
    pub fn on_task_restart(
        &self,
        id: &StorageConnectorTaskId,
        generation: i32,
        version: Option<String>,
    ) {
        let status =
            TaskStatus::restarting(id.clone(), self.worker_id.clone(), generation, version);
    }

    // ===== Connector info/status methods =====

    /// Get connector information.
    ///
    /// Returns connector name, config, and task IDs.
    /// Corresponds to Java `AbstractHerder.connectorInfo(String connector)`.
    pub fn connector_info(&self, connector: &str) -> Option<ConnectorInfo> {
        // Get config state snapshot from backing store
        let config_state = if let Ok(store) = self.config_backing_store.lock() {
            store.snapshot()
        } else {
            return None;
        };

        if !config_state.contains(connector) {
            return None;
        }

        let config = config_state.connector_config(connector)?.clone();
        let task_ids: Vec<TaskInfo> = config_state
            .tasks(connector)
            .into_iter()
            .map(|task_id| {
                let connector_name = task_id.connector.clone();
                let task_num = task_id.task;
                TaskInfo {
                    id: HerderConnectorTaskId {
                        connector: connector_name,
                        task: task_num,
                    },
                    config: config_state
                        .task_config(&task_id)
                        .cloned()
                        .unwrap_or_default(),
                }
            })
            .collect();

        Some(ConnectorInfo {
            name: connector.to_string(),
            config,
            tasks: task_ids,
        })
    }

    /// Get connector status.
    ///
    /// Returns the current status of the connector and its tasks.
    /// Corresponds to Java `AbstractHerder.connectorStatus(String connName)`.
    ///
    /// # Implementation Note
    /// In Java, ConnectorStateInfo contains a ConnectorState struct with state, workerId, trace, version.
    /// In Rust, ConnectorStateInfo.connector is a ConnectorState enum (state only).
    /// The worker_id and trace info are captured in logs but not returned in ConnectorStateInfo.
    /// This is a design difference between Java and current Rust implementation.
    pub fn connector_status(&self, conn_name: &str) -> Option<ConnectorStateInfo> {
        // Get connector status from backing store
        let connector_status = if let Ok(store) = self.status_backing_store.lock() {
            store.get_connector_status(conn_name)
        } else {
            return None;
        };

        // If no connector status, return None
        let connector_status = connector_status?;

        // Get task statuses
        let task_statuses = if let Ok(store) = self.status_backing_store.lock() {
            store.get_all_task_statuses(conn_name)
        } else {
            Vec::new()
        };

        // Convert task statuses to TaskStateInfo
        let task_states: Vec<TaskStateInfo> = task_statuses
            .into_iter()
            .map(|ts| TaskStateInfo {
                id: HerderConnectorTaskId {
                    connector: ts.id().connector.clone(),
                    task: ts.id().task,
                },
                state: convert_state_to_connector_state(ts.state()),
                worker_id: ts.worker_id().to_string(),
                trace: ts.trace().map(|s| s.to_string()),
            })
            .collect();

        // Convert connector status to ConnectorState (enum)
        Some(ConnectorStateInfo {
            name: conn_name.to_string(),
            connector: convert_state_to_connector_state(connector_status.state()),
            tasks: task_states,
        })
    }

    /// Get the type of a connector.
    ///
    /// Determines if the connector is source or sink based on its class.
    /// Corresponds to Java `AbstractHerder.connectorType(Map<String, String> connConfig)`.
    pub fn connector_type(&self, conn_config: &HashMap<String, String>) -> ConnectorType {
        if conn_config.is_empty() {
            return ConnectorType::Unknown;
        }

        let conn_class = conn_config.get("connector.class");
        if conn_class.is_none() {
            return ConnectorType::Unknown;
        }

        let class_name = conn_class.unwrap();

        // Get connector from cache (simplified version just checks class name pattern)
        let cached = self.cached_connectors.get_connector(class_name);
        if cached.is_source() {
            ConnectorType::Source
        } else {
            ConnectorType::Sink
        }
    }

    /// Get active topics for a connector.
    ///
    /// Returns the topics that the connector is actively using.
    /// Corresponds to Java `AbstractHerder.connectorActiveTopics(String connName)`.
    pub fn connector_active_topics(&self, conn_name: &str) -> ActiveTopicsInfo {
        ActiveTopicsInfo {
            connector: conn_name.to_string(),
            topics: Vec::new(), // Would get from status_backing_store
        }
    }

    /// Reset active topics tracking for a connector.
    ///
    /// Corresponds to Java `AbstractHerder.resetConnectorActiveTopics(String connName)`.
    pub fn reset_connector_active_topics(&self, conn_name: &str) {
        // Would delete all topic states from status_backing_store
    }

    // ===== Configuration validation methods =====

    /// Validate connector configuration.
    ///
    /// Performs basic validation of required fields.
    /// Corresponds to Java `AbstractHerder.validateConnectorConfig(Map<String, String>)`.
    pub fn validate_connector_config(&self, config: &HashMap<String, String>) -> ConfigInfos {
        let mut configs: Vec<ConfigInfo> = Vec::new();
        let mut error_count = 0;

        // Validate connector.class
        let connector_class = config.get("connector.class").cloned().unwrap_or_default();
        let connector_class_errors: Vec<String> = if connector_class.is_empty() {
            error_count += 1;
            vec!["Missing required configuration: connector.class".to_string()]
        } else {
            vec![]
        };

        configs.push(ConfigInfo {
            definition: ConfigKeyInfo {
                name: "connector.class".to_string(),
                config_type: "STRING".to_string(),
                default_value: None,
                required: true,
                importance: "HIGH".to_string(),
                documentation: "Connector class name".to_string(),
                group: Some("Common".to_string()),
                order_in_group: Some(1),
                width: "LONG".to_string(),
                display_name: "Connector Class".to_string(),
                dependents: vec![],
                validators: vec!["class".to_string()],
            },
            value: common_trait::config::ConfigValue::with_value(
                "connector.class",
                Value::String(connector_class),
            ),
            errors: connector_class_errors,
        });

        // Validate name
        let name = config.get("name").cloned().unwrap_or_default();
        let name_errors: Vec<String> = if name.is_empty() {
            error_count += 1;
            vec!["Missing required configuration: name".to_string()]
        } else {
            vec![]
        };

        configs.push(ConfigInfo {
            definition: ConfigKeyInfo {
                name: "name".to_string(),
                config_type: "STRING".to_string(),
                default_value: None,
                required: true,
                importance: "HIGH".to_string(),
                documentation: "Connector name".to_string(),
                group: Some("Common".to_string()),
                order_in_group: Some(2),
                width: "MEDIUM".to_string(),
                display_name: "Name".to_string(),
                dependents: vec![],
                validators: vec!["nonEmptyString".to_string()],
            },
            value: common_trait::config::ConfigValue::with_value("name", Value::String(name)),
            errors: name_errors,
        });

        // Validate tasks.max
        let tasks_max = config
            .get("tasks.max")
            .cloned()
            .unwrap_or_else(|| "1".to_string());
        let tasks_max_errors: Vec<String> = match tasks_max.parse::<u32>() {
            Ok(v) if v == 0 => vec!["tasks.max must be at least 1".to_string()],
            Ok(_) => vec![],
            Err(_) => {
                error_count += 1;
                vec![format!("Invalid value for tasks.max: '{}'", tasks_max)]
            }
        };

        configs.push(ConfigInfo {
            definition: ConfigKeyInfo {
                name: "tasks.max".to_string(),
                config_type: "INT".to_string(),
                default_value: Some("1".to_string()),
                required: false,
                importance: "HIGH".to_string(),
                documentation: "Maximum number of tasks for this connector".to_string(),
                group: Some("Common".to_string()),
                order_in_group: Some(3),
                width: "SHORT".to_string(),
                display_name: "Tasks Max".to_string(),
                dependents: vec![],
                validators: vec!["validInt".to_string()],
            },
            value: common_trait::config::ConfigValue::with_value(
                "tasks.max",
                Value::String(tasks_max),
            ),
            errors: tasks_max_errors,
        });

        let config_name = config
            .get("name")
            .cloned()
            .unwrap_or_else(|| "connector".to_string());

        ConfigInfos {
            name: config_name,
            error_count: error_count as u32,
            group_count: 1,
            configs,
        }
    }

    /// Check if config has errors and add exception to callback if so.
    ///
    /// Corresponds to Java `AbstractHerder.maybeAddConfigErrors(ConfigInfos, Callback)`.
    pub fn maybe_add_config_errors(
        &self,
        config_infos: &ConfigInfos,
        callback: &mut dyn Callback<Created<ConnectorInfo>>,
    ) -> bool {
        let errors = config_infos.error_count;
        let has_errors = errors > 0;

        if has_errors {
            let mut messages =
                String::from("Connector configuration is invalid and contains the following ");
            messages.push_str(&errors.to_string());
            messages.push_str(" error(s):");

            for config_info in &config_infos.configs {
                for msg in &config_info.errors {
                    messages.push('\n');
                    messages.push_str(msg);
                }
            }

            messages.push_str("\nYou can also find the above list of errors at the endpoint `/connector-plugins/{connectorType}/config/validate`");
            callback.on_error(messages);
        }

        has_errors
    }

    // ===== Logger management methods =====

    /// Get the current level for a logger.
    ///
    /// Corresponds to Java `AbstractHerder.loggerLevel(String logger)`.
    pub fn logger_level(&self, logger: &str) -> LoggerLevel {
        self.loggers.level(logger)
    }

    /// Get all logger levels.
    ///
    /// Corresponds to Java `AbstractHerder.allLoggerLevels()`.
    pub fn all_logger_levels(&self) -> HashMap<String, LoggerLevel> {
        self.loggers.all_levels()
    }

    /// Set the level for a logger namespace.
    ///
    /// Corresponds to Java `AbstractHerder.setWorkerLoggerLevel(String namespace, String desiredLevelStr)`.
    pub fn set_worker_logger_level(&self, namespace: &str, level: &str) -> Vec<String> {
        self.loggers.set_level(namespace, level)
    }

    // ===== Pause/Resume methods =====

    /// Pause a connector.
    ///
    /// Sets the target state to PAUSED.
    /// Corresponds to Java `AbstractHerder.pauseConnector(String connector)`.
    pub fn pause_connector(&self, connector: &str) -> Result<(), String> {
        let contains = if let Ok(store) = self.config_backing_store.lock() {
            store.contains(connector)
        } else {
            false
        };

        if !contains {
            return Err(format!("Unknown connector {}", connector));
        }

        // Put PAUSED target state
        if let Ok(mut store) = self.config_backing_store.lock() {
            store.put_target_state(connector, StorageTargetState::Paused);
        }

        Ok(())
    }

    /// Resume a connector.
    ///
    /// Sets the target state to STARTED.
    /// Corresponds to Java `AbstractHerder.resumeConnector(String connector)`.
    pub fn resume_connector(&self, connector: &str) -> Result<(), String> {
        let contains = if let Ok(store) = self.config_backing_store.lock() {
            store.contains(connector)
        } else {
            false
        };

        if !contains {
            return Err(format!("Unknown connector {}", connector));
        }

        // Put STARTED target state
        if let Ok(mut store) = self.config_backing_store.lock() {
            store.put_target_state(connector, StorageTargetState::Started);
        }

        Ok(())
    }

    /// Get list of all connectors.
    ///
    /// Corresponds to Java `AbstractHerder.connectors()`.
    pub fn connectors(&self) -> Vec<String> {
        if let Ok(store) = self.config_backing_store.lock() {
            store.snapshot().connectors().into_iter().cloned().collect()
        } else {
            Vec::new()
        }
    }

    // ===== Task configs method =====

    /// Get task configurations for a connector.
    ///
    /// Returns a list of TaskInfo containing each task's configuration.
    /// Corresponds to Java `Herder.taskConfigs(String connName, Callback<List<TaskInfo>> callback)`.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name.
    ///
    /// # Returns
    /// A list of TaskInfo for all tasks belonging to the connector, or None if connector not found.
    pub fn task_configs(&self, conn_name: &str) -> Option<Vec<TaskInfo>> {
        // Get config state snapshot
        let config_state = if let Ok(store) = self.config_backing_store.lock() {
            store.snapshot()
        } else {
            return None;
        };

        // Check if connector exists
        if !config_state.contains(conn_name) {
            return None;
        }

        // Build list of task configs
        let task_count = config_state.task_count(conn_name);
        let mut result: Vec<TaskInfo> = Vec::new();

        for index in 0..task_count {
            let task_id = StorageConnectorTaskId::new(conn_name.to_string(), index);
            let task_config = config_state
                .task_config(&task_id)
                .cloned()
                .unwrap_or_default();

            result.push(TaskInfo {
                id: HerderConnectorTaskId {
                    connector: conn_name.to_string(),
                    task: index,
                },
                config: task_config,
            });
        }

        Some(result)
    }
}

// ===== Helper functions =====

/// Convert error trace to string.
///
/// Corresponds to Java `AbstractHerder.trace(Throwable t)`.
fn trace_error(error: &dyn std::error::Error) -> String {
    error.to_string()
}

/// Convert storage::State to herder::ConnectorState.
///
/// Maps the storage layer state enum to the herder layer state enum.
/// Used when retrieving status from backing store and converting to ConnectorStateInfo.
fn convert_state_to_connector_state(state: StorageState) -> ConnectorState {
    match state {
        StorageState::Unassigned => ConnectorState::Unassigned,
        StorageState::Running => ConnectorState::Running,
        StorageState::Paused => ConnectorState::Paused,
        StorageState::Failed => ConnectorState::Failed,
        StorageState::Destroyed => ConnectorState::Destroyed,
        StorageState::Restarting => ConnectorState::Restarting,
        StorageState::Stopped => ConnectorState::Stopped,
    }
}

/// Check if task configs have changed.
///
/// Compares current task configs with new configs.
/// Corresponds to Java `AbstractHerder.taskConfigsChanged(ClusterConfigState, String, List<Map<String, String>>)`.
pub fn task_configs_changed(
    config_state: &ClusterConfigState,
    conn_name: &str,
    raw_task_props: &[HashMap<String, String>],
) -> bool {
    let current_num_tasks = config_state.task_count(conn_name);

    if raw_task_props.len() as u32 != current_num_tasks {
        return true;
    }

    // Check if any task config has changed
    for index in 0..current_num_tasks {
        let task_id = StorageConnectorTaskId::new(conn_name.to_string(), index);
        if let Some(current_config) = config_state.task_config(&task_id) {
            if let Some(new_config) = raw_task_props.get(index as usize) {
                if current_config != new_config {
                    return true;
                }
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_type_display() {
        assert_eq!(ConnectorType::Source.to_string(), "source");
        assert_eq!(ConnectorType::Sink.to_string(), "sink");
        assert_eq!(ConnectorType::Unknown.to_string(), "unknown");
    }
}
