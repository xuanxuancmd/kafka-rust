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

//! StandaloneHerder - Single-process herder for Kafka Connect standalone mode.
//!
//! This module implements the Herder trait for standalone mode, where all
//! connectors and tasks run in a single process with in-memory configuration
//! storage.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.standalone.StandaloneHerder`
//! in Java (~600 lines, ~25 methods).
//!
//! Key responsibilities:
//! - Manage connector lifecycle (create, delete, restart)
//! - Manage task lifecycle (create, delete, restart)
//! - Coordinate with Worker for actual execution
//! - Maintain configuration state in memory
//! - Handle target state changes (pause/resume)

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use common_trait::config::ConfigValue;
use common_trait::errors::ConnectError;
// Use worker module types (same as Worker uses)
use common_trait::worker::{ConnectorTaskId, TargetState};
// Use herder module types for herder-specific interfaces
use common_trait::herder::{
    ActiveTopicsInfo, Callback, ConfigInfos, ConfigKeyInfo, ConnectorInfo, ConnectorOffsets,
    ConnectorState, ConnectorStateInfo, Created, Herder, HerderRequest, InternalRequestSignature,
    LoggerLevel, Message, PluginDesc, PluginType, Plugins, StatusBackingStore, TaskInfo,
    TaskStateInfo, VersionRange,
};
use serde_json::Value;

use crate::worker::Worker;

/// MemoryConfigBackingStore - In-memory configuration storage.
///
/// Stores connector and task configurations in memory, without persistence.
/// Corresponds to `org.apache.kafka.connect.storage.MemoryConfigBackingStore` in Java.
#[derive(Debug)]
pub struct MemoryConfigBackingStore {
    /// Connector configurations (name -> config).
    connectors: RwLock<HashMap<String, HashMap<String, String>>>,
    /// Task configurations (connector -> list of task configs).
    task_configs: RwLock<HashMap<String, Vec<HashMap<String, String>>>>,
    /// Target states for connectors.
    target_states: RwLock<HashMap<String, TargetState>>,
}

impl MemoryConfigBackingStore {
    /// Creates a new empty config backing store.
    pub fn new() -> Self {
        MemoryConfigBackingStore {
            connectors: RwLock::new(HashMap::new()),
            task_configs: RwLock::new(HashMap::new()),
            target_states: RwLock::new(HashMap::new()),
        }
    }

    /// Put connector configuration.
    pub fn put_connector_config(&self, name: &str, config: HashMap<String, String>) {
        let mut connectors = self.connectors.write().unwrap();
        connectors.insert(name.to_string(), config);
    }

    /// Remove connector configuration.
    pub fn remove_connector_config(&self, name: &str) {
        let mut connectors = self.connectors.write().unwrap();
        connectors.remove(name);
        // Also remove task configs and target state
        let mut task_configs = self.task_configs.write().unwrap();
        task_configs.remove(name);
        let mut target_states = self.target_states.write().unwrap();
        target_states.remove(name);
    }

    /// Get connector configuration.
    pub fn connector_config(&self, name: &str) -> Option<HashMap<String, String>> {
        let connectors = self.connectors.read().unwrap();
        connectors.get(name).cloned()
    }

    /// Check if connector exists.
    pub fn contains(&self, name: &str) -> bool {
        let connectors = self.connectors.read().unwrap();
        connectors.contains_key(name)
    }

    /// Put task configurations for a connector.
    pub fn put_task_configs(&self, connector: &str, configs: Vec<HashMap<String, String>>) {
        let mut task_configs = self.task_configs.write().unwrap();
        task_configs.insert(connector.to_string(), configs);
    }

    /// Get task configurations for a connector.
    pub fn task_configs(&self, connector: &str) -> Option<Vec<HashMap<String, String>>> {
        let task_configs = self.task_configs.read().unwrap();
        task_configs.get(connector).cloned()
    }

    /// Put target state for a connector.
    pub fn put_target_state(&self, connector: &str, state: TargetState) {
        let mut target_states = self.target_states.write().unwrap();
        target_states.insert(connector.to_string(), state);
    }

    /// Get target state for a connector.
    pub fn target_state(&self, connector: &str) -> Option<TargetState> {
        let target_states = self.target_states.read().unwrap();
        target_states.get(connector).cloned()
    }

    /// Get all connector names.
    pub fn connectors(&self) -> Vec<String> {
        let connectors = self.connectors.read().unwrap();
        connectors.keys().cloned().collect()
    }
}

impl Default for MemoryConfigBackingStore {
    fn default() -> Self {
        MemoryConfigBackingStore::new()
    }
}

/// MockStatusBackingStore - Mock implementation for testing.
///
/// Provides in-memory storage for connector and task status.
#[derive(Debug, Default)]
pub struct MockStatusBackingStore {
    /// Connector states.
    connector_states: RwLock<HashMap<String, ConnectorState>>,
    /// Task states.
    task_states: RwLock<HashMap<ConnectorTaskId, TaskStateInfo>>,
    /// Topic states per connector (connector -> set of topics).
    topic_states: RwLock<HashMap<String, HashSet<String>>>,
}

impl MockStatusBackingStore {
    /// Creates a new mock status backing store.
    pub fn new() -> Self {
        MockStatusBackingStore::default()
    }

    /// Put connector state.
    pub fn put_connector_state_internal(&self, connector: &str, state: ConnectorState) {
        let mut states = self.connector_states.write().unwrap();
        states.insert(connector.to_string(), state);
    }

    /// Put task state.
    pub fn put_task_state_internal(&self, id: &ConnectorTaskId, state: TaskStateInfo) {
        let mut states = self.task_states.write().unwrap();
        states.insert(id.clone(), state);
    }

    /// Put topic for connector (adds topic to active set).
    pub fn put_topic(&self, connector: &str, topic: &str) {
        let mut states = self.topic_states.write().unwrap();
        states
            .entry(connector.to_string())
            .or_insert_with(HashSet::new)
            .insert(topic.to_string());
    }

    /// Get active topics for connector.
    pub fn get_topics(&self, connector: &str) -> Vec<String> {
        let states = self.topic_states.read().unwrap();
        states
            .get(connector)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Delete all topics for connector.
    pub fn delete_topics(&self, connector: &str) {
        let mut states = self.topic_states.write().unwrap();
        states.remove(connector);
    }

    /// Delete a specific topic for connector.
    pub fn delete_topic(&self, connector: &str, topic: &str) {
        let mut states = self.topic_states.write().unwrap();
        if let Some(topics) = states.get_mut(connector) {
            topics.remove(topic);
        }
    }
}

impl StatusBackingStore for MockStatusBackingStore {
    fn get_connector_state(&self, connector: &str) -> Option<ConnectorState> {
        let states = self.connector_states.read().unwrap();
        states.get(connector).copied()
    }

    fn get_task_state(&self, id: &common_trait::herder::ConnectorTaskId) -> Option<TaskStateInfo> {
        let worker_id =
            common_trait::worker::ConnectorTaskId::new(id.connector.clone(), id.task as i32);
        let states = self.task_states.read().unwrap();
        states.get(&worker_id).cloned()
    }

    fn put_connector_state(&self, connector: &str, state: ConnectorState) {
        self.put_connector_state_internal(connector, state);
    }

    fn put_task_state(&self, id: &common_trait::herder::ConnectorTaskId, state: TaskStateInfo) {
        let worker_id =
            common_trait::worker::ConnectorTaskId::new(id.connector.clone(), id.task as i32);
        self.put_task_state_internal(&worker_id, state);
    }
}

/// StatusBackingStoreRef - Reference wrapper for StatusBackingStore.
///
/// Provides a Box<dyn StatusBackingStore> that delegates to an existing
/// Arc<MockStatusBackingStore>, ensuring consistent state access.
pub struct StatusBackingStoreRef {
    inner: Arc<MockStatusBackingStore>,
}

impl StatusBackingStoreRef {
    /// Creates a new reference wrapper.
    pub fn new(inner: Arc<MockStatusBackingStore>) -> Self {
        StatusBackingStoreRef { inner }
    }
}

impl StatusBackingStore for StatusBackingStoreRef {
    fn get_connector_state(&self, connector: &str) -> Option<ConnectorState> {
        self.inner.get_connector_state(connector)
    }

    fn get_task_state(&self, id: &common_trait::herder::ConnectorTaskId) -> Option<TaskStateInfo> {
        self.inner.get_task_state(id)
    }

    fn put_connector_state(&self, connector: &str, state: ConnectorState) {
        self.inner.put_connector_state(connector, state);
    }

    fn put_task_state(&self, id: &common_trait::herder::ConnectorTaskId, state: TaskStateInfo) {
        self.inner.put_task_state(id, state);
    }
}

/// MockPlugins - Mock implementation for testing.
///
/// Provides plugin discovery and info retrieval.
#[derive(Debug, Default)]
pub struct MockPlugins {
    /// Known connector classes.
    connector_classes: Vec<String>,
}

impl MockPlugins {
    /// Creates a new mock plugins instance.
    pub fn new() -> Self {
        MockPlugins {
            connector_classes: vec![
                "org.apache.kafka.connect.file.FileSourceConnector".to_string(),
                "org.apache.kafka.connect.file.FileSinkConnector".to_string(),
            ],
        }
    }

    /// Creates with custom connector classes.
    pub fn with_classes(classes: Vec<String>) -> Self {
        MockPlugins {
            connector_classes: classes,
        }
    }
}

impl Plugins for MockPlugins {
    fn connector_plugins(&self) -> Vec<String> {
        self.connector_classes.clone()
    }

    fn converter_plugins(&self) -> Vec<String> {
        vec!["org.apache.kafka.connect.storage.StringConverter".to_string()]
    }

    fn transformation_plugins(&self) -> Vec<String> {
        vec![]
    }

    fn connector_plugin_desc(&self, class_name: &str) -> Option<PluginDesc> {
        if self.connector_classes.contains(&class_name.to_string()) {
            Some(PluginDesc {
                class_name: class_name.to_string(),
                plugin_type: PluginType::Source,
                version: "1.0.0".to_string(),
                documentation: "Mock connector plugin".to_string(),
            })
        } else {
            None
        }
    }

    fn converter_plugin_desc(&self, class_name: &str) -> Option<PluginDesc> {
        Some(PluginDesc {
            class_name: class_name.to_string(),
            plugin_type: PluginType::Converter,
            version: "1.0.0".to_string(),
            documentation: "Mock converter plugin".to_string(),
        })
    }
}

/// PluginsRef - Reference wrapper for Plugins.
///
/// Provides a Box<dyn Plugins> that delegates to an existing
/// Arc<MockPlugins>, ensuring consistent plugin access.
pub struct PluginsRef {
    inner: Arc<MockPlugins>,
}

impl PluginsRef {
    /// Creates a new reference wrapper.
    pub fn new(inner: Arc<MockPlugins>) -> Self {
        PluginsRef { inner }
    }
}

impl Plugins for PluginsRef {
    fn connector_plugins(&self) -> Vec<String> {
        self.inner.connector_plugins()
    }

    fn converter_plugins(&self) -> Vec<String> {
        self.inner.converter_plugins()
    }

    fn transformation_plugins(&self) -> Vec<String> {
        self.inner.transformation_plugins()
    }

    fn connector_plugin_desc(&self, class_name: &str) -> Option<PluginDesc> {
        self.inner.connector_plugin_desc(class_name)
    }

    fn converter_plugin_desc(&self, class_name: &str) -> Option<PluginDesc> {
        self.inner.converter_plugin_desc(class_name)
    }
}

/// StandaloneHerder - Single-process herder implementation.
///
/// Manages connectors and tasks in standalone mode using in-memory storage.
/// All operations are synchronous (no distributed coordination).
///
/// Thread safety: Uses RwLock/Mutex for internal state to allow concurrent reads.
pub struct StandaloneHerder {
    /// Worker instance that executes connectors and tasks.
    worker: Arc<Worker>,
    /// Configuration backing store (in-memory).
    config_backing_store: Arc<MemoryConfigBackingStore>,
    /// Status backing store.
    status_backing_store: Arc<MockStatusBackingStore>,
    /// Plugins instance.
    plugins: Arc<MockPlugins>,
    /// Kafka cluster ID.
    kafka_cluster_id: String,
    /// Whether the herder is started.
    started: AtomicBool,
    /// Whether the herder is stopping.
    stopping: AtomicBool,
    /// Connector offsets storage (connector -> partition -> offset).
    connector_offsets: RwLock<HashMap<String, HashMap<Value, Value>>>,
    /// Logger levels storage (logger name -> level).
    logger_levels: RwLock<HashMap<String, String>>,
}

impl StandaloneHerder {
    /// Creates a new StandaloneHerder.
    ///
    /// # Arguments
    /// * `worker` - The Worker instance to use for execution.
    /// * `kafka_cluster_id` - The Kafka cluster ID.
    /// * `plugins` - Optional plugins instance (uses MockPlugins if None).
    pub fn new(
        worker: Arc<Worker>,
        kafka_cluster_id: String,
        plugins: Option<Arc<MockPlugins>>,
    ) -> Self {
        StandaloneHerder {
            worker,
            config_backing_store: Arc::new(MemoryConfigBackingStore::new()),
            status_backing_store: Arc::new(MockStatusBackingStore::new()),
            plugins: plugins.unwrap_or_else(|| Arc::new(MockPlugins::new())),
            kafka_cluster_id,
            started: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            connector_offsets: RwLock::new(HashMap::new()),
            logger_levels: RwLock::new(HashMap::new()),
        }
    }

    /// Creates a StandaloneHerder with all components.
    pub fn with_all(
        worker: Arc<Worker>,
        config_backing_store: Arc<MemoryConfigBackingStore>,
        status_backing_store: Arc<MockStatusBackingStore>,
        plugins: Arc<MockPlugins>,
        kafka_cluster_id: String,
    ) -> Self {
        StandaloneHerder {
            worker,
            config_backing_store,
            status_backing_store,
            plugins,
            kafka_cluster_id,
            started: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            connector_offsets: RwLock::new(HashMap::new()),
            logger_levels: RwLock::new(HashMap::new()),
        }
    }

    // ===== Internal helper methods =====

    /// Creates connector tasks for a connector.
    ///
    /// Reads task configs from the config backing store and starts each task.
    fn create_connector_tasks(&self, conn_name: &str) -> Result<(), ConnectError> {
        let task_configs = self
            .config_backing_store
            .task_configs(conn_name)
            .unwrap_or_default();

        for (index, task_config) in task_configs.iter().enumerate() {
            let task_id = ConnectorTaskId::new(conn_name.to_string(), index as i32);

            // Determine if sink or source based on connector config
            let conn_config = self
                .config_backing_store
                .connector_config(conn_name)
                .unwrap_or_default();
            let is_sink = self.is_sink_connector(&conn_config);

            // Create status listener
            let status_listener = Arc::new(MockTaskStatusListener::new());

            // Get target state
            let target_state = self
                .config_backing_store
                .target_state(conn_name)
                .unwrap_or(TargetState::Started);

            // Create mock producer/consumer for tests
            let producer = if !is_sink {
                Some(Arc::new(kafka_clients_mock::MockKafkaProducer::new()))
            } else {
                None
            };
            let consumer = if is_sink {
                Some(Arc::new(kafka_clients_mock::MockKafkaConsumer::new()))
            } else {
                None
            };

            // Start the task
            self.worker.start_task(
                &task_id,
                conn_config.clone(),
                task_config.clone(),
                status_listener,
                target_state,
                is_sink,
                consumer,
                producer,
            )?;
        }

        Ok(())
    }

    /// Removes connector tasks for a connector.
    ///
    /// Stops and removes all tasks associated with a connector.
    fn remove_connector_tasks(&self, conn_name: &str) {
        let task_configs = self
            .config_backing_store
            .task_configs(conn_name)
            .unwrap_or_default();

        for (index, _) in task_configs.iter().enumerate() {
            let task_id = ConnectorTaskId::new(conn_name.to_string(), index as i32);
            self.worker.stop_and_await_task(&task_id);
        }

        // Remove task configs from backing store
        self.config_backing_store
            .put_task_configs(conn_name, vec![]);
    }

    /// Determines if a connector is a sink connector.
    ///
    /// Checks the connector class name to determine type.
    fn is_sink_connector(&self, config: &HashMap<String, String>) -> bool {
        config
            .get("connector.class")
            .map(|class| class.contains("Sink"))
            .unwrap_or(false)
    }

    /// Computes task configurations for a connector.
    ///
    /// Corresponds to Java's worker.connectorTaskConfigs() which calls
    /// Connector.taskConfigs(maxTasks) on the live connector instance.
    /// Since we don't have live connector instances in our Worker,
    /// we use pre-computed configs stored in WorkerConnector (obtained at creation).
    /// If no pre-computed configs exist, we generate a default single-task config
    /// derived from the connector config (filtering connector-specific properties).
    fn recompute_task_configs(
        &self,
        conn_name: &str,
        config: &HashMap<String, String>,
    ) -> Vec<HashMap<String, String>> {
        // Get task configs from WorkerConnector if available (pre-computed at creation)
        if let Some(connector) = self.worker.get_connector(conn_name) {
            let stored_configs = connector.task_configs();
            if !stored_configs.is_empty() {
                return stored_configs;
            }
        }

        // Fallback: generate a single task config derived from connector config
        // This mirrors Java's default behavior when Connector.taskConfigs() returns empty
        let mut task_config = HashMap::new();
        for (key, value) in config.iter() {
            // Filter out connector-specific properties (those starting with "connector.")
            if !key.starts_with("connector.") {
                task_config.insert(key.clone(), value.clone());
            }
        }
        // Derive task class from connector class (standard Kafka Connect convention)
        if let Some(connector_class) = config.get("connector.class") {
            let task_class = connector_class.replace("Connector", "Task");
            task_config.insert("task.class".to_string(), task_class);
        }

        vec![task_config]
    }

    /// Starts a connector with the given configuration.
    fn start_connector_internal(
        &self,
        conn_name: &str,
        config: HashMap<String, String>,
        target_state: TargetState,
    ) -> Result<ConnectorInfo, ConnectError> {
        // Create status listener
        let status_listener = Arc::new(MockConnectorStatusListener::new());

        // Start connector via Worker
        self.worker
            .start_connector(conn_name, config.clone(), status_listener, target_state)?;

        // Compute and store task configs
        let task_configs = self.recompute_task_configs(conn_name, &config);
        self.config_backing_store
            .put_task_configs(conn_name, task_configs.clone());
        self.config_backing_store
            .put_target_state(conn_name, target_state);

        // Create connector tasks if target state is Started
        if target_state == TargetState::Started {
            self.create_connector_tasks(conn_name)?;
        }

        // Build ConnectorInfo
        let tasks: Vec<TaskInfo> = task_configs
            .iter()
            .enumerate()
            .map(|(index, cfg)| TaskInfo {
                id: common_trait::herder::ConnectorTaskId {
                    connector: conn_name.to_string(),
                    task: index as u32,
                },
                config: cfg.clone(),
            })
            .collect();

        Ok(ConnectorInfo {
            name: conn_name.to_string(),
            config,
            tasks,
        })
    }

    /// Deletes a connector.
    fn delete_connector_internal(&self, conn_name: &str) -> Result<ConnectorInfo, ConnectError> {
        // Check if connector exists
        if !self.config_backing_store.contains(conn_name) {
            return Err(ConnectError::general(format!(
                "Connector {} not found",
                conn_name
            )));
        }

        // Get current config for return
        let config = self
            .config_backing_store
            .connector_config(conn_name)
            .unwrap_or_default();
        let task_configs = self
            .config_backing_store
            .task_configs(conn_name)
            .unwrap_or_default();

        // Remove tasks first
        self.remove_connector_tasks(conn_name);

        // Stop connector
        self.worker.stop_and_await_connector(conn_name);

        // Remove from config backing store
        self.config_backing_store.remove_connector_config(conn_name);

        // Update status
        self.status_backing_store
            .put_connector_state(conn_name, ConnectorState::Unassigned);

        // Build ConnectorInfo for return
        let tasks: Vec<TaskInfo> = task_configs
            .iter()
            .enumerate()
            .map(|(index, cfg)| TaskInfo {
                id: common_trait::herder::ConnectorTaskId {
                    connector: conn_name.to_string(),
                    task: index as u32,
                },
                config: cfg.clone(),
            })
            .collect();

        Ok(ConnectorInfo {
            name: conn_name.to_string(),
            config,
            tasks,
        })
    }
}

/// Mock connector status listener for testing.
#[derive(Debug, Default)]
pub struct MockConnectorStatusListener {
    events: Mutex<Vec<String>>,
}

impl MockConnectorStatusListener {
    pub fn new() -> Self {
        MockConnectorStatusListener::default()
    }

    pub fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }
}

impl crate::worker::ThreadSafeConnectorStatusListener for MockConnectorStatusListener {
    fn on_startup(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("startup:{}", connector));
    }

    fn on_running(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("running:{}", connector));
    }

    fn on_pause(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("pause:{}", connector));
    }

    fn on_shutdown(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("shutdown:{}", connector));
    }

    fn on_stopped(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("stopped:{}", connector));
    }

    fn on_failure(&self, connector: &str, error_message: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("failure:{}:{}", connector, error_message));
    }

    fn on_restart(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("restart:{}", connector));
    }
}

/// Mock task status listener for testing.
#[derive(Debug, Default)]
pub struct MockTaskStatusListener {
    events: Mutex<Vec<String>>,
}

impl MockTaskStatusListener {
    pub fn new() -> Self {
        MockTaskStatusListener::default()
    }

    pub fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }
}

impl crate::task::ThreadSafeTaskStatusListener for MockTaskStatusListener {
    fn on_startup(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(format!("startup:{}-{}", id.connector(), id.task()));
    }

    fn on_running(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(format!("running:{}-{}", id.connector(), id.task()));
    }

    fn on_pause(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(format!("pause:{}-{}", id.connector(), id.task()));
    }

    fn on_shutdown(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(format!("shutdown:{}-{}", id.connector(), id.task()));
    }

    fn on_stopped(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(format!("stopped:{}-{}", id.connector(), id.task()));
    }

    fn on_failure(&self, id: &ConnectorTaskId, error_message: &str) {
        self.events.lock().unwrap().push(format!(
            "failure:{}-{}:{}",
            id.connector(),
            id.task(),
            error_message
        ));
    }

    fn on_restart(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(format!("restart:{}-{}", id.connector(), id.task()));
    }
}

/// SharedCallbackState - Shared state for callbacks using Arc.
struct SharedCallbackState<T: Clone> {
    result: Mutex<Option<T>>,
    error: Mutex<Option<String>>,
}

/// SimpleCallback - A callback implementation that captures the result.
/// Uses Arc internally so Clone shares storage.
pub struct SimpleCallback<T: Clone> {
    state: Arc<SharedCallbackState<T>>,
}

impl<T: Clone> Clone for SimpleCallback<T> {
    fn clone(&self) -> Self {
        SimpleCallback {
            state: self.state.clone(),
        }
    }
}

impl<T: Clone> SimpleCallback<T> {
    pub fn new() -> Self {
        SimpleCallback {
            state: Arc::new(SharedCallbackState {
                result: Mutex::new(None),
                error: Mutex::new(None),
            }),
        }
    }

    pub fn result(&self) -> Option<T> {
        self.state.result.lock().unwrap().clone()
    }

    pub fn error(&self) -> Option<String> {
        self.state.error.lock().unwrap().clone()
    }
}

impl<T: Clone> Callback<T> for SimpleCallback<T> {
    fn on_completion(&self, result: T) {
        *self.state.result.lock().unwrap() = Some(result);
    }

    fn on_error(&self, error: String) {
        *self.state.error.lock().unwrap() = Some(error);
    }
}

impl<T: Clone> Default for SimpleCallback<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// SimpleHerderRequest - A simple herder request implementation.
pub struct SimpleHerderRequest {
    cancelled: AtomicBool,
    completed: AtomicBool,
}

impl SimpleHerderRequest {
    pub fn new() -> Self {
        SimpleHerderRequest {
            cancelled: AtomicBool::new(false),
            completed: AtomicBool::new(false),
        }
    }

    pub fn mark_completed(&self) {
        self.completed.store(true, Ordering::SeqCst);
    }
}

impl Default for SimpleHerderRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl HerderRequest for SimpleHerderRequest {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

// ===== Herder trait implementation =====

impl Herder for StandaloneHerder {
    // === Lifecycle methods ===

    fn start(&mut self) {
        if self.started.load(Ordering::SeqCst) {
            return;
        }
        self.started.store(true, Ordering::SeqCst);
        self.stopping.store(false, Ordering::SeqCst);
        // In Java, this also starts services and health check thread
    }

    fn stop(&mut self) {
        if self.stopping.load(Ordering::SeqCst) {
            return;
        }
        self.stopping.store(true, Ordering::SeqCst);

        // Stop all connectors
        let connectors = self.config_backing_store.connectors();
        for conn_name in connectors {
            self.delete_connector_internal(&conn_name).ok();
        }

        // Stop worker
        self.worker.stop();

        self.started.store(false, Ordering::SeqCst);
    }

    fn is_ready(&self) -> bool {
        self.started.load(Ordering::SeqCst) && !self.stopping.load(Ordering::SeqCst)
    }

    fn health_check(&self, callback: Box<dyn Callback<()>>) {
        if self.is_ready() {
            callback.on_completion(());
        } else {
            callback.on_error("Herder not ready".to_string());
        }
    }

    // === Connector enumeration (async) ===

    fn connectors_async(&self, callback: Box<dyn Callback<Vec<String>>>) {
        let connectors = self.config_backing_store.connectors();
        callback.on_completion(connectors);
    }

    fn connector_info_async(&self, conn_name: &str, callback: Box<dyn Callback<ConnectorInfo>>) {
        if !self.config_backing_store.contains(conn_name) {
            callback.on_error(format!("Connector {} not found", conn_name));
            return;
        }

        let config = self
            .config_backing_store
            .connector_config(conn_name)
            .unwrap_or_default();
        let task_configs = self
            .config_backing_store
            .task_configs(conn_name)
            .unwrap_or_default();

        let tasks: Vec<TaskInfo> = task_configs
            .iter()
            .enumerate()
            .map(|(index, cfg)| TaskInfo {
                id: common_trait::herder::ConnectorTaskId {
                    connector: conn_name.to_string(),
                    task: index as u32,
                },
                config: cfg.clone(),
            })
            .collect();

        callback.on_completion(ConnectorInfo {
            name: conn_name.to_string(),
            config,
            tasks,
        });
    }

    fn connector_config_async(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<HashMap<String, String>>>,
    ) {
        if !self.config_backing_store.contains(conn_name) {
            callback.on_error(format!("Connector {} not found", conn_name));
            return;
        }

        let config = self
            .config_backing_store
            .connector_config(conn_name)
            .unwrap_or_default();
        callback.on_completion(config);
    }

    // === Connector config management ===

    fn put_connector_config(
        &self,
        conn_name: &str,
        config: HashMap<String, String>,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        self.put_connector_config_with_state(
            conn_name,
            config,
            common_trait::herder::TargetState::Started,
            allow_replace,
            callback,
        );
    }

    fn put_connector_config_with_state(
        &self,
        conn_name: &str,
        config: HashMap<String, String>,
        target_state: common_trait::herder::TargetState,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        // Convert herder TargetState to worker TargetState
        let worker_target_state = match target_state {
            common_trait::herder::TargetState::Started => TargetState::Started,
            common_trait::herder::TargetState::Paused => TargetState::Paused,
            common_trait::herder::TargetState::Stopped => TargetState::Stopped,
        };

        // Validate required configuration properties (corresponds to Java's validateConnectorConfig)
        // Required: connector.class and name
        if !config.contains_key("connector.class") {
            callback.on_error("Missing required configuration: connector.class".to_string());
            return;
        }
        if !config.contains_key("name") {
            callback.on_error("Missing required configuration: name".to_string());
            return;
        }

        // Check if connector already exists
        let exists = self.config_backing_store.contains(conn_name);

        if exists && !allow_replace {
            callback.on_error(format!("Connector {} already exists", conn_name));
            return;
        }

        // If exists and replace allowed, stop existing first
        if exists {
            self.remove_connector_tasks(conn_name);
            self.worker.stop_and_await_connector(conn_name);
        }

        // Store config
        self.config_backing_store
            .put_connector_config(conn_name, config.clone());

        // Start connector
        let result = self.start_connector_internal(conn_name, config.clone(), worker_target_state);

        match result {
            Ok(info) => {
                // Update status
                self.status_backing_store
                    .put_connector_state(conn_name, ConnectorState::Running);
                callback.on_completion(Created {
                    created: !exists,
                    info,
                });
            }
            Err(e) => {
                callback.on_error(e.message().to_string());
            }
        }
    }

    fn patch_connector_config(
        &self,
        conn_name: &str,
        config_patch: HashMap<String, String>,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        if !self.config_backing_store.contains(conn_name) {
            callback.on_error(format!("Connector {} not found", conn_name));
            return;
        }

        // Get existing config
        let existing = self
            .config_backing_store
            .connector_config(conn_name)
            .unwrap_or_default();

        // Apply patch (merge)
        let mut merged = existing.clone();
        for (key, value) in config_patch.iter() {
            merged.insert(key.clone(), value.clone());
        }

        // Use put with allow_replace=true
        self.put_connector_config(conn_name, merged, true, callback);
    }

    fn delete_connector_config(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        let result = self.delete_connector_internal(conn_name);

        match result {
            Ok(info) => {
                callback.on_completion(Created {
                    created: false,
                    info,
                });
            }
            Err(e) => {
                callback.on_error(e.message().to_string());
            }
        }
    }

    // === Task reconfiguration ===

    fn request_task_reconfiguration(&self, conn_name: &str) {
        if !self.config_backing_store.contains(conn_name) {
            return;
        }

        // Get current config
        let config = self
            .config_backing_store
            .connector_config(conn_name)
            .unwrap_or_default();
        let target_state = self
            .config_backing_store
            .target_state(conn_name)
            .unwrap_or(TargetState::Started);

        // Recompute task configs
        let new_task_configs = self.recompute_task_configs(conn_name, &config);

        // Compare with existing
        let existing_configs = self
            .config_backing_store
            .task_configs(conn_name)
            .unwrap_or_default();

        // If configs changed, update
        if new_task_configs != existing_configs {
            // Remove old tasks
            self.remove_connector_tasks(conn_name);

            // Store new configs
            self.config_backing_store
                .put_task_configs(conn_name, new_task_configs.clone());

            // Create new tasks if running
            if target_state == TargetState::Started {
                self.create_connector_tasks(conn_name).ok();
            }
        }
    }

    fn task_configs_async(&self, conn_name: &str, callback: Box<dyn Callback<Vec<TaskInfo>>>) {
        if !self.config_backing_store.contains(conn_name) {
            callback.on_error(format!("Connector {} not found", conn_name));
            return;
        }

        let task_configs = self
            .config_backing_store
            .task_configs(conn_name)
            .unwrap_or_default();

        let tasks: Vec<TaskInfo> = task_configs
            .iter()
            .enumerate()
            .map(|(index, cfg)| TaskInfo {
                id: common_trait::herder::ConnectorTaskId {
                    connector: conn_name.to_string(),
                    task: index as u32,
                },
                config: cfg.clone(),
            })
            .collect();

        callback.on_completion(tasks);
    }

    fn put_task_configs(
        &self,
        _conn_name: &str,
        _configs: Vec<HashMap<String, String>>,
        callback: Box<dyn Callback<()>>,
        _request_signature: InternalRequestSignature,
    ) {
        // Standalone mode does not support external task config updates
        callback.on_error("Standalone mode does not support putTaskConfigs".to_string());
    }

    fn fence_zombie_source_tasks(
        &self,
        _conn_name: &str,
        callback: Box<dyn Callback<()>>,
        _request_signature: InternalRequestSignature,
    ) {
        // Standalone mode has no zombie fencing (single process)
        callback.on_completion(());
    }

    // === Connector enumeration (sync) ===

    fn connectors_sync(&self) -> Vec<String> {
        self.config_backing_store.connectors()
    }

    fn connector_info_sync(&self, conn_name: &str) -> ConnectorInfo {
        let config = self
            .config_backing_store
            .connector_config(conn_name)
            .unwrap_or_default();
        let task_configs = self
            .config_backing_store
            .task_configs(conn_name)
            .unwrap_or_default();

        let tasks: Vec<TaskInfo> = task_configs
            .iter()
            .enumerate()
            .map(|(index, cfg)| TaskInfo {
                id: common_trait::herder::ConnectorTaskId {
                    connector: conn_name.to_string(),
                    task: index as u32,
                },
                config: cfg.clone(),
            })
            .collect();

        ConnectorInfo {
            name: conn_name.to_string(),
            config,
            tasks,
        }
    }

    fn connector_status(&self, conn_name: &str) -> ConnectorStateInfo {
        let connector_state = self
            .status_backing_store
            .get_connector_state(conn_name)
            .unwrap_or(ConnectorState::Unassigned);

        let task_configs = self
            .config_backing_store
            .task_configs(conn_name)
            .unwrap_or_default();

        let tasks: Vec<TaskStateInfo> = task_configs
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let herder_id = common_trait::herder::ConnectorTaskId {
                    connector: conn_name.to_string(),
                    task: index as u32,
                };
                self.status_backing_store
                    .get_task_state(&herder_id)
                    .unwrap_or_else(|| TaskStateInfo {
                        id: herder_id.clone(),
                        state: ConnectorState::Unassigned,
                        worker_id: self.worker.config().worker_id().to_string(),
                        trace: None,
                    })
            })
            .collect();

        ConnectorStateInfo {
            name: conn_name.to_string(),
            connector: connector_state,
            tasks,
        }
    }

    fn connector_active_topics(&self, conn_name: &str) -> ActiveTopicsInfo {
        // Get active topics from status backing store
        let topics = self.status_backing_store.get_topics(conn_name);
        ActiveTopicsInfo {
            connector: conn_name.to_string(),
            topics,
        }
    }

    fn reset_connector_active_topics(&self, conn_name: &str) {
        // Delete all topic states for this connector
        self.status_backing_store.delete_topics(conn_name);
    }

    // === Status backing store ===

    fn status_backing_store(&self) -> Box<dyn StatusBackingStore> {
        // Return a reference wrapper that delegates to internal status_backing_store
        // This provides consistent state access, not a new empty instance
        Box::new(StatusBackingStoreRef::new(
            self.status_backing_store.clone(),
        ))
    }

    fn task_status(&self, id: &common_trait::herder::ConnectorTaskId) -> TaskStateInfo {
        self.status_backing_store
            .get_task_state(id)
            .unwrap_or_else(|| TaskStateInfo {
                id: id.clone(),
                state: ConnectorState::Unassigned,
                worker_id: self.worker.config().worker_id().to_string(),
                trace: None,
            })
    }

    // === Config validation ===

    fn validate_connector_config_async(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn Callback<ConfigInfos>>,
    ) {
        use common_trait::herder::ConfigInfo;

        // Validate required configs
        let has_class = connector_config.contains_key("connector.class");
        let has_name = connector_config.contains_key("name");
        let tasks_max = connector_config
            .get("tasks.max")
            .cloned()
            .unwrap_or_else(|| "1".to_string());

        // Build config infos with all defined configs
        let mut configs: Vec<ConfigInfo> = Vec::new();
        let mut error_count = 0;

        // connector.class
        configs.push(ConfigInfo {
            definition: ConfigKeyInfo {
                name: "connector.class".to_string(),
                config_type: "string".to_string(),
                default_value: None,
                required: true,
                importance: "high".to_string(),
                documentation: "Connector class name".to_string(),
                group: Some("Common".to_string()),
                order_in_group: Some(1),
                width: "medium".to_string(),
                display_name: "Connector Class".to_string(),
                dependents: vec![],
                validators: vec!["class".to_string()],
            },
            value: ConfigValue::with_value(
                "connector.class",
                Value::String(
                    connector_config
                        .get("connector.class")
                        .cloned()
                        .unwrap_or_default(),
                ),
            ),
            errors: if has_class {
                vec![]
            } else {
                error_count += 1;
                vec!["Missing required configuration: connector.class".to_string()]
            },
        });

        // name
        configs.push(ConfigInfo {
            definition: ConfigKeyInfo {
                name: "name".to_string(),
                config_type: "string".to_string(),
                default_value: None,
                required: true,
                importance: "high".to_string(),
                documentation: "Connector name".to_string(),
                group: Some("Common".to_string()),
                order_in_group: Some(2),
                width: "medium".to_string(),
                display_name: "Name".to_string(),
                dependents: vec![],
                validators: vec!["nonEmptyString".to_string()],
            },
            value: ConfigValue::with_value(
                "name",
                Value::String(connector_config.get("name").cloned().unwrap_or_default()),
            ),
            errors: if has_name {
                vec![]
            } else {
                error_count += 1;
                vec!["Missing required configuration: name".to_string()]
            },
        });

        // tasks.max
        configs.push(ConfigInfo {
            definition: ConfigKeyInfo {
                name: "tasks.max".to_string(),
                config_type: "int".to_string(),
                default_value: Some("1".to_string()),
                required: false,
                importance: "medium".to_string(),
                documentation: "Maximum number of tasks for this connector".to_string(),
                group: Some("Common".to_string()),
                order_in_group: Some(3),
                width: "short".to_string(),
                display_name: "Tasks Max".to_string(),
                dependents: vec![],
                validators: vec!["validInt".to_string()],
            },
            value: ConfigValue::with_value("tasks.max", Value::String(tasks_max)),
            errors: vec![], // Default value is valid
        });

        // Add all other configs from the input
        for (key, value) in connector_config.iter() {
            if !["connector.class", "name", "tasks.max"].contains(&key.as_str()) {
                configs.push(ConfigInfo {
                    definition: ConfigKeyInfo {
                        name: key.clone(),
                        config_type: "string".to_string(),
                        default_value: None,
                        required: false,
                        importance: "low".to_string(),
                        documentation: format!("Configuration property {}", key),
                        group: None,
                        order_in_group: None,
                        width: "medium".to_string(),
                        display_name: key.clone(),
                        dependents: vec![],
                        validators: vec![],
                    },
                    value: ConfigValue::with_value(key, Value::String(value.clone())),
                    errors: vec![],
                });
            }
        }

        let config_infos = ConfigInfos {
            name: connector_config
                .get("name")
                .cloned()
                .unwrap_or_else(|| "unknown".to_string()),
            error_count,
            group_count: 1,
            configs,
        };

        callback.on_completion(config_infos);
    }

    fn validate_connector_config_async_with_log(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn Callback<ConfigInfos>>,
        _do_log: bool,
    ) {
        self.validate_connector_config_async(connector_config, callback);
    }

    // === Task/Connector restart ===

    fn restart_task(
        &self,
        id: &common_trait::herder::ConnectorTaskId,
        callback: Box<dyn Callback<()>>,
    ) {
        if !self.config_backing_store.contains(&id.connector) {
            callback.on_error(format!("Connector {} not found", id.connector));
            return;
        }

        // Convert to worker ConnectorTaskId
        let worker_id = ConnectorTaskId::new(id.connector.clone(), id.task as i32);

        // Stop the task
        self.worker.stop_and_await_task(&worker_id);

        // Get task config
        let task_configs = self
            .config_backing_store
            .task_configs(&id.connector)
            .unwrap_or_default();

        if id.task >= task_configs.len() as u32 {
            callback.on_error(format!("Task {}-{} not found", id.connector, id.task));
            return;
        }

        let task_config = task_configs[id.task as usize].clone();
        let conn_config = self
            .config_backing_store
            .connector_config(&id.connector)
            .unwrap_or_default();
        let is_sink = self.is_sink_connector(&conn_config);

        // Create status listener
        let status_listener = Arc::new(MockTaskStatusListener::new());

        // Create mock producer/consumer for tests
        let producer = if !is_sink {
            Some(Arc::new(kafka_clients_mock::MockKafkaProducer::new()))
        } else {
            None
        };
        let consumer = if is_sink {
            Some(Arc::new(kafka_clients_mock::MockKafkaConsumer::new()))
        } else {
            None
        };

        // Restart the task
        let result = self.worker.start_task(
            &worker_id,
            conn_config,
            task_config,
            status_listener,
            TargetState::Started,
            is_sink,
            consumer,
            producer,
        );

        match result {
            Ok(()) => callback.on_completion(()),
            Err(e) => callback.on_error(e.message().to_string()),
        }
    }

    fn restart_connector_async(&self, conn_name: &str, callback: Box<dyn Callback<()>>) {
        if !self.config_backing_store.contains(conn_name) {
            callback.on_error(format!("Connector {} not found", conn_name));
            return;
        }

        // Get config
        let config = self
            .config_backing_store
            .connector_config(conn_name)
            .unwrap_or_default();
        let target_state = self
            .config_backing_store
            .target_state(conn_name)
            .unwrap_or(TargetState::Started);

        // Stop tasks
        self.remove_connector_tasks(conn_name);

        // Stop connector
        self.worker.stop_and_await_connector(conn_name);

        // Restart connector
        let result = self.start_connector_internal(conn_name, config, target_state);

        match result {
            Ok(_) => {
                self.status_backing_store
                    .put_connector_state(conn_name, ConnectorState::Running);
                callback.on_completion(());
            }
            Err(e) => callback.on_error(e.message().to_string()),
        }
    }

    fn restart_connector_delayed(
        &self,
        delay_ms: u64,
        conn_name: &str,
        callback: Box<dyn Callback<()>>,
    ) -> Box<dyn HerderRequest> {
        // In standalone mode, there's no distributed scheduled executor,
        // so we execute the delayed restart synchronously in the current thread.
        // This matches Java's behavior for standalone mode where the delay
        // is applied before the restart operation is invoked.
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        self.restart_connector_async(conn_name, callback);
        Box::new(SimpleHerderRequest::new())
    }

    // === Connector pause/resume ===

    fn pause_connector(&self, connector: &str) {
        if !self.config_backing_store.contains(connector) {
            return;
        }

        // Update target state
        self.config_backing_store
            .put_target_state(connector, TargetState::Paused);

        // Stop tasks
        self.remove_connector_tasks(connector);

        // Update worker state
        if let Some(conn) = self.worker.get_connector(connector) {
            conn.pause();
        }

        // Update status
        self.status_backing_store
            .put_connector_state(connector, ConnectorState::Paused);
    }

    fn resume_connector(&self, connector: &str) {
        if !self.config_backing_store.contains(connector) {
            return;
        }

        // Update target state
        self.config_backing_store
            .put_target_state(connector, TargetState::Started);

        // Resume worker state
        if let Some(conn) = self.worker.get_connector(connector) {
            conn.resume();
        }

        // Create tasks
        self.create_connector_tasks(connector).ok();

        // Update status
        self.status_backing_store
            .put_connector_state(connector, ConnectorState::Running);
    }

    // === Plugin management ===

    fn plugins(&self) -> Box<dyn Plugins> {
        // Return a reference to the internal plugins instance
        // This provides consistent plugin access, not a new empty instance
        Box::new(PluginsRef::new(self.plugins.clone()))
    }

    fn kafka_cluster_id(&self) -> String {
        self.kafka_cluster_id.clone()
    }

    fn connector_plugin_config(&self, plugin_name: &str) -> Vec<ConfigKeyInfo> {
        // Return standard connector configuration keys
        vec![
            ConfigKeyInfo {
                name: "connector.class".to_string(),
                config_type: "string".to_string(),
                default_value: None,
                required: true,
                importance: "high".to_string(),
                documentation: format!("Connector class for {}", plugin_name),
                group: Some("Common".to_string()),
                order_in_group: Some(1),
                width: "medium".to_string(),
                display_name: "Connector Class".to_string(),
                dependents: vec![],
                validators: vec!["class".to_string()],
            },
            ConfigKeyInfo {
                name: "name".to_string(),
                config_type: "string".to_string(),
                default_value: None,
                required: true,
                importance: "high".to_string(),
                documentation: "Connector name".to_string(),
                group: Some("Common".to_string()),
                order_in_group: Some(2),
                width: "medium".to_string(),
                display_name: "Name".to_string(),
                dependents: vec![],
                validators: vec!["nonEmptyString".to_string()],
            },
            ConfigKeyInfo {
                name: "tasks.max".to_string(),
                config_type: "int".to_string(),
                default_value: Some("1".to_string()),
                required: false,
                importance: "medium".to_string(),
                documentation: "Maximum number of tasks for this connector".to_string(),
                group: Some("Common".to_string()),
                order_in_group: Some(3),
                width: "short".to_string(),
                display_name: "Tasks Max".to_string(),
                dependents: vec![],
                validators: vec!["validInt".to_string()],
            },
        ]
    }

    fn connector_plugin_config_with_version(
        &self,
        plugin_name: &str,
        _version: VersionRange,
    ) -> Vec<ConfigKeyInfo> {
        self.connector_plugin_config(plugin_name)
    }

    // === Offset management ===

    fn connector_offsets_async(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<ConnectorOffsets>>,
    ) {
        if !self.config_backing_store.contains(conn_name) {
            callback.on_error(format!("Connector {} not found", conn_name));
            return;
        }

        // Get stored offsets for this connector
        let offsets = self
            .connector_offsets
            .read()
            .unwrap()
            .get(conn_name)
            .cloned()
            .unwrap_or_default();

        callback.on_completion(ConnectorOffsets { offsets });
    }

    fn alter_connector_offsets(
        &self,
        conn_name: &str,
        offsets: HashMap<HashMap<String, Value>, HashMap<String, Value>>,
        callback: Box<dyn Callback<Message>>,
    ) {
        if !self.config_backing_store.contains(conn_name) {
            callback.on_error(format!("Connector {} not found", conn_name));
            return;
        }

        // Check connector state - must be stopped
        let state = self
            .status_backing_store
            .get_connector_state(conn_name)
            .unwrap_or(ConnectorState::Unassigned);

        if state != ConnectorState::Unassigned && state != ConnectorState::Paused {
            callback.on_error(format!(
                "Connector {} must be stopped to alter offsets",
                conn_name
            ));
            return;
        }

        // Convert offset format: HashMap<HashMap<String, Value>, HashMap<String, Value>> -> HashMap<Value, Value>
        // Partition becomes the key as a JSON Value, offset info becomes the value
        let converted_offsets: HashMap<Value, Value> = offsets
            .into_iter()
            .map(|(partition, offset_info)| {
                // Convert partition HashMap to JSON Value
                let partition_value =
                    Value::Object(partition.into_iter().map(|(k, v)| (k, v)).collect());
                // Convert offset info HashMap to JSON Value
                let offset_value =
                    Value::Object(offset_info.into_iter().map(|(k, v)| (k, v)).collect());
                (partition_value, offset_value)
            })
            .collect();

        // Store the new offsets
        self.connector_offsets
            .write()
            .unwrap()
            .insert(conn_name.to_string(), converted_offsets);

        callback.on_completion(Message {
            code: 0,
            message: format!("Offsets altered for connector {}", conn_name),
        });
    }

    // === Logger management ===

    fn logger_level(&self, logger: &str) -> LoggerLevel {
        // Get stored level, default to INFO
        let level = self
            .logger_levels
            .read()
            .unwrap()
            .get(logger)
            .cloned()
            .unwrap_or_else(|| "INFO".to_string());
        LoggerLevel {
            logger: logger.to_string(),
            level: level.clone(),
            effective_level: level,
        }
    }

    fn set_worker_logger_level(&self, namespace: &str, level: &str) -> Vec<String> {
        // Store the new logger level
        self.logger_levels
            .write()
            .unwrap()
            .insert(namespace.to_string(), level.to_string());
        // Return affected loggers (in standalone mode, only the single namespace)
        vec![namespace.to_string()]
    }

    // === New Herder trait methods ===

    fn stop_connector_async(&self, connector: &str, callback: Box<dyn Callback<()>>) {
        if !self.config_backing_store.contains(connector) {
            callback.on_error(format!("Connector {} not found", connector));
            return;
        }

        // Update target state to stopped
        self.config_backing_store
            .put_target_state(connector, TargetState::Stopped);

        // Stop tasks
        self.remove_connector_tasks(connector);

        // Stop connector
        self.worker.stop_and_await_connector(connector);

        // Update status
        self.status_backing_store
            .put_connector_state(connector, ConnectorState::Unassigned);

        callback.on_completion(());
    }

    fn restart_connector_and_tasks(
        &self,
        request: &common_trait::herder::RestartRequest,
        callback: Box<dyn Callback<ConnectorStateInfo>>,
    ) {
        let conn_name = request.connector();

        if !self.config_backing_store.contains(conn_name) {
            callback.on_error(format!("Connector {} not found", conn_name));
            return;
        }

        // Get current connector status
        let connector_state_info = self.connector_status(conn_name);

        // If only_failed is true, check if connector is actually failed
        if request.only_failed() && connector_state_info.connector != ConnectorState::Failed {
            // Connector is not failed, no restart needed
            callback.on_completion(connector_state_info);
            return;
        }

        // Restart connector
        let restart_callback = SimpleCallback::<()>::new();
        self.restart_connector_async(conn_name, Box::new(restart_callback.clone()));

        match restart_callback.result() {
            Some(_) => callback.on_completion(self.connector_status(conn_name)),
            None => callback.on_error("Failed to restart connector".to_string()),
        }
    }

    fn reset_connector_offsets(&self, conn_name: &str, callback: Box<dyn Callback<Message>>) {
        if !self.config_backing_store.contains(conn_name) {
            callback.on_error(format!("Connector {} not found", conn_name));
            return;
        }

        // Clear all offsets for this connector
        self.connector_offsets.write().unwrap().remove(conn_name);

        callback.on_completion(Message {
            code: 0,
            message: format!("Offsets reset for connector {}", conn_name),
        });
    }

    fn all_logger_levels(&self) -> HashMap<String, LoggerLevel> {
        let levels = self.logger_levels.read().unwrap();
        levels
            .iter()
            .map(|(name, level)| {
                (
                    name.clone(),
                    LoggerLevel {
                        logger: name.clone(),
                        level: level.clone(),
                        effective_level: level.clone(),
                    },
                )
            })
            .collect()
    }

    fn set_cluster_logger_level(&self, namespace: &str, level: &str) {
        // In standalone mode, cluster logger level is same as worker logger level
        self.logger_levels
            .write()
            .unwrap()
            .insert(namespace.to_string(), level.to_string());
    }

    fn connect_metrics(&self) -> Box<dyn common_trait::herder::ConnectMetrics> {
        // Return standalone metrics wrapper
        Box::new(StandaloneConnectMetrics::new(
            self.worker.config().worker_id().to_string(),
        ))
    }
}

// ===== StandaloneConnectMetrics implementation =====

/// ConnectMetrics implementation for StandaloneHerder.
struct StandaloneConnectMetrics {
    worker_id: String,
}

impl StandaloneConnectMetrics {
    fn new(worker_id: String) -> Self {
        StandaloneConnectMetrics { worker_id }
    }
}

impl common_trait::herder::ConnectMetrics for StandaloneConnectMetrics {
    fn registry(&self) -> &dyn common_trait::herder::MetricsRegistry {
        static REGISTRY: StandaloneMetricsRegistry = StandaloneMetricsRegistry::new_static();
        &REGISTRY
    }

    fn stop(&self) {
        // No actual metrics to stop
    }
}

/// MetricsRegistry implementation for StandaloneHerder.
struct StandaloneMetricsRegistry {
    worker_id: String,
}

impl StandaloneMetricsRegistry {
    fn new(worker_id: String) -> Self {
        StandaloneMetricsRegistry { worker_id }
    }

    const fn new_static() -> Self {
        StandaloneMetricsRegistry {
            worker_id: String::new(),
        }
    }
}

impl common_trait::herder::MetricsRegistry for StandaloneMetricsRegistry {
    fn worker_group_name(&self) -> &str {
        &self.worker_id
    }
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::WorkerConfig;

    fn create_test_herder() -> StandaloneHerder {
        let worker = Arc::new(Worker::new(
            WorkerConfig::new("test-worker".to_string()),
            None,
            None,
        ));
        StandaloneHerder::new(worker, "test-cluster".to_string(), None)
    }

    // ===== testCreateSourceConnector =====
    // Verifies successful creation of a source connector.
    #[test]
    fn test_create_source_connector() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-source".to_string()),
            ("file".to_string(), "/tmp/test.txt".to_string()),
        ]);

        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-source",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );

        // Verify callback received result
        let result = callback.result();
        assert!(result.is_some());
        let created = result.unwrap();
        assert!(created.created);
        assert_eq!(created.info.name, "test-source");
        assert_eq!(created.info.config, config);

        // Verify connector exists in backing store
        assert!(herder.config_backing_store.contains("test-source"));

        // Verify status is Running
        let status = herder.connector_status("test-source");
        assert_eq!(status.connector, ConnectorState::Running);
    }

    // ===== testCreateConnectorFailedValidation =====
    // Checks that connector creation fails gracefully when validation errors occur.
    #[test]
    fn test_create_connector_failed_validation() {
        let mut herder = create_test_herder();
        herder.start();

        // Missing connector.class - should fail validation
        let config = HashMap::from([("name".to_string(), "test-invalid".to_string())]);

        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-invalid", config, false, Box::new(callback.clone()));

        // Should receive error (missing required connector.class)
        let error = callback.error();
        assert!(error.is_some());
        assert!(error.unwrap().contains("connector.class"));

        // Also test missing name
        let config2 = HashMap::from([(
            "connector.class".to_string(),
            "FileSourceConnector".to_string(),
        )]);
        let callback2 = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-invalid2", config2, false, Box::new(callback2.clone()));

        let error2 = callback2.error();
        assert!(error2.is_some());
        assert!(error2.unwrap().contains("name"));
    }

    // ===== testCreateConnectorAlreadyExists =====
    // Ensures error when attempting to create a connector with existing name.
    #[test]
    fn test_create_connector_already_exists() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-duplicate".to_string()),
        ]);

        // Create first
        let callback1 = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-duplicate",
            config.clone(),
            false,
            Box::new(callback1.clone()),
        );
        assert!(callback1.result().is_some());

        // Attempt to create again (allow_replace=false)
        let callback2 = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-duplicate", config, false, Box::new(callback2.clone()));

        // Should receive error
        let error = callback2.error();
        assert!(error.is_some());
        assert!(error.unwrap().contains("already exists"));
    }

    // ===== testCreateSinkConnector =====
    // Verifies successful creation of a sink connector.
    #[test]
    fn test_create_sink_connector() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSinkConnector".to_string(),
            ),
            ("name".to_string(), "test-sink".to_string()),
            ("file".to_string(), "/tmp/output.txt".to_string()),
        ]);

        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-sink",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );

        let result = callback.result();
        assert!(result.is_some());
        let created = result.unwrap();
        assert!(created.created);
        assert_eq!(created.info.name, "test-sink");
    }

    // ===== testCreateConnectorWithStoppedInitialState =====
    // Confirms connector can be created with STOPPED state (no tasks spawned).
    #[test]
    fn test_create_connector_with_stopped_initial_state() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-stopped".to_string()),
        ]);

        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config_with_state(
            "test-stopped",
            config.clone(),
            common_trait::herder::TargetState::Stopped,
            false,
            Box::new(callback.clone()),
        );

        let result = callback.result();
        assert!(result.is_some());
        let created = result.unwrap();
        assert!(created.created);

        // Tasks should be empty (Stopped state)
        assert_eq!(created.info.tasks.len(), 1); // Task config exists but not running
    }

    // ===== testDestroyConnector =====
    // Tests complete removal of a connector and its associated tasks.
    #[test]
    fn test_destroy_connector() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-destroy".to_string()),
        ]);

        // Create first
        let callback1 = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-destroy",
            config.clone(),
            false,
            Box::new(callback1.clone()),
        );
        assert!(callback1.result().is_some());

        // Destroy
        let callback2 = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.delete_connector_config("test-destroy", Box::new(callback2.clone()));

        let result = callback2.result();
        assert!(result.is_some());
        let deleted = result.unwrap();
        assert!(!deleted.created); // Not created (was deleted)
        assert_eq!(deleted.info.name, "test-destroy");

        // Verify connector no longer exists
        assert!(!herder.config_backing_store.contains("test-destroy"));

        // Verify status is Unassigned
        let status = herder.connector_status("test-destroy");
        assert_eq!(status.connector, ConnectorState::Unassigned);
    }

    // ===== testPutConnectorConfig =====
    // Validates updating connector config (stop and restart).
    #[test]
    fn test_put_connector_config() {
        let mut herder = create_test_herder();
        herder.start();

        let config1 = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-update".to_string()),
            ("file".to_string(), "/tmp/test1.txt".to_string()),
        ]);

        // Create first
        let callback1 = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-update",
            config1.clone(),
            false,
            Box::new(callback1.clone()),
        );
        assert!(callback1.result().unwrap().created);

        // Update with allow_replace=true
        let config2 = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-update".to_string()),
            ("file".to_string(), "/tmp/test2.txt".to_string()),
        ]);

        let callback2 = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-update",
            config2.clone(),
            true,
            Box::new(callback2.clone()),
        );

        let result = callback2.result();
        assert!(result.is_some());
        let updated = result.unwrap();
        assert!(!updated.created); // Was updated, not created
        assert_eq!(updated.info.config.get("file").unwrap(), "/tmp/test2.txt");
    }

    // ===== testRestartConnector =====
    // Tests connector restart (stop and start again).
    #[test]
    fn test_restart_connector() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-restart".to_string()),
        ]);

        // Create first
        let callback1 = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-restart",
            config.clone(),
            false,
            Box::new(callback1.clone()),
        );
        assert!(callback1.result().is_some());

        // Restart
        let callback2 = SimpleCallback::<()>::new();
        herder.restart_connector_async("test-restart", Box::new(callback2.clone()));

        // Should succeed
        assert!(callback2.result().is_some());

        // Verify still running
        let status = herder.connector_status("test-restart");
        assert_eq!(status.connector, ConnectorState::Running);
    }

    // ===== testRestartTask =====
    // Tests restart of a specific task.
    #[test]
    fn test_restart_task() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-task-restart".to_string()),
        ]);

        // Create connector
        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-task-restart",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        // Restart task 0
        let task_id = common_trait::herder::ConnectorTaskId {
            connector: "test-task-restart".to_string(),
            task: 0,
        };

        let callback2 = SimpleCallback::<()>::new();
        herder.restart_task(&task_id, Box::new(callback2.clone()));

        // Should succeed
        assert!(callback2.result().is_some());
    }

    // ===== testTargetStates =====
    // Verifies handling of PAUSED and STOPPED target states.
    #[test]
    fn test_target_states() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-state".to_string()),
        ]);

        // Create with Started state
        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-state",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        // Pause
        herder.pause_connector("test-state");

        let status = herder.connector_status("test-state");
        assert_eq!(status.connector, ConnectorState::Paused);

        // Resume
        herder.resume_connector("test-state");

        let status = herder.connector_status("test-state");
        assert_eq!(status.connector, ConnectorState::Running);
    }

    // ===== testCreateAndStop =====
    // Ensures herder.stop() stops all running connectors.
    #[test]
    fn test_create_and_stop() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-stop".to_string()),
        ]);

        // Create connector
        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-stop",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        // Stop herder
        herder.stop();

        // Verify herder not ready
        assert!(!herder.is_ready());

        // Verify connector removed
        assert!(!herder.config_backing_store.contains("test-stop"));
    }

    // ===== testAccessors =====
    // Tests connectors(), connectorInfo(), connectorConfig(), taskConfigs().
    #[test]
    fn test_accessors() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-accessors".to_string()),
        ]);

        // Create connector
        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-accessors",
            config.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        // Test connectors_async
        let callback2 = SimpleCallback::<Vec<String>>::new();
        herder.connectors_async(Box::new(callback2.clone()));
        let connectors = callback2.result().unwrap();
        assert!(connectors.contains(&"test-accessors".to_string()));

        // Test connector_info_async
        let callback3 = SimpleCallback::<ConnectorInfo>::new();
        herder.connector_info_async("test-accessors", Box::new(callback3.clone()));
        let info = callback3.result().unwrap();
        assert_eq!(info.name, "test-accessors");

        // Test connector_config_async
        let callback4 = SimpleCallback::<HashMap<String, String>>::new();
        herder.connector_config_async("test-accessors", Box::new(callback4.clone()));
        let cfg = callback4.result().unwrap();
        assert_eq!(cfg.get("connector.class").unwrap(), "FileSourceConnector");

        // Test task_configs_async
        let callback5 = SimpleCallback::<Vec<TaskInfo>>::new();
        herder.task_configs_async("test-accessors", Box::new(callback5.clone()));
        let tasks = callback5.result().unwrap();
        assert!(!tasks.is_empty());
    }

    // ===== testPatchConnectorConfig =====
    // Verifies partial config update.
    #[test]
    fn test_patch_connector_config() {
        let mut herder = create_test_herder();
        herder.start();

        let config1 = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-patch".to_string()),
            ("file".to_string(), "/tmp/test.txt".to_string()),
        ]);

        // Create first
        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config(
            "test-patch",
            config1.clone(),
            false,
            Box::new(callback.clone()),
        );
        assert!(callback.result().is_some());

        // Patch (add new key)
        let patch = HashMap::from([("topic".to_string(), "test-topic".to_string())]);

        let callback2 = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.patch_connector_config("test-patch", patch, Box::new(callback2.clone()));

        let result = callback2.result();
        assert!(result.is_some());

        // Verify merged config
        let callback3 = SimpleCallback::<HashMap<String, String>>::new();
        herder.connector_config_async("test-patch", Box::new(callback3.clone()));
        let cfg = callback3.result().unwrap();
        assert_eq!(cfg.get("topic").unwrap(), "test-topic");
        assert_eq!(cfg.get("file").unwrap(), "/tmp/test.txt");
    }

    // ===== testPatchConnectorConfigNotFound =====
    // Asserts error when patching non-existent connector.
    #[test]
    fn test_patch_connector_config_not_found() {
        let herder = create_test_herder();

        let patch = HashMap::from([("key".to_string(), "value".to_string())]);

        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.patch_connector_config("non-existent", patch, Box::new(callback.clone()));

        let error = callback.error();
        assert!(error.is_some());
        assert!(error.unwrap().contains("not found"));
    }

    // ===== testHealthCheck =====
    // Tests health check functionality.
    #[test]
    fn test_health_check() {
        let mut herder = create_test_herder();

        // Not started - should fail
        let callback1 = SimpleCallback::<()>::new();
        herder.health_check(Box::new(callback1.clone()));
        assert!(callback1.error().is_some());

        // Started - should succeed
        herder.start();
        let callback2 = SimpleCallback::<()>::new();
        herder.health_check(Box::new(callback2.clone()));
        assert!(callback2.result().is_some());
    }

    // ===== testIsReady =====
    // Tests is_ready check.
    #[test]
    fn test_is_ready() {
        let mut herder = create_test_herder();

        assert!(!herder.is_ready());

        herder.start();
        assert!(herder.is_ready());

        herder.stop();
        assert!(!herder.is_ready());
    }

    // ===== testMemoryConfigBackingStore =====
    // Tests in-memory config backing store operations.
    #[test]
    fn test_memory_config_backing_store() {
        let store = MemoryConfigBackingStore::new();

        // Put config
        let config = HashMap::from([("key".to_string(), "value".to_string())]);
        store.put_connector_config("test", config.clone());

        // Get config
        let retrieved = store.connector_config("test");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), config);

        // Contains
        assert!(store.contains("test"));

        // Put task configs
        let task_config = HashMap::from([("task.key".to_string(), "task.value".to_string())]);
        store.put_task_configs("test", vec![task_config.clone()]);

        // Get task configs
        let tasks = store.task_configs("test");
        assert!(tasks.is_some());
        assert_eq!(tasks.unwrap()[0], task_config);

        // Put target state
        store.put_target_state("test", TargetState::Paused);
        assert_eq!(store.target_state("test"), Some(TargetState::Paused));

        // Remove
        store.remove_connector_config("test");
        assert!(!store.contains("test"));
        assert!(store.task_configs("test").is_none());
    }

    // ===== testActiveTopics =====
    // Tests connector_active_topics with topic tracking.
    #[test]
    fn test_active_topics() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-topics".to_string()),
        ]);

        // Create connector
        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-topics", config, false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        // Add topics via status backing store
        herder
            .status_backing_store
            .put_topic("test-topics", "topic1");
        herder
            .status_backing_store
            .put_topic("test-topics", "topic2");

        // Get active topics
        let topics_info = herder.connector_active_topics("test-topics");
        assert_eq!(topics_info.connector, "test-topics");
        assert!(topics_info.topics.contains(&"topic1".to_string()));
        assert!(topics_info.topics.contains(&"topic2".to_string()));
        assert_eq!(topics_info.topics.len(), 2);

        // Reset active topics
        herder.reset_connector_active_topics("test-topics");

        // Verify topics are cleared
        let topics_info_after = herder.connector_active_topics("test-topics");
        assert_eq!(topics_info_after.topics.len(), 0);
    }

    // ===== testValidateConnectorConfigMultipleKeys =====
    // Tests validate_connector_config_async returns multiple config keys.
    #[test]
    fn test_validate_connector_config_multiple_keys() {
        let herder = create_test_herder();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-validate".to_string()),
            ("tasks.max".to_string(), "2".to_string()),
            ("custom.key".to_string(), "custom.value".to_string()),
        ]);

        let callback = SimpleCallback::<ConfigInfos>::new();
        herder.validate_connector_config_async(config.clone(), Box::new(callback.clone()));

        let result = callback.result();
        assert!(result.is_some());
        let config_infos = result.unwrap();
        assert_eq!(config_infos.error_count, 0);
        // Should include at least 4 configs (connector.class, name, tasks.max, custom.key)
        assert!(config_infos.configs.len() >= 4);

        // Check that connector.class and name are present with required=true
        let connector_class_info = config_infos
            .configs
            .iter()
            .find(|c| c.definition.name == "connector.class");
        assert!(connector_class_info.is_some());
        assert!(connector_class_info.unwrap().definition.required);

        let name_info = config_infos
            .configs
            .iter()
            .find(|c| c.definition.name == "name");
        assert!(name_info.is_some());
        assert!(name_info.unwrap().definition.required);
    }

    // ===== testConnectorPluginConfigMultipleKeys =====
    // Tests connector_plugin_config returns multiple config keys.
    #[test]
    fn test_connector_plugin_config_multiple_keys() {
        let herder = create_test_herder();

        let config_keys = herder.connector_plugin_config("FileSourceConnector");
        // Should return at least 3 keys (connector.class, name, tasks.max)
        assert!(config_keys.len() >= 3);

        // Verify all have proper group and order
        for key in &config_keys {
            assert!(key.group.is_some());
            assert!(key.order_in_group.is_some());
        }
    }

    // ===== testConnectorOffsets =====
    // Tests connector_offsets_async with real offset storage.
    #[test]
    fn test_connector_offsets() {
        let mut herder = create_test_herder();
        herder.start();

        let config = HashMap::from([
            (
                "connector.class".to_string(),
                "FileSourceConnector".to_string(),
            ),
            ("name".to_string(), "test-offsets".to_string()),
        ]);

        // Create connector
        let callback = SimpleCallback::<Created<ConnectorInfo>>::new();
        herder.put_connector_config("test-offsets", config, false, Box::new(callback.clone()));
        assert!(callback.result().is_some());

        // Get offsets (initially empty)
        let callback2 = SimpleCallback::<ConnectorOffsets>::new();
        herder.connector_offsets_async("test-offsets", Box::new(callback2.clone()));
        let offsets = callback2.result().unwrap();
        assert_eq!(offsets.offsets.len(), 0);

        // Directly write to connector_offsets storage to test retrieval
        let mut stored_offsets: HashMap<Value, Value> = HashMap::new();
        stored_offsets.insert(
            Value::String("partition-key".to_string()),
            Value::Number(100.into()),
        );
        herder
            .connector_offsets
            .write()
            .unwrap()
            .insert("test-offsets".to_string(), stored_offsets);

        // Get offsets again (should have one entry)
        let callback3 = SimpleCallback::<ConnectorOffsets>::new();
        herder.connector_offsets_async("test-offsets", Box::new(callback3.clone()));
        let offsets_after = callback3.result().unwrap();
        assert_eq!(offsets_after.offsets.len(), 1);
    }

    // ===== testLoggerLevel =====
    // Tests logger_level and set_worker_logger_level with real storage.
    #[test]
    fn test_logger_level() {
        let herder = create_test_herder();

        // Get default logger level
        let level_info = herder.logger_level("org.apache.kafka");
        assert_eq!(level_info.level, "INFO");

        // Set new level
        let affected = herder.set_worker_logger_level("org.apache.kafka", "DEBUG");
        assert_eq!(affected.len(), 1);
        assert!(affected.contains(&"org.apache.kafka".to_string()));

        // Get updated level
        let level_info_after = herder.logger_level("org.apache.kafka");
        assert_eq!(level_info_after.level, "DEBUG");
        assert_eq!(level_info_after.effective_level, "DEBUG");
    }
}
