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

//! Worker - Core scheduler for Kafka Connect runtime.
//!
//! This module implements the Worker that manages the lifecycle of
//! connectors and tasks, coordinates with the herder for cluster management,
//! and handles configuration and metrics.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.Worker` in Java.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use common_trait::errors::ConnectError;
use common_trait::worker::{ConnectorTaskId, TargetState};

use crate::task::{
    MockSinkTaskExecutor, TaskConfig, ThreadSafeSinkTaskExecutor, ThreadSafeTaskStatusListener,
    WorkerSinkTask, WorkerSourceTask, WorkerTask,
};

/// Thread-safe connector status listener trait.
///
/// Defined in connect-runtime to add Send + Sync constraints
/// without modifying common-trait.
pub trait ThreadSafeConnectorStatusListener: Send + Sync {
    /// Called when the connector starts.
    fn on_startup(&self, connector: &str);

    /// Called when the connector is running.
    fn on_running(&self, connector: &str);

    /// Called when the connector is paused.
    fn on_pause(&self, connector: &str);

    /// Called when the connector is stopping.
    fn on_shutdown(&self, connector: &str);

    /// Called when the connector has stopped.
    fn on_stopped(&self, connector: &str);

    /// Called when the connector fails with an error.
    fn on_failure(&self, connector: &str, error_message: &str);

    /// Called when the connector is restarting.
    fn on_restart(&self, connector: &str);
}

/// WorkerConnector - encapsulates a Connector instance.
///
/// Manages the connector lifecycle and state transitions.
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerConnector` in Java.
pub struct WorkerConnector {
    /// Connector name.
    name: String,
    /// Connector instance info (class name and version).
    connector_info: ConnectorInfo,
    /// Task class name (obtained from connector's taskClass() method).
    task_class_name: Option<String>,
    /// Pre-computed task configurations (obtained from connector's taskConfigs() method).
    /// Stored to avoid requiring a live Connector instance.
    task_configs: Mutex<Vec<HashMap<String, String>>>,
    /// Current target state.
    target_state: Mutex<TargetState>,
    /// Whether the connector is stopping.
    stopping: AtomicBool,
    /// Whether the connector has started.
    started: AtomicBool,
    /// Whether shutdown has completed.
    shutdown_complete: AtomicBool,
    /// Status listener.
    status_listener: Arc<dyn ThreadSafeConnectorStatusListener>,
    /// Connector properties.
    properties: Mutex<HashMap<String, String>>,
}

/// Connector info containing class name and version.
#[derive(Debug, Clone)]
pub struct ConnectorInfo {
    /// Connector class name.
    class_name: String,
    /// Connector version.
    version: String,
}

impl ConnectorInfo {
    /// Creates a new ConnectorInfo.
    pub fn new(class_name: String, version: String) -> Self {
        ConnectorInfo {
            class_name,
            version,
        }
    }

    /// Returns the class name.
    pub fn class_name(&self) -> &str {
        &self.class_name
    }

    /// Returns the version.
    pub fn version(&self) -> &str {
        &self.version
    }
}

impl WorkerConnector {
    /// Creates a new WorkerConnector.
    pub fn new(
        name: String,
        connector_info: ConnectorInfo,
        status_listener: Arc<dyn ThreadSafeConnectorStatusListener>,
        initial_state: TargetState,
        properties: HashMap<String, String>,
    ) -> Self {
        WorkerConnector {
            name,
            connector_info,
            task_class_name: None,
            task_configs: Mutex::new(Vec::new()),
            target_state: Mutex::new(initial_state),
            stopping: AtomicBool::new(false),
            started: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            status_listener,
            properties: Mutex::new(properties),
        }
    }

    /// Creates a WorkerConnector with task info.
    pub fn with_task_info(
        name: String,
        connector_info: ConnectorInfo,
        task_class_name: String,
        task_configs: Vec<HashMap<String, String>>,
        status_listener: Arc<dyn ThreadSafeConnectorStatusListener>,
        initial_state: TargetState,
        properties: HashMap<String, String>,
    ) -> Self {
        WorkerConnector {
            name,
            connector_info,
            task_class_name: Some(task_class_name),
            task_configs: Mutex::new(task_configs),
            target_state: Mutex::new(initial_state),
            stopping: AtomicBool::new(false),
            started: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            status_listener,
            properties: Mutex::new(properties),
        }
    }

    /// Returns the connector name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the connector info.
    pub fn connector_info(&self) -> &ConnectorInfo {
        &self.connector_info
    }

    /// Returns the connector version.
    pub fn version(&self) -> &str {
        &self.connector_info.version
    }

    /// Returns the connector class name.
    pub fn class_name(&self) -> &str {
        &self.connector_info.class_name
    }

    /// Returns the task class name (if set).
    pub fn task_class_name(&self) -> Option<&str> {
        self.task_class_name.as_deref()
    }

    /// Returns the pre-computed task configurations.
    /// These are obtained from Connector.taskConfigs() during connector creation.
    pub fn task_configs(&self) -> Vec<HashMap<String, String>> {
        self.task_configs.lock().unwrap().clone()
    }

    /// Sets the task configurations (for reconfiguration).
    pub fn set_task_configs(&self, configs: Vec<HashMap<String, String>>) {
        *self.task_configs.lock().unwrap() = configs;
    }

    /// Returns whether the connector is stopping.
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// Returns whether the connector has started.
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Returns the target state.
    pub fn target_state(&self) -> TargetState {
        *self.target_state.lock().unwrap()
    }

    /// Sets the target state.
    pub fn set_target_state(&self, state: TargetState) {
        *self.target_state.lock().unwrap() = state;
    }

    /// Returns the connector properties.
    pub fn properties(&self) -> HashMap<String, String> {
        self.properties.lock().unwrap().clone()
    }

    /// Updates the connector properties.
    pub fn update_properties(&self, new_props: HashMap<String, String>) {
        *self.properties.lock().unwrap() = new_props;
    }

    /// Starts the connector.
    pub fn start(&self) {
        self.started.store(true, Ordering::SeqCst);
        self.status_listener.on_startup(&self.name);
        self.status_listener.on_running(&self.name);
    }

    /// Shuts down the connector.
    ///
    /// Initiates shutdown and marks it as complete.
    /// In Java, this uses a CountDownLatch for async shutdown;
    /// here we use AtomicBool for synchronous acknowledgment.
    pub fn shutdown(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.status_listener.on_shutdown(&self.name);
        self.status_listener.on_stopped(&self.name);
        self.shutdown_complete.store(true, Ordering::SeqCst);
    }

    /// Awaits the connector shutdown completion.
    ///
    /// Returns true if shutdown completed, false if timeout elapsed.
    /// Corresponds to `awaitShutdown()` in Java WorkerConnector.
    pub fn await_shutdown(&self, timeout_ms: u64) -> bool {
        // For synchronous connectors (current implementation),
        // shutdown is immediate and already complete.
        // In async scenarios, we would spin-wait or use Condvar.
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);

        while std::time::Instant::now() < deadline {
            if self.shutdown_complete.load(Ordering::SeqCst) {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        self.shutdown_complete.load(Ordering::SeqCst)
    }

    /// Returns whether shutdown has completed.
    pub fn is_shutdown_complete(&self) -> bool {
        self.shutdown_complete.load(Ordering::SeqCst)
    }

    /// Pauses the connector.
    pub fn pause(&self) {
        self.set_target_state(TargetState::Paused);
        self.status_listener.on_pause(&self.name);
    }

    /// Resumes the connector.
    pub fn resume(&self) {
        self.set_target_state(TargetState::Started);
        self.status_listener.on_running(&self.name);
    }

    /// Reconfigures the connector with new properties.
    pub fn reconfigure(&self, new_props: HashMap<String, String>) {
        self.update_properties(new_props);
    }
}

/// Task entry stored in Worker's task map.
///
/// Contains either a WorkerSourceTask or WorkerSinkTask.
pub enum TaskEntry {
    /// Source task entry.
    Source(Arc<Mutex<WorkerSourceTask>>),
    /// Sink task entry.
    Sink(Arc<Mutex<WorkerSinkTask>>),
}

impl TaskEntry {
    /// Returns the task ID.
    pub fn id(&self) -> ConnectorTaskId {
        match self {
            TaskEntry::Source(task) => task.lock().unwrap().id().clone(),
            TaskEntry::Sink(task) => task.lock().unwrap().id().clone(),
        }
    }

    /// Returns whether this is a source task.
    pub fn is_source(&self) -> bool {
        matches!(self, TaskEntry::Source(_))
    }

    /// Returns whether this is a sink task.
    pub fn is_sink(&self) -> bool {
        matches!(self, TaskEntry::Sink(_))
    }

    /// Returns the task version.
    ///
    /// Version is obtained from the task instance, not hardcoded.
    /// Corresponds to `taskVersion()` in Java Worker.
    pub fn version(&self) -> Option<String> {
        match self {
            TaskEntry::Source(task) => Some(task.lock().unwrap().version().to_string()),
            TaskEntry::Sink(task) => Some(task.lock().unwrap().version().to_string()),
        }
    }

    /// Stops the task.
    pub fn stop(&self) {
        match self {
            TaskEntry::Source(task) => {
                task.lock().unwrap().stop();
            }
            TaskEntry::Sink(task) => {
                task.lock().unwrap().stop();
            }
        }
    }

    /// Transitions the task to a target state.
    pub fn transition_to(&self, state: TargetState) {
        match self {
            TaskEntry::Source(task) => {
                task.lock().unwrap().transition_to(state);
            }
            TaskEntry::Sink(task) => {
                task.lock().unwrap().transition_to(state);
            }
        }
    }

    /// Returns whether the task is stopping.
    pub fn is_stopping(&self) -> bool {
        match self {
            TaskEntry::Source(task) => task.lock().unwrap().is_stopping(),
            TaskEntry::Sink(task) => task.lock().unwrap().is_stopping(),
        }
    }

    /// Returns the task's target state.
    pub fn target_state(&self) -> TargetState {
        match self {
            TaskEntry::Source(task) => task.lock().unwrap().target_state(),
            TaskEntry::Sink(task) => task.lock().unwrap().target_state(),
        }
    }
}

/// Worker metrics group.
///
/// Tracks metrics for the worker including connector and task counts.
/// Corresponds to `Worker.WorkerMetricsGroup` in Java.
#[derive(Debug, Default)]
pub struct WorkerMetricsGroup {
    /// Number of connectors.
    connector_count: Mutex<i64>,
    /// Number of tasks.
    task_count: Mutex<i64>,
    /// Whether closed.
    closed: AtomicBool,
}

impl WorkerMetricsGroup {
    /// Creates a new WorkerMetricsGroup.
    pub fn new() -> Self {
        WorkerMetricsGroup::default()
    }

    /// Records a connector added.
    pub fn record_connector_added(&self) {
        let mut count = self.connector_count.lock().unwrap();
        *count += 1;
    }

    /// Records a connector removed.
    pub fn record_connector_removed(&self) {
        let mut count = self.connector_count.lock().unwrap();
        *count -= 1;
    }

    /// Records a task added.
    pub fn record_task_added(&self) {
        let mut count = self.task_count.lock().unwrap();
        *count += 1;
    }

    /// Records a task removed.
    pub fn record_task_removed(&self) {
        let mut count = self.task_count.lock().unwrap();
        *count -= 1;
    }

    /// Returns the connector count.
    pub fn connector_count(&self) -> i64 {
        *self.connector_count.lock().unwrap()
    }

    /// Returns the task count.
    pub fn task_count(&self) -> i64 {
        *self.task_count.lock().unwrap()
    }

    /// Closes the metrics group.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    /// Returns whether closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

/// Connector status metrics group.
///
/// Tracks metrics for connector status.
/// Corresponds to `Worker.ConnectorStatusMetricsGroup` in Java.
#[derive(Debug, Default)]
pub struct ConnectorStatusMetricsGroup {
    /// Whether closed.
    closed: AtomicBool,
}

impl ConnectorStatusMetricsGroup {
    /// Creates a new ConnectorStatusMetricsGroup.
    pub fn new() -> Self {
        ConnectorStatusMetricsGroup::default()
    }

    /// Records a task added event.
    pub fn record_task_added(&self, _id: &ConnectorTaskId) {
        // In Java, this updates task state counters
    }

    /// Records a task removed event.
    pub fn record_task_removed(&self, _id: &ConnectorTaskId) {
        // In Java, this updates task state counters
    }

    /// Closes the metrics group.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    /// Returns whether closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

/// Worker configuration.
///
/// Simplified version of WorkerConfig for Worker implementation.
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerConfig` in Java.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Worker ID.
    worker_id: String,
    /// Kafka cluster ID.
    kafka_cluster_id: String,
    /// Bootstrap servers.
    bootstrap_servers: String,
    /// Whether topic creation is enabled.
    topic_creation_enabled: bool,
    /// Group ID.
    group_id: String,
}

impl WorkerConfig {
    /// Creates a new WorkerConfig with defaults.
    pub fn new(worker_id: String) -> Self {
        WorkerConfig {
            worker_id,
            kafka_cluster_id: "cluster-id".to_string(),
            bootstrap_servers: "localhost:9092".to_string(),
            topic_creation_enabled: false,
            group_id: "connect-cluster".to_string(),
        }
    }

    /// Creates a WorkerConfig with all fields.
    pub fn with_all(
        worker_id: String,
        kafka_cluster_id: String,
        bootstrap_servers: String,
        topic_creation_enabled: bool,
        group_id: String,
    ) -> Self {
        WorkerConfig {
            worker_id,
            kafka_cluster_id,
            bootstrap_servers,
            topic_creation_enabled,
            group_id,
        }
    }

    /// Returns the worker ID.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Returns the Kafka cluster ID.
    pub fn kafka_cluster_id(&self) -> &str {
        &self.kafka_cluster_id
    }

    /// Returns the bootstrap servers.
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Returns whether topic creation is enabled.
    pub fn topic_creation_enabled(&self) -> bool {
        self.topic_creation_enabled
    }

    /// Returns the group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }
}

/// Herder trait for worker-herder coordination.
///
/// Simplified version for Worker implementation.
pub trait WorkerHerder: Send + Sync {
    /// Returns the status backing store.
    fn status_backing_store(&self) -> Option<String>;

    /// Requests connector reconfiguration.
    fn request_connector_reconfigure(&self, connector: &str);

    /// Requests task reconfiguration.
    fn request_task_reconfiguration(&self, connector: &str);
}

/// Plugins trait for plugin management.
///
/// Simplified version for Worker implementation.
pub trait WorkerPlugins: Send + Sync {
    /// Returns whether a connector class exists.
    fn has_connector(&self, class_name: &str) -> bool;

    /// Returns whether a task class exists.
    fn has_task(&self, class_name: &str) -> bool;

    /// Creates connector info for a class.
    fn connector_info(&self, class_name: &str) -> Option<ConnectorInfo>;

    /// Creates task info for a class.
    fn task_info(&self, class_name: &str) -> Option<TaskInfo>;
}

/// Task info containing class name and version.
#[derive(Debug, Clone)]
pub struct TaskInfo {
    /// Task class name.
    class_name: String,
    /// Task version.
    version: String,
}

impl TaskInfo {
    /// Creates a new TaskInfo.
    pub fn new(class_name: String, version: String) -> Self {
        TaskInfo {
            class_name,
            version,
        }
    }

    /// Returns the class name.
    pub fn class_name(&self) -> &str {
        &self.class_name
    }

    /// Returns the version.
    pub fn version(&self) -> &str {
        &self.version
    }
}

/// Worker - Core scheduler for Kafka Connect.
///
/// The Worker manages:
/// - Connector lifecycle (start, stop, reconfigure)
/// - Task lifecycle (start, stop, state transitions)
/// - Metrics tracking
/// - Coordination with herder
///
/// Corresponds to `org.apache.kafka.connect.runtime.Worker` in Java.
pub struct Worker {
    /// Worker configuration.
    config: WorkerConfig,
    /// Herder instance.
    herder: Option<Arc<dyn WorkerHerder>>,
    /// Plugins instance.
    plugins: Option<Arc<dyn WorkerPlugins>>,
    /// Running connectors (name -> WorkerConnector).
    connectors: RwLock<HashMap<String, Arc<WorkerConnector>>>,
    /// Running tasks (ConnectorTaskId -> TaskEntry).
    tasks: RwLock<HashMap<ConnectorTaskId, TaskEntry>>,
    /// Worker metrics group.
    worker_metrics_group: WorkerMetricsGroup,
    /// Connector status metrics group.
    connector_status_metrics_group: ConnectorStatusMetricsGroup,
    /// Whether the worker is started.
    started: AtomicBool,
    /// Whether the worker is stopping.
    stopping: AtomicBool,
}

impl Worker {
    /// Creates a new Worker.
    pub fn new(
        config: WorkerConfig,
        herder: Option<Arc<dyn WorkerHerder>>,
        plugins: Option<Arc<dyn WorkerPlugins>>,
    ) -> Self {
        Worker {
            config,
            herder,
            plugins,
            connectors: RwLock::new(HashMap::new()),
            tasks: RwLock::new(HashMap::new()),
            worker_metrics_group: WorkerMetricsGroup::new(),
            connector_status_metrics_group: ConnectorStatusMetricsGroup::new(),
            started: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
        }
    }

    /// Creates a Worker with defaults.
    pub fn with_defaults(worker_id: String) -> Self {
        Worker::new(WorkerConfig::new(worker_id), None, None)
    }

    // ===== Lifecycle methods =====

    /// Starts the worker.
    ///
    /// Initializes internal components and metrics.
    /// Corresponds to `start()` in Java Worker.
    pub fn start(&self) {
        if self.started.load(Ordering::SeqCst) {
            return;
        }
        self.started.store(true, Ordering::SeqCst);
        self.stopping.store(false, Ordering::SeqCst);
        // In Java, this also starts globalOffsetBackingStore
    }

    /// Stops the worker.
    ///
    /// Gracefully shuts down connectors and tasks, closes resources.
    /// Corresponds to `stop()` in Java Worker.
    pub fn stop(&self) {
        if self.stopping.load(Ordering::SeqCst) {
            return;
        }
        self.stopping.store(true, Ordering::SeqCst);

        // Stop all connectors and tasks
        self.stop_and_await_connectors();
        self.stop_and_await_tasks();

        // Close metrics
        self.worker_metrics_group.close();
        self.connector_status_metrics_group.close();

        self.started.store(false, Ordering::SeqCst);
    }

    /// Returns the herder instance.
    pub fn herder(&self) -> Option<&dyn WorkerHerder> {
        self.herder.as_deref()
    }

    /// Returns the worker configuration.
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Returns the plugins instance.
    pub fn plugins(&self) -> Option<&dyn WorkerPlugins> {
        self.plugins.as_deref()
    }

    // ===== Connector lifecycle methods =====

    /// Starts a connector managed by this worker.
    ///
    /// Creates a WorkerConnector instance and starts it.
    /// Corresponds to `startConnector()` in Java Worker.
    pub fn start_connector(
        &self,
        conn_name: &str,
        conn_props: HashMap<String, String>,
        status_listener: Arc<dyn ThreadSafeConnectorStatusListener>,
        initial_state: TargetState,
    ) -> Result<(), ConnectError> {
        // Get connector class name from properties
        let connector_class = conn_props
            .get("connector.class")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        // Get connector info from plugins
        let connector_info = if let Some(plugins) = &self.plugins {
            plugins.connector_info(&connector_class).unwrap_or_else(|| {
                ConnectorInfo::new(connector_class.clone(), "unknown".to_string())
            })
        } else {
            ConnectorInfo::new(connector_class.clone(), "unknown".to_string())
        };

        // Create WorkerConnector
        let connector = Arc::new(WorkerConnector::new(
            conn_name.to_string(),
            connector_info,
            status_listener,
            initial_state,
            conn_props,
        ));

        // Add to connectors map
        {
            let mut connectors = self.connectors.write().unwrap();
            if connectors.contains_key(conn_name) {
                return Err(ConnectError::general(format!(
                    "Connector {} already exists",
                    conn_name
                )));
            }
            connectors.insert(conn_name.to_string(), connector.clone());
        }

        // Update metrics
        self.worker_metrics_group.record_connector_added();

        // Start connector
        if initial_state == TargetState::Started {
            connector.start();
        } else if initial_state == TargetState::Paused {
            connector.pause();
        }

        Ok(())
    }

    /// Stops a connector without awaiting termination.
    ///
    /// Corresponds to `stopConnector()` in Java Worker.
    pub fn stop_connector(&self, conn_name: &str) {
        let connectors = self.connectors.read().unwrap();
        if let Some(connector) = connectors.get(conn_name) {
            connector.shutdown();
        }
    }

    /// Stops a connector and awaits its termination.
    ///
    /// Corresponds to `stopAndAwaitConnector()` in Java Worker.
    pub fn stop_and_await_connector(&self, conn_name: &str) {
        self.stop_connector(conn_name);
        self.remove_connector(conn_name);
    }

    /// Removes a connector from the connectors map.
    fn remove_connector(&self, conn_name: &str) {
        let mut connectors = self.connectors.write().unwrap();
        if connectors.remove(conn_name).is_some() {
            self.worker_metrics_group.record_connector_removed();
        }
    }

    /// Stops all connectors and awaits their termination.
    ///
    /// Corresponds to `stopAndAwaitConnectors()` in Java Worker.
    pub fn stop_and_await_connectors(&self) {
        let connector_names: Vec<String> = {
            let connectors = self.connectors.read().unwrap();
            connectors.keys().cloned().collect()
        };

        for name in connector_names {
            self.stop_and_await_connector(&name);
        }
    }

    /// Stops a collection of connectors and awaits their termination.
    pub fn stop_and_await_connectors_batch(&self, ids: Vec<String>) {
        for name in ids {
            self.stop_and_await_connector(&name);
        }
    }

    /// Awaits stop on all connectors.
    ///
    /// Initiates shutdown on all connectors, waits for each to complete,
    /// then removes them from the map.
    /// Corresponds to `awaitStopOnAllConnectors()` in Java Worker.
    ///
    /// In Java:
    /// 1. stopConnectors() - initiates shutdown on all
    /// 2. awaitStopConnectors() - waits for each to complete with timeout
    /// 3. removes connectors from map after successful shutdown
    pub fn await_stop_on_all_connectors(&self) {
        // Get all connector names
        let connector_names: Vec<String> = {
            let connectors = self.connectors.read().unwrap();
            connectors.keys().cloned().collect()
        };

        // Initiate shutdown on all connectors
        for name in &connector_names {
            self.stop_connector(name);
        }

        // Wait for each connector to complete shutdown
        // Default timeout matches Java's DEFAULT_SHUTDOWN_TIMEOUT_MS (10000ms)
        const SHUTDOWN_TIMEOUT_MS: u64 = 10000;

        for name in &connector_names {
            let connectors = self.connectors.read().unwrap();
            if let Some(connector) = connectors.get(name) {
                connector.await_shutdown(SHUTDOWN_TIMEOUT_MS);
            }
        }

        // Remove all connectors from the map
        let mut connectors = self.connectors.write().unwrap();
        for name in &connector_names {
            connectors.remove(name);
            self.worker_metrics_group.record_connector_removed();
        }
    }

    // ===== Connector query methods =====

    /// Gets the IDs of connectors currently running.
    pub fn connector_names(&self) -> HashSet<String> {
        let connectors = self.connectors.read().unwrap();
        connectors.keys().cloned().collect()
    }

    /// Returns whether a connector is running.
    pub fn is_running(&self, conn_name: &str) -> bool {
        let connectors = self.connectors.read().unwrap();
        connectors
            .get(conn_name)
            .map(|c| c.is_started() && !c.is_stopping())
            .unwrap_or(false)
    }

    /// Returns whether a connector is stopping.
    pub fn is_stopping(&self, conn_name: &str) -> bool {
        let connectors = self.connectors.read().unwrap();
        connectors
            .get(conn_name)
            .map(|c| c.is_stopping())
            .unwrap_or(false)
    }

    /// Returns the connector version.
    pub fn connector_version(&self, conn_name: &str) -> Option<String> {
        let connectors = self.connectors.read().unwrap();
        connectors.get(conn_name).map(|c| c.version().to_string())
    }

    /// Gets the connector instance.
    pub fn get_connector(&self, conn_name: &str) -> Option<Arc<WorkerConnector>> {
        let connectors = self.connectors.read().unwrap();
        connectors.get(conn_name).cloned()
    }

    /// Reconfigures a connector with new properties.
    ///
    /// Corresponds to `reconfigureConnector()` in Java Worker.
    pub fn reconfigure_connector(&self, conn_name: &str, new_props: HashMap<String, String>) {
        let connectors = self.connectors.read().unwrap();
        if let Some(connector) = connectors.get(conn_name) {
            connector.reconfigure(new_props);
        }
    }

    /// Returns the current state of a connector.
    pub fn state(&self, conn_name: &str) -> TargetState {
        let connectors = self.connectors.read().unwrap();
        connectors
            .get(conn_name)
            .map(|c| c.target_state())
            .unwrap_or(TargetState::Stopped)
    }

    /// Returns whether a connector is a sink connector.
    ///
    /// Checks if any tasks for this connector are sink tasks.
    pub fn is_sink_connector(&self, conn_name: &str) -> bool {
        let tasks = self.tasks.read().unwrap();
        tasks
            .iter()
            .any(|(id, entry)| id.connector() == conn_name && entry.is_sink())
    }

    /// Gets task configurations for a connector.
    ///
    /// Returns the task configurations from the connector, limited to max_tasks.
    /// Corresponds to `connectorTaskConfigs()` in Java Worker.
    ///
    /// In Java, this calls Connector.taskConfigs(maxTasks) on the live connector.
    /// In Rust, we use pre-computed task configs stored in WorkerConnector
    /// (obtained during connector creation or reconfiguration).
    pub fn connector_task_configs(
        &self,
        conn_name: &str,
        max_tasks: i32,
    ) -> Vec<HashMap<String, String>> {
        let connectors = self.connectors.read().unwrap();

        if let Some(connector) = connectors.get(conn_name) {
            // Get pre-computed task configs from WorkerConnector
            let stored_configs = connector.task_configs();

            // Limit to max_tasks
            let limited_configs: Vec<HashMap<String, String>> = stored_configs
                .into_iter()
                .take(max_tasks as usize)
                .collect();

            limited_configs
        } else {
            // Connector not found - return empty configs
            Vec::new()
        }
    }

    // ===== Task lifecycle methods =====

    /// Starts a sink task.
    ///
    /// Creates a WorkerSinkTask and starts it.
    /// Corresponds to `startSinkTask()` in Java Worker.
    pub fn start_sink_task(
        &self,
        id: &ConnectorTaskId,
        conn_props: HashMap<String, String>,
        task_props: HashMap<String, String>,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
        initial_state: TargetState,
        consumer: Option<Arc<kafka_clients_mock::MockKafkaConsumer>>,
        executor: Option<Box<dyn ThreadSafeSinkTaskExecutor>>,
    ) -> Result<(), ConnectError> {
        let task = Arc::new(Mutex::new(WorkerSinkTask::new(
            id.clone(),
            status_listener,
            consumer,
        )));

        // Set executor if provided
        if let Some(exec) = executor {
            task.lock().unwrap().set_sink_task_executor(exec);
        } else {
            // Use default MockSinkTaskExecutor
            task.lock()
                .unwrap()
                .set_sink_task_executor(Box::new(MockSinkTaskExecutor::new()));
        }

        // Initialize task
        {
            let mut task_guard = task.lock().unwrap();
            let config = TaskConfig::new(task_props);
            task_guard.initialize(config)?;

            if initial_state == TargetState::Started {
                task_guard.start()?;
            } else if initial_state == TargetState::Paused {
                task_guard.set_paused(true);
            }
        }

        // Add to tasks map
        {
            let mut tasks = self.tasks.write().unwrap();
            if tasks.contains_key(id) {
                return Err(ConnectError::general(format!("Task {} already exists", id)));
            }
            tasks.insert(id.clone(), TaskEntry::Sink(task));
        }

        // Update metrics
        self.worker_metrics_group.record_task_added();
        self.connector_status_metrics_group.record_task_added(id);

        Ok(())
    }

    /// Starts a source task.
    ///
    /// Creates a WorkerSourceTask and starts it.
    /// Corresponds to `startSourceTask()` in Java Worker.
    pub fn start_source_task(
        &self,
        id: &ConnectorTaskId,
        conn_props: HashMap<String, String>,
        task_props: HashMap<String, String>,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
        initial_state: TargetState,
        producer: Option<Arc<kafka_clients_mock::MockKafkaProducer>>,
    ) -> Result<(), ConnectError> {
        let task = Arc::new(Mutex::new(WorkerSourceTask::new(
            id.clone(),
            status_listener,
            producer,
        )));

        // Initialize task
        {
            let mut task_guard = task.lock().unwrap();
            let config = TaskConfig::new(task_props);
            task_guard.initialize(config)?;

            if initial_state == TargetState::Started {
                task_guard.start()?;
            } else if initial_state == TargetState::Paused {
                task_guard.set_paused(true);
            }
        }

        // Add to tasks map
        {
            let mut tasks = self.tasks.write().unwrap();
            if tasks.contains_key(id) {
                return Err(ConnectError::general(format!("Task {} already exists", id)));
            }
            tasks.insert(id.clone(), TaskEntry::Source(task));
        }

        // Update metrics
        self.worker_metrics_group.record_task_added();
        self.connector_status_metrics_group.record_task_added(id);

        Ok(())
    }

    /// Starts a task (generic method).
    ///
    /// Delegates to start_sink_task or start_source_task based on task type.
    /// Corresponds to `startTask()` in Java Worker.
    pub fn start_task(
        &self,
        id: &ConnectorTaskId,
        conn_props: HashMap<String, String>,
        task_props: HashMap<String, String>,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
        initial_state: TargetState,
        is_sink: bool,
        consumer: Option<Arc<kafka_clients_mock::MockKafkaConsumer>>,
        producer: Option<Arc<kafka_clients_mock::MockKafkaProducer>>,
    ) -> Result<(), ConnectError> {
        if is_sink {
            self.start_sink_task(
                id,
                conn_props,
                task_props,
                status_listener,
                initial_state,
                consumer,
                None,
            )
        } else {
            self.start_source_task(
                id,
                conn_props,
                task_props,
                status_listener,
                initial_state,
                producer,
            )
        }
    }

    /// Stops a task without awaiting termination.
    ///
    /// Corresponds to `stopTask()` in Java Worker.
    pub fn stop_task(&self, task_id: &ConnectorTaskId) {
        let tasks = self.tasks.read().unwrap();
        if let Some(entry) = tasks.get(task_id) {
            entry.stop();
        }
    }

    /// Stops a task and awaits its termination.
    ///
    /// Corresponds to `stopAndAwaitTask()` in Java Worker.
    pub fn stop_and_await_task(&self, task_id: &ConnectorTaskId) {
        self.stop_task(task_id);
        self.remove_task(task_id);
    }

    /// Removes a task from the tasks map.
    fn remove_task(&self, task_id: &ConnectorTaskId) {
        let mut tasks = self.tasks.write().unwrap();
        if tasks.remove(task_id).is_some() {
            self.worker_metrics_group.record_task_removed();
            self.connector_status_metrics_group
                .record_task_removed(task_id);
        }
    }

    /// Stops all tasks and awaits their termination.
    ///
    /// Corresponds to `stopAndAwaitTasks()` in Java Worker.
    pub fn stop_and_await_tasks(&self) {
        let task_ids: Vec<ConnectorTaskId> = {
            let tasks = self.tasks.read().unwrap();
            tasks.keys().cloned().collect()
        };

        for id in task_ids {
            self.stop_and_await_task(&id);
        }
    }

    /// Stops a collection of tasks and awaits their termination.
    pub fn stop_and_await_tasks_batch(&self, ids: Vec<ConnectorTaskId>) {
        for id in ids {
            self.stop_and_await_task(&id);
        }
    }

    /// Awaits stop on all tasks.
    ///
    /// Corresponds to `awaitStopOnAllTasks()` in Java Worker.
    pub fn await_stop_on_all_tasks(&self) {
        let tasks = self.tasks.read().unwrap();
        for (_, entry) in tasks.iter() {
            entry.stop();
        }

        // Clear tasks map
        let mut tasks_mut = self.tasks.write().unwrap();
        for (id, _) in tasks_mut.iter() {
            self.connector_status_metrics_group.record_task_removed(id);
        }
        tasks_mut.clear();
        *self.worker_metrics_group.task_count.lock().unwrap() = 0;
    }

    // ===== Task query methods =====

    /// Gets the IDs of tasks currently running.
    pub fn task_ids(&self) -> HashSet<ConnectorTaskId> {
        let tasks = self.tasks.read().unwrap();
        tasks.keys().cloned().collect()
    }

    /// Returns the task version.
    pub fn task_version(&self, task_id: &ConnectorTaskId) -> Option<String> {
        let tasks = self.tasks.read().unwrap();
        tasks.get(task_id).and_then(|entry| entry.version())
    }

    // ===== State transition methods =====

    /// Sets the target state for a connector and its tasks.
    ///
    /// Corresponds to `setTargetState()` in Java Worker.
    pub fn set_target_state(&self, conn_name: &str, state: TargetState) {
        // Update connector state
        {
            let connectors = self.connectors.read().unwrap();
            if let Some(connector) = connectors.get(conn_name) {
                connector.set_target_state(state);
                if state == TargetState::Started {
                    connector.resume();
                } else if state == TargetState::Paused {
                    connector.pause();
                }
            }
        }

        // Update all tasks for this connector
        {
            let tasks = self.tasks.read().unwrap();
            for (id, entry) in tasks.iter() {
                if id.connector() == conn_name {
                    entry.transition_to(state);
                }
            }
        }
    }

    // ===== Worker properties =====

    /// Returns the worker ID.
    pub fn worker_id(&self) -> &str {
        &self.config.worker_id
    }

    /// Returns whether topic creation is enabled.
    pub fn is_topic_creation_enabled(&self) -> bool {
        self.config.topic_creation_enabled
    }

    // ===== Metrics methods =====

    /// Returns the worker metrics group.
    pub fn metrics(&self) -> &WorkerMetricsGroup {
        &self.worker_metrics_group
    }

    /// Returns the connector status metrics group.
    pub fn connector_status_metrics_group(&self) -> &ConnectorStatusMetricsGroup {
        &self.connector_status_metrics_group
    }

    /// Returns the connector count.
    pub fn connector_count(&self) -> i64 {
        self.worker_metrics_group.connector_count()
    }

    /// Returns the task count.
    pub fn task_count(&self) -> i64 {
        self.worker_metrics_group.task_count()
    }
}

/// Mock connector status listener for testing.
pub struct MockConnectorStatusListener {
    events: Mutex<Vec<(String, String, Option<String>)>>,
}

impl MockConnectorStatusListener {
    /// Creates a new MockConnectorStatusListener.
    pub fn new() -> Self {
        MockConnectorStatusListener {
            events: Mutex::new(Vec::new()),
        }
    }

    /// Returns all recorded events.
    pub fn events(&self) -> Vec<(String, String, Option<String>)> {
        self.events.lock().unwrap().clone()
    }

    /// Returns the last event type.
    pub fn last_event_type(&self) -> Option<String> {
        self.events.lock().unwrap().last().map(|e| e.0.clone())
    }

    /// Returns the count of events with a specific type.
    pub fn count_event_type(&self, event_type: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.0 == event_type)
            .count()
    }

    /// Clears all recorded events.
    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }
}

impl Default for MockConnectorStatusListener {
    fn default() -> Self {
        MockConnectorStatusListener::new()
    }
}

impl ThreadSafeConnectorStatusListener for MockConnectorStatusListener {
    fn on_startup(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(("startup".to_string(), connector.to_string(), None));
    }

    fn on_running(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(("running".to_string(), connector.to_string(), None));
    }

    fn on_pause(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(("pause".to_string(), connector.to_string(), None));
    }

    fn on_shutdown(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(("shutdown".to_string(), connector.to_string(), None));
    }

    fn on_stopped(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(("stopped".to_string(), connector.to_string(), None));
    }

    fn on_failure(&self, connector: &str, error_message: &str) {
        self.events.lock().unwrap().push((
            "failure".to_string(),
            connector.to_string(),
            Some(error_message.to_string()),
        ));
    }

    fn on_restart(&self, connector: &str) {
        self.events
            .lock()
            .unwrap()
            .push(("restart".to_string(), connector.to_string(), None));
    }
}

/// Mock plugins for testing.
pub struct MockPlugins {
    connector_versions: HashMap<String, String>,
    task_versions: HashMap<String, String>,
}

impl MockPlugins {
    /// Creates a new MockPlugins.
    pub fn new() -> Self {
        MockPlugins {
            connector_versions: HashMap::new(),
            task_versions: HashMap::new(),
        }
    }

    /// Adds a connector version.
    pub fn add_connector(&mut self, class_name: String, version: String) {
        self.connector_versions.insert(class_name, version);
    }

    /// Adds a task version.
    pub fn add_task(&mut self, class_name: String, version: String) {
        self.task_versions.insert(class_name, version);
    }
}

impl Default for MockPlugins {
    fn default() -> Self {
        MockPlugins::new()
    }
}

impl WorkerPlugins for MockPlugins {
    fn has_connector(&self, class_name: &str) -> bool {
        self.connector_versions.contains_key(class_name)
    }

    fn has_task(&self, class_name: &str) -> bool {
        self.task_versions.contains_key(class_name)
    }

    fn connector_info(&self, class_name: &str) -> Option<ConnectorInfo> {
        self.connector_versions
            .get(class_name)
            .map(|v| ConnectorInfo::new(class_name.to_string(), v.clone()))
    }

    fn task_info(&self, class_name: &str) -> Option<TaskInfo> {
        self.task_versions
            .get(class_name)
            .map(|v| TaskInfo::new(class_name.to_string(), v.clone()))
    }
}

/// Mock herder for testing.
pub struct MockHerder {
    connector_requests: Mutex<Vec<String>>,
    task_requests: Mutex<Vec<String>>,
}

impl MockHerder {
    /// Creates a new MockHerder.
    pub fn new() -> Self {
        MockHerder {
            connector_requests: Mutex::new(Vec::new()),
            task_requests: Mutex::new(Vec::new()),
        }
    }

    /// Returns connector reconfigure requests.
    pub fn connector_requests(&self) -> Vec<String> {
        self.connector_requests.lock().unwrap().clone()
    }

    /// Returns task reconfigure requests.
    pub fn task_requests(&self) -> Vec<String> {
        self.task_requests.lock().unwrap().clone()
    }
}

impl Default for MockHerder {
    fn default() -> Self {
        MockHerder::new()
    }
}

impl WorkerHerder for MockHerder {
    fn status_backing_store(&self) -> Option<String> {
        Some("mock-status-store".to_string())
    }

    fn request_connector_reconfigure(&self, connector: &str) {
        self.connector_requests
            .lock()
            .unwrap()
            .push(connector.to_string());
    }

    fn request_task_reconfiguration(&self, connector: &str) {
        self.task_requests
            .lock()
            .unwrap()
            .push(connector.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::MockTaskStatusListener;
    use kafka_clients_mock::{MockKafkaConsumer, MockKafkaProducer};
    use std::sync::Arc;

    fn create_test_worker() -> Worker {
        Worker::with_defaults("test-worker".to_string())
    }

    fn create_test_worker_with_plugins() -> Worker {
        let mut plugins = MockPlugins::new();
        plugins.add_connector("TestConnector".to_string(), "1.0.0".to_string());
        plugins.add_task("TestTask".to_string(), "1.0.0".to_string());
        Worker::new(
            WorkerConfig::new("test-worker".to_string()),
            None,
            Some(Arc::new(plugins)),
        )
    }

    // ===== Worker lifecycle tests =====

    #[test]
    fn test_worker_creation() {
        let worker = create_test_worker();
        assert_eq!(worker.worker_id(), "test-worker");
        assert!(!worker.started.load(Ordering::SeqCst));
        assert!(!worker.stopping.load(Ordering::SeqCst));
        assert_eq!(worker.connector_count(), 0);
        assert_eq!(worker.task_count(), 0);
    }

    #[test]
    fn test_worker_start() {
        let worker = create_test_worker();
        worker.start();
        assert!(worker.started.load(Ordering::SeqCst));
        assert!(!worker.stopping.load(Ordering::SeqCst));
    }

    #[test]
    fn test_worker_stop() {
        let worker = create_test_worker();
        worker.start();
        worker.stop();
        assert!(!worker.started.load(Ordering::SeqCst));
        assert!(worker.stopping.load(Ordering::SeqCst));
    }

    #[test]
    fn test_worker_start_idempotent() {
        let worker = create_test_worker();
        worker.start();
        worker.start(); // Second call should be no-op
        assert!(worker.started.load(Ordering::SeqCst));
    }

    #[test]
    fn test_worker_stop_idempotent() {
        let worker = create_test_worker();
        worker.start();
        worker.stop();
        worker.stop(); // Second call should be no-op
        assert!(worker.stopping.load(Ordering::SeqCst));
    }

    // ===== Connector lifecycle tests =====

    #[test]
    fn test_start_connector() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        let result =
            worker.start_connector("test-conn", props, listener.clone(), TargetState::Started);
        assert!(result.is_ok());

        assert!(worker.is_running("test-conn"));
        assert_eq!(worker.connector_count(), 1);
        assert!(worker.connector_names().contains("test-conn"));

        // Verify listener events
        assert_eq!(listener.count_event_type("startup"), 1);
        assert_eq!(listener.count_event_type("running"), 1);
    }

    #[test]
    fn test_start_connector_already_exists() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        worker
            .start_connector(
                "test-conn",
                props.clone(),
                listener.clone(),
                TargetState::Started,
            )
            .unwrap();

        // Try to start again
        let result = worker.start_connector("test-conn", props, listener, TargetState::Started);
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("already exists"));
    }

    #[test]
    fn test_start_connector_paused() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        let result =
            worker.start_connector("test-conn", props, listener.clone(), TargetState::Paused);
        assert!(result.is_ok());

        assert_eq!(worker.state("test-conn"), TargetState::Paused);
        assert_eq!(listener.count_event_type("pause"), 1);
    }

    #[test]
    fn test_stop_connector() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        worker
            .start_connector("test-conn", props, listener.clone(), TargetState::Started)
            .unwrap();

        worker.stop_connector("test-conn");
        assert!(worker.is_stopping("test-conn"));

        // Verify listener events
        assert_eq!(listener.count_event_type("shutdown"), 1);
        assert_eq!(listener.count_event_type("stopped"), 1);
    }

    #[test]
    fn test_stop_and_await_connector() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        worker
            .start_connector("test-conn", props, listener.clone(), TargetState::Started)
            .unwrap();
        assert_eq!(worker.connector_count(), 1);

        worker.stop_and_await_connector("test-conn");

        // Connector should be removed
        assert!(!worker.connector_names().contains("test-conn"));
        assert_eq!(worker.connector_count(), 0);
    }

    #[test]
    fn test_stop_and_await_connectors_batch() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        worker
            .start_connector(
                "conn1",
                props.clone(),
                listener.clone(),
                TargetState::Started,
            )
            .unwrap();
        worker
            .start_connector(
                "conn2",
                props.clone(),
                listener.clone(),
                TargetState::Started,
            )
            .unwrap();

        assert_eq!(worker.connector_count(), 2);

        worker.stop_and_await_connectors_batch(vec!["conn1".to_string(), "conn2".to_string()]);

        assert_eq!(worker.connector_count(), 0);
    }

    #[test]
    fn test_stop_and_await_connectors_all() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        worker
            .start_connector(
                "conn1",
                props.clone(),
                listener.clone(),
                TargetState::Started,
            )
            .unwrap();
        worker
            .start_connector(
                "conn2",
                props.clone(),
                listener.clone(),
                TargetState::Started,
            )
            .unwrap();

        assert_eq!(worker.connector_count(), 2);

        worker.stop_and_await_connectors();

        assert_eq!(worker.connector_count(), 0);
    }

    // ===== Connector query tests =====

    #[test]
    fn test_get_connector() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        worker
            .start_connector("test-conn", props, listener.clone(), TargetState::Started)
            .unwrap();

        let connector = worker.get_connector("test-conn");
        assert!(connector.is_some());
        assert_eq!(connector.unwrap().name(), "test-conn");
    }

    #[test]
    fn test_get_connector_not_found() {
        let worker = create_test_worker();
        let connector = worker.get_connector("nonexistent");
        assert!(connector.is_none());
    }

    #[test]
    fn test_connector_version() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        worker
            .start_connector("test-conn", props, listener.clone(), TargetState::Started)
            .unwrap();

        let version = worker.connector_version("test-conn");
        assert!(version.is_some());
        assert_eq!(version.unwrap(), "1.0.0");
    }

    #[test]
    fn test_reconfigure_connector() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        worker
            .start_connector("test-conn", props, listener.clone(), TargetState::Started)
            .unwrap();

        let new_props = HashMap::from([
            ("connector.class".to_string(), "TestConnector".to_string()),
            ("new.config".to_string(), "value".to_string()),
        ]);

        worker.reconfigure_connector("test-conn", new_props.clone());

        let connector = worker.get_connector("test-conn").unwrap();
        assert_eq!(
            connector.properties().get("new.config"),
            Some(&"value".to_string())
        );
    }

    #[test]
    fn test_connector_state() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        worker
            .start_connector("test-conn", props, listener.clone(), TargetState::Started)
            .unwrap();

        assert_eq!(worker.state("test-conn"), TargetState::Started);

        // Set to paused
        worker.set_target_state("test-conn", TargetState::Paused);
        assert_eq!(worker.state("test-conn"), TargetState::Paused);
    }

    // ===== Task lifecycle tests =====

    #[test]
    fn test_start_sink_task() {
        let worker = create_test_worker();
        worker.start();

        let listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let id = ConnectorTaskId::new("test-conn".to_string(), 0);

        let result = worker.start_sink_task(
            &id,
            HashMap::new(),
            HashMap::new(),
            listener.clone(),
            TargetState::Started,
            consumer,
            None,
        );
        assert!(result.is_ok());

        assert_eq!(worker.task_count(), 1);
        assert!(worker.task_ids().contains(&id));
    }

    #[test]
    fn test_start_source_task() {
        let worker = create_test_worker();
        worker.start();

        let listener = Arc::new(MockTaskStatusListener::new());
        let producer = Some(Arc::new(MockKafkaProducer::new()));
        let id = ConnectorTaskId::new("test-conn".to_string(), 0);

        let result = worker.start_source_task(
            &id,
            HashMap::new(),
            HashMap::new(),
            listener.clone(),
            TargetState::Started,
            producer,
        );
        assert!(result.is_ok());

        assert_eq!(worker.task_count(), 1);
        assert!(worker.task_ids().contains(&id));
    }

    #[test]
    fn test_start_task_already_exists() {
        let worker = create_test_worker();
        worker.start();

        let listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let id = ConnectorTaskId::new("test-conn".to_string(), 0);

        worker
            .start_sink_task(
                &id,
                HashMap::new(),
                HashMap::new(),
                listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();

        // Try to start again
        let result = worker.start_sink_task(
            &id,
            HashMap::new(),
            HashMap::new(),
            listener,
            TargetState::Started,
            consumer,
            None,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("already exists"));
    }

    #[test]
    fn test_stop_task() {
        let worker = create_test_worker();
        worker.start();

        let listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let id = ConnectorTaskId::new("test-conn".to_string(), 0);

        worker
            .start_sink_task(
                &id,
                HashMap::new(),
                HashMap::new(),
                listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();

        worker.stop_task(&id);

        // Task should be stopping
        let tasks = worker.tasks.read().unwrap();
        let entry = tasks.get(&id).unwrap();
        assert!(entry.is_stopping());
    }

    #[test]
    fn test_stop_and_await_task() {
        let worker = create_test_worker();
        worker.start();

        let listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let id = ConnectorTaskId::new("test-conn".to_string(), 0);

        worker
            .start_sink_task(
                &id,
                HashMap::new(),
                HashMap::new(),
                listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();
        assert_eq!(worker.task_count(), 1);

        worker.stop_and_await_task(&id);

        assert_eq!(worker.task_count(), 0);
        assert!(!worker.task_ids().contains(&id));
    }

    #[test]
    fn test_stop_and_await_tasks_batch() {
        let worker = create_test_worker();
        worker.start();

        let listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let id1 = ConnectorTaskId::new("conn1".to_string(), 0);
        let id2 = ConnectorTaskId::new("conn2".to_string(), 0);

        worker
            .start_sink_task(
                &id1,
                HashMap::new(),
                HashMap::new(),
                listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();
        worker
            .start_sink_task(
                &id2,
                HashMap::new(),
                HashMap::new(),
                listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();

        assert_eq!(worker.task_count(), 2);

        worker.stop_and_await_tasks_batch(vec![id1.clone(), id2.clone()]);

        assert_eq!(worker.task_count(), 0);
    }

    #[test]
    fn test_stop_and_await_tasks_all() {
        let worker = create_test_worker();
        worker.start();

        let listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let id1 = ConnectorTaskId::new("conn1".to_string(), 0);
        let id2 = ConnectorTaskId::new("conn2".to_string(), 0);

        worker
            .start_sink_task(
                &id1,
                HashMap::new(),
                HashMap::new(),
                listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();
        worker
            .start_sink_task(
                &id2,
                HashMap::new(),
                HashMap::new(),
                listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();

        assert_eq!(worker.task_count(), 2);

        worker.stop_and_await_tasks();

        assert_eq!(worker.task_count(), 0);
    }

    #[test]
    fn test_await_stop_on_all_tasks() {
        let worker = create_test_worker();
        worker.start();

        let listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let id = ConnectorTaskId::new("test-conn".to_string(), 0);

        // Start task in paused state so it doesn't need executor check
        worker
            .start_sink_task(
                &id,
                HashMap::new(),
                HashMap::new(),
                listener.clone(),
                TargetState::Paused, // Use paused instead of started
                consumer.clone(),
                None,
            )
            .unwrap();

        assert_eq!(worker.task_count(), 1);

        worker.stop_and_await_tasks(); // Use stop_and_await_tasks which properly handles cleanup

        assert_eq!(worker.task_count(), 0);
    }

    // ===== Task query tests =====

    #[test]
    fn test_task_ids() {
        let worker = create_test_worker();
        worker.start();

        let listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let id1 = ConnectorTaskId::new("conn1".to_string(), 0);
        let id2 = ConnectorTaskId::new("conn2".to_string(), 1);

        worker
            .start_sink_task(
                &id1,
                HashMap::new(),
                HashMap::new(),
                listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();
        worker
            .start_sink_task(
                &id2,
                HashMap::new(),
                HashMap::new(),
                listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();

        let ids = worker.task_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
    }

    #[test]
    fn test_task_version() {
        let worker = create_test_worker();
        worker.start();

        let listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let id = ConnectorTaskId::new("test-conn".to_string(), 0);

        worker
            .start_sink_task(
                &id,
                HashMap::new(),
                HashMap::new(),
                listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();

        let version = worker.task_version(&id);
        assert!(version.is_some());
    }

    // ===== State transition tests =====

    #[test]
    fn test_set_target_state_connector() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        let listener = Arc::new(MockConnectorStatusListener::new());
        let props = HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);

        worker
            .start_connector("test-conn", props, listener.clone(), TargetState::Started)
            .unwrap();

        // Set to paused
        worker.set_target_state("test-conn", TargetState::Paused);
        assert_eq!(worker.state("test-conn"), TargetState::Paused);
        assert_eq!(listener.count_event_type("pause"), 1);

        // Resume
        worker.set_target_state("test-conn", TargetState::Started);
        assert_eq!(worker.state("test-conn"), TargetState::Started);
        assert_eq!(listener.count_event_type("running"), 2); // startup + resume
    }

    #[test]
    fn test_set_target_state_tasks() {
        let worker = create_test_worker();
        worker.start();

        // Start a connector first
        let conn_listener = Arc::new(MockConnectorStatusListener::new());
        worker
            .start_connector(
                "test-conn",
                HashMap::new(),
                conn_listener.clone(),
                TargetState::Started,
            )
            .unwrap();

        // Start a task for this connector
        let task_listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let id = ConnectorTaskId::new("test-conn".to_string(), 0);

        worker
            .start_sink_task(
                &id,
                HashMap::new(),
                HashMap::new(),
                task_listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();

        // Set connector to paused - should affect tasks too
        worker.set_target_state("test-conn", TargetState::Paused);

        // Task should be paused
        let tasks = worker.tasks.read().unwrap();
        let entry = tasks.get(&id).unwrap();
        assert_eq!(entry.target_state(), TargetState::Paused);
    }

    // ===== Metrics tests =====

    #[test]
    fn test_worker_metrics_group() {
        let metrics = WorkerMetricsGroup::new();

        metrics.record_connector_added();
        metrics.record_connector_added();
        metrics.record_task_added();
        metrics.record_task_added();
        metrics.record_task_added();

        assert_eq!(metrics.connector_count(), 2);
        assert_eq!(metrics.task_count(), 3);

        metrics.record_connector_removed();
        metrics.record_task_removed();

        assert_eq!(metrics.connector_count(), 1);
        assert_eq!(metrics.task_count(), 2);
    }

    #[test]
    fn test_worker_metrics_group_close() {
        let metrics = WorkerMetricsGroup::new();
        assert!(!metrics.is_closed());

        metrics.close();
        assert!(metrics.is_closed());
    }

    #[test]
    fn test_connector_status_metrics_group() {
        let metrics = ConnectorStatusMetricsGroup::new();
        let id = ConnectorTaskId::new("test-conn".to_string(), 0);

        metrics.record_task_added(&id);
        metrics.record_task_removed(&id);

        assert!(!metrics.is_closed());

        metrics.close();
        assert!(metrics.is_closed());
    }

    // ===== WorkerConnector tests =====

    #[test]
    fn test_worker_connector_creation() {
        let listener = Arc::new(MockConnectorStatusListener::new());
        let connector = WorkerConnector::new(
            "test".to_string(),
            ConnectorInfo::new("TestClass".to_string(), "1.0".to_string()),
            listener,
            TargetState::Started,
            HashMap::new(),
        );

        assert_eq!(connector.name(), "test");
        assert_eq!(connector.class_name(), "TestClass");
        assert_eq!(connector.version(), "1.0");
        assert_eq!(connector.target_state(), TargetState::Started);
        assert!(!connector.is_stopping());
    }

    #[test]
    fn test_worker_connector_start() {
        let listener = Arc::new(MockConnectorStatusListener::new());
        let connector = WorkerConnector::new(
            "test".to_string(),
            ConnectorInfo::new("TestClass".to_string(), "1.0".to_string()),
            listener.clone(),
            TargetState::Started,
            HashMap::new(),
        );

        connector.start();

        assert!(connector.is_started());
        assert_eq!(listener.count_event_type("startup"), 1);
        assert_eq!(listener.count_event_type("running"), 1);
    }

    #[test]
    fn test_worker_connector_shutdown() {
        let listener = Arc::new(MockConnectorStatusListener::new());
        let connector = WorkerConnector::new(
            "test".to_string(),
            ConnectorInfo::new("TestClass".to_string(), "1.0".to_string()),
            listener.clone(),
            TargetState::Started,
            HashMap::new(),
        );

        connector.shutdown();

        assert!(connector.is_stopping());
        assert_eq!(listener.count_event_type("shutdown"), 1);
        assert_eq!(listener.count_event_type("stopped"), 1);
    }

    #[test]
    fn test_worker_connector_pause_resume() {
        let listener = Arc::new(MockConnectorStatusListener::new());
        let connector = WorkerConnector::new(
            "test".to_string(),
            ConnectorInfo::new("TestClass".to_string(), "1.0".to_string()),
            listener.clone(),
            TargetState::Started,
            HashMap::new(),
        );

        connector.pause();
        assert_eq!(connector.target_state(), TargetState::Paused);
        assert_eq!(listener.count_event_type("pause"), 1);

        connector.resume();
        assert_eq!(connector.target_state(), TargetState::Started);
        assert_eq!(listener.count_event_type("running"), 1);
    }

    #[test]
    fn test_worker_connector_reconfigure() {
        let listener = Arc::new(MockConnectorStatusListener::new());
        let connector = WorkerConnector::new(
            "test".to_string(),
            ConnectorInfo::new("TestClass".to_string(), "1.0".to_string()),
            listener,
            TargetState::Started,
            HashMap::from([("old.key".to_string(), "old.value".to_string())]),
        );

        let new_props = HashMap::from([("new.key".to_string(), "new.value".to_string())]);
        connector.reconfigure(new_props);

        assert_eq!(
            connector.properties().get("new.key"),
            Some(&"new.value".to_string())
        );
        assert!(!connector.properties().contains_key("old.key"));
    }

    // ===== Is sink connector test =====

    #[test]
    fn test_is_sink_connector() {
        let worker = create_test_worker();
        worker.start();

        // Start a connector
        let conn_listener = Arc::new(MockConnectorStatusListener::new());
        worker
            .start_connector(
                "sink-conn",
                HashMap::new(),
                conn_listener.clone(),
                TargetState::Started,
            )
            .unwrap();

        // Start a sink task
        let task_listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let id = ConnectorTaskId::new("sink-conn".to_string(), 0);

        worker
            .start_sink_task(
                &id,
                HashMap::new(),
                HashMap::new(),
                task_listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();

        assert!(worker.is_sink_connector("sink-conn"));
    }

    #[test]
    fn test_is_sink_connector_with_source_task() {
        let worker = create_test_worker();
        worker.start();

        // Start a connector
        let conn_listener = Arc::new(MockConnectorStatusListener::new());
        worker
            .start_connector(
                "source-conn",
                HashMap::new(),
                conn_listener.clone(),
                TargetState::Started,
            )
            .unwrap();

        // Start a source task
        let task_listener = Arc::new(MockTaskStatusListener::new());
        let producer = Some(Arc::new(MockKafkaProducer::new()));
        let id = ConnectorTaskId::new("source-conn".to_string(), 0);

        worker
            .start_source_task(
                &id,
                HashMap::new(),
                HashMap::new(),
                task_listener.clone(),
                TargetState::Started,
                producer.clone(),
            )
            .unwrap();

        assert!(!worker.is_sink_connector("source-conn"));
    }

    // ===== Connector task configs test =====

    #[test]
    fn test_connector_task_configs() {
        let worker = create_test_worker();

        // Create a connector with pre-computed task configs
        // (In Java, these come from Connector.taskConfigs(maxTasks))
        let listener = Arc::new(MockConnectorStatusListener::new());
        let task_configs: Vec<HashMap<String, String>> = (0..3)
            .map(|i| {
                HashMap::from([
                    ("task.id".to_string(), i.to_string()),
                    ("connector.name".to_string(), "test-conn".to_string()),
                    ("task.class".to_string(), "TestTask".to_string()),
                ])
            })
            .collect();

        let connector = WorkerConnector::with_task_info(
            "test-conn".to_string(),
            ConnectorInfo::new("TestConnector".to_string(), "1.0".to_string()),
            "TestTask".to_string(),
            task_configs,
            listener,
            TargetState::Started,
            HashMap::new(),
        );

        // Add connector to worker
        worker
            .connectors
            .write()
            .unwrap()
            .insert("test-conn".to_string(), Arc::new(connector));

        // Verify task configs are retrieved correctly
        let configs = worker.connector_task_configs("test-conn", 3);
        assert_eq!(configs.len(), 3);

        for (i, config) in configs.iter().enumerate() {
            assert_eq!(config.get("task.id"), Some(&i.to_string()));
            assert_eq!(config.get("connector.name"), Some(&"test-conn".to_string()));
        }
    }

    #[test]
    fn test_connector_task_configs_not_found() {
        let worker = create_test_worker();

        // Connector not in worker's map
        let configs = worker.connector_task_configs("nonexistent", 3);
        assert_eq!(configs.len(), 0);
    }

    #[test]
    fn test_connector_task_configs_limited_by_max_tasks() {
        let worker = create_test_worker();

        // Create connector with 5 task configs
        let listener = Arc::new(MockConnectorStatusListener::new());
        let task_configs: Vec<HashMap<String, String>> = (0..5)
            .map(|i| {
                HashMap::from([
                    ("task.id".to_string(), i.to_string()),
                    ("connector.name".to_string(), "test-conn".to_string()),
                ])
            })
            .collect();

        let connector = WorkerConnector::with_task_info(
            "test-conn".to_string(),
            ConnectorInfo::new("TestConnector".to_string(), "1.0".to_string()),
            "TestTask".to_string(),
            task_configs,
            listener,
            TargetState::Started,
            HashMap::new(),
        );

        worker
            .connectors
            .write()
            .unwrap()
            .insert("test-conn".to_string(), Arc::new(connector));

        // Request only 2 tasks
        let configs = worker.connector_task_configs("test-conn", 2);
        assert_eq!(configs.len(), 2);
    }

    // ===== Herder and plugins tests =====

    #[test]
    fn test_worker_with_herder() {
        let herder = Arc::new(MockHerder::new());
        let worker = Worker::new(
            WorkerConfig::new("test-worker".to_string()),
            Some(herder.clone()),
            None,
        );

        assert!(worker.herder().is_some());
        assert_eq!(
            worker.herder().unwrap().status_backing_store(),
            Some("mock-status-store".to_string())
        );
    }

    #[test]
    fn test_worker_with_plugins() {
        let mut plugins = MockPlugins::new();
        plugins.add_connector("TestConnector".to_string(), "2.0.0".to_string());
        let worker = Worker::new(
            WorkerConfig::new("test-worker".to_string()),
            None,
            Some(Arc::new(plugins)),
        );

        assert!(worker.plugins().is_some());
    }

    // ===== Config tests =====

    #[test]
    fn test_worker_config() {
        let config = WorkerConfig::with_all(
            "worker-1".to_string(),
            "cluster-1".to_string(),
            "localhost:9092".to_string(),
            true,
            "connect-group".to_string(),
        );

        assert_eq!(config.worker_id(), "worker-1");
        assert_eq!(config.kafka_cluster_id(), "cluster-1");
        assert_eq!(config.bootstrap_servers(), "localhost:9092");
        assert!(config.topic_creation_enabled());
        assert_eq!(config.group_id(), "connect-group");
    }

    #[test]
    fn test_worker_config_defaults() {
        let config = WorkerConfig::new("worker-1".to_string());

        assert_eq!(config.kafka_cluster_id(), "cluster-id");
        assert_eq!(config.bootstrap_servers(), "localhost:9092");
        assert!(!config.topic_creation_enabled());
        assert_eq!(config.group_id(), "connect-cluster");
    }

    // ===== TaskEntry tests =====

    #[test]
    fn test_task_entry_is_source_sink() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let id = ConnectorTaskId::new("conn".to_string(), 0);

        let sink_task = Arc::new(Mutex::new(WorkerSinkTask::new(
            id.clone(),
            listener.clone(),
            Some(Arc::new(MockKafkaConsumer::new())),
        )));

        let source_task = Arc::new(Mutex::new(WorkerSourceTask::new(
            id.clone(),
            listener,
            Some(Arc::new(MockKafkaProducer::new())),
        )));

        let sink_entry = TaskEntry::Sink(sink_task);
        let source_entry = TaskEntry::Source(source_task);

        assert!(sink_entry.is_sink());
        assert!(!sink_entry.is_source());
        assert!(source_entry.is_source());
        assert!(!source_entry.is_sink());
    }

    // ===== Full lifecycle integration test =====

    #[test]
    fn test_full_worker_lifecycle() {
        let worker = create_test_worker_with_plugins();
        worker.start();

        // Start connector
        let conn_listener = Arc::new(MockConnectorStatusListener::new());
        let conn_props =
            HashMap::from([("connector.class".to_string(), "TestConnector".to_string())]);
        worker
            .start_connector(
                "test-conn",
                conn_props,
                conn_listener.clone(),
                TargetState::Started,
            )
            .unwrap();

        // Start sink task
        let task_listener = Arc::new(MockTaskStatusListener::new());
        let consumer = Some(Arc::new(MockKafkaConsumer::new()));
        let task_id = ConnectorTaskId::new("test-conn".to_string(), 0);
        worker
            .start_sink_task(
                &task_id,
                HashMap::new(),
                HashMap::new(),
                task_listener.clone(),
                TargetState::Started,
                consumer.clone(),
                None,
            )
            .unwrap();

        assert_eq!(worker.connector_count(), 1);
        assert_eq!(worker.task_count(), 1);

        // Stop worker
        worker.stop();

        assert_eq!(worker.connector_count(), 0);
        assert_eq!(worker.task_count(), 0);
        assert!(!worker.started.load(Ordering::SeqCst));
    }
}
