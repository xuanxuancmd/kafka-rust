//! Worker module
//!
//! Provides the Worker trait for managing connector lifecycle.

use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use connect_api::{
    connector_impl::Converter, Connector, SinkConnector, SinkTask, SourceConnector, SourceTask,
};

use crate::{ConnectRuntimeError, ConnectRuntimeResult, WorkerConnectorConfig, WorkerWorkerConfig};

use crate::herder::TaskState;

/// Worker trait for managing connector lifecycle
pub trait Worker {
    /// Start -> worker
    fn start(&mut self) -> Result<(), Box<dyn Error>>;

    /// Stop -> worker
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    /// Start a connector
    fn start_connector(
        &mut self,
        conn_name: String,
        conn_props: HashMap<String, String>,
        ctx: Box<dyn CloseableConnectorContext>,
        status_listener: Box<dyn ConnectorStatusListener>,
        initial_state: TargetState,
        on_state_change: Box<dyn Callback<TargetState>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Check if connector is a sink connector
    fn is_sink_connector(&self, conn_name: String) -> bool;

    /// Get connector task configs
    fn connector_task_configs(
        &self,
        conn_name: String,
        conn_config: ConnectorConfig,
    ) -> Vec<HashMap<String, String>>;

    /// Stop and await all connectors
    fn stop_and_await_connectors(&mut self) -> Result<(), Box<dyn Error>>;

    /// Stop and await a specific connector
    fn stop_and_await_connector(&mut self, conn_name: String) -> Result<(), Box<dyn Error>>;

    /// Get all connector names
    fn connector_names(&self) -> Vec<String>;

    /// Check if connector is running
    fn is_running(&self, conn_name: String) -> bool;

    /// Get connector version
    fn connector_version(&self, conn_name: String) -> Option<String>;

    /// Get task version
    fn task_version(&self, task_id: ConnectorTaskId) -> Option<String>;

    /// Start a sink task
    fn start_sink_task(
        &mut self,
        task_id: ConnectorTaskId,
        task: Box<dyn SinkTask>,
    ) -> Result<(), Box<dyn Error>>;

    /// Start a source task
    fn start_source_task(
        &mut self,
        task_id: ConnectorTaskId,
        task: Box<dyn SourceTask>,
    ) -> Result<(), Box<dyn Error>>;

    /// Start an exactly-once source task
    fn start_exactly_once_source_task(
        &mut self,
        task_id: ConnectorTaskId,
        task: Box<dyn SourceTask>,
    ) -> Result<(), Box<dyn Error>>;

    /// Fence zombie tasks
    fn fence_zombies(
        &mut self,
        conn_name: String,
        num_tasks: i32,
        conn_props: HashMap<String, String>,
    ) -> Result<(), Box<dyn Error>>;

    /// Stop and await all tasks
    fn stop_and_await_tasks(&mut self) -> Result<(), Box<dyn Error>>;

    /// Stop and await a specific task
    fn stop_and_await_task(&mut self, task_id: ConnectorTaskId) -> Result<(), Box<dyn Error>>;

    /// Get all task IDs
    fn task_ids(&self) -> Vec<ConnectorTaskId>;

    /// Get worker config
    fn config(&self) -> WorkerConfig;

    /// Get config transformer
    fn config_transformer(&self) -> Box<dyn WorkerConfigTransformer>;

    /// Get metrics
    fn metrics(&self) -> Box<dyn WorkerMetrics>;

    /// Set target state for a connector
    fn set_target_state(
        &mut self,
        conn_name: String,
        state: TargetState,
        state_change_callback: Box<dyn Callback<TargetState>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Get connector offsets
    fn connector_offsets(
        &self,
        conn_name: String,
        conn_config: HashMap<String, String>,
        cb: Box<dyn Callback<ConnectorOffsets>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Modify connector offsets
    fn modify_connector_offsets(
        &self,
        conn_name: String,
        conn_config: HashMap<String, String>,
        offsets: HashMap<HashMap<String, Box<dyn Any>>, HashMap<String, Box<dyn Any>>>,
        cb: Box<dyn Callback<Message>>,
    ) -> Result<(), Box<dyn Error>>;
}

/// Worker implementation
pub struct WorkerImpl {
    /// Worker ID
    worker_id: String,
    /// Worker configuration
    config: WorkerConfig,
    /// Internal key converter
    internal_key_converter: Option<Box<dyn Converter>>,
    /// Internal value converter
    internal_value_converter: Option<Box<dyn Converter>>,
    /// Connectors managed by this worker
    connectors: Arc<Mutex<HashMap<String, WorkerConnector>>>,
    /// Tasks managed by this worker
    tasks: Arc<Mutex<HashMap<ConnectorTaskId, WorkerTask>>>,
    /// Worker metrics
    metrics: Arc<Mutex<WorkerMetricsImpl>>,
    /// Worker start time
    start_time: Option<Instant>,
    /// Running state
    running: Arc<Mutex<bool>>,
}

/// Worker connector wrapper
struct WorkerConnector {
    /// Connector name
    name: String,
    /// Connector instance
    connector: Option<Box<dyn Connector>>,
    /// Source connector instance (if applicable)
    source_connector: Option<Box<dyn SourceConnector>>,
    /// Sink connector instance (if applicable)
    sink_connector: Option<Box<dyn SinkConnector>>,
    /// Connector state
    state: ConnectorState,
    /// Target state
    target_state: TargetState,
    /// Connector config
    config: HashMap<String, String>,
    /// Task configs
    task_configs: Vec<HashMap<String, String>>,
    /// Connector version
    version: Option<String>,
}

/// Worker task wrapper
struct WorkerTask {
    /// Task ID
    id: ConnectorTaskId,
    /// Task state
    state: TaskState,
    /// Task config
    config: HashMap<String, String>,
    /// Task version
    version: Option<String>,
    /// Records produced count
    records_produced: Arc<Mutex<i64>>,
    /// Records consumed count
    records_consumed: Arc<Mutex<i64>>,
    /// Records failed count
    records_failed: Arc<Mutex<i64>>,
    /// Records skipped count
    records_skipped: Arc<Mutex<i64>>,
    /// Commit success count
    commit_success_count: Arc<Mutex<i64>>,
    /// Commit failure count
    commit_failure_count: Arc<Mutex<i64>>,
    /// Commit latency in milliseconds
    commit_latency_ms: Arc<Mutex<f64>>,
    /// Poll latency in milliseconds
    poll_latency_ms: Arc<Mutex<f64>>,
}

impl WorkerImpl {
    /// Create a new worker
    pub fn new(worker_id: String, config: WorkerConfig) -> Self {
        Self {
            worker_id,
            config,
            internal_key_converter: None,
            internal_value_converter: None,
            connectors: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(WorkerMetricsImpl::new())),
            start_time: None,
            running: Arc::new(Mutex::new(false)),
        }
    }

    /// Set internal converters
    pub fn with_converters(
        mut self,
        key_converter: Option<Box<dyn Converter>>,
        value_converter: Option<Box<dyn Converter>>,
    ) -> Self {
        self.internal_key_converter = key_converter;
        self.internal_value_converter = value_converter;
        self
    }
}

impl Worker for WorkerImpl {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if *running {
            return Err(Box::new(ConnectRuntimeError::worker_error(
                "Worker is already running".to_string(),
            )) as Box<dyn Error>);
        }

        self.start_time = Some(Instant::now());
        *running = true;

        Ok(())
    }

    fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if !*running {
            return Err(Box::new(ConnectRuntimeError::worker_error(
                "Worker is not running".to_string(),
            )) as Box<dyn Error>);
        }

        // Release the lock before calling methods that need &mut self
        drop(running);

        // Stop all connectors
        self.stop_and_await_connectors()?;

        // Stop all tasks
        self.stop_and_await_tasks()?;

        // Re-acquire lock to update running state
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        *running = false;
        self.start_time = None;

        Ok(())
    }

    fn start_connector(
        &mut self,
        conn_name: String,
        conn_props: HashMap<String, String>,
        _ctx: Box<dyn CloseableConnectorContext>,
        _status_listener: Box<dyn ConnectorStatusListener>,
        initial_state: TargetState,
        _on_state_change: Box<dyn Callback<TargetState>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if connectors.contains_key(&conn_name) {
            return Err(Box::new(ConnectRuntimeError::connector_error(format!(
                "Connector '{}' already exists",
                conn_name
            ))) as Box<dyn Error>);
        }

        let worker_connector = WorkerConnector {
            name: conn_name.clone(),
            connector: None,
            source_connector: None,
            sink_connector: None,
            state: ConnectorState::Uninitialized,
            target_state: initial_state,
            config: conn_props,
            task_configs: Vec::new(),
            version: None,
        };

        connectors.insert(conn_name, worker_connector);

        Ok(())
    }

    fn is_sink_connector(&self, conn_name: String) -> bool {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return false;
        }

        let connectors = connectors.unwrap();
        if let Some(connector) = connectors.get(&conn_name) {
            connector.sink_connector.is_some()
        } else {
            false
        }
    }

    fn connector_task_configs(
        &self,
        conn_name: String,
        _conn_config: ConnectorConfig,
    ) -> Vec<HashMap<String, String>> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Vec::new();
        }

        let connectors = connectors.unwrap();
        if let Some(connector) = connectors.get(&conn_name) {
            connector.task_configs.clone()
        } else {
            Vec::new()
        }
    }

    fn stop_and_await_connectors(&mut self) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        for connector in connectors.values_mut() {
            connector.state = ConnectorState::Stopped;
        }

        Ok(())
    }

    fn stop_and_await_connector(&mut self, conn_name: String) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if let Some(connector) = connectors.get_mut(&conn_name) {
            connector.state = ConnectorState::Stopped;
            Ok(())
        } else {
            Err(Box::new(ConnectRuntimeError::connector_error(format!(
                "Connector '{}' not found",
                conn_name
            ))) as Box<dyn Error>)
        }
    }

    fn connector_names(&self) -> Vec<String> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Vec::new();
        }

        let connectors = connectors.unwrap();
        connectors.keys().cloned().collect()
    }

    fn is_running(&self, conn_name: String) -> bool {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return false;
        }

        let connectors = connectors.unwrap();
        if let Some(connector) = connectors.get(&conn_name) {
            connector.state == ConnectorState::Running
        } else {
            false
        }
    }

    fn connector_version(&self, conn_name: String) -> Option<String> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return None;
        }

        let connectors = connectors.unwrap();
        if let Some(connector) = connectors.get(&conn_name) {
            connector.version.clone()
        } else {
            None
        }
    }

    fn task_version(&self, task_id: ConnectorTaskId) -> Option<String> {
        let tasks = self.tasks.lock();
        if let Err(e) = tasks {
            return None;
        }

        let tasks = tasks.unwrap();
        if let Some(task) = tasks.get(&task_id) {
            task.version.clone()
        } else {
            None
        }
    }

    fn start_sink_task(
        &mut self,
        task_id: ConnectorTaskId,
        _task: Box<dyn SinkTask>,
    ) -> Result<(), Box<dyn Error>> {
        let mut tasks = self.tasks.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire tasks lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if tasks.contains_key(&task_id) {
            return Err(Box::new(ConnectRuntimeError::task_error(format!(
                "Task '{:?}' already exists",
                task_id
            ))) as Box<dyn Error>);
        }

        let worker_task = WorkerTask {
            id: task_id.clone(),
            state: TaskState::Running,
            config: HashMap::new(),
            version: None,
            records_produced: Arc::new(Mutex::new(0)),
            records_consumed: Arc::new(Mutex::new(0)),
            records_failed: Arc::new(Mutex::new(0)),
            records_skipped: Arc::new(Mutex::new(0)),
            commit_success_count: Arc::new(Mutex::new(0)),
            commit_failure_count: Arc::new(Mutex::new(0)),
            commit_latency_ms: Arc::new(Mutex::new(0.0)),
            poll_latency_ms: Arc::new(Mutex::new(0.0)),
        };

        tasks.insert(task_id, worker_task);

        Ok(())
    }

    fn start_source_task(
        &mut self,
        task_id: ConnectorTaskId,
        _task: Box<dyn SourceTask>,
    ) -> Result<(), Box<dyn Error>> {
        let mut tasks = self.tasks.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire tasks lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if tasks.contains_key(&task_id) {
            return Err(Box::new(ConnectRuntimeError::task_error(format!(
                "Task '{:?}' already exists",
                task_id
            ))) as Box<dyn Error>);
        }

        let worker_task = WorkerTask {
            id: task_id.clone(),
            state: TaskState::Running,
            config: HashMap::new(),
            version: None,
            records_produced: Arc::new(Mutex::new(0)),
            records_consumed: Arc::new(Mutex::new(0)),
            records_failed: Arc::new(Mutex::new(0)),
            records_skipped: Arc::new(Mutex::new(0)),
            commit_success_count: Arc::new(Mutex::new(0)),
            commit_failure_count: Arc::new(Mutex::new(0)),
            commit_latency_ms: Arc::new(Mutex::new(0.0)),
            poll_latency_ms: Arc::new(Mutex::new(0.0)),
        };

        tasks.insert(task_id, worker_task);

        Ok(())
    }

    fn start_exactly_once_source_task(
        &mut self,
        task_id: ConnectorTaskId,
        _task: Box<dyn SourceTask>,
    ) -> Result<(), Box<dyn Error>> {
        let mut tasks = self.tasks.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire tasks lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if tasks.contains_key(&task_id) {
            return Err(Box::new(ConnectRuntimeError::task_error(format!(
                "Task '{:?}' already exists",
                task_id
            ))) as Box<dyn Error>);
        }

        let worker_task = WorkerTask {
            id: task_id.clone(),
            state: TaskState::Running,
            config: HashMap::new(),
            version: None,
            records_produced: Arc::new(Mutex::new(0)),
            records_consumed: Arc::new(Mutex::new(0)),
            records_failed: Arc::new(Mutex::new(0)),
            records_skipped: Arc::new(Mutex::new(0)),
            commit_success_count: Arc::new(Mutex::new(0)),
            commit_failure_count: Arc::new(Mutex::new(0)),
            commit_latency_ms: Arc::new(Mutex::new(0.0)),
            poll_latency_ms: Arc::new(Mutex::new(0.0)),
        };

        tasks.insert(task_id, worker_task);

        Ok(())
    }

    fn fence_zombies(
        &mut self,
        _conn_name: String,
        _num_tasks: i32,
        _conn_props: HashMap<String, String>,
    ) -> Result<(), Box<dyn Error>> {
        // Implementation would fence zombie tasks
        Ok(())
    }

    fn stop_and_await_tasks(&mut self) -> Result<(), Box<dyn Error>> {
        let mut tasks = self.tasks.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire tasks lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        for task in tasks.values_mut() {
            task.state = TaskState::Stopped;
        }

        Ok(())
    }

    fn stop_and_await_task(&mut self, task_id: ConnectorTaskId) -> Result<(), Box<dyn Error>> {
        let mut tasks = self.tasks.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire tasks lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if let Some(task) = tasks.get_mut(&task_id) {
            task.state = TaskState::Stopped;
            Ok(())
        } else {
            Err(Box::new(ConnectRuntimeError::task_error(format!(
                "Task '{:?}' not found",
                task_id
            ))) as Box<dyn Error>)
        }
    }

    fn task_ids(&self) -> Vec<ConnectorTaskId> {
        let tasks = self.tasks.lock();
        if let Err(e) = tasks {
            return Vec::new();
        }

        let tasks = tasks.unwrap();
        tasks.keys().cloned().collect()
    }

    fn config(&self) -> WorkerConfig {
        self.config.clone()
    }

    fn config_transformer(&self) -> Box<dyn WorkerConfigTransformer> {
        Box::new(DefaultWorkerConfigTransformer)
    }

    fn metrics(&self) -> Box<dyn WorkerMetrics> {
        Box::new(WorkerMetricsProxy {
            metrics: self.metrics.clone(),
            start_time: self.start_time,
        })
    }

    fn set_target_state(
        &mut self,
        conn_name: String,
        state: TargetState,
        _state_change_callback: Box<dyn Callback<TargetState>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::worker_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if let Some(connector) = connectors.get_mut(&conn_name) {
            connector.target_state = state;
            Ok(())
        } else {
            Err(Box::new(ConnectRuntimeError::connector_error(format!(
                "Connector '{}' not found",
                conn_name
            ))) as Box<dyn Error>)
        }
    }

    fn connector_offsets(
        &self,
        conn_name: String,
        _conn_config: HashMap<String, String>,
        cb: Box<dyn Callback<ConnectorOffsets>>,
    ) -> Result<(), Box<dyn Error>> {
        let offsets = ConnectorOffsets {
            offsets: HashMap::new(),
        };

        cb.call(offsets);

        Ok(())
    }

    fn modify_connector_offsets(
        &self,
        _conn_name: String,
        _conn_config: HashMap<String, String>,
        _offsets: HashMap<HashMap<String, Box<dyn Any>>, HashMap<String, Box<dyn Any>>>,
        cb: Box<dyn Callback<Message>>,
    ) -> Result<(), Box<dyn Error>> {
        let message = Message {
            content: "Offsets modified".to_string(),
        };

        cb.call(message);

        Ok(())
    }
}

/// Closeable connector context
pub trait CloseableConnectorContext {
    /// Close the context
    fn close(&mut self) -> Result<(), Box<dyn Error>>;
}

/// Connector status listener
pub trait ConnectorStatusListener {
    /// Called when connector status changes
    fn on_status_change(&self, status: ConnectorStatus);
}

/// Callback trait for async operations
pub trait Callback<T> {
    /// Called when operation completes
    fn call(&self, result: T);
}

/// Target state for a connector
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetState {
    Started,
    Stopped,
    Paused,
}

/// Connector task ID
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectorTaskId {
    pub connector: String,
    pub task: i32,
}

/// Connector config
pub struct ConnectorConfig {
    pub props: HashMap<String, String>,
}

/// Worker config
#[derive(Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub config: HashMap<String, String>,
}

/// Worker config transformer
pub trait WorkerConfigTransformer {
    /// Transform worker config
    fn transform(&self, config: WorkerConfig) -> Result<WorkerConfig, Box<dyn Error>>;
}

/// Worker metrics
pub trait WorkerMetrics {
    /// Get metrics snapshot
    fn snapshot(&self) -> HashMap<String, f64>;
}

/// Connector status
#[derive(Debug, Clone)]
pub struct ConnectorStatus {
    pub name: String,
    pub state: ConnectorState,
    pub trace: String,
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

/// Connector offsets
pub struct ConnectorOffsets {
    pub offsets: HashMap<HashMap<String, Box<dyn Any>>, HashMap<String, Box<dyn Any>>>,
}

/// Message type
pub struct Message {
    pub content: String,
}

/// Worker metrics implementation
struct WorkerMetricsImpl {
    /// Records produced count
    records_produced: i64,
    /// Records consumed count
    records_consumed: i64,
    /// Records failed count
    records_failed: i64,
    /// Records skipped count
    records_skipped: i64,
}

impl WorkerMetricsImpl {
    /// Create new worker metrics
    fn new() -> Self {
        Self {
            records_produced: 0,
            records_consumed: 0,
            records_failed: 0,
            records_skipped: 0,
        }
    }
}

/// Worker metrics proxy
struct WorkerMetricsProxy {
    /// Metrics reference
    metrics: Arc<Mutex<WorkerMetricsImpl>>,
    /// Worker start time
    start_time: Option<Instant>,
}

impl WorkerMetrics for WorkerMetricsProxy {
    fn snapshot(&self) -> HashMap<String, f64> {
        let metrics = self.metrics.lock();
        if let Err(e) = metrics {
            return HashMap::new();
        }

        let metrics = metrics.unwrap();
        let mut snapshot = HashMap::new();

        snapshot.insert(
            "records-produced".to_string(),
            metrics.records_produced as f64,
        );
        snapshot.insert(
            "records-consumed".to_string(),
            metrics.records_consumed as f64,
        );
        snapshot.insert("records-failed".to_string(), metrics.records_failed as f64);
        snapshot.insert(
            "records-skipped".to_string(),
            metrics.records_skipped as f64,
        );

        if let Some(start_time) = self.start_time {
            let uptime = start_time.elapsed().as_millis() as f64;
            snapshot.insert("uptime-ms".to_string(), uptime);
        }

        snapshot
    }
}

/// Default worker config transformer
struct DefaultWorkerConfigTransformer;

impl WorkerConfigTransformer for DefaultWorkerConfigTransformer {
    fn transform(&self, config: WorkerConfig) -> Result<WorkerConfig, Box<dyn Error>> {
        Ok(config)
    }
}
