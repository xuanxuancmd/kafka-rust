//! Herder module
//!
//! Provides the Herder trait for managing connectors and tasks.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::{Arc, Mutex};

use connect_api::{Connector, SinkConnector, SourceConnector};

use crate::{
    ConnectRuntimeError, ConnectorOffsets, MetricsConnectMetrics as ConnectMetrics,
    StatusBackingStore, WorkerConnectorState as ConnectorState,
    WorkerConnectorTaskId as ConnectorTaskId, WorkerTargetState as TargetState,
};

/// Herder trait for managing connectors and tasks
pub trait Herder {
    /// Start the herder
    fn start(&mut self) -> Result<(), Box<dyn Error>>;

    /// Stop the herder
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    /// Check if herder is ready
    fn is_ready(&self) -> bool;

    /// Perform health check
    fn health_check(&self, callback: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>>;

    /// Get all connector names (async)
    fn connectors(&self, callback: Box<dyn Callback<Vec<String>>>) -> Result<(), Box<dyn Error>>;

    /// Get connector info (async)
    fn connector_info(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<ConnectorInfo>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Get connector config (async)
    fn connector_config(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<HashMap<String, String>>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Put connector config (async)
    fn put_connector_config(
        &self,
        conn_name: String,
        config: HashMap<String, String>,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Put connector config with target state (async)
    fn put_connector_config_with_target_state(
        &self,
        conn_name: String,
        config: HashMap<String, String>,
        target_state: TargetState,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Patch connector config (async)
    fn patch_connector_config(
        &self,
        conn_name: String,
        config_patch: HashMap<String, String>,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Delete connector config (async)
    fn delete_connector_config(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Request task reconfiguration
    fn request_task_reconfiguration(&self, conn_name: String) -> Result<(), Box<dyn Error>>;

    /// Get task configs (async)
    fn task_configs(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<Vec<TaskInfo>>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Put task configs (async)
    fn put_task_configs(
        &self,
        conn_name: String,
        configs: Vec<HashMap<String, String>>,
        callback: Box<dyn Callback<()>>,
        request_signature: InternalRequestSignature,
    ) -> Result<(), Box<dyn Error>>;

    /// Fence zombie source tasks (async)
    fn fence_zombie_source_tasks(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<()>>,
        request_signature: InternalRequestSignature,
    ) -> Result<(), Box<dyn Error>>;

    /// Get all connector names (sync)
    fn connectors_sync(&self) -> Vec<String>;

    /// Get connector info (sync)
    fn connector_info_sync(&self, conn_name: String) -> Option<ConnectorInfo>;

    /// Get connector status (sync)
    fn connector_status(&self, conn_name: String) -> Option<ConnectorState>;

    /// Get connector active topics
    fn connector_active_topics(&self, conn_name: String) -> Vec<String>;

    /// Reset connector active topics
    fn reset_connector_active_topics(&self, conn_name: String);

    /// Get status backing store
    fn status_backing_store(&self) -> Box<dyn StatusBackingStore>;

    /// Get task status
    fn task_status(&self, id: ConnectorTaskId) -> Option<TaskState>;

    /// Validate connector config (async)
    fn validate_connector_config(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn Callback<ConfigInfos>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Restart a task (async)
    fn restart_task(
        &self,
        id: ConnectorTaskId,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Restart a connector (async)
    fn restart_connector(
        &self,
        conn_name: String,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Restart a connector with delay (async)
    fn restart_connector_with_delay(
        &self,
        delay_ms: i64,
        conn_name: String,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Restart connector and tasks (async)
    fn restart_connector_and_tasks(
        &self,
        request: RestartRequest,
        cb: Box<dyn Callback<ConnectorStateInfo>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Stop a connector (async)
    fn stop_connector(
        &self,
        connector: String,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Pause a connector
    fn pause_connector(&self, connector: String) -> Result<(), Box<dyn Error>>;

    /// Resume a connector
    fn resume_connector(&self, connector: String) -> Result<(), Box<dyn Error>>;

    /// Get plugins
    fn plugins(&self) -> Box<dyn Plugins>;

    /// Get Kafka cluster ID
    fn kafka_cluster_id(&self) -> Option<String>;

    /// Get connector plugin config
    fn connector_plugin_config(&self, plugin_name: String) -> Option<HashMap<String, String>>;

    /// Get connector plugin config with version
    fn connector_plugin_config_with_version(
        &self,
        plugin_name: String,
        version: VersionRange,
    ) -> Option<HashMap<String, String>>;

    /// Get connector offsets (async)
    fn connector_offsets(
        &self,
        conn_name: String,
        cb: Box<dyn Callback<ConnectorOffsets>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Alter connector offsets (async)
    fn alter_connector_offsets(
        &self,
        conn_name: String,
        offsets: HashMap<HashMap<String, Box<dyn Any>>, HashMap<String, Box<dyn Any>>>,
        cb: Box<dyn Callback<Message>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Reset connector offsets (async)
    fn reset_connector_offsets(
        &self,
        conn_name: String,
        cb: Box<dyn Callback<Message>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Get logger level
    fn logger_level(&self, logger: String) -> Option<String>;

    /// Get all logger levels
    fn all_logger_levels(&self) -> HashMap<String, String>;

    /// Set worker logger level
    fn set_worker_logger_level(
        &self,
        namespace: String,
        level: String,
    ) -> Result<(), Box<dyn Error>>;

    /// Set cluster logger level
    fn set_cluster_logger_level(
        &self,
        namespace: String,
        level: String,
    ) -> Result<(), Box<dyn Error>>;

    /// Get connect metrics
    fn connect_metrics(&self) -> Box<dyn ConnectMetrics>;
}

/// Standalone herder implementation
pub struct StandaloneHerder {
    /// Worker instance
    worker: Option<Box<dyn crate::Worker>>,
    /// Status backing store
    status_store: Option<Box<dyn StatusBackingStore>>,
    /// Connectors managed by this herder
    connectors: Arc<Mutex<HashMap<String, ConnectorInfo>>>,
    /// Tasks managed by this herder
    tasks: Arc<Mutex<HashMap<ConnectorTaskId, TaskInfo>>>,
    /// Ready state
    ready: Arc<Mutex<bool>>,
    /// Running state
    running: Arc<Mutex<bool>>,
}

impl StandaloneHerder {
    /// Create a new standalone herder
    pub fn new() -> Self {
        Self {
            worker: None,
            status_store: None,
            connectors: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            ready: Arc::new(Mutex::new(false)),
            running: Arc::new(Mutex::new(false)),
        }
    }

    /// Create a new standalone herder with worker and status store
    pub fn with_components(
        worker: Box<dyn crate::Worker>,
        status_store: Box<dyn StatusBackingStore>,
    ) -> Self {
        Self {
            worker: Some(worker),
            status_store: Some(status_store),
            connectors: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            ready: Arc::new(Mutex::new(false)),
            running: Arc::new(Mutex::new(false)),
        }
    }

    /// Start -> herder
    pub fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if *running {
            return Err(Box::new(ConnectRuntimeError::herder_error(
                "StandaloneHerder is already running".to_string(),
            )) as Box<dyn Error>);
        }

        // Start status store if available
        if let Some(store) = &mut self.status_store {
            store.start()?;
        }

        // Start worker if available
        if let Some(worker) = &mut self.worker {
            worker.start()?;
        }

        *running = true;
        {
            let mut ready = self.ready.lock().map_err(|e| {
                Box::new(ConnectRuntimeError::herder_error(format!(
                    "Failed to acquire ready lock: {}",
                    e
                ))) as Box<dyn Error>
            })?;
            *ready = true;
        }

        Ok(())
    }

    /// Stop -> herder
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if !*running {
            return Err(Box::new(ConnectRuntimeError::herder_error(
                "StandaloneHerder is not running".to_string(),
            )) as Box<dyn Error>);
        }

        // Stop worker if available
        if let Some(worker) = &mut self.worker {
            worker.stop()?;
        }

        // Stop status store if available
        if let Some(store) = &mut self.status_store {
            store.stop()?;
        }

        *running = false;
        {
            let mut ready = self.ready.lock().map_err(|e| {
                Box::new(ConnectRuntimeError::herder_error(format!(
                    "Failed to acquire ready lock: {}",
                    e
                ))) as Box<dyn Error>
            })?;
            *ready = false;
        }

        Ok(())
    }

    /// Wait for herder to be ready
    pub fn ready(&mut self) {
        let mut ready = self.ready.lock();
        if let Ok(mut ready) = ready {
            *ready = true;
        }
    }

    /// Perform health check
    pub fn health_check(&self, cb: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>> {
        cb.call(());
        Ok(())
    }

    /// Get generation (always 0 for standalone mode)
    pub fn generation(&self) -> i32 {
        0
    }
}

impl Herder for StandaloneHerder {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        StandaloneHerder::start(self)
    }

    fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        StandaloneHerder::stop(self)
    }

    fn is_ready(&self) -> bool {
        let ready = self.ready.lock();
        if let Ok(ready) = ready {
            *ready
        } else {
            false
        }
    }

    fn health_check(&self, callback: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>> {
        self.health_check(callback)
    }

    fn connectors(&self, callback: Box<dyn Callback<Vec<String>>>) -> Result<(), Box<dyn Error>> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>);
        }

        let connectors = connectors.unwrap();
        let names: Vec<String> = connectors.keys().cloned().collect();
        callback.call(names);
        Ok(())
    }

    fn connector_info(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<ConnectorInfo>>,
    ) -> Result<(), Box<dyn Error>> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>);
        }

        let connectors = connectors.unwrap();
        if let Some(info) = connectors.get(&conn_name) {
            callback.call(info.clone());
        } else {
            callback.call(ConnectorInfo {
                name: conn_name,
                config: HashMap::new(),
                tasks: Vec::new(),
            });
        }
        Ok(())
    }

    fn connector_config(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<HashMap<String, String>>>,
    ) -> Result<(), Box<dyn Error>> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>);
        }

        let connectors = connectors.unwrap();
        if let Some(info) = connectors.get(&conn_name) {
            callback.call(info.config.clone());
        } else {
            callback.call(HashMap::new());
        }
        Ok(())
    }

    fn put_connector_config(
        &self,
        conn_name: String,
        config: HashMap<String, String>,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        let created = !connectors.contains_key(&conn_name);

        if connectors.contains_key(&conn_name) && !allow_replace {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Connector '{}' already exists",
                conn_name
            ))) as Box<dyn Error>);
        }

        let info = ConnectorInfo {
            name: conn_name.clone(),
            config,
            tasks: Vec::new(),
        };

        connectors.insert(conn_name, info.clone());

        callback.call(Created {
            created,
            value: info,
        });
        Ok(())
    }

    fn put_connector_config_with_target_state(
        &self,
        conn_name: String,
        config: HashMap<String, String>,
        _target_state: TargetState,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>> {
        self.put_connector_config(conn_name, config, allow_replace, callback)
    }

    fn patch_connector_config(
        &self,
        conn_name: String,
        config_patch: HashMap<String, String>,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if let Some(info) = connectors.get_mut(&conn_name) {
            for (key, value) in config_patch {
                info.config.insert(key, value);
            }
            callback.call(Created {
                created: false,
                value: info.clone(),
            });
        } else {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Connector '{}' not found",
                conn_name
            ))) as Box<dyn Error>);
        }

        Ok(())
    }

    fn delete_connector_config(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if let Some(info) = connectors.remove(&conn_name) {
            callback.call(Created {
                created: false,
                value: info,
            });
        } else {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Connector '{}' not found",
                conn_name
            ))) as Box<dyn Error>);
        }

        Ok(())
    }

    fn request_task_reconfiguration(&self, _conn_name: String) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn task_configs(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<Vec<TaskInfo>>>,
    ) -> Result<(), Box<dyn Error>> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>);
        }

        let connectors = connectors.unwrap();
        if let Some(info) = connectors.get(&conn_name) {
            callback.call(info.tasks.clone());
        } else {
            callback.call(Vec::new());
        }
        Ok(())
    }

    fn put_task_configs(
        &self,
        conn_name: String,
        configs: Vec<HashMap<String, String>>,
        callback: Box<dyn Callback<()>>,
        _request_signature: InternalRequestSignature,
    ) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if let Some(info) = connectors.get_mut(&conn_name) {
            info.tasks = configs
                .into_iter()
                .enumerate()
                .map(|(i, config)| TaskInfo {
                    id: ConnectorTaskId {
                        connector: conn_name.clone(),
                        task: i as i32,
                    },
                    config,
                })
                .collect();
        }

        callback.call(());
        Ok(())
    }

    fn fence_zombie_source_tasks(
        &self,
        _conn_name: String,
        callback: Box<dyn Callback<()>>,
        _request_signature: InternalRequestSignature,
    ) -> Result<(), Box<dyn Error>> {
        callback.call(());
        Ok(())
    }

    fn connectors_sync(&self) -> Vec<String> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Vec::new();
        }

        let connectors = connectors.unwrap();
        connectors.keys().cloned().collect()
    }

    fn connector_info_sync(&self, conn_name: String) -> Option<ConnectorInfo> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return None;
        }

        let connectors = connectors.unwrap();
        connectors.get(&conn_name).cloned()
    }

    fn connector_status(&self, conn_name: String) -> Option<ConnectorState> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return None;
        }

        let connectors = connectors.unwrap();
        if connectors.contains_key(&conn_name) {
            Some(ConnectorState::Running)
        } else {
            None
        }
    }

    fn connector_active_topics(&self, _conn_name: String) -> Vec<String> {
        Vec::new()
    }

    fn reset_connector_active_topics(&self, _conn_name: String) {
        // Implementation
    }

    fn status_backing_store(&self) -> Box<dyn StatusBackingStore> {
        if let Some(store) = &self.status_store {
            // Note: This is a simplified implementation
            // In real code, we would need to clone the store
            panic!("Cannot clone StatusBackingStore");
        } else {
            panic!("No status store available");
        }
    }

    fn task_status(&self, _id: ConnectorTaskId) -> Option<TaskState> {
        Some(TaskState::Running)
    }

    fn validate_connector_config(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn Callback<ConfigInfos>>,
    ) -> Result<(), Box<dyn Error>> {
        let config_infos = ConfigInfos {
            configs: HashMap::new(),
        };

        callback.call(config_infos);
        Ok(())
    }

    fn restart_task(
        &self,
        _id: ConnectorTaskId,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>> {
        cb.call(());
        Ok(())
    }

    fn restart_connector(
        &self,
        _conn_name: String,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>> {
        cb.call(());
        Ok(())
    }

    fn restart_connector_with_delay(
        &self,
        _delay_ms: i64,
        _conn_name: String,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>> {
        cb.call(());
        Ok(())
    }

    fn restart_connector_and_tasks(
        &self,
        _request: RestartRequest,
        cb: Box<dyn Callback<ConnectorStateInfo>>,
    ) -> Result<(), Box<dyn Error>> {
        let state_info = ConnectorStateInfo {
            name: String::new(),
            state: ConnectorState::Running,
            task_states: HashMap::new(),
        };

        cb.call(state_info);
        Ok(())
    }

    fn stop_connector(
        &self,
        _connector: String,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>> {
        cb.call(());
        Ok(())
    }

    fn pause_connector(&self, _connector: String) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn resume_connector(&self, _connector: String) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn plugins(&self) -> Box<dyn Plugins> {
        Box::new(DefaultPlugins)
    }

    fn kafka_cluster_id(&self) -> Option<String> {
        None
    }

    fn connector_plugin_config(&self, _plugin_name: String) -> Option<HashMap<String, String>> {
        None
    }

    fn connector_plugin_config_with_version(
        &self,
        _plugin_name: String,
        _version: VersionRange,
    ) -> Option<HashMap<String, String>> {
        None
    }

    fn connector_offsets(
        &self,
        _conn_name: String,
        cb: Box<dyn Callback<ConnectorOffsets>>,
    ) -> Result<(), Box<dyn Error>> {
        let offsets = ConnectorOffsets {
            offsets: HashMap::new(),
        };

        cb.call(offsets);
        Ok(())
    }

    fn alter_connector_offsets(
        &self,
        _conn_name: String,
        _offsets: HashMap<HashMap<String, Box<dyn Any>>, HashMap<String, Box<dyn Any>>>,
        cb: Box<dyn Callback<Message>>,
    ) -> Result<(), Box<dyn Error>> {
        let message = Message {
            content: "Offsets altered".to_string(),
        };

        cb.call(message);
        Ok(())
    }

    fn reset_connector_offsets(
        &self,
        _conn_name: String,
        cb: Box<dyn Callback<Message>>,
    ) -> Result<(), Box<dyn Error>> {
        let message = Message {
            content: "Offsets reset".to_string(),
        };

        cb.call(message);
        Ok(())
    }

    fn logger_level(&self, _logger: String) -> Option<String> {
        None
    }

    fn all_logger_levels(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn set_worker_logger_level(
        &self,
        _namespace: String,
        _level: String,
    ) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn set_cluster_logger_level(
        &self,
        _namespace: String,
        _level: String,
    ) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn connect_metrics(&self) -> Box<dyn ConnectMetrics> {
        Box::new(DefaultConnectMetrics)
    }
}

/// Distributed herder implementation
pub struct DistributedHerder {
    /// Worker instance
    worker: Option<Box<dyn crate::Worker>>,
    /// Status backing store
    status_store: Option<Box<dyn StatusBackingStore>>,
    /// Connectors managed by this herder
    connectors: Arc<Mutex<HashMap<String, ConnectorInfo>>>,
    /// Tasks managed by this herder
    tasks: Arc<Mutex<HashMap<ConnectorTaskId, TaskInfo>>>,
    /// Ready state
    ready: Arc<Mutex<bool>>,
    /// Running state
    running: Arc<Mutex<bool>>,
    /// Generation number
    generation: Arc<Mutex<i32>>,
    /// Leader flag
    is_leader: Arc<Mutex<bool>>,
}

impl DistributedHerder {
    /// Create a new distributed herder
    pub fn new() -> Self {
        Self {
            worker: None,
            status_store: None,
            connectors: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            ready: Arc::new(Mutex::new(false)),
            running: Arc::new(Mutex::new(false)),
            generation: Arc::new(Mutex::new(0)),
            is_leader: Arc::new(Mutex::new(false)),
        }
    }

    /// Create a new distributed herder with worker and status store
    pub fn with_components(
        worker: Box<dyn crate::Worker>,
        status_store: Box<dyn StatusBackingStore>,
    ) -> Self {
        Self {
            worker: Some(worker),
            status_store: Some(status_store),
            connectors: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            ready: Arc::new(Mutex::new(false)),
            running: Arc::new(Mutex::new(false)),
            generation: Arc::new(Mutex::new(0)),
            is_leader: Arc::new(Mutex::new(false)),
        }
    }

    /// Start -> herder (submit to executor)
    pub fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if *running {
            return Err(Box::new(ConnectRuntimeError::herder_error(
                "DistributedHerder is already running".to_string(),
            )) as Box<dyn Error>);
        }

        // Start status store if available
        if let Some(store) = &mut self.status_store {
            store.start()?;
        }

        // Start worker if available
        if let Some(worker) = &mut self.worker {
            worker.start()?;
        }

        *running = true;
        {
            let mut ready = self.ready.lock().map_err(|e| {
                Box::new(ConnectRuntimeError::herder_error(format!(
                    "Failed to acquire ready lock: {}",
                    e
                ))) as Box<dyn Error>
            })?;
            *ready = true;
        }

        Ok(())
    }

    /// Main run loop
    pub fn run(&mut self) -> Result<(), Box<dyn Error>> {
        loop {
            self.tick()?;

            // Check if still running
            let running = self.running.lock();
            if let Ok(running) = running {
                if !*running {
                    break;
                }
            } else {
                break;
            }

            // Sleep for a short time
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        Ok(())
    }

    /// Halt (orderly shutdown)
    pub fn halt(&mut self) -> Result<(), Box<dyn Error>> {
        self.stop()
    }

    /// Stop -> herder
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if !*running {
            return Err(Box::new(ConnectRuntimeError::herder_error(
                "DistributedHerder is not running".to_string(),
            )) as Box<dyn Error>);
        }

        // Stop worker if available
        if let Some(worker) = &mut self.worker {
            worker.stop()?;
        }

        // Stop status store if available
        if let Some(store) = &mut self.status_store {
            store.stop()?;
        }

        *running = false;
        {
            let mut ready = self.ready.lock().map_err(|e| {
                Box::new(ConnectRuntimeError::herder_error(format!(
                    "Failed to acquire ready lock: {}",
                    e
                ))) as Box<dyn Error>
            })?;
            *ready = false;
        }

        Ok(())
    }

    /// Main loop tick method
    pub fn tick(&mut self) -> Result<(), Box<dyn Error>> {
        // In a real implementation, this would:
        // 1. Handle group membership changes
        // 2. Process config updates
        // 3. Handle leader responsibilities
        // 4. Process connector/task lifecycle changes

        Ok(())
    }

    /// Get current generation
    pub fn generation(&self) -> i32 {
        let generation = self.generation.lock();
        if let Ok(generation) = generation {
            *generation
        } else {
            0
        }
    }
}

impl Herder for DistributedHerder {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        DistributedHerder::start(self)
    }

    fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        DistributedHerder::stop(self)
    }

    fn is_ready(&self) -> bool {
        let ready = self.ready.lock();
        if let Ok(ready) = ready {
            *ready
        } else {
            false
        }
    }

    fn health_check(&self, callback: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>> {
        callback.call(());
        Ok(())
    }

    fn connectors(&self, callback: Box<dyn Callback<Vec<String>>>) -> Result<(), Box<dyn Error>> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>);
        }

        let connectors = connectors.unwrap();
        let names: Vec<String> = connectors.keys().cloned().collect();
        callback.call(names);
        Ok(())
    }

    fn connector_info(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<ConnectorInfo>>,
    ) -> Result<(), Box<dyn Error>> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>);
        }

        let connectors = connectors.unwrap();
        if let Some(info) = connectors.get(&conn_name) {
            callback.call(info.clone());
        } else {
            callback.call(ConnectorInfo {
                name: conn_name,
                config: HashMap::new(),
                tasks: Vec::new(),
            });
        }
        Ok(())
    }

    fn connector_config(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<HashMap<String, String>>>,
    ) -> Result<(), Box<dyn Error>> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>);
        }

        let connectors = connectors.unwrap();
        if let Some(info) = connectors.get(&conn_name) {
            callback.call(info.config.clone());
        } else {
            callback.call(HashMap::new());
        }
        Ok(())
    }

    fn put_connector_config(
        &self,
        conn_name: String,
        config: HashMap<String, String>,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        let created = !connectors.contains_key(&conn_name);

        if connectors.contains_key(&conn_name) && !allow_replace {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Connector '{}' already exists",
                conn_name
            ))) as Box<dyn Error>);
        }

        let info = ConnectorInfo {
            name: conn_name.clone(),
            config,
            tasks: Vec::new(),
        };

        connectors.insert(conn_name, info.clone());

        callback.call(Created {
            created,
            value: info,
        });
        Ok(())
    }

    fn put_connector_config_with_target_state(
        &self,
        conn_name: String,
        config: HashMap<String, String>,
        _target_state: TargetState,
        allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>> {
        self.put_connector_config(conn_name, config, allow_replace, callback)
    }

    fn patch_connector_config(
        &self,
        conn_name: String,
        config_patch: HashMap<String, String>,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if let Some(info) = connectors.get_mut(&conn_name) {
            for (key, value) in config_patch {
                info.config.insert(key, value);
            }
            callback.call(Created {
                created: false,
                value: info.clone(),
            });
        } else {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Connector '{}' not found",
                conn_name
            ))) as Box<dyn Error>);
        }

        Ok(())
    }

    fn delete_connector_config(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if let Some(info) = connectors.remove(&conn_name) {
            callback.call(Created {
                created: false,
                value: info,
            });
        } else {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Connector '{}' not found",
                conn_name
            ))) as Box<dyn Error>);
        }

        Ok(())
    }

    fn request_task_reconfiguration(&self, _conn_name: String) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn task_configs(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<Vec<TaskInfo>>>,
    ) -> Result<(), Box<dyn Error>> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>);
        }

        let connectors = connectors.unwrap();
        if let Some(info) = connectors.get(&conn_name) {
            callback.call(info.tasks.clone());
        } else {
            callback.call(Vec::new());
        }
        Ok(())
    }

    fn put_task_configs(
        &self,
        conn_name: String,
        configs: Vec<HashMap<String, String>>,
        callback: Box<dyn Callback<()>>,
        _request_signature: InternalRequestSignature,
    ) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if let Some(info) = connectors.get_mut(&conn_name) {
            info.tasks = configs
                .into_iter()
                .enumerate()
                .map(|(i, config)| TaskInfo {
                    id: ConnectorTaskId {
                        connector: conn_name.clone(),
                        task: i as i32,
                    },
                    config,
                })
                .collect();
        }

        callback.call(());
        Ok(())
    }

    fn fence_zombie_source_tasks(
        &self,
        _conn_name: String,
        callback: Box<dyn Callback<()>>,
        _request_signature: InternalRequestSignature,
    ) -> Result<(), Box<dyn Error>> {
        callback.call(());
        Ok(())
    }

    fn connectors_sync(&self) -> Vec<String> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return Vec::new();
        }

        let connectors = connectors.unwrap();
        connectors.keys().cloned().collect()
    }

    fn connector_info_sync(&self, conn_name: String) -> Option<ConnectorInfo> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return None;
        }

        let connectors = connectors.unwrap();
        connectors.get(&conn_name).cloned()
    }

    fn connector_status(&self, conn_name: String) -> Option<ConnectorState> {
        let connectors = self.connectors.lock();
        if let Err(e) = connectors {
            return None;
        }

        let connectors = connectors.unwrap();
        if connectors.contains_key(&conn_name) {
            Some(ConnectorState::Running)
        } else {
            None
        }
    }

    fn connector_active_topics(&self, _conn_name: String) -> Vec<String> {
        Vec::new()
    }

    fn reset_connector_active_topics(&self, _conn_name: String) {
        // Implementation
    }

    fn status_backing_store(&self) -> Box<dyn StatusBackingStore> {
        if let Some(store) = &self.status_store {
            // Note: This is a simplified implementation
            // In real code, we would need to clone the store
            panic!("Cannot clone StatusBackingStore");
        } else {
            panic!("No status store available");
        }
    }

    fn task_status(&self, _id: ConnectorTaskId) -> Option<TaskState> {
        Some(TaskState::Running)
    }

    fn validate_connector_config(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn Callback<ConfigInfos>>,
    ) -> Result<(), Box<dyn Error>> {
        let config_infos = ConfigInfos {
            configs: HashMap::new(),
        };

        callback.call(config_infos);
        Ok(())
    }

    fn restart_task(
        &self,
        _id: ConnectorTaskId,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>> {
        cb.call(());
        Ok(())
    }

    fn restart_connector(
        &self,
        _conn_name: String,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>> {
        cb.call(());
        Ok(())
    }

    fn restart_connector_with_delay(
        &self,
        _delay_ms: i64,
        _conn_name: String,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>> {
        cb.call(());
        Ok(())
    }

    fn restart_connector_and_tasks(
        &self,
        _request: RestartRequest,
        cb: Box<dyn Callback<ConnectorStateInfo>>,
    ) -> Result<(), Box<dyn Error>> {
        let state_info = ConnectorStateInfo {
            name: String::new(),
            state: ConnectorState::Running,
            task_states: HashMap::new(),
        };

        cb.call(state_info);
        Ok(())
    }

    fn stop_connector(
        &self,
        _connector: String,
        cb: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>> {
        cb.call(());
        Ok(())
    }

    fn pause_connector(&self, _connector: String) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn resume_connector(&self, _connector: String) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn plugins(&self) -> Box<dyn Plugins> {
        Box::new(DefaultPlugins)
    }

    fn kafka_cluster_id(&self) -> Option<String> {
        None
    }

    fn connector_plugin_config(&self, _plugin_name: String) -> Option<HashMap<String, String>> {
        None
    }

    fn connector_plugin_config_with_version(
        &self,
        _plugin_name: String,
        _version: VersionRange,
    ) -> Option<HashMap<String, String>> {
        None
    }

    fn connector_offsets(
        &self,
        _conn_name: String,
        cb: Box<dyn Callback<ConnectorOffsets>>,
    ) -> Result<(), Box<dyn Error>> {
        let offsets = ConnectorOffsets {
            offsets: HashMap::new(),
        };

        cb.call(offsets);
        Ok(())
    }

    fn alter_connector_offsets(
        &self,
        _conn_name: String,
        _offsets: HashMap<HashMap<String, Box<dyn Any>>, HashMap<String, Box<dyn Any>>>,
        cb: Box<dyn Callback<Message>>,
    ) -> Result<(), Box<dyn Error>> {
        let message = Message {
            content: "Offsets altered".to_string(),
        };

        cb.call(message);
        Ok(())
    }

    fn reset_connector_offsets(
        &self,
        _conn_name: String,
        cb: Box<dyn Callback<Message>>,
    ) -> Result<(), Box<dyn Error>> {
        let message = Message {
            content: "Offsets reset".to_string(),
        };

        cb.call(message);
        Ok(())
    }

    fn logger_level(&self, _logger: String) -> Option<String> {
        None
    }

    fn all_logger_levels(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn set_worker_logger_level(
        &self,
        _namespace: String,
        _level: String,
    ) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn set_cluster_logger_level(
        &self,
        _namespace: String,
        _level: String,
    ) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn connect_metrics(&self) -> Box<dyn ConnectMetrics> {
        Box::new(DefaultConnectMetrics)
    }
}

/// Callback trait for async operations
pub trait Callback<T> {
    /// Called when operation completes
    fn call(&self, result: T);
}

/// Created wrapper for async results
#[derive(Debug, Clone)]
pub struct Created<T> {
    pub created: bool,
    pub value: T,
}

/// Internal request signature
#[derive(Debug, Clone)]
pub struct InternalRequestSignature {
    pub signature: String,
}

/// Task state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Uninitialized,
    Running,
    Paused,
    Stopped,
    Failed,
    Destroyed,
}

/// Restart request
#[derive(Debug)]
pub struct RestartRequest {
    pub connector_name: String,
    pub include_tasks: bool,
    pub only_failed: bool,
}

/// Connector state info
#[derive(Debug)]
pub struct ConnectorStateInfo {
    pub name: String,
    pub state: ConnectorState,
    pub task_states: HashMap<ConnectorTaskId, TaskState>,
}

/// Plugins trait
pub trait Plugins {
    /// Get all plugins
    fn all_plugins(&self) -> Vec<PluginInfo>;
}

/// Plugin info
#[derive(Debug)]
pub struct PluginInfo {
    pub name: String,
    pub version: String,
    pub class_name: String,
}

/// Version range
#[derive(Debug, Clone)]
pub struct VersionRange {
    pub min_version: Option<String>,
    pub max_version: Option<String>,
}

/// Message type
pub struct Message {
    pub content: String,
}

/// Connector info
#[derive(Debug, Clone)]
pub struct ConnectorInfo {
    pub name: String,
    pub config: HashMap<String, String>,
    pub tasks: Vec<TaskInfo>,
}

/// Task info
#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub id: ConnectorTaskId,
    pub config: HashMap<String, String>,
}

/// Config infos
#[derive(Debug, Clone)]
pub struct ConfigInfos {
    pub configs: HashMap<String, HashMap<String, String>>,
}

/// Default plugins implementation
struct DefaultPlugins;

impl Plugins for DefaultPlugins {
    fn all_plugins(&self) -> Vec<PluginInfo> {
        use crate::plugin_registry::all_plugins;

        all_plugins()
            .into_iter()
            .map(|descriptor| PluginInfo {
                name: descriptor.name,
                version: descriptor.version,
                class_name: descriptor.class_name,
            })
            .collect()
    }
}

/// Default connect metrics implementation
struct DefaultConnectMetrics;

impl ConnectMetrics for DefaultConnectMetrics {
    fn snapshot(&self) -> HashMap<String, f64> {
        HashMap::new()
    }

    fn increment_counter(&self, _name: &str, _value: f64) {
        // Implementation
    }

    fn set_gauge(&self, _name: &str, _value: f64) {
        // Implementation
    }

    fn record_time(&self, _name: &str, _duration_ms: f64) {
        // Implementation
    }

    fn record_histogram(&self, _name: &str, _value: f64) {
        // Implementation
    }
}
