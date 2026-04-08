//! Embedded Connect Cluster
//!
//! Provides a test helper class for simulating a complete Kafka cluster in test environments.

use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};

use crate::{
    errors::ConnectRuntimeError,
    herder::{
        Callback, ConfigInfos, ConnectorInfo, ConnectorStateInfo, Created, Herder,
        InternalRequestSignature, Message, PluginInfo, Plugins, RestartRequest, TaskInfo,
        TaskState as HerderTaskState, VersionRange,
    },
    metrics::ConnectMetrics,
    storage::{
        ConfigBackingStore, MemoryConfigBackingStore, MemoryStatusBackingStore, StatusBackingStore,
        TargetState as StorageTargetState,
    },
    worker::{ConnectorOffsets, ConnectorState, ConnectorTaskId, TargetState as WorkerTargetState},
};

/// Embedded Connect Cluster for testing
///
/// This is a test helper class that simulates a complete Kafka cluster
/// in test environments. It uses in-memory storage and provides basic
/// cluster management functionality.
pub struct EmbeddedConnectCluster {
    /// Herder configuration
    config: WorkerConfig,
    /// Status backing store
    status_store: Arc<Mutex<MemoryStatusBackingStore>>,
    /// Config backing store
    config_store: Arc<Mutex<MemoryConfigBackingStore>>,
    /// Connectors managed by this cluster
    connectors: Arc<Mutex<HashMap<String, ConnectorInfo>>>,
    /// Tasks managed by this cluster
    tasks: Arc<Mutex<HashMap<ConnectorTaskId, TaskInfo>>>,
    /// Ready state
    ready: Arc<Mutex<bool>>,
    /// Running state
    running: Arc<Mutex<bool>>,
    /// Worker ID
    worker_id: String,
}

/// Worker config type alias
pub type WorkerConfig = crate::config::WorkerConfig;

impl EmbeddedConnectCluster {
    /// Create a new embedded connect cluster
    pub fn new() -> Self {
        Self {
            config: WorkerConfig::new("embedded-worker".to_string()),
            status_store: Arc::new(Mutex::new(MemoryStatusBackingStore::new())),
            config_store: Arc::new(Mutex::new(MemoryConfigBackingStore::new())),
            connectors: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            ready: Arc::new(Mutex::new(false)),
            running: Arc::new(Mutex::new(false)),
            worker_id: "embedded-worker".to_string(),
        }
    }

    /// Create a new embedded connect cluster with a specific worker ID
    pub fn with_worker_id(worker_id: String) -> Self {
        Self {
            config: WorkerConfig::new(worker_id.clone()),
            status_store: Arc::new(Mutex::new(MemoryStatusBackingStore::new())),
            config_store: Arc::new(Mutex::new(MemoryConfigBackingStore::new())),
            connectors: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            ready: Arc::new(Mutex::new(false)),
            running: Arc::new(Mutex::new(false)),
            worker_id,
        }
    }

    /// Get status backing store
    pub fn status_store(&self) -> Arc<Mutex<MemoryStatusBackingStore>> {
        Arc::clone(&self.status_store)
    }

    /// Get config backing store
    pub fn config_store(&self) -> Arc<Mutex<MemoryConfigBackingStore>> {
        Arc::clone(&self.config_store)
    }

    /// Get worker ID
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Start cluster
    pub fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if *running {
            return Err(Box::new(ConnectRuntimeError::herder_error(
                "EmbeddedConnectCluster is already running".to_string(),
            )) as Box<dyn Error>);
        }

        // Start status store
        {
            let mut store = self.status_store.lock().map_err(|e| {
                Box::new(ConnectRuntimeError::herder_error(format!(
                    "Failed to acquire status store lock: {}",
                    e
                ))) as Box<dyn Error>
            })?;
            store.start()?;
        }

        // Start config store
        {
            let mut store = self.config_store.lock().map_err(|e| {
                Box::new(ConnectRuntimeError::herder_error(format!(
                    "Failed to acquire config store lock: {}",
                    e
                ))) as Box<dyn Error>
            })?;
            store.start()?;
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

    /// Stop cluster
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if !*running {
            return Err(Box::new(ConnectRuntimeError::herder_error(
                "EmbeddedConnectCluster is not running".to_string(),
            )) as Box<dyn Error>);
        }

        // Stop status store
        {
            let mut store = self.status_store.lock().map_err(|e| {
                Box::new(ConnectRuntimeError::herder_error(format!(
                    "Failed to acquire status store lock: {}",
                    e
                ))) as Box<dyn Error>
            })?;
            store.stop()?;
        }

        // Stop config store
        {
            let mut store = self.config_store.lock().map_err(|e| {
                Box::new(ConnectRuntimeError::herder_error(format!(
                    "Failed to acquire config store lock: {}",
                    e
                ))) as Box<dyn Error>
            })?;
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

    /// Check if cluster is ready
    pub fn is_ready(&self) -> bool {
        let ready = self.ready.lock();
        if let Ok(ready) = ready {
            *ready
        } else {
            false
        }
    }

    /// Check if cluster is running
    pub fn is_running(&self) -> bool {
        let running = self.running.lock();
        if let Ok(running) = running {
            *running
        } else {
            false
        }
    }

    /// Add a connector to cluster
    pub fn add_connector(
        &self,
        name: String,
        config: HashMap<String, String>,
    ) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if connectors.contains_key(&name) {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Connector '{}' already exists",
                name
            ))) as Box<dyn Error>);
        }

        let info = ConnectorInfo {
            name: name.clone(),
            config,
            tasks: Vec::new(),
        };

        connectors.insert(name, info);
        Ok(())
    }

    /// Remove a connector from cluster
    pub fn remove_connector(&self, name: String) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.connectors.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire connectors lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if !connectors.contains_key(&name) {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Connector '{}' not found",
                name
            ))) as Box<dyn Error>);
        }

        connectors.remove(&name);
        Ok(())
    }

    /// Get all connector names
    pub fn connector_names(&self) -> Vec<String> {
        let connectors = self.connectors.lock();
        if let Err(_) = connectors {
            return Vec::new();
        }

        let connectors = connectors.unwrap();
        connectors.keys().cloned().collect()
    }

    /// Get connector info
    pub fn get_connector_info(&self, name: String) -> Option<ConnectorInfo> {
        let connectors = self.connectors.lock();
        if let Err(_) = connectors {
            return None;
        }

        let connectors = connectors.unwrap();
        connectors.get(&name).cloned()
    }

    /// Add a task to cluster
    pub fn add_task(
        &self,
        id: ConnectorTaskId,
        config: HashMap<String, String>,
    ) -> Result<(), Box<dyn Error>> {
        let mut tasks = self.tasks.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire tasks lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if tasks.contains_key(&id) {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Task '{:?}' already exists",
                id
            ))) as Box<dyn Error>);
        }

        let info = TaskInfo {
            id: id.clone(),
            config,
        };
        tasks.insert(id, info);
        Ok(())
    }

    /// Remove a task from cluster
    pub fn remove_task(&self, id: ConnectorTaskId) -> Result<(), Box<dyn Error>> {
        let mut tasks = self.tasks.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire tasks lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if !tasks.contains_key(&id) {
            return Err(Box::new(ConnectRuntimeError::herder_error(format!(
                "Task '{:?}' not found",
                id
            ))) as Box<dyn Error>);
        }

        tasks.remove(&id);
        Ok(())
    }

    /// Get all tasks
    pub fn get_tasks(&self) -> Vec<TaskInfo> {
        let tasks = self.tasks.lock();
        if let Err(_) = tasks {
            return Vec::new();
        }

        let tasks = tasks.unwrap();
        tasks.values().cloned().collect()
    }

    /// Get tasks for a specific connector
    pub fn get_connector_tasks(&self, connector: String) -> Vec<TaskInfo> {
        let tasks = self.tasks.lock();
        if let Err(_) = tasks {
            return Vec::new();
        }

        let tasks = tasks.unwrap();
        tasks
            .values()
            .filter(|task| task.id.connector == connector)
            .cloned()
            .collect()
    }

    /// Clear all connectors and tasks
    pub fn clear(&self) {
        let connectors = self.connectors.lock();
        if let Ok(mut connectors) = connectors {
            connectors.clear();
        }

        let tasks = self.tasks.lock();
        if let Ok(mut tasks) = tasks {
            tasks.clear();
        }
    }
}

impl Herder for EmbeddedConnectCluster {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        EmbeddedConnectCluster::start(self)
    }

    fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        EmbeddedConnectCluster::stop(self)
    }

    fn is_ready(&self) -> bool {
        EmbeddedConnectCluster::is_ready(self)
    }

    fn health_check(&self, callback: Box<dyn Callback<()>>) -> Result<(), Box<dyn Error>> {
        callback.call(());
        Ok(())
    }

    fn connectors(&self, callback: Box<dyn Callback<Vec<String>>>) -> Result<(), Box<dyn Error>> {
        let names = self.connector_names();
        callback.call(names);
        Ok(())
    }

    fn connector_info(
        &self,
        conn_name: String,
        callback: Box<dyn Callback<ConnectorInfo>>,
    ) -> Result<(), Box<dyn Error>> {
        if let Some(info) = self.get_connector_info(conn_name.clone()) {
            callback.call(info);
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
        if let Some(info) = self.get_connector_info(conn_name) {
            callback.call(info.config);
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
        target_state: WorkerTargetState,
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

        // Update config store with target state
        let mut config_store = self.config_store.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire config store lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        // Convert WorkerTargetState to StorageTargetState
        let storage_target_state = match target_state {
            WorkerTargetState::Started => StorageTargetState::Started,
            WorkerTargetState::Stopped => StorageTargetState::Stopped,
            WorkerTargetState::Paused => StorageTargetState::Paused,
        };

        config_store.put_connector_config(
            info.name.clone(),
            info.config.clone(),
            storage_target_state,
        )?;

        callback.call(Created {
            created,
            value: info,
        });
        Ok(())
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
        let tasks = self.get_connector_tasks(conn_name);
        callback.call(tasks);
        Ok(())
    }

    fn put_task_configs(
        &self,
        conn_name: String,
        configs: Vec<HashMap<String, String>>,
        callback: Box<dyn Callback<()>>,
        _request_signature: InternalRequestSignature,
    ) -> Result<(), Box<dyn Error>> {
        let mut tasks = self.tasks.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::herder_error(format!(
                "Failed to acquire tasks lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        // Remove existing tasks for this connector
        tasks.retain(|id, _| id.connector != conn_name);

        // Add new tasks
        for (i, config) in configs.into_iter().enumerate() {
            let id = ConnectorTaskId {
                connector: conn_name.clone(),
                task: i as i32,
            };
            let info = TaskInfo {
                id: id.clone(),
                config,
            };
            tasks.insert(id, info);
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
        self.connector_names()
    }

    fn connector_info_sync(&self, conn_name: String) -> Option<ConnectorInfo> {
        self.get_connector_info(conn_name)
    }

    fn connector_status(&self, conn_name: String) -> Option<ConnectorState> {
        if self.get_connector_info(conn_name).is_some() {
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
        // Note: This is a simplified implementation
        // In real code, we would need to clone the store
        panic!("Cannot clone StatusBackingStore");
    }

    fn task_status(&self, _id: ConnectorTaskId) -> Option<HerderTaskState> {
        Some(HerderTaskState::Running)
    }

    fn validate_connector_config(
        &self,
        _connector_config: HashMap<String, String>,
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
        Box::new(EmbeddedPlugins)
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
        Box::new(EmbeddedConnectMetrics)
    }
}

/// Embedded plugins implementation
struct EmbeddedPlugins;

impl Plugins for EmbeddedPlugins {
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

/// Embedded connect metrics implementation
struct EmbeddedConnectMetrics;

impl ConnectMetrics for EmbeddedConnectMetrics {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[test]
    fn test_herder_trait_implementation() {
        let mut cluster = EmbeddedConnectCluster::new();
        assert!(cluster.start().is_ok());

        // Test is_ready
        assert!(cluster.is_ready());

        // Test connectors_sync
        assert_eq!(cluster.connectors_sync().len(), 0);

        // Test connector_status
        assert!(cluster
            .connector_status("nonexistent".to_string())
            .is_none());

        assert!(cluster.stop().is_ok());
    }
}
