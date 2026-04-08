//! Storage module
//!
//! Provides storage traits for status, config, and offset data.

use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};

use connect_api::TopicPartition;

use crate::ConnectRuntimeError;

/// Status backing store trait
pub trait StatusBackingStore {
    /// Start the store
    fn start(&mut self) -> Result<(), Box<dyn Error>>;

    /// Stop the store
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    /// Put connector status
    fn put(&mut self, status: ConnectorStatus) -> Result<(), Box<dyn Error>>;

    /// Put connector status safely
    fn put_safe(&mut self, status: ConnectorStatus) -> Result<(), Box<dyn Error>>;

    /// Get connector status
    fn get(&self, connector: String) -> Option<ConnectorStatus>;

    /// Get all task statuses for a connector
    fn get_all(&self, connector: String) -> Vec<TaskStatus>;

    /// Put task status
    fn put_task(&mut self, status: TaskStatus) -> Result<(), Box<dyn Error>>;

    /// Put task status safely
    fn put_task_safe(&mut self, status: TaskStatus) -> Result<(), Box<dyn Error>>;

    /// Get task status
    fn get_task(&self, id: ConnectorTaskId) -> Option<TaskStatus>;

    /// Put topic status
    fn put_topic(&mut self, status: TopicStatus) -> Result<(), Box<dyn Error>>;

    /// Get topic status
    fn get_topic(&self, connector: String, topic: String) -> Option<TopicStatus>;

    /// Get all topic statuses for a connector
    fn get_all_topics(&self, connector: String) -> Vec<TopicStatus>;

    /// Delete topic status
    fn delete_topic(&mut self, connector: String, topic: String) -> Result<(), Box<dyn Error>>;
}

/// Config backing store trait
pub trait ConfigBackingStore {
    /// Start the store
    fn start(&mut self) -> Result<(), Box<dyn Error>>;

    /// Stop the store
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    /// Get a snapshot of all connector configs
    fn snapshot(&self) -> HashMap<String, HashMap<String, String>>;

    /// Check if connector config exists
    fn contains(&self, connector: String) -> bool;

    /// Put connector config
    fn put_connector_config(
        &mut self,
        connector: String,
        properties: HashMap<String, String>,
        target_state: TargetState,
    ) -> Result<(), Box<dyn Error>>;

    /// Remove connector config
    fn remove_connector_config(&mut self, connector: String) -> Result<(), Box<dyn Error>>;

    /// Put task configs
    fn put_task_configs(
        &mut self,
        connector: String,
        configs: Vec<HashMap<String, String>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Remove task configs
    fn remove_task_configs(&mut self, connector: String) -> Result<(), Box<dyn Error>>;

    /// Put target state
    fn put_target_state(
        &mut self,
        connector: String,
        state: TargetState,
    ) -> Result<(), Box<dyn Error>>;

    /// Claim write privileges
    fn claim_write_privileges(&mut self) -> Result<(), Box<dyn Error>>;
}

/// Offset backing store trait
pub trait OffsetBackingStore {
    /// Start the store
    fn start(&mut self) -> Result<(), Box<dyn Error>>;

    /// Stop the store
    fn stop(&mut self) -> Result<(), Box<dyn Error>>;

    /// Configure the store
    fn configure(&mut self, configs: HashMap<String, Box<dyn Any>>) -> Result<(), Box<dyn Error>>;

    /// Get offset for a partition (async)
    fn get<T>(
        &self,
        partition: HashMap<String, T>,
        callback: Box<dyn Callback<HashMap<String, Box<dyn Any>>>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Put offsets (async)
    fn put(
        &mut self,
        offsets: HashMap<String, HashMap<String, Box<dyn Any>>>,
        callback: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>>;

    /// Get connector partitions
    fn connector_partitions(&self, connector: String) -> Vec<TopicPartition>;
}

/// Connector status
#[derive(Debug, Clone)]
pub struct ConnectorStatus {
    pub name: String,
    pub state: ConnectorState,
    pub trace: String,
    pub worker_id: String,
}

/// Task status
#[derive(Debug, Clone)]
pub struct TaskStatus {
    pub id: ConnectorTaskId,
    pub state: TaskState,
    pub trace: String,
    pub worker_id: String,
}

/// Topic status
#[derive(Debug, Clone)]
pub struct TopicStatus {
    pub connector: String,
    pub topic: String,
    pub state: TopicState,
    pub partition_count: i32,
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
    Uninitialized,
    Running,
    Paused,
    Stopped,
    Failed,
    Destroyed,
}

/// Topic state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicState {
    Active,
    Inactive,
    Failed,
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

/// Callback trait for async operations
pub trait Callback<T> {
    /// Called when operation completes
    fn call(&self, result: T);
}

/// Memory status backing store (for testing and standalone mode)
pub struct MemoryStatusBackingStore {
    /// Connector statuses
    connector_statuses: Arc<Mutex<HashMap<String, ConnectorStatus>>>,
    /// Task statuses
    task_statuses: Arc<Mutex<HashMap<ConnectorTaskId, TaskStatus>>>,
    /// Topic statuses
    topic_statuses: Arc<Mutex<HashMap<(String, String), TopicStatus>>>,
    /// Running state
    running: Arc<Mutex<bool>>,
}

impl MemoryStatusBackingStore {
    /// Create a new memory status backing store
    pub fn new() -> Self {
        Self {
            connector_statuses: Arc::new(Mutex::new(HashMap::new())),
            task_statuses: Arc::new(Mutex::new(HashMap::new())),
            topic_statuses: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(Mutex::new(false)),
        }
    }
}

impl StatusBackingStore for MemoryStatusBackingStore {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if *running {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "MemoryStatusBackingStore is already running".to_string(),
            )) as Box<dyn Error>);
        }

        *running = true;
        Ok(())
    }

    fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if !*running {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "MemoryStatusBackingStore is not running".to_string(),
            )) as Box<dyn Error>);
        }

        *running = false;
        Ok(())
    }

    fn put(&mut self, status: ConnectorStatus) -> Result<(), Box<dyn Error>> {
        let mut connector_statuses = self.connector_statuses.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire connector statuses lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        connector_statuses.insert(status.name.clone(), status);
        Ok(())
    }

    fn put_safe(&mut self, status: ConnectorStatus) -> Result<(), Box<dyn Error>> {
        self.put(status)
    }

    fn get(&self, connector: String) -> Option<ConnectorStatus> {
        let connector_statuses = self.connector_statuses.lock();
        if let Err(e) = connector_statuses {
            return None;
        }

        let connector_statuses = connector_statuses.unwrap();
        connector_statuses.get(&connector).cloned()
    }

    fn get_all(&self, connector: String) -> Vec<TaskStatus> {
        let task_statuses = self.task_statuses.lock();
        if let Err(e) = task_statuses {
            return Vec::new();
        }

        let task_statuses = task_statuses.unwrap();
        task_statuses
            .values()
            .filter(|status| status.id.connector == connector)
            .cloned()
            .collect()
    }

    fn put_task(&mut self, status: TaskStatus) -> Result<(), Box<dyn Error>> {
        let mut task_statuses = self.task_statuses.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire task statuses lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        task_statuses.insert(status.id.clone(), status);
        Ok(())
    }

    fn put_task_safe(&mut self, status: TaskStatus) -> Result<(), Box<dyn Error>> {
        self.put_task(status)
    }

    fn get_task(&self, id: ConnectorTaskId) -> Option<TaskStatus> {
        let task_statuses = self.task_statuses.lock();
        if let Err(e) = task_statuses {
            return None;
        }

        let task_statuses = task_statuses.unwrap();
        task_statuses.get(&id).cloned()
    }

    fn put_topic(&mut self, status: TopicStatus) -> Result<(), Box<dyn Error>> {
        let mut topic_statuses = self.topic_statuses.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire topic statuses lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        topic_statuses.insert((status.connector.clone(), status.topic.clone()), status);
        Ok(())
    }

    fn get_topic(&self, connector: String, topic: String) -> Option<TopicStatus> {
        let topic_statuses = self.topic_statuses.lock();
        if let Err(e) = topic_statuses {
            return None;
        }

        let topic_statuses = topic_statuses.unwrap();
        topic_statuses.get(&(connector, topic)).cloned()
    }

    fn get_all_topics(&self, connector: String) -> Vec<TopicStatus> {
        let topic_statuses = self.topic_statuses.lock();
        if let Err(e) = topic_statuses {
            return Vec::new();
        }

        let topic_statuses = topic_statuses.unwrap();
        topic_statuses
            .values()
            .filter(|status| status.connector == connector)
            .cloned()
            .collect()
    }

    fn delete_topic(&mut self, connector: String, topic: String) -> Result<(), Box<dyn Error>> {
        let mut topic_statuses = self.topic_statuses.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire topic statuses lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        topic_statuses.remove(&(connector, topic));
        Ok(())
    }
}

/// Memory config backing store (for testing and standalone mode)
pub struct MemoryConfigBackingStore {
    /// Connector configs
    connector_configs: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
    /// Task configs
    task_configs: Arc<Mutex<HashMap<String, Vec<HashMap<String, String>>>>>,
    /// Target states
    target_states: Arc<Mutex<HashMap<String, TargetState>>>,
    /// Running state
    running: Arc<Mutex<bool>>,
}

impl MemoryConfigBackingStore {
    /// Create a new memory config backing store
    pub fn new() -> Self {
        Self {
            connector_configs: Arc::new(Mutex::new(HashMap::new())),
            task_configs: Arc::new(Mutex::new(HashMap::new())),
            target_states: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(Mutex::new(false)),
        }
    }
}

impl ConfigBackingStore for MemoryConfigBackingStore {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if *running {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "MemoryConfigBackingStore is already running".to_string(),
            )) as Box<dyn Error>);
        }

        *running = true;
        Ok(())
    }

    fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if !*running {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "MemoryConfigBackingStore is not running".to_string(),
            )) as Box<dyn Error>);
        }

        *running = false;
        Ok(())
    }

    fn snapshot(&self) -> HashMap<String, HashMap<String, String>> {
        let connector_configs = self.connector_configs.lock();
        if let Err(e) = connector_configs {
            return HashMap::new();
        }

        let connector_configs = connector_configs.unwrap();
        connector_configs.clone()
    }

    fn contains(&self, connector: String) -> bool {
        let connector_configs = self.connector_configs.lock();
        if let Err(e) = connector_configs {
            return false;
        }

        let connector_configs = connector_configs.unwrap();
        connector_configs.contains_key(&connector)
    }

    fn put_connector_config(
        &mut self,
        connector: String,
        properties: HashMap<String, String>,
        target_state: TargetState,
    ) -> Result<(), Box<dyn Error>> {
        let mut connector_configs = self.connector_configs.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire connector configs lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        connector_configs.insert(connector.clone(), properties);

        let mut target_states = self.target_states.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire target states lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        target_states.insert(connector, target_state);
        Ok(())
    }

    fn remove_connector_config(&mut self, connector: String) -> Result<(), Box<dyn Error>> {
        let mut connector_configs = self.connector_configs.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire connector configs lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        connector_configs.remove(&connector);

        let mut target_states = self.target_states.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire target states lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        target_states.remove(&connector);
        Ok(())
    }

    fn put_task_configs(
        &mut self,
        connector: String,
        configs: Vec<HashMap<String, String>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut task_configs = self.task_configs.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire task configs lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        task_configs.insert(connector, configs);
        Ok(())
    }

    fn remove_task_configs(&mut self, connector: String) -> Result<(), Box<dyn Error>> {
        let mut task_configs = self.task_configs.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire task configs lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        task_configs.remove(&connector);
        Ok(())
    }

    fn put_target_state(
        &mut self,
        connector: String,
        state: TargetState,
    ) -> Result<(), Box<dyn Error>> {
        let mut target_states = self.target_states.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire target states lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        target_states.insert(connector, state);
        Ok(())
    }

    fn claim_write_privileges(&mut self) -> Result<(), Box<dyn Error>> {
        // In memory mode, no need to claim privileges
        Ok(())
    }
}

/// Memory offset backing store (for testing and standalone mode)
pub struct MemoryOffsetBackingStore {
    /// Offsets - using String as key instead of HashMap<String, Box<dyn Any>>
    offsets: Arc<Mutex<HashMap<String, HashMap<String, Box<dyn Any>>>>>,
    /// Running state
    running: Arc<Mutex<bool>>,
}

impl MemoryOffsetBackingStore {
    /// Create a new memory offset backing store
    pub fn new() -> Self {
        Self {
            offsets: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(Mutex::new(false)),
        }
    }
}

impl OffsetBackingStore for MemoryOffsetBackingStore {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if *running {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "MemoryOffsetBackingStore is already running".to_string(),
            )) as Box<dyn Error>);
        }

        *running = true;
        Ok(())
    }

    fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if !*running {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "MemoryOffsetBackingStore is not running".to_string(),
            )) as Box<dyn Error>);
        }

        *running = false;
        Ok(())
    }

    fn configure(&mut self, _configs: HashMap<String, Box<dyn Any>>) -> Result<(), Box<dyn Error>> {
        // No configuration needed for memory store
        Ok(())
    }

    fn get<T>(
        &self,
        partition: HashMap<String, T>,
        callback: Box<dyn Callback<HashMap<String, Box<dyn Any>>>>,
    ) -> Result<(), Box<dyn Error>> {
        let offsets = self.offsets.lock();
        if let Err(e) = offsets {
            return Err(Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire offsets lock: {}",
                e
            ))) as Box<dyn Error>);
        }

        let offsets = offsets.unwrap();
        // Note: This is a simplified implementation
        // In real code, we would need to convert partition to the correct key type
        let offset = HashMap::new();
        callback.call(offset);
        Ok(())
    }

    fn put(
        &mut self,
        offsets: HashMap<String, HashMap<String, Box<dyn Any>>>,
        callback: Box<dyn Callback<()>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut stored_offsets = self.offsets.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire offsets lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        for (key, value) in offsets {
            stored_offsets.insert(key, value);
        }

        callback.call(());
        Ok(())
    }

    fn connector_partitions(&self, _connector: String) -> Vec<TopicPartition> {
        Vec::new()
    }
}
