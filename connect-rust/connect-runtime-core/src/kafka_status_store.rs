//! Kafka Status Backing Store
//!
//! Provides a Kafka-based implementation of the status backing store,
//! using Kafka topics to persist connector and task status information.

use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::errors::ConnectRuntimeError;
use crate::storage::{
    ConnectorState, ConnectorStatus, ConnectorTaskId, StatusBackingStore, TaskState, TaskStatus,
    TopicState, TopicStatus,
};

use connect_api::Closeable;

/// Key prefix for connector status records
const CONNECTOR_STATUS_PREFIX: &str = "connector:";

/// Key prefix for task status records
const TASK_STATUS_PREFIX: &str = "task:";

/// Key prefix for topic status records
const TOPIC_STATUS_PREFIX: &str = "topic:";

/// Default timeout for Kafka operations
const DEFAULT_TIMEOUT_MS: u64 = 30000;

/// Kafka-based status backing store
///
/// This implementation uses a Kafka topic to persist connector and task status information.
/// It provides distributed state management for Kafka Connect runtime.
pub struct KafkaStatusBackingStore {
    /// Kafka topic name for storing status data
    topic: String,
    /// Kafka bootstrap servers
    bootstrap_servers: String,
    /// Operation timeout
    timeout: Duration,
    /// Running state
    running: Arc<Mutex<bool>>,
    /// Local cache for connector statuses
    connector_cache: Arc<Mutex<HashMap<String, ConnectorStatus>>>,
    /// Local cache for task statuses
    task_cache: Arc<Mutex<HashMap<ConnectorTaskId, TaskStatus>>>,
    /// Local cache for topic statuses
    topic_cache: Arc<Mutex<HashMap<(String, String), TopicStatus>>>,
}

impl KafkaStatusBackingStore {
    /// Create a new Kafka status backing store
    ///
    /// # Arguments
    /// * `topic` - Kafka topic name for storing status data
    /// * `bootstrap_servers` - Kafka bootstrap servers (e.g., "localhost:9092")
    ///
    /// # Returns
    /// A new instance of KafkaStatusBackingStore
    pub fn new(topic: String, bootstrap_servers: String) -> Self {
        Self {
            topic,
            bootstrap_servers,
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            running: Arc::new(Mutex::new(false)),
            connector_cache: Arc::new(Mutex::new(HashMap::new())),
            task_cache: Arc::new(Mutex::new(HashMap::new())),
            topic_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new Kafka status backing store with custom timeout
    ///
    /// # Arguments
    /// * `topic` - Kafka topic name for storing status data
    /// * `bootstrap_servers` - Kafka bootstrap servers
    /// * `timeout` - Operation timeout duration
    ///
    /// # Returns
    /// A new instance of KafkaStatusBackingStore with custom timeout
    pub fn with_timeout(topic: String, bootstrap_servers: String, timeout: Duration) -> Self {
        Self {
            topic,
            bootstrap_servers,
            timeout,
            running: Arc::new(Mutex::new(false)),
            connector_cache: Arc::new(Mutex::new(HashMap::new())),
            task_cache: Arc::new(Mutex::new(HashMap::new())),
            topic_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Generate a key for connector status
    fn connector_key(&self, connector_name: &str) -> String {
        format!("{}{}", CONNECTOR_STATUS_PREFIX, connector_name)
    }

    /// Generate a key for task status
    fn task_key(&self, task_id: &ConnectorTaskId) -> String {
        format!(
            "{}{}:{}",
            TASK_STATUS_PREFIX, task_id.connector, task_id.task
        )
    }

    /// Generate a key for topic status
    fn topic_key(&self, connector: &str, topic: &str) -> String {
        format!("{}{}:{}", TOPIC_STATUS_PREFIX, connector, topic)
    }

    /// Parse connector name from key
    fn parse_connector_key(&self, key: &str) -> Option<String> {
        if key.starts_with(CONNECTOR_STATUS_PREFIX) {
            Some(key[CONNECTOR_STATUS_PREFIX.len()..].to_string())
        } else {
            None
        }
    }

    /// Parse task ID from key
    fn parse_task_key(&self, key: &str) -> Option<ConnectorTaskId> {
        if key.starts_with(TASK_STATUS_PREFIX) {
            let rest = &key[TASK_STATUS_PREFIX.len()..];
            if let Some((connector, task_str)) = rest.split_once(':') {
                if let Ok(task) = task_str.parse::<i32>() {
                    return Some(ConnectorTaskId {
                        connector: connector.to_string(),
                        task,
                    });
                }
            }
        }
        None
    }

    /// Serialize connector status to bytes
    fn serialize_connector_status(
        &self,
        status: &ConnectorStatus,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut data = Vec::new();

        // Serialize name
        let name_bytes = status.name.as_bytes();
        data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(name_bytes);

        // Serialize state
        data.push(status.state as u8);

        // Serialize trace
        let trace_bytes = status.trace.as_bytes();
        data.extend_from_slice(&(trace_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(trace_bytes);

        // Serialize worker_id
        let worker_id_bytes = status.worker_id.as_bytes();
        data.extend_from_slice(&(worker_id_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(worker_id_bytes);

        Ok(data)
    }

    /// Deserialize connector status from bytes
    fn deserialize_connector_status(&self, data: &[u8]) -> Result<ConnectorStatus, Box<dyn Error>> {
        let mut offset = 0;

        // Deserialize name
        if offset + 4 > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid connector status data: missing name length".to_string(),
            )) as Box<dyn Error>);
        }
        let name_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + name_len > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid connector status data: missing name".to_string(),
            )) as Box<dyn Error>);
        }
        let name = String::from_utf8(data[offset..offset + name_len].to_vec()).map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Invalid UTF-8 in name: {}",
                e
            ))) as Box<dyn Error>
        })?;
        offset += name_len;

        // Deserialize state
        if offset >= data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid connector status data: missing state".to_string(),
            )) as Box<dyn Error>);
        }
        let state = data[offset];
        offset += 1;

        // Deserialize trace
        if offset + 4 > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid connector status data: missing trace length".to_string(),
            )) as Box<dyn Error>);
        }
        let trace_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + trace_len > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid connector status data: missing trace".to_string(),
            )) as Box<dyn Error>);
        }
        let trace = String::from_utf8(data[offset..offset + trace_len].to_vec()).map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Invalid UTF-8 in trace: {}",
                e
            ))) as Box<dyn Error>
        })?;
        offset += trace_len;

        // Deserialize worker_id
        if offset + 4 > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid connector status data: missing worker_id length".to_string(),
            )) as Box<dyn Error>);
        }
        let worker_id_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + worker_id_len > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid connector status data: missing worker_id".to_string(),
            )) as Box<dyn Error>);
        }
        let worker_id =
            String::from_utf8(data[offset..offset + worker_id_len].to_vec()).map_err(|e| {
                Box::new(ConnectRuntimeError::storage_error(format!(
                    "Invalid UTF-8 in worker_id: {}",
                    e
                ))) as Box<dyn Error>
            })?;

        Ok(ConnectorStatus {
            name,
            state: match state {
                0 => ConnectorState::Uninitialized,
                1 => ConnectorState::Running,
                2 => ConnectorState::Paused,
                3 => ConnectorState::Stopped,
                4 => ConnectorState::Failed,
                5 => ConnectorState::Destroyed,
                _ => ConnectorState::Uninitialized,
            },
            trace,
            worker_id,
        })
    }

    /// Serialize task status to bytes
    fn serialize_task_status(&self, status: &TaskStatus) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut data = Vec::new();

        // Serialize connector name
        let connector_bytes = status.id.connector.as_bytes();
        data.extend_from_slice(&(connector_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(connector_bytes);

        // Serialize task number
        data.extend_from_slice(&status.id.task.to_be_bytes());

        // Serialize state
        data.push(status.state as u8);

        // Serialize trace
        let trace_bytes = status.trace.as_bytes();
        data.extend_from_slice(&(trace_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(trace_bytes);

        // Serialize worker_id
        let worker_id_bytes = status.worker_id.as_bytes();
        data.extend_from_slice(&(worker_id_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(worker_id_bytes);

        Ok(data)
    }

    /// Deserialize task status from bytes
    fn deserialize_task_status(&self, data: &[u8]) -> Result<TaskStatus, Box<dyn Error>> {
        let mut offset = 0;

        // Deserialize connector name
        if offset + 4 > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid task status data: missing connector length".to_string(),
            )) as Box<dyn Error>);
        }
        let connector_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + connector_len > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid task status data: missing connector".to_string(),
            )) as Box<dyn Error>);
        }
        let connector =
            String::from_utf8(data[offset..offset + connector_len].to_vec()).map_err(|e| {
                Box::new(ConnectRuntimeError::storage_error(format!(
                    "Invalid UTF-8 in connector: {}",
                    e
                ))) as Box<dyn Error>
            })?;
        offset += connector_len;

        // Deserialize task number
        if offset + 4 > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid task status data: missing task number".to_string(),
            )) as Box<dyn Error>);
        }
        let task = i32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        // Deserialize state
        if offset >= data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid task status data: missing state".to_string(),
            )) as Box<dyn Error>);
        }
        let state = data[offset];
        offset += 1;

        // Deserialize trace
        if offset + 4 > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid task status data: missing trace length".to_string(),
            )) as Box<dyn Error>);
        }
        let trace_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + trace_len > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid task status data: missing trace".to_string(),
            )) as Box<dyn Error>);
        }
        let trace = String::from_utf8(data[offset..offset + trace_len].to_vec()).map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Invalid UTF-8 in trace: {}",
                e
            ))) as Box<dyn Error>
        })?;
        offset += trace_len;

        // Deserialize worker_id
        if offset + 4 > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid task status data: missing worker_id length".to_string(),
            )) as Box<dyn Error>);
        }
        let worker_id_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + worker_id_len > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid task status data: missing worker_id".to_string(),
            )) as Box<dyn Error>);
        }
        let worker_id =
            String::from_utf8(data[offset..offset + worker_id_len].to_vec()).map_err(|e| {
                Box::new(ConnectRuntimeError::storage_error(format!(
                    "Invalid UTF-8 in worker_id: {}",
                    e
                ))) as Box<dyn Error>
            })?;

        Ok(TaskStatus {
            id: ConnectorTaskId { connector, task },
            state: match state {
                0 => TaskState::Uninitialized,
                1 => TaskState::Running,
                2 => TaskState::Paused,
                3 => TaskState::Stopped,
                4 => TaskState::Failed,
                5 => TaskState::Destroyed,
                _ => TaskState::Uninitialized,
            },
            trace,
            worker_id,
        })
    }

    /// Serialize topic status to bytes
    fn serialize_topic_status(&self, status: &TopicStatus) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut data = Vec::new();

        // Serialize connector
        let connector_bytes = status.connector.as_bytes();
        data.extend_from_slice(&(connector_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(connector_bytes);

        // Serialize topic
        let topic_bytes = status.topic.as_bytes();
        data.extend_from_slice(&(topic_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(topic_bytes);

        // Serialize state
        data.push(status.state as u8);

        // Serialize partition count
        data.extend_from_slice(&status.partition_count.to_be_bytes());

        Ok(data)
    }

    /// Deserialize topic status from bytes
    fn deserialize_topic_status(&self, data: &[u8]) -> Result<TopicStatus, Box<dyn Error>> {
        let mut offset = 0;

        // Deserialize connector
        if offset + 4 > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid topic status data: missing connector length".to_string(),
            )) as Box<dyn Error>);
        }
        let connector_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + connector_len > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid topic status data: missing connector".to_string(),
            )) as Box<dyn Error>);
        }
        let connector =
            String::from_utf8(data[offset..offset + connector_len].to_vec()).map_err(|e| {
                Box::new(ConnectRuntimeError::storage_error(format!(
                    "Invalid UTF-8 in connector: {}",
                    e
                ))) as Box<dyn Error>
            })?;
        offset += connector_len;

        // Deserialize topic
        if offset + 4 > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid topic status data: missing topic length".to_string(),
            )) as Box<dyn Error>);
        }
        let topic_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + topic_len > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid topic status data: missing topic".to_string(),
            )) as Box<dyn Error>);
        }
        let topic = String::from_utf8(data[offset..offset + topic_len].to_vec()).map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Invalid UTF-8 in topic: {}",
                e
            ))) as Box<dyn Error>
        })?;
        offset += topic_len;

        // Deserialize state
        if offset >= data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid topic status data: missing state".to_string(),
            )) as Box<dyn Error>);
        }
        let state = data[offset];
        offset += 1;

        // Deserialize partition count
        if offset + 4 > data.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid topic status data: missing partition count".to_string(),
            )) as Box<dyn Error>);
        }
        let partition_count = i32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);

        Ok(TopicStatus {
            connector,
            topic,
            state: match state {
                0 => TopicState::Active,
                1 => TopicState::Inactive,
                2 => TopicState::Failed,
                _ => TopicState::Active,
            },
            partition_count,
        })
    }
}

impl StatusBackingStore for KafkaStatusBackingStore {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if *running {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "KafkaStatusBackingStore is already running".to_string(),
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
                "KafkaStatusBackingStore is not running".to_string(),
            )) as Box<dyn Error>);
        }

        *running = false;
        Ok(())
    }

    fn put(&mut self, status: ConnectorStatus) -> Result<(), Box<dyn Error>> {
        // Update local cache
        let mut connector_cache = self.connector_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire connector cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        connector_cache.insert(status.name.clone(), status.clone());
        drop(connector_cache);

        Ok(())
    }

    fn put_safe(&mut self, status: ConnectorStatus) -> Result<(), Box<dyn Error>> {
        self.put(status)
    }

    fn get(&self, connector: String) -> Option<ConnectorStatus> {
        // Check local cache
        let connector_cache = self.connector_cache.lock();
        if let Ok(cache) = connector_cache {
            if let Some(status) = cache.get(&connector) {
                return Some(status.clone());
            }
        }

        None
    }

    fn get_all(&self, connector: String) -> Vec<TaskStatus> {
        let task_cache = self.task_cache.lock();
        if let Err(e) = task_cache {
            return Vec::new();
        }

        let task_cache = task_cache.unwrap();
        task_cache
            .values()
            .filter(|status| status.id.connector == connector)
            .cloned()
            .collect()
    }

    fn put_task(&mut self, status: TaskStatus) -> Result<(), Box<dyn Error>> {
        // Update local cache
        let mut task_cache = self.task_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire task cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        task_cache.insert(status.id.clone(), status.clone());
        drop(task_cache);

        Ok(())
    }

    fn put_task_safe(&mut self, status: TaskStatus) -> Result<(), Box<dyn Error>> {
        self.put_task(status)
    }

    fn get_task(&self, id: ConnectorTaskId) -> Option<TaskStatus> {
        // Check local cache
        let task_cache = self.task_cache.lock();
        if let Ok(cache) = task_cache {
            if let Some(status) = cache.get(&id) {
                return Some(status.clone());
            }
        }

        None
    }

    fn put_topic(&mut self, status: TopicStatus) -> Result<(), Box<dyn Error>> {
        // Update local cache
        let mut topic_cache = self.topic_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire topic cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        topic_cache.insert(
            (status.connector.clone(), status.topic.clone()),
            status.clone(),
        );
        drop(topic_cache);

        Ok(())
    }

    fn get_topic(&self, connector: String, topic: String) -> Option<TopicStatus> {
        // Check local cache
        let topic_cache = self.topic_cache.lock();
        if let Ok(cache) = topic_cache {
            let key = (connector, topic);
            if let Some(status) = cache.get(&key) {
                return Some(status.clone());
            }
        }

        None
    }

    fn get_all_topics(&self, connector: String) -> Vec<TopicStatus> {
        let topic_cache = self.topic_cache.lock();
        if let Err(e) = topic_cache {
            return Vec::new();
        }

        let topic_cache = topic_cache.unwrap();
        topic_cache
            .values()
            .filter(|status| status.connector == connector)
            .cloned()
            .collect()
    }

    fn delete_topic(&mut self, connector: String, topic: String) -> Result<(), Box<dyn Error>> {
        // Remove from local cache
        let mut topic_cache = self.topic_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire topic cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        topic_cache.remove(&(connector.clone(), topic.clone()));

        Ok(())
    }
}

/// Closeable implementation for KafkaStatusBackingStore
impl Closeable for KafkaStatusBackingStore {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        // Stop
        self.stop()?;

        // Clear caches
        let mut connector_cache = self.connector_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire connector cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        connector_cache.clear();

        let mut task_cache = self.task_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire task cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        task_cache.clear();

        let mut topic_cache = self.topic_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire topic cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        topic_cache.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{ConnectorState, TaskState, TopicState};

    #[test]
    fn test_kafka_status_store_creation() {
        let store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        assert_eq!(store.topic, "connect-status");
        assert_eq!(store.bootstrap_servers, "localhost:9092");
    }

    #[test]
    fn test_kafka_status_store_key_generation() {
        let store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        let connector_key = store.connector_key("test-connector");
        assert_eq!(connector_key, "connector:test-connector");

        let task_id = ConnectorTaskId {
            connector: "test-connector".to_string(),
            task: 0,
        };
        let task_key = store.task_key(&task_id);
        assert_eq!(task_key, "task:test-connector:0");

        let topic_key = store.topic_key("test-connector", "test-topic");
        assert_eq!(topic_key, "topic:test-connector:test-topic");
    }

    #[test]
    fn test_kafka_status_store_connector_serialization() {
        let store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        let status = ConnectorStatus {
            name: "test-connector".to_string(),
            state: ConnectorState::Running,
            trace: "Test trace".to_string(),
            worker_id: "worker-1".to_string(),
        };

        let serialized = store.serialize_connector_status(&status).unwrap();
        let deserialized = store.deserialize_connector_status(&serialized).unwrap();

        assert_eq!(deserialized.name, status.name);
        assert_eq!(deserialized.state, status.state);
        assert_eq!(deserialized.trace, status.trace);
        assert_eq!(deserialized.worker_id, status.worker_id);
    }

    #[test]
    fn test_kafka_status_store_task_serialization() {
        let store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        let status = TaskStatus {
            id: ConnectorTaskId {
                connector: "test-connector".to_string(),
                task: 0,
            },
            state: TaskState::Running,
            trace: "Task trace".to_string(),
            worker_id: "worker-1".to_string(),
        };

        let serialized = store.serialize_task_status(&status).unwrap();
        let deserialized = store.deserialize_task_status(&serialized).unwrap();

        assert_eq!(deserialized.id.connector, status.id.connector);
        assert_eq!(deserialized.id.task, status.id.task);
        assert_eq!(deserialized.state, status.state);
        assert_eq!(deserialized.trace, status.trace);
        assert_eq!(deserialized.worker_id, status.worker_id);
    }

    #[test]
    fn test_kafka_status_store_topic_serialization() {
        let store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        let status = TopicStatus {
            connector: "test-connector".to_string(),
            topic: "test-topic".to_string(),
            state: TopicState::Active,
            partition_count: 3,
        };

        let serialized = store.serialize_topic_status(&status).unwrap();
        let deserialized = store.deserialize_topic_status(&serialized).unwrap();

        assert_eq!(deserialized.connector, status.connector);
        assert_eq!(deserialized.topic, status.topic);
        assert_eq!(deserialized.state, status.state);
        assert_eq!(deserialized.partition_count, status.partition_count);
    }

    #[test]
    fn test_kafka_status_store_start_stop() {
        let mut store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());
        assert!(store.stop().is_ok());
    }

    #[test]
    fn test_kafka_status_store_double_start() {
        let mut store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());
        assert!(store.start().is_err());
    }

    #[test]
    fn test_kafka_status_store_stop_without_start() {
        let mut store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.stop().is_err());
    }

    #[test]
    fn test_kafka_status_store_put_and_get_connector() {
        let mut store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        let status = ConnectorStatus {
            name: "test-connector".to_string(),
            state: ConnectorState::Running,
            trace: "Test trace".to_string(),
            worker_id: "worker-1".to_string(),
        };

        assert!(store.start().is_ok());
        assert!(store.put(status.clone()).is_ok());

        let retrieved = store.get("test-connector".to_string());
        assert!(retrieved.is_some());

        let retrieved_status = retrieved.unwrap();
        assert_eq!(retrieved_status.name, status.name);
        assert_eq!(retrieved_status.state, status.state);
    }

    #[test]
    fn test_kafka_status_store_put_and_get_task() {
        let mut store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        let status = TaskStatus {
            id: ConnectorTaskId {
                connector: "test-connector".to_string(),
                task: 0,
            },
            state: TaskState::Running,
            trace: "Task trace".to_string(),
            worker_id: "worker-1".to_string(),
        };

        assert!(store.start().is_ok());
        assert!(store.put_task(status.clone()).is_ok());

        let retrieved = store.get_task(status.id.clone());
        assert!(retrieved.is_some());

        let retrieved_status = retrieved.unwrap();
        assert_eq!(retrieved_status.id.connector, status.id.connector);
        assert_eq!(retrieved_status.id.task, status.id.task);
        assert_eq!(retrieved_status.state, status.state);
    }

    #[test]
    fn test_kafka_status_store_put_and_get_topic() {
        let mut store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        let status = TopicStatus {
            connector: "test-connector".to_string(),
            topic: "test-topic".to_string(),
            state: TopicState::Active,
            partition_count: 3,
        };

        assert!(store.start().is_ok());
        assert!(store.put_topic(status.clone()).is_ok());

        let retrieved = store.get_topic("test-connector".to_string(), "test-topic".to_string());
        assert!(retrieved.is_some());

        let retrieved_status = retrieved.unwrap();
        assert_eq!(retrieved_status.connector, status.connector);
        assert_eq!(retrieved_status.topic, status.topic);
        assert_eq!(retrieved_status.state, status.state);
    }

    #[test]
    fn test_kafka_status_store_delete_topic() {
        let mut store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        let status = TopicStatus {
            connector: "test-connector".to_string(),
            topic: "test-topic".to_string(),
            state: TopicState::Active,
            partition_count: 3,
        };

        assert!(store.start().is_ok());
        assert!(store.put_topic(status.clone()).is_ok());

        let retrieved = store.get_topic("test-connector".to_string(), "test-topic".to_string());
        assert!(retrieved.is_some());

        assert!(store
            .delete_topic("test-connector".to_string(), "test-topic".to_string())
            .is_ok());

        let retrieved_after_delete =
            store.get_topic("test-connector".to_string(), "test-topic".to_string());
        assert!(retrieved_after_delete.is_none());
    }

    #[test]
    fn test_kafka_status_store_get_all_tasks() {
        let mut store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        let task1 = TaskStatus {
            id: ConnectorTaskId {
                connector: "test-connector".to_string(),
                task: 0,
            },
            state: TaskState::Running,
            trace: "Task 1 trace".to_string(),
            worker_id: "worker-1".to_string(),
        };

        let task2 = TaskStatus {
            id: ConnectorTaskId {
                connector: "test-connector".to_string(),
                task: 1,
            },
            state: TaskState::Running,
            trace: "Task 2 trace".to_string(),
            worker_id: "worker-1".to_string(),
        };

        assert!(store.start().is_ok());
        assert!(store.put_task(task1.clone()).is_ok());
        assert!(store.put_task(task2.clone()).is_ok());

        let all_tasks = store.get_all("test-connector".to_string());
        assert_eq!(all_tasks.len(), 2);
    }

    #[test]
    fn test_kafka_status_store_get_all_topics() {
        let mut store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        let topic1 = TopicStatus {
            connector: "test-connector".to_string(),
            topic: "test-topic-1".to_string(),
            state: TopicState::Active,
            partition_count: 3,
        };

        let topic2 = TopicStatus {
            connector: "test-connector".to_string(),
            topic: "test-topic-2".to_string(),
            state: TopicState::Active,
            partition_count: 5,
        };

        assert!(store.start().is_ok());
        assert!(store.put_topic(topic1.clone()).is_ok());
        assert!(store.put_topic(topic2.clone()).is_ok());

        let all_topics = store.get_all_topics("test-connector".to_string());
        assert_eq!(all_topics.len(), 2);
    }

    #[test]
    fn test_kafka_status_store_close() {
        let mut store = KafkaStatusBackingStore::new(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());
        assert!(store.close().is_ok());
    }

    #[test]
    fn test_kafka_status_store_with_timeout() {
        let store = KafkaStatusBackingStore::with_timeout(
            "connect-status".to_string(),
            "localhost:9092".to_string(),
            Duration::from_secs(60),
        );

        assert_eq!(store.timeout, Duration::from_secs(60));
    }
}
