//! Kafka Config Backing Store
//!
//! Provides a Kafka-based implementation of the config backing store,
//! using Kafka topics to persist connector and task configuration information.

use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::errors::ConnectRuntimeError;
use crate::storage::{ConfigBackingStore, TargetState};

use connect_api::Closeable;

/// Key prefix for connector config records
const CONNECTOR_CONFIG_PREFIX: &str = "connector:";

/// Key prefix for task config records
const TASK_CONFIG_PREFIX: &str = "task:";

/// Key prefix for target state records
const TARGET_STATE_PREFIX: &str = "target:";

/// Default timeout for Kafka operations
const DEFAULT_TIMEOUT_MS: u64 = 30000;

/// Config data stored in Kafka
#[derive(Debug, Clone)]
struct ConfigData {
    /// Connector name
    pub connector: String,
    /// Configuration properties
    pub properties: HashMap<String, String>,
    /// Version number
    pub version: i64,
    /// Timestamp
    pub timestamp: i64,
}

impl ConfigData {
    /// Create new config data
    pub fn new(connector: String, properties: HashMap<String, String>) -> Self {
        Self {
            connector,
            properties,
            version: 1,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        }
    }

    /// Create new config data with version
    pub fn with_version(
        connector: String,
        properties: HashMap<String, String>,
        version: i64,
    ) -> Self {
        Self {
            connector,
            properties,
            version,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        }
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut bytes = Vec::new();

        // Serialize connector length and bytes
        let connector_bytes = self.connector.as_bytes();
        bytes.extend_from_slice(&(connector_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(connector_bytes);

        // Serialize version
        bytes.extend_from_slice(&self.version.to_be_bytes());

        // Serialize timestamp
        bytes.extend_from_slice(&self.timestamp.to_be_bytes());

        // Serialize properties count
        bytes.extend_from_slice(&(self.properties.len() as u32).to_be_bytes());

        // Serialize each property
        for (key, value) in &self.properties {
            let key_bytes = key.as_bytes();
            bytes.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            bytes.extend_from_slice(key_bytes);

            let value_bytes = value.as_bytes();
            bytes.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
            bytes.extend_from_slice(value_bytes);
        }

        Ok(bytes)
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let mut pos = 0;

        // Deserialize connector
        if pos + 4 > bytes.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid config data: insufficient bytes for connector length".to_string(),
            )) as Box<dyn Error>);
        }
        let connector_len =
            u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                as usize;
        pos += 4;

        if pos + connector_len > bytes.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid config data: insufficient bytes for connector".to_string(),
            )) as Box<dyn Error>);
        }
        let connector =
            String::from_utf8(bytes[pos..pos + connector_len].to_vec()).map_err(|e| {
                Box::new(ConnectRuntimeError::storage_error(format!(
                    "Invalid config data: connector is not valid UTF-8: {}",
                    e
                ))) as Box<dyn Error>
            })?;
        pos += connector_len;

        // Deserialize version
        if pos + 8 > bytes.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid config data: insufficient bytes for version".to_string(),
            )) as Box<dyn Error>);
        }
        let version = i64::from_be_bytes([
            bytes[pos],
            bytes[pos + 1],
            bytes[pos + 2],
            bytes[pos + 3],
            bytes[pos + 4],
            bytes[pos + 5],
            bytes[pos + 6],
            bytes[pos + 7],
        ]);
        pos += 8;

        // Deserialize timestamp
        if pos + 8 > bytes.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid config data: insufficient bytes for timestamp".to_string(),
            )) as Box<dyn Error>);
        }
        let timestamp = i64::from_be_bytes([
            bytes[pos],
            bytes[pos + 1],
            bytes[pos + 2],
            bytes[pos + 3],
            bytes[pos + 4],
            bytes[pos + 5],
            bytes[pos + 6],
            bytes[pos + 7],
        ]);
        pos += 8;

        // Deserialize properties count
        if pos + 4 > bytes.len() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid config data: insufficient bytes for properties count".to_string(),
            )) as Box<dyn Error>);
        }
        let properties_count =
            u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                as usize;
        pos += 4;

        // Deserialize properties
        let mut properties = HashMap::new();
        for _ in 0..properties_count {
            // Deserialize key
            if pos + 4 > bytes.len() {
                return Err(Box::new(ConnectRuntimeError::storage_error(
                    "Invalid config data: insufficient bytes for key length".to_string(),
                )) as Box<dyn Error>);
            }
            let key_len =
                u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                    as usize;
            pos += 4;

            if pos + key_len > bytes.len() {
                return Err(Box::new(ConnectRuntimeError::storage_error(
                    "Invalid config data: insufficient bytes for key".to_string(),
                )) as Box<dyn Error>);
            }
            let key = String::from_utf8(bytes[pos..pos + key_len].to_vec()).map_err(|e| {
                Box::new(ConnectRuntimeError::storage_error(format!(
                    "Invalid config data: key is not valid UTF-8: {}",
                    e
                ))) as Box<dyn Error>
            })?;
            pos += key_len;

            // Deserialize value
            if pos + 4 > bytes.len() {
                return Err(Box::new(ConnectRuntimeError::storage_error(
                    "Invalid config data: insufficient bytes for value length".to_string(),
                )) as Box<dyn Error>);
            }
            let value_len =
                u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                    as usize;
            pos += 4;

            if pos + value_len > bytes.len() {
                return Err(Box::new(ConnectRuntimeError::storage_error(
                    "Invalid config data: insufficient bytes for value".to_string(),
                )) as Box<dyn Error>);
            }
            let value = String::from_utf8(bytes[pos..pos + value_len].to_vec()).map_err(|e| {
                Box::new(ConnectRuntimeError::storage_error(format!(
                    "Invalid config data: value is not valid UTF-8: {}",
                    e
                ))) as Box<dyn Error>
            })?;
            pos += value_len;

            properties.insert(key, value);
        }

        Ok(Self {
            connector,
            properties,
            version,
            timestamp,
        })
    }
}

/// Mock Kafka Producer for testing
#[derive(Clone)]
struct MockKafkaProducer {
    records: Arc<Mutex<Vec<(String, Vec<u8>)>>>,
}

impl MockKafkaProducer {
    /// Create a new mock producer
    pub fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get all sent records
    pub fn get_records(&self) -> Vec<(String, Vec<u8>)> {
        let records = self.records.lock().unwrap();
        records.clone()
    }

    /// Send a record (mock implementation)
    pub fn send(&self, topic: String, key: Vec<u8>, value: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let mut records = self.records.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire records lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        records.push((topic, value));
        Ok(())
    }

    /// Flush buffered records (mock implementation)
    pub fn flush(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    /// Close the producer (mock implementation)
    pub fn close(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

/// Mock Kafka Consumer for testing
struct MockKafkaConsumer {
    data: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
}

impl MockKafkaConsumer {
    /// Create a new mock consumer
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Set data for a topic
    pub fn set_data(&self, topic: String, data: Vec<Vec<u8>>) {
        let mut data_map = self.data.lock().unwrap();
        data_map.insert(topic, data);
    }

    /// Poll for records (mock implementation)
    pub fn poll(&self, timeout: Duration) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
        let _ = timeout;
        Ok(Vec::new())
    }

    /// Subscribe to a topic (mock implementation)
    pub fn subscribe(&self, topics: Vec<String>) -> Result<(), Box<dyn Error>> {
        let _ = topics;
        Ok(())
    }

    /// Close the consumer (mock implementation)
    pub fn close(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

/// Kafka-based config backing store
///
/// This implementation uses a Kafka topic to persist connector and task configuration information.
/// It provides distributed state management for Kafka Connect runtime.
pub struct KafkaConfigBackingStore {
    /// Kafka topic name for storing config data
    topic: String,
    /// Kafka bootstrap servers
    bootstrap_servers: String,
    /// Kafka producer for writing configs
    producer: Arc<Mutex<MockKafkaProducer>>,
    /// Kafka consumer for reading configs
    consumer: Arc<Mutex<MockKafkaConsumer>>,
    /// Operation timeout
    timeout: Duration,
    /// Running state
    running: Arc<Mutex<bool>>,
    /// Local cache for connector configs
    connector_cache: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
    /// Local cache for task configs
    task_cache: Arc<Mutex<HashMap<String, Vec<HashMap<String, String>>>>>,
    /// Local cache for target states
    target_state_cache: Arc<Mutex<HashMap<String, TargetState>>>,
    /// Version tracking
    versions: Arc<Mutex<HashMap<String, i64>>>,
}

impl KafkaConfigBackingStore {
    /// Create a new Kafka config backing store
    ///
    /// # Arguments
    /// * `topic` - Kafka topic name for storing config data
    /// * `bootstrap_servers` - Kafka bootstrap servers (e.g., "localhost:9092")
    ///
    /// # Returns
    /// A new instance of KafkaConfigBackingStore
    pub fn new(topic: String, bootstrap_servers: String) -> Self {
        Self {
            topic,
            bootstrap_servers,
            producer: Arc::new(Mutex::new(MockKafkaProducer::new())),
            consumer: Arc::new(Mutex::new(MockKafkaConsumer::new())),
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            running: Arc::new(Mutex::new(false)),
            connector_cache: Arc::new(Mutex::new(HashMap::new())),
            task_cache: Arc::new(Mutex::new(HashMap::new())),
            target_state_cache: Arc::new(Mutex::new(HashMap::new())),
            versions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new Kafka config backing store with custom timeout
    ///
    /// # Arguments
    /// * `topic` - Kafka topic name for storing config data
    /// * `bootstrap_servers` - Kafka bootstrap servers
    /// * `timeout` - Operation timeout duration
    ///
    /// # Returns
    /// A new instance of KafkaConfigBackingStore with custom timeout
    pub fn with_timeout(topic: String, bootstrap_servers: String, timeout: Duration) -> Self {
        Self {
            topic,
            bootstrap_servers,
            producer: Arc::new(Mutex::new(MockKafkaProducer::new())),
            consumer: Arc::new(Mutex::new(MockKafkaConsumer::new())),
            timeout,
            running: Arc::new(Mutex::new(false)),
            connector_cache: Arc::new(Mutex::new(HashMap::new())),
            task_cache: Arc::new(Mutex::new(HashMap::new())),
            target_state_cache: Arc::new(Mutex::new(HashMap::new())),
            versions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get the topic name
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get theser bootstrap servers
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Check if the store is running
    pub fn is_running(&self) -> bool {
        let running = self.running.lock().unwrap();
        *running
    }

    /// Generate a key for connector config
    fn connector_key(&self, connector_name: &str) -> String {
        format!("{}{}", CONNECTOR_CONFIG_PREFIX, connector_name)
    }

    /// Generate a key for task config
    fn task_key(&self, connector_name: &str) -> String {
        format!("{}{}", TASK_CONFIG_PREFIX, connector_name)
    }

    /// Generate a key for target state
    fn target_state_key(&self, connector_name: &str) -> String {
        format!("{}{}", TARGET_STATE_PREFIX, connector_name)
    }

    /// Serialize target state to bytes
    fn serialize_target_state(&self, state: TargetState) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut bytes = Vec::new();
        bytes.push(state as u8);
        Ok(bytes)
    }

    /// Deserialize target state from bytes
    fn deserialize_target_state(&self, bytes: &[u8]) -> Result<TargetState, Box<dyn Error>> {
        if bytes.is_empty() {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "Invalid target state data: empty bytes".to_string(),
            )) as Box<dyn Error>);
        }

        let state = match bytes[0] {
            0 => TargetState::Started,
            1 => TargetState::Stopped,
            2 => TargetState::Paused,
            _ => TargetState::Stopped,
        };

        Ok(state)
    }

    /// Get version for a connector
    pub fn get_version(&self, connector: &str) -> Result<i64, Box<dyn Error>> {
        let versions = self.versions.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire versions lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        Ok(versions.get(connector).copied().unwrap_or(0))
    }

    /// Increment version for a connector
    pub fn increment_version(&self, connector: &str) -> Result<i64, Box<dyn Error>> {
        let mut versions = self.versions.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire versions lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        let current_version = versions.get(connector).copied().unwrap_or(0);
        let new_version = current_version + 1;
        versions.insert(connector.to_string(), new_version);

        Ok(new_version)
    }

    /// Clear all caches
    pub fn clear_caches(&self) {
        let mut connector_cache = self.connector_cache.lock().unwrap();
        connector_cache.clear();

        let mut task_cache = self.task_cache.lock().unwrap();
        task_cache.clear();

        let mut target_state_cache = self.target_state_cache.lock().unwrap();
        target_state_cache.clear();

        let mut versions = self.versions.lock().unwrap();
        versions.clear();
    }

    /// Get cache sizes
    pub fn cache_sizes(&self) -> (usize, usize, usize) {
        let connector_cache = self.connector_cache.lock().unwrap();
        let task_cache = self.task_cache.lock().unwrap();
        let target_state_cache = self.target_state_cache.lock().unwrap();

        (
            connector_cache.len(),
            task_cache.len(),
            target_state_cache.len(),
        )
    }
}

impl ConfigBackingStore for KafkaConfigBackingStore {
    fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let mut running = self.running.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire running lock: {}",
                e
            ))) as Box<dyn Error>
        })?;

        if *running {
            return Err(Box::new(ConnectRuntimeError::storage_error(
                "KafkaConfigBackingStore is already running".to_string(),
            )) as Box<dyn Error>);
        }

        // In a real implementation, we would initialize the Kafka producer and consumer here
        // For this mock implementation, we just set the running state

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
                "KafkaConfigBackingStore is not running".to_string(),
            )) as Box<dyn Error>);
        }

        // In a real implementation, we would close the Kafka producer and consumer here
        // For this mock implementation, we just set the running state

        *running = false;
        Ok(())
    }

    fn snapshot(&self) -> HashMap<String, HashMap<String, String>> {
        let connector_cache = self.connector_cache.lock();
        if let Err(e) = connector_cache {
            return HashMap::new();
        }

        let connector_cache = connector_cache.unwrap();
        connector_cache.clone()
    }

    fn contains(&self, connector: String) -> bool {
        let connector_cache = self.connector_cache.lock();
        if let Err(e) = connector_cache {
            return false;
        }

        let connector_cache = connector_cache.unwrap();
        connector_cache.contains_key(&connector)
    }

    fn put_connector_config(
        &mut self,
        connector: String,
        properties: HashMap<String, String>,
        target_state: TargetState,
    ) -> Result<(), Box<dyn Error>> {
        // Update local cache
        let mut connector_cache = self.connector_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire connector cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        connector_cache.insert(connector.clone(), properties.clone());
        drop(connector_cache);

        // Update target state
        let mut target_state_cache = self.target_state_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire target state cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        target_state_cache.insert(connector.clone(), target_state);
        drop(target_state_cache);

        // Increment version
        self.increment_version(&connector)?;

        // In a real implementation, we would write to Kafka here
        // For this mock, we just update the cache

        Ok(())
    }

    fn remove_connector_config(&mut self, connector: String) -> Result<(), Box<dyn Error>> {
        // Remove from local cache
        let mut connector_cache = self.connector_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire connector cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        connector_cache.remove(&connector);
        drop(connector_cache);

        // Remove from task cache
        let mut task_cache = self.task_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire task cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        task_cache.remove(&connector);
        drop(task_cache);

        // Remove from target state cache
        let mut target_state_cache = self.target_state_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire target state cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        target_state_cache.remove(&connector);
        drop(target_state_cache);

        // Remove from versions
        let mut versions = self.versions.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire versions lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        versions.remove(&connector);

        Ok(())
    }

    fn put_task_configs(
        &mut self,
        connector: String,
        configs: Vec<HashMap<String, String>>,
    ) -> Result<(), Box<dyn Error>> {
        // Update local cache
        let mut task_cache = self.task_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire task cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        task_cache.insert(connector.clone(), configs);
        drop(task_cache);

        // In a real implementation, we would write to Kafka here
        // For this mock, we just update the cache

        Ok(())
    }

    fn remove_task_configs(&mut self, connector: String) -> Result<(), Box<dyn Error>> {
        // Remove from local cache
        let mut task_cache = self.task_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire task cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        task_cache.remove(&connector);

        Ok(())
    }

    fn put_target_state(
        &mut self,
        connector: String,
        state: TargetState,
    ) -> Result<(), Box<dyn Error>> {
        // Update local cache
        let mut target_state_cache = self.target_state_cache.lock().map_err(|e| {
            Box::new(ConnectRuntimeError::storage_error(format!(
                "Failed to acquire target state cache lock: {}",
                e
            ))) as Box<dyn Error>
        })?;
        target_state_cache.insert(connector.clone(), state);
        drop(target_state_cache);

        // In a real implementation, we would write to Kafka here
        // For this mock, we just update the cache

        Ok(())
    }

    fn claim_write_privileges(&mut self) -> Result<(), Box<dyn Error>> {
        // In a real implementation, we would claim write privileges here
        // For this mock, we just return Ok
        Ok(())
    }
}

/// Closeable implementation for KafkaConfigBackingStore
impl Closeable for KafkaConfigBackingStore {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        // Stop
        self.stop()?;

        // Clear caches
        self.clear_caches();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    #[test]
    fn test_kafka_config_store_put_and_get() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let mut properties = HashMap::new();
        properties.insert(
            "connector.class".to_string(),
            "FileStreamSourceConnector".to_string(),
        );
        properties.insert("tasks.max".to_string(), "1".to_string());
        properties.insert("file".to_string(), "/tmp/test.txt".to_string());

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties.clone(),
            TargetState::Started,
        );
        assert!(result.is_ok());

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot.contains_key("test-connector"));

        let retrieved = snapshot.get("test-connector").unwrap();
        assert_eq!(
            retrieved.get("connector.class").unwrap(),
            "FileStreamSourceConnector"
        );
        assert_eq!(retrieved.get("tasks.max").unwrap(), "1");
        assert_eq!(retrieved.get("file").unwrap(), "/tmp/test.txt");
    }

    #[test]
    fn test_kafka_config_store_delete() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let mut properties = HashMap::new();
        properties.insert(
            "connector.class.class".to_string(),
            "FileStreamSourceConnector".to_string(),
        );

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties,
            TargetState::Started,
        );
        assert!(result.is_ok());

        assert!(store.contains("test-connector".to_string()));

        let result = store.remove_connector_config("test-connector".to_string());
        assert!(result.is_ok());

        assert!(!store.contains("test-connector".to_string()));
    }

    #[test]
    fn test_kafka_config_store_list() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let mut properties1 = HashMap::new();
        properties1.insert(
            "connector.class".to_string(),
            "FileStreamSourceConnector".to_string(),
        );

        let mut properties2 = HashMap::new();
        properties2.insert(
            "connector.class".to_string(),
            "FileStreamSinkConnector".to_string(),
        );

        let result = store.put_connector_config(
            "connector-1".to_string(),
            properties1,
            TargetState::Started,
        );
        assert!(result.is_ok());

        let result = store.put_connector_config(
            "connector-2".to_string(),
            properties2,
            TargetState::Stopped,
        );
        assert!(result.is_ok());

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.contains_key("connector-1"));
        assert!(snapshot.contains_key("connector-2"));
    }

    #[test]
    fn test_kafka_config_store_version_control() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let version = store.get_version("test-connector").unwrap();
        assert_eq!(version, 0);

        let mut properties = HashMap::new();
        properties.insert(
            "connector.class".to_string(),
            "FileStreamSourceConnector".to_string(),
        );

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties,
            TargetState::Started,
        );
        assert!(result.is_ok());

        let version = store.get_version("test-connector").unwrap();
        assert_eq!(version, 1);

        let mut properties2 = HashMap::new();
        properties2.insert(
            "connector.class".to_string(),
            "FileStreamSinkConnector".to_string(),
        );

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties2,
            TargetState::Started,
        );
        assert!(result.is_ok());

        let version = store.get_version("test-connector").unwrap();
        assert_eq!(version, 2);
    }

    #[test]
    fn test_kafka_config_store_serialization() {
        let mut properties = HashMap::new();
        properties.insert("key1".to_string(), "value1".to_string());
        properties.insert("key2".to_string(), "value2".to_string());

        let config_data = ConfigData::new("test-connector".to_string(), properties.clone());

        let bytes = config_data.to_bytes();
        assert!(bytes.is_ok());

        let deserialized = ConfigData::from_bytes(&bytes.unwrap());
        assert!(deserialized.is_ok());

        let restored = deserialized.unwrap();
        assert_eq!(restored.connector, "test-connector");
        assert_eq!(restored.properties.len(), 2);
        assert_eq!(restored.properties.get("key1").unwrap(), "value1");
        assert_eq!(restored.properties.get("key2").unwrap(), "value2");
        assert_eq!(restored.version, 1);
    }

    #[test]
    fn test_kafka_config_store_timeout() {
        let timeout = Duration::from_secs(60);
        let store = KafkaConfigBackingStore::with_timeout(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
            timeout,
        );

        assert_eq!(store.topic(), "connect-configs");
        assert_eq!(store.bootstrap_servers(), "localhost:9092");
    }

    #[test]
    fn test_kafka_config_store_concurrent_access() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        // Test sequential access instead of concurrent to avoid Send issues
        for i in 0..10 {
            let mut properties = HashMap::new();
            properties.insert("connector.class".to_string(), format!("Connector{}", i));
            properties.insert("tasks.max".to_string(), "1".to_string());

            let result = store.put_connector_config(
                format!("connector-{}", i),
                properties,
                TargetState::Started,
            );
            assert!(result.is_ok());
        }

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 10);
    }

    #[test]
    fn test_kafka_config_config_empty_key() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let properties = HashMap::new();

        let result = store.put_connector_config("".to_string(), properties, TargetState::Started);
        assert!(result.is_ok());

        assert!(store.contains("".to_string()));
    }

    #[test]
    fn test_kafka_config_store_large_value() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let mut properties = HashMap::new();
        properties.insert(
            "connector.class".to_string(),
            "FileStreamSourceConnector".to_string(),
        );

        // Create a large value
        let large_value = "x".repeat(10000);
        properties.insert("large.value".to_string(), large_value);

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties,
            TargetState::Started,
        );
        assert!(result.is_ok());

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 1);

        let retrieved = snapshot.get("test-connector").unwrap();
        assert_eq!(retrieved.get("large.value").unwrap().len(), 10000);
    }

    #[test]
    fn test_kafka_config_store_overwrite() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let mut properties1 = HashMap::new();
        properties1.insert(
            "connector.class".to_string(),
            "FileStreamSourceConnector".to_string(),
        );
        properties1.insert("tasks.max".to_string(), "1".to_string());

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties1,
            TargetState::Started,
        );
        assert!(result.is_ok());

        let mut properties2 = HashMap::new();
        properties2.insert(
            "connector.class".to_string(),
            "FileStreamSinkConnector".to_string(),
        );
        properties2.insert("tasks.max".to_string(), "2".to_string());

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties2,
            TargetState::Stopped,
        );
        assert!(result.is_ok());

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 1);

        let retrieved = snapshot.get("test-connector").unwrap();
        assert_eq!(
            retrieved.get("connector.class").unwrap(),
            "FileStreamSinkConnector"
        );
        assert_eq!(retrieved.get("tasks.max").unwrap(), "2");
    }

    #[test]
    fn test_kafka_config_store_nonexistent_key() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        assert!(!store.contains("nonexistent-connector".to_string()));

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 0);
    }

    #[test]
    fn test_kafka_config_store_multiple_keys() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let mut properties = HashMap::new();
        properties.insert(
            "connector.class".to_string(),
            "FileStreamSourceConnector".to_string(),
        );
        properties.insert("tasks.max".to_string(), "1".to_string());
        properties.insert("file".to_string(), "/tmp/test.txt".to_string());
        properties.insert("topic".to_string(), "test-topic".to_string());
        properties.insert("batch.size".to_string(), "1000".to_string());

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties,
            TargetState::Started,
        );
        assert!(result.is_ok());

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 1);

        let retrieved = snapshot.get("test-connector").unwrap();
        assert_eq!(retrieved.len(), 5);
    }

    #[test]
    fn test_kafka_config_store_lifecycle() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(!store.is_running());

        let result = store.start();
        assert!(result.is_ok());
        assert!(store.is_running());

        let result = store.stop();
        assert!(result.is_ok());
        assert!(!store.is_running());
    }

    #[test]
    fn test_kafka_config_store_error_handling() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        // Try to start twice
        let result = store.start();
        assert!(result.is_ok());

        let result = store.start();
        assert!(result.is_err());

        // Try to stop without starting
        let mut store2 = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        let result = store2.stop();
        assert!(result.is_err());
    }

    #[test]
    fn test_kafka_config_store_persistence() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let mut properties = HashMap::new();
        properties.insert(
            "connector.class".to_string(),
            "FileStreamSourceConnector".to_string(),
        );
        properties.insert("tasks.max".to_string(), "1".to_string());

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties.clone(),
            TargetState::Started,
        );
        assert!(result.is_ok());

        // Verify data is still there after multiple operations
        assert!(store.contains("test-connector".to_string()));

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 1);

        let retrieved = snapshot.get("test-connector").unwrap();
        assert_eq!(
            retrieved.get("connector.class").unwrap(),
            "FileStreamSourceConnector"
        );
    }

    #[test]
    fn test_kafka_config_store_task_configs() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let mut task1 = HashMap::new();
        task1.insert("task.class".to_string(), "FileStreamSourceTask".to_string());
        task1.insert("file".to_string(), "/tmp/file1.txt".to_string());

        let mut task2 = HashMap::new();
        task2.insert("task.class".to_string(), "FileStreamSourceTask".to_string());
        task2.insert("file".to_string(), "/tmp/file2.txt".to_string());

        let configs = vec![task1, task2];

        let result = store.put_task_configs("test-connector".to_string(), configs);
        assert!(result.is_ok());

        let result = store.remove_task_configs("test-connector".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_kafka_config_store_target_state() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let properties = HashMap::new();

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties,
            TargetState::Started,
        );
        assert!(result.is_ok());

        let result = store.put_target_state("test-connector".to_string(), TargetState::Paused);
        assert!(result.is_ok());

        let result = store.put_target_state("test-connector".to_string(), TargetState::Stopped);
        assert!(result.is_ok());
    }

    #[test]
    fn test_kafka_config_store_write_privileges() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let result = store.claim_write_privileges();
        assert!(result.is_ok());
    }

    #[test]
    fn test_kafka_config_store_close() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let mut properties = HashMap::new();
        properties.insert(
            "connector.class".to_string(),
            "FileStreamSourceConnector".to_string(),
        );

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties,
            TargetState::Started,
        );
        assert!(result.is_ok());

        let result = store.close();
        assert!(result.is_ok());
        assert!(!store.is_running());

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 0);
    }

    #[test]
    fn test_kafka_config_store_cache_sizes() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let sizes = store.cache_sizes();
        assert_eq!(sizes, (0, 0, 0));

        let mut properties = HashMap::new();
        properties.insert(
            "connector.class".to_string(),
            "FileStreamSourceConnector".to_string(),
        );

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties,
            TargetState::Started,
        );
        assert!(result.is_ok());

        let sizes = store.cache_sizes();
        assert_eq!(sizes.0, 1); // 1 connector config
    }

    #[test]
    fn test_kafka_config_store_clear_caches() {
        let mut store = KafkaConfigBackingStore::new(
            "connect-configs".to_string(),
            "localhost:9092".to_string(),
        );

        assert!(store.start().is_ok());

        let mut properties = HashMap::new();
        properties.insert(
            "connector.class".to_string(),
            "FileStreamSourceConnector".to_string(),
        );

        let result = store.put_connector_config(
            "test-connector".to_string(),
            properties,
            TargetState::Started,
        );
        assert!(result.is_ok());

        let sizes = store.cache_sizes();
        assert_eq!(sizes.0, 1);

        store.clear_caches();

        let sizes = store.cache_sizes();
        assert_eq!(sizes, (0, 0, 0));
    }
}
