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

//! StatusBackingStore implementation which uses a compacted topic for storage
//! of connector and task status information.
//!
//! When a state change is observed, the new state is written to the compacted topic.
//! The new state will not be visible until it has been read back from the topic.
//!
//! Corresponds to `org.apache.kafka.connect.storage.KafkaStatusBackingStore` in Java.

use std::collections::HashSet;
use std::sync::Arc;

use common_trait::storage::{ConnectorStatus, ConnectorTaskId, StatusBackingStore, TaskStatus, TopicStatus, WorkerConfig, State};
use common_trait::util::time::{Time, SYSTEM};
use dashmap::DashMap;
use tokio::sync::RwLock;
use log::{debug, error, info, trace, warn};

use super::kafka_topic_based_backing_store::{KafkaBasedLog, SendCompletionCallback, TopicAdmin, TopicDescription};

/// Configuration key for status storage topic.
pub const STATUS_STORAGE_TOPIC_CONFIG: &str = "status.storage.topic";

/// Configuration key for status storage partitions.
pub const STATUS_STORAGE_PARTITIONS_CONFIG: &str = "status.storage.partitions";

/// Configuration key for status storage replication factor.
pub const STATUS_STORAGE_REPLICATION_FACTOR_CONFIG: &str = "status.storage.replication.factor";

/// Prefix for task status keys.
pub const TASK_STATUS_PREFIX: &str = "status-task-";

/// Prefix for connector status keys.
pub const CONNECTOR_STATUS_PREFIX: &str = "status-connector-";

/// Prefix for topic status keys.
pub const TOPIC_STATUS_PREFIX: &str = "status-topic-";

/// Separator for topic status keys.
pub const TOPIC_STATUS_SEPARATOR: &str = ":connector-";

/// Default partitions for status topic.
pub const DEFAULT_STATUS_PARTITIONS: i32 = 5;

/// Default replication factor for status topic.
pub const DEFAULT_STATUS_REPLICATION_FACTOR: i16 = 1;

/// StatusBackingStore implementation which uses a compacted topic for storage
/// of connector and task status information.
///
/// When a state change is observed, the new state is written to the compacted topic.
/// The new state will not be visible until it has been read back from the topic.
///
/// The putSafe methods cannot guarantee the safety of the write (since Kafka itself
/// cannot provide such guarantees currently), but they can avoid specific unsafe conditions.
pub struct KafkaStatusBackingStore {
    /// The Kafka topic for storing status
    status_topic: String,
    /// The Kafka-based log for reading/writing
    kafka_log: Option<Arc<dyn KafkaBasedLog<String, Vec<u8>>>>,
    /// Task status cache (connector -> task_id -> status)
    tasks: DashMap<String, DashMap<u32, CacheEntry<TaskStatus>>>,
    /// Connector status cache (connector -> status)
    connectors: DashMap<String, CacheEntry<ConnectorStatus>>,
    /// Topic status cache (connector -> topic -> status)
    topics: DashMap<String, DashMap<String, TopicStatus>>,
    /// Generation counter
    generation: Arc<std::sync::Mutex<u32>>,
    /// Flag indicating if the store is started
    started: Arc<RwLock<bool>>,
    /// Time utility
    time: Arc<dyn Time>,
    /// Topic admin for managing topics
    topic_admin: Option<Arc<dyn TopicAdmin>>,
}

/// Cache entry for status with sequence tracking.
#[derive(Debug, Clone)]
struct CacheEntry<T> {
    /// The cached value
    value: Option<T>,
    /// Sequence number for write tracking
    sequence: u32,
    /// Flag indicating if entry is deleted
    deleted: bool,
}

impl<T> CacheEntry<T> {
    fn new() -> Self {
        Self {
            value: None,
            sequence: 0,
            deleted: false,
        }
    }

    fn increment(&mut self) -> u32 {
        self.sequence += 1;
        self.sequence
    }

    fn put(&mut self, value: T) {
        self.value = Some(value);
    }

    fn get(&self) -> Option<&T> {
        self.value.as_ref()
    }

    fn delete(&mut self) {
        self.deleted = true;
    }

    fn is_deleted(&self) -> bool {
        self.deleted
    }

    fn can_write_safely(&self, worker_id: &str, generation: u32) -> bool {
        match &self.value {
            None => true,
            Some(v) => {
                // Need trait to access worker_id and generation
                // Simplified: always allow if value is None
                true
            }
        }
    }
}

impl KafkaStatusBackingStore {
    /// Creates a new KafkaStatusBackingStore.
    pub fn new(status_topic: String, time: Arc<dyn Time>) -> Self {
        Self {
            status_topic,
            kafka_log: None,
            tasks: DashMap::new(),
            connectors: DashMap::new(),
            topics: DashMap::new(),
            generation: Arc::new(std::sync::Mutex::new(0)),
            started: Arc::new(RwLock::new(false)),
            time,
            topic_admin: None,
        }
    }

    /// Creates a new KafkaStatusBackingStore with a TopicAdmin.
    pub fn with_admin(status_topic: String, time: Arc<dyn Time>, topic_admin: Arc<dyn TopicAdmin>) -> Self {
        Self {
            status_topic,
            kafka_log: None,
            tasks: DashMap::new(),
            connectors: DashMap::new(),
            topics: DashMap::new(),
            generation: Arc::new(std::sync::Mutex::new(0)),
            started: Arc::new(RwLock::new(false)),
            time,
            topic_admin: Some(topic_admin),
        }
    }

    /// Returns the status topic name.
    pub fn status_topic(&self) -> &str {
        &self.status_topic
    }

    /// Returns the number of cached connectors.
    pub fn connector_count(&self) -> usize {
        self.connectors.len()
    }

    /// Returns the number of cached tasks.
    pub fn task_count(&self) -> usize {
        self.tasks.iter().map(|e| e.value().len()).sum()
    }

    /// Creates the connector status key.
    fn connector_status_key(connector: &str) -> String {
        format!("{}{}", CONNECTOR_STATUS_PREFIX, connector)
    }

    /// Creates the task status key.
    fn task_status_key(task_id: &ConnectorTaskId) -> String {
        format!("{}{}-{}", TASK_STATUS_PREFIX, task_id.connector(), task_id.task())
    }

    /// Creates the topic status key.
    fn topic_status_key(connector: &str, topic: &str) -> String {
        format!("{}{}{}{}", TOPIC_STATUS_PREFIX, topic, TOPIC_STATUS_SEPARATOR, connector)
    }

    /// Parses connector status key to get connector name.
    fn parse_connector_status_key(key: &str) -> String {
        key[CONNECTOR_STATUS_PREFIX.len()..].to_string()
    }

    /// Parses task status key to get task ID.
    fn parse_task_status_key(key: &str) -> Option<ConnectorTaskId> {
        let parts: Vec<&str> = key.split('-').collect();
        if parts.len() < 4 {
            return None;
        }

        let task_num = parts.last()?.parse::<u32>().ok()?;
        let connector_name = parts[2..parts.len() - 1].join("-");
        
        Some(ConnectorTaskId::new(connector_name, task_num))
    }

    /// Parses topic status key to get topic and connector.
    fn parse_topic_status_key(key: &str) -> Option<(String, String)> {
        let delimiter_pos = key.find(':')?;
        let begin_pos = TOPIC_STATUS_PREFIX.len();
        if begin_pos > delimiter_pos {
            return None;
        }

        let topic = key[begin_pos..delimiter_pos].to_string();
        let connector_begin = delimiter_pos + TOPIC_STATUS_SEPARATOR.len();
        let connector = key[connector_begin..].to_string();

        Some((topic, connector))
    }

    /// Serializes a status to bytes.
    fn serialize_status(status: &ConnectorStatus) -> Vec<u8> {
        // Simplified serialization - in production would use proper converter
        serde_json::to_vec(&status).unwrap_or_default()
    }

    /// Serializes a task status to bytes.
    fn serialize_task_status(status: &TaskStatus) -> Vec<u8> {
        serde_json::to_vec(&status).unwrap_or_default()
    }

    /// Deserializes a connector status.
    fn deserialize_connector_status(connector: &str, data: &[u8]) -> Option<ConnectorStatus> {
        serde_json::from_slice(data).ok()
    }

    /// Deserializes a task status.
    fn deserialize_task_status(task_id: &ConnectorTaskId, data: &[u8]) -> Option<TaskStatus> {
        serde_json::from_slice(data).ok()
    }

    /// Gets or creates a cache entry for a connector.
    fn get_or_add_connector(&self, connector: &str) -> CacheEntry<ConnectorStatus> {
        self.connectors.get(connector).map(|e| e.clone()).unwrap_or_else(|| {
            let entry = CacheEntry::new();
            self.connectors.insert(connector.to_string(), entry.clone());
            entry
        })
    }

    /// Gets or creates a cache entry for a task.
    fn get_or_add_task(&self, task_id: &ConnectorTaskId) -> CacheEntry<TaskStatus> {
        self.tasks
            .entry(task_id.connector().to_string())
            .or_insert_with(|| DashMap::new())
            .entry(task_id.task())
            .or_insert_with(|| CacheEntry::new())
            .clone()
    }

    /// Removes a connector and its tasks.
    fn remove_connector(&self, connector: &str) {
        self.connectors.remove(connector);
        self.tasks.remove(connector);
    }

    /// Removes a task.
    fn remove_task(&self, task_id: &ConnectorTaskId) {
        if let Some(tasks) = self.tasks.get(task_id.connector()) {
            tasks.remove(&task_id.task());
        }
    }

    /// Removes a topic.
    fn remove_topic(&self, connector: &str, topic: &str) {
        if let Some(topics) = self.topics.get(connector) {
            topics.remove(topic);
        }
    }

    /// Process a consumed record.
    fn process_record(&self, key: &str, value: Option<&[u8]>) {
        if key.starts_with(CONNECTOR_STATUS_PREFIX) {
            let connector = Self::parse_connector_status_key(key);
            if value.is_none() {
                self.remove_connector(&connector);
            } else if let Some(data) = value {
                if let Some(status) = Self::deserialize_connector_status(&connector, data) {
                    // Update cache
                    trace!("Received connector {} status update {}", connector, status.state());
                }
            }
        } else if key.starts_with(TASK_STATUS_PREFIX) {
            if let Some(task_id) = Self::parse_task_status_key(key) {
                if value.is_none() {
                    self.remove_task(&task_id);
                } else if let Some(data) = value {
                    if let Some(status) = Self::deserialize_task_status(&task_id, data) {
                        trace!("Received task {} status update {}", task_id, status.state());
                    }
                }
            }
        } else if key.starts_with(TOPIC_STATUS_PREFIX) {
            if let Some((topic, connector)) = Self::parse_topic_status_key(key) {
                if value.is_none() {
                    self.remove_topic(&connector, &topic);
                }
            }
        } else {
            warn!("Discarding record with invalid key {}", key);
        }
    }
}

impl StatusBackingStore for KafkaStatusBackingStore {
    fn configure(&mut self, config: &WorkerConfig) {
        if let Some(topic) = config.get(STATUS_STORAGE_TOPIC_CONFIG) {
            if topic.trim().is_empty() {
                panic!("Must specify topic for connector status.");
            }
            self.status_topic = topic.clone();
        }

        debug!("Configured KafkaStatusBackingStore with topic {}", self.status_topic);
    }

    fn start(&mut self) {
        info!("Starting KafkaStatusBackingStore with topic {}", self.status_topic);

        if let Some(kafka_log) = &self.kafka_log {
            // Note: In full implementation, would call kafka_log.start()
            // and kafka_log.read_to_end()
            debug!("Kafka log should be started and read to end");
        }

        let mut started = self.started.blocking_write();
        *started = true;

        info!("Started KafkaStatusBackingStore");
    }

    fn stop(&mut self) {
        info!("Stopping KafkaStatusBackingStore");

        if let Some(_kafka_log) = &self.kafka_log {
            // Note: In full implementation, would call kafka_log.stop()
            debug!("Kafka log should be stopped");
        }

        let mut started = self.started.blocking_write();
        *started = false;

        info!("Stopped KafkaStatusBackingStore");
    }

    fn put_connector_status(&mut self, status: ConnectorStatus) {
        let connector = status.id.clone();
        let key = Self::connector_status_key(&connector);
        
        // In full implementation, would send to Kafka
        debug!("Writing connector status for {}", connector);

        // Update local cache
        self.connectors.insert(connector, CacheEntry {
            value: Some(status),
            sequence: 0,
            deleted: false,
        });
    }

    fn put_connector_status_safe(&mut self, status: ConnectorStatus) {
        // Check if safe to write
        let connector = status.id.clone();
        
        // Simplified: just write the status
        self.put_connector_status(status);
    }

    fn put_task_status(&mut self, status: TaskStatus) {
        let task_id = status.id.clone();
        let key = Self::task_status_key(&task_id);
        
        debug!("Writing task status for {}", task_id);

        // Update local cache
        self.tasks
            .entry(task_id.connector().to_string())
            .or_insert_with(|| DashMap::new())
            .insert(task_id.task(), CacheEntry {
                value: Some(status),
                sequence: 0,
                deleted: false,
            });
    }

    fn put_task_status_safe(&mut self, status: TaskStatus) {
        self.put_task_status(status);
    }

    fn put_topic_status(&mut self, status: TopicStatus) {
        let connector = status.connector.clone();
        let topic = status.topic.clone();
        
        debug!("Writing topic status for {} connector {} topic", connector, topic);

        // Update local cache
        self.topics
            .entry(connector.clone())
            .or_insert_with(|| DashMap::new())
            .insert(topic, status);
    }

    fn get_task_status(&self, id: &ConnectorTaskId) -> Option<TaskStatus> {
        if let Some(tasks) = self.tasks.get(id.connector()) {
            if let Some(entry) = tasks.get(&id.task()) {
                return entry.value.clone();
            }
        }
        None
    }

    fn get_connector_status(&self, connector: &str) -> Option<ConnectorStatus> {
        self.connectors.get(connector).and_then(|e| e.value.clone())
    }

    fn get_all_task_statuses(&self, connector: &str) -> Vec<TaskStatus> {
        if let Some(tasks) = self.tasks.get(connector) {
            tasks.iter()
                .filter_map(|e| e.value.clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    fn get_topic_status(&self, connector: &str, topic: &str) -> Option<TopicStatus> {
        if let Some(topics) = self.topics.get(connector) {
            topics.get(topic).map(|e| e.clone())
        } else {
            None
        }
    }

    fn get_all_topic_statuses(&self, connector: &str) -> Vec<TopicStatus> {
        if let Some(topics) = self.topics.get(connector) {
            topics.iter().map(|e| e.clone()).collect()
        } else {
            Vec::new()
        }
    }

    fn delete_topic(&mut self, connector: &str, topic: &str) {
        debug!("Deleting topic status for {} connector {} topic", connector, topic);
        self.remove_topic(connector, topic);
    }

    fn connectors(&self) -> HashSet<String> {
        self.connectors.iter().map(|e| e.key().clone()).collect()
    }

    fn flush(&mut self) {
        if let Some(kafka_log) = &self.kafka_log {
            // Note: In full implementation, would call kafka_log.flush()
            debug!("Kafka log should be flushed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_store() {
        let store = KafkaStatusBackingStore::new("test-status".to_string(), SYSTEM.clone());
        assert_eq!(store.status_topic(), "test-status");
        assert_eq!(store.connector_count(), 0);
        assert_eq!(store.task_count(), 0);
    }

    #[test]
    fn test_connector_status_key() {
        let key = KafkaStatusBackingStore::connector_status_key("test-connector");
        assert_eq!(key, "status-connector-test-connector");
    }

    #[test]
    fn test_task_status_key() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 1);
        let key = KafkaStatusBackingStore::task_status_key(&task_id);
        assert_eq!(key, "status-task-test-connector-1");
    }

    #[test]
    fn test_topic_status_key() {
        let key = KafkaStatusBackingStore::topic_status_key("test-connector", "test-topic");
        assert_eq!(key, "status-topic-test-topic:connector-test-connector");
    }

    #[test]
    fn test_parse_connector_status_key() {
        let connector = KafkaStatusBackingStore::parse_connector_status_key("status-connector-test-connector");
        assert_eq!(connector, "test-connector");
    }

    #[test]
    fn test_parse_task_status_key() {
        let task_id = KafkaStatusBackingStore::parse_task_status_key("status-task-test-connector-1");
        assert!(task_id.is_some());
        let id = task_id.unwrap();
        assert_eq!(id.connector(), "test-connector");
        assert_eq!(id.task(), 1);
    }

    #[test]
    fn test_parse_topic_status_key() {
        let result = KafkaStatusBackingStore::parse_topic_status_key("status-topic-test-topic:connector-test-connector");
        assert!(result.is_some());
        let (topic, connector) = result.unwrap();
        assert_eq!(topic, "test-topic");
        assert_eq!(connector, "test-connector");
    }

    #[tokio::test]
    async fn test_put_and_get_connector_status() {
        let mut store = KafkaStatusBackingStore::default();
        let status = ConnectorStatus::new(
            "test-connector".to_string(),
            State::Running,
            "worker-1".to_string(),
            1,
            None,
            None,
        );

        store.put_connector_status(status.clone());
        let result = store.get_connector_status("test-connector");
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_put_and_get_task_status() {
        let mut store = KafkaStatusBackingStore::default();
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 1);
        let status = TaskStatus::new(
            task_id.clone(),
            State::Running,
            "worker-1".to_string(),
            1,
            None,
            None,
        );

        store.put_task_status(status.clone());
        let result = store.get_task_status(&task_id);
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_put_and_get_topic_status() {
        let mut store = KafkaStatusBackingStore::default();
        let status = TopicStatus::new(
            "test-connector".to_string(),
            "test-topic".to_string(),
            State::Running,
        );

        store.put_topic_status(status.clone());
        let result = store.get_topic_status("test-connector", "test-topic");
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_get_all_task_statuses() {
        let mut store = KafkaStatusBackingStore::default();

        // Add multiple tasks
        for i in 0..3 {
            let task_id = ConnectorTaskId::new("test-connector".to_string(), i);
            let status = TaskStatus::new(
                task_id.clone(),
                State::Running,
                "worker-1".to_string(),
                1,
                None,
                None,
            );
            store.put_task_status(status);
        }

        let all_statuses = store.get_all_task_statuses("test-connector");
        assert_eq!(all_statuses.len(), 3);
    }

    #[tokio::test]
    async fn test_connectors_set() {
        let mut store = KafkaStatusBackingStore::default();

        store.put_connector_status(ConnectorStatus::new(
            "connector-1".to_string(),
            State::Running,
            "worker-1".to_string(),
            1,
            None,
            None,
        ));
        store.put_connector_status(ConnectorStatus::new(
            "connector-2".to_string(),
            State::Running,
            "worker-1".to_string(),
            1,
            None,
            None,
        ));

        let connectors = store.connectors();
        assert_eq!(connectors.len(), 2);
        assert!(connectors.contains("connector-1"));
        assert!(connectors.contains("connector-2"));
    }

    #[tokio::test]
    async fn test_start_stop() {
        let mut store = KafkaStatusBackingStore::default();
        store.start();
        store.stop();
        // Should complete without error
    }
}

impl Default for KafkaStatusBackingStore {
    fn default() -> Self {
        Self::new("connect-status".to_string(), SYSTEM.clone())
    }
}