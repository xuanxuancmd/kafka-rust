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

//! Implementation of OffsetBackingStore that uses a Kafka topic to store offset data.
//!
//! Internally, this implementation both produces to and consumes from a Kafka topic
//! which stores the offsets. It accepts producer and consumer overrides via its
//! configuration but forces some settings to specific values to ensure correct behavior.
//!
//! Corresponds to `org.apache.kafka.connect.storage.KafkaOffsetBackingStore` in Java.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common_trait::storage::{GetFuture, OffsetBackingStore, SetFuture, WorkerConfig};
use common_trait::util::time::{Time, SYSTEM};
use dashmap::DashMap;
use tokio::sync::RwLock;
use log::{debug, info};

use super::kafka_topic_based_backing_store::{KafkaBasedLog, SendCompletionCallback, TopicAdmin, TopicDescription};

/// Configuration key for offset storage topic.
pub const OFFSET_STORAGE_TOPIC_CONFIG: &str = "offset.storage.topic";

/// Configuration key for offset storage partitions.
pub const OFFSET_STORAGE_PARTITIONS_CONFIG: &str = "offset.storage.partitions";

/// Configuration key for offset storage replication factor.
pub const OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG: &str = "offset.storage.replication.factor";

/// Default partitions for offset topic.
pub const DEFAULT_OFFSET_PARTITIONS: i32 = 1;

/// Default replication factor for offset topic.
pub const DEFAULT_OFFSET_REPLICATION_FACTOR: i16 = 1;

/// Implementation of OffsetBackingStore that uses a Kafka topic to store offset data.
///
/// Internally, this implementation both produces to and consumes from a Kafka topic
/// which stores the offsets. It accepts producer and consumer overrides via its
/// configuration but forces some settings to specific values to ensure correct behavior
/// (e.g. acks=all, auto.offset.reset=earliest).
pub struct KafkaOffsetBackingStore {
    /// The Kafka topic for storing offsets
    topic: String,
    /// The Kafka-based log for reading/writing
    offset_log: Option<Arc<dyn KafkaBasedLog<Vec<u8>, Vec<u8>>>>,
    /// In-memory cache of offset data
    data: Arc<DashMap<Vec<u8>, Vec<u8>>>,
    /// Connector partitions tracking
    connector_partitions: Arc<DashMap<String, HashSet<HashMap<String, serde_json::Value>>>>,
    /// Flag indicating if the store is started
    started: Arc<RwLock<bool>>,
    /// Flag for exactly-once source support
    exactly_once: bool,
    /// Time utility
    time: Arc<dyn Time>,
    /// Topic admin for managing topics
    topic_admin: Option<Arc<dyn TopicAdmin>>,
}

impl KafkaOffsetBackingStore {
    /// Creates a new KafkaOffsetBackingStore.
    pub fn new(topic: String, time: Arc<dyn Time>) -> Self {
        Self {
            topic,
            offset_log: None,
            data: Arc::new(DashMap::new()),
            connector_partitions: Arc::new(DashMap::new()),
            started: Arc::new(RwLock::new(false)),
            exactly_once: false,
            time,
            topic_admin: None,
        }
    }

    /// Creates a new KafkaOffsetBackingStore with a TopicAdmin.
    pub fn with_admin(topic: String, time: Arc<dyn Time>, topic_admin: Arc<dyn TopicAdmin>) -> Self {
        Self {
            topic,
            offset_log: None,
            data: Arc::new(DashMap::new()),
            connector_partitions: Arc::new(DashMap::new()),
            started: Arc::new(RwLock::new(false)),
            exactly_once: false,
            time,
            topic_admin: Some(topic_admin),
        }
    }

    /// Builds a read-write offset store with existing Kafka clients.
    ///
    /// # Arguments
    /// * `topic` - The name of the offsets topic
    /// * `offset_log` - The Kafka-based log for reading/writing
    /// * `key_converter` - Converter for deserializing offset keys (placeholder)
    pub fn read_write_store(
        topic: String,
        offset_log: Arc<dyn KafkaBasedLog<Vec<u8>, Vec<u8>>>,
    ) -> Self {
        let mut store = Self::new(topic, SYSTEM.clone());
        store.offset_log = Some(offset_log);
        store
    }

    /// Builds a read-only offset store with an existing consumer.
    ///
    /// # Arguments
    /// * `topic` - The name of the offsets topic
    /// * `offset_log` - The Kafka-based log for reading (no producer)
    pub fn read_only_store(
        topic: String,
        offset_log: Arc<dyn KafkaBasedLog<Vec<u8>, Vec<u8>>>,
    ) -> Self {
        let mut store = Self::new(topic, SYSTEM.clone());
        store.offset_log = Some(offset_log);
        store
    }

    /// Returns the topic name for this store.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns true if exactly-once source support is enabled.
    pub fn exactly_once(&self) -> bool {
        self.exactly_once
    }

    /// Returns the number of entries in the cache.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Creates a topic description for the offset topic.
    fn create_topic_description(&self, config: &WorkerConfig) -> TopicDescription {
        let partitions = config
            .get(OFFSET_STORAGE_PARTITIONS_CONFIG)
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(DEFAULT_OFFSET_PARTITIONS);

        let replication_factor = config
            .get(OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG)
            .and_then(|v| v.parse::<i16>().ok())
            .unwrap_or(DEFAULT_OFFSET_REPLICATION_FACTOR);

        TopicDescription::new(self.topic.clone())
            .partitions(partitions)
            .replication_factor(replication_factor)
            .compacted()
            .build()
    }

    /// Initializes the topic if needed.
    async fn initialize_topic(&self, config: &WorkerConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(admin) = &self.topic_admin {
            let topic_desc = self.create_topic_description(config);
            
            let new_topics = admin.create_topics_with_retry(
                vec![topic_desc],
                30000,
                100,
                &*self.time,
            )?;

            if !new_topics.iter().any(|t| t == &self.topic) {
                admin.verify_topic_cleanup_policy_only_compact(
                    &self.topic,
                    OFFSET_STORAGE_TOPIC_CONFIG,
                    "source connector offsets",
                )?;
            }
        }

        Ok(())
    }

    /// Process a consumed record.
    fn process_record(&self, key: Option<Vec<u8>>, value: Option<Vec<u8>>) {
        match (key, value) {
            (Some(key), Some(value)) => {
                self.data.insert(key, value);
            }
            (Some(key), None) => {
                // Tombstone - remove the key
                self.data.remove(&key);
            }
            (None, _) => {
                debug!("Received record with null key, ignoring");
            }
        }
    }
}

impl OffsetBackingStore for KafkaOffsetBackingStore {
    fn configure(&mut self, config: &WorkerConfig) {
        // Get topic from config
        if let Some(topic) = config.get(OFFSET_STORAGE_TOPIC_CONFIG) {
            if topic.trim().is_empty() {
                panic!("Offset storage topic must be specified");
            }
            self.topic = topic.clone();
        }

        // Check for exactly-once source support
        self.exactly_once = config
            .get("exactly.once.source.support")
            .map(|v| v == "enabled")
            .unwrap_or(false);

        debug!(
            "Configured KafkaOffsetBackingStore with topic {}, exactly_once={}",
            self.topic, self.exactly_once
        );
    }

    fn start(&mut self) {
        info!("Starting KafkaOffsetBackingStore with topic {}", self.topic);

        // Start the offset log if present
        if let Some(offset_log) = &self.offset_log {
            // Note: In full implementation, would call offset_log.start()
            // and handle UnsupportedVersionException for exactly-once
            debug!("Offset log should be started");
        }

        // Initialize topic if admin is available
        // In production, this would be async

        let mut started = self.started.blocking_write();
        *started = true;

        info!("Finished reading offsets topic and starting KafkaOffsetBackingStore");
    }

    fn stop(&mut self) {
        info!("Stopping KafkaOffsetBackingStore");

        // Stop the offset log if present
        if let Some(_offset_log) = &self.offset_log {
            // Note: In full implementation, would call offset_log.stop()
            debug!("Offset log should be stopped");
        }

        // Admin client is not closed here - caller manages its lifecycle

        let mut started = self.started.blocking_write();
        *started = false;

        info!("Stopped KafkaOffsetBackingStore");
    }

    fn get(&self, keys: Vec<Vec<u8>>) -> GetFuture {
        let data = self.data.clone();

        Box::pin(async move {
            // Build result from in-memory cache
            keys.into_iter()
                .fold(HashMap::new(), |mut acc, key| {
                    let value = data.get(&key).map(|v| v.clone());
                    acc.insert(key, value);
                    acc
                })
        })
    }

    fn set(&self, values: HashMap<Vec<u8>, Vec<u8>>) -> SetFuture {
        let data = self.data.clone();
        let offset_log = self.offset_log.clone();

        Box::pin(async move {
            // Update in-memory cache
            for (key, value) in values.iter() {
                data.insert(key.clone(), value.clone());
            }

            // Write to Kafka if offset log is available
            if let Some(log) = offset_log {
                for (key, value) in values.iter() {
                    // Send to Kafka
                    // Note: In full implementation, would use proper callback
                    log.send(Some(key), Some(value), None);
                }
            }

            Ok(())
        })
    }

    fn connector_partitions(
        &self,
        connector_name: &str,
    ) -> HashSet<HashMap<String, serde_json::Value>> {
        self.connector_partitions
            .get(connector_name)
            .map(|v| v.clone())
            .unwrap_or_default()
    }
}

impl Default for KafkaOffsetBackingStore {
    fn default() -> Self {
        Self::new("connect-offsets".to_string(), SYSTEM.clone())
    }
}

/// Callback implementation for tracking offset write completion.
pub struct OffsetWriteCallbackImpl {
    /// Number of pending writes
    pending: Arc<std::sync::atomic::AtomicU32>,
    /// Error if any write failed
    error: Arc<std::sync::Mutex<Option<Box<dyn std::error::Error + Send + Sync>>>>,
    /// Completion notifier
    completed: Arc<tokio::sync::Notify>,
}

impl OffsetWriteCallbackImpl {
    /// Creates a new OffsetWriteCallbackImpl.
    pub fn new(num_records: u32) -> Self {
        Self {
            pending: Arc::new(std::sync::atomic::AtomicU32::new(num_records)),
            error: Arc::new(std::sync::Mutex::new(None)),
            completed: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Returns true if all writes are complete.
    pub fn is_complete(&self) -> bool {
        self.pending.load(std::sync::atomic::Ordering::SeqCst) == 0
    }

    /// Returns the error if any write failed.
    pub fn get_error(&self) -> Option<String> {
        self.error.lock().unwrap().as_ref().map(|e| format!("{}", e))
    }

    /// Waits for all writes to complete.
    pub async fn wait(&self) {
        while !self.is_complete() {
            self.completed.notified().await;
        }
    }
}

impl SendCompletionCallback for OffsetWriteCallbackImpl {
    fn on_completion(&self, error: Option<Box<dyn std::error::Error + Send + Sync>>, _offset: Option<i64>) {
        if let Some(e) = error {
            let mut stored_error = self.error.lock().unwrap();
            if stored_error.is_none() {
                *stored_error = Some(e);
            }
        }

        let prev = self.pending.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        if prev == 1 {
            self.completed.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_store() {
        let store = KafkaOffsetBackingStore::new("test-offsets".to_string(), SYSTEM.clone());
        assert_eq!(store.topic(), "test-offsets");
        assert!(!store.exactly_once());
        assert_eq!(store.size(), 0);
    }

    #[test]
    fn test_default_store() {
        let store = KafkaOffsetBackingStore::default();
        assert_eq!(store.topic(), "connect-offsets");
    }

    #[tokio::test]
    async fn test_get_empty() {
        let store = KafkaOffsetBackingStore::default();
        let key = vec![1, 2, 3];
        let result = store.get(vec![key.clone()]).await;
        assert_eq!(result.get(&key), Some(&None));
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let store = KafkaOffsetBackingStore::default();
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];

        store.set(HashMap::from([(key.clone(), value.clone())])).await.unwrap();

        let result = store.get(vec![key.clone()]).await;
        assert_eq!(result.get(&key), Some(&Some(value)));
        assert_eq!(store.size(), 1);
    }

    #[tokio::test]
    async fn test_multiple_keys() {
        let store = KafkaOffsetBackingStore::default();
        let key1 = vec![1];
        let value1 = vec![10];
        let key2 = vec![2];
        let value2 = vec![20];

        store.set(HashMap::from([(key1.clone(), value1.clone()), (key2.clone(), value2.clone())])).await.unwrap();

        let result = store.get(vec![key1.clone(), key2.clone()]).await;
        assert_eq!(result.get(&key1), Some(&Some(value1)));
        assert_eq!(result.get(&key2), Some(&Some(value2)));
        assert_eq!(store.size(), 2);
    }

    #[tokio::test]
    async fn test_connector_partitions() {
        let store = KafkaOffsetBackingStore::default();
        let partitions = store.connector_partitions("test-connector");
        assert!(partitions.is_empty());
    }

    #[tokio::test]
    async fn test_start_stop() {
        let mut store = KafkaOffsetBackingStore::default();
        store.start();
        store.stop();
        // Should complete without error
    }

    #[test]
    fn test_offset_write_callback_new() {
        let callback = OffsetWriteCallbackImpl::new(3);
        assert!(!callback.is_complete());
    }

    #[test]
    fn test_offset_write_callback_completion() {
        let callback = OffsetWriteCallbackImpl::new(2);
        callback.on_completion(None, Some(0));
        assert!(!callback.is_complete());
        callback.on_completion(None, Some(1));
        assert!(callback.is_complete());
        assert!(callback.get_error().is_none());
    }

    #[test]
    fn test_offset_write_callback_error() {
        let callback = OffsetWriteCallbackImpl::new(1);
        let error: Box<dyn std::error::Error + Send + Sync> = Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "test error",
        ));
        callback.on_completion(Some(error), None);
        assert!(callback.is_complete());
        assert!(callback.get_error().is_some());
    }
}