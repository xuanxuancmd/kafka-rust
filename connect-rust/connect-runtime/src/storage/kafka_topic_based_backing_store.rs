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

//! Base class for backing stores that use a Kafka topic for storage.
//!
//! Provides common functionality for creating and managing Kafka topics used
//! as backing stores for connector offsets, configurations, and status information.
//!
//! Corresponds to `org.apache.kafka.connect.storage.KafkaTopicBasedBackingStore` in Java.

use std::collections::HashMap;
use std::sync::Arc;

use log::{debug, error, info, warn};

use common_trait::storage::WorkerConfig;
use common_trait::util::time::{Time, SYSTEM};

/// Topic cleanup policy for compaction.
pub const CLEANUP_POLICY_COMPACT: &str = "compact";

/// Default timeout for topic operations in milliseconds.
pub const DEFAULT_TOPIC_TIMEOUT_MS: u64 = 30000;

/// Default retry backoff for topic operations in milliseconds.
pub const DEFAULT_RETRY_BACKOFF_MS: u64 = 100;

/// Description of a Kafka topic to be created.
#[derive(Debug, Clone)]
pub struct TopicDescription {
    /// The topic name
    pub name: String,
    /// Number of partitions
    pub partitions: i32,
    /// Replication factor
    pub replication_factor: i16,
    /// Topic configuration (key-value pairs)
    pub config: HashMap<String, String>,
    /// Whether the topic should be compacted
    pub compacted: bool,
}

impl TopicDescription {
    /// Creates a new TopicDescription.
    pub fn new(name: String) -> Self {
        Self {
            name,
            partitions: 1,
            replication_factor: 1,
            config: HashMap::new(),
            compacted: true,
        }
    }

    /// Sets the number of partitions.
    pub fn partitions(mut self, partitions: i32) -> Self {
        self.partitions = partitions;
        self
    }

    /// Sets the replication factor.
    pub fn replication_factor(mut self, replication_factor: i16) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    /// Sets a configuration property.
    pub fn config(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }

    /// Sets the topic to be compacted.
    pub fn compacted(mut self) -> Self {
        self.compacted = true;
        self.config.insert(
            "cleanup.policy".to_string(),
            CLEANUP_POLICY_COMPACT.to_string(),
        );
        self
    }

    /// Builds the final TopicDescription.
    pub fn build(mut self) -> Self {
        if self.compacted {
            self.config.insert(
                "cleanup.policy".to_string(),
                CLEANUP_POLICY_COMPACT.to_string(),
            );
        }
        self
    }
}

/// Topic admin interface for managing Kafka topics.
/// This trait abstracts the operations needed for topic creation and verification.
pub trait TopicAdmin: Send + Sync {
    /// Creates topics with retry support.
    ///
    /// # Arguments
    /// * `topic_descriptions` - List of topics to create
    /// * `timeout_ms` - Timeout in milliseconds
    /// * `retry_backoff_ms` - Backoff time between retries
    /// * `time` - Time utility for timing operations
    ///
    /// # Returns
    /// Set of newly created topic names
    fn create_topics_with_retry(
        &self,
        topic_descriptions: Vec<TopicDescription>,
        timeout_ms: u64,
        retry_backoff_ms: u64,
        time: &dyn Time,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>>;

    /// Verifies that a topic has only compact cleanup policy.
    ///
    /// # Arguments
    /// * `topic` - Topic name to verify
    /// * `topic_config` - Configuration key for this topic type
    /// * `topic_purpose` - Description of the topic purpose
    fn verify_topic_cleanup_policy_only_compact(
        &self,
        topic: &str,
        topic_config: &str,
        topic_purpose: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Checks if a topic exists.
    fn topic_exists(&self, topic: &str) -> bool;
}

/// Callback for consumed records from Kafka.
pub trait ConsumeCallback<K, V>: Send + Sync {
    /// Called when a record is consumed.
    ///
    /// # Arguments
    /// * `error` - Error if consumption failed
    /// * `record` - The consumed record (key, value, offset, partition)
    fn on_completion(
        &self,
        error: Option<Box<dyn std::error::Error + Send + Sync>>,
        record: Option<(K, V, i64, i32)>,
    );
}

/// Kafka-based log interface for reading and writing to a Kafka topic.
pub trait KafkaBasedLog<K, V>: Send + Sync {
    /// Starts the log, reading from the beginning.
    fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Stops the log.
    fn stop(&mut self);

    /// Reads to the end of the log.
    fn read_to_end(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Sends a record to the log.
    ///
    /// # Arguments
    /// * `key` - The record key
    /// * `value` - The record value (None for tombstone)
    /// * `callback` - Callback for completion
    fn send(
        &self,
        key: Option<&K>,
        value: Option<&V>,
        callback: Option<Arc<dyn SendCompletionCallback>>,
    );

    /// Flushes pending writes.
    fn flush(&self);

    /// Returns the partition count for this log's topic.
    fn partition_count(&self) -> i32;
}

/// Callback for send completion.
pub trait SendCompletionCallback: Send + Sync {
    /// Called when the send completes.
    fn on_completion(
        &self,
        error: Option<Box<dyn std::error::Error + Send + Sync>>,
        offset: Option<i64>,
    );
}

/// Base class for Kafka topic-based backing stores.
///
/// Provides common functionality for creating and managing Kafka topics used
/// as backing stores. Subclasses implement specific storage behavior for
/// offsets, configurations, or status information.
pub struct KafkaTopicBasedBackingStore {
    /// Time utility for timing operations
    time: Arc<dyn Time>,
    /// Topic admin for managing topics
    topic_admin: Option<Arc<dyn TopicAdmin>>,
}

impl KafkaTopicBasedBackingStore {
    /// Creates a new KafkaTopicBasedBackingStore.
    pub fn new(time: Arc<dyn Time>) -> Self {
        Self {
            time,
            topic_admin: None,
        }
    }

    /// Creates a new KafkaTopicBasedBackingStore with a TopicAdmin.
    pub fn with_admin(time: Arc<dyn Time>, topic_admin: Arc<dyn TopicAdmin>) -> Self {
        Self {
            time,
            topic_admin: Some(topic_admin),
        }
    }

    /// Creates a topic initializer function.
    ///
    /// This creates the topic if it doesn't exist, and verifies the cleanup policy
    /// if it already exists.
    ///
    /// # Arguments
    /// * `topic` - The topic name
    /// * `topic_description` - Description of the topic to create
    /// * `config` - Worker configuration
    /// * `topic_config_key` - Configuration key for this topic
    /// * `topic_purpose` - Description of the topic purpose
    pub fn topic_initializer(
        &self,
        topic: &str,
        topic_description: TopicDescription,
        config: &WorkerConfig,
        topic_config_key: &str,
        topic_purpose: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(admin) = &self.topic_admin {
            debug!("Creating Connect internal topic for {}", topic_purpose);

            // Create the topic with retry
            let timeout_ms = self.get_topic_timeout(config);
            let backoff_ms = self.get_retry_backoff(config);

            let new_topics = admin.create_topics_with_retry(
                vec![topic_description],
                timeout_ms,
                backoff_ms,
                &*self.time,
            )?;

            if !new_topics.iter().any(|t| t == topic) {
                // Topic already existed, verify cleanup policy
                debug!(
                    "Using admin client to check cleanup policy of '{}' topic is '{}'",
                    topic, CLEANUP_POLICY_COMPACT
                );
                admin.verify_topic_cleanup_policy_only_compact(
                    topic,
                    topic_config_key,
                    topic_purpose,
                )?;
            }

            info!(
                "Successfully initialized topic {} for {}",
                topic, topic_purpose
            );
            Ok(())
        } else {
            warn!("No TopicAdmin available, skipping topic initialization");
            Ok(())
        }
    }

    /// Gets the topic timeout from config.
    fn get_topic_timeout(&self, config: &WorkerConfig) -> u64 {
        config
            .get("default.api.timeout.ms")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TOPIC_TIMEOUT_MS)
    }

    /// Gets the retry backoff from config.
    fn get_retry_backoff(&self, config: &WorkerConfig) -> u64 {
        config
            .get("retry.backoff.ms")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_RETRY_BACKOFF_MS)
    }

    /// Returns the time utility.
    pub fn time(&self) -> &Arc<dyn Time> {
        &self.time
    }

    /// Returns the topic admin if available.
    pub fn topic_admin(&self) -> Option<&Arc<dyn TopicAdmin>> {
        self.topic_admin.as_ref()
    }
}

/// Trait for subclasses to define their specific topic configuration and purpose.
pub trait TopicBasedStore {
    /// Returns the configuration key for this topic.
    fn get_topic_config(&self) -> &str;

    /// Returns the purpose description for this topic.
    fn get_topic_purpose(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_description_new() {
        let desc = TopicDescription::new("test-topic".to_string());
        assert_eq!(desc.name, "test-topic");
        assert_eq!(desc.partitions, 1);
        assert_eq!(desc.replication_factor, 1);
        assert!(desc.compacted);
    }

    #[test]
    fn test_topic_description_builder() {
        let desc = TopicDescription::new("test-topic".to_string())
            .partitions(3)
            .replication_factor(2)
            .compacted()
            .config("retention.ms".to_string(), "86400000".to_string())
            .build();

        assert_eq!(desc.name, "test-topic");
        assert_eq!(desc.partitions, 3);
        assert_eq!(desc.replication_factor, 2);
        assert!(desc.compacted);
        assert_eq!(
            desc.config.get("cleanup.policy"),
            Some(&CLEANUP_POLICY_COMPACT.to_string())
        );
        assert_eq!(
            desc.config.get("retention.ms"),
            Some(&"86400000".to_string())
        );
    }

    #[test]
    fn test_kafka_topic_based_backing_store_new() {
        let store = KafkaTopicBasedBackingStore::new(SYSTEM.clone());
        assert!(store.topic_admin.is_none());
    }

    #[test]
    fn test_default_timeout() {
        assert_eq!(DEFAULT_TOPIC_TIMEOUT_MS, 30000);
        assert_eq!(DEFAULT_RETRY_BACKOFF_MS, 100);
    }

    #[test]
    fn test_cleanup_policy_constant() {
        assert_eq!(CLEANUP_POLICY_COMPACT, "compact");
    }
}
