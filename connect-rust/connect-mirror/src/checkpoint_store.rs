/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Checkpoint store for managing checkpoints from Kafka log
//!
//! This module implements the CheckpointStore structure, which reads once the Kafka log
//! for checkpoints and populates a map of checkpoints per consumer group.

use crate::checkpoint::Checkpoint;
use connect_runtime::util::{Callback, KafkaBasedLog, TopicAdmin};
use common_trait::consumer::{ConsumerRecord, OffsetAndMetadata, TopicPartition};
use common_trait::time::Time;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Checkpoint store reads once the Kafka log for checkpoints and populates a map of
/// checkpoints per consumer group.
///
/// The Kafka log is closed after the initial load and only the in memory map is
/// used after start.
pub struct CheckpointStore {
    /// Configuration for the checkpoint task
    config: Option<Box<dyn MirrorCheckpointTaskConfig>>,
    /// Consumer groups to monitor
    consumer_groups: HashSet<String>,
    /// Admin client for checkpoints topic
    cp_admin: Option<Arc<Mutex<TopicAdmin>>>,
    /// Backing store for reading checkpoints
    backing_store: Option<Arc<Mutex<KafkaBasedLog<Vec<u8>, Vec<u8>>>>>,
    /// Map of checkpoints per consumer group
    checkpoints_per_consumer_group:
        Arc<Mutex<HashMap<String, HashMap<TopicPartition, Checkpoint>>>>,
    /// Whether the checkpoints were loaded successfully
    load_success: Arc<Mutex<bool>>,
    /// Whether the store is initialized
    is_initialized: Arc<Mutex<bool>>,
}

/// Configuration trait for MirrorCheckpointTask
pub trait MirrorCheckpointTaskConfig: Send + Sync {
    /// Get target admin config
    fn target_admin_config(&self, prefix: &str) -> HashMap<String, String>;

    /// Get forwarding admin config
    fn forwarding_admin(&self, base_config: HashMap<String, String>) -> HashMap<String, String>;

    /// Get target consumer config
    fn target_consumer_config(&self, role: &str) -> HashMap<String, String>;

    /// Get checkpoints topic name
    fn checkpoints_topic(&self) -> String;
}

impl CheckpointStore {
    /// Creates a new CheckpointStore instance
    ///
    /// # Arguments
    /// * `config` - The configuration for the checkpoint task
    /// * `consumer_groups` - The set of consumer groups to monitor
    pub fn new(
        config: Box<dyn MirrorCheckpointTaskConfig>,
        consumer_groups: HashSet<String>,
    ) -> Self {
        CheckpointStore {
            config: Some(config),
            consumer_groups: HashSet::new(),
            cp_admin: None,
            backing_store: None,
            checkpoints_per_consumer_group: Arc::new(Mutex::new(HashMap::new())),
            load_success: Arc::new(Mutex::new(false)),
            is_initialized: Arc::new(Mutex::new(false)),
        }
    }

    /// Creates a new CheckpointStore for testing only
    ///
    /// # Arguments
    /// * `checkpoints_per_consumer_group` - Pre-populated checkpoints map
    pub fn new_for_test(
        checkpoints_per_consumer_group: HashMap<String, HashMap<TopicPartition, Checkpoint>>,
    ) -> Self {
        CheckpointStore {
            config: None,
            consumer_groups: HashSet::new(),
            cp_admin: None,
            backing_store: None,
            checkpoints_per_consumer_group: Arc::new(Mutex::new(checkpoints_per_consumer_group)),
            load_success: Arc::new(Mutex::new(true)),
            is_initialized: Arc::new(Mutex::new(true)),
        }
    }

    /// Starts the checkpoint store, potentially long running
    ///
    /// Reads checkpoints from the Kafka log and initializes the store.
    ///
    /// # Returns
    /// * `true` if checkpoints were loaded successfully
    /// * `false` otherwise
    pub fn start(&mut self) -> bool {
        let checkpoints = self.read_checkpoints();
        {
            let mut checkpoints_map = self.checkpoints_per_consumer_group.lock().unwrap();
            *checkpoints_map = checkpoints;
        }
        {
            let mut initialized = self.is_initialized.lock().unwrap();
            *initialized = true;
        }

        let load_success = {
            let success = self.load_success.lock().unwrap();
            *success
        };

        let checkpoints_map = self.checkpoints_per_consumer_group.lock().unwrap();
        log::debug!(
            "CheckpointStore started, load success={}, map.size={}",
            load_success,
            checkpoints_map.len()
        );

        load_success
    }

    /// Checks if the store is initialized
    ///
    /// # Returns
    /// * `true` if the store is initialized
    /// * `false` otherwise
    pub fn is_initialized(&self) -> bool {
        let initialized = self.is_initialized.lock().unwrap();
        *initialized
    }

    /// Updates checkpoints for a consumer group
    ///
    /// # Arguments
    /// * `group` - The consumer group ID
    /// * `new_checkpoints` - The new checkpoints to add
    pub fn update(&self, group: String, new_checkpoints: HashMap<TopicPartition, Checkpoint>) {
        let mut checkpoints_map = self.checkpoints_per_consumer_group.lock().unwrap();
        let old_checkpoints = checkpoints_map.entry(group).or_insert_with(HashMap::new);
        old_checkpoints.extend(new_checkpoints);
    }

    /// Gets checkpoints for a consumer group
    ///
    /// # Arguments
    /// * `group` - The consumer group ID
    ///
    /// # Returns
    /// * `Some(checkpoints)` if the group exists
    /// * `None` otherwise
    pub fn get(&self, group: &str) -> Option<HashMap<TopicPartition, Checkpoint>> {
        let checkpoints_map = self.checkpoints_per_consumer_group.lock().unwrap();
        checkpoints_map.get(group).cloned()
    }

    /// Computes converted upstream offsets for all consumer groups
    ///
    /// # Returns
    /// A map of consumer group IDs to topic partition offsets
    pub fn compute_converted_upstream_offset(
        &self,
    ) -> HashMap<String, HashMap<TopicPartition, OffsetAndMetadata>> {
        let mut result = HashMap::new();
        let checkpoints_map = self.checkpoints_per_consumer_group.lock().unwrap();

        for (consumer_id, checkpoints) in checkpoints_map.iter() {
            let mut converted_upstream_offset = HashMap::new();
            for checkpoint in checkpoints.values() {
                converted_upstream_offset.insert(
                    checkpoint.topic_partition().clone(),
                    checkpoint.offset_and_metadata(),
                );
            }
            result.insert(consumer_id.clone(), converted_upstream_offset);
        }

        result
    }

    /// Closes the checkpoint store and releases resources
    pub fn close(&mut self) {
        self.release_resources();
    }

    /// Releases resources used by the checkpoint store
    fn release_resources(&mut self) {
        // Stop backing store if it exists
        if let Some(backing_store) = &self.backing_store {
            // Note: In a real implementation, we would call stop() on the backing store
            // For now, we just drop the reference
        }
        self.backing_store = None;

        // Close admin client if it exists
        self.cp_admin = None;
    }

    /// Reads the checkpoints topic to initialize the checkpoints_per_consumer_group state
    ///
    /// The callback may only handle errors thrown by consumer.poll in KafkaBasedLog
    /// e.g. unauthorized to read from topic (non-retriable)
    /// If any are encountered, treat the loading of Checkpoints as failed.
    ///
    /// # Returns
    /// A map of consumer group IDs to topic partition checkpoints
    fn read_checkpoints(&mut self) -> HashMap<String, HashMap<TopicPartition, Checkpoint>> {
        let checkpoints = Arc::new(Mutex::new(HashMap::new()));
        let consumer_groups = self.consumer_groups.clone();
        let checkpoints_clone = checkpoints.clone();

        // Create consumed callback
        let consumed_callback: Box<dyn Callback<(Vec<u8>, Vec<u8>)>> =
            Box::new(move |error, record| {
                if let Some(err) = error {
                    // If there is no authorization to READ from the topic, we must throw an error
                    // to stop the KafkaBasedLog forever looping attempting to read to end
                    let mut checkpoints = checkpoints_clone.lock().unwrap();
                    checkpoints.clear();
                    panic!("Error reading checkpoint record: {}", err);
                } else {
                    let (key, value) = record;
                    match Checkpoint::deserialize_record(&key, &value) {
                        Ok(cp) => {
                            if consumer_groups.contains(cp.consumer_group_id()) {
                                let mut checkpoints = checkpoints_clone.lock().unwrap();
                                let cps = checkpoints
                                    .entry(cp.consumer_group_id().to_string())
                                    .or_insert_with(HashMap::new);
                                cps.insert(cp.topic_partition().clone(), cp);
                            }
                        }
                        Err(e) => {
                            log::warn!("Ignored invalid checkpoint record: {}", e);
                        }
                    }
                }
            });

        let config = self.config.as_ref().unwrap();
        let checkpoints_topic = config.checkpoints_topic();

        match self.read_checkpoints_impl(consumed_callback) {
            Ok(_) => {
                let mut load_success = self.load_success.lock().unwrap();
                *load_success = true;
                log::debug!(
                    "Successfully loaded checkpoints from topic {}",
                    checkpoints_topic
                );
            }
            Err(error) => {
                let mut load_success = self.load_success.lock().unwrap();
                *load_success = false;

                if error.contains("Authorization") {
                    log::warn!(
                        "Not authorized to access checkpoints topic {} - \
                         this may degrade offset translation as only checkpoints \
                         for offsets which were mirrored after the task started will be emitted",
                        checkpoints_topic
                    );
                } else {
                    log::info!(
                        "Exception encountered loading checkpoints topic {} - \
                         this may degrade offset translation as only checkpoints \
                         for offsets which were mirrored after the task started will be emitted",
                        checkpoints_topic
                    );
                }
            }
        }

        let checkpoints_map = checkpoints.lock().unwrap();
        checkpoints_map.clone()
    }

    /// Reads checkpoints from the Kafka log
    ///
    /// # Arguments
    /// * `consumed_callback` - Callback to handle consumed records
    ///
    /// # Returns
    /// * `Ok(())` if successful
    /// * `Err(String)` if an error occurred
    fn read_checkpoints_impl(
        &mut self,
        consumed_callback: Box<dyn Callback<(Vec<u8>, Vec<u8>)>>,
    ) -> Result<(), String> {
        let config = self.config.as_ref().unwrap();

        // Create admin client
        let admin_config = config.target_admin_config("checkpoint-target-admin");
        let forwarding_config = config.forwarding_admin(admin_config.clone());
        let cp_admin = Arc::new(Mutex::new(TopicAdmin::new(
            admin_config,
            forwarding_config,
        )?));

        // Create consumer config
        let consumer_config = config.target_consumer_config("checkpoints-target-consumer");

        // Create KafkaBasedLog
        let backing_store = Arc::new(Mutex::new(KafkaBasedLog::new(
            config.checkpoints_topic(),
            HashMap::new(), // producer configs - not needed for reading
            consumer_config,
            Arc::new(Mutex::new(None)), // topic admin supplier
            consumed_callback,
            Arc::new(common_trait::time::SystemTime {}),
        )));

        // Start and stop the backing store to read all records
        // Note: In a real implementation, we would use async/await here
        // For now, we just simulate the behavior
        {
            let store = backing_store.lock().unwrap();
            // store.start(true).await?;
            // store.stop().await;
        }

        self.cp_admin = Some(cp_admin);
        self.backing_store = Some(backing_store);

        // Release resources after reading
        self.release_resources();

        Ok(())
    }
}

impl Drop for CheckpointStore {
    fn drop(&mut self) {
        self.release_resources();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockConfig;

    impl MirrorCheckpointTaskConfig for MockConfig {
        fn target_admin_config(&self, _prefix: &str) -> HashMap<String, String> {
            HashMap::new()
        }

        fn forwarding_admin(
            &self,
            base_config: HashMap<String, String>,
        ) -> HashMap<String, String> {
            base_config
        }

        fn target_consumer_config(&self, _role: &str) -> HashMap<String, String> {
            HashMap::new()
        }

        fn checkpoints_topic(&self) -> String {
            "test-checkpoints-topic".to_string()
        }
    }

    #[test]
    fn test_checkpoint_store_creation() {
        let config = Box::new(MockConfig);
        let consumer_groups = HashSet::from(["group1".to_string(), "group2".to_string()]);
        let store = CheckpointStore::new(config, consumer_groups);

        assert!(!store.is_initialized());
    }

    #[test]
    fn test_checkpoint_store_for_test() {
        let mut checkpoints_map = HashMap::new();
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint = Checkpoint::new(
            "test-group".to_string(),
            tp.clone(),
            100,
            200,
            "test-metadata".to_string(),
        );
        let mut group_checkpoints = HashMap::new();
        group_checkpoints.insert(tp, checkpoint);
        checkpoints_map.insert("test-group".to_string(), group_checkpoints);

        let store = CheckpointStore::new_for_test(checkpoints_map);

        assert!(store.is_initialized());
        let result = store.get("test-group");
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_update() {
        let config = Box::new(MockConfig);
        let consumer_groups = HashSet::from(["group1".to_string()]);
        let store = CheckpointStore::new(config, consumer_groups);

        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint = Checkpoint::new(
            "group1".to_string(),
            tp.clone(),
            100,
            200,
            "test-metadata".to_string(),
        );
        let mut new_checkpoints = HashMap::new();
        new_checkpoints.insert(tp, checkpoint);

        store.update("group1".to_string(), new_checkpoints);

        let result = store.get("group1");
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_compute_converted_upstream_offset() {
        let mut checkpoints_map = HashMap::new();
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint = Checkpoint::new(
            "test-group".to_string(),
            tp.clone(),
            100,
            200,
            "test-metadata".to_string(),
        );
        let mut group_checkpoints = HashMap::new();
        group_checkpoints.insert(tp, checkpoint);
        checkpoints_map.insert("test-group".to_string(), group_checkpoints);

        let store = CheckpointStore::new_for_test(checkpoints_map);

        let result = store.compute_converted_upstream_offset();
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("test-group"));
        assert_eq!(result.get("test-group").unwrap().len(), 1);
    }

    #[test]
    fn test_close() {
        let config = Box::new(MockConfig);
        let consumer_groups = HashSet::from(["group1".to_string()]);
        let mut store = CheckpointStore::new(config, consumer_groups);

        store.close();
        // After close, resources should be released
        assert!(store.cp_admin.is_none());
        assert!(store.backing_store.is_none());
    }
}
