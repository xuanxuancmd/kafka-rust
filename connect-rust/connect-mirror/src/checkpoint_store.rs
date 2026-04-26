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

//! CheckpointStore - Store for consumer group checkpoints.
//!
//! This module provides the CheckpointStore which is responsible for
//! reading and storing checkpoint records from the checkpoints topic.
//! It maintains a two-level mapping: consumer_group -> TopicPartition -> Checkpoint.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.CheckpointStore

use std::collections::HashMap;
use std::time::Duration;

use common_trait::protocol::SchemaError;
use common_trait::TopicPartition;
use connect_mirror_client::Checkpoint;
use kafka_clients_mock::MockKafkaConsumer;

/// CheckpointStore - stores checkpoints for consumer groups.
///
/// The CheckpointStore is responsible for:
/// - Reading Checkpoint records from the checkpoints topic
/// - Maintaining a mapping of consumer_group -> TopicPartition -> Checkpoint
/// - Providing checkpoint lookup for specific consumer groups
///
/// Used by MirrorCheckpointTask to sync consumer group offsets from
/// source cluster to target cluster.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.CheckpointStore
pub struct CheckpointStore {
    /// Kafka consumer for reading checkpoints topic
    consumer: MockKafkaConsumer,
    /// Topic to read checkpoints from
    checkpoints_topic: String,
    /// Internal storage: consumer_group -> TopicPartition -> Checkpoint
    checkpoints: HashMap<String, HashMap<TopicPartition, Checkpoint>>,
    /// Flag indicating if the store has completed initial read
    initialized: bool,
}

impl std::fmt::Debug for CheckpointStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointStore")
            .field("checkpoints_topic", &self.checkpoints_topic)
            .field("initialized", &self.initialized)
            .field("group_count", &self.checkpoints.len())
            .field(
                "total_checkpoint_count",
                &self.checkpoints.values().map(|m| m.len()).sum::<usize>(),
            )
            .finish()
    }
}

impl CheckpointStore {
    /// Creates a new CheckpointStore.
    ///
    /// # Arguments
    /// * `consumer` - Kafka consumer for reading checkpoints topic
    /// * `checkpoints_topic` - Topic to read checkpoints from
    pub fn new(consumer: MockKafkaConsumer, checkpoints_topic: impl Into<String>) -> Self {
        CheckpointStore {
            consumer,
            checkpoints_topic: checkpoints_topic.into(),
            checkpoints: HashMap::new(),
            initialized: false,
        }
    }

    /// Returns the checkpoints topic name.
    pub fn checkpoints_topic(&self) -> &str {
        &self.checkpoints_topic
    }

    /// Returns whether the store has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Returns the number of consumer groups with checkpoint data.
    pub fn group_count(&self) -> usize {
        self.checkpoints.len()
    }

    /// Returns the number of checkpoints for a specific consumer group.
    pub fn checkpoint_count(&self, group: &str) -> usize {
        self.checkpoints.get(group).map(|m| m.len()).unwrap_or(0)
    }

    /// Returns total checkpoint count across all groups.
    pub fn total_checkpoint_count(&self) -> usize {
        self.checkpoints.values().map(|m| m.len()).sum()
    }

    /// Starts reading from the checkpoints topic.
    ///
    /// This method subscribes to the checkpoints topic and consumes
    /// all existing Checkpoint records, building the internal mapping.
    ///
    /// In Java, this uses KafkaBasedLog which reads to end before returning.
    /// In mock implementation, we poll until no more records are available.
    ///
    /// Returns the number of Checkpoint records read.
    ///
    /// Corresponds to Java: CheckpointStore.start()
    pub fn start(&mut self) -> Result<usize, SchemaError> {
        // Subscribe to checkpoints topic if not already subscribed
        // Note: subscribe() clears existing state, so we check first
        let subscription = self.consumer.subscription();
        if !subscription.contains(&self.checkpoints_topic) {
            self.consumer
                .subscribe(vec![self.checkpoints_topic.clone()]);
        }

        let mut records_read = 0;

        // Poll until no more records
        loop {
            let records = self.consumer.poll(Duration::from_millis(100));

            if records.is_empty() {
                break;
            }

            // Process each record
            for record in records.iter() {
                if let Some(key) = record.key() {
                    if let Some(value) = record.value() {
                        // Deserialize Checkpoint from key/value
                        let checkpoint = Checkpoint::deserialize_record(key, value)?;

                        // Store the Checkpoint
                        self.handle_record(checkpoint);
                        records_read += 1;
                    }
                }
            }
        }

        self.initialized = true;
        Ok(records_read)
    }

    /// Handles a single Checkpoint record.
    ///
    /// This method updates the internal storage with a new Checkpoint.
    /// The Checkpoint is stored in a two-level HashMap:
    /// consumer_group -> TopicPartition -> Checkpoint
    ///
    /// Corresponds to Java: CheckpointStore.handleRecord()
    fn handle_record(&mut self, checkpoint: Checkpoint) {
        let group = checkpoint.consumer_group_id().to_string();
        let tp = checkpoint.topic_partition().clone();

        // Get or create the inner map for this consumer group
        let group_checkpoints = self.checkpoints.entry(group).or_insert_with(HashMap::new);

        // Insert/update the checkpoint for this TopicPartition
        group_checkpoints.insert(tp, checkpoint);
    }

    /// Stores a checkpoint in the internal cache.
    ///
    /// This method adds or updates a checkpoint for a specific consumer group
    /// and TopicPartition.
    ///
    /// # Arguments
    /// * `checkpoint` - The checkpoint to store
    ///
    /// Corresponds to Java: CheckpointStore.put()
    pub fn put(&mut self, checkpoint: Checkpoint) {
        self.handle_record(checkpoint);
    }

    /// Gets all checkpoints for a specific consumer group.
    ///
    /// Returns a reference to the HashMap of TopicPartition -> Checkpoint
    /// for the given consumer group.
    ///
    /// # Arguments
    /// * `group` - The consumer group ID
    ///
    /// # Returns
    /// * `Some(&HashMap<TopicPartition, Checkpoint>)` if the group exists
    /// * `None` if the group has no checkpoints
    ///
    /// Corresponds to Java: CheckpointStore.get(String group)
    pub fn get(&self, group: &str) -> Option<&HashMap<TopicPartition, Checkpoint>> {
        self.checkpoints.get(group)
    }

    /// Gets a specific checkpoint for a consumer group and TopicPartition.
    ///
    /// # Arguments
    /// * `group` - The consumer group ID
    /// * `tp` - The TopicPartition
    ///
    /// # Returns
    /// * `Some(&Checkpoint)` if the checkpoint exists
    /// * `None` if no checkpoint for this group and partition
    pub fn get_checkpoint(&self, group: &str, tp: &TopicPartition) -> Option<&Checkpoint> {
        self.checkpoints.get(group).and_then(|m| m.get(tp))
    }

    /// Stops the CheckpointStore.
    ///
    /// This clears the internal state and unsubscribes the consumer.
    ///
    /// Corresponds to Java: CheckpointStore.stop() / close()
    pub fn stop(&mut self) {
        self.consumer.clear();
        self.checkpoints.clear();
        self.initialized = false;
    }

    /// Adds a checkpoint directly to the store (for testing).
    ///
    /// This bypasses the consumer and directly adds a checkpoint to internal storage.
    pub fn add_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.handle_record(checkpoint);
    }

    /// Gets all checkpoints (for testing/debugging).
    pub fn get_all_checkpoints(&self) -> &HashMap<String, HashMap<TopicPartition, Checkpoint>> {
        &self.checkpoints
    }
}

impl Default for CheckpointStore {
    fn default() -> Self {
        CheckpointStore::new(MockKafkaConsumer::new(), "checkpoints")
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_clients_mock::ConsumerRecord;

    #[test]
    fn test_checkpoint_store_new() {
        let consumer = MockKafkaConsumer::new();
        let store = CheckpointStore::new(consumer, "mm2-checkpoints");

        assert_eq!(store.checkpoints_topic(), "mm2-checkpoints");
        assert!(!store.is_initialized());
        assert_eq!(store.group_count(), 0);
        assert_eq!(store.total_checkpoint_count(), 0);
    }

    #[test]
    fn test_checkpoint_store_default() {
        let store = CheckpointStore::default();

        assert_eq!(store.checkpoints_topic(), "checkpoints");
        assert!(!store.is_initialized());
    }

    #[test]
    fn test_start_empty_topic() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["checkpoints".to_string()]);
        let mut store = CheckpointStore::new(consumer, "checkpoints");

        let result = store.start().unwrap();
        assert_eq!(result, 0);
        assert!(store.is_initialized());
    }

    #[test]
    fn test_start_with_records() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["checkpoints".to_string()]);

        // Create Checkpoint records for different groups and partitions
        let tp0 = TopicPartition::new("source-topic", 0);
        let tp1 = TopicPartition::new("source-topic", 1);

        let checkpoint1 = Checkpoint::new("group1", tp0.clone(), 100, 200, "");
        let checkpoint2 = Checkpoint::new("group1", tp1.clone(), 50, 100, "");
        let checkpoint3 = Checkpoint::new("group2", tp0.clone(), 150, 300, "");

        // Serialize and add to consumer
        let key1 = checkpoint1.record_key().unwrap();
        let value1 = checkpoint1.record_value().unwrap();
        let key2 = checkpoint2.record_key().unwrap();
        let value2 = checkpoint2.record_value().unwrap();
        let key3 = checkpoint3.record_key().unwrap();
        let value3 = checkpoint3.record_value().unwrap();

        let tp_topic = TopicPartition::new("checkpoints", 0);
        let record1 = ConsumerRecord::new("checkpoints", 0, 0, Some(key1), Some(value1), 1000);
        let record2 = ConsumerRecord::new("checkpoints", 0, 1, Some(key2), Some(value2), 1001);
        let record3 = ConsumerRecord::new("checkpoints", 0, 2, Some(key3), Some(value3), 1002);

        consumer.add_records(HashMap::from([(
            tp_topic.clone(),
            vec![record1, record2, record3],
        )]));

        let mut store = CheckpointStore::new(consumer, "checkpoints");
        let result = store.start().unwrap();

        assert_eq!(result, 3);
        assert!(store.is_initialized());
        assert_eq!(store.group_count(), 2);
        assert_eq!(store.checkpoint_count("group1"), 2);
        assert_eq!(store.checkpoint_count("group2"), 1);
    }

    #[test]
    fn test_put_and_get() {
        let mut store = CheckpointStore::default();

        let tp = TopicPartition::new("topic1", 0);
        let checkpoint = Checkpoint::new("group1", tp.clone(), 100, 200, "metadata1");

        store.put(checkpoint);

        // Get all checkpoints for group1
        let group_checkpoints = store.get("group1").unwrap();
        assert_eq!(group_checkpoints.len(), 1);

        // Get specific checkpoint
        let cp = store.get_checkpoint("group1", &tp).unwrap();
        assert_eq!(cp.upstream_offset(), 100);
        assert_eq!(cp.downstream_offset(), 200);
        assert_eq!(cp.metadata(), "metadata1");
    }

    #[test]
    fn test_get_nonexistent_group() {
        let store = CheckpointStore::default();

        assert_eq!(store.get("unknown-group"), None);
        assert_eq!(store.checkpoint_count("unknown-group"), 0);
    }

    #[test]
    fn test_get_checkpoint_nonexistent_partition() {
        let mut store = CheckpointStore::default();

        let tp = TopicPartition::new("topic1", 0);
        let checkpoint = Checkpoint::new("group1", tp.clone(), 100, 200, "");
        store.put(checkpoint);

        // Different partition should return None
        let tp_other = TopicPartition::new("topic1", 1);
        assert_eq!(store.get_checkpoint("group1", &tp_other), None);

        // Different topic should return None
        let tp_other_topic = TopicPartition::new("topic2", 0);
        assert_eq!(store.get_checkpoint("group1", &tp_other_topic), None);
    }

    #[test]
    fn test_put_overwrites_existing() {
        let mut store = CheckpointStore::default();

        let tp = TopicPartition::new("topic1", 0);

        // Put initial checkpoint
        let checkpoint1 = Checkpoint::new("group1", tp.clone(), 100, 200, "old");
        store.put(checkpoint1);

        // Put updated checkpoint for same group and partition
        let checkpoint2 = Checkpoint::new("group1", tp.clone(), 150, 300, "new");
        store.put(checkpoint2);

        // Should have one checkpoint with updated values
        let group_checkpoints = store.get("group1").unwrap();
        assert_eq!(group_checkpoints.len(), 1);

        let cp = store.get_checkpoint("group1", &tp).unwrap();
        assert_eq!(cp.upstream_offset(), 150);
        assert_eq!(cp.downstream_offset(), 300);
        assert_eq!(cp.metadata(), "new");
    }

    #[test]
    fn test_multiple_groups_multiple_partitions() {
        let mut store = CheckpointStore::default();

        let tp0 = TopicPartition::new("topic1", 0);
        let tp1 = TopicPartition::new("topic1", 1);
        let tp2 = TopicPartition::new("topic2", 0);

        // Add checkpoints for multiple groups and partitions
        store.put(Checkpoint::new("group1", tp0.clone(), 100, 200, ""));
        store.put(Checkpoint::new("group1", tp1.clone(), 50, 100, ""));
        store.put(Checkpoint::new("group2", tp0.clone(), 150, 300, ""));
        store.put(Checkpoint::new("group2", tp2.clone(), 200, 400, ""));
        store.put(Checkpoint::new("group3", tp1.clone(), 75, 150, ""));

        assert_eq!(store.group_count(), 3);
        assert_eq!(store.total_checkpoint_count(), 5);

        // Verify group1
        assert_eq!(store.checkpoint_count("group1"), 2);
        assert_eq!(
            store
                .get_checkpoint("group1", &tp0)
                .unwrap()
                .upstream_offset(),
            100
        );
        assert_eq!(
            store
                .get_checkpoint("group1", &tp1)
                .unwrap()
                .upstream_offset(),
            50
        );

        // Verify group2
        assert_eq!(store.checkpoint_count("group2"), 2);
        assert_eq!(
            store
                .get_checkpoint("group2", &tp0)
                .unwrap()
                .upstream_offset(),
            150
        );
        assert_eq!(
            store
                .get_checkpoint("group2", &tp2)
                .unwrap()
                .upstream_offset(),
            200
        );

        // Verify group3
        assert_eq!(store.checkpoint_count("group3"), 1);
        assert_eq!(
            store
                .get_checkpoint("group3", &tp1)
                .unwrap()
                .upstream_offset(),
            75
        );
    }

    #[test]
    fn test_stop() {
        let mut store = CheckpointStore::default();
        store.initialized = true;

        let tp = TopicPartition::new("topic1", 0);
        store.put(Checkpoint::new("group1", tp.clone(), 100, 200, ""));

        assert_eq!(store.group_count(), 1);
        assert!(store.is_initialized());

        store.stop();

        assert!(!store.is_initialized());
        assert_eq!(store.group_count(), 0);
        assert_eq!(store.total_checkpoint_count(), 0);
    }

    #[test]
    fn test_full_workflow() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["checkpoints".to_string()]);

        // Create Checkpoint records
        let tp0 = TopicPartition::new("source-topic", 0);
        let tp1 = TopicPartition::new("source-topic", 1);

        let checkpoint0_1 = Checkpoint::new("group1", tp0.clone(), 100, 200, "meta1");
        let checkpoint0_2 = Checkpoint::new("group1", tp1.clone(), 50, 100, "meta2");
        let checkpoint0_3 = Checkpoint::new("group2", tp0.clone(), 150, 300, "meta3");

        // Serialize
        let records: Vec<ConsumerRecord> = vec![
            ConsumerRecord::new(
                "checkpoints",
                0,
                0,
                Some(checkpoint0_1.record_key().unwrap()),
                Some(checkpoint0_1.record_value().unwrap()),
                1000,
            ),
            ConsumerRecord::new(
                "checkpoints",
                0,
                1,
                Some(checkpoint0_2.record_key().unwrap()),
                Some(checkpoint0_2.record_value().unwrap()),
                1001,
            ),
            ConsumerRecord::new(
                "checkpoints",
                0,
                2,
                Some(checkpoint0_3.record_key().unwrap()),
                Some(checkpoint0_3.record_value().unwrap()),
                1002,
            ),
        ];

        let tp_topic = TopicPartition::new("checkpoints", 0);
        consumer.add_records(HashMap::from([(tp_topic.clone(), records)]));

        // Create store and start
        let mut store = CheckpointStore::new(consumer, "checkpoints");
        let result = store.start().unwrap();

        assert_eq!(result, 3);
        assert!(store.is_initialized());

        // Verify checkpoints
        assert_eq!(store.group_count(), 2);

        // Check group1 checkpoints
        let group1 = store.get("group1").unwrap();
        assert_eq!(group1.len(), 2);

        let cp0 = store.get_checkpoint("group1", &tp0).unwrap();
        assert_eq!(cp0.upstream_offset(), 100);
        assert_eq!(cp0.downstream_offset(), 200);

        let cp1 = store.get_checkpoint("group1", &tp1).unwrap();
        assert_eq!(cp1.upstream_offset(), 50);
        assert_eq!(cp1.downstream_offset(), 100);

        // Check group2 checkpoints
        let group2 = store.get("group2").unwrap();
        assert_eq!(group2.len(), 1);

        let cp2 = store.get_checkpoint("group2", &tp0).unwrap();
        assert_eq!(cp2.upstream_offset(), 150);
        assert_eq!(cp2.downstream_offset(), 300);

        // Stop
        store.stop();
        assert!(!store.is_initialized());
    }

    #[test]
    fn test_handle_bad_records() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["checkpoints".to_string()]);

        // Create a valid checkpoint
        let tp = TopicPartition::new("source-topic", 0);
        let valid_checkpoint = Checkpoint::new("group1", tp.clone(), 100, 200, "");
        let valid_key = valid_checkpoint.record_key().unwrap();
        let valid_value = valid_checkpoint.record_value().unwrap();

        // Add valid record, record with null value, and record with bad key
        let tp_topic = TopicPartition::new("checkpoints", 0);
        let records: Vec<ConsumerRecord> = vec![
            // Valid record
            ConsumerRecord::new(
                "checkpoints",
                0,
                0,
                Some(valid_key),
                Some(valid_value),
                1000,
            ),
            // Record with null value (should be skipped)
            ConsumerRecord::new("checkpoints", 0, 1, Some(vec![1, 2, 3]), None, 1001),
            // Record with null key (should be skipped)
            ConsumerRecord::new("checkpoints", 0, 2, None, Some(vec![1, 2, 3]), 1002),
        ];

        consumer.add_records(HashMap::from([(tp_topic.clone(), records)]));

        let mut store = CheckpointStore::new(consumer, "checkpoints");
        let result = store.start().unwrap();

        // Only one valid record should be processed
        assert_eq!(result, 1);
        assert_eq!(store.group_count(), 1);
    }
}
