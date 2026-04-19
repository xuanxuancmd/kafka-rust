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

//! Mock implementation of Kafka Consumer client.
//!
//! This corresponds to `org.apache.kafka.clients.consumer.KafkaConsumer` in Java.
//! Provides an in-memory mock for testing purposes with support for:
//! - Topic subscription: subscribe to topics
//! - Record consumption: poll for records
//! - Offset management: commit sync/async, seek, position

use common_trait::TopicPartition;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::consumer_record::{ConsumerRecord, ConsumerRecords};
use crate::offset_commit_callback::{CommitError, OffsetCommitCallback};
use crate::OffsetAndMetadata;

/// Callback type for async offset commit using closures.
///
/// This is an alternative to `OffsetCommitCallback` trait for convenience.
/// Uses `Box<dyn Fn>` to support closures that capture variables.
pub type OffsetCommitCallbackFn =
    Box<dyn Fn(Result<HashMap<TopicPartition, OffsetAndMetadata>, CommitError>) + Send + Sync>;

/// Error for consumer operations.
///
/// This corresponds to various consumer exceptions in Java.
#[derive(Debug, Clone)]
pub enum ConsumerError {
    /// No subscription or assignment.
    NoSubscription,
    /// Invalid offset.
    InvalidOffset {
        topic: String,
        partition: i32,
        offset: i64,
        message: String,
    },
    /// Partition not assigned.
    PartitionNotAssigned { topic: String, partition: i32 },
    /// Operation failed.
    Failed { message: String },
}

impl std::fmt::Display for ConsumerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumerError::NoSubscription => {
                write!(f, "Consumer is not subscribed to any topics")
            }
            ConsumerError::InvalidOffset {
                topic,
                partition,
                offset,
                message,
            } => {
                write!(
                    f,
                    "Invalid offset {} for topic '{}' partition {}: {}",
                    offset, topic, partition, message
                )
            }
            ConsumerError::PartitionNotAssigned { topic, partition } => {
                write!(
                    f,
                    "Partition {} of topic '{}' is not assigned to this consumer",
                    partition, topic
                )
            }
            ConsumerError::Failed { message } => {
                write!(f, "Consumer operation failed: {}", message)
            }
        }
    }
}

impl std::error::Error for ConsumerError {}

/// Internal record queue for a partition.
type PartitionRecordQueue = VecDeque<ConsumerRecord>;

/// Internal consumer state.
struct InternalConsumerState {
    subscribed_topics: HashSet<String>,
    partition_positions: HashMap<TopicPartition, i64>,
    committed_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    record_queues: HashMap<TopicPartition, PartitionRecordQueue>,
}

impl InternalConsumerState {
    fn new() -> Self {
        InternalConsumerState {
            subscribed_topics: HashSet::new(),
            partition_positions: HashMap::new(),
            committed_offsets: HashMap::new(),
            record_queues: HashMap::new(),
        }
    }
}

/// Mock Kafka Consumer for testing purposes.
///
/// This provides an in-memory implementation of the Kafka Consumer
/// that supports topic subscription, record consumption, and offset management.
/// All operations are synchronous and stored in memory.
///
/// Thread-safe via internal `Arc<Mutex<...>` wrapper.
///
/// This corresponds to `org.apache.kafka.clients.consumer.KafkaConsumer` in Java.
pub struct MockKafkaConsumer {
    state: Arc<Mutex<InternalConsumerState>>,
}

impl MockKafkaConsumer {
    /// Creates a new MockKafkaConsumer with empty state.
    pub fn new() -> Self {
        MockKafkaConsumer {
            state: Arc::new(Mutex::new(InternalConsumerState::new())),
        }
    }

    /// Subscribes the consumer to a list of topics.
    ///
    /// This corresponds to `KafkaConsumer.subscribe(Collection<String> topics)` in Java.
    ///
    /// # Arguments
    /// * `topics` - The topics to subscribe to
    ///
    /// # Behavior
    /// - Replaces any existing subscription
    /// - Initializes partition positions to 0 for all subscribed topics
    /// - Creates empty record queues for each subscribed topic's partitions
    pub fn subscribe(&self, topics: Vec<String>) {
        let mut state = self.state.lock().unwrap();

        // Clear existing subscription
        state.subscribed_topics.clear();
        state.partition_positions.clear();
        state.record_queues.clear();

        // Add new subscription
        for topic in topics {
            state.subscribed_topics.insert(topic.clone());
            // Initialize partition 0 by default (can be expanded via add_records)
            let tp = TopicPartition::new(&topic, 0);
            state.partition_positions.insert(tp.clone(), 0);
            state.record_queues.insert(tp, VecDeque::new());
        }
    }

    /// Adds records to the mock consumer for testing.
    ///
    /// This is a helper method for setting up test scenarios.
    /// Records are added to the internal queue and will be returned by `poll()`.
    ///
    /// # Arguments
    /// * `records` - Records to add, grouped by TopicPartition
    pub fn add_records(&self, records: HashMap<TopicPartition, Vec<ConsumerRecord>>) {
        let mut state = self.state.lock().unwrap();

        for (tp, mut new_records) in records {
            // Ensure partition is initialized if topic is subscribed
            if state.subscribed_topics.contains(tp.topic()) {
                // Initialize partition position if not exists
                if !state.partition_positions.contains_key(&tp) {
                    state.partition_positions.insert(tp.clone(), 0);
                }
                // Initialize queue if not exists
                if !state.record_queues.contains_key(&tp) {
                    state.record_queues.insert(tp.clone(), VecDeque::new());
                }

                // Sort records by offset before adding
                new_records.sort_by_key(|r| r.offset());

                let queue = state.record_queues.get_mut(&tp).unwrap();
                for record in new_records {
                    queue.push_back(record);
                }
            }
        }
    }

    /// Polls for records from subscribed topics.
    ///
    /// This corresponds to `KafkaConsumer.poll(Duration timeout)` in Java.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait (in mock, this is ignored; records are returned immediately)
    ///
    /// # Returns
    /// * `ConsumerRecords` containing all records available at current positions
    ///
    /// # Behavior
    /// - Returns records from subscribed topics starting at current partition positions
    /// - Advances partition positions after consuming records
    /// - Returns empty ConsumerRecords if no subscription or no records available
    pub fn poll(&self, _timeout: Duration) -> ConsumerRecords {
        let mut state = self.state.lock().unwrap();

        if state.subscribed_topics.is_empty() {
            return ConsumerRecords::empty();
        }

        // Clone subscribed topics to avoid borrow conflicts
        let subscribed_topics: Vec<String> = state.subscribed_topics.iter().cloned().collect();

        let mut result_records: HashMap<TopicPartition, Vec<ConsumerRecord>> = HashMap::new();

        // Collect records for each subscribed partition
        for topic in subscribed_topics {
            // Find all partitions for this topic
            let partitions: Vec<TopicPartition> = state
                .partition_positions
                .keys()
                .filter(|tp| tp.topic() == topic)
                .cloned()
                .collect();

            for tp in partitions {
                let position = state.partition_positions.get(&tp).copied().unwrap_or(0);

                if let Some(queue) = state.record_queues.get_mut(&tp) {
                    let mut partition_records: Vec<ConsumerRecord> = Vec::new();
                    let mut new_position = position;

                    // Take records from current position
                    while let Some(record) = queue.front() {
                        if record.offset() >= position {
                            let record = queue.pop_front().unwrap();
                            partition_records.push(record.clone());
                            new_position = record.offset() + 1;
                        } else {
                            // Skip records below current position
                            queue.pop_front();
                        }
                    }

                    if !partition_records.is_empty() {
                        state.partition_positions.insert(tp.clone(), new_position);
                        result_records.insert(tp, partition_records);
                    }
                }
            }
        }

        ConsumerRecords::new(result_records)
    }

    /// Synchronously commits offsets.
    ///
    /// This corresponds to `KafkaConsumer.commitSync()` in Java.
    /// Blocks until the commit succeeds (in mock, this is immediate).
    ///
    /// # Behavior
    /// - Commits current partition positions as the next offset to read
    /// - If no specific offsets provided, commits all current positions
    pub fn commit_sync(&self) -> Result<(), CommitError> {
        self.commit_sync_offsets(None)
    }

    /// Synchronously commits specific offsets.
    ///
    /// This corresponds to `KafkaConsumer.commitSync(Map<TopicPartition, OffsetAndMetadata>)` in Java.
    ///
    /// # Arguments
    /// * `offsets` - Specific offsets to commit (if None, commits current positions)
    pub fn commit_sync_offsets(
        &self,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
    ) -> Result<(), CommitError> {
        let mut state = self.state.lock().unwrap();

        if state.subscribed_topics.is_empty() {
            return Err(CommitError::new("Consumer is not subscribed to any topics"));
        }

        let offsets_to_commit = if let Some(offsets) = offsets {
            offsets
        } else {
            // Commit current positions
            state
                .partition_positions
                .iter()
                .map(|(tp, pos)| (tp.clone(), OffsetAndMetadata::new(*pos)))
                .collect()
        };

        for (tp, offset_meta) in offsets_to_commit {
            state.committed_offsets.insert(tp, offset_meta);
        }

        Ok(())
    }

    /// Asynchronously commits offsets.
    ///
    /// This corresponds to `KafkaConsumer.commitAsync()` in Java.
    /// In mock, this invokes the callback synchronously for testing convenience,
    /// but the semantics (callback-based result) are preserved.
    ///
    /// # Arguments
    /// * `callback` - Callback to invoke with commit result
    pub fn commit_async(&self, callback: Option<OffsetCommitCallbackFn>) {
        self.commit_async_offsets(None, callback);
    }

    /// Asynchronously commits specific offsets.
    ///
    /// This corresponds to `KafkaConsumer.commitAsync(Map<TopicPartition, OffsetAndMetadata>, OffsetCommitCallback)` in Java.
    ///
    /// # Arguments
    /// * `offsets` - Specific offsets to commit (if None, commits current positions)
    /// * `callback` - Callback to invoke with commit result
    pub fn commit_async_offsets(
        &self,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
        callback: Option<OffsetCommitCallbackFn>,
    ) {
        let result = self.commit_sync_offsets(offsets);

        if let Some(cb) = callback {
            match result {
                Ok(()) => {
                    let state = self.state.lock().unwrap();
                    let committed: HashMap<TopicPartition, OffsetAndMetadata> =
                        state.committed_offsets.clone();
                    cb(Ok(committed));
                }
                Err(e) => {
                    cb(Err(e));
                }
            }
        }
    }

    /// Seeks to a specific offset for a partition.
    ///
    /// This corresponds to `KafkaConsumer.seek(TopicPartition, long)` in Java.
    /// Overrides the fetch offset for the next `poll()` call.
    ///
    /// # Arguments
    /// * `partition` - The topic partition to seek
    /// * `offset` - The offset to seek to
    ///
    /// # Errors
    /// Returns error if partition is not subscribed or assigned
    pub fn seek(&self, partition: &TopicPartition, offset: i64) -> Result<(), ConsumerError> {
        let mut state = self.state.lock().unwrap();

        // Check if topic is subscribed
        if !state.subscribed_topics.contains(partition.topic()) {
            return Err(ConsumerError::PartitionNotAssigned {
                topic: partition.topic().to_string(),
                partition: partition.partition(),
            });
        }

        // Check if partition is tracked
        if !state.partition_positions.contains_key(partition) {
            return Err(ConsumerError::PartitionNotAssigned {
                topic: partition.topic().to_string(),
                partition: partition.partition(),
            });
        }

        // Update position
        state.partition_positions.insert(partition.clone(), offset);

        Ok(())
    }

    /// Returns the current position for a partition.
    ///
    /// This corresponds to `KafkaConsumer.position(TopicPartition)` in Java.
    /// Returns the offset of the next record that will be fetched.
    ///
    /// # Arguments
    /// * `partition` - The topic partition to query
    ///
    /// # Returns
    /// * The offset of the next record to be fetched
    ///
    /// # Errors
    /// Returns error if partition is not subscribed or assigned
    pub fn position(&self, partition: &TopicPartition) -> Result<i64, ConsumerError> {
        let state = self.state.lock().unwrap();

        // Check if topic is subscribed
        if !state.subscribed_topics.contains(partition.topic()) {
            return Err(ConsumerError::PartitionNotAssigned {
                topic: partition.topic().to_string(),
                partition: partition.partition(),
            });
        }

        // Check if partition is tracked
        if !state.partition_positions.contains_key(partition) {
            return Err(ConsumerError::PartitionNotAssigned {
                topic: partition.topic().to_string(),
                partition: partition.partition(),
            });
        }

        Ok(state
            .partition_positions
            .get(partition)
            .copied()
            .unwrap_or(0))
    }

    /// Returns the committed offset for a partition.
    ///
    /// This corresponds to `KafkaConsumer.committed(TopicPartition)` in Java.
    ///
    /// # Arguments
    /// * `partition` - The topic partition to query
    ///
    /// # Returns
    /// * The committed offset and metadata, or None if not committed
    pub fn committed(&self, partition: &TopicPartition) -> Option<OffsetAndMetadata> {
        let state = self.state.lock().unwrap();
        state.committed_offsets.get(partition).cloned()
    }

    // ===== Utility methods for testing =====

    /// Clears all state (subscriptions, positions, offsets, records).
    pub fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.subscribed_topics.clear();
        state.partition_positions.clear();
        state.committed_offsets.clear();
        state.record_queues.clear();
    }

    /// Returns the subscribed topics.
    pub fn subscription(&self) -> HashSet<String> {
        let state = self.state.lock().unwrap();
        state.subscribed_topics.clone()
    }

    /// Returns whether the consumer is subscribed to any topics.
    pub fn is_subscribed(&self) -> bool {
        let state = self.state.lock().unwrap();
        !state.subscribed_topics.is_empty()
    }

    /// Returns all committed offsets.
    pub fn all_committed(&self) -> HashMap<TopicPartition, OffsetAndMetadata> {
        let state = self.state.lock().unwrap();
        state.committed_offsets.clone()
    }

    /// Returns the number of pending records.
    pub fn pending_count(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.record_queues.values().map(|q| q.len()).sum()
    }
}

impl Default for MockKafkaConsumer {
    fn default() -> Self {
        MockKafkaConsumer::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_record() {
        let record = ConsumerRecord::new(
            "test-topic",
            0,
            100,
            Some(vec![1, 2]),
            Some(vec![3, 4]),
            1234567890,
        );
        assert_eq!(record.topic(), "test-topic");
        assert_eq!(record.partition(), 0);
        assert_eq!(record.offset(), 100);
        assert_eq!(record.key(), Some(&vec![1, 2]));
        assert_eq!(record.value(), Some(&vec![3, 4]));
        assert_eq!(record.timestamp(), 1234567890);
    }

    #[test]
    fn test_consumer_record_with_headers() {
        let headers = HashMap::from([
            ("header1".to_string(), vec![1]),
            ("header2".to_string(), vec![2]),
        ]);
        let record = ConsumerRecord::with_headers(
            "test-topic",
            0,
            100,
            Some(vec![1]),
            Some(vec![2]),
            12345,
            headers,
        );
        assert_eq!(record.headers().len(), 2);
    }

    #[test]
    fn test_consumer_records_empty() {
        let records = ConsumerRecords::empty();
        assert!(records.is_empty());
        assert_eq!(records.count(), 0);
    }

    #[test]
    fn test_consumer_records() {
        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        let r2 = ConsumerRecord::new("topic1", 0, 1, None, Some(vec![2]), 1001);
        let r3 = ConsumerRecord::new("topic1", 1, 0, None, Some(vec![3]), 1002);

        let tp0 = TopicPartition::new("topic1", 0);
        let tp1 = TopicPartition::new("topic1", 1);

        let records = ConsumerRecords::new(HashMap::from([
            (tp0.clone(), vec![r1, r2]),
            (tp1.clone(), vec![r3]),
        ]));

        assert!(!records.is_empty());
        assert_eq!(records.count(), 3);
        assert_eq!(records.partitions().len(), 2);
        assert_eq!(records.records(&tp0).unwrap().len(), 2);
        assert_eq!(records.records(&tp1).unwrap().len(), 1);
    }

    #[test]
    fn test_subscribe() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string(), "topic2".to_string()]);

        let subscription = consumer.subscription();
        assert_eq!(subscription.len(), 2);
        assert!(subscription.contains("topic1"));
        assert!(subscription.contains("topic2"));
        assert!(consumer.is_subscribed());
    }

    #[test]
    fn test_subscribe_replaces_existing() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);
        consumer.subscribe(vec!["topic2".to_string()]);

        let subscription = consumer.subscription();
        assert_eq!(subscription.len(), 1);
        assert!(subscription.contains("topic2"));
        assert!(!subscription.contains("topic1"));
    }

    #[test]
    fn test_poll_no_subscription() {
        let consumer = MockKafkaConsumer::new();
        let records = consumer.poll(Duration::from_millis(100));
        assert!(records.is_empty());
    }

    #[test]
    fn test_poll_with_records() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        let r2 = ConsumerRecord::new("topic1", 0, 1, None, Some(vec![2]), 1001);

        consumer.add_records(HashMap::from([(tp.clone(), vec![r1, r2])]));

        let records = consumer.poll(Duration::from_millis(100));
        assert_eq!(records.count(), 2);

        // Check position advanced
        let pos = consumer.position(&tp).unwrap();
        assert_eq!(pos, 2);
    }

    #[test]
    fn test_poll_multiple_partitions() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp0 = TopicPartition::new("topic1", 0);
        let tp1 = TopicPartition::new("topic1", 1);

        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        let r2 = ConsumerRecord::new("topic1", 1, 0, None, Some(vec![2]), 1001);

        consumer.add_records(HashMap::from([
            (tp0.clone(), vec![r1]),
            (tp1.clone(), vec![r2]),
        ]));

        let records = consumer.poll(Duration::from_millis(100));
        assert_eq!(records.count(), 2);
        assert_eq!(records.partitions().len(), 2);
    }

    #[test]
    fn test_poll_respects_position() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        let r2 = ConsumerRecord::new("topic1", 0, 1, None, Some(vec![2]), 1001);
        let r3 = ConsumerRecord::new("topic1", 0, 2, None, Some(vec![3]), 1002);

        consumer.add_records(HashMap::from([(tp.clone(), vec![r1, r2, r3])]));

        // First poll gets all 3
        let records1 = consumer.poll(Duration::from_millis(100));
        assert_eq!(records1.count(), 3);
        assert_eq!(consumer.position(&tp).unwrap(), 3);

        // Second poll gets nothing (position is at end)
        let records2 = consumer.poll(Duration::from_millis(100));
        assert!(records2.is_empty());
    }

    #[test]
    fn test_seek() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        let r2 = ConsumerRecord::new("topic1", 0, 1, None, Some(vec![2]), 1001);
        let r3 = ConsumerRecord::new("topic1", 0, 2, None, Some(vec![3]), 1002);

        consumer.add_records(HashMap::from([(tp.clone(), vec![r1, r2, r3])]));

        // Poll all
        let _ = consumer.poll(Duration::from_millis(100));
        assert_eq!(consumer.position(&tp).unwrap(), 3);

        // Seek back to offset 1
        consumer.seek(&tp, 1).unwrap();
        assert_eq!(consumer.position(&tp).unwrap(), 1);

        // Note: seek doesn't restore records in mock (they were consumed)
        // This tests position tracking behavior
    }

    #[test]
    fn test_seek_unassigned_partition() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic2", 0);
        let result = consumer.seek(&tp, 0);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConsumerError::PartitionNotAssigned { .. }
        ));
    }

    #[test]
    fn test_position() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        let pos = consumer.position(&tp).unwrap();
        assert_eq!(pos, 0);

        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        consumer.add_records(HashMap::from([(tp.clone(), vec![r1])]));

        let _ = consumer.poll(Duration::from_millis(100));
        let pos = consumer.position(&tp).unwrap();
        assert_eq!(pos, 1);
    }

    #[test]
    fn test_position_unassigned_partition() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic2", 0);
        let result = consumer.position(&tp);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConsumerError::PartitionNotAssigned { .. }
        ));
    }

    #[test]
    fn test_commit_sync() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        let r2 = ConsumerRecord::new("topic1", 0, 1, None, Some(vec![2]), 1001);

        consumer.add_records(HashMap::from([(tp.clone(), vec![r1, r2])]));
        let _ = consumer.poll(Duration::from_millis(100));

        // Commit current positions
        consumer.commit_sync().unwrap();

        let committed = consumer.committed(&tp).unwrap();
        assert_eq!(committed.offset(), 2);
    }

    #[test]
    fn test_commit_sync_specific_offsets() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);

        consumer.commit_sync_offsets(Some(offsets)).unwrap();

        let committed = consumer.committed(&tp).unwrap();
        assert_eq!(committed.offset(), 100);
    }

    #[test]
    fn test_commit_sync_no_subscription() {
        let consumer = MockKafkaConsumer::new();
        let result = consumer.commit_sync();
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_async() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(50))]);

        // Create a simple callback that doesn't capture variables
        consumer.commit_async_offsets(Some(offsets), None);

        // Verify the committed offset directly
        let committed = consumer.committed(&tp).unwrap();
        assert_eq!(committed.offset(), 50);
    }

    #[test]
    fn test_commit_async_with_callback_invocation() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(50))]);

        // Use a callback that verifies the result
        let callback = Box::new(
            |result: Result<HashMap<TopicPartition, OffsetAndMetadata>, CommitError>| {
                assert!(result.is_ok());
                let committed_offsets = result.unwrap();
                assert!(committed_offsets.contains_key(&TopicPartition::new("topic1", 0)));
            },
        );

        consumer.commit_async_offsets(Some(offsets), Some(callback));
    }

    #[test]
    fn test_commit_async_error_no_subscription() {
        let consumer = MockKafkaConsumer::new();
        // Not subscribed - should fail, verify no committed offsets

        consumer.commit_async(None);

        assert!(consumer.all_committed().is_empty());
    }

    #[test]
    fn test_clear() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        consumer.add_records(HashMap::from([(tp.clone(), vec![r1])]));
        let _ = consumer.poll(Duration::from_millis(100));
        consumer.commit_sync().unwrap();

        consumer.clear();

        assert!(!consumer.is_subscribed());
        assert!(consumer.all_committed().is_empty());
        assert_eq!(consumer.pending_count(), 0);
    }

    #[test]
    fn test_records_sorted_by_offset() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        // Add records in reverse order
        let r3 = ConsumerRecord::new("topic1", 0, 2, None, Some(vec![3]), 1002);
        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        let r2 = ConsumerRecord::new("topic1", 0, 1, None, Some(vec![2]), 1001);

        consumer.add_records(HashMap::from([(tp.clone(), vec![r3, r1, r2])]));

        let records = consumer.poll(Duration::from_millis(100));
        let partition_records = records.records(&tp).unwrap();

        // Should be returned in order
        assert_eq!(partition_records[0].offset(), 0);
        assert_eq!(partition_records[1].offset(), 1);
        assert_eq!(partition_records[2].offset(), 2);
    }

    #[test]
    fn test_pending_count() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        let r2 = ConsumerRecord::new("topic1", 0, 1, None, Some(vec![2]), 1001);

        consumer.add_records(HashMap::from([(tp.clone(), vec![r1, r2])]));
        assert_eq!(consumer.pending_count(), 2);

        let _ = consumer.poll(Duration::from_millis(100));
        assert_eq!(consumer.pending_count(), 0);
    }

    #[test]
    fn test_consumer_records_iter() {
        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        let r2 = ConsumerRecord::new("topic1", 1, 0, None, Some(vec![2]), 1001);

        let tp0 = TopicPartition::new("topic1", 0);
        let tp1 = TopicPartition::new("topic1", 1);

        let records = ConsumerRecords::new(HashMap::from([(tp0, vec![r1]), (tp1, vec![r2])]));

        let count = records.iter().count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_offset_and_metadata_with_consumer() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["topic1".to_string()]);

        let tp = TopicPartition::new("topic1", 0);
        let offset_meta = OffsetAndMetadata::with_metadata(100, "test-metadata");

        consumer
            .commit_sync_offsets(Some(HashMap::from([(tp.clone(), offset_meta)])))
            .unwrap();

        let committed = consumer.committed(&tp).unwrap();
        assert_eq!(committed.offset(), 100);
        assert_eq!(committed.metadata(), "test-metadata");
    }
}
