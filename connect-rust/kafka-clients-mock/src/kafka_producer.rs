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

//! Mock implementation of Kafka Producer client.
//!
//! This corresponds to `org.apache.kafka.clients.producer.KafkaProducer` in Java.
//! Provides an in-memory mock for testing purposes with support for:
//! - Record sending: send records to topics
//! - Flush: flush pending records
//! - Partition metadata: query partition info
//! - Close: close producer and reject further operations

use common_trait::TopicPartition;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::producer_record::ProducerRecord;
use crate::RecordMetadata;

/// Metadata for a partition.
///
/// This corresponds to `org.apache.kafka.common.PartitionInfo` in Java.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    partition: i32,
    leader: i32,
    replicas: Vec<i32>,
    in_sync_replicas: Vec<i32>,
}

impl PartitionInfo {
    /// Creates a new partition info.
    pub fn new(
        partition: i32,
        leader: i32,
        replicas: Vec<i32>,
        in_sync_replicas: Vec<i32>,
    ) -> Self {
        PartitionInfo {
            partition,
            leader,
            replicas,
            in_sync_replicas,
        }
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the leader broker ID.
    pub fn leader(&self) -> i32 {
        self.leader
    }

    /// Returns the replica broker IDs.
    pub fn replicas(&self) -> &Vec<i32> {
        &self.replicas
    }

    /// Returns the in-sync replica broker IDs.
    pub fn in_sync_replicas(&self) -> &Vec<i32> {
        &self.in_sync_replicas
    }
}

/// Error for producer operations.
///
/// This corresponds to various producer exceptions in Java.
#[derive(Debug, Clone)]
pub enum ProducerError {
    /// Producer is closed.
    Closed { message: String },
    /// Topic does not exist.
    TopicDoesNotExist { topic: String },
    /// Invalid partition.
    InvalidPartition { topic: String, partition: i32 },
    /// Send failed.
    SendFailed { topic: String, message: String },
}

impl std::fmt::Display for ProducerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProducerError::Closed { message } => {
                write!(f, "Producer is closed: {}", message)
            }
            ProducerError::TopicDoesNotExist { topic } => {
                write!(f, "Topic '{}' does not exist", topic)
            }
            ProducerError::InvalidPartition { topic, partition } => {
                write!(f, "Invalid partition {} for topic '{}'", partition, topic)
            }
            ProducerError::SendFailed { topic, message } => {
                write!(f, "Send failed for topic '{}': {}", topic, message)
            }
        }
    }
}

impl std::error::Error for ProducerError {}

/// Callback type for async send result.
///
/// This corresponds to `org.apache.kafka.clients.producer.Callback` in Java.
pub type SendCallbackFn = Box<dyn Fn(Result<RecordMetadata, ProducerError>) + Send + Sync>;

/// Internal stored record with metadata.
struct StoredRecord {
    topic: String,
    partition: i32,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    timestamp: i64,
    offset: i64,
}

impl StoredRecord {
    fn to_record_metadata(&self) -> RecordMetadata {
        let tp = TopicPartition::new(&self.topic, self.partition);
        let key_size = self.key.as_ref().map(|k| k.len() as i32).unwrap_or(0);
        let value_size = self.value.as_ref().map(|v| v.len() as i32).unwrap_or(0);
        RecordMetadata::new(tp, self.offset, 0, self.timestamp, key_size, value_size)
    }
}

/// Internal producer state.
struct InternalProducerState {
    is_closed: bool,
    topics: HashMap<String, TopicMetadata>,
    pending_records: VecDeque<StoredRecord>,
    next_offset: HashMap<TopicPartition, i64>,
}

/// Internal topic metadata.
struct TopicMetadata {
    num_partitions: i32,
    partition_infos: Vec<PartitionInfo>,
}

impl TopicMetadata {
    fn new(num_partitions: i32) -> Self {
        let partition_infos: Vec<PartitionInfo> = (0..num_partitions)
            .map(|p| PartitionInfo::new(p, 0, vec![0, 1, 2], vec![0, 1]))
            .collect();
        TopicMetadata {
            num_partitions,
            partition_infos,
        }
    }
}

impl InternalProducerState {
    fn new() -> Self {
        InternalProducerState {
            is_closed: false,
            topics: HashMap::new(),
            pending_records: VecDeque::new(),
            next_offset: HashMap::new(),
        }
    }
}

/// Mock Kafka Producer for testing purposes.
///
/// This provides an in-memory implementation of the Kafka Producer
/// that supports record sending, flushing, partition metadata, and closing.
/// All operations are synchronous and stored in memory.
///
/// Thread-safe via internal `Arc<Mutex<...>` wrapper.
///
/// This corresponds to `org.apache.kafka.clients.producer.KafkaProducer` in Java.
pub struct MockKafkaProducer {
    state: Arc<Mutex<InternalProducerState>>,
}

impl MockKafkaProducer {
    /// Creates a new MockKafkaProducer with empty state.
    pub fn new() -> Self {
        MockKafkaProducer {
            state: Arc::new(Mutex::new(InternalProducerState::new())),
        }
    }

    /// Creates a MockKafkaProducer with pre-existing topics.
    ///
    /// This is useful for testing partition routing.
    pub fn with_topics(topics: HashMap<String, i32>) -> Self {
        let mut producer = MockKafkaProducer::new();
        for (topic_name, num_partitions) in topics {
            producer.add_topic(topic_name, num_partitions);
        }
        producer
    }

    /// Adds a topic with the specified number of partitions.
    ///
    /// This is a helper method for setting up test scenarios.
    pub fn add_topic(&self, topic_name: impl Into<String>, num_partitions: i32) {
        let mut state = self.state.lock().unwrap();
        let name = topic_name.into();
        state
            .topics
            .insert(name.clone(), TopicMetadata::new(num_partitions));

        // Initialize offsets for each partition
        for p in 0..num_partitions {
            let tp = TopicPartition::new(&name, p);
            state.next_offset.insert(tp, 0);
        }
    }

    /// Sends a record to a topic.
    ///
    /// This corresponds to `KafkaProducer.send(ProducerRecord)` in Java.
    /// In mock, the record is stored in an internal buffer and RecordMetadata is returned.
    ///
    /// # Arguments
    /// * `record` - The producer record to send
    ///
    /// # Returns
    /// * `Result<RecordMetadata, ProducerError>` - The metadata for the sent record
    ///
    /// # Behavior
    /// - Returns error if producer is closed
    /// - If partition is specified, uses that partition
    /// - If partition is not specified, uses partition 0
    /// - Record is added to pending queue (flush() clears it)
    /// - Returns RecordMetadata with assigned offset
    pub fn send(&self, record: &ProducerRecord) -> Result<RecordMetadata, ProducerError> {
        self.send_with_callback(record, None)
    }

    /// Sends a record with a callback.
    ///
    /// This corresponds to `KafkaProducer.send(ProducerRecord, Callback)` in Java.
    ///
    /// # Arguments
    /// * `record` - The producer record to send
    /// * `callback` - Optional callback to invoke with result
    pub fn send_with_callback(
        &self,
        record: &ProducerRecord,
        callback: Option<SendCallbackFn>,
    ) -> Result<RecordMetadata, ProducerError> {
        let mut state = self.state.lock().unwrap();

        // Check if producer is closed
        if state.is_closed {
            let error = ProducerError::Closed {
                message: "Cannot send after producer is closed".to_string(),
            };
            if let Some(cb) = callback {
                cb(Err(error.clone()));
            }
            return Err(error);
        }

        // Check if topic exists (if topics are registered)
        let topic_name = record.topic();
        if !state.topics.is_empty() && !state.topics.contains_key(topic_name) {
            let error = ProducerError::TopicDoesNotExist {
                topic: topic_name.to_string(),
            };
            if let Some(cb) = callback {
                cb(Err(error.clone()));
            }
            return Err(error);
        }

        // Determine partition
        let partition = record.partition().unwrap_or_else(|| {
            // If topics are registered, use partition 0
            // Otherwise, default to partition 0
            0
        });

        // Check partition validity if topic metadata exists
        if let Some(topic_meta) = state.topics.get(topic_name) {
            if partition < 0 || partition >= topic_meta.num_partitions {
                let error = ProducerError::InvalidPartition {
                    topic: topic_name.to_string(),
                    partition,
                };
                if let Some(cb) = callback {
                    cb(Err(error.clone()));
                }
                return Err(error);
            }
        }

        // Get next offset
        let tp = TopicPartition::new(topic_name, partition);
        let offset = state.next_offset.get(&tp).copied().unwrap_or(0);
        state.next_offset.insert(tp.clone(), offset + 1);

        // Use provided timestamp or generate one
        let timestamp = record.timestamp().unwrap_or_else(|| {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        });

        // Create stored record
        let stored = StoredRecord {
            topic: topic_name.to_string(),
            partition,
            key: record.key().cloned(),
            value: record.value().cloned(),
            timestamp,
            offset,
        };

        // Add to pending queue
        state.pending_records.push_back(stored);

        // Create RecordMetadata
        let key_size = record.key().map(|k| k.len() as i32).unwrap_or(0);
        let value_size = record.value().map(|v| v.len() as i32).unwrap_or(0);
        let metadata = RecordMetadata::new(tp, offset, 0, timestamp, key_size, value_size);

        // Invoke callback if provided
        if let Some(cb) = callback {
            cb(Ok(metadata.clone()));
        }

        Ok(metadata)
    }

    /// Flushes all pending records.
    ///
    /// This corresponds to `KafkaProducer.flush()` in Java.
    /// In mock, this clears the pending queue, simulating that all records
    /// have been "sent".
    ///
    /// # Behavior
    /// - Returns error if producer is closed
    /// - Clears the pending record queue
    /// - Records can be retrieved via `sent_records()` after flush
    pub fn flush(&self) -> Result<(), ProducerError> {
        let mut state = self.state.lock().unwrap();

        if state.is_closed {
            return Err(ProducerError::Closed {
                message: "Cannot flush after producer is closed".to_string(),
            });
        }

        // Clear pending records (simulate they have been sent)
        state.pending_records.clear();

        Ok(())
    }

    /// Returns partition metadata for a topic.
    ///
    /// This corresponds to `KafkaProducer.partitionsFor(String)` in Java.
    ///
    /// # Arguments
    /// * `topic` - The topic name
    ///
    /// # Returns
    /// * `Result<Vec<PartitionInfo>, ProducerError>` - Partition metadata list
    ///
    /// # Behavior
    /// - Returns error if producer is closed
    /// - Returns error if topic does not exist (when topics are registered)
    /// - If no topics are registered, returns a default single partition
    pub fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, ProducerError> {
        let state = self.state.lock().unwrap();

        if state.is_closed {
            return Err(ProducerError::Closed {
                message: "Cannot query partitions after producer is closed".to_string(),
            });
        }

        // If topic metadata exists, return it
        if let Some(topic_meta) = state.topics.get(topic) {
            return Ok(topic_meta.partition_infos.clone());
        }

        // If topics are registered but this one doesn't exist
        if !state.topics.is_empty() {
            return Err(ProducerError::TopicDoesNotExist {
                topic: topic.to_string(),
            });
        }

        // If no topics are registered, return default single partition
        Ok(vec![PartitionInfo::new(0, 0, vec![0, 1, 2], vec![0, 1])])
    }

    /// Closes the producer.
    ///
    /// This corresponds to `KafkaProducer.close()` in Java.
    /// After close, all subsequent operations will return ProducerError::Closed.
    ///
    /// # Behavior
    /// - Sets is_closed flag to true
    /// - Clears pending records
    /// - All subsequent send/flush/partitionsFor calls will fail
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.is_closed = true;
        state.pending_records.clear();
    }

    /// Returns whether the producer is closed.
    pub fn is_closed(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.is_closed
    }

    // ===== Utility methods for testing =====

    /// Returns the number of pending records (not yet flushed).
    pub fn pending_count(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.pending_records.len()
    }

    /// Returns all sent records (records that have been added via send()).
    ///
    /// Note: In mock, records are not actually "sent" until flush() is called.
    /// This method returns records currently in the pending queue.
    pub fn sent_records(&self) -> Vec<(String, i32, Option<Vec<u8>>, Option<Vec<u8>>, i64)> {
        let state = self.state.lock().unwrap();
        state
            .pending_records
            .iter()
            .map(|r| {
                (
                    r.topic.clone(),
                    r.partition,
                    r.key.clone(),
                    r.value.clone(),
                    r.offset,
                )
            })
            .collect()
    }

    /// Clears all state (topics, records, offsets).
    ///
    /// Note: This does NOT reset the closed state.
    /// Once closed, the producer cannot be reopened.
    pub fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.topics.clear();
        state.pending_records.clear();
        state.next_offset.clear();
    }

    /// Returns the topics registered with this producer.
    pub fn topics(&self) -> Vec<String> {
        let state = self.state.lock().unwrap();
        state.topics.keys().cloned().collect()
    }

    /// Returns the current offset for a topic partition.
    pub fn current_offset(&self, topic: &str, partition: i32) -> i64 {
        let state = self.state.lock().unwrap();
        let tp = TopicPartition::new(topic, partition);
        state.next_offset.get(&tp).copied().unwrap_or(0)
    }
}

impl Default for MockKafkaProducer {
    fn default() -> Self {
        MockKafkaProducer::new()
    }
}

// ===== Producer trait implementation =====

impl common_trait::worker::Producer for MockKafkaProducer {
    fn init_transactions(&self) {
        // No-op in mock
    }

    fn begin_transaction(&self) {
        // No-op in mock
    }

    fn commit_transaction(&self) {
        // No-op in mock
    }

    fn abort_transaction(&self) {
        // No-op in mock
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_record_new() {
        let record = ProducerRecord::new("test-topic", Some(vec![1, 2, 3]));
        assert_eq!(record.topic(), "test-topic");
        assert!(record.partition().is_none());
        assert!(record.key().is_none());
        assert_eq!(record.value(), Some(&vec![1, 2, 3]));
        assert!(record.timestamp().is_none());
    }

    #[test]
    fn test_producer_record_with_key_value() {
        let record = ProducerRecord::with_key_value("test-topic", Some(vec![1]), Some(vec![2]));
        assert_eq!(record.key(), Some(&vec![1]));
        assert_eq!(record.value(), Some(&vec![2]));
    }

    #[test]
    fn test_producer_record_with_partition() {
        let record = ProducerRecord::with_partition("test-topic", 5, Some(vec![1]), Some(vec![2]));
        assert_eq!(record.partition(), Some(5));
    }

    #[test]
    fn test_partition_info() {
        let info = PartitionInfo::new(0, 1, vec![1, 2, 3], vec![1, 2]);
        assert_eq!(info.partition(), 0);
        assert_eq!(info.leader(), 1);
        assert_eq!(info.replicas(), &vec![1, 2, 3]);
        assert_eq!(info.in_sync_replicas(), &vec![1, 2]);
    }

    #[test]
    fn test_producer_new() {
        let producer = MockKafkaProducer::new();
        assert!(!producer.is_closed());
        assert_eq!(producer.pending_count(), 0);
        assert!(producer.topics().is_empty());
    }

    #[test]
    fn test_producer_with_topics() {
        let topics = HashMap::from([("topic1".to_string(), 3), ("topic2".to_string(), 5)]);
        let producer = MockKafkaProducer::with_topics(topics);

        let registered = producer.topics();
        assert_eq!(registered.len(), 2);
        assert!(registered.contains(&"topic1".to_string()));
        assert!(registered.contains(&"topic2".to_string()));
    }

    #[test]
    fn test_add_topic() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("test-topic", 3);

        assert_eq!(producer.topics().len(), 1);
        assert!(producer.topics().contains(&"test-topic".to_string()));

        let partitions = producer.partitions_for("test-topic").unwrap();
        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions[0].partition(), 0);
        assert_eq!(partitions[1].partition(), 1);
        assert_eq!(partitions[2].partition(), 2);
    }

    #[test]
    fn test_send_basic() {
        let producer = MockKafkaProducer::new();
        let record = ProducerRecord::new("test-topic", Some(vec![1, 2, 3]));

        let result = producer.send(&record);
        assert!(result.is_ok());

        let metadata = result.unwrap();
        assert_eq!(metadata.topic(), "test-topic");
        assert_eq!(metadata.partition(), 0); // Default partition
        assert_eq!(metadata.offset(), 0); // First offset

        assert_eq!(producer.pending_count(), 1);
    }

    #[test]
    fn test_send_with_partition() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("test-topic", 3);

        let record = ProducerRecord::with_partition("test-topic", 2, Some(vec![1]), Some(vec![2]));

        let result = producer.send(&record);
        assert!(result.is_ok());

        let metadata = result.unwrap();
        assert_eq!(metadata.partition(), 2);
    }

    #[test]
    fn test_send_offset_increment() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("test-topic", 1);

        let r1 = ProducerRecord::new("test-topic", Some(vec![1]));
        let r2 = ProducerRecord::new("test-topic", Some(vec![2]));
        let r3 = ProducerRecord::new("test-topic", Some(vec![3]));

        let m1 = producer.send(&r1).unwrap();
        let m2 = producer.send(&r2).unwrap();
        let m3 = producer.send(&r3).unwrap();

        assert_eq!(m1.offset(), 0);
        assert_eq!(m2.offset(), 1);
        assert_eq!(m3.offset(), 2);

        assert_eq!(producer.current_offset("test-topic", 0), 3);
    }

    #[test]
    fn test_send_to_registered_topic() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("existing-topic", 2);

        // Send to existing topic - should succeed
        let record = ProducerRecord::new("existing-topic", Some(vec![1]));
        let result = producer.send(&record);
        assert!(result.is_ok());

        // Send to non-existing topic - should fail
        let record2 = ProducerRecord::new("non-existing-topic", Some(vec![1]));
        let result2 = producer.send(&record2);
        assert!(result2.is_err());
        assert!(matches!(
            result2.unwrap_err(),
            ProducerError::TopicDoesNotExist { .. }
        ));
    }

    #[test]
    fn test_send_invalid_partition() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("test-topic", 3);

        // Valid partition
        let valid_record =
            ProducerRecord::with_partition("test-topic", 2, Some(vec![1]), Some(vec![2]));
        assert!(producer.send(&valid_record).is_ok());

        // Invalid partition (out of range)
        let invalid_record =
            ProducerRecord::with_partition("test-topic", 5, Some(vec![1]), Some(vec![2]));
        let result = producer.send(&invalid_record);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ProducerError::InvalidPartition { .. }
        ));
    }

    #[test]
    fn test_send_without_topic_registration() {
        // Without topic registration, send should still work
        let producer = MockKafkaProducer::new();

        let record = ProducerRecord::new("any-topic", Some(vec![1]));
        let result = producer.send(&record);
        assert!(result.is_ok());

        // partitions_for should return default single partition
        let partitions = producer.partitions_for("any-topic").unwrap();
        assert_eq!(partitions.len(), 1);
    }

    #[test]
    fn test_send_with_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};

        static CALLBACK_INVOKED: AtomicBool = AtomicBool::new(false);
        CALLBACK_INVOKED.store(false, Ordering::SeqCst);

        let producer = MockKafkaProducer::new();
        let record = ProducerRecord::new("test-topic", Some(vec![1]));

        let callback = Box::new(|_result: Result<RecordMetadata, ProducerError>| {
            CALLBACK_INVOKED.store(true, Ordering::SeqCst);
        });

        let result = producer.send_with_callback(&record, Some(callback));
        assert!(result.is_ok());

        // Callback should have been invoked (synchronously in mock)
        assert!(CALLBACK_INVOKED.load(Ordering::SeqCst));

        // Verify the returned metadata
        let metadata = result.unwrap();
        assert_eq!(metadata.topic(), "test-topic");
    }

    #[test]
    fn test_flush() {
        let producer = MockKafkaProducer::new();

        let r1 = ProducerRecord::new("test-topic", Some(vec![1]));
        let r2 = ProducerRecord::new("test-topic", Some(vec![2]));

        producer.send(&r1).unwrap();
        producer.send(&r2).unwrap();

        assert_eq!(producer.pending_count(), 2);

        // Flush should clear pending records
        let result = producer.flush();
        assert!(result.is_ok());
        assert_eq!(producer.pending_count(), 0);
    }

    #[test]
    fn test_partitions_for() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("test-topic", 5);

        let partitions = producer.partitions_for("test-topic").unwrap();
        assert_eq!(partitions.len(), 5);

        for (i, p) in partitions.iter().enumerate() {
            assert_eq!(p.partition(), i as i32);
        }
    }

    #[test]
    fn test_partitions_for_nonexistent_topic() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("existing-topic", 1);

        let result = producer.partitions_for("nonexistent-topic");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ProducerError::TopicDoesNotExist { .. }
        ));
    }

    #[test]
    fn test_close() {
        let producer = MockKafkaProducer::new();
        assert!(!producer.is_closed());

        // Send should work before close
        let record = ProducerRecord::new("test-topic", Some(vec![1]));
        assert!(producer.send(&record).is_ok());

        producer.close();
        assert!(producer.is_closed());

        // Pending records should be cleared
        assert_eq!(producer.pending_count(), 0);
    }

    #[test]
    fn test_send_after_close() {
        let producer = MockKafkaProducer::new();
        producer.close();

        let record = ProducerRecord::new("test-topic", Some(vec![1]));
        let result = producer.send(&record);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ProducerError::Closed { .. }));
    }

    #[test]
    fn test_flush_after_close() {
        let producer = MockKafkaProducer::new();
        producer.close();

        let result = producer.flush();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ProducerError::Closed { .. }));
    }

    #[test]
    fn test_partitions_for_after_close() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("test-topic", 1);
        producer.close();

        let result = producer.partitions_for("test-topic");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ProducerError::Closed { .. }));
    }

    #[test]
    fn test_close_is_permanent() {
        let producer = MockKafkaProducer::new();
        producer.close();

        // Clear does not reset closed state
        producer.clear();
        assert!(producer.is_closed());

        // Still cannot send
        let record = ProducerRecord::new("test-topic", Some(vec![1]));
        assert!(producer.send(&record).is_err());
    }

    #[test]
    fn test_sent_records() {
        let producer = MockKafkaProducer::new();

        let r1 = ProducerRecord::new("topic1", Some(vec![1]));
        let r2 = ProducerRecord::with_partition("topic2", 2, Some(vec![2]), Some(vec![3]));

        producer.send(&r1).unwrap();
        producer.send(&r2).unwrap();

        let records = producer.sent_records();
        assert_eq!(records.len(), 2);

        assert_eq!(records[0].0, "topic1");
        assert_eq!(records[0].1, 0); // Default partition
        assert_eq!(records[1].0, "topic2");
        assert_eq!(records[1].1, 2); // Explicit partition
    }

    #[test]
    fn test_clear() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("test-topic", 1);

        let record = ProducerRecord::new("test-topic", Some(vec![1]));
        producer.send(&record).unwrap();

        assert_eq!(producer.pending_count(), 1);
        assert_eq!(producer.topics().len(), 1);

        producer.clear();

        assert_eq!(producer.pending_count(), 0);
        assert!(producer.topics().is_empty());
        // Offset should be reset
        assert_eq!(producer.current_offset("test-topic", 0), 0);
    }

    #[test]
    fn test_producer_error_display() {
        let e1 = ProducerError::Closed {
            message: "test".to_string(),
        };
        assert!(e1.to_string().contains("closed"));

        let e2 = ProducerError::TopicDoesNotExist {
            topic: "test".to_string(),
        };
        assert!(e2.to_string().contains("does not exist"));

        let e3 = ProducerError::InvalidPartition {
            topic: "test".to_string(),
            partition: 5,
        };
        assert!(e3.to_string().contains("Invalid partition"));
    }

    #[test]
    fn test_producer_trait_impl() {
        let producer = MockKafkaProducer::new();

        // Test Producer trait implementation
        let trait_producer: &dyn common_trait::worker::Producer = &producer;

        trait_producer.init_transactions();
        trait_producer.begin_transaction();
        trait_producer.commit_transaction();
        trait_producer.abort_transaction();

        // These are no-op in mock, but should not panic
        assert!(!producer.is_closed());
    }

    #[test]
    fn test_multiple_partition_offsets() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("test-topic", 3);

        // Send to partition 0
        let r0_1 = ProducerRecord::with_partition("test-topic", 0, None, Some(vec![1]));
        let r0_2 = ProducerRecord::with_partition("test-topic", 0, None, Some(vec![2]));

        // Send to partition 1
        let r1_1 = ProducerRecord::with_partition("test-topic", 1, None, Some(vec![3]));

        // Send to partition 2
        let r2_1 = ProducerRecord::with_partition("test-topic", 2, None, Some(vec![4]));
        let r2_2 = ProducerRecord::with_partition("test-topic", 2, None, Some(vec![5]));

        let m0_1 = producer.send(&r0_1).unwrap();
        let m0_2 = producer.send(&r0_2).unwrap();
        let m1_1 = producer.send(&r1_1).unwrap();
        let m2_1 = producer.send(&r2_1).unwrap();
        let m2_2 = producer.send(&r2_2).unwrap();

        // Verify offsets per partition
        assert_eq!(m0_1.offset(), 0);
        assert_eq!(m0_2.offset(), 1);
        assert_eq!(m1_1.offset(), 0);
        assert_eq!(m2_1.offset(), 0);
        assert_eq!(m2_2.offset(), 1);

        // Verify current offsets
        assert_eq!(producer.current_offset("test-topic", 0), 2);
        assert_eq!(producer.current_offset("test-topic", 1), 1);
        assert_eq!(producer.current_offset("test-topic", 2), 2);
    }

    #[test]
    fn test_send_callback_error_when_closed() {
        use std::sync::atomic::{AtomicBool, Ordering};

        static CALLBACK_INVOKED: AtomicBool = AtomicBool::new(false);
        CALLBACK_INVOKED.store(false, Ordering::SeqCst);

        let producer = MockKafkaProducer::new();
        producer.close();

        let callback = Box::new(|result: Result<RecordMetadata, ProducerError>| {
            if let Err(e) = result {
                assert!(matches!(e, ProducerError::Closed { .. }));
            }
            CALLBACK_INVOKED.store(true, Ordering::SeqCst);
        });

        let record = ProducerRecord::new("test-topic", Some(vec![1]));
        let result = producer.send_with_callback(&record, Some(callback));

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ProducerError::Closed { .. }));
        assert!(CALLBACK_INVOKED.load(Ordering::SeqCst));
    }

    #[test]
    fn test_timestamp_assignment() {
        let producer = MockKafkaProducer::new();

        // Without explicit timestamp
        let r1 = ProducerRecord::new("test-topic", Some(vec![1]));
        let m1 = producer.send(&r1).unwrap();
        assert!(m1.timestamp() > 0); // Should have auto-generated timestamp

        // With explicit timestamp
        let r2 = ProducerRecord::with_all(
            "test-topic",
            None,
            None,
            Some(vec![2]),
            Some(1234567890),
            HashMap::new(),
        );
        let m2 = producer.send(&r2).unwrap();
        assert_eq!(m2.timestamp(), 1234567890);
    }
}
