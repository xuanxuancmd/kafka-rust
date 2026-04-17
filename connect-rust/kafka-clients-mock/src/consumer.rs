//! Mock Consumer Implementation
//!
//! This module provides a mock implementation of KafkaConsumer for testing purposes.

use common_trait::consumer::{KafkaConsumer, ConsumerRecord, TopicPartition};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Mock consumer for testing Kafka consumer functionality.
pub struct MockConsumer {
    /// In-memory storage for records
    records: Arc<Mutex<HashMap<String, Vec<MockConsumerRecord>>>>,
    /// Current position for each partition
    positions: Arc<Mutex<HashMap<(String, i32), i64>>>,
    /// Subscribed topics
    subscribed_topics: Arc<Mutex<HashSet<String>>>,
    /// Assigned partitions
    assigned_partitions: Arc<Mutex<HashSet<(String, i32)>>>,
    /// Whether consumer is closed
    closed: Arc<Mutex<bool>>,
}

/// Mock consumer record representation
#[derive(Debug, Clone)]
pub struct MockConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp: Option<i64>,
}

impl MockConsumer {
    /// Create a new mock consumer
    pub fn new() -> Self {
        MockConsumer {
            records: Arc::new
(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            subscribed_topics: Arc::new(Mutex::new(HashSet::new())),
            assigned_partitions: Arc::new(Mutex::new(HashSet::new())),
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Create a new mock consumer with shared storage
    pub fn new_with_storage(records: Arc<Mutex<HashMap<String, Vec<MockConsumerRecord>>>>) -> Self {
        MockConsumer {
            records,
            positions: Arc::new(Mutex::new(HashMap::new())),
            subscribed_topics: Arc::new(Mutex::new(HashSet::new())),
            assigned_partitions: Arc::new(Mutex::new(HashSet::new())),
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Add records to a topic
    pub fn add_records(&self, topic: &str, records: Vec<MockConsumerRecord>) {
        let mut all_records = self.records.lock().unwrap();
        let topic_records = all_records.entry(topic.to_string()).or_insert_with(Vec::new);
        topic_records.extend(records);
    }

    /// Get all records for a topic
    pub fn get_records_for_topic(&self, topic: &str) -> Vec<MockConsumerRecord> {
        let records = self.records.lock().unwrap();
        records.get(topic).cloned().unwrap_or_default()
    }

    /// Get all records
    pub fn get_all_records(&self) -> Vec<MockConsumerRecord> {
        let records = self.records.lock().unwrap();
        records.values().flatten().cloned().collect()
    }

    /// Clear all records
    pub fn clear_records(&self) {
        let mut records = self.records.lock().unwrap();
        records.clear();
    }

    /// Get current position for a topic-partition
    pub fn get_position(&self, topic: &str, partition: i32) -> i64 {
        let positions = self.positions.lock().unwrap();
        let key = (topic.to_string(), partition);
        *positions.get(&key).unwrap_or(&0)
    }

    /// Set position for a topic-partition
    pub fn set_position(&self, topic: &str, partition: i32, offset: i64) {
        let mut positions = self.positions.lock().unwrap();
        positions.insert((topic.to_string(), partition), offset);
    }

    /// Get subscribed topics
    pub fn get_subscribed_topics(&self) -> HashSet<String> {
        let topics = self.subscribed_topics.lock().unwrap();
        topics.clone()
    }

    /// Get assigned partitions
    pub fn get_assigned_partitions(&self) -> HashSet<(String, i32)> {
        let partitions = self.assigned_partitions.lock().unwrap();
        partitions.clone()
    }
}

impl Default for MockConsumer {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaConsumer<Vec<u8>, Vec<u8>> for MockConsumer {
    fn subscribe(&self, topics: Vec<String>) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let closed = self.closed.clone();
        let subscribed_topics = self.subscribed_topics.clone();

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Consumer is closed".to_string());
            }

            let mut topics_guard = subscribed_topics.lock().unwrap();
            for topic in topics {
                topics_guard.insert(topic);
            }
            Ok(())
        }
    }

    fn subscribe_pattern(&self, _pattern: &str) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let closed = self.closed.clone();

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Consumer is closed".to_string());
            }
            Ok(())
        }
    }

    fn assign(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let closed = self.closed.clone();
        let assigned_partitions = self.assigned_partitions.clone();

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Consumer is closed".to_string());
            }

            let mut partitions_guard = assigned_partitions.lock().unwrap();
            for partition in partitions {
                partitions_guard.insert((partition.topic, partition.partition));
            }
            Ok(())
        }
    }

    fn poll(
        &self,
        _timeout: Duration,
    ) -> impl std::future::Future<Output = Result<Vec<ConsumerRecord<Vec<u8>, Vec<u8>>>, String>> + Send {
        let closed = self.closed.clone();
        let records = self.records.clone();
        let assigned_partitions = self.assigned_partitions.clone();
        let positions = self.positions.clone();

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Consumer is closed".to_string());
            }

            // Get assigned partitions
            let partitions = {
                let guard = assigned_partitions.lock().unwrap();
                guard.clone()
            };

            if partitions.is_empty() {
                return Ok(Vec::new());
            }

            // Get current positions and build results
            let mut results = Vec::new();
            {
                let positions_guard = positions.lock().unwrap();
                let records_guard = records.lock().unwrap();

                for (topic, partition) in &partitions {
                    let topic_str = topic.clone();
                    if let Some(topic_records) = records_guard.get(&topic_str) {
                        // Get current position for this partition
                        let current_offset = *positions_guard

                            .get(&(topic_str.clone(), *partition))
                            .unwrap_or(&0);

                        // Find records from current position
                        for record in topic_records {
                            if record.partition == *partition && record.offset >= current_offset {
                                // For mock, we use Vec<u8> as concrete type
                                // In real Kafka, serde would handle serialization
                                results.push(ConsumerRecord {
                                    topic: record.topic.clone(),
                                    partition: record.partition,
                                    offset: record.offset,
                                    key: record.key.clone(),
                                    value: record.value.clone(),
                                    timestamp: record.timestamp.unwrap_or(0),
                                    timestamp_type: common_trait::consumer::TimestampType::CreateTime,
                                });
                            }
                        }
                    }
                }
            }

            // Update positions to track consumption
            if !results.is_empty() {
                let mut positions_guard = positions.lock().unwrap();
                for record in &results {
                    let key = (record.topic.clone(), record.partition);
                    // Set position to next offset after highest record consumed
                    let current = positions_guard.get(&key).copied().unwrap_or(0);
                    if record.offset + 1 > current {
                        positions_guard.insert(key, record.offset + 1);
                    }
                }
            }

            Ok(results)
        }
    }

    fn commit_sync(&self) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let closed = self.closed.clone();

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Consumer is closed".to_string());
            }

            // Mock commit is a no-op
            Ok(())
        }
    }

    fn commit_sync_offsets(
        &self,
        _offsets: HashMap<TopicPartition, common_trait::consumer::OffsetAndMetadata>,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let closed = self.closed.clone();

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Consumer is closed".to_string());
            }

            // Mock commit is a no-op
            Ok(())
        }
    }

    fn commit_async(&self) -> impl std::future::Future<Output = ()> + Send {
        async move {
            // Mock async commit is a no-op
        }
    }

    fn assignment(&self) -> impl std::future::Future<Output = Result<Vec<TopicPartition>, String>> + Send {
        let closed = self.closed.clone();
        let assigned_partitions = self.assigned_partitions.clone();

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Consumer is closed".to_string());
            }

            let partitions_guard = assigned_partitions.lock().unwrap();
            let partitions: Vec<TopicPartition> = partitions_guard

                .iter()
                .map(|(topic, partition)| TopicPartition {
                    topic: topic.clone(),
                    partition: *partition,
                })
                .collect();

            Ok(partitions)
        }
    }

    fn subscription(&self) -> impl std::future::Future<Output = Result<Vec<String>, String>> + Send {
        let closed = self.closed.clone();
        let subscribed_topics = self.subscribed_topics.clone();

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Consumer is closed".to_string());
            }

            let topics_guard = subscribed_topics.lock().unwrap();
            Ok(topics_guard.iter().cloned().collect())
        }
    }

    fn unsubscribe(&self) -> impl std::future::Future<Output = ()> + Send {
        let subscribed_topics = self.subscribed_topics.clone();

        async move {
            let mut topics_guard = subscribed_topics.lock().unwrap();
            topics_guard.clear();
        }
    }

    fn seek(
        &self,
        partition: TopicPartition,
        offset: i64,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let closed = self.closed.clone();
        let positions = self.positions.clone();

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Consumer is closed".to_string());
            }

            let mut positions_guard = positions.lock().unwrap();
            positions_guard.insert((partition.topic, partition.partition), offset);
            Ok(())
        }
    }

    fn position(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> impl std::future::Future<Output = Result<HashMap<TopicPartition, i64>, String>> + Send {
        let closed = self.closed.clone();
        let positions = self.positions.clone();

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Consumer is closed".to_string());
            }

            let positions_guard = positions.lock().unwrap();
            let mut result = HashMap::new();

            for partition in partitions {
                let key = (partition.topic.clone(), partition.partition);
                let offset = positions_guard.get(&key).copied().unwrap_or(0);
                result.insert(partition, offset);
            }

            Ok(result)
        }
    }

    fn close(&self) -> impl std::future::Future<Output = ()> + Send {
        let closed = self.closed.clone();

        async move {
            let mut c = closed.lock().unwrap();
            *c = true;
        }
    }

    fn close_with_timeout(&self, _timeout: Duration) -> impl std::future::Future<Output = ()> + Send {
        let closed = self.closed.clone();

        async move {
            let mut c = closed.lock().unwrap();
            *c = true;
        }
    }
}
