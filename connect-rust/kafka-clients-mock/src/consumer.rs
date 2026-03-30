//! Mock Consumer Implementation
//!
//! This module provides a mock implementation of KafkaConsumer for testing purposes.

use kafka_clients_trait::consumer::{KafkaConsumer, ConsumerRecord, TopicPartition};
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
    /// Whether the consumer is closed
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

    /// Add records to a topic
    pub fn add_records(&self, topic: &str, records: Vec<MockConsumerRecord>) {
        let mut all_records = self.records.lock().unwrap();
        let topic_records = all_records.entry(topic.to_string()).or_insert_with(Vec::new);
        topic_records.extend(records);
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

impl<K, V> KafkaConsumer<K, V> for MockConsumer
where
    K: Clone + Send + 'static,
    V: Clone + Send + 'static,
{
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
    ) -> impl std::future::Future<Output = Result<Vec<ConsumerRecord<K, V>>, String>> + Send {
        let closed = self.closed.clone();

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Consumer is closed".to_string());
            }

            // Mock poll returns empty records
            Ok(Vec::new())
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
        _offsets: HashMap<TopicPartition, kafka_clients_trait::consumer::OffsetAndMetadata>,
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
