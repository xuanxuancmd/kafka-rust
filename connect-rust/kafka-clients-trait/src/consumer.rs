//! Kafka Consumer Trait
//!
//! Defines core interface for Kafka consumers.

use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::time::Duration;

/// Represents a topic and partition
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl std::fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

/// Represents offset and metadata for a topic partition
#[derive(Debug, Clone)]
pub struct OffsetAndMetadata {
    pub offset: i64,
    pub metadata: String,
    pub leader_epoch: Option<i32>,
}

/// Represents a consumer record
#[derive(Debug)]
pub struct ConsumerRecord<K, V> {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<K>,
    pub value: Option<V>,
    pub timestamp: i64,
    pub timestamp_type: TimestampType,
}

// 为 ConsumerRecord 添加 Clone 约束，确保 K 和 V 也实现 Clone
impl<K: Clone, V: Clone> Clone for ConsumerRecord<K, V> {
    fn clone(&self) -> Self {
        ConsumerRecord {
            topic: self.topic.clone(),
            partition: self.partition,
            offset: self.offset,
            key: self.key.clone(),
            value: self.value.clone(),
            timestamp: self.timestamp,
            timestamp_type: self.timestamp_type,
        }
    }
}

/// Checkpoint - 用于反序列化 checkpoint 记录
///
/// 对应 Java 的 Checkpoint 类
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub group_id: String,
    pub topic_partition: TopicPartition,
    pub upstream_offset: i64,
    pub metadata: String,
    pub version: i32,
}

/// Types of timestamps
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    NoTimestampType,
    CreateTime,
    LogAppendTime,
}

/// Consumer group metadata
#[derive(Debug, Clone)]
pub struct ConsumerGroupMetadata {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
}

/// Core Kafka Consumer trait (async version)
pub trait KafkaConsumer<K, V>: Send {
    /// Subscribes to a list of topics
    fn subscribe(&self, topics: Vec<String>) -> impl Future<Output = Result<(), String>> + Send;

    /// Subscribes to topics matching a pattern
    fn subscribe_pattern(&self, pattern: &str) -> impl Future<Output = Result<(), String>> + Send;

    /// Manually assigns partitions to this consumer
    fn assign(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> impl Future<Output = Result<(), String>> + Send;

    /// Fetches data for subscribed topics/partitions
    fn poll(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<Vec<ConsumerRecord<K, V>>, String>> + Send;

    /// Commits offsets synchronously
    fn commit_sync(&self) -> impl Future<Output = Result<(), String>> + Send;

    /// Commits specific offsets synchronously
    fn commit_sync_offsets(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> impl Future<Output = Result<(), String>> + Send;

    /// Commits offsets asynchronously
    fn commit_async(&self) -> impl Future<Output = ()> + Send;

    /// Returns set of partitions assigned to this consumer
    fn assignment(&self) -> impl Future<Output = Result<Vec<TopicPartition>, String>> + Send;

    /// Returns current subscription
    fn subscription(&self) -> impl Future<Output = Result<Vec<String>, String>> + Send;

    /// Unsubscribes from all topics
    fn unsubscribe(&self) -> impl Future<Output = ()> + Send;

    /// Seeks to a specific offset
    fn seek(
        &self,
        partition: TopicPartition,
        offset: i64,
    ) -> impl Future<Output = Result<(), String>> + Send;

    /// Returns current position (offset) for partitions
    fn position(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, i64>, String>> + Send;

    /// Closes the consumer
    fn close(&self) -> impl Future<Output = ()> + Send;

    /// Closes the consumer with a timeout
    fn close_with_timeout(&self, timeout: Duration) -> impl Future<Output = ()> + Send;
}

/// Sync Kafka Consumer trait (synchronous version for MirrorClient)
///
/// This trait provides synchronous methods for MirrorClient which doesn't need async operations.
pub trait KafkaConsumerSync<K, V>: Send + Sync {
    /// Manually assigns partitions to this consumer
    fn assign_sync(&mut self, partitions: Vec<TopicPartition>) -> Result<(), Box<dyn Error>>;

    /// Seeks to a specific offset
    fn seek_sync(&mut self, partition: TopicPartition, offset: i64) -> Result<(), Box<dyn Error>>;

    /// Seeks to the beginning of partitions
    fn seek_to_beginning(&mut self, partitions: Vec<TopicPartition>) -> Result<(), Box<dyn Error>>;

    /// Fetches data for subscribed topics/partitions
    fn poll_sync(&mut self, timeout: Duration)
        -> Result<Vec<ConsumerRecord<K, V>>, Box<dyn Error>>;

    /// Returns current position (offset) for partitions
    fn position_sync(&self, partition: &TopicPartition) -> Result<i64, Box<dyn Error>>;

    /// Returns end offsets for partitions
    fn end_offsets(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> Result<HashMap<TopicPartition, i64>, Box<dyn Error>>;

    /// Closes the consumer
    fn close_sync(&mut self) -> Result<(), Box<dyn Error>>;
}

/// Mock Kafka Consumer Client (sync version for MirrorClient)
///
/// Provides a mock implementation of KafkaConsumerSync for testing purposes.
#[derive(Debug, Clone)]
pub struct MockKafkaConsumerSync<K, V> {
    records: Vec<ConsumerRecord<K, V>>,
}

impl<K, V> MockKafkaConsumerSync<K, V> {
    /// Creates a new mock consumer
    pub fn new() -> Self {
        MockKafkaConsumerSync {
            records: Vec::new(),
        }
    }

    /// Creates a mock consumer with predefined records
    pub fn with_records(records: Vec<ConsumerRecord<K, V>>) -> Self {
        MockKafkaConsumerSync { records }
    }

    /// Adds a record to the mock
    pub fn add_record(&mut self, record: ConsumerRecord<K, V>) {
        self.records.push(record);
    }

    /// Clears all records
    pub fn clear_records(&mut self) {
        self.records.clear();
    }
}

impl<K: Clone + Send + Sync, V: Clone + Send + Sync> KafkaConsumerSync<K, V>
    for MockKafkaConsumerSync<K, V>
{
    fn assign_sync(&mut self, _partitions: Vec<TopicPartition>) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn seek_sync(
        &mut self,
        _partition: TopicPartition,
        _offset: i64,
    ) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn seek_to_beginning(
        &mut self,
        _partitions: Vec<TopicPartition>,
    ) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn poll_sync(
        &mut self,
        _timeout: Duration,
    ) -> Result<Vec<ConsumerRecord<K, V>>, Box<dyn Error>> {
        Ok(self.records.clone())
    }

    fn position_sync(&self, _partition: &TopicPartition) -> Result<i64, Box<dyn Error>> {
        Ok(0)
    }

    fn end_offsets(
        &self,
        _partitions: Vec<TopicPartition>,
    ) -> Result<HashMap<TopicPartition, i64>, Box<dyn Error>> {
        Ok(HashMap::new())
    }

    fn close_sync(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}
