//! Kafka Consumer Trait
//!
//! Defines the core interface for Kafka consumers.

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

/// Represents a topic and partition
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

/// Represents offset and metadata for a topic partition
#[derive(Debug, Clone)]
pub struct OffsetAndMetadata {
    pub offset: i64,
    pub metadata: String,
    pub leader_epoch: Option<i32>,
}

/// Represents a consumer record
#[derive(Debug, Clone)]
pub struct ConsumerRecord<K, V> {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<K>,
    pub value: Option<V>,
    pub timestamp: i64,
    pub timestamp_type: TimestampType,
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

/// Core Kafka Consumer trait
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

    /// Returns the set of partitions assigned to this consumer
    fn assignment(&self) -> impl Future<Output = Result<Vec<TopicPartition>, String>> + Send;

    /// Returns the current subscription
    fn subscription(&self) -> impl Future<Output = Result<Vec<String>, String>> + Send;

    /// Unsubscribes from all topics
    fn unsubscribe(&self) -> impl Future<Output = ()> + Send;

    /// Seeks to a specific offset
    fn seek(
        &self,
        partition: TopicPartition,
        offset: i64,
    ) -> impl Future<Output = Result<(), String>> + Send;

    /// Returns the current position (offset) for partitions
    fn position(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, i64>, String>> + Send;

    /// Closes the consumer
    fn close(&self) -> impl Future<Output = ()> + Send;

    /// Closes the consumer with a timeout
    fn close_with_timeout(&self, timeout: Duration) -> impl Future<Output = ()> + Send;
}
