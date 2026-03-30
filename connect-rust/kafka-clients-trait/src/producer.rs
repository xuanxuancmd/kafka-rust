//! Kafka Producer Trait
//!
//! Defines the core interface for Kafka producers.

use std::future::Future;
use std::time::Duration;

/// Represents metadata about a record that has been sent to Kafka
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}

/// Represents a record to be sent to Kafka
#[derive(Debug, Clone)]
pub struct ProducerRecord<K, V> {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<K>,
    pub value: Option<V>,
    pub timestamp: Option<i64>,
}

/// Callback for handling record send results
pub trait Callback: Send + Sync {
    fn on_completion(&self, metadata: Option<RecordMetadata>, error: Option<String>);
}

/// Core Kafka Producer trait
pub trait KafkaProducer<K, V>: Send + Sync {
    /// Asynchronously sends a record to a topic
    fn send(
        &self,
        record: ProducerRecord<K, V>,
    ) -> impl Future<Output = Result<RecordMetadata, String>> + Send;

    /// Asynchronously sends a record with a callback
    fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Box<dyn Callback>,
    ) -> impl Future<Output = ()> + Send;

    /// Forces all buffered records to be immediately sent
    fn flush(&self) -> impl Future<Output = ()> + Send;

    /// Returns partition information for a given topic
    fn partitions_for(
        &self,
        topic: &str,
    ) -> impl Future<Output = Result<Vec<PartitionInfo>, String>> + Send;

    /// Initializes transactions for this producer
    fn init_transactions(&self) -> impl Future<Output = Result<(), String>> + Send;

    /// Begins a new transaction
    fn begin_transaction(&self) -> impl Future<Output = Result<(), String>> + Send;

    /// Commits the current transaction
    fn commit_transaction(&self) -> impl Future<Output = Result<(), String>> + Send;

    /// Aborts the current transaction
    fn abort_transaction(&self) -> impl Future<Output = Result<(), String>> + Send;

    /// Closes the producer
    fn close(&self) -> impl Future<Output = ()> + Send;

    /// Closes the producer with a timeout
    fn close_with_timeout(&self, timeout: Duration) -> impl Future<Output = ()> + Send;
}

/// Information about a partition
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub topic: String,
    pub partition: i32,
    pub leader: Option<i32>,
    pub replicas: i32,
    pub in_sync_replicas: i32,
}
