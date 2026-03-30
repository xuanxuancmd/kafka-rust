//! Mock Producer Implementation
//!
//! This module provides a mock implementation of KafkaProducer for testing purposes.

use kafka_clients_trait::producer::{KafkaProducer, ProducerRecord, RecordMetadata};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Mock producer for testing Kafka producer functionality.
pub struct MockProducer {
    /// In-memory storage for sent records
    records: Arc<Mutex<Vec<MockRecord>>>,
    /// Whether the producer is closed
    closed: Arc<Mutex<bool>>,
    /// Whether transactions are initialized
    transactional: Arc<Mutex<bool>>,
    /// Whether a transaction is in progress
    in_transaction: Arc<Mutex<bool>>,
}

/// Mock record representation
#[derive(Debug, Clone)]
pub struct MockRecord {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub partition: Option<i32>,
}

impl MockProducer {
    /// Create a new mock producer
    pub fn new() -> Self {
        MockProducer {
            records: Arc::new(Mutex::new(Vec::new())),
            closed: Arc::new(Mutex::new(false)),
            transactional: Arc::new(Mutex::new(false)),
            in_transaction: Arc::new(Mutex::new(false)),
        }
    }

    /// Get all sent records
    pub fn get_records(&self) -> Vec<MockRecord> {
        let records = self.records.lock().unwrap();
        records.clone()
    }

    /// Clear all sent records
    pub fn clear_records(&self) {
        let mut records = self.records.lock().unwrap();
        records.clear();
    }
}

impl Default for MockProducer {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> KafkaProducer<K, V> for MockProducer
where
    K: Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    fn send(
        &self,
        record: ProducerRecord<K, V>,
    ) -> impl std::future::Future<Output = Result<RecordMetadata, String>> + Send {
        let records = self.records.clone();
        let closed = self.closed.clone();
        let topic = record.topic.clone();
        let partition = record.partition.unwrap_or(0);

        async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Producer is closed".to_string());
            }

            let mock_record = MockRecord {
                topic: topic.clone(),
                key: None,
                value: None,
                partition: Some(partition),
            };

            let mut all_records = records.lock().unwrap();
            all_records.push(mock_record);

            Ok(RecordMetadata {
                topic,
                partition,
                offset: 0,
                timestamp: 0,
            })
        }
    }

    fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        _callback: Box<dyn kafka_clients_trait::producer::Callback>,
    ) -> impl std::future::Future<Output = ()> + Send {
        let records = self.records.clone();
        let closed = self.closed.clone();
        let topic = record.topic.clone();
        let partition = record.partition.unwrap_or(0);

        async move {
            let is_closed = *closed.lock().unwrap();
            if !is_closed {
                let mock_record = MockRecord {
                    topic: topic.clone(),
                    key: None,
                    value: None,
                    partition: Some(partition),
                };

                let mut all_records = records.lock().unwrap();
                all_records.push(mock_record);
            }
        }
    }

    fn flush(&self) -> impl std::future::Future<Output = ()> + Send {
        async move {
            // Mock flush is a no-op
        }
    }

    fn partitions_for(
        &self,
        topic: &str,
    ) -> impl std::future::Future<Output = Result<Vec<kafka_clients_trait::producer::PartitionInfo>, String>> + Send {
        let topic = topic.to_string();

        async move {
            Ok(vec![kafka_clients_trait::producer::PartitionInfo {
                topic: topic.clone(),
                partition: 0,
                leader: Some(0),
                replicas: 1,
                in_sync_replicas: 1,
            }])
        }
    }

    fn init_transactions(&self) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let transactional = self.transactional.clone();

        async move {
            let mut tx = transactional.lock().unwrap();
            *tx = true;
            Ok(())
        }
    }

    fn begin_transaction(&self) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let transactional = self.transactional.clone();
        let in_transaction = self.in_transaction.clone();

        async move {
            let is_transactional = *transactional.lock().unwrap();
            if !is_transactional {
                return Err("Transactions not initialized".to_string());
            }

            let mut tx = in_transaction.lock().unwrap();
            *tx = true;
            Ok(())
        }
    }

    fn commit_transaction(&self) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let in_transaction = self.in_transaction.clone();

        async move {
            let is_in_tx = *in_transaction.lock().unwrap();
            if !is_in_tx {
                return Err("No transaction in progress".to_string());
            }

            let mut tx = in_transaction.lock().unwrap();
            *tx = false;
            Ok(())
        }
    }

    fn abort_transaction(&self) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let in_transaction = self.in_transaction.clone();

        async move {
            let is_in_tx = *in_transaction.lock().unwrap();
            if !is_in_tx {
                return Err("No transaction in progress".to_string());
            }

            let mut tx = in_transaction.lock().unwrap();
            *tx = false;
            Ok(())
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
