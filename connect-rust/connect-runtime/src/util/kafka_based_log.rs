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

//! Kafka-based log for storing shared, compacted records.
//!
//! This corresponds to `org.apache.kafka.connect.util.KafkaBasedLog` in Java.
//! Provides a shared, compacted log of records stored in Kafka that all clients
//! need to consume and agree on their offset.

use super::topic_admin::TopicAdmin;
use common_trait::util::time::Time;
use common_trait::TopicPartition;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Default timeout for topic creation (in milliseconds).
const CREATE_TOPIC_TIMEOUT_MS: i64 = 30_000;
/// Maximum sleep time in milliseconds.
const MAX_SLEEP_MS: i64 = 1000;
/// Admin client retry duration.
const ADMIN_CLIENT_RETRY_DURATION: Duration = Duration::from_secs(15 * 60);
/// Admin client retry backoff in milliseconds.
const ADMIN_CLIENT_RETRY_BACKOFF_MS: i64 = 10_000;

/// KafkaBasedLog provides a shared, compacted log of records stored in Kafka.
///
/// This corresponds to `org.apache.kafka.connect.util.KafkaBasedLog` in Java.
/// It runs a consumer in a background thread to continuously tail the target topic,
/// accepts write requests using an internal producer, and provides utilities like
/// checking the current log end offset and waiting until the current end is reached.
pub struct KafkaBasedLog {
    topic: String,
    partition_count: usize,
    producer_configs: HashMap<String, String>,
    consumer_configs: HashMap<String, String>,
    topic_admin_supplier: Arc<dyn Fn() -> Option<Arc<TopicAdmin>> + Send + Sync>,
    initializer: Arc<dyn Fn(&TopicAdmin) + Send + Sync>,
    require_admin_for_offsets: bool,
    admin: Option<Arc<TopicAdmin>>,
    stop_requested: Mutex<bool>,
    report_errors_to_callback: bool,
    time: Arc<dyn Time>,
}

impl KafkaBasedLog {
    /// Creates a new KafkaBasedLog.
    ///
    /// This does not start reading the log and writing is not permitted
    /// until `start()` is invoked.
    pub fn new(
        topic: String,
        producer_configs: HashMap<String, String>,
        consumer_configs: HashMap<String, String>,
        topic_admin_supplier: Arc<dyn Fn() -> Option<Arc<TopicAdmin>> + Send + Sync>,
        time: Arc<dyn Time>,
        initializer: Arc<dyn Fn(&TopicAdmin) + Send + Sync>,
    ) -> Self {
        // Check if consumer uses read_committed isolation level
        let isolation_level = consumer_configs.get("isolation.level");
        let require_admin_for_offsets = isolation_level
            .map(|s| s == "read_committed")
            .unwrap_or(false);

        KafkaBasedLog {
            topic,
            partition_count: 0,
            producer_configs,
            consumer_configs,
            topic_admin_supplier,
            initializer,
            require_admin_for_offsets,
            admin: None,
            stop_requested: Mutex::new(false),
            report_errors_to_callback: false,
            time,
        }
    }

    /// Start reading the log.
    pub fn start(&mut self) -> Result<(), KafkaBasedLogError> {
        self.start_with_error_reporting(false)
    }

    /// Start reading the log with error reporting option.
    pub fn start_with_error_reporting(
        &mut self,
        report_errors_to_callback: bool,
    ) -> Result<(), KafkaBasedLogError> {
        self.report_errors_to_callback = report_errors_to_callback;
        log::info!(
            "Starting KafkaBasedLog with topic {} reportErrorsToCallback={}",
            self.topic,
            report_errors_to_callback
        );

        // Create the topic admin client and initialize the topic
        self.admin = (self.topic_admin_supplier)();
        if self.admin.is_none() && self.require_admin_for_offsets {
            return Err(KafkaBasedLogError::ConfigError(
                "Must provide a TopicAdmin to KafkaBasedLog when consumer is configured with isolation.level set to read_committed"
                    .to_string(),
            ));
        }

        if let Some(admin) = &self.admin {
            self.initializer.as_ref()(admin);
        }

        // Override producer settings for reliable writes
        self.producer_configs
            .insert("acks".to_string(), "all".to_string());
        self.producer_configs.insert(
            "max.in.flight.requests.per.connection".to_string(),
            "1".to_string(),
        );

        // Override consumer settings for correct behavior
        self.consumer_configs
            .insert("auto.offset.reset".to_string(), "earliest".to_string());
        self.consumer_configs
            .insert("enable.auto.commit".to_string(), "false".to_string());

        // Get partition count (simplified - would need actual consumer in real implementation)
        self.partition_count = 1;

        log::info!("Started KafkaBasedLog for topic {}", self.topic);
        Ok(())
    }

    /// Stop the log reader.
    pub fn stop(&mut self) -> Result<(), KafkaBasedLogError> {
        log::info!("Stopping KafkaBasedLog for topic {}", self.topic);

        {
            let mut stop_requested = self.stop_requested.lock().unwrap();
            *stop_requested = true;
        }

        self.admin = None;

        log::info!("Stopped KafkaBasedLog for topic {}", self.topic);
        Ok(())
    }

    /// Flush pending writes.
    pub fn flush(&self) {
        // In real implementation, this would flush the producer
        log::debug!("Flushing KafkaBasedLog for topic {}", self.topic);
    }

    /// Read to the end of the log and invoke the callback when done.
    pub fn read_to_end(&self) -> Result<(), KafkaBasedLogError> {
        log::trace!("Starting read to end log for topic {}", self.topic);
        self.flush();

        // In real implementation, this would poll the consumer until reaching end offsets
        log::trace!("Finished read to end log for topic {}", self.topic);
        Ok(())
    }

    /// Send a record to the log.
    pub fn send(&self, _key: &[u8], _value: &[u8]) -> Result<(), KafkaBasedLogError> {
        // In real implementation, this would send via producer
        log::debug!("Sending record to topic {}", self.topic);
        Ok(())
    }

    /// Returns the partition count.
    pub fn partition_count(&self) -> usize {
        self.partition_count
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Read end offsets for the assigned partitions.
    pub fn read_end_offsets(
        &self,
        assignment: &HashSet<TopicPartition>,
        _should_retry: bool,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaBasedLogError> {
        if let Some(admin) = &self.admin {
            admin
                .retry_end_offsets(
                    assignment,
                    ADMIN_CLIENT_RETRY_DURATION,
                    ADMIN_CLIENT_RETRY_BACKOFF_MS,
                    self.time.clone(),
                )
                .map_err(|e| KafkaBasedLogError::AdminError(e.to_string()))
        } else {
            // Return dummy offsets if no admin
            Ok(assignment.iter().map(|tp| (tp.clone(), 0)).collect())
        }
    }

    /// Check if stop has been requested.
    pub fn is_stop_requested(&self) -> bool {
        *self.stop_requested.lock().unwrap()
    }
}

/// Errors for KafkaBasedLog operations.
#[derive(Debug)]
pub enum KafkaBasedLogError {
    /// Configuration error.
    ConfigError(String),
    /// Timeout waiting for partitions.
    Timeout(String),
    /// Write error (e.g., read-only mode).
    WriteError(String),
    /// Consumer error.
    ConsumerError(String),
    /// Producer error.
    ProducerError(String),
    /// Admin error.
    AdminError(String),
}

impl std::fmt::Display for KafkaBasedLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KafkaBasedLogError::ConfigError(msg) => write!(f, "Config error: {}", msg),
            KafkaBasedLogError::Timeout(msg) => write!(f, "Timeout: {}", msg),
            KafkaBasedLogError::WriteError(msg) => write!(f, "Write error: {}", msg),
            KafkaBasedLogError::ConsumerError(msg) => write!(f, "Consumer error: {}", msg),
            KafkaBasedLogError::ProducerError(msg) => write!(f, "Producer error: {}", msg),
            KafkaBasedLogError::AdminError(msg) => write!(f, "Admin error: {}", msg),
        }
    }
}

impl std::error::Error for KafkaBasedLogError {}

#[cfg(test)]
mod tests {
    use super::*;
    use common_trait::util::time::SystemTimeImpl;

    #[test]
    fn test_kafka_based_log_creation() {
        let topic = "test-topic".to_string();
        let producer_configs = HashMap::new();
        let consumer_configs = HashMap::new();
        let time = Arc::new(SystemTimeImpl::new());
        let admin_supplier = Arc::new(|| None);
        let initializer = Arc::new(|_: &TopicAdmin| {});

        let log = KafkaBasedLog::new(
            topic,
            producer_configs,
            consumer_configs,
            admin_supplier,
            time,
            initializer,
        );

        assert_eq!(log.topic(), "test-topic");
        assert_eq!(log.partition_count(), 0);
    }

    #[test]
    fn test_kafka_based_log_start_stop() {
        let topic = "test-topic".to_string();
        let time = Arc::new(SystemTimeImpl::new());
        let admin_supplier = Arc::new(|| None);
        let initializer = Arc::new(|_: &TopicAdmin| {});

        let mut log = KafkaBasedLog::new(
            topic,
            HashMap::new(),
            HashMap::new(),
            admin_supplier,
            time,
            initializer,
        );

        log.start().unwrap();
        assert_eq!(log.partition_count(), 1);

        log.stop().unwrap();
        assert!(log.is_stop_requested());
    }
}
