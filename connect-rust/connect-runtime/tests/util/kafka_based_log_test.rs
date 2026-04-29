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

//! Tests for KafkaBasedLog.
//!
//! Corresponds to `org.apache.kafka.connect.util.KafkaBasedLogTest` in Java.

use common_trait::util::time::SystemTimeImpl;
use common_trait::TopicPartition;
use connect_runtime::util::{KafkaBasedLog, KafkaBasedLogError, TopicAdmin};
use kafka_clients_mock::MockAdmin;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Helper to create a basic KafkaBasedLog for testing.
fn create_test_log(topic: &str, consumer_configs: HashMap<String, String>) -> KafkaBasedLog {
    let time = Arc::new(SystemTimeImpl::new());
    let admin_supplier = Arc::new(|| None);
    let initializer = Arc::new(|_: &TopicAdmin| {});

    KafkaBasedLog::new(
        topic.to_string(),
        HashMap::new(),
        consumer_configs,
        admin_supplier,
        time,
        initializer,
    )
}

/// Helper to create a KafkaBasedLog with admin supplier.
fn create_test_log_with_admin(topic: &str, admin: Arc<MockAdmin>) -> KafkaBasedLog {
    let time = Arc::new(SystemTimeImpl::new());
    let topic_admin = Arc::new(TopicAdmin::for_testing(None, admin.clone(), true));
    let admin_supplier = Arc::new(move || Some(topic_admin.clone()));
    let initializer = Arc::new(|_: &TopicAdmin| {});

    KafkaBasedLog::new(
        topic.to_string(),
        HashMap::new(),
        HashMap::new(),
        admin_supplier,
        time,
        initializer,
    )
}

// ===== Basic Creation Tests =====

#[test]
fn test_kafka_based_log_creation_basic() {
    let log = create_test_log("test-topic", HashMap::new());

    assert_eq!(log.topic(), "test-topic");
    assert_eq!(log.partition_count(), 0);
    assert!(!log.is_stop_requested());
}

#[test]
fn test_kafka_based_log_creation_with_configs() {
    let mut producer_configs = HashMap::new();
    producer_configs.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );

    let mut consumer_configs = HashMap::new();
    consumer_configs.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    consumer_configs.insert("group.id".to_string(), "test-group".to_string());

    let time = Arc::new(SystemTimeImpl::new());
    let admin_supplier = Arc::new(|| None);
    let initializer = Arc::new(|_: &TopicAdmin| {});

    let log = KafkaBasedLog::new(
        "config-topic".to_string(),
        producer_configs,
        consumer_configs,
        admin_supplier,
        time,
        initializer,
    );

    assert_eq!(log.topic(), "config-topic");
}

#[test]
fn test_kafka_based_log_with_read_committed_isolation() {
    let mut consumer_configs = HashMap::new();
    consumer_configs.insert("isolation.level".to_string(), "read_committed".to_string());

    let time = Arc::new(SystemTimeImpl::new());
    let admin_supplier = Arc::new(|| None);
    let initializer = Arc::new(|_: &TopicAdmin| {});

    let mut log = KafkaBasedLog::new(
        "committed-topic".to_string(),
        HashMap::new(),
        consumer_configs,
        admin_supplier,
        time,
        initializer,
    );

    // Starting without admin should fail when using read_committed isolation
    let result = log.start();
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        KafkaBasedLogError::ConfigError(_)
    ));
}

#[test]
fn test_kafka_based_log_with_read_committed_and_admin() {
    let mut consumer_configs = HashMap::new();
    consumer_configs.insert("isolation.level".to_string(), "read_committed".to_string());

    let admin = Arc::new(MockAdmin::new());
    let time = Arc::new(SystemTimeImpl::new());
    let topic_admin = Arc::new(TopicAdmin::for_testing(None, admin.clone(), true));
    let admin_supplier = Arc::new(move || Some(topic_admin.clone()));
    let initializer = Arc::new(|_: &TopicAdmin| {});

    let mut log = KafkaBasedLog::new(
        "committed-with-admin-topic".to_string(),
        HashMap::new(),
        consumer_configs,
        admin_supplier,
        time,
        initializer,
    );

    // Should succeed with admin
    let result = log.start();
    assert!(result.is_ok());
}

// ===== Start/Stop Tests =====

#[test]
fn test_start_stop_basic() {
    let mut log = create_test_log("start-stop-topic", HashMap::new());

    // Start the log
    let start_result = log.start();
    assert!(start_result.is_ok());
    assert_eq!(log.partition_count(), 1);
    assert!(!log.is_stop_requested());

    // Stop the log
    let stop_result = log.stop();
    assert!(stop_result.is_ok());
    assert!(log.is_stop_requested());
}

#[test]
fn test_start_with_error_reporting() {
    let mut log = create_test_log("error-report-topic", HashMap::new());

    let start_result = log.start_with_error_reporting(true);
    assert!(start_result.is_ok());

    let stop_result = log.stop();
    assert!(stop_result.is_ok());
}

#[test]
fn test_stop_before_start() {
    let mut log = create_test_log("stop-before-start-topic", HashMap::new());

    // Stop before start should succeed
    let stop_result = log.stop();
    assert!(stop_result.is_ok());
    assert!(log.is_stop_requested());
}

#[test]
fn test_multiple_start_stop_cycles() {
    let mut log = create_test_log("cycle-topic", HashMap::new());

    // First cycle
    assert!(log.start().is_ok());
    assert!(log.stop().is_ok());

    // Reset stop flag manually (simulating new instance)
    // Note: In real usage, a new instance would be created
    let mut log2 = create_test_log("cycle-topic", HashMap::new());
    assert!(log2.start().is_ok());
    assert!(log2.stop().is_ok());
}

// ===== Send and Read Tests =====

#[test]
fn test_send_basic() {
    let mut log = create_test_log("send-topic", HashMap::new());
    log.start().unwrap();

    // Send a record
    let key = b"test-key";
    let value = b"test-value";
    let result = log.send(key, value);
    assert!(result.is_ok());
}

#[test]
fn test_send_empty_key_value() {
    let mut log = create_test_log("empty-send-topic", HashMap::new());
    log.start().unwrap();

    // Send with empty key and value
    let result = log.send(&[], &[]);
    assert!(result.is_ok());
}

#[test]
fn test_read_to_end_basic() {
    let mut log = create_test_log("read-end-topic", HashMap::new());
    log.start().unwrap();

    // Read to end
    let result = log.read_to_end();
    assert!(result.is_ok());
}

#[test]
fn test_flush() {
    let mut log = create_test_log("flush-topic", HashMap::new());
    log.start().unwrap();

    // Flush should succeed
    log.flush();
}

// ===== End Offsets Tests =====

#[test]
fn test_read_end_offsets_without_admin() {
    let mut log = create_test_log("offsets-no-admin-topic", HashMap::new());
    log.start().unwrap();

    let mut assignment = HashSet::new();
    assignment.insert(TopicPartition::new("test-topic", 0));
    assignment.insert(TopicPartition::new("test-topic", 1));

    // Without admin, returns zero offsets
    let offsets = log.read_end_offsets(&assignment, false).unwrap();
    assert_eq!(offsets.len(), 2);
    assert_eq!(offsets.get(&TopicPartition::new("test-topic", 0)), Some(&0));
    assert_eq!(offsets.get(&TopicPartition::new("test-topic", 1)), Some(&0));
}

#[test]
fn test_read_end_offsets_with_admin() {
    let admin = Arc::new(MockAdmin::new());
    // Create a topic first
    let topic = kafka_clients_mock::NewTopic::new("offsets-topic", 2, 1);
    admin.create_topics(vec![topic]);

    let mut log = create_test_log_with_admin("offsets-topic", admin.clone());
    log.start().unwrap();

    let mut assignment = HashSet::new();
    assignment.insert(TopicPartition::new("offsets-topic", 0));
    assignment.insert(TopicPartition::new("offsets-topic", 1));

    // With admin, returns actual offsets (mock returns 1000 for latest)
    let offsets = log.read_end_offsets(&assignment, false).unwrap();
    assert_eq!(offsets.len(), 2);
}

#[test]
fn test_read_end_offsets_empty_assignment() {
    let mut log = create_test_log("empty-assignment-topic", HashMap::new());
    log.start().unwrap();

    let assignment = HashSet::new();

    let offsets = log.read_end_offsets(&assignment, false).unwrap();
    assert!(offsets.is_empty());
}

#[test]
fn test_read_end_offsets_nonexistent_topic() {
    let admin = Arc::new(MockAdmin::new());
    let mut log = create_test_log_with_admin("nonexistent-topic", admin.clone());
    log.start().unwrap();

    let mut assignment = HashSet::new();
    assignment.insert(TopicPartition::new("nonexistent-topic", 0));

    // Topic doesn't exist in admin, should return error
    let result = log.read_end_offsets(&assignment, false);
    assert!(result.is_err());
}

// ===== Error Handling Tests =====

#[test]
fn test_error_display_config_error() {
    let error = KafkaBasedLogError::ConfigError("test config error".to_string());
    assert!(error.to_string().contains("Config error"));
}

#[test]
fn test_error_display_timeout() {
    let error = KafkaBasedLogError::Timeout("test timeout".to_string());
    assert!(error.to_string().contains("Timeout"));
}

#[test]
fn test_error_display_write_error() {
    let error = KafkaBasedLogError::WriteError("test write error".to_string());
    assert!(error.to_string().contains("Write error"));
}

#[test]
fn test_error_display_consumer_error() {
    let error = KafkaBasedLogError::ConsumerError("test consumer error".to_string());
    assert!(error.to_string().contains("Consumer error"));
}

#[test]
fn test_error_display_producer_error() {
    let error = KafkaBasedLogError::ProducerError("test producer error".to_string());
    assert!(error.to_string().contains("Producer error"));
}

#[test]
fn test_error_display_admin_error() {
    let error = KafkaBasedLogError::AdminError("test admin error".to_string());
    assert!(error.to_string().contains("Admin error"));
}

#[test]
fn test_error_is_std_error() {
    let error = KafkaBasedLogError::ConfigError("test".to_string());
    // Verify it implements std::error::Error
    let _: &dyn std::error::Error = &error;
}

// ===== Topic Name Tests =====

#[test]
fn test_topic_name_accessor() {
    let topics = [
        "simple-topic",
        "topic-with-dashes",
        "topic_with_underscores",
        "TOPIC_WITH_UPPERCASE",
        "topic123",
    ];

    for topic_name in topics {
        let log = create_test_log(topic_name, HashMap::new());
        assert_eq!(log.topic(), topic_name);
    }
}

#[test]
fn test_partition_count_after_start() {
    let mut log = create_test_log("partition-count-topic", HashMap::new());

    // Before start, partition count is 0
    assert_eq!(log.partition_count(), 0);

    // After start, partition count is set
    log.start().unwrap();
    assert_eq!(log.partition_count(), 1);
}

// ===== Integration-style Tests =====

#[test]
fn test_full_workflow() {
    let mut log = create_test_log("workflow-topic", HashMap::new());

    // Start
    log.start().unwrap();

    // Send some data
    log.send(b"key1", b"value1").unwrap();
    log.send(b"key2", b"value2").unwrap();

    // Flush
    log.flush();

    // Read to end
    log.read_to_end().unwrap();

    // Stop
    log.stop().unwrap();
    assert!(log.is_stop_requested());
}

#[test]
fn test_workflow_with_admin_client() {
    let admin = Arc::new(MockAdmin::new());
    let topic = kafka_clients_mock::NewTopic::new("admin-workflow-topic", 3, 1);
    admin.create_topics(vec![topic]);

    let mut log = create_test_log_with_admin("admin-workflow-topic", admin.clone());

    // Start
    log.start().unwrap();

    // Send
    log.send(b"key", b"value").unwrap();

    // Read end offsets
    let mut assignment = HashSet::new();
    assignment.insert(TopicPartition::new("admin-workflow-topic", 0));
    assignment.insert(TopicPartition::new("admin-workflow-topic", 1));
    assignment.insert(TopicPartition::new("admin-workflow-topic", 2));

    let offsets = log.read_end_offsets(&assignment, false).unwrap();
    assert_eq!(offsets.len(), 3);

    // Stop
    log.stop().unwrap();
}
