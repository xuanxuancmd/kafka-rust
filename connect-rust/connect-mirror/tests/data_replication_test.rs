// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! End-to-end tests for data replication from external system.
//!
//! This module tests the complete data replication flow:
//! - External Kafka cluster (simulated by MockConsumer)
//! - MirrorSourceConnector/MirrorSourceTask (data pulling and transformation)
//! - Local Kafka cluster (simulated by MockProducer)
//!
//! Corresponds to Java: MirrorConnectorsIntegrationBaseTest.testReplication() (lines 282-431)

use std::collections::HashMap;

use common_trait::TopicPartition;
use connect_api::connector::ConnectRecord;
use connect_api::connector::Task;
use connect_api::source::SourceTask as SourceTaskTrait;
use connect_mirror::config::{SOURCE_CLUSTER_ALIAS_CONFIG, TARGET_CLUSTER_ALIAS_CONFIG};
use connect_mirror::source::mirror_source_task::MirrorSourceTask;
use connect_mirror_client::DefaultReplicationPolicy;
use connect_mirror_client::ReplicationPolicy;
use kafka_clients_mock::{ConsumerRecord, MockKafkaConsumer, MockKafkaProducer, ProducerRecord};

// ============================================================================
// Helper Functions
// ============================================================================

/// Creates test records for a topic partition.
fn create_test_records(
    topic: &str,
    partition: i32,
    num_records: usize,
    start_offset: i64,
) -> Vec<ConsumerRecord> {
    let mut records = Vec::with_capacity(num_records);
    for i in 0..num_records {
        let offset = start_offset + i as i64;
        let key = Some(format!("key-{}", offset).into_bytes());
        let value = Some(format!("value-{}", offset).into_bytes());
        let timestamp = 1000 + offset;
        records.push(ConsumerRecord::new(
            topic, partition, offset, key, value, timestamp,
        ));
    }
    records
}

/// Creates test configuration for MirrorSourceTask.
fn create_test_config(
    source_alias: &str,
    target_alias: &str,
    topic_partitions: &str,
) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert(
        SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
        source_alias.to_string(),
    );
    props.insert(
        TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
        target_alias.to_string(),
    );
    props.insert("topic.partitions".to_string(), topic_partitions.to_string());
    props
}

/// Creates a MockConsumer pre-loaded with test records.
fn create_mock_consumer_with_records(
    topic: &str,
    partition: i32,
    records: Vec<ConsumerRecord>,
) -> MockKafkaConsumer {
    let consumer = MockKafkaConsumer::new();
    let tp = TopicPartition::new(topic, partition);

    let mut records_map: HashMap<TopicPartition, Vec<ConsumerRecord>> = HashMap::new();
    records_map.insert(tp.clone(), records);

    consumer.assign(vec![tp]);
    consumer.add_records(records_map);

    consumer
}

// ============================================================================
// Tests - Basic Data Replication
// ============================================================================

/// Tests basic data replication flow from external system.
#[test]
fn test_data_replication_basic() {
    let source_topic = "source-topic";
    let partition = 0;
    let num_records = 10;
    let records = create_test_records(source_topic, partition, num_records, 0);

    let consumer = create_mock_consumer_with_records(source_topic, partition, records);

    let mut task = MirrorSourceTask::with_consumer(consumer);

    let config = create_test_config("backup", "primary", "source-topic:0");
    task.start(config);

    let source_records = task.poll().expect("poll should succeed");

    assert_eq!(source_records.len(), num_records);

    let replication_policy = DefaultReplicationPolicy::new();
    let expected_target_topic = replication_policy.format_remote_topic("backup", source_topic);

    for source_record in &source_records {
        assert_eq!(source_record.topic(), expected_target_topic);

        let source_partition = source_record.source_partition();
        assert_eq!(
            source_partition.get("topic").and_then(|v| v.as_str()),
            Some(source_topic)
        );
        assert_eq!(
            source_partition.get("partition").and_then(|v| v.as_i64()),
            Some(partition as i64)
        );
    }

    task.stop();
}

/// Tests data replication with multiple partitions.
#[test]
fn test_data_replication_multiple_partitions() {
    let source_topic = "multi-part-topic";
    let num_partitions = 3;
    let records_per_partition = 5;

    let consumer = MockKafkaConsumer::new();

    let partitions: Vec<TopicPartition> = (0..num_partitions)
        .map(|p| TopicPartition::new(source_topic, p))
        .collect();
    consumer.assign(partitions.clone());

    for i in 0..num_partitions {
        let records = create_test_records(source_topic, i, records_per_partition, 0);
        let mut records_map: HashMap<TopicPartition, Vec<ConsumerRecord>> = HashMap::new();
        records_map.insert(TopicPartition::new(source_topic, i), records);
        consumer.add_records(records_map);
    }

    let mut task = MirrorSourceTask::with_consumer(consumer);

    let tp_config = format!("{}:0,{}:1,{}:2", source_topic, source_topic, source_topic);
    let config = create_test_config("source", "target", &tp_config);
    task.start(config);

    let source_records = task.poll().expect("poll should succeed");

    let expected_total = num_partitions as usize * records_per_partition as usize;
    assert_eq!(source_records.len(), expected_total);

    let replication_policy = DefaultReplicationPolicy::new();
    let expected_target_topic = replication_policy.format_remote_topic("source", source_topic);

    for record in &source_records {
        assert_eq!(record.topic(), expected_target_topic);
    }

    task.stop();
}

/// Tests data replication with large offset values.
#[test]
fn test_data_replication_large_offsets() {
    let source_topic = "large-offset-topic";
    let partition = 0;

    let start_offset = 1000000i64;
    let records = create_test_records(source_topic, partition, 5, start_offset);

    let consumer = create_mock_consumer_with_records(source_topic, partition, records);

    let mut task = MirrorSourceTask::with_consumer(consumer);
    let config = create_test_config("cluster-a", "cluster-b", "large-offset-topic:0");
    task.start(config);

    let source_records = task.poll().expect("poll should succeed");

    assert_eq!(source_records.len(), 5);

    for (i, record) in source_records.iter().enumerate() {
        let source_offset = record.source_offset();
        let offset_value = source_offset.get("offset").and_then(|v| v.as_i64());
        assert_eq!(offset_value, Some(start_offset + i as i64));
    }

    task.stop();
}

/// Tests data replication with null key/value records.
#[test]
fn test_data_replication_null_values() {
    let source_topic = "null-values-topic";
    let partition = 0;

    let records = vec![
        ConsumerRecord::new(
            source_topic,
            partition,
            0,
            None,
            Some(b"value-only".to_vec()),
            1000,
        ),
        ConsumerRecord::new(
            source_topic,
            partition,
            1,
            Some(b"key-only".to_vec()),
            None,
            1001,
        ),
        ConsumerRecord::new(source_topic, partition, 2, None, None, 1002),
    ];

    let consumer = create_mock_consumer_with_records(source_topic, partition, records);

    let mut task = MirrorSourceTask::with_consumer(consumer);
    let config = create_test_config("src", "tgt", "null-values-topic:0");
    task.start(config);

    let source_records = task.poll().expect("poll should succeed");

    assert_eq!(source_records.len(), 3);

    for source_record in &source_records {
        // Key and value are represented as Option<Value>
        // Null values become JSON null
        if let Some(key) = source_record.key() {
            assert!(!key.is_null());
        }
        let value = source_record.value();
        // Value can be JSON object or null
        assert!(!value.is_null() || source_record.key().is_none());
    }

    task.stop();
}

// ============================================================================
// Tests - Empty Partition Handling
// ============================================================================

/// Tests handling of empty partition during replication.
#[test]
fn test_empty_partition_handling() {
    let source_topic = "empty-partition-topic";
    let partition = 0;

    let consumer = MockKafkaConsumer::new();
    let tp = TopicPartition::new(source_topic, partition);
    consumer.assign(vec![tp]);

    let mut task = MirrorSourceTask::with_consumer(consumer);
    let config = create_test_config("empty-source", "empty-target", "empty-partition-topic:0");
    task.start(config);

    let source_records = task.poll().expect("poll should succeed");
    assert!(source_records.is_empty());

    let second_poll = task.poll().expect("second poll should succeed");
    assert!(second_poll.is_empty());

    task.stop();
}

/// Tests mixed scenario: some partitions have data, some empty.
#[test]
fn test_mixed_empty_and_filled_partitions() {
    let source_topic = "mixed-topic";

    let consumer = MockKafkaConsumer::new();

    let partitions = vec![
        TopicPartition::new(source_topic, 0),
        TopicPartition::new(source_topic, 1),
        TopicPartition::new(source_topic, 2),
    ];
    consumer.assign(partitions.clone());

    // Add records only to partition 0
    let records_p0 = create_test_records(source_topic, 0, 5, 0);
    let mut records_map: HashMap<TopicPartition, Vec<ConsumerRecord>> = HashMap::new();
    records_map.insert(TopicPartition::new(source_topic, 0), records_p0);
    consumer.add_records(records_map);

    let mut task = MirrorSourceTask::with_consumer(consumer);

    let tp_config = format!("{}:0,{}:1,{}:2", source_topic, source_topic, source_topic);
    let config = create_test_config("mixed-src", "mixed-tgt", &tp_config);
    task.start(config);

    let source_records = task.poll().expect("poll should succeed");

    assert_eq!(source_records.len(), 5);

    for record in &source_records {
        let source_partition = record.source_partition();
        let partition_val = source_partition.get("partition").and_then(|v| v.as_i64());
        assert_eq!(partition_val, Some(0));
    }

    task.stop();
}

// ============================================================================
// Tests - Topic Transformation
// ============================================================================

/// Tests topic transformation with DefaultReplicationPolicy.
#[test]
fn test_topic_transformation_default_policy() {
    let source_topic = "test-topic";
    let partition = 0;
    let records = create_test_records(source_topic, partition, 1, 0);

    let consumer = create_mock_consumer_with_records(source_topic, partition, records);

    let mut task = MirrorSourceTask::with_consumer(consumer);
    let config = create_test_config("backup", "primary", "test-topic:0");
    task.start(config);

    let source_records = task.poll().expect("poll should succeed");
    assert_eq!(source_records.len(), 1);

    let replication_policy = DefaultReplicationPolicy::new();
    let expected_target_topic = replication_policy.format_remote_topic("backup", source_topic);
    assert_eq!(source_records[0].topic(), expected_target_topic);

    task.stop();
}

// ============================================================================
// Tests - Full Replication Workflow
// ============================================================================

/// Tests complete replication workflow with offset tracking.
#[test]
fn test_full_replication_workflow() {
    let source_topic = "workflow-topic";
    let partition = 0;
    let num_records = 20;

    let source_records_data = create_test_records(source_topic, partition, num_records, 0);

    let consumer = create_mock_consumer_with_records(source_topic, partition, source_records_data);

    let producer = MockKafkaProducer::new();

    let mut task = MirrorSourceTask::with_consumer(consumer);
    let config = create_test_config("workflow-source", "workflow-target", "workflow-topic:0");
    task.start(config);

    let source_records = task.poll().expect("poll should succeed");

    let replication_policy = DefaultReplicationPolicy::new();
    let target_topic = replication_policy.format_remote_topic("workflow-source", source_topic);

    for source_record in &source_records {
        let key = source_record.key().and_then(|k| {
            if k.is_null() {
                None
            } else {
                Some(k.to_string().into_bytes())
            }
        });
        let value = source_record.value();
        let value_bytes = if value.is_null() {
            None
        } else {
            Some(value.to_string().into_bytes())
        };

        let producer_record =
            ProducerRecord::with_partition(target_topic.clone(), partition, key, value_bytes);

        let result = producer.send(&producer_record);
        assert!(result.is_ok(), "Producer send should succeed");
    }

    let sent_records = producer.sent_records();
    assert_eq!(sent_records.len(), num_records);

    for record_meta in sent_records {
        // tuple is (topic, partition, key, value, offset)
        assert_eq!(record_meta.0, target_topic);
    }

    task.stop();
}

/// Tests consecutive poll operations maintain state correctly.
#[test]
fn test_multiple_poll_batches() {
    let source_topic = "batch-topic";
    let partition = 0;

    let consumer = MockKafkaConsumer::new();
    let tp = TopicPartition::new(source_topic, partition);
    consumer.assign(vec![tp.clone()]);

    let first_batch_records = create_test_records(source_topic, partition, 10, 0);
    let mut records_map: HashMap<TopicPartition, Vec<ConsumerRecord>> = HashMap::new();
    records_map.insert(tp.clone(), first_batch_records);
    consumer.add_records(records_map);

    let mut task = MirrorSourceTask::with_consumer(consumer);
    let config = create_test_config("batch-src", "batch-tgt", "batch-topic:0");
    task.start(config);

    let first_poll_records = task.poll().expect("first poll should succeed");
    assert_eq!(first_poll_records.len(), 10);

    // Add second batch
    let second_batch_records = create_test_records(source_topic, partition, 10, 10);
    let mut records_map2: HashMap<TopicPartition, Vec<ConsumerRecord>> = HashMap::new();
    records_map2.insert(tp.clone(), second_batch_records);
    task.consumer_mut().add_records(records_map2);

    let second_poll_records = task.poll().expect("second poll should succeed");
    assert_eq!(second_poll_records.len(), 10);

    for (i, record) in second_poll_records.iter().enumerate() {
        let source_offset = record.source_offset();
        let offset_val = source_offset.get("offset").and_then(|v| v.as_i64());
        assert_eq!(offset_val, Some(10 + i as i64));
    }

    task.stop();
}

// ============================================================================
// Tests - Timestamp Preservation
// ============================================================================

/// Tests that timestamps are preserved during replication.
#[test]
fn test_timestamp_preservation() {
    let source_topic = "timestamp-topic";
    let partition = 0;

    let base_timestamp = 1700000000000i64;
    let records: Vec<ConsumerRecord> = (0..5)
        .map(|i| {
            ConsumerRecord::new(
                source_topic,
                partition,
                i,
                Some(format!("key-{}", i).into_bytes()),
                Some(format!("value-{}", i).into_bytes()),
                base_timestamp + i * 1000,
            )
        })
        .collect();

    let consumer = create_mock_consumer_with_records(source_topic, partition, records.clone());

    let mut task = MirrorSourceTask::with_consumer(consumer);
    let config = create_test_config("ts-src", "ts-tgt", "timestamp-topic:0");
    task.start(config);

    let source_records = task.poll().expect("poll should succeed");

    for (i, source_record) in source_records.iter().enumerate() {
        let expected_ts = base_timestamp + i as i64 * 1000;
        assert_eq!(source_record.timestamp(), Some(expected_ts));
    }

    task.stop();
}

// ============================================================================
// Tests - Lifecycle and State Management
// ============================================================================

/// Tests task lifecycle: initialize -> start -> poll -> stop.
#[test]
fn test_task_lifecycle() {
    let topic = "lifecycle-topic";
    let partition = 0;
    let records = create_test_records(topic, partition, 5, 0);

    let consumer = create_mock_consumer_with_records(topic, partition, records);

    let mut task = MirrorSourceTask::with_consumer(consumer);

    assert!(!task.is_stopping());

    let config = create_test_config("life-src", "life-tgt", "lifecycle-topic:0");
    task.start(config);

    assert!(!task.is_stopping());

    let records = task.poll().expect("poll should work");
    assert_eq!(records.len(), 5);

    task.stop();

    assert!(task.is_stopping());

    let empty_poll = task.poll().expect("poll after stop should succeed");
    assert!(empty_poll.is_empty());
}

/// Tests that task handles gracefully when no partitions assigned.
#[test]
fn test_no_partitions_assigned() {
    let consumer = MockKafkaConsumer::new();

    let mut task = MirrorSourceTask::with_consumer(consumer);
    let config = create_test_config("nopart-src", "nopart-tgt", "");
    task.start(config);

    let records = task.poll().expect("poll should succeed");
    assert!(records.is_empty());

    task.stop();
}

/// Tests that task handles wakeup gracefully.
#[test]
fn test_consumer_wakeup_handling() {
    let topic = "wakeup-topic";
    let partition = 0;
    let records = create_test_records(topic, partition, 5, 0);

    let consumer = create_mock_consumer_with_records(topic, partition, records);

    let mut task = MirrorSourceTask::with_consumer(consumer);
    let config = create_test_config("wup-src", "wup-tgt", "wakeup-topic:0");
    task.start(config);

    let first_poll = task.poll().expect("first poll should succeed");
    assert_eq!(first_poll.len(), 5);

    task.consumer_mut().wakeup();

    let second_poll = task.poll().expect("poll after wakeup should succeed");
    assert!(second_poll.is_empty());

    task.stop();
}
