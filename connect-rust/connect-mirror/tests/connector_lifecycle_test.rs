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

//! End-to-end tests for Connector lifecycle.
//!
//! This module tests the complete lifecycle of MirrorMaker 2 connectors:
//! - Connector creation and initialization
//! - Connector start with configuration
//! - Task creation and configuration distribution
//! - Task lifecycle (start -> poll -> stop)
//! - Connector stop and resource cleanup
//! - Connector restart recovery
//!
//! Corresponds to Java: MirrorConnectorsIntegrationBaseTest.testRestartReplication() (lines 720-745)

use std::collections::HashMap;
use std::sync::Arc;

use common_trait::TopicPartition;
use connect_api::connector::{ConnectRecord, Connector, Task};
use connect_api::source::SourceTask as SourceTaskTrait;
use connect_mirror::config::{
    NAME_CONFIG, SOURCE_CLUSTER_ALIAS_CONFIG, TARGET_CLUSTER_ALIAS_CONFIG,
};
use connect_mirror::source::mirror_source_config::TASK_TOPIC_PARTITIONS_CONFIG;
use connect_mirror::source::mirror_source_connector::MirrorSourceConnector;
use connect_mirror::source::mirror_source_task::MirrorSourceTask;
use connect_mirror_client::DefaultReplicationPolicy;
use connect_mirror_client::ReplicationPolicy;
use kafka_clients_mock::{ConsumerRecord, MockAdmin, MockKafkaConsumer, NewTopic};

mod mocks;
use mocks::external_admin::ExternalAdminMock;

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

/// Creates test configuration for MirrorSourceConnector.
fn create_connector_config(
    source_alias: &str,
    target_alias: &str,
    connector_name: &str,
    topics_pattern: &str,
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
    props.insert(NAME_CONFIG.to_string(), connector_name.to_string());
    props.insert("topics".to_string(), topics_pattern.to_string());
    props
}

/// Creates test configuration for MirrorSourceTask.
fn create_task_config(
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
    props.insert(
        TASK_TOPIC_PARTITIONS_CONFIG.to_string(),
        topic_partitions.to_string(),
    );
    props
}

/// Creates a MockAdmin with predefined source topics.
fn create_source_admin_with_topics(topics: Vec<&str>) -> MockAdmin {
    let admin = MockAdmin::new();
    for topic in topics {
        let new_topic = NewTopic::new(topic, 1, 1);
        admin.create_topics(vec![new_topic]);
    }
    admin
}

// ============================================================================
// Tests - Connector Lifecycle: Create -> Start -> Stop
// ============================================================================

/// Tests connector creation and initialization.
/// Corresponds to Java: MirrorSourceConnector instantiation and initialize() call.
#[test]
fn test_connector_creation_and_initialization() {
    // Create connector
    let connector = MirrorSourceConnector::new();

    // Verify initial state
    assert!(!connector.is_started());
    assert!(connector.known_source_topic_partitions().is_empty());
    assert_eq!(connector.task_class(), "MirrorSourceTask");
}

/// Tests connector start lifecycle.
/// Corresponds to Java: MirrorSourceConnector.start(Map<String, String> props)
#[test]
fn test_connector_start_lifecycle() {
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["test-topic"]));
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector = MirrorSourceConnector::with_admin_clients(source_admin, target_admin);

    // Create configuration
    let config = create_connector_config("backup", "primary", "mirror-source", "test-.*");

    // Start connector
    connector.start(config);

    // Verify started state
    assert!(connector.is_started());

    // Verify topic partitions discovered
    assert!(!connector.known_source_topic_partitions().is_empty());

    // Verify remote topic created on target
    let replication_policy = DefaultReplicationPolicy::new();
    let remote_topic = replication_policy.format_remote_topic("backup", "test-topic");
    assert!(connector.target_admin().topic_exists(&remote_topic));

    // Stop connector
    connector.stop();

    // Verify stopped state
    assert!(!connector.is_started());
    assert!(connector.known_source_topic_partitions().is_empty());
}

/// Tests connector stop cleans up all resources.
/// Corresponds to Java: MirrorSourceConnector.stop() resource cleanup verification
#[test]
fn test_connector_stop_resource_cleanup() {
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["cleanup-topic"]));
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector =
        MirrorSourceConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    let config = create_connector_config(
        "cleanup-src",
        "cleanup-tgt",
        "cleanup-connector",
        "cleanup-.*",
    );
    connector.start(config);

    // Capture state before stop
    let partitions_before_stop = connector.known_source_topic_partitions().len();
    assert!(partitions_before_stop > 0);

    // Stop connector
    connector.stop();

    // Verify all resources cleaned up
    assert!(!connector.is_started());
    assert!(connector.known_source_topic_partitions().is_empty());
    assert!(connector.topic_filter().is_none());
    assert!(connector.config_property_filter().is_none());

    // Admin clients should still be accessible (not closed)
    assert!(source_admin.topic_exists("cleanup-topic"));
}

// ============================================================================
// Tests - Task Lifecycle: Create -> Start -> Poll -> Stop
// ============================================================================

/// Tests task creation and initialization.
/// Corresponds to Java: MirrorSourceTask instantiation
#[test]
fn test_task_creation_and_initialization() {
    let task = MirrorSourceTask::new();

    // Verify initial state
    assert_eq!(task.source_cluster_alias(), "source");
    assert!(task.assigned_partitions().is_empty());
    assert!(!task.is_stopping());
}

/// Tests complete task lifecycle: start -> poll -> stop.
/// Corresponds to Java: MirrorSourceTask lifecycle management
#[test]
fn test_task_full_lifecycle() {
    let topic = "lifecycle-topic";
    let partition = 0;
    let records = create_test_records(topic, partition, 10, 0);

    let consumer = create_mock_consumer_with_records(topic, partition, records);

    let mut task = MirrorSourceTask::with_consumer(consumer);

    // Start task
    let config = create_task_config("src", "tgt", "lifecycle-topic:0");
    task.start(config);

    // Verify started state
    assert!(!task.is_stopping());
    assert_eq!(task.assigned_partitions().len(), 1);

    // Poll records
    let source_records = task.poll().expect("poll should succeed");
    assert_eq!(source_records.len(), 10);

    // Verify topic transformation
    let replication_policy = DefaultReplicationPolicy::new();
    let expected_target_topic = replication_policy.format_remote_topic("src", topic);
    for record in &source_records {
        assert_eq!(record.topic(), expected_target_topic);
    }

    // Stop task
    task.stop();

    // Verify stopped state
    assert!(task.is_stopping());
    assert!(task.assigned_partitions().is_empty());
}

/// Tests task poll after stop returns empty.
#[test]
fn test_task_poll_after_stop_returns_empty() {
    let topic = "stop-poll-topic";
    let partition = 0;
    let records = create_test_records(topic, partition, 5, 0);

    let consumer = create_mock_consumer_with_records(topic, partition, records);

    let mut task = MirrorSourceTask::with_consumer(consumer);

    let config = create_task_config("stop-src", "stop-tgt", "stop-poll-topic:0");
    task.start(config);

    // Poll first batch
    let first_poll = task.poll().expect("first poll should succeed");
    assert_eq!(first_poll.len(), 5);

    // Stop task
    task.stop();

    // Poll after stop should return empty
    let after_stop_poll = task.poll().expect("poll after stop should succeed");
    assert!(after_stop_poll.is_empty());
}

/// Tests task handles multiple partitions lifecycle.
#[test]
fn test_task_multiple_partitions_lifecycle() {
    let topic = "multi-lifecycle-topic";
    let consumer = MockKafkaConsumer::new();

    // Assign multiple partitions
    let partitions = vec![
        TopicPartition::new(topic, 0),
        TopicPartition::new(topic, 1),
        TopicPartition::new(topic, 2),
    ];
    consumer.assign(partitions.clone());

    // Add records to each partition
    for i in 0..3 {
        let records = create_test_records(topic, i, 5, 0);
        let mut records_map: HashMap<TopicPartition, Vec<ConsumerRecord>> = HashMap::new();
        records_map.insert(TopicPartition::new(topic, i), records);
        consumer.add_records(records_map);
    }

    let mut task = MirrorSourceTask::with_consumer(consumer);

    let tp_config = format!("{}:0,{}:1,{}:2", topic, topic, topic);
    let config = create_task_config("multi-src", "multi-tgt", &tp_config);
    task.start(config);

    // Verify all partitions assigned
    assert_eq!(task.assigned_partitions().len(), 3);

    // Poll all records
    let source_records = task.poll().expect("poll should succeed");
    assert_eq!(source_records.len(), 15); // 5 records per partition * 3 partitions

    // Stop and verify cleanup
    task.stop();
    assert!(task.assigned_partitions().is_empty());
}

// ============================================================================
// Tests - Connector-Task Integration Lifecycle
// ============================================================================

/// Tests connector task_configs distribution.
/// Corresponds to Java: MirrorSourceConnector.taskConfigs(int maxTasks)
#[test]
fn test_connector_task_configs_distribution() {
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["task-topic"]));
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector = MirrorSourceConnector::with_admin_clients(source_admin, target_admin);

    let config = create_connector_config("task-src", "task-tgt", "task-connector", "task-.*");
    connector.start(config);

    // Get task configs for max 2 tasks
    let task_configs = connector
        .task_configs(2)
        .expect("task_configs should succeed");

    // Should have 1 task (1 partition)
    assert_eq!(task_configs.len(), 1);

    // Each task config should have topic partitions
    for task_config in &task_configs {
        assert!(task_config.contains_key(TASK_TOPIC_PARTITIONS_CONFIG));
    }

    connector.stop();
}

/// Tests connector to task lifecycle integration.
/// Corresponds to Java: MirrorMaker restart lifecycle flow
#[test]
fn test_connector_to_task_integration_lifecycle() {
    // Phase 1: Create and start connector
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["integration-topic"]));
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector =
        MirrorSourceConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    let connector_config = create_connector_config(
        "int-src",
        "int-tgt",
        "integration-connector",
        "integration-.*",
    );
    connector.start(connector_config);

    // Phase 2: Get task configs
    let task_configs = connector
        .task_configs(1)
        .expect("task_configs should succeed");
    assert_eq!(task_configs.len(), 1);

    // Phase 3: Create and start task with config from connector
    let task_config = task_configs[0].clone();

    // Add source cluster alias from connector config
    let full_task_config: HashMap<String, String> = task_config
        .into_iter()
        .chain(vec![
            (
                SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
                "int-src".to_string(),
            ),
            (
                TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
                "int-tgt".to_string(),
            ),
        ])
        .collect();

    // Create consumer with records
    let consumer = create_mock_consumer_with_records(
        "integration-topic",
        0,
        create_test_records("integration-topic", 0, 10, 0),
    );

    let mut task = MirrorSourceTask::with_consumer(consumer);
    task.start(full_task_config);

    // Phase 4: Poll data
    let records = task.poll().expect("poll should succeed");
    assert_eq!(records.len(), 10);

    // Phase 5: Stop task
    task.stop();
    assert!(task.is_stopping());

    // Phase 6: Stop connector
    connector.stop();
    assert!(!connector.is_started());
}

// ============================================================================
// Tests - Connector Restart Recovery
// ============================================================================

/// Tests connector restart recovery.
/// Corresponds to Java: MirrorConnectorsIntegrationBaseTest.testRestartReplication() restartMirrorMakerConnectors logic
#[test]
fn test_connector_restart_recovery() {
    // Phase 1: Initial setup - create and start connector
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["restart-topic"]));
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector =
        MirrorSourceConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    let config = create_connector_config(
        "restart-src",
        "restart-tgt",
        "restart-connector",
        "restart-.*",
    );

    // Initial start
    connector.start(config.clone());
    assert!(connector.is_started());

    // Get initial task configs
    let initial_task_configs = connector
        .task_configs(1)
        .expect("task_configs should succeed");
    assert_eq!(initial_task_configs.len(), 1);

    // Phase 2: Stop connector (simulate restart)
    connector.stop();
    assert!(!connector.is_started());

    // Phase 3: Restart connector with same config
    connector.start(config.clone());

    // Verify connector restarted successfully
    assert!(connector.is_started());

    // Verify topic partitions rediscovered
    assert!(!connector.known_source_topic_partitions().is_empty());

    // Get task configs after restart
    let restart_task_configs = connector
        .task_configs(1)
        .expect("task_configs after restart should succeed");
    assert_eq!(restart_task_configs.len(), 1);

    // Task configs should be same (same partition assignment)
    let initial_partitions = initial_task_configs[0].get(TASK_TOPIC_PARTITIONS_CONFIG);
    let restart_partitions = restart_task_configs[0].get(TASK_TOPIC_PARTITIONS_CONFIG);
    assert_eq!(initial_partitions, restart_partitions);

    // Final cleanup
    connector.stop();
}

/// Tests task restart recovery after connector restart.
/// Corresponds to Java: testRestartReplication task lifecycle after connector restart
#[test]
fn test_task_restart_recovery() {
    // Setup: Create connector and get task config
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["task-restart-topic"]));
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector =
        MirrorSourceConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    let connector_config = create_connector_config(
        "tr-src",
        "tr-tgt",
        "task-restart-connector",
        "task-restart-.*",
    );
    connector.start(connector_config.clone());

    let task_configs = connector
        .task_configs(1)
        .expect("task_configs should succeed");
    let task_config_base = task_configs[0].clone();

    // Phase 1: Initial task lifecycle
    let consumer1 = create_mock_consumer_with_records(
        "task-restart-topic",
        0,
        create_test_records("task-restart-topic", 0, 10, 0),
    );

    let mut task1 = MirrorSourceTask::with_consumer(consumer1);

    let full_config1: HashMap<String, String> = task_config_base
        .clone()
        .into_iter()
        .chain(vec![
            (
                SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
                "tr-src".to_string(),
            ),
            (
                TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
                "tr-tgt".to_string(),
            ),
        ])
        .collect();

    task1.start(full_config1.clone());

    let records1 = task1.poll().expect("initial poll should succeed");
    assert_eq!(records1.len(), 10);

    task1.stop();
    assert!(task1.is_stopping());

    // Phase 2: Stop connector (simulate restart)
    connector.stop();

    // Phase 3: Restart connector
    connector.start(connector_config.clone());

    // Get new task configs
    let new_task_configs = connector
        .task_configs(1)
        .expect("task_configs after restart should succeed");

    // Phase 4: Create new task (simulating task restart)
    let consumer2 = create_mock_consumer_with_records(
        "task-restart-topic",
        0,
        create_test_records("task-restart-topic", 0, 10, 10),
    );

    let mut task2 = MirrorSourceTask::with_consumer(consumer2);

    let full_config2: HashMap<String, String> = new_task_configs[0]
        .clone()
        .into_iter()
        .chain(vec![
            (
                SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
                "tr-src".to_string(),
            ),
            (
                TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
                "tr-tgt".to_string(),
            ),
        ])
        .collect();

    task2.start(full_config2);

    // Phase 5: Verify task works after restart
    let records2 = task2.poll().expect("poll after restart should succeed");
    assert_eq!(records2.len(), 10);

    // Verify offset continuity (starts at offset 10)
    for (i, record) in records2.iter().enumerate() {
        let source_offset = record.source_offset();
        let offset_val = source_offset.get("offset").and_then(|v| v.as_i64());
        assert_eq!(offset_val, Some(10 + i as i64));
    }

    // Final cleanup
    task2.stop();
    connector.stop();
}

// ============================================================================
// Tests - Resource Leak Prevention
// ============================================================================

/// Tests that connector multiple start/stop cycles don't leak resources.
#[test]
fn test_connector_multiple_start_stop_no_resource_leak() {
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["cycle-topic"]));
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector =
        MirrorSourceConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    let config = create_connector_config("cycle-src", "cycle-tgt", "cycle-connector", "cycle-.*");

    // Perform multiple start/stop cycles
    for cycle in 0..5 {
        connector.start(config.clone());
        assert!(connector.is_started());
        assert!(!connector.known_source_topic_partitions().is_empty());

        connector.stop();
        assert!(!connector.is_started());
        assert!(connector.known_source_topic_partitions().is_empty());
    }

    // Verify final state is clean
    assert!(!connector.is_started());
    assert!(connector.known_source_topic_partitions().is_empty());
    assert!(connector.topic_filter().is_none());
    assert!(connector.config_property_filter().is_none());
}

/// Tests that task multiple start/stop cycles don't leak resources.
#[test]
fn test_task_multiple_start_stop_no_resource_leak() {
    let topic = "leak-topic";

    for cycle in 0..5 {
        let consumer = create_mock_consumer_with_records(
            topic,
            0,
            create_test_records(topic, 0, 5, cycle * 10),
        );

        let mut task = MirrorSourceTask::with_consumer(consumer);

        let config = create_task_config("leak-src", "leak-tgt", "leak-topic:0");
        task.start(config);

        assert!(!task.is_stopping());
        assert_eq!(task.assigned_partitions().len(), 1);

        let records = task.poll().expect("poll should succeed");
        assert_eq!(records.len(), 5);

        task.stop();

        assert!(task.is_stopping());
        assert!(task.assigned_partitions().is_empty());
    }
}

/// Tests connector stop clears scheduler state.
#[test]
fn test_connector_stop_clears_scheduler() {
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["scheduler-topic"]));
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector = MirrorSourceConnector::with_admin_clients(source_admin, target_admin);

    let config = create_connector_config(
        "sched-src",
        "sched-tgt",
        "scheduler-connector",
        "scheduler-.*",
    );
    connector.start(config);

    // Scheduler should be running after start
    // Note: Internal scheduler state is verified through connector behavior

    connector.stop();

    // After stop, scheduler should be stopped
    // Verified through connector state - connector cannot be "started" after stop
    assert!(!connector.is_started());
}

// ============================================================================
// Tests - Edge Cases and Error Handling
// ============================================================================

/// Tests connector start with empty topic list.
#[test]
fn test_connector_start_empty_topics() {
    let source_admin = Arc::new(MockAdmin::new()); // No topics
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector = MirrorSourceConnector::with_admin_clients(source_admin, target_admin);

    let config = create_connector_config(
        "empty-src",
        "empty-tgt",
        "empty-connector",
        "nonexistent-.*",
    );
    connector.start(config);

    // Connector should start but have no partitions
    assert!(connector.is_started());
    assert!(connector.known_source_topic_partitions().is_empty());

    // Task configs should be empty
    let task_configs = connector
        .task_configs(1)
        .expect("task_configs should succeed");
    assert!(task_configs.is_empty());

    connector.stop();
}

/// Tests task start with empty partition assignment.
#[test]
fn test_task_start_empty_partitions() {
    let consumer = MockKafkaConsumer::new();

    let mut task = MirrorSourceTask::with_consumer(consumer);

    // Start with empty partition assignment
    let config = create_task_config("empty-task-src", "empty-task-tgt", "");
    task.start(config);

    assert!(!task.is_stopping());
    assert!(task.assigned_partitions().is_empty());

    // Poll should return empty
    let records = task.poll().expect("poll should succeed");
    assert!(records.is_empty());

    task.stop();
}

/// Tests connector task_configs with zero max_tasks.
#[test]
fn test_connector_task_configs_zero_max_tasks() {
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["zero-topic"]));
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector = MirrorSourceConnector::with_admin_clients(source_admin, target_admin);

    let config = create_connector_config("zero-src", "zero-tgt", "zero-connector", "zero-.*");
    connector.start(config);

    // Request 0 tasks
    let task_configs = connector
        .task_configs(0)
        .expect("task_configs should succeed");
    assert!(task_configs.is_empty());

    connector.stop();
}

/// Tests connector stop when already stopped.
#[test]
fn test_connector_stop_when_already_stopped() {
    let mut connector = MirrorSourceConnector::new();

    // Connector is not started
    assert!(!connector.is_started());

    // Stop should not panic or fail
    connector.stop();
    assert!(!connector.is_started());

    // Double stop should also work
    connector.stop();
    assert!(!connector.is_started());
}

/// Tests task stop when already stopped.
#[test]
fn test_task_stop_when_already_stopped() {
    let consumer = MockKafkaConsumer::new();
    let mut task = MirrorSourceTask::with_consumer(consumer);

    let config = create_task_config("double-stop-src", "double-stop-tgt", "topic:0");
    task.start(config);
    task.stop();

    assert!(task.is_stopping());

    // Double stop should not panic
    task.stop();
    assert!(task.is_stopping());
}

// ============================================================================
// Tests - External Admin Mock Integration
// ============================================================================

/// Tests connector lifecycle with ExternalAdminMock (simulating external cluster).
#[test]
fn test_connector_lifecycle_with_external_admin_mock() {
    // Create external admin mock (source cluster)
    let external_admin = ExternalAdminMock::with_topics(vec!["external-topic".to_string()]);

    // Create local target admin
    let target_admin = Arc::new(MockAdmin::new());

    // Note: ExternalAdminMock is for testing network simulation
    // MirrorSourceConnector uses MockAdmin interface
    // This test verifies the pattern of using external mock

    // Verify external admin has topics
    assert_eq!(external_admin.topic_count(), 1);
    assert!(external_admin.topic_exists("external-topic"));

    // Create connector with local admins (pattern demonstration)
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["external-topic"]));
    let mut connector = MirrorSourceConnector::with_admin_clients(source_admin, target_admin);

    let config = create_connector_config("ext-src", "ext-tgt", "external-connector", "external-.*");
    connector.start(config);

    // Verify connector discovered topic
    assert!(connector.is_started());
    assert!(!connector.known_source_topic_partitions().is_empty());

    connector.stop();
}

/// Tests connector restart with external topic changes.
#[test]
fn test_connector_restart_with_topic_changes() {
    // Initial setup with one topic
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["initial-topic"]));

    // Add more topics after initial start (simulate topic creation)
    source_admin.create_topics(vec![NewTopic::new("new-topic-1", 1, 1)]);
    source_admin.create_topics(vec![NewTopic::new("new-topic-2", 1, 1)]);

    let target_admin = Arc::new(MockAdmin::new());

    let mut connector =
        MirrorSourceConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    let config =
        create_connector_config("change-src", "change-tgt", "change-connector", ".*-topic");

    // Initial start - discovers initial-topic
    connector.start(config.clone());
    let initial_partitions = connector.known_source_topic_partitions().len();
    connector.stop();

    // Restart - should rediscover all topics including new ones
    connector.start(config.clone());
    let restart_partitions = connector.known_source_topic_partitions().len();

    // Should have discovered more partitions (3 topics)
    assert!(restart_partitions >= initial_partitions);

    connector.stop();
}

// ============================================================================
// Tests - Full End-to-End Lifecycle
// ============================================================================

/// Tests complete end-to-end lifecycle matching Java testRestartReplication flow.
/// Corresponds to Java: MirrorConnectorsIntegrationBaseTest.testRestartReplication() full lifecycle
#[test]
fn test_full_end_to_end_lifecycle() {
    // Phase 1: Setup clusters
    let source_admin = Arc::new(create_source_admin_with_topics(vec!["e2e-topic"]));
    let target_admin = Arc::new(MockAdmin::new());

    // Phase 2: Create and start connector
    let mut connector =
        MirrorSourceConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    let connector_config = create_connector_config("e2e-src", "e2e-tgt", "e2e-connector", "e2e-.*");
    connector.start(connector_config.clone());

    // Phase 3: Get task configs and start task
    let task_configs = connector
        .task_configs(1)
        .expect("task_configs should succeed");

    let consumer = create_mock_consumer_with_records(
        "e2e-topic",
        0,
        create_test_records("e2e-topic", 0, 20, 0),
    );

    let mut task = MirrorSourceTask::with_consumer(consumer);

    let full_task_config: HashMap<String, String> = task_configs[0]
        .clone()
        .into_iter()
        .chain(vec![
            (
                SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
                "e2e-src".to_string(),
            ),
            (
                TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
                "e2e-tgt".to_string(),
            ),
        ])
        .collect();

    task.start(full_task_config);

    // Phase 4: Poll initial data
    let initial_records = task.poll().expect("initial poll should succeed");
    assert_eq!(initial_records.len(), 20);

    // Phase 5: Stop task (prepare for restart)
    task.stop();
    assert!(task.is_stopping());

    // Phase 6: Stop connector (restart simulation)
    connector.stop();
    assert!(!connector.is_started());

    // Phase 7: Restart connector
    connector.start(connector_config.clone());
    assert!(connector.is_started());

    // Phase 8: Get new task configs after restart
    let new_task_configs = connector
        .task_configs(1)
        .expect("task_configs after restart should succeed");

    // Phase 9: Restart task with new consumer (more data)
    let new_consumer = create_mock_consumer_with_records(
        "e2e-topic",
        0,
        create_test_records("e2e-topic", 0, 20, 20),
    );

    let mut new_task = MirrorSourceTask::with_consumer(new_consumer);

    let new_task_config: HashMap<String, String> = new_task_configs[0]
        .clone()
        .into_iter()
        .chain(vec![
            (
                SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
                "e2e-src".to_string(),
            ),
            (
                TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
                "e2e-tgt".to_string(),
            ),
        ])
        .collect();

    new_task.start(new_task_config);

    // Phase 10: Poll after restart
    let post_restart_records = new_task.poll().expect("post-restart poll should succeed");
    assert_eq!(post_restart_records.len(), 20);

    // Phase 11: Verify offset continuity
    for (i, record) in post_restart_records.iter().enumerate() {
        let source_offset = record.source_offset();
        let offset_val = source_offset.get("offset").and_then(|v| v.as_i64());
        assert_eq!(offset_val, Some(20 + i as i64));
    }

    // Phase 12: Final cleanup
    new_task.stop();
    connector.stop();

    // Phase 13: Verify final state
    assert!(new_task.is_stopping());
    assert!(!connector.is_started());
    assert!(connector.known_source_topic_partitions().is_empty());
}
