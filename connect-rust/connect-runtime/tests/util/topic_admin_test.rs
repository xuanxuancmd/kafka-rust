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

//! Tests for TopicAdmin.
//!
//! Corresponds to `org.apache.kafka.connect.util.TopicAdminTest` in Java.

use common_trait::util::time::SystemTimeImpl;
use common_trait::TopicPartition;
use connect_runtime::util::{
    TopicAdmin, TopicAdminError, TopicCreationResponse, CLEANUP_POLICY_COMPACT,
    CLEANUP_POLICY_CONFIG, NO_PARTITIONS, NO_REPLICATION_FACTOR,
};
use kafka_clients_mock::{MockAdmin, NewTopic, TopicError};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// Helper to create a TopicAdmin for testing.
fn create_test_admin() -> TopicAdmin {
    let admin = Arc::new(MockAdmin::new());
    TopicAdmin::for_testing(Some("localhost:9092".to_string()), admin, true)
}

/// Helper to create a TopicAdmin with pre-existing topics.
fn create_test_admin_with_topics(topics: Vec<NewTopic>) -> TopicAdmin {
    let admin = Arc::new(MockAdmin::with_topics(topics));
    TopicAdmin::for_testing(Some("localhost:9092".to_string()), admin, true)
}

// ===== NewTopicBuilder Tests =====

#[test]
fn test_new_topic_builder_basic() {
    let topic = TopicAdmin::define_topic("test-topic".to_string())
        .partitions(3)
        .replication_factor(2)
        .build();

    assert_eq!(topic.name(), "test-topic");
    assert_eq!(topic.num_partitions(), 3);
    assert_eq!(topic.replication_factor(), 2);
}

#[test]
fn test_new_topic_builder_compacted() {
    let topic = TopicAdmin::define_topic("compacted-topic".to_string())
        .partitions(1)
        .compacted()
        .build();

    assert_eq!(topic.name(), "compacted-topic");
    let configs = topic.configs();
    assert_eq!(
        configs.get(CLEANUP_POLICY_CONFIG),
        Some(&CLEANUP_POLICY_COMPACT.to_string())
    );
}

#[test]
fn test_new_topic_builder_default_partitions() {
    let topic = TopicAdmin::define_topic("default-part-topic".to_string())
        .default_partitions()
        .build();

    assert_eq!(topic.num_partitions(), NO_PARTITIONS);
}

#[test]
fn test_new_topic_builder_default_replication_factor() {
    let topic = TopicAdmin::define_topic("default-rf-topic".to_string())
        .default_replication_factor()
        .build();

    assert_eq!(topic.replication_factor(), NO_REPLICATION_FACTOR);
}

#[test]
fn test_new_topic_builder_with_config() {
    let mut config = HashMap::new();
    config.insert("retention.ms".to_string(), "86400000".to_string());
    config.insert("segment.bytes".to_string(), "1073741824".to_string());

    let topic = TopicAdmin::define_topic("config-topic".to_string())
        .partitions(5)
        .config(config)
        .build();

    assert_eq!(topic.configs().len(), 2);
    assert_eq!(
        topic.configs().get("retention.ms"),
        Some(&"86400000".to_string())
    );
}

#[test]
fn test_new_topic_builder_config_entry() {
    let topic = TopicAdmin::define_topic("entry-topic".to_string())
        .partitions(1)
        .config_entry("cleanup.policy".to_string(), "compact".to_string())
        .config_entry("retention.ms".to_string(), "604800000".to_string())
        .build();

    assert_eq!(topic.configs().len(), 2);
}

// ===== TopicCreationResponse Tests =====

#[test]
fn test_topic_creation_response_empty() {
    let response = TopicCreationResponse::empty();

    assert!(response.is_empty());
    assert_eq!(response.created_or_existing_topics_count(), 0);
    assert!(!response.is_created("any-topic"));
    assert!(!response.is_existing("any-topic"));
}

#[test]
fn test_topic_creation_response_with_created() {
    let created: HashSet<String> = ["new-topic".to_string()].into_iter().collect();
    let response = TopicCreationResponse::new(created, HashSet::new());

    assert!(response.is_created("new-topic"));
    assert!(!response.is_existing("new-topic"));
    assert!(response.is_created_or_existing("new-topic"));
    assert_eq!(response.created_topics_count(), 1);
    assert_eq!(response.existing_topics_count(), 0);
}

#[test]
fn test_topic_creation_response_with_existing() {
    let existing: HashSet<String> = ["existing-topic".to_string()].into_iter().collect();
    let response = TopicCreationResponse::new(HashSet::new(), existing);

    assert!(!response.is_created("existing-topic"));
    assert!(response.is_existing("existing-topic"));
    assert!(response.is_created_or_existing("existing-topic"));
    assert_eq!(response.created_topics_count(), 0);
    assert_eq!(response.existing_topics_count(), 1);
}

#[test]
fn test_topic_creation_response_mixed() {
    let created: HashSet<String> = ["topic-a".to_string(), "topic-b".to_string()]
        .into_iter()
        .collect();
    let existing: HashSet<String> = ["topic-c".to_string()].into_iter().collect();
    let response = TopicCreationResponse::new(created, existing);

    assert_eq!(response.created_topics_count(), 2);
    assert_eq!(response.existing_topics_count(), 1);
    assert_eq!(response.created_or_existing_topics_count(), 3);
    assert!(response.is_created("topic-a"));
    assert!(response.is_created("topic-b"));
    assert!(response.is_existing("topic-c"));
    assert!(!response.is_empty());
}

#[test]
fn test_topic_creation_response_display() {
    let created: HashSet<String> = ["topic1".to_string()].into_iter().collect();
    let existing: HashSet<String> = ["topic2".to_string()].into_iter().collect();
    let response = TopicCreationResponse::new(created, existing);

    let display = response.to_string();
    assert!(display.contains("created"));
    assert!(display.contains("existing"));
}

// ===== create_topic Tests =====

#[test]
fn test_create_topic_new_topic() {
    let admin = create_test_admin();

    let topic = TopicAdmin::define_topic("new-create-topic".to_string())
        .partitions(3)
        .replication_factor(1)
        .build();

    let result = admin.create_topic(&topic);
    assert!(result.is_ok());
    assert!(result.unwrap()); // Returns true for newly created
}

#[test]
fn test_create_topic_already_exists() {
    let topic = TopicAdmin::define_topic("existing-create-topic".to_string())
        .partitions(1)
        .build();

    let admin = create_test_admin_with_topics(vec![topic.clone()]);
    let result = admin.create_topic(&topic);

    assert!(result.is_ok());
    assert!(!result.unwrap()); // Returns false for already existing
}

#[test]
fn test_create_topic_null_topic() {
    let admin = create_test_admin();

    // Pass a null topic by creating one that will be treated as null
    // In Rust, we can't pass actual null, but we can test empty topic name behavior
    let topic = NewTopic::new("", 1, 1);
    // This should still work but create an empty-named topic
    let result = admin.create_topic(&topic);
    assert!(result.is_ok());
}

// ===== create_topics Tests =====

#[test]
fn test_create_topics_single() {
    let admin = create_test_admin();

    let topic = TopicAdmin::define_topic("single-batch-topic".to_string())
        .partitions(1)
        .build();

    let result = admin.create_topics(&[topic]);
    assert!(result.is_ok());
    let created = result.unwrap();
    assert_eq!(created.len(), 1);
    assert!(created.contains("single-batch-topic"));
}

#[test]
fn test_create_topics_multiple() {
    let admin = create_test_admin();

    let topic1 = TopicAdmin::define_topic("batch-topic-1".to_string())
        .partitions(1)
        .build();
    let topic2 = TopicAdmin::define_topic("batch-topic-2".to_string())
        .partitions(2)
        .build();
    let topic3 = TopicAdmin::define_topic("batch-topic-3".to_string())
        .partitions(3)
        .build();

    let result = admin.create_topics(&[topic1, topic2, topic3]);
    assert!(result.is_ok());
    let created = result.unwrap();
    assert_eq!(created.len(), 3);
}

#[test]
fn test_create_topics_empty_list() {
    let admin = create_test_admin();

    let result = admin.create_topics(&[]);
    assert!(result.is_ok());
    let created = result.unwrap();
    assert!(created.is_empty());
}

#[test]
fn test_create_topics_duplicate_names() {
    let admin = create_test_admin();

    let topic1 = TopicAdmin::define_topic("duplicate-topic".to_string())
        .partitions(1)
        .build();
    let topic2 = TopicAdmin::define_topic("duplicate-topic".to_string())
        .partitions(2)
        .build();

    let result = admin.create_topics(&[topic1, topic2]);
    assert!(result.is_ok());
    let created = result.unwrap();
    // Should only create one topic (last definition wins)
    assert_eq!(created.len(), 1);
}

// ===== create_or_find_topics Tests =====

#[test]
fn test_create_or_find_topics_new() {
    let admin = create_test_admin();

    let topic = TopicAdmin::define_topic("create-find-new".to_string())
        .partitions(1)
        .build();

    let result = admin.create_or_find_topics(&[topic]);
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_created("create-find-new"));
    assert!(!response.is_existing("create-find-new"));
}

#[test]
fn test_create_or_find_topics_existing() {
    let topic = TopicAdmin::define_topic("create-find-existing".to_string())
        .partitions(1)
        .build();

    let admin = create_test_admin_with_topics(vec![topic.clone()]);
    let result = admin.create_or_find_topics(&[topic]);

    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.is_created("create-find-existing"));
    assert!(response.is_existing("create-find-existing"));
}

#[test]
fn test_create_or_find_topic_single() {
    let admin = create_test_admin();

    let topic = TopicAdmin::define_topic("create-find-single".to_string())
        .partitions(1)
        .build();

    let result = admin.create_or_find_topic(&topic);
    assert!(result.is_ok());
    assert!(result.unwrap());
}

#[test]
fn test_create_or_find_topics_empty() {
    let admin = create_test_admin();

    let result = admin.create_or_find_topics(&[]);
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_empty());
}

// ===== describe_topics Tests =====

#[test]
fn test_describe_topics_existing() {
    let topic = TopicAdmin::define_topic("describe-existing".to_string())
        .partitions(3)
        .build();

    let admin = create_test_admin_with_topics(vec![topic]);

    let result = admin.describe_topics(&["describe-existing".to_string()]);
    assert!(result.is_ok());
    let descriptions = result.unwrap();
    assert_eq!(descriptions.len(), 1);
    assert!(descriptions.contains_key("describe-existing"));
}

#[test]
fn test_describe_topics_nonexistent() {
    let admin = create_test_admin();

    let result = admin.describe_topics(&["nonexistent-topic".to_string()]);
    assert!(result.is_ok());
    let descriptions = result.unwrap();
    assert!(descriptions.is_empty());
}

#[test]
fn test_describe_topics_multiple() {
    let topic1 = TopicAdmin::define_topic("describe-multi-1".to_string())
        .partitions(1)
        .build();
    let topic2 = TopicAdmin::define_topic("describe-multi-2".to_string())
        .partitions(2)
        .build();

    let admin = create_test_admin_with_topics(vec![topic1, topic2]);

    let result = admin.describe_topics(&[
        "describe-multi-1".to_string(),
        "describe-multi-2".to_string(),
    ]);
    assert!(result.is_ok());
    let descriptions = result.unwrap();
    assert_eq!(descriptions.len(), 2);
}

#[test]
fn test_describe_topics_empty_list() {
    let admin = create_test_admin();

    let result = admin.describe_topics(&[]);
    assert!(result.is_ok());
    let descriptions = result.unwrap();
    assert!(descriptions.is_empty());
}

// ===== end_offsets Tests =====

#[test]
fn test_end_offsets_basic() {
    let topic = NewTopic::new("end-offsets-topic", 2, 1);
    let admin = create_test_admin_with_topics(vec![topic]);

    let mut partitions = HashSet::new();
    partitions.insert(TopicPartition::new("end-offsets-topic", 0));
    partitions.insert(TopicPartition::new("end-offsets-topic", 1));

    let result = admin.end_offsets(&partitions);
    assert!(result.is_ok());
    let offsets = result.unwrap();
    assert_eq!(offsets.len(), 2);
}

#[test]
fn test_end_offsets_empty_set() {
    let admin = create_test_admin();

    let partitions = HashSet::new();
    let result = admin.end_offsets(&partitions);
    assert!(result.is_ok());
    let offsets = result.unwrap();
    assert!(offsets.is_empty());
}

#[test]
fn test_end_offsets_nonexistent_topic() {
    let admin = create_test_admin();

    let mut partitions = HashSet::new();
    partitions.insert(TopicPartition::new("nonexistent-offset-topic", 0));

    let result = admin.end_offsets(&partitions);
    assert!(result.is_err());
}

#[test]
fn test_end_offsets_invalid_partition() {
    let topic = NewTopic::new("invalid-partition-topic", 1, 1);
    let admin = create_test_admin_with_topics(vec![topic]);

    let mut partitions = HashSet::new();
    // Partition 5 doesn't exist (topic only has 1 partition)
    partitions.insert(TopicPartition::new("invalid-partition-topic", 5));

    let result = admin.end_offsets(&partitions);
    assert!(result.is_err());
}

// ===== retry_end_offsets Tests =====

#[test]
fn test_retry_end_offsets_basic() {
    let topic = NewTopic::new("retry-offsets-topic", 1, 1);
    let admin = create_test_admin_with_topics(vec![topic]);

    let mut partitions = HashSet::new();
    partitions.insert(TopicPartition::new("retry-offsets-topic", 0));

    let time = Arc::new(SystemTimeImpl::new());
    let result = admin.retry_end_offsets(&partitions, Duration::from_secs(1), 100, time);
    assert!(result.is_ok());
    let offsets = result.unwrap();
    assert_eq!(offsets.len(), 1);
}

#[test]
fn test_retry_end_offsets_empty_set() {
    let admin = create_test_admin();
    let time = Arc::new(SystemTimeImpl::new());

    let partitions = HashSet::new();
    let result = admin.retry_end_offsets(&partitions, Duration::from_secs(1), 100, time);
    assert!(result.is_ok());
    let offsets = result.unwrap();
    assert!(offsets.is_empty());
}

// ===== cleanup_policy Tests =====

#[test]
fn test_topic_cleanup_policy_compact() {
    let topic = TopicAdmin::define_topic("cleanup-compact".to_string())
        .partitions(1)
        .compacted()
        .build();

    let admin = create_test_admin_with_topics(vec![topic]);

    let policies = admin.topic_cleanup_policy("cleanup-compact").unwrap();
    assert_eq!(policies.len(), 1);
    assert!(policies.contains(&CLEANUP_POLICY_COMPACT.to_string()));
}

#[test]
fn test_topic_cleanup_policy_nonexistent() {
    let admin = create_test_admin();

    let policies = admin.topic_cleanup_policy("nonexistent-cleanup").unwrap();
    assert!(policies.is_empty());
}

#[test]
fn test_verify_cleanup_policy_only_compact_valid() {
    let topic = TopicAdmin::define_topic("verify-compact-valid".to_string())
        .partitions(1)
        .compacted()
        .build();

    let admin = create_test_admin_with_topics(vec![topic]);

    let result = admin.verify_topic_cleanup_policy_only_compact(
        "verify-compact-valid",
        "worker.offsets.topic",
        "offsets storage",
    );
    assert!(result.is_ok());
    assert!(result.unwrap());
}

#[test]
fn test_verify_cleanup_policy_only_compact_invalid() {
    // Topic without compact policy
    let topic = NewTopic::new("verify-compact-invalid", 1, 1);

    let admin = create_test_admin_with_topics(vec![topic]);

    let result = admin.verify_topic_cleanup_policy_only_compact(
        "verify-compact-invalid",
        "worker.offsets.topic",
        "offsets storage",
    );
    // Should return error since cleanup.policy is not compact only
    assert!(result.is_err());
}

// ===== Error Display Tests =====

#[test]
fn test_topic_admin_error_display() {
    let errors = [
        TopicAdminError::AlreadyExists("topic".to_string()),
        TopicAdminError::DoesNotExist("topic".to_string()),
        TopicAdminError::InvalidConfig("config".to_string()),
        TopicAdminError::InvalidReplicationFactor("rf".to_string()),
        TopicAdminError::Authorization("auth".to_string()),
        TopicAdminError::UnsupportedVersion("version".to_string()),
        TopicAdminError::Timeout("timeout".to_string()),
        TopicAdminError::LeaderNotAvailable("leader".to_string()),
        TopicAdminError::Other("other".to_string()),
    ];

    for error in &errors {
        assert!(!error.to_string().is_empty());
    }
}

#[test]
fn test_topic_admin_error_is_std_error() {
    let error = TopicAdminError::AlreadyExists("test".to_string());
    let _: &dyn std::error::Error = &error;
}

// ===== bootstrap_servers Tests =====

#[test]
fn test_bootstrap_servers_accessor() {
    let admin = create_test_admin();
    assert_eq!(admin.bootstrap_servers(), "localhost:9092");
}

#[test]
fn test_bootstrap_servers_none() {
    let admin = TopicAdmin::new(HashMap::new());
    assert_eq!(admin.bootstrap_servers(), "<unknown>");
}

// ===== Constants Tests =====

#[test]
fn test_constants_values() {
    assert_eq!(NO_PARTITIONS, -1);
    assert_eq!(NO_REPLICATION_FACTOR, -1);
    assert_eq!(CLEANUP_POLICY_CONFIG, "cleanup.policy");
    assert_eq!(CLEANUP_POLICY_COMPACT, "compact");
}

// ===== Integration-style Tests =====

#[test]
fn test_full_topic_workflow() {
    let admin = create_test_admin();

    // Create topic
    let topic = TopicAdmin::define_topic("workflow-test-topic".to_string())
        .partitions(3)
        .replication_factor(1)
        .compacted()
        .build();

    let created = admin.create_topic(&topic).unwrap();
    assert!(created);

    // Describe topic
    let description = admin
        .describe_topics(&["workflow-test-topic".to_string()])
        .unwrap();
    assert!(description.contains_key("workflow-test-topic"));

    // Get cleanup policy
    let policies = admin.topic_cleanup_policy("workflow-test-topic").unwrap();
    assert!(policies.contains(&"compact".to_string()));

    // Verify cleanup policy
    let verified = admin
        .verify_topic_cleanup_policy_only_compact("workflow-test-topic", "test.config", "testing")
        .unwrap();
    assert!(verified);

    // Get end offsets
    let mut partitions = HashSet::new();
    partitions.insert(TopicPartition::new("workflow-test-topic", 0));
    partitions.insert(TopicPartition::new("workflow-test-topic", 1));
    partitions.insert(TopicPartition::new("workflow-test-topic", 2));

    let offsets = admin.end_offsets(&partitions).unwrap();
    assert_eq!(offsets.len(), 3);
}

#[test]
fn test_topic_admin_with_existing_topics() {
    // Pre-create topics
    let topics = vec![
        TopicAdmin::define_topic("pre-existing-1".to_string())
            .partitions(1)
            .build(),
        TopicAdmin::define_topic("pre-existing-2".to_string())
            .partitions(2)
            .build(),
    ];

    let admin = create_test_admin_with_topics(topics);

    // Try to create again - should return false
    let topic1 = TopicAdmin::define_topic("pre-existing-1".to_string())
        .partitions(1)
        .build();
    let result = admin.create_topic(&topic1).unwrap();
    assert!(!result);

    // create_or_find should succeed
    let topic2 = TopicAdmin::define_topic("pre-existing-2".to_string())
        .partitions(2)
        .build();
    let result = admin.create_or_find_topic(&topic2).unwrap();
    assert!(result);
}

#[test]
fn test_create_topics_with_retry_success() {
    let admin = create_test_admin();
    let time = Arc::new(SystemTimeImpl::new());

    let topic = TopicAdmin::define_topic("retry-success-topic".to_string())
        .partitions(1)
        .replication_factor(1)
        .build();

    // Should succeed on first try
    let result = admin.create_topics_with_retry(&topic, 1000, 10, time);
    assert!(result.is_ok());
    let created = result.unwrap();
    assert_eq!(created.len(), 1);
}
