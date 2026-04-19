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

//! Tests for MirrorClient module
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorClient
//! and org.apache.kafka.connect.mirror.MirrorClientTest

use common_trait::KafkaException;
use common_trait::TopicPartition;
use connect_mirror_client::{
    AdminClient, Consumer, ConsumerRecord, DefaultReplicationPolicy, IdentityReplicationPolicy,
    MirrorClient, MockAdminClient, MockConsumer, OffsetAndMetadata, ReplicationPolicy,
    SOURCE_CLUSTER_ALIAS_CONFIG,
};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

// Helper function for IdentityReplicationPolicy tests
fn identity_replication_policy(source: &str) -> Box<dyn ReplicationPolicy> {
    let mut policy = IdentityReplicationPolicy::new();
    let mut config = HashMap::new();
    config.insert(SOURCE_CLUSTER_ALIAS_CONFIG.to_string(), source.to_string());
    policy.configure(&config);
    Box::new(policy)
}

#[test]
fn test_new_mirror_client() {
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    let client = MirrorClient::new(props);
    // Test that replication_policy returns a valid reference
    let policy = client.replication_policy();
    let remote_topic = policy.format_remote_topic("source", "topic");
    assert_eq!(remote_topic, "source.topic");
}

#[test]
fn test_new_for_test() {
    let admin = Box::new(MockAdminClient::empty());
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);
    // Test that replication_policy returns a valid reference
    let policy = client.replication_policy();
    let remote_topic = policy.format_remote_topic("source", "topic");
    assert_eq!(remote_topic, "source.topic");
}

#[test]
fn test_is_remote_topic() {
    let admin = Box::new(MockAdminClient::empty());
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    // Remote topic with source prefix
    assert!(client.is_remote_topic("source.topic"));

    // Internal topic
    assert!(!client.is_remote_topic("heartbeats"));
    assert!(!client.is_remote_topic("mm2-offset-syncs.cluster.internal"));
    assert!(!client.is_remote_topic("__consumer_offsets"));

    // Local topic without source prefix
    assert!(!client.is_remote_topic("local-topic"));
}

#[test]
fn test_count_hops_for_topic() {
    let admin = Box::new(MockAdminClient::empty());
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    // Single hop: source.topic -> source is "source"
    let hops = client.count_hops_for_topic("source.topic", "source");
    assert_eq!(hops, 1);

    // Multiple hops: cluster2.cluster1.topic
    // cluster2.cluster1.topic -> cluster1.topic (hop 1) -> topic (hop 2) -> source cluster1
    let hops = client.count_hops_for_topic("cluster2.cluster1.topic", "cluster1");
    assert_eq!(hops, 2);

    // Unreachable cluster
    let hops = client.count_hops_for_topic("source.topic", "unknown");
    assert_eq!(hops, -1);

    // Local topic without source
    let hops = client.count_hops_for_topic("local-topic", "source");
    assert_eq!(hops, -1);
}

#[test]
fn test_all_sources() {
    let admin = Box::new(MockAdminClient::empty());
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    // Single source
    let sources = client.all_sources("source.topic");
    assert!(sources.contains("source"));

    // Multiple hops
    let sources = client.all_sources("cluster2.cluster1.topic");
    assert!(sources.contains("cluster1"));
    assert!(sources.contains("cluster2"));

    // Local topic without source
    let sources = client.all_sources("local-topic");
    assert!(sources.is_empty());
}

#[test]
fn test_heartbeat_topics() {
    let mut topics = HashSet::new();
    topics.insert("heartbeats".to_string());
    topics.insert("source.heartbeats".to_string());
    topics.insert("normal-topic".to_string());

    let admin = Box::new(MockAdminClient::new(topics));
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    let heartbeat_topics = client.heartbeat_topics().unwrap();
    // DefaultReplicationPolicy checks original_topic == "heartbeats"
    assert!(heartbeat_topics.contains("heartbeats"));
    // source.heartbeats -> original_topic is "heartbeats"
    assert!(heartbeat_topics.contains("source.heartbeats"));
    assert!(!heartbeat_topics.contains("normal-topic"));
}

#[test]
fn test_checkpoint_topics() {
    let mut topics = HashSet::new();
    topics.insert("cluster.checkpoints.internal".to_string());
    topics.insert("normal-topic".to_string());

    let admin = Box::new(MockAdminClient::new(topics));
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    let checkpoint_topics = client.checkpoint_topics().unwrap();
    assert!(checkpoint_topics.contains("cluster.checkpoints.internal"));
    assert!(!checkpoint_topics.contains("normal-topic"));
}

#[test]
fn test_remote_topics() {
    let mut topics = HashSet::new();
    topics.insert("source.remote-topic".to_string());
    topics.insert("heartbeats".to_string());
    topics.insert("__consumer_offsets".to_string());
    topics.insert("local-topic".to_string());

    let admin = Box::new(MockAdminClient::new(topics));
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    let remote_topics = client.remote_topics().unwrap();
    assert!(remote_topics.contains("source.remote-topic"));
    assert!(!remote_topics.contains("heartbeats"));
    assert!(!remote_topics.contains("__consumer_offsets"));
    assert!(!remote_topics.contains("local-topic"));
}

#[test]
fn test_remote_topics_for_source() {
    let mut topics = HashSet::new();
    topics.insert("source1.topic1".to_string());
    topics.insert("source2.topic2".to_string());
    topics.insert("local-topic".to_string());

    let admin = Box::new(MockAdminClient::new(topics));
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    let remote_topics = client.remote_topics_for_source("source1").unwrap();
    assert!(remote_topics.contains("source1.topic1"));
    assert!(!remote_topics.contains("source2.topic2"));
    assert!(!remote_topics.contains("local-topic"));
}

#[test]
fn test_replication_hops() {
    let mut topics = HashSet::new();
    topics.insert("source1.heartbeats".to_string());
    topics.insert("source2.source1.heartbeats".to_string());
    topics.insert("source3.source2.source1.heartbeats".to_string());

    let admin = Box::new(MockAdminClient::new(topics));
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    // Single hop from source1
    let hops = client.replication_hops("source1").unwrap();
    assert_eq!(hops, 1);

    // Two hops from source1 via source2
    let hops = client.replication_hops("source2").unwrap();
    assert_eq!(hops, 1);

    // Three hops from source1 via source2, source3
    // But source3.source2.source1.heartbeats -> source2.source1.heartbeats -> source1.heartbeats
    // source is source3, so 1 hop to source3
    let hops = client.replication_hops("source3").unwrap();
    assert_eq!(hops, 1);

    // Unreachable cluster
    let hops = client.replication_hops("unknown").unwrap();
    assert_eq!(hops, -1);
}

#[test]
fn test_upstream_clusters() {
    let mut topics = HashSet::new();
    topics.insert("source1.heartbeats".to_string());
    topics.insert("source2.source1.heartbeats".to_string());
    topics.insert("local-topic".to_string());

    let admin = Box::new(MockAdminClient::new(topics));
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    let clusters = client.upstream_clusters().unwrap();
    assert!(clusters.contains("source1"));
    assert!(clusters.contains("source2"));
    assert!(!clusters.contains("local-topic"));
}

#[test]
fn test_offset_and_metadata() {
    let offset = OffsetAndMetadata::new(100, "metadata");
    assert_eq!(offset.offset(), 100);
    assert_eq!(offset.metadata(), "metadata");
}

#[test]
fn test_consumer_record() {
    let record = ConsumerRecord::new("topic", 0, 100, vec![1, 2, 3], vec![4, 5, 6]);
    assert_eq!(record.topic(), "topic");
    assert_eq!(record.partition(), 0);
    assert_eq!(record.offset(), 100);
    assert_eq!(record.key(), &[1, 2, 3]);
    assert_eq!(record.value(), &[4, 5, 6]);
}

#[test]
fn test_mock_admin_client() {
    let mut admin = MockAdminClient::empty();
    admin.add_topic("topic1");
    admin.add_topic("topic2");

    let topics = admin.list_topics().unwrap();
    assert!(topics.contains("topic1"));
    assert!(topics.contains("topic2"));

    admin.remove_topic("topic1");
    let topics = admin.list_topics().unwrap();
    assert!(!topics.contains("topic1"));
    assert!(topics.contains("topic2"));
}

#[test]
fn test_mock_consumer() {
    let mut consumer = MockConsumer::new();
    let tp = TopicPartition::new("topic", 0);

    consumer.assign(vec![tp.clone()]);
    consumer.seek_to_beginning(vec![tp.clone()]);

    consumer.add_record(ConsumerRecord::new("topic", 0, 0, vec![], vec![]));
    consumer.add_record(ConsumerRecord::new("topic", 0, 1, vec![], vec![]));
    consumer.set_end_offset(tp.clone(), 2);

    let records = consumer.poll(Duration::from_millis(100));
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset(), 0);

    let position = consumer.position(tp.clone());
    assert_eq!(position, 1);

    let end_offsets = consumer.end_offsets(vec![tp.clone()]);
    assert_eq!(end_offsets.get(&tp), Some(&2));
}

#[test]
fn test_end_of_stream() {
    let mut consumer = MockConsumer::new();
    let tp = TopicPartition::new("topic", 0);

    consumer.add_record(ConsumerRecord::new("topic", 0, 0, vec![], vec![]));
    consumer.set_end_offset(tp.clone(), 1);
    consumer.assign(vec![tp.clone()]);

    // Not at end yet (position 0 < end offset 1)
    consumer.seek_to_beginning(vec![tp.clone()]);
    assert!(!MirrorClient::end_of_stream(&consumer, vec![tp.clone()]));

    // After consuming, position is 1, which equals end offset
    consumer.poll(Duration::from_millis(100));
    assert!(MirrorClient::end_of_stream(&consumer, vec![tp.clone()]));
}

#[test]
fn test_is_heartbeat_topic() {
    let admin = Box::new(MockAdminClient::empty());
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    // Heartbeats topics
    assert!(client.is_heartbeat_topic("heartbeats"));
    assert!(client.is_heartbeat_topic("source1.heartbeats"));
    assert!(client.is_heartbeat_topic("source2.source1.heartbeats"));

    // Not heartbeats topics
    assert!(!client.is_heartbeat_topic("heartbeats!"));
    assert!(!client.is_heartbeat_topic("!heartbeats"));
    assert!(!client.is_heartbeat_topic("source1heartbeats"));
    assert!(!client.is_heartbeat_topic("source1-heartbeats"));
}

#[test]
fn test_is_checkpoint_topic() {
    let admin = Box::new(MockAdminClient::empty());
    let policy = Box::new(DefaultReplicationPolicy::new());
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    // Checkpoint topics
    assert!(client.is_checkpoint_topic("source1.checkpoints.internal"));

    // Not checkpoint topics
    assert!(!client.is_checkpoint_topic("checkpoints.internal"));
    assert!(!client.is_checkpoint_topic("checkpoints-internal"));
    assert!(!client.is_checkpoint_topic("checkpoints.internal!"));
    assert!(!client.is_checkpoint_topic("!checkpoints.internal"));
    assert!(!client.is_checkpoint_topic("source1checkpointsinternal"));
}

#[test]
fn test_remote_topics_separator() {
    let mut topics = HashSet::new();
    topics.insert("topic1".to_string());
    topics.insert("topic2".to_string());
    topics.insert("topic3".to_string());
    topics.insert("source1__topic4".to_string());
    topics.insert("source1__source2__topic5".to_string());
    topics.insert("source3__source4__source5__topic6".to_string());

    let admin = Box::new(MockAdminClient::new(topics));
    let mut policy = DefaultReplicationPolicy::new();
    // Configure with custom separator
    let mut config = HashMap::new();
    config.insert("replication.policy.separator".to_string(), "__".to_string());
    policy.configure(&config);
    let policy = Box::new(policy);
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    let remote_topics = client.remote_topics().unwrap();
    assert!(!remote_topics.contains("topic1"));
    assert!(!remote_topics.contains("topic2"));
    assert!(!remote_topics.contains("topic3"));
    assert!(remote_topics.contains("source1__topic4"));
    assert!(remote_topics.contains("source1__source2__topic5"));
    assert!(remote_topics.contains("source3__source4__source5__topic6"));
}

#[test]
fn test_identity_replication_upstream_clusters() {
    // IdentityReplicationPolicy treats heartbeats as a special case, so these should work as usual.
    let mut topics = HashSet::new();
    topics.insert("topic1".to_string());
    topics.insert("topic2".to_string());
    topics.insert("heartbeats".to_string());
    topics.insert("source1.heartbeats".to_string());
    topics.insert("source1.source2.heartbeats".to_string());
    topics.insert("source3.source4.source5.heartbeats".to_string());

    let admin = Box::new(MockAdminClient::new(topics));
    let policy = identity_replication_policy("source");
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    let sources = client.upstream_clusters().unwrap();
    assert!(sources.contains("source1"));
    assert!(sources.contains("source2"));
    assert!(sources.contains("source3"));
    assert!(sources.contains("source4"));
    assert!(sources.contains("source5"));
    assert_eq!(sources.len(), 5);
}

#[test]
fn test_identity_replication_remote_topics() {
    // IdentityReplicationPolicy should consider any topic to be remote.
    let mut topics = HashSet::new();
    topics.insert("topic1".to_string());
    topics.insert("topic2".to_string());
    topics.insert("topic3".to_string());
    topics.insert("heartbeats".to_string());
    topics.insert("backup.heartbeats".to_string());

    let admin = Box::new(MockAdminClient::new(topics));
    let policy = identity_replication_policy("source");
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    let remote_topics = client.remote_topics().unwrap();
    assert!(remote_topics.contains("topic1"));
    assert!(remote_topics.contains("topic2"));
    assert!(remote_topics.contains("topic3"));
    // Heartbeats are treated as a special case
    assert!(!remote_topics.contains("heartbeats"));
    assert!(remote_topics.contains("backup.heartbeats"));
}

#[test]
fn test_identity_replication_topic_source() {
    let admin = Box::new(MockAdminClient::empty());
    let policy = identity_replication_policy("primary");
    let consumer_config = HashMap::new();
    let client = MirrorClient::new_for_test(admin, policy, consumer_config);

    let policy = client.replication_policy();
    assert_eq!(policy.format_remote_topic("primary", "topic1"), "topic1");
    assert_eq!(policy.topic_source("topic1"), Some("primary".to_string()));

    // Heartbeats are handled as a special case
    assert_eq!(
        policy.format_remote_topic("backup", "heartbeats"),
        "backup.heartbeats"
    );
    assert_eq!(
        policy.topic_source("backup.heartbeats"),
        Some("backup".to_string())
    );
}
