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

//! Tests for MirrorClientConfig module
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorClientConfig

use connect_mirror_client::{
    ForwardingAdmin, MirrorClientConfig, ADMIN_CLIENT_PREFIX, CONSUMER_CLIENT_PREFIX,
    FORWARDING_ADMIN_CLASS, FORWARDING_ADMIN_CLASS_DEFAULT, INTERNAL_TOPIC_SEPARATOR_ENABLED,
    INTERNAL_TOPIC_SEPARATOR_ENABLED_DEFAULT, PRODUCER_CLIENT_PREFIX, REPLICATION_POLICY_CLASS,
    REPLICATION_POLICY_CLASS_DEFAULT, REPLICATION_POLICY_SEPARATOR,
    REPLICATION_POLICY_SEPARATOR_DEFAULT,
};
use std::collections::HashMap;

#[test]
fn test_new() {
    let config = MirrorClientConfig::new(HashMap::new());
    assert_eq!(config.originals().len(), 0);
}

#[test]
fn test_replication_policy_default() {
    let config = MirrorClientConfig::new(HashMap::new());
    let policy = config.replication_policy();
    // Test that default policy formats topics correctly
    let remote_topic = policy.format_remote_topic("source", "topic");
    assert_eq!(remote_topic, "source.topic");
}

#[test]
fn test_replication_policy_custom_separator() {
    let mut props = HashMap::new();
    props.insert(REPLICATION_POLICY_SEPARATOR.to_string(), "__".to_string());
    let config = MirrorClientConfig::new(props);
    let policy = config.replication_policy();
    let remote_topic = policy.format_remote_topic("source", "topic");
    assert_eq!(remote_topic, "source__topic");
}

#[test]
fn test_admin_config() {
    let mut props = HashMap::new();
    props.insert(
        "admin.bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    props.insert("admin.client.id".to_string(), "test-client".to_string());
    props.insert("bootstrap.servers".to_string(), "common:9092".to_string());
    let config = MirrorClientConfig::new(props);
    let admin_config = config.admin_config();
    assert_eq!(
        admin_config.get("bootstrap.servers"),
        Some(&"localhost:9092".to_string())
    );
    assert_eq!(
        admin_config.get("client.id"),
        Some(&"test-client".to_string())
    );
}

#[test]
fn test_consumer_config() {
    let mut props = HashMap::new();
    props.insert(
        "consumer.bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    props.insert("consumer.group.id".to_string(), "test-group".to_string());
    let config = MirrorClientConfig::new(props);
    let consumer_config = config.consumer_config();
    assert_eq!(
        consumer_config.get("bootstrap.servers"),
        Some(&"localhost:9092".to_string())
    );
    assert_eq!(
        consumer_config.get("group.id"),
        Some(&"test-group".to_string())
    );
}

#[test]
fn test_producer_config() {
    let mut props = HashMap::new();
    props.insert(
        "producer.bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    props.insert("producer.acks".to_string(), "all".to_string());
    let config = MirrorClientConfig::new(props);
    let producer_config = config.producer_config();
    assert_eq!(
        producer_config.get("bootstrap.servers"),
        Some(&"localhost:9092".to_string())
    );
    assert_eq!(producer_config.get("acks"), Some(&"all".to_string()));
}

#[test]
fn test_forwarding_admin() {
    let mut props = HashMap::new();
    props.insert(
        "admin.bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    let config = MirrorClientConfig::new(props);
    let admin = config.forwarding_admin();
    assert_eq!(
        admin.config().get("bootstrap.servers"),
        Some(&"localhost:9092".to_string())
    );
}

#[test]
fn test_constants() {
    assert_eq!(REPLICATION_POLICY_CLASS, "replication.policy.class");
    assert_eq!(
        REPLICATION_POLICY_CLASS_DEFAULT,
        "org.apache.kafka.connect.mirror.DefaultReplicationPolicy"
    );
    assert_eq!(REPLICATION_POLICY_SEPARATOR, "replication.policy.separator");
    assert_eq!(REPLICATION_POLICY_SEPARATOR_DEFAULT, ".");
    assert_eq!(
        INTERNAL_TOPIC_SEPARATOR_ENABLED,
        "replication.policy.internal.topic.separator.enabled"
    );
    assert_eq!(INTERNAL_TOPIC_SEPARATOR_ENABLED_DEFAULT, true);
    assert_eq!(FORWARDING_ADMIN_CLASS, "forwarding.admin.class");
    assert_eq!(
        FORWARDING_ADMIN_CLASS_DEFAULT,
        "org.apache.kafka.clients.admin.ForwardingAdmin"
    );
    assert_eq!(ADMIN_CLIENT_PREFIX, "admin.");
    assert_eq!(CONSUMER_CLIENT_PREFIX, "consumer.");
    assert_eq!(PRODUCER_CLIENT_PREFIX, "producer.");
}
