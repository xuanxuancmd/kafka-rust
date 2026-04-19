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

//! Tests for ReplicationPolicy module
//! Corresponds to Java: org.apache.kafka.connect.mirror.ReplicationPolicy
//! and org.apache.kafka.connect.mirror.DefaultReplicationPolicy

use connect_mirror_client::{
    DefaultReplicationPolicy, ReplicationPolicy, INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG,
    REPLICATION_POLICY_SEPARATOR,
};
use std::collections::HashMap;

#[test]
fn test_internal_topic() {
    let mut policy = DefaultReplicationPolicy::new();
    let mut config = HashMap::new();
    config.insert(REPLICATION_POLICY_SEPARATOR.to_string(), ".".to_string());
    policy.configure(&config);

    // starts with '__'
    assert!(policy.is_internal_topic("__consumer_offsets"));
    // starts with '.'
    assert!(policy.is_internal_topic(".hiddentopic"));

    // starts with 'mm2' and ends with '.internal': default DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG in standalone mode.
    assert!(policy.is_internal_topic("mm2-offsets.CLUSTER.internal"));
    // non-internal topic.
    assert!(!policy.is_internal_topic("mm2-offsets_CLUSTER_internal"));
}

#[test]
fn offset_syncs_topic_separator_enabled() {
    let mut policy = DefaultReplicationPolicy::new();
    let mut config = HashMap::new();
    config.insert(REPLICATION_POLICY_SEPARATOR.to_string(), "__".to_string());

    // INTERNAL_TOPIC_SEPARATOR_ENABLED = false -> uses "."
    config.insert(
        INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );
    policy.configure(&config);
    assert_eq!(
        "mm2-offset-syncs.CLUSTER.internal",
        policy.offset_syncs_topic("CLUSTER")
    );

    // INTERNAL_TOPIC_SEPARATOR_ENABLED = true -> uses separator "__"
    config.insert(
        INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG.to_string(),
        "true".to_string(),
    );
    policy.configure(&config);
    assert_eq!(
        "mm2-offset-syncs__CLUSTER__internal",
        policy.offset_syncs_topic("CLUSTER")
    );
}

#[test]
fn checkpoints_topic_separator_enabled() {
    let mut policy = DefaultReplicationPolicy::new();
    let mut config = HashMap::new();
    config.insert(REPLICATION_POLICY_SEPARATOR.to_string(), "__".to_string());

    // INTERNAL_TOPIC_SEPARATOR_ENABLED = false -> uses "."
    config.insert(
        INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );
    policy.configure(&config);
    assert_eq!(
        "CLUSTER.checkpoints.internal",
        policy.checkpoints_topic("CLUSTER")
    );

    // INTERNAL_TOPIC_SEPARATOR_ENABLED = true -> uses separator "__"
    config.insert(
        INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG.to_string(),
        "true".to_string(),
    );
    policy.configure(&config);
    assert_eq!(
        "CLUSTER__checkpoints__internal",
        policy.checkpoints_topic("CLUSTER")
    );
}

#[test]
fn heartbeats_topic_not_effected() {
    let mut policy = DefaultReplicationPolicy::new();
    let mut config = HashMap::new();
    config.insert(REPLICATION_POLICY_SEPARATOR.to_string(), "__".to_string());

    // INTERNAL_TOPIC_SEPARATOR_ENABLED = true
    config.insert(
        INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG.to_string(),
        "true".to_string(),
    );
    policy.configure(&config);
    assert_eq!("heartbeats", policy.heartbeats_topic());

    // INTERNAL_TOPIC_SEPARATOR_ENABLED = false
    config.insert(
        INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );
    policy.configure(&config);
    assert_eq!("heartbeats", policy.heartbeats_topic());
}
