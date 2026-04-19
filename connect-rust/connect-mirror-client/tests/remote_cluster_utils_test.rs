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

//! Tests for RemoteClusterUtils module
//! Corresponds to Java: org.apache.kafka.connect.mirror.RemoteClusterUtils

use connect_mirror_client::{
    checkpoint_topics, heartbeat_topics, replication_hops, translate_offsets, upstream_clusters,
};
use std::collections::HashMap;
use std::time::Duration;

#[test]
fn test_replication_hops() {
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );

    // This test uses the default MockAdminClient which returns empty topics
    // So replication_hops should return -1 (no heartbeat topics found)
    let result = replication_hops(props, "source1");
    assert_eq!(result.unwrap(), -1);
}

#[test]
fn test_heartbeat_topics() {
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );

    // This test uses the default MockAdminClient which returns empty topics
    let result = heartbeat_topics(props);
    assert!(result.unwrap().is_empty());
}

#[test]
fn test_checkpoint_topics() {
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );

    // This test uses the default MockAdminClient which returns empty topics
    let result = checkpoint_topics(props);
    assert!(result.unwrap().is_empty());
}

#[test]
fn test_upstream_clusters() {
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );

    // This test uses the default MockAdminClient which returns empty topics
    let result = upstream_clusters(props);
    assert!(result.unwrap().is_empty());
}

#[test]
fn test_translate_offsets() {
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );

    // This test uses MockConsumer which returns no records
    let result = translate_offsets(props, "source", "test-group", Duration::from_secs(5));
    assert!(result.unwrap().is_empty());
}
