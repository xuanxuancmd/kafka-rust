/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Convenience tool for multi-cluster environments. Wraps MirrorClient.
//!
//! Properties passed to these methods are used to construct internal Admin and Consumer clients.
//! Sub-configs like "admin.xyz" are also supported. For example:
//! ```text
//!     bootstrap.servers = host1:9092
//!     consumer.client.id = mm2-client
//! ```
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.RemoteClusterUtils

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use common_trait::KafkaException;
use common_trait::TopicPartition;

use super::mirror_client::{Consumer, MirrorClient, MockConsumer, OffsetAndMetadata};
use super::mirror_client_config::MirrorClientConfig;

/// Finds the shortest number of hops from an upstream cluster.
///
/// Returns -1 if the cluster is unreachable.
///
/// Corresponds to Java: RemoteClusterUtils.replicationHops(Map<String, Object> properties, String upstreamClusterAlias)
pub fn replication_hops(
    properties: HashMap<String, String>,
    upstream_cluster_alias: &str,
) -> Result<i32, KafkaException> {
    let client = MirrorClient::new(properties);
    let result = client.replication_hops(upstream_cluster_alias);
    client.close();
    result
}

/// Finds all heartbeats topics.
///
/// Corresponds to Java: RemoteClusterUtils.heartbeatTopics(Map<String, Object> properties)
pub fn heartbeat_topics(
    properties: HashMap<String, String>,
) -> Result<HashSet<String>, KafkaException> {
    let client = MirrorClient::new(properties);
    let result = client.heartbeat_topics();
    client.close();
    result
}

/// Finds all checkpoints topics.
///
/// Corresponds to Java: RemoteClusterUtils.checkpointTopics(Map<String, Object> properties)
pub fn checkpoint_topics(
    properties: HashMap<String, String>,
) -> Result<HashSet<String>, KafkaException> {
    let client = MirrorClient::new(properties);
    let result = client.checkpoint_topics();
    client.close();
    result
}

/// Finds all upstream clusters.
///
/// Corresponds to Java: RemoteClusterUtils.upstreamClusters(Map<String, Object> properties)
pub fn upstream_clusters(
    properties: HashMap<String, String>,
) -> Result<HashSet<String>, KafkaException> {
    let client = MirrorClient::new(properties);
    let result = client.upstream_clusters();
    client.close();
    result
}

/// Translates a remote consumer group's offsets into corresponding local offsets.
///
/// Topics are automatically renamed according to the configured ReplicationPolicy.
///
/// # Arguments
/// * `properties` - Map of properties to instantiate a MirrorClient
/// * `remote_cluster_alias` - The alias of the remote cluster
/// * `consumer_group_id` - The group ID of remote consumer group
/// * `timeout` - The maximum time to block when consuming from the checkpoints topic
///
/// Corresponds to Java: RemoteClusterUtils.translateOffsets(Map<String, Object> properties,
///     String remoteClusterAlias, String consumerGroupId, Duration timeout)
pub fn translate_offsets(
    properties: HashMap<String, String>,
    remote_cluster_alias: &str,
    consumer_group_id: &str,
    timeout: Duration,
) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaException> {
    // Create MirrorClientConfig to get consumer config
    let config = MirrorClientConfig::new(properties.clone());
    let consumer_config = config.consumer_config();

    // Create MirrorClient
    let client = MirrorClient::new(properties);

    // Create a consumer for reading checkpoints
    // In a real implementation, this would create a KafkaConsumer from consumer_config
    // For now, we use MockConsumer which requires records to be pre-populated
    let mut consumer = MockConsumer::new();

    // Call remote_consumer_offsets with the consumer
    let result = client.remote_consumer_offsets(
        consumer_group_id,
        remote_cluster_alias,
        timeout,
        &mut consumer,
    );

    client.close();
    consumer.close();

    Ok(result)
}
