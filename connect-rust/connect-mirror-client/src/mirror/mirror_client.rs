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

//! Client to interact with MirrorMaker internal topics (checkpoints, heartbeats) on a given cluster.
//!
//! This corresponds to Java: org.apache.kafka.connect.mirror.MirrorClient
//! Whenever possible use the methods from RemoteClusterUtils instead of directly using MirrorClient.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use common_trait::KafkaException;
use common_trait::TopicPartition;

use super::checkpoint::Checkpoint;
use super::mirror_client_config::MirrorClientConfig;
use super::replication_policy::ReplicationPolicy;

// ============================================================================
// OffsetAndMetadata - corresponds to Java: OffsetAndMetadata
// ============================================================================

/// OffsetAndMetadata represents a consumer offset with optional metadata.
///
/// Corresponds to Java: org.apache.kafka.clients.consumer.OffsetAndMetadata
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetAndMetadata {
    offset: i64,
    metadata: String,
}

impl OffsetAndMetadata {
    /// Creates a new OffsetAndMetadata with the given offset and metadata.
    pub fn new(offset: i64, metadata: impl Into<String>) -> Self {
        OffsetAndMetadata {
            offset,
            metadata: metadata.into(),
        }
    }

    /// Returns the offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the metadata.
    pub fn metadata(&self) -> &str {
        &self.metadata
    }
}

impl Default for OffsetAndMetadata {
    fn default() -> Self {
        Self::new(0, "")
    }
}

// ============================================================================
// ConsumerRecord - corresponds to Java: ConsumerRecord
// ============================================================================

/// ConsumerRecord represents a record consumed from a Kafka topic.
///
/// Corresponds to Java: org.apache.kafka.clients.consumer.ConsumerRecord
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    topic: String,
    partition: i32,
    offset: i64,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl ConsumerRecord {
    /// Creates a new ConsumerRecord.
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Self {
        ConsumerRecord {
            topic: topic.into(),
            partition,
            offset,
            key,
            value,
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the key bytes.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value bytes.
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

// ============================================================================
// AdminClient trait - corresponds to Java: Admin
// ============================================================================

/// AdminClient trait for Kafka admin operations.
///
/// This is a simplified version of Java's Admin client interface,
/// designed to work with MirrorClient. Since connect-api has compilation
/// issues, we define a trait that can be implemented by mock clients for testing.
///
/// Corresponds to Java: org.apache.kafka.clients.admin.Admin
pub trait AdminClient: Send + Sync {
    /// Lists all topics in the cluster.
    ///
    /// Corresponds to Java: Admin.listTopics().names().get()
    fn list_topics(&self) -> Result<HashSet<String>, KafkaException>;

    /// Closes the admin client and releases any resources.
    ///
    /// Corresponds to Java: Admin.close()
    fn close(&self);
}

// ============================================================================
// Consumer trait - corresponds to Java: Consumer
// ============================================================================

/// Consumer trait for Kafka consumer operations.
///
/// This is a simplified version of Java's Consumer interface,
/// designed to work with MirrorClient's remoteConsumerOffsets method.
///
/// Corresponds to Java: org.apache.kafka.clients.consumer.Consumer
pub trait Consumer: Send + Sync {
    /// Assigns the consumer to the given partitions.
    ///
    /// Corresponds to Java: Consumer.assign(Collection<TopicPartition> partitions)
    fn assign(&mut self, partitions: Vec<TopicPartition>);

    /// Seeks to the beginning of the given partitions.
    ///
    /// Corresponds to Java: Consumer.seekToBeginning(Collection<TopicPartition> partitions)
    fn seek_to_beginning(&mut self, partitions: Vec<TopicPartition>);

    /// Polls for records with the given timeout.
    ///
    /// Corresponds to Java: Consumer.poll(Duration timeout)
    fn poll(&mut self, timeout: Duration) -> Vec<ConsumerRecord>;

    /// Returns the end offsets for the given partitions.
    ///
    /// Corresponds to Java: Consumer.endOffsets(Collection<TopicPartition> partitions)
    fn end_offsets(&self, partitions: Vec<TopicPartition>) -> HashMap<TopicPartition, i64>;

    /// Returns the current position for the given partition.
    ///
    /// Corresponds to Java: Consumer.position(TopicPartition partition)
    fn position(&self, partition: TopicPartition) -> i64;

    /// Closes the consumer and releases any resources.
    ///
    /// Corresponds to Java: Consumer.close()
    fn close(&mut self);
}

// ============================================================================
// MirrorClient - corresponds to Java: MirrorClient
// ============================================================================

/// Client to interact with MirrorMaker internal topics (checkpoints, heartbeats) on a given cluster.
///
/// Whenever possible use the methods from RemoteClusterUtils instead of directly using MirrorClient.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorClient
pub struct MirrorClient {
    admin_client: Box<dyn AdminClient>,
    replication_policy: Box<dyn ReplicationPolicy>,
    consumer_config: HashMap<String, String>,
}

impl MirrorClient {
    /// Creates a new MirrorClient with the given properties.
    ///
    /// Corresponds to Java: MirrorClient(Map<String, Object> props)
    pub fn new(props: HashMap<String, String>) -> Self {
        Self::new_with_config(MirrorClientConfig::new(props))
    }

    /// Creates a new MirrorClient with the given configuration.
    ///
    /// Corresponds to Java: MirrorClient(MirrorClientConfig config)
    pub fn new_with_config(config: MirrorClientConfig) -> Self {
        let admin_client = config.forwarding_admin();
        let consumer_config = config.consumer_config();
        let replication_policy = config.replication_policy();

        // We need to wrap ForwardingAdmin as AdminClient
        // For now, we use a simple wrapper
        MirrorClient {
            admin_client: Box::new(ForwardingAdminWrapper::new(admin_client)),
            replication_policy,
            consumer_config,
        }
    }

    /// Creates a new MirrorClient for testing with the given components.
    ///
    /// Corresponds to Java: MirrorClient(Admin adminClient, ReplicationPolicy replicationPolicy, Map<String, Object> consumerConfig)
    pub fn new_for_test(
        admin_client: Box<dyn AdminClient>,
        policy: Box<dyn ReplicationPolicy>,
        consumer_config: HashMap<String, String>,
    ) -> Self {
        MirrorClient {
            admin_client,
            replication_policy: policy,
            consumer_config,
        }
    }

    /// Closes internal clients.
    ///
    /// Corresponds to Java: MirrorClient.close()
    pub fn close(&self) {
        self.admin_client.close();
    }

    /// Gets the ReplicationPolicy instance used to interpret remote topics.
    ///
    /// Corresponds to Java: MirrorClient.replicationPolicy()
    pub fn replication_policy(&self) -> &dyn ReplicationPolicy {
        self.replication_policy.as_ref()
    }

    /// Computes the shortest number of hops from an upstream source cluster.
    ///
    /// For example, given replication flow A->B->C, there are two hops from A to C.
    /// Returns -1 if the upstream cluster is unreachable.
    ///
    /// Corresponds to Java: MirrorClient.replicationHops(String upstreamClusterAlias)
    pub fn replication_hops(&self, upstream_cluster_alias: &str) -> Result<i32, KafkaException> {
        let heartbeat_topics = self.heartbeat_topics()?;
        let hops: Vec<i32> = heartbeat_topics
            .iter()
            .map(|topic| self.count_hops_for_topic(topic, upstream_cluster_alias))
            .filter(|&hops| hops != -1)
            .collect();

        Ok(hops.iter().min().copied().unwrap_or(-1))
    }

    /// Finds all heartbeats topics on this cluster.
    ///
    /// Heartbeats topics are replicated from other clusters.
    ///
    /// Corresponds to Java: MirrorClient.heartbeatTopics()
    pub fn heartbeat_topics(&self) -> Result<HashSet<String>, KafkaException> {
        let topics = self.list_topics()?;
        Ok(topics
            .iter()
            .filter(|t| self.is_heartbeat_topic(t))
            .cloned()
            .collect())
    }

    /// Finds all checkpoints topics on this cluster.
    ///
    /// Corresponds to Java: MirrorClient.checkpointTopics()
    pub fn checkpoint_topics(&self) -> Result<HashSet<String>, KafkaException> {
        let topics = self.list_topics()?;
        Ok(topics
            .iter()
            .filter(|t| self.is_checkpoint_topic(t))
            .cloned()
            .collect())
    }

    /// Finds upstream clusters, which may be multiple hops away, based on incoming heartbeats.
    ///
    /// Corresponds to Java: MirrorClient.upstreamClusters()
    pub fn upstream_clusters(&self) -> Result<HashSet<String>, KafkaException> {
        let topics = self.list_topics()?;
        let heartbeat_topics: Vec<String> = topics
            .iter()
            .filter(|t| self.is_heartbeat_topic(t))
            .cloned()
            .collect();

        let mut clusters = HashSet::new();
        for topic in heartbeat_topics {
            let sources = self.all_sources(&topic);
            clusters.extend(sources);
        }
        Ok(clusters)
    }

    /// Finds all remote topics on this cluster.
    ///
    /// This does not include internal topics (heartbeats, checkpoints).
    ///
    /// Corresponds to Java: MirrorClient.remoteTopics()
    pub fn remote_topics(&self) -> Result<HashSet<String>, KafkaException> {
        let topics = self.list_topics()?;
        Ok(topics
            .iter()
            .filter(|t| self.is_remote_topic(t))
            .cloned()
            .collect())
    }

    /// Finds all remote topics that have been replicated directly from the given source cluster.
    ///
    /// Corresponds to Java: MirrorClient.remoteTopics(String source)
    pub fn remote_topics_for_source(
        &self,
        source: &str,
    ) -> Result<HashSet<String>, KafkaException> {
        let topics = self.list_topics()?;
        Ok(topics
            .iter()
            .filter(|t| {
                self.is_remote_topic(t)
                    && self.replication_policy.topic_source(t) == Some(source.to_string())
            })
            .cloned()
            .collect())
    }

    /// Translates a remote consumer group's offsets into corresponding local offsets.
    ///
    /// Topics are automatically renamed according to the ReplicationPolicy.
    ///
    /// Corresponds to Java: MirrorClient.remoteConsumerOffsets(String consumerGroupId, String remoteClusterAlias, Duration timeout)
    pub fn remote_consumer_offsets(
        &self,
        consumer_group_id: &str,
        remote_cluster_alias: &str,
        timeout: Duration,
        consumer: &mut dyn Consumer,
    ) -> HashMap<TopicPartition, OffsetAndMetadata> {
        let deadline = std::time::Instant::now() + timeout;
        let mut offsets = HashMap::new();

        // checkpoint topics are not "remote topics", as they are not replicated.
        // So we don't need to use ReplicationPolicy to create the checkpoint topic here.
        let checkpoint_topic = self
            .replication_policy
            .checkpoints_topic(remote_cluster_alias);
        let checkpoint_assignment = vec![TopicPartition::new(&checkpoint_topic, 0)];

        consumer.assign(checkpoint_assignment.clone());
        consumer.seek_to_beginning(checkpoint_assignment.clone());

        while std::time::Instant::now() < deadline
            && !Self::end_of_stream(consumer, checkpoint_assignment.clone())
        {
            let records = consumer.poll(timeout);
            for record in records {
                if let Ok(checkpoint) = Checkpoint::deserialize_record(record.key(), record.value())
                {
                    if checkpoint.consumer_group_id() == consumer_group_id {
                        offsets.insert(
                            checkpoint.topic_partition().clone(),
                            OffsetAndMetadata::new(
                                checkpoint.downstream_offset(),
                                checkpoint.metadata(),
                            ),
                        );
                    }
                }
            }
        }

        offsets
    }

    // ========================================================================
    // Internal methods - package-private in Java
    // ========================================================================

    /// Lists all topics in the cluster.
    ///
    /// Corresponds to Java: MirrorClient.listTopics()
    pub fn list_topics(&self) -> Result<HashSet<String>, KafkaException> {
        self.admin_client.list_topics()
    }

    /// Counts the number of hops for a topic to reach the source cluster.
    ///
    /// Corresponds to Java: MirrorClient.countHopsForTopic(String topic, String sourceClusterAlias)
    pub fn count_hops_for_topic(&self, topic: &str, source_cluster_alias: &str) -> i32 {
        let mut hops = 0;
        let mut visited = HashSet::new();
        let mut current_topic = topic.to_string();

        loop {
            hops += 1;
            let source = self.replication_policy.topic_source(&current_topic);
            if source.is_none() {
                return -1;
            }
            let source = source.unwrap();
            if source == source_cluster_alias {
                return hops;
            }
            if visited.contains(&source) {
                // Extra check for IdentityReplicationPolicy and similar impls that cannot prevent cycles.
                // We assume we're stuck in a cycle and will never find sourceClusterAlias.
                return -1;
            }
            visited.insert(source);
            let upstream_topic = self.replication_policy.upstream_topic(&current_topic);
            match upstream_topic {
                Some(upstream) => current_topic = upstream,
                None => return -1,
            }
        }
    }

    /// Returns true if the topic is a heartbeat topic.
    ///
    /// Corresponds to Java: MirrorClient.isHeartbeatTopic(String topic)
    pub fn is_heartbeat_topic(&self, topic: &str) -> bool {
        self.replication_policy.is_heartbeats_topic(topic)
    }

    /// Returns true if the topic is a checkpoint topic.
    ///
    /// Corresponds to Java: MirrorClient.isCheckpointTopic(String topic)
    pub fn is_checkpoint_topic(&self, topic: &str) -> bool {
        self.replication_policy.is_checkpoints_topic(topic)
    }

    /// Returns true if the topic is a remote topic (replicated from another cluster).
    ///
    /// Corresponds to Java: MirrorClient.isRemoteTopic(String topic)
    pub fn is_remote_topic(&self, topic: &str) -> bool {
        !self.replication_policy.is_internal_topic(topic)
            && self.replication_policy.topic_source(topic).is_some()
    }

    /// Returns all source clusters for a topic (may include multiple hops).
    ///
    /// Corresponds to Java: MirrorClient.allSources(String topic)
    pub fn all_sources(&self, topic: &str) -> HashSet<String> {
        let mut sources = HashSet::new();
        let mut current_topic = topic.to_string();
        let mut source = self.replication_policy.topic_source(&current_topic);

        while let Some(s) = source {
            if sources.contains(&s) {
                // The extra Set.contains above is for ReplicationPolicies that cannot prevent cycles.
                break;
            }
            sources.insert(s.clone());

            let upstream_topic = self.replication_policy.upstream_topic(&current_topic);
            match upstream_topic {
                Some(upstream) => {
                    current_topic = upstream;
                    source = self.replication_policy.topic_source(&current_topic);
                }
                None => break,
            }
        }

        sources
    }

    /// Checks if the consumer has reached the end of the stream for the given partitions.
    ///
    /// Corresponds to Java: MirrorClient.endOfStream(Consumer<?, ?> consumer, Collection<TopicPartition> assignments)
    pub fn end_of_stream(consumer: &dyn Consumer, assignments: Vec<TopicPartition>) -> bool {
        let end_offsets = consumer.end_offsets(assignments.clone());
        for topic_partition in assignments {
            let position = consumer.position(topic_partition.clone());
            let end_offset = end_offsets.get(&topic_partition).copied().unwrap_or(0);
            if position < end_offset {
                return false;
            }
        }
        true
    }
}

// ============================================================================
// ForwardingAdminWrapper - wraps ForwardingAdmin as AdminClient
// ============================================================================

use super::mirror_client_config::ForwardingAdmin;

/// Wrapper to make ForwardingAdmin implement AdminClient trait.
struct ForwardingAdminWrapper {
    admin: ForwardingAdmin,
}

impl ForwardingAdminWrapper {
    fn new(admin: ForwardingAdmin) -> Self {
        ForwardingAdminWrapper { admin }
    }
}

impl AdminClient for ForwardingAdminWrapper {
    fn list_topics(&self) -> Result<HashSet<String>, KafkaException> {
        // ForwardingAdmin doesn't have real admin capabilities in our current implementation
        // This is a placeholder that returns an empty set
        // In a real implementation, this would use KafkaAdminClient
        Ok(HashSet::new())
    }

    fn close(&self) {
        // ForwardingAdmin has no close method in our implementation
        // This is a placeholder
    }
}

// ============================================================================
// MockAdminClient - for testing
// ============================================================================

/// Mock implementation of AdminClient for testing purposes.
pub struct MockAdminClient {
    topics: HashSet<String>,
}

impl MockAdminClient {
    /// Creates a new MockAdminClient with the given topics.
    pub fn new(topics: HashSet<String>) -> Self {
        MockAdminClient { topics }
    }

    /// Creates a new MockAdminClient with no topics.
    pub fn empty() -> Self {
        MockAdminClient {
            topics: HashSet::new(),
        }
    }

    /// Adds a topic to the mock client.
    pub fn add_topic(&mut self, topic: impl Into<String>) {
        self.topics.insert(topic.into());
    }

    /// Removes a topic from the mock client.
    pub fn remove_topic(&mut self, topic: &str) {
        self.topics.remove(topic);
    }
}

impl Default for MockAdminClient {
    fn default() -> Self {
        Self::empty()
    }
}

impl AdminClient for MockAdminClient {
    fn list_topics(&self) -> Result<HashSet<String>, KafkaException> {
        Ok(self.topics.clone())
    }

    fn close(&self) {
        // No resources to close in mock
    }
}

// ============================================================================
// MockConsumer - for testing
// ============================================================================

/// Mock implementation of Consumer for testing purposes.
pub struct MockConsumer {
    assigned_partitions: Vec<TopicPartition>,
    records: Vec<ConsumerRecord>,
    end_offsets_map: HashMap<TopicPartition, i64>,
    current_position: HashMap<TopicPartition, i64>,
    record_index: usize,
}

impl MockConsumer {
    /// Creates a new MockConsumer.
    pub fn new() -> Self {
        MockConsumer {
            assigned_partitions: Vec::new(),
            records: Vec::new(),
            end_offsets_map: HashMap::new(),
            current_position: HashMap::new(),
            record_index: 0,
        }
    }

    /// Adds a record to the mock consumer.
    pub fn add_record(&mut self, record: ConsumerRecord) {
        let tp = TopicPartition::new(record.topic(), record.partition());
        self.records.push(record);
        self.end_offsets_map.insert(tp, self.records.len() as i64);
    }

    /// Sets the end offset for a partition.
    pub fn set_end_offset(&mut self, partition: TopicPartition, offset: i64) {
        self.end_offsets_map.insert(partition, offset);
    }

    /// Clears all records.
    pub fn clear_records(&mut self) {
        self.records.clear();
        self.record_index = 0;
    }
}

impl Default for MockConsumer {
    fn default() -> Self {
        Self::new()
    }
}

impl Consumer for MockConsumer {
    fn assign(&mut self, partitions: Vec<TopicPartition>) {
        self.assigned_partitions = partitions.clone();
        for tp in partitions {
            self.current_position.insert(tp, 0);
        }
    }

    fn seek_to_beginning(&mut self, partitions: Vec<TopicPartition>) {
        for tp in partitions {
            self.current_position.insert(tp, 0);
        }
        self.record_index = 0;
    }

    fn poll(&mut self, _timeout: Duration) -> Vec<ConsumerRecord> {
        if self.record_index < self.records.len() {
            let record = self.records[self.record_index].clone();
            self.record_index += 1;
            let tp = TopicPartition::new(record.topic(), record.partition());
            self.current_position.insert(tp, record.offset() + 1);
            vec![record]
        } else {
            Vec::new()
        }
    }

    fn end_offsets(&self, partitions: Vec<TopicPartition>) -> HashMap<TopicPartition, i64> {
        partitions
            .iter()
            .filter_map(|tp| {
                self.end_offsets_map
                    .get(tp)
                    .map(|&offset| (tp.clone(), offset))
            })
            .collect()
    }

    fn position(&self, partition: TopicPartition) -> i64 {
        self.current_position.get(&partition).copied().unwrap_or(0)
    }

    fn close(&mut self) {
        // No resources to close in mock
    }
}
