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

//! Mock implementation of Kafka Admin client.
//!
//! This corresponds to `org.apache.kafka.clients.admin.Admin` in Java.
//! Provides an in-memory mock for testing purposes with support for:
//! - Topic management: create, delete, list topics
//! - Consumer group offset management: list, alter, delete offsets

use common_trait::{AdminClient, ConnectError, TopicPartition};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::OffsetAndMetadata;

/// Topic configuration for creating new topics.
///
/// This corresponds to `org.apache.kafka.clients.admin.NewTopic` in Java.
#[derive(Debug, Clone)]
pub struct NewTopic {
    name: String,
    num_partitions: i32,
    replication_factor: i32,
    configs: HashMap<String, String>,
}

impl NewTopic {
    /// Creates a new topic specification with the given name, partitions, and replication factor.
    pub fn new(name: impl Into<String>, num_partitions: i32, replication_factor: i32) -> Self {
        NewTopic {
            name: name.into(),
            num_partitions,
            replication_factor,
            configs: HashMap::new(),
        }
    }

    /// Creates a new topic with the given name and specific partition assignment.
    pub fn with_partitions(name: impl Into<String>, partitions: Vec<Vec<i32>>) -> Self {
        NewTopic {
            name: name.into(),
            num_partitions: partitions.len() as i32,
            replication_factor: -1,
            configs: HashMap::new(),
        }
    }

    /// Adds a configuration for the topic.
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.insert(key.into(), value.into());
        self
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the number of partitions.
    pub fn num_partitions(&self) -> i32 {
        self.num_partitions
    }

    /// Returns the replication factor.
    pub fn replication_factor(&self) -> i32 {
        self.replication_factor
    }

    /// Returns the topic configurations.
    pub fn configs(&self) -> &HashMap<String, String> {
        &self.configs
    }
}

/// Metadata for a created topic.
///
/// This corresponds to `org.apache.kafka.clients.admin.TopicMetadataAndConfig` in Java.
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    name: String,
    num_partitions: i32,
    replication_factor: i32,
    configs: HashMap<String, String>,
    partition_metadata: Vec<PartitionMetadata>,
}

impl TopicMetadata {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the number of partitions.
    pub fn num_partitions(&self) -> i32 {
        self.num_partitions
    }

    /// Returns the replication factor.
    pub fn replication_factor(&self) -> i32 {
        self.replication_factor
    }

    /// Returns the topic configurations.
    pub fn configs(&self) -> &HashMap<String, String> {
        &self.configs
    }

    /// Returns the partition metadata.
    pub fn partition_metadata(&self) -> &Vec<PartitionMetadata> {
        &self.partition_metadata
    }
}

/// Metadata for a partition.
///
/// This corresponds to `org.apache.kafka.clients.admin.PartitionMetadata` in Java.
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    partition: i32,
    leader: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
}

impl PartitionMetadata {
    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the leader broker ID.
    pub fn leader(&self) -> i32 {
        self.leader
    }

    /// Returns the replica broker IDs.
    pub fn replicas(&self) -> &Vec<i32> {
        &self.replicas
    }

    /// Returns the in-sync replica broker IDs.
    pub fn isr(&self) -> &Vec<i32> {
        &self.isr
    }
}

/// Result of topic listing operation.
///
/// This corresponds to `org.apache.kafka.clients.admin.TopicListing` in Java.
#[derive(Debug, Clone)]
pub struct TopicListing {
    name: String,
    is_internal: bool,
}

impl TopicListing {
    /// Creates a new topic listing.
    pub fn new(name: impl Into<String>, is_internal: bool) -> Self {
        TopicListing {
            name: name.into(),
            is_internal,
        }
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether the topic is internal.
    pub fn is_internal(&self) -> bool {
        self.is_internal
    }
}

/// Error for topic operations.
///
/// This corresponds to various topic-related exceptions in Java.
#[derive(Debug, Clone)]
pub enum TopicError {
    /// Topic already exists.
    AlreadyExists { topic: String },
    /// Topic does not exist.
    DoesNotExist { topic: String },
    /// Invalid topic configuration.
    InvalidConfig { topic: String, message: String },
    /// Operation failed.
    Failed { topic: String, message: String },
}

impl std::fmt::Display for TopicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicError::AlreadyExists { topic } => {
                write!(f, "Topic '{}' already exists", topic)
            }
            TopicError::DoesNotExist { topic } => {
                write!(f, "Topic '{}' does not exist", topic)
            }
            TopicError::InvalidConfig { topic, message } => {
                write!(f, "Invalid config for topic '{}': {}", topic, message)
            }
            TopicError::Failed { topic, message } => {
                write!(f, "Operation failed for topic '{}': {}", topic, message)
            }
        }
    }
}

impl std::error::Error for TopicError {}

/// Error for consumer group operations.
///
/// This corresponds to various consumer group exceptions in Java.
#[derive(Debug, Clone)]
pub enum ConsumerGroupError {
    /// Consumer group does not exist.
    DoesNotExist { group_id: String },
    /// Consumer group is not empty (has active members).
    NotEmpty { group_id: String },
    /// Invalid offset.
    InvalidOffset { group_id: String, message: String },
    /// Operation failed.
    Failed { group_id: String, message: String },
}

impl std::fmt::Display for ConsumerGroupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumerGroupError::DoesNotExist { group_id } => {
                write!(f, "Consumer group '{}' does not exist", group_id)
            }
            ConsumerGroupError::NotEmpty { group_id } => {
                write!(f, "Consumer group '{}' is not empty", group_id)
            }
            ConsumerGroupError::InvalidOffset { group_id, message } => {
                write!(f, "Invalid offset for group '{}': {}", group_id, message)
            }
            ConsumerGroupError::Failed { group_id, message } => {
                write!(f, "Operation failed for group '{}': {}", group_id, message)
            }
        }
    }
}

impl std::error::Error for ConsumerGroupError {}

/// Internal topic information stored in the mock.
#[derive(Debug, Clone)]
struct InternalTopic {
    name: String,
    num_partitions: i32,
    replication_factor: i32,
    configs: HashMap<String, String>,
    is_internal: bool,
}

impl InternalTopic {
    fn from_new_topic(new_topic: &NewTopic, is_internal: bool) -> Self {
        InternalTopic {
            name: new_topic.name.clone(),
            num_partitions: new_topic.num_partitions,
            replication_factor: new_topic.replication_factor,
            configs: new_topic.configs.clone(),
            is_internal,
        }
    }

    fn to_metadata(&self) -> TopicMetadata {
        let partition_metadata: Vec<PartitionMetadata> = (0..self.num_partitions)
            .map(|p| PartitionMetadata {
                partition: p,
                leader: 0,
                replicas: vec![0, 1, 2],
                isr: vec![0, 1],
            })
            .collect();

        TopicMetadata {
            name: self.name.clone(),
            num_partitions: self.num_partitions,
            replication_factor: self.replication_factor,
            configs: self.configs.clone(),
            partition_metadata,
        }
    }

    fn to_listing(&self) -> TopicListing {
        TopicListing::new(&self.name, self.is_internal)
    }
}

/// Internal consumer group offset information.
type GroupOffsets = HashMap<TopicPartition, OffsetAndMetadata>;

/// Mock Admin client for testing purposes.
///
/// This provides an in-memory implementation of the Kafka Admin client
/// that supports topic management and consumer group offset operations.
/// All operations are synchronous and stored in memory.
///
/// Thread-safe via internal `Arc<Mutex<...>` wrapper.
///
/// This corresponds to `org.apache.kafka.clients.admin.KafkaAdminClient` in Java.
pub struct MockAdmin {
    topics: Arc<Mutex<HashMap<String, InternalTopic>>>,
    consumer_group_offsets: Arc<Mutex<HashMap<String, GroupOffsets>>>,
}

impl MockAdmin {
    /// Creates a new MockAdmin with empty state.
    pub fn new() -> Self {
        MockAdmin {
            topics: Arc::new(Mutex::new(HashMap::new())),
            consumer_group_offsets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Creates a new MockAdmin with pre-existing topics.
    pub fn with_topics(topics: Vec<NewTopic>) -> Self {
        let mut admin = MockAdmin::new();
        let result = admin.create_topics(topics);
        if result.failed_count() > 0 {
            panic!("Failed to create initial topics: {:?}", result.errors());
        }
        admin
    }

    // ===== Topic Management =====

    /// Creates new topics.
    ///
    /// This corresponds to `Admin.createTopics()` in Java.
    /// Returns a `CreateTopicsResult` containing the results for each topic.
    ///
    /// Behavior:
    /// - If a topic already exists, returns an error for that topic.
    /// - If validate_only is true, validates the topic config without creating.
    /// - Other topics in the batch are created successfully.
    pub fn create_topics(&self, topics: Vec<NewTopic>) -> CreateTopicsResult {
        self.create_topics_with_options(topics, CreateTopicsOptions::default())
    }

    /// Creates new topics with options.
    ///
    /// # Arguments
    /// * `topics` - The topics to create
    /// * `options` - Options controlling the operation (timeout, validate_only, etc.)
    pub fn create_topics_with_options(
        &self,
        topics: Vec<NewTopic>,
        options: CreateTopicsOptions,
    ) -> CreateTopicsResult {
        let mut result = CreateTopicsResult::new();
        let mut topics_state = self.topics.lock().unwrap();

        for new_topic in topics {
            let topic_name = new_topic.name.clone();

            if topics_state.contains_key(&topic_name) {
                result.add_error(
                    topic_name.clone(),
                    TopicError::AlreadyExists { topic: topic_name },
                );
            } else if options.validate_only {
                result.add_success(
                    topic_name.clone(),
                    TopicMetadata {
                        name: topic_name,
                        num_partitions: new_topic.num_partitions,
                        replication_factor: new_topic.replication_factor,
                        configs: new_topic.configs.clone(),
                        partition_metadata: Vec::new(),
                    },
                );
            } else {
                let internal_topic = InternalTopic::from_new_topic(&new_topic, false);
                let metadata = internal_topic.to_metadata();
                topics_state.insert(topic_name.clone(), internal_topic);
                result.add_success(topic_name, metadata);
            }
        }

        result
    }

    /// Deletes topics.
    ///
    /// This corresponds to `Admin.deleteTopics()` in Java.
    /// Returns a `DeleteTopicsResult` containing the results for each topic.
    ///
    /// Behavior:
    /// - If a topic does not exist, returns an error for that topic.
    /// - Other topics in the batch are deleted successfully.
    pub fn delete_topics(&self, topic_names: Vec<String>) -> DeleteTopicsResult {
        self.delete_topics_with_options(topic_names, DeleteTopicsOptions::default())
    }

    /// Deletes topics with options.
    pub fn delete_topics_with_options(
        &self,
        topic_names: Vec<String>,
        _options: DeleteTopicsOptions,
    ) -> DeleteTopicsResult {
        let mut result = DeleteTopicsResult::new();
        let mut topics_state = self.topics.lock().unwrap();

        for topic_name in topic_names {
            if topics_state.remove(&topic_name).is_some() {
                result.add_success(topic_name);
            } else {
                result.add_error(
                    topic_name.clone(),
                    TopicError::DoesNotExist { topic: topic_name },
                );
            }
        }

        result
    }

    /// Lists all topics.
    ///
    /// This corresponds to `Admin.listTopics()` in Java.
    /// Returns a `ListTopicsResult` containing all topic listings.
    pub fn list_topics(&self) -> ListTopicsResult {
        self.list_topics_with_options(ListTopicsOptions::default())
    }

    /// Lists topics with options.
    ///
    /// # Arguments
    /// * `options` - Options controlling the operation (include internal topics, etc.)
    pub fn list_topics_with_options(&self, options: ListTopicsOptions) -> ListTopicsResult {
        let topics_state = self.topics.lock().unwrap();
        let listings: Vec<TopicListing> = topics_state
            .values()
            .filter(|t| options.list_internal || !t.is_internal)
            .map(|t| t.to_listing())
            .collect();

        ListTopicsResult::new(listings)
    }

    /// Describes topics.
    ///
    /// This corresponds to `Admin.describeTopics()` in Java.
    /// Returns metadata for the requested topics.
    pub fn describe_topics(&self, topic_names: Vec<String>) -> DescribeTopicsResult {
        let mut result = DescribeTopicsResult::new();
        let topics_state = self.topics.lock().unwrap();

        for topic_name in topic_names {
            if let Some(topic) = topics_state.get(&topic_name) {
                result.add_success(topic_name, topic.to_metadata());
            } else {
                result.add_error(
                    topic_name.clone(),
                    TopicError::DoesNotExist { topic: topic_name },
                );
            }
        }

        result
    }

    // ===== Consumer Group Offset Management =====

    /// Lists consumer group offsets.
    ///
    /// This corresponds to `Admin.listConsumerGroupOffsets()` in Java.
    /// Returns the offsets for all partitions in the consumer group.
    ///
    /// If the consumer group does not exist, returns an empty map.
    pub fn list_consumer_group_offsets(
        &self,
        group_id: &str,
    ) -> HashMap<TopicPartition, OffsetAndMetadata> {
        self.list_consumer_group_offsets_with_options(
            group_id,
            ListConsumerGroupOffsetsOptions::default(),
        )
    }

    /// Lists consumer group offsets with options.
    pub fn list_consumer_group_offsets_with_options(
        &self,
        group_id: &str,
        _options: ListConsumerGroupOffsetsOptions,
    ) -> HashMap<TopicPartition, OffsetAndMetadata> {
        let offsets_state = self.consumer_group_offsets.lock().unwrap();
        offsets_state
            .get(group_id)
            .cloned()
            .unwrap_or_else(HashMap::new)
    }

    /// Alters consumer group offsets.
    ///
    /// This corresponds to `Admin.alterConsumerGroupOffsets()` in Java.
    /// Sets the offsets for the specified partitions in the consumer group.
    ///
    /// Behavior:
    /// - If the consumer group does not exist, creates it.
    /// - Offsets are merged with existing offsets (overwrites existing partitions).
    ///
    /// # Arguments
    /// * `group_id` - The consumer group ID
    /// * `offsets` - Map of TopicPartition to OffsetAndMetadata
    pub fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> AlterConsumerGroupOffsetsResult {
        self.alter_consumer_group_offsets_with_options(
            group_id,
            offsets,
            AlterConsumerGroupOffsetsOptions::default(),
        )
    }

    /// Alters consumer group offsets with options.
    pub fn alter_consumer_group_offsets_with_options(
        &self,
        group_id: &str,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        _options: AlterConsumerGroupOffsetsOptions,
    ) -> AlterConsumerGroupOffsetsResult {
        let mut result = AlterConsumerGroupOffsetsResult::new();
        let mut offsets_state = self.consumer_group_offsets.lock().unwrap();

        // Get or create the group offsets map
        let group_offsets = offsets_state
            .entry(group_id.to_string())
            .or_insert_with(HashMap::new);

        for (tp, offset_and_metadata) in offsets {
            group_offsets.insert(tp.clone(), offset_and_metadata);
            result.add_success(tp);
        }

        result
    }

    /// Deletes consumer group offsets.
    ///
    /// This corresponds to `Admin.deleteConsumerGroupOffsets()` in Java.
    /// Removes the offsets for the specified partitions in the consumer group.
    ///
    /// Behavior:
    /// - If the consumer group does not exist, returns an error.
    /// - If a partition does not have an offset, returns an error for that partition.
    pub fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: HashSet<TopicPartition>,
    ) -> DeleteConsumerGroupOffsetsResult {
        let mut result = DeleteConsumerGroupOffsetsResult::new();
        let mut offsets_state = self.consumer_group_offsets.lock().unwrap();

        if !offsets_state.contains_key(group_id) {
            result.set_error(ConsumerGroupError::DoesNotExist {
                group_id: group_id.to_string(),
            });
            return result;
        }

        let group_offsets = offsets_state.get_mut(group_id).unwrap();

        for tp in partitions {
            if group_offsets.remove(&tp).is_some() {
                result.add_success(tp);
            } else {
                result.add_error(
                    tp.clone(),
                    ConsumerGroupError::InvalidOffset {
                        group_id: group_id.to_string(),
                        message: format!("No offset for partition {}", tp),
                    },
                );
            }
        }

        result
    }

    /// Deletes consumer groups.
    ///
    /// This corresponds to `Admin.deleteConsumerGroups()` in Java.
    /// Removes the consumer groups and all their offsets.
    ///
    /// Behavior:
    /// - If a consumer group does not exist, returns an error for that group.
    pub fn delete_consumer_groups(&self, group_ids: HashSet<String>) -> DeleteConsumerGroupsResult {
        let mut result = DeleteConsumerGroupsResult::new();
        let mut offsets_state = self.consumer_group_offsets.lock().unwrap();

        for group_id in group_ids {
            if offsets_state.remove(&group_id).is_some() {
                result.add_success(group_id);
            } else {
                result.add_error(
                    group_id.clone(),
                    ConsumerGroupError::DoesNotExist { group_id },
                );
            }
        }

        result
    }

    // ===== Utility methods for testing =====

    /// Clears all state (topics and consumer group offsets).
    pub fn clear(&self) {
        self.topics.lock().unwrap().clear();
        self.consumer_group_offsets.lock().unwrap().clear();
    }

    /// Returns the number of topics.
    pub fn topic_count(&self) -> usize {
        self.topics.lock().unwrap().len()
    }

    /// Returns whether a topic exists.
    pub fn topic_exists(&self, topic_name: &str) -> bool {
        self.topics.lock().unwrap().contains_key(topic_name)
    }

    /// Returns the number of consumer groups.
    pub fn consumer_group_count(&self) -> usize {
        self.consumer_group_offsets.lock().unwrap().len()
    }

    /// Returns whether a consumer group exists.
    pub fn consumer_group_exists(&self, group_id: &str) -> bool {
        self.consumer_group_offsets
            .lock()
            .unwrap()
            .contains_key(group_id)
    }
}

impl Default for MockAdmin {
    fn default() -> Self {
        MockAdmin::new()
    }
}

// ===== AdminClient trait implementation =====

impl AdminClient for MockAdmin {
    fn list_consumer_group_offsets(&self, group_id: &str) -> HashMap<String, serde_json::Value> {
        let offsets = self.list_consumer_group_offsets(group_id);
        offsets
            .into_iter()
            .map(|(tp, om)| {
                let key = format!("{}:{}", tp.topic(), tp.partition());
                let value = serde_json::json!({
                    "offset": om.offset(),
                    "metadata": om.metadata(),
                    "leader_epoch": om.leader_epoch()
                });
                (key, value)
            })
            .collect()
    }

    fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: HashMap<String, serde_json::Value>,
    ) -> Result<(), ConnectError> {
        let parsed_offsets: HashMap<TopicPartition, OffsetAndMetadata> = offsets
            .into_iter()
            .filter_map(|(key, value)| {
                let parts: Vec<&str> = key.split(':').collect();
                if parts.len() != 2 {
                    return None;
                }
                let topic = parts[0];
                let partition: i32 = parts[1].parse().ok()?;
                let tp = TopicPartition::new(topic, partition);
                let offset = value.get("offset")?.as_i64()?;
                let om = OffsetAndMetadata::new(offset);
                Some((tp, om))
            })
            .collect();

        let result = self.alter_consumer_group_offsets(group_id, parsed_offsets);
        if result.has_errors() {
            Err(ConnectError::General {
                message: "Failed to alter some consumer group offsets".to_string(),
            })
        } else {
            Ok(())
        }
    }

    fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: HashSet<String>,
    ) -> Result<(), ConnectError> {
        let parsed_partitions: HashSet<TopicPartition> = partitions
            .into_iter()
            .filter_map(|key| {
                let parts: Vec<&str> = key.split(':').collect();
                if parts.len() != 2 {
                    return None;
                }
                let topic = parts[0];
                let partition: i32 = parts[1].parse().ok()?;
                Some(TopicPartition::new(topic, partition))
            })
            .collect();

        let result = self.delete_consumer_group_offsets(group_id, parsed_partitions);
        if result.has_errors() {
            Err(ConnectError::General {
                message: "Failed to delete some consumer group offsets".to_string(),
            })
        } else {
            Ok(())
        }
    }

    fn delete_consumer_groups(&self, group_ids: HashSet<String>) -> Result<(), ConnectError> {
        let result = self.delete_consumer_groups(group_ids);
        if result.has_errors() {
            Err(ConnectError::General {
                message: "Failed to delete some consumer groups".to_string(),
            })
        } else {
            Ok(())
        }
    }

    fn fence_producers(&self, _transactional_ids: Vec<String>) -> Result<(), ConnectError> {
        Ok(())
    }
}

// ===== Options Types =====

/// Options for createTopics operation.
#[derive(Debug, Clone)]
pub struct CreateTopicsOptions {
    timeout_ms: i32,
    validate_only: bool,
}

impl CreateTopicsOptions {
    pub fn new() -> Self {
        CreateTopicsOptions {
            timeout_ms: 60000,
            validate_only: false,
        }
    }

    pub fn timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    pub fn timeout_ms_value(&self) -> i32 {
        self.timeout_ms
    }

    pub fn validate_only_value(&self) -> bool {
        self.validate_only
    }
}

impl Default for CreateTopicsOptions {
    fn default() -> Self {
        CreateTopicsOptions::new()
    }
}

/// Options for deleteTopics operation.
#[derive(Debug, Clone)]
pub struct DeleteTopicsOptions {
    timeout_ms: i32,
}

impl DeleteTopicsOptions {
    pub fn new() -> Self {
        DeleteTopicsOptions { timeout_ms: 60000 }
    }

    pub fn timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn timeout_ms_value(&self) -> i32 {
        self.timeout_ms
    }
}

impl Default for DeleteTopicsOptions {
    fn default() -> Self {
        DeleteTopicsOptions::new()
    }
}

/// Options for listTopics operation.
#[derive(Debug, Clone)]
pub struct ListTopicsOptions {
    timeout_ms: i32,
    list_internal: bool,
}

impl ListTopicsOptions {
    pub fn new() -> Self {
        ListTopicsOptions {
            timeout_ms: 60000,
            list_internal: false,
        }
    }

    pub fn timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn list_internal(mut self, list_internal: bool) -> Self {
        self.list_internal = list_internal;
        self
    }

    pub fn timeout_ms_value(&self) -> i32 {
        self.timeout_ms
    }

    pub fn list_internal_value(&self) -> bool {
        self.list_internal
    }
}

impl Default for ListTopicsOptions {
    fn default() -> Self {
        ListTopicsOptions::new()
    }
}

/// Options for listConsumerGroupOffsets operation.
#[derive(Debug, Clone)]
pub struct ListConsumerGroupOffsetsOptions {
    timeout_ms: i32,
}

impl ListConsumerGroupOffsetsOptions {
    pub fn new() -> Self {
        ListConsumerGroupOffsetsOptions { timeout_ms: 60000 }
    }

    pub fn timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn timeout_ms_value(&self) -> i32 {
        self.timeout_ms
    }
}

impl Default for ListConsumerGroupOffsetsOptions {
    fn default() -> Self {
        ListConsumerGroupOffsetsOptions::new()
    }
}

/// Options for alterConsumerGroupOffsets operation.
#[derive(Debug, Clone)]
pub struct AlterConsumerGroupOffsetsOptions {
    timeout_ms: i32,
}

impl AlterConsumerGroupOffsetsOptions {
    pub fn new() -> Self {
        AlterConsumerGroupOffsetsOptions { timeout_ms: 60000 }
    }

    pub fn timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn timeout_ms_value(&self) -> i32 {
        self.timeout_ms
    }
}

impl Default for AlterConsumerGroupOffsetsOptions {
    fn default() -> Self {
        AlterConsumerGroupOffsetsOptions::new()
    }
}

// ===== Result Types =====

/// Result of createTopics operation.
#[derive(Debug)]
pub struct CreateTopicsResult {
    successes: HashMap<String, TopicMetadata>,
    errors: HashMap<String, TopicError>,
}

impl CreateTopicsResult {
    fn new() -> Self {
        CreateTopicsResult {
            successes: HashMap::new(),
            errors: HashMap::new(),
        }
    }

    fn add_success(&mut self, topic_name: String, metadata: TopicMetadata) {
        self.successes.insert(topic_name, metadata);
    }

    fn add_error(&mut self, topic_name: String, error: TopicError) {
        self.errors.insert(topic_name, error);
    }

    /// Returns the metadata for a successfully created topic.
    pub fn metadata(&self, topic_name: &str) -> Option<&TopicMetadata> {
        self.successes.get(topic_name)
    }

    /// Returns the error for a failed topic creation.
    pub fn error(&self, topic_name: &str) -> Option<&TopicError> {
        self.errors.get(topic_name)
    }

    /// Returns all successful topic creations.
    pub fn successes(&self) -> &HashMap<String, TopicMetadata> {
        &self.successes
    }

    /// Returns all failed topic creations.
    pub fn errors(&self) -> &HashMap<String, TopicError> {
        &self.errors
    }

    /// Returns the number of successful creations.
    pub fn success_count(&self) -> usize {
        self.successes.len()
    }

    /// Returns the number of failed creations.
    pub fn failed_count(&self) -> usize {
        self.errors.len()
    }

    /// Returns whether all topics were created successfully.
    pub fn all_success(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns whether any topics failed.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

/// Result of deleteTopics operation.
#[derive(Debug)]
pub struct DeleteTopicsResult {
    successes: HashSet<String>,
    errors: HashMap<String, TopicError>,
}

impl DeleteTopicsResult {
    fn new() -> Self {
        DeleteTopicsResult {
            successes: HashSet::new(),
            errors: HashMap::new(),
        }
    }

    fn add_success(&mut self, topic_name: String) {
        self.successes.insert(topic_name);
    }

    fn add_error(&mut self, topic_name: String, error: TopicError) {
        self.errors.insert(topic_name, error);
    }

    /// Returns whether a topic was deleted successfully.
    pub fn success(&self, topic_name: &str) -> bool {
        self.successes.contains(topic_name)
    }

    /// Returns the error for a failed topic deletion.
    pub fn error(&self, topic_name: &str) -> Option<&TopicError> {
        self.errors.get(topic_name)
    }

    /// Returns all successfully deleted topics.
    pub fn successes(&self) -> &HashSet<String> {
        &self.successes
    }

    /// Returns all failed topic deletions.
    pub fn errors(&self) -> &HashMap<String, TopicError> {
        &self.errors
    }

    /// Returns the number of successful deletions.
    pub fn success_count(&self) -> usize {
        self.successes.len()
    }

    /// Returns the number of failed deletions.
    pub fn failed_count(&self) -> usize {
        self.errors.len()
    }

    /// Returns whether all topics were deleted successfully.
    pub fn all_success(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns whether any topics failed.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

/// Result of listTopics operation.
#[derive(Debug)]
pub struct ListTopicsResult {
    listings: Vec<TopicListing>,
}

impl ListTopicsResult {
    fn new(listings: Vec<TopicListing>) -> Self {
        ListTopicsResult { listings }
    }

    /// Returns all topic listings.
    pub fn listings(&self) -> &Vec<TopicListing> {
        &self.listings
    }

    /// Returns topic names.
    pub fn names(&self) -> Vec<String> {
        self.listings.iter().map(|l| l.name.clone()).collect()
    }

    /// Returns the number of topics.
    pub fn count(&self) -> usize {
        self.listings.len()
    }
}

/// Result of describeTopics operation.
#[derive(Debug)]
pub struct DescribeTopicsResult {
    successes: HashMap<String, TopicMetadata>,
    errors: HashMap<String, TopicError>,
}

impl DescribeTopicsResult {
    fn new() -> Self {
        DescribeTopicsResult {
            successes: HashMap::new(),
            errors: HashMap::new(),
        }
    }

    fn add_success(&mut self, topic_name: String, metadata: TopicMetadata) {
        self.successes.insert(topic_name, metadata);
    }

    fn add_error(&mut self, topic_name: String, error: TopicError) {
        self.errors.insert(topic_name, error);
    }

    /// Returns the metadata for a topic.
    pub fn metadata(&self, topic_name: &str) -> Option<&TopicMetadata> {
        self.successes.get(topic_name)
    }

    /// Returns the error for a topic.
    pub fn error(&self, topic_name: &str) -> Option<&TopicError> {
        self.errors.get(topic_name)
    }

    /// Returns all successful topic descriptions.
    pub fn successes(&self) -> &HashMap<String, TopicMetadata> {
        &self.successes
    }

    /// Returns all failed topic descriptions.
    pub fn errors(&self) -> &HashMap<String, TopicError> {
        &self.errors
    }
}

/// Result of alterConsumerGroupOffsets operation.
#[derive(Debug)]
pub struct AlterConsumerGroupOffsetsResult {
    successes: HashSet<TopicPartition>,
    errors: HashMap<TopicPartition, ConsumerGroupError>,
}

impl AlterConsumerGroupOffsetsResult {
    fn new() -> Self {
        AlterConsumerGroupOffsetsResult {
            successes: HashSet::new(),
            errors: HashMap::new(),
        }
    }

    fn add_success(&mut self, tp: TopicPartition) {
        self.successes.insert(tp);
    }

    fn add_error(&mut self, tp: TopicPartition, error: ConsumerGroupError) {
        self.errors.insert(tp, error);
    }

    /// Returns whether a partition offset was altered successfully.
    pub fn success(&self, tp: &TopicPartition) -> bool {
        self.successes.contains(tp)
    }

    /// Returns the error for a failed partition.
    pub fn error(&self, tp: &TopicPartition) -> Option<&ConsumerGroupError> {
        self.errors.get(tp)
    }

    /// Returns all successfully altered partitions.
    pub fn successes(&self) -> &HashSet<TopicPartition> {
        &self.successes
    }

    /// Returns all failed partitions.
    pub fn errors(&self) -> &HashMap<TopicPartition, ConsumerGroupError> {
        &self.errors
    }

    /// Returns whether all partitions were altered successfully.
    pub fn all_success(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns whether any partitions failed.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

/// Result of deleteConsumerGroupOffsets operation.
#[derive(Debug)]
pub struct DeleteConsumerGroupOffsetsResult {
    successes: HashSet<TopicPartition>,
    errors: HashMap<TopicPartition, ConsumerGroupError>,
    group_error: Option<ConsumerGroupError>,
}

impl DeleteConsumerGroupOffsetsResult {
    fn new() -> Self {
        DeleteConsumerGroupOffsetsResult {
            successes: HashSet::new(),
            errors: HashMap::new(),
            group_error: None,
        }
    }

    fn add_success(&mut self, tp: TopicPartition) {
        self.successes.insert(tp);
    }

    fn add_error(&mut self, tp: TopicPartition, error: ConsumerGroupError) {
        self.errors.insert(tp, error);
    }

    fn set_error(&mut self, error: ConsumerGroupError) {
        self.group_error = Some(error);
    }

    /// Returns whether a partition offset was deleted successfully.
    pub fn success(&self, tp: &TopicPartition) -> bool {
        self.successes.contains(tp)
    }

    /// Returns the error for a failed partition.
    pub fn partition_error(&self, tp: &TopicPartition) -> Option<&ConsumerGroupError> {
        self.errors.get(tp)
    }

    /// Returns the group-level error (if group does not exist).
    pub fn group_error(&self) -> Option<&ConsumerGroupError> {
        self.group_error.as_ref()
    }

    /// Returns all successfully deleted partitions.
    pub fn successes(&self) -> &HashSet<TopicPartition> {
        &self.successes
    }

    /// Returns all failed partitions.
    pub fn errors(&self) -> &HashMap<TopicPartition, ConsumerGroupError> {
        &self.errors
    }

    /// Returns whether any errors occurred.
    pub fn has_errors(&self) -> bool {
        self.group_error.is_some() || !self.errors.is_empty()
    }
}

/// Result of deleteConsumerGroups operation.
#[derive(Debug)]
pub struct DeleteConsumerGroupsResult {
    successes: HashSet<String>,
    errors: HashMap<String, ConsumerGroupError>,
}

impl DeleteConsumerGroupsResult {
    fn new() -> Self {
        DeleteConsumerGroupsResult {
            successes: HashSet::new(),
            errors: HashMap::new(),
        }
    }

    fn add_success(&mut self, group_id: String) {
        self.successes.insert(group_id);
    }

    fn add_error(&mut self, group_id: String, error: ConsumerGroupError) {
        self.errors.insert(group_id, error);
    }

    /// Returns whether a group was deleted successfully.
    pub fn success(&self, group_id: &str) -> bool {
        self.successes.contains(group_id)
    }

    /// Returns the error for a failed group.
    pub fn error(&self, group_id: &str) -> Option<&ConsumerGroupError> {
        self.errors.get(group_id)
    }

    /// Returns all successfully deleted groups.
    pub fn successes(&self) -> &HashSet<String> {
        &self.successes
    }

    /// Returns all failed groups.
    pub fn errors(&self) -> &HashMap<String, ConsumerGroupError> {
        &self.errors
    }

    /// Returns whether any errors occurred.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_topic() {
        let topic = NewTopic::new("test-topic", 3, 2);
        assert_eq!(topic.name(), "test-topic");
        assert_eq!(topic.num_partitions(), 3);
        assert_eq!(topic.replication_factor(), 2);
        assert!(topic.configs().is_empty());
    }

    #[test]
    fn test_new_topic_with_config() {
        let topic = NewTopic::new("test-topic", 3, 2)
            .config("retention.ms", "86400000")
            .config("cleanup.policy", "compact");
        assert_eq!(topic.configs().len(), 2);
        assert_eq!(
            topic.configs().get("retention.ms"),
            Some(&"86400000".to_string())
        );
    }

    #[test]
    fn test_create_topics() {
        let admin = MockAdmin::new();
        let topics = vec![NewTopic::new("topic1", 3, 2), NewTopic::new("topic2", 5, 3)];
        let result = admin.create_topics(topics);

        assert!(result.all_success());
        assert_eq!(result.success_count(), 2);
        assert_eq!(admin.topic_count(), 2);
        assert!(admin.topic_exists("topic1"));
        assert!(admin.topic_exists("topic2"));
    }

    #[test]
    fn test_create_duplicate_topic() {
        let admin = MockAdmin::new();
        let topics = vec![NewTopic::new("topic1", 3, 2), NewTopic::new("topic1", 5, 3)];
        let result = admin.create_topics(topics);

        assert!(!result.all_success());
        assert_eq!(result.success_count(), 1);
        assert_eq!(result.failed_count(), 1);
        assert!(matches!(
            result.error("topic1"),
            Some(TopicError::AlreadyExists { .. })
        ));
    }

    #[test]
    fn test_create_topics_validate_only() {
        let admin = MockAdmin::new();
        let topics = vec![NewTopic::new("topic1", 3, 2)];
        let options = CreateTopicsOptions::new().validate_only(true);
        let result = admin.create_topics_with_options(topics, options);

        assert!(result.all_success());
        assert_eq!(admin.topic_count(), 0);
    }

    #[test]
    fn test_delete_topics() {
        let admin = MockAdmin::new();
        admin.create_topics(vec![NewTopic::new("topic1", 3, 2)]);

        let result = admin.delete_topics(vec!["topic1".to_string()]);
        assert!(result.all_success());
        assert_eq!(admin.topic_count(), 0);
        assert!(!admin.topic_exists("topic1"));
    }

    #[test]
    fn test_delete_nonexistent_topic() {
        let admin = MockAdmin::new();
        let result = admin.delete_topics(vec!["nonexistent".to_string()]);

        assert!(!result.all_success());
        assert_eq!(result.failed_count(), 1);
        assert!(matches!(
            result.error("nonexistent"),
            Some(TopicError::DoesNotExist { .. })
        ));
    }

    #[test]
    fn test_list_topics() {
        let admin = MockAdmin::new();
        admin.create_topics(vec![
            NewTopic::new("topic1", 3, 2),
            NewTopic::new("topic2", 5, 3),
        ]);

        let result = admin.list_topics();
        assert_eq!(result.count(), 2);
        let names = result.names();
        assert!(names.contains(&"topic1".to_string()));
        assert!(names.contains(&"topic2".to_string()));
    }

    #[test]
    fn test_describe_topics() {
        let admin = MockAdmin::new();
        admin.create_topics(vec![NewTopic::new("topic1", 3, 2)]);

        let result = admin.describe_topics(vec!["topic1".to_string()]);
        assert!(result.metadata("topic1").is_some());
        let metadata = result.metadata("topic1").unwrap();
        assert_eq!(metadata.num_partitions(), 3);
        assert_eq!(metadata.partition_metadata().len(), 3);
    }

    #[test]
    fn test_alter_consumer_group_offsets() {
        let admin = MockAdmin::new();
        let offsets = HashMap::from([
            (
                TopicPartition::new("topic1", 0),
                OffsetAndMetadata::new(100),
            ),
            (
                TopicPartition::new("topic1", 1),
                OffsetAndMetadata::new(200),
            ),
        ]);

        let result = admin.alter_consumer_group_offsets("group1", offsets);
        assert!(result.all_success());
        assert!(admin.consumer_group_exists("group1"));

        let stored_offsets = admin.list_consumer_group_offsets("group1");
        assert_eq!(stored_offsets.len(), 2);
        assert_eq!(
            stored_offsets
                .get(&TopicPartition::new("topic1", 0))
                .unwrap()
                .offset(),
            100
        );
    }

    #[test]
    fn test_list_consumer_group_offsets_nonexistent() {
        let admin = MockAdmin::new();
        let offsets = admin.list_consumer_group_offsets("nonexistent-group");
        assert!(offsets.is_empty());
    }

    #[test]
    fn test_delete_consumer_group_offsets() {
        let admin = MockAdmin::new();
        admin.alter_consumer_group_offsets(
            "group1",
            HashMap::from([(
                TopicPartition::new("topic1", 0),
                OffsetAndMetadata::new(100),
            )]),
        );

        let result = admin.delete_consumer_group_offsets(
            "group1",
            HashSet::from([TopicPartition::new("topic1", 0)]),
        );
        assert!(!result.has_errors());

        let offsets = admin.list_consumer_group_offsets("group1");
        assert!(offsets.is_empty());
    }

    #[test]
    fn test_delete_consumer_group_offsets_nonexistent_group() {
        let admin = MockAdmin::new();
        let result = admin.delete_consumer_group_offsets(
            "nonexistent-group",
            HashSet::from([TopicPartition::new("topic1", 0)]),
        );

        assert!(result.has_errors());
        assert!(matches!(
            result.group_error(),
            Some(ConsumerGroupError::DoesNotExist { .. })
        ));
    }

    #[test]
    fn test_delete_consumer_groups() {
        let admin = MockAdmin::new();
        admin.alter_consumer_group_offsets(
            "group1",
            HashMap::from([(
                TopicPartition::new("topic1", 0),
                OffsetAndMetadata::new(100),
            )]),
        );

        let result = admin.delete_consumer_groups(HashSet::from(["group1".to_string()]));
        assert!(!result.has_errors());
        assert!(!admin.consumer_group_exists("group1"));
    }

    #[test]
    fn test_delete_nonexistent_consumer_group() {
        let admin = MockAdmin::new();
        let result = admin.delete_consumer_groups(HashSet::from(["nonexistent".to_string()]));

        assert!(result.has_errors());
        assert!(matches!(
            result.error("nonexistent"),
            Some(ConsumerGroupError::DoesNotExist { .. })
        ));
    }

    #[test]
    fn test_clear() {
        let admin = MockAdmin::new();
        admin.create_topics(vec![NewTopic::new("topic1", 3, 2)]);
        admin.alter_consumer_group_offsets(
            "group1",
            HashMap::from([(
                TopicPartition::new("topic1", 0),
                OffsetAndMetadata::new(100),
            )]),
        );

        admin.clear();
        assert_eq!(admin.topic_count(), 0);
        assert_eq!(admin.consumer_group_count(), 0);
    }

    #[test]
    fn test_admin_client_trait() {
        let admin = MockAdmin::new();

        // Test trait implementation
        let trait_admin: &dyn AdminClient = &admin;

        // alter_consumer_group_offsets via trait
        let offsets = HashMap::from([("topic1:0".to_string(), serde_json::json!({"offset": 100}))]);
        trait_admin
            .alter_consumer_group_offsets("group1", offsets)
            .unwrap();

        // list_consumer_group_offsets via trait
        let result = trait_admin.list_consumer_group_offsets("group1");
        assert!(result.contains_key("topic1:0"));

        // delete_consumer_group_offsets via trait
        trait_admin
            .delete_consumer_group_offsets("group1", HashSet::from(["topic1:0".to_string()]))
            .unwrap();

        // delete_consumer_groups via trait
        trait_admin
            .delete_consumer_groups(HashSet::from(["group1".to_string()]))
            .unwrap();
    }

    #[test]
    fn test_with_topics() {
        let topics = vec![NewTopic::new("topic1", 3, 2), NewTopic::new("topic2", 5, 3)];
        let admin = MockAdmin::with_topics(topics);

        assert_eq!(admin.topic_count(), 2);
        assert!(admin.topic_exists("topic1"));
        assert!(admin.topic_exists("topic2"));
    }
}
