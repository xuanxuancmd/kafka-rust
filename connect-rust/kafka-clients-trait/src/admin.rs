//! Kafka Admin Client Trait
//!
//! Defines the core interface for Kafka admin operations.

use crate::consumer::TopicPartition;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

/// Represents a new topic configuration
#[derive(Debug, Clone)]
pub struct NewTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub configs: HashMap<String, String>,
}

/// Represents topic description
#[derive(Debug, Clone)]
pub struct TopicDescription {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<PartitionMetadata>,
    pub authorized_operations: Vec<String>,
}

/// Represents partition metadata
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub topic: String,
    pub partition: i32,
    pub leader: Option<Node>,
    pub replicas: Vec<Node>,
    pub in_sync_replicas: Vec<Node>,
    pub offline_replicas: Vec<Node>,
}

/// Represents a cluster node
#[derive(Debug, Clone)]
pub struct Node {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

/// Represents cluster description
#[derive(Debug, Clone)]
pub struct ClusterDescription {
    pub cluster_id: String,
    pub controller: Option<Node>,
    pub nodes: Vec<Node>,
    pub authorized_operations: Vec<String>,
}

/// Core Kafka Admin trait
pub trait KafkaAdmin: Send + Sync {
    /// Creates new topics
    fn create_topics(
        &self,
        new_topics: Vec<NewTopic>,
    ) -> impl Future<Output = Result<HashMap<String, String>, String>> + Send;

    /// Deletes topics
    fn delete_topics(
        &self,
        topics: Vec<String>,
    ) -> impl Future<Output = Result<HashMap<String, String>, String>> + Send;

    /// Describes topics
    fn describe_topics(
        &self,
        topic_names: Vec<String>,
    ) -> impl Future<Output = Result<HashMap<String, TopicDescription>, String>> + Send;

    /// Lists all topics
    fn list_topics(&self) -> impl Future<Output = Result<Vec<String>, String>> + Send;

    /// Describes the cluster
    fn describe_cluster(&self) -> impl Future<Output = Result<ClusterDescription, String>> + Send;

    /// Lists consumer groups
    fn list_consumer_groups(&self) -> impl Future<Output = Result<Vec<String>, String>> + Send;

    /// Describes a consumer group
    fn describe_consumer_group(
        &self,
        group_id: &str,
    ) -> impl Future<Output = Result<ConsumerGroupDescription, String>> + Send;

    /// Deletes a consumer group
    fn delete_consumer_group(
        &self,
        group_id: &str,
    ) -> impl Future<Output = Result<(), String>> + Send;

    /// Closes the admin client
    fn close(&self) -> impl Future<Output = ()> + Send;

    /// Closes the admin client with a timeout
    fn close_with_timeout(&self, timeout: Duration) -> impl Future<Output = ()> + Send;
}

/// Represents consumer group description
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescription {
    pub group_id: String,
    pub is_simple_consumer_group: bool,
    pub state: String,
    pub members: Vec<MemberDescription>,
}

/// Represents member description
#[derive(Debug, Clone)]
pub struct MemberDescription {
    pub consumer_id: String,
    pub host: String,
    pub client_id: String,
    pub assignments: Vec<TopicPartition>,
}
