//! Kafka Admin Client Trait
//!
//! Defines core interface for Kafka admin operations.

use crate::consumer::TopicPartition;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
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

/// Core Kafka Admin trait (async version)
pub trait KafkaAdmin: Send + Sync {
    /// Creates new topics
    fn create_topics(
        &self,
        new_topics: Vec<NewTopic>,
    ) -> Pin<Box<dyn Future<Output = Result<HashMap<String, String>, String>> + Send>>;

    /// Deletes topics
    fn delete_topics(
        &self,
        topics: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<HashMap<String, String>, String>> + Send>>;

    /// Describes topics
    fn describe_topics(
        &self,
        topic_names: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<HashMap<String, TopicDescription>, String>> + Send>>;

    /// Lists all topics
    fn list_topics(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>, String>> + Send>>;

    /// Describes cluster
    fn describe_cluster(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ClusterDescription, String>> + Send>>;

    /// Lists consumer groups
    fn list_consumer_groups(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, String>> + Send>>;

    /// Describes a consumer group
    fn describe_consumer_group(
        &self,
        group_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<ConsumerGroupDescription, String>> + Send>>;

    /// Deletes a consumer group
    fn delete_consumer_group(
        &self,
        group_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

    /// Closes admin client
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    /// Closes admin client with a timeout
    fn close_with_timeout(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
}

/// Sync Kafka Admin trait (synchronous version for MirrorClient)
///
/// This trait provides synchronous methods for MirrorClient which doesn't need async operations.
pub trait KafkaAdminSync: Send + Sync {
    /// Lists all topics
    fn list_topics_sync(&self) -> Result<Vec<String>, Box<dyn Error>>;

    /// Closes admin client
    fn close_sync(&mut self) -> Result<(), Box<dyn Error>>;
}

/// ForwardingAdmin trait for forwarding admin operations
///
/// This trait is used by MirrorClientConfig to create forwarding admin instances.
pub trait ForwardingAdmin: Send + Sync {}

/// Mock ForwardingAdmin implementation
///
/// Provides a mock implementation of ForwardingAdmin for testing purposes.
#[derive(Debug, Clone)]
pub struct MockForwardingAdmin {
    config: HashMap<String, String>,
}

impl MockForwardingAdmin {
    /// Creates a new mock forwarding admin
    pub fn new(config: HashMap<String, String>) -> Self {
        MockForwardingAdmin { config }
    }
}

impl ForwardingAdmin for MockForwardingAdmin {}

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

/// Mock Kafka Admin Client (sync version for MirrorClient)
///
/// Provides a mock implementation of KafkaAdminSync for testing purposes.
#[derive(Debug, Clone)]
pub struct MockKafkaAdminSync {
    topics: std::collections::HashSet<String>,
}

impl MockKafkaAdminSync {
    /// Creates a new mock admin client
    pub fn new() -> Self {
        MockKafkaAdminSync {
            topics: std::collections::HashSet::new(),
        }
    }

    /// Creates a mock admin client with predefined topics
    pub fn with_topics(topics: Vec<String>) -> Self {
        MockKafkaAdminSync {
            topics: topics.into_iter().collect(),
        }
    }

    /// Adds a topic to the mock
    pub fn add_topic(&mut self, topic: String) {
        self.topics.insert(topic);
    }

    /// Removes a topic from the mock
    pub fn remove_topic(&mut self, topic: String) {
        self.topics.remove(&topic);
    }
}

impl KafkaAdminSync for MockKafkaAdminSync {
    fn list_topics_sync(&self) -> Result<Vec<String>, Box<dyn Error>> {
        Ok(self.topics.iter().cloned().collect())
    }

    fn close_sync(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

/// Simple Future implementation for testing
struct SimpleFuture<T> {
    value: Option<T>,
}

impl<T: Unpin> Future for SimpleFuture<T> {
    type Output = T;

    fn poll(
        self: Pin<&mut Self>,
        mut _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // 使用 get_mut() 获取内部可变引用
        let inner = self.get_mut();
        if let Some(value) = inner.value.take() {
            std::task::Poll::Ready(value)
        } else {
            std::task::Poll::Pending
        }
    }
}

/// Mock Kafka Admin Client (async version)
///
/// Provides a mock implementation of KafkaAdmin for testing purposes.
#[derive(Debug, Clone)]
pub struct MockKafkaAdmin {
    topics: std::collections::HashSet<String>,
}

impl MockKafkaAdmin {
    /// Creates a new mock admin client
    pub fn new() -> Self {
        MockKafkaAdmin {
            topics: std::collections::HashSet::new(),
        }
    }

    /// Creates a mock admin client with predefined topics
    pub fn with_topics(topics: Vec<String>) -> Self {
        MockKafkaAdmin {
            topics: topics.into_iter().collect(),
        }
    }

    /// Adds a topic to the mock
    pub fn add_topic(&mut self, topic: String) {
        self.topics.insert(topic);
    }

    /// Removes a topic from the mock
    pub fn remove_topic(&mut self, topic: String) {
        self.topics.remove(&topic);
    }
}

impl KafkaAdmin for MockKafkaAdmin {
    fn create_topics(
        &self,
        _new_topics: Vec<NewTopic>,
    ) -> Pin<Box<dyn Future<Output = Result<HashMap<String, String>, String>> + Send>> {
        Box::pin(SimpleFuture {
            value: Some(Err("Not implemented".to_string())),
        })
    }

    fn delete_topics(
        &self,
        _topics: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<HashMap<String, String>, String>> + Send>> {
        Box::pin(SimpleFuture {
            value: Some(Err("Not implemented".to_string())),
        })
    }

    fn describe_topics(
        &self,
        _topic_names: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<HashMap<String, TopicDescription>, String>> + Send>>
    {
        Box::pin(SimpleFuture {
            value: Some(Err("Not implemented".to_string())),
        })
    }

    fn list_topics(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>, String>> + Send>> {
        Box::pin(SimpleFuture {
            value: Some(Err("Not implemented".to_string())),
        })
    }

    fn describe_cluster(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ClusterDescription, String>> + Send>> {
        Box::pin(SimpleFuture {
            value: Some(Err("Not implemented".to_string())),
        })
    }

    fn list_consumer_groups(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, String>> + Send>> {
        Box::pin(SimpleFuture {
            value: Some(Err("Not implemented".to_string())),
        })
    }

    fn describe_consumer_group(
        &self,
        _group_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<ConsumerGroupDescription, String>> + Send>> {
        Box::pin(SimpleFuture {
            value: Some(Err("Not implemented".to_string())),
        })
    }

    fn delete_consumer_group(
        &self,
        _group_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        Box::pin(SimpleFuture {
            value: Some(Err("Not implemented".to_string())),
        })
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(SimpleFuture { value: Some(()) })
    }

    fn close_with_timeout(
        &self,
        _timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        Box::pin(SimpleFuture {
            value: Some(Err("Not implemented".to_string())),
        })
    }
}
