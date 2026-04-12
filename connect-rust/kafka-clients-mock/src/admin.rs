//! Mock Admin Client Implementation
//!
//! This module provides a mock implementation of KafkaAdmin for testing purposes.

use kafka_clients_trait::admin::{
    KafkaAdmin, NewTopic, TopicDescription, PartitionMetadata, Node, ClusterDescription,
    ConsumerGroupDescription, MemberDescription,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Mock admin client for testing Kafka admin functionality.
pub struct MockAdminClient {
    /// In-memory storage for topics
    topics: Arc<Mutex<HashMap<String, MockTopicInfo>>>,
    /// Whether the client is closed
    closed: Arc<Mutex<bool>>,
}

/// Mock topic information
#[derive(Debug, Clone)]
pub struct MockTopicInfo {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub config: HashMap<String, String>,
}

impl MockAdminClient {
    /// Create a new mock admin client
    pub fn new() -> Self {
        MockAdminClient {
            topics: Arc::new(Mutex::new(HashMap::new())),
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Add a topic to the mock
    pub fn add_topic(&self, topic: MockTopicInfo) {
        let mut topics = self.topics.lock().unwrap();
        topics.insert(topic.name.clone(), topic);
    }

    /// Get all topics
    pub fn get_topics(&self) -> HashMap<String, MockTopicInfo> {
        let topics = self.topics.lock().unwrap();
        topics.clone()
    }

    /// Clear all topics
    pub fn clear_topics(&self) {
        let mut topics = self.topics.lock().unwrap();
        topics.clear();
    }
}

impl Default for MockAdminClient {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaAdmin for MockAdminClient {
    fn create_topics(
        &self,
        new_topics: Vec<NewTopic>,
    ) -> Pin<Box<dyn Future<Output = Result<HashMap<String, String>, String>> + Send>> {
        let topics = self.topics.clone();
        let closed = self.closed.clone();

        Box::pin(async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Admin client is closed".to_string());
            }

            let mut results = HashMap::new();
            let mut all_topics = topics.lock().unwrap();

            for new_topic in new_topics {
                if all_topics.contains_key(&new_topic.name) {
                    results.insert(new_topic.name.clone(), format!("Topic '{}' already exists", new_topic.name));
                    continue;
                }

                let topic_info = MockTopicInfo {
                    name: new_topic.name.clone(),
                    num_partitions: new_topic.num_partitions,
                    replication_factor: new_topic.replication_factor,
                    config: new_topic.configs,
                };

                all_topics.insert(new_topic.name.clone(), topic_info);
                results.insert(new_topic.name.clone(), "Success".to_string());
            }

            Ok(results)
        })
    }

    fn delete_topics(
        &self,
        topic_names: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<HashMap<String, String>, String>> + Send>> {
        let topics = self.topics.clone();
        let closed = self.closed.clone();

        Box::pin(async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Admin client is closed".to_string());
            }

            let mut results = HashMap::new();
            let mut all_topics = topics.lock().unwrap();

            for topic_name in topic_names {
                if !all_topics.contains_key(&topic_name) {
                    results.insert(topic_name.clone(), format!("Topic '{}' does not exist", topic_name));
                    continue;
                }

                all_topics.remove(&topic_name);
                results.insert(topic_name.clone(), "Success".to_string());
            }

            Ok(results)
        })
    }

    fn describe_topics(
        &self,
        topic_names: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<HashMap<String, TopicDescription>, String>> + Send>> {
        let topics = self.topics.clone();
        let closed = self.closed.clone();

        Box::pin(async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Admin client is closed".to_string());
            }

            let mut results = HashMap::new();
            let all_topics = topics.lock().unwrap();

            for topic_name in topic_names {
                if let Some(_topic_info) = all_topics.get(&topic_name) {
                    let description = TopicDescription {
                        name: topic_name.clone(),
                        internal: false,
                        partitions: vec![PartitionMetadata {
                            topic: topic_name.clone(),
                            partition: 0,
                            leader: Some(Node {
                                id: 0,
                                host: "localhost".to_string(),
                                port: 9092,
                                rack: None,
                            }),
                            replicas: vec![Node {
                                id: 0,
                                host: "localhost".to_string(),
                                port: 9092,
                                rack: None,
                            }],
                            in_sync_replicas: vec![Node {
                                id: 0,
                                host: "localhost".to_string(),
                                port: 9092,
                                rack: None,
                            }],
                            offline_replicas: vec![],
                        }],
                        authorized_operations: vec!["READ".to_string(), "WRITE".to_string()],
                    };
                    results.insert(topic_name.clone(), description);
                }
            }

            Ok(results)
        })
    }

    fn list_topics(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>, String>> + Send>> {
        let topics = self.topics.clone();
        let closed = self.closed.clone();

        Box::pin(async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Admin client is closed".to_string());
            }

            let all_topics = topics.lock().unwrap();
            Ok(all_topics.keys().cloned().collect())
        })
    }

    fn describe_cluster(&self) -> Pin<Box<dyn Future<Output = Result<ClusterDescription, String>> + Send>> {
        let closed = self.closed.clone();

        Box::pin(async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Admin client is closed".to_string());
            }

            Ok(ClusterDescription {
                cluster_id: "mock-cluster".to_string(),
                controller: Some(Node {
                    id: 0,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }),
                nodes: vec![Node {
                    id: 0,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }],
                authorized_operations: vec!["ALL".to_string()],
            })
        })
    }

    fn list_consumer_groups(&self) -> Pin<Box<dyn Future<Output = Result<Vec<String>, String>> + Send>> {
        let closed = self.closed.clone();
 
        Box::pin(async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Admin client is closed".to_string());
            }
 
            // Mock returns empty list
            Ok(Vec::new())
        })
    }
 
    fn describe_consumer_group(
        &self,
        group_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<ConsumerGroupDescription, String>> + Send>> {
        let group_id = group_id.to_string();
        let closed = self.closed.clone();
 
        Box::pin(async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Admin client is closed".to_string());
            }
 
            Ok(ConsumerGroupDescription {
                group_id: group_id.clone(),
                is_simple_consumer_group: false,
                state: "Stable".to_string(),
                members: vec![MemberDescription {
                    consumer_id: "consumer-1".to_string(),
                    host: "localhost".to_string(),
                    client_id: "mock-client".to_string(),
                    assignments: vec![],
                }],
            })
        })
    }
 
    fn delete_consumer_group(
        &self,
        _group_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        let closed = self.closed.clone();
 
        Box::pin(async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Admin client is closed".to_string());
            }
 
            // Mock delete is a no-op
            Ok(())
        })
    }
 
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let closed = self.closed.clone();
 
        Box::pin(async move {
            let mut c = closed.lock().unwrap();
            *c = true;
        })
    }
 
    fn close_with_timeout(&self, _timeout: Duration) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        let closed = self.closed.clone();
 
        Box::pin(async move {
            let is_closed = *closed.lock().unwrap();
            if is_closed {
                return Err("Admin client is closed".to_string());
            }
            let mut c = closed.lock().unwrap();
            *c = true;
            Ok(())
        })
    }
}
