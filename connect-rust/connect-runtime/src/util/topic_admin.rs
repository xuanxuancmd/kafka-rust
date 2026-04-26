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

//! Utility to simplify creating and managing topics via Admin client.
//!
//! This corresponds to `org.apache.kafka.connect.util.TopicAdmin` in Java.

use common_trait::util::time::Time;
use common_trait::TopicPartition;
use kafka_clients_mock::{MockAdmin, NewTopic, TopicMetadata};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// Constants for topic creation.
pub const NO_PARTITIONS: i32 = -1;
pub const NO_REPLICATION_FACTOR: i32 = -1;

/// Cleanup policy configuration key.
pub const CLEANUP_POLICY_CONFIG: &str = "cleanup.policy";
/// Compact cleanup policy value.
pub const CLEANUP_POLICY_COMPACT: &str = "compact";

/// Response from topic creation operation.
///
/// Contains the names of topics that were newly created and those that already existed.
#[derive(Debug, Clone)]
pub struct TopicCreationResponse {
    created: HashSet<String>,
    existing: HashSet<String>,
}

impl TopicCreationResponse {
    /// Creates a new TopicCreationResponse.
    pub fn new(
        created_topic_names: HashSet<String>,
        existing_topic_names: HashSet<String>,
    ) -> Self {
        TopicCreationResponse {
            created: created_topic_names,
            existing: existing_topic_names,
        }
    }

    /// Empty creation response.
    pub fn empty() -> Self {
        TopicCreationResponse {
            created: HashSet::new(),
            existing: HashSet::new(),
        }
    }

    /// Returns the set of newly created topics.
    pub fn created_topics(&self) -> &HashSet<String> {
        &self.created
    }

    /// Checks if a specific topic was newly created.
    pub fn is_created(&self, topic_name: &str) -> bool {
        self.created.contains(topic_name)
    }

    /// Checks if a specific topic already existed.
    pub fn is_existing(&self, topic_name: &str) -> bool {
        self.existing.contains(topic_name)
    }

    /// Checks if a topic was either created or already existed.
    pub fn is_created_or_existing(&self, topic_name: &str) -> bool {
        self.is_created(topic_name) || self.is_existing(topic_name)
    }

    /// Returns the count of newly created topics.
    pub fn created_topics_count(&self) -> usize {
        self.created.len()
    }

    /// Returns the count of existing topics.
    pub fn existing_topics_count(&self) -> usize {
        self.existing.len()
    }

    /// Returns the total count of created or existing topics.
    pub fn created_or_existing_topics_count(&self) -> usize {
        self.created_topics_count() + self.existing_topics_count()
    }

    /// Returns true if this response is empty.
    pub fn is_empty(&self) -> bool {
        self.created_or_existing_topics_count() == 0
    }
}

impl std::fmt::Display for TopicCreationResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TopicCreationResponse{{created={:?}, existing={:?}}}",
            self.created.iter().cloned().collect::<Vec<_>>(),
            self.existing.iter().cloned().collect::<Vec<_>>()
        )
    }
}

/// Builder for creating NewTopic specifications.
///
/// This corresponds to the inner NewTopicBuilder class in Java.
#[derive(Debug)]
pub struct NewTopicBuilder {
    name: String,
    num_partitions: i32,
    replication_factor: i32,
    configs: HashMap<String, String>,
}

impl NewTopicBuilder {
    /// Creates a new builder for the given topic name.
    pub fn new(topic_name: String) -> Self {
        NewTopicBuilder {
            name: topic_name,
            num_partitions: NO_PARTITIONS,
            replication_factor: NO_REPLICATION_FACTOR,
            configs: HashMap::new(),
        }
    }

    /// Specify the desired number of partitions for the topic.
    ///
    /// Must be positive, or -1 to signify using the broker's default.
    pub fn partitions(mut self, num_partitions: i32) -> Self {
        self.num_partitions = num_partitions;
        self
    }

    /// Use the broker's default number of partitions.
    pub fn default_partitions(mut self) -> Self {
        self.num_partitions = NO_PARTITIONS;
        self
    }

    /// Specify the desired replication factor for the topic.
    ///
    /// Must be positive, or -1 to signify using the broker's default.
    pub fn replication_factor(mut self, replication_factor: i32) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    /// Use the broker's default replication factor.
    pub fn default_replication_factor(mut self) -> Self {
        self.replication_factor = NO_REPLICATION_FACTOR;
        self
    }

    /// Specify that the topic should be compacted.
    pub fn compacted(mut self) -> Self {
        self.configs.insert(
            CLEANUP_POLICY_CONFIG.to_string(),
            CLEANUP_POLICY_COMPACT.to_string(),
        );
        self
    }

    /// Specify configuration properties for the topic.
    pub fn config(mut self, configs: HashMap<String, String>) -> Self {
        self.configs = configs;
        self
    }

    /// Add a single configuration property.
    pub fn config_entry(mut self, key: String, value: String) -> Self {
        self.configs.insert(key, value);
        self
    }

    /// Build the NewTopic representation.
    pub fn build(self) -> NewTopic {
        let mut topic = NewTopic::new(self.name, self.num_partitions, self.replication_factor);
        for (key, value) in self.configs {
            topic = topic.config(key, value);
        }
        topic
    }
}

/// Utility to simplify creating and managing topics via Admin client.
///
/// This corresponds to `org.apache.kafka.connect.util.TopicAdmin` in Java.
pub struct TopicAdmin {
    bootstrap_servers: String,
    admin: Arc<MockAdmin>,
    log_creation: bool,
}

impl TopicAdmin {
    /// Creates a new TopicAdmin with the given admin configuration.
    pub fn new(admin_config: HashMap<String, String>) -> Self {
        let bootstrap_servers = admin_config
            .get("bootstrap.servers")
            .cloned()
            .unwrap_or_else(|| "<unknown>".to_string());
        let admin = Arc::new(MockAdmin::new());
        TopicAdmin {
            bootstrap_servers,
            admin,
            log_creation: true,
        }
    }

    /// Creates a new TopicAdmin with an existing Admin client.
    pub fn with_admin(admin_config: HashMap<String, String>, admin: Arc<MockAdmin>) -> Self {
        let bootstrap_servers = admin_config
            .get("bootstrap.servers")
            .cloned()
            .unwrap_or_else(|| "<unknown>".to_string());
        TopicAdmin {
            bootstrap_servers,
            admin,
            log_creation: true,
        }
    }

    /// Creates a TopicAdmin for testing with controlled logging.
    pub fn for_testing(
        bootstrap_servers: Option<String>,
        admin: Arc<MockAdmin>,
        log_creation: bool,
    ) -> Self {
        TopicAdmin {
            bootstrap_servers: bootstrap_servers.unwrap_or_else(|| "<unknown>".to_string()),
            admin,
            log_creation,
        }
    }

    /// Returns a builder to define a NewTopic.
    pub fn define_topic(topic_name: String) -> NewTopicBuilder {
        NewTopicBuilder::new(topic_name)
    }

    /// Attempt to create the topic described by the given definition.
    ///
    /// Returns true if the topic was created, false if it already existed.
    pub fn create_topic(&self, topic: &NewTopic) -> Result<bool, TopicAdminError> {
        let response = self.create_or_find_topics(&[topic.clone()])?;
        Ok(response.is_created(topic.name()))
    }

    /// Attempt to create multiple topics.
    ///
    /// Returns the names of topics that were created.
    pub fn create_topics(&self, topics: &[NewTopic]) -> Result<HashSet<String>, TopicAdminError> {
        let response = self.create_or_find_topics(topics)?;
        Ok(response.created_topics().clone())
    }

    /// Create topics with retry logic for specific exceptions.
    pub fn create_topics_with_retry(
        &self,
        topic: &NewTopic,
        timeout_ms: i64,
        backoff_ms: i64,
        time: Arc<dyn Time>,
    ) -> Result<HashSet<String>, TopicAdminError> {
        let timer = time.timer(timeout_ms);

        loop {
            match self.create_topics(&[topic.clone()]) {
                Ok(created) => return Ok(created),
                Err(e) => {
                    if timer.not_expired() && self.should_retry_topic_creation(&e) {
                        log::info!(
                            "'{}' topic creation failed due to '{}', retrying, {}ms remaining",
                            topic.name(),
                            e,
                            timer.remaining_ms()
                        );
                    } else {
                        return Err(e);
                    }
                }
            }
            time.sleep(backoff_ms);
        }
    }

    /// Check if an error should trigger a retry.
    fn should_retry_topic_creation(&self, error: &TopicAdminError) -> bool {
        matches!(
            error,
            TopicAdminError::InvalidReplicationFactor(_) | TopicAdminError::Timeout(_)
        )
    }

    /// Attempt to create or find a single topic.
    pub fn create_or_find_topic(&self, topic: &NewTopic) -> Result<bool, TopicAdminError> {
        let response = self.create_or_find_topics(&[topic.clone()])?;
        Ok(response.is_created_or_existing(topic.name()))
    }

    /// Attempt to create or find multiple topics.
    ///
    /// Returns a response indicating which topics were created vs already existing.
    pub fn create_or_find_topics(
        &self,
        topics: &[NewTopic],
    ) -> Result<TopicCreationResponse, TopicAdminError> {
        if topics.is_empty() {
            return Ok(TopicCreationResponse::empty());
        }

        // Build topicsByName map, handling duplicates by using last definition
        let topics_by_name: HashMap<String, NewTopic> = topics
            .iter()
            .map(|t| (t.name().to_string(), t.clone()))
            .collect();

        if topics_by_name.is_empty() {
            return Ok(TopicCreationResponse::empty());
        }

        let topic_names: Vec<String> = topics_by_name.keys().cloned().collect();
        let topic_name_list = topic_names.join(", ");

        // Attempt to create topics
        let results = self
            .admin
            .create_topics(topics_by_name.values().cloned().collect::<Vec<_>>());

        let mut newly_created: HashSet<String> = HashSet::new();
        let mut existing: HashSet<String> = HashSet::new();

        // Process successful topic creations
        for (topic, _metadata) in results.successes() {
            if self.log_creation {
                log::info!(
                    "Created topic {} on brokers at {}",
                    topics_by_name.get(topic).map(|t| t.name()).unwrap_or(topic),
                    self.bootstrap_servers
                );
            }
            newly_created.insert(topic.clone());
        }

        // Process topic creation errors
        for (topic, e) in results.errors() {
            match e {
                kafka_clients_mock::TopicError::AlreadyExists { .. } => {
                    log::debug!(
                        "Found existing topic '{}' on the brokers at {}",
                        topic,
                        self.bootstrap_servers
                    );
                    existing.insert(topic.clone());
                }
                kafka_clients_mock::TopicError::UnsupportedVersion { .. } => {
                    log::debug!(
                        "Unable to create topic(s) '{}' since brokers at {} do not support the CreateTopics API",
                        topic_name_list,
                        self.bootstrap_servers
                    );
                    return Ok(TopicCreationResponse::empty());
                }
                kafka_clients_mock::TopicError::Authorization { .. } => {
                    log::debug!(
                        "Not authorized to create topic(s) '{}' on brokers {}",
                        topic_name_list,
                        self.bootstrap_servers
                    );
                    return Ok(TopicCreationResponse::empty());
                }
                kafka_clients_mock::TopicError::InvalidConfig { message, .. } => {
                    return Err(TopicAdminError::InvalidConfig(format!(
                        "Unable to create topic(s) '{}': {}",
                        topic_name_list, message
                    )));
                }
                kafka_clients_mock::TopicError::Timeout { .. } => {
                    return Err(TopicAdminError::Timeout(format!(
                        "Timed out while checking for or creating topic(s) '{}'",
                        topic_name_list
                    )));
                }
                kafka_clients_mock::TopicError::DoesNotExist { .. } => {
                    // This shouldn't happen during creation
                    return Err(TopicAdminError::Other(format!(
                        "Unexpected error creating topic '{}'",
                        topic
                    )));
                }
                kafka_clients_mock::TopicError::InvalidReplicationFactor { .. } => {
                    return Err(TopicAdminError::InvalidReplicationFactor(format!(
                        "Invalid replication factor for topic '{}'",
                        topic
                    )));
                }
                kafka_clients_mock::TopicError::LeaderNotAvailable { topic: _, message } => {
                    return Err(TopicAdminError::LeaderNotAvailable(format!(
                        "Leader not available for topic '{}': {}",
                        topic, message
                    )));
                }
                _ => {
                    return Err(TopicAdminError::Other(format!(
                        "Error while attempting to create/find topic(s) '{}'",
                        topic_name_list
                    )));
                }
            }
        }

        Ok(TopicCreationResponse::new(newly_created, existing))
    }

    /// Describe topics to get their metadata.
    pub fn describe_topics(
        &self,
        topics: &[String],
    ) -> Result<HashMap<String, TopicMetadata>, TopicAdminError> {
        if topics.is_empty() {
            return Ok(HashMap::new());
        }

        let topic_name_list = topics.join(", ");
        let results = self.admin.describe_topics(topics.to_vec());

        let mut existing_topics = HashMap::new();
        // Process successful topic descriptions
        for (topic, metadata) in results.successes() {
            existing_topics.insert(topic.clone(), metadata.clone());
        }

        // Process topic description errors
        for (topic, e) in results.errors() {
            match e {
                kafka_clients_mock::TopicError::DoesNotExist { .. } => {
                    log::debug!(
                        "Topic '{}' does not exist on the brokers at {}",
                        topic,
                        self.bootstrap_servers
                    );
                }
                kafka_clients_mock::TopicError::Authorization { .. } => {
                    return Err(TopicAdminError::Authorization(format!(
                        "Not authorized to describe topic(s) '{}' on brokers {}",
                        topic_name_list, self.bootstrap_servers
                    )));
                }
                kafka_clients_mock::TopicError::UnsupportedVersion { .. } => {
                    return Err(TopicAdminError::UnsupportedVersion(format!(
                            "Unable to describe topic(s) '{}' since brokers at {} do not support the DescribeTopics API",
                            topic_name_list,
                            self.bootstrap_servers
                        )));
                }
                kafka_clients_mock::TopicError::Timeout { .. } => {
                    return Err(TopicAdminError::Timeout(format!(
                        "Timed out while describing topics '{}'",
                        topic_name_list
                    )));
                }
                _ => {
                    return Err(TopicAdminError::Other(format!(
                        "Error while attempting to describe topics '{}'",
                        topic_name_list
                    )));
                }
            }
        }

        Ok(existing_topics)
    }

    /// Verify that a topic uses only compact cleanup policy.
    pub fn verify_topic_cleanup_policy_only_compact(
        &self,
        topic: &str,
        worker_topic_config: &str,
        topic_purpose: &str,
    ) -> Result<bool, TopicAdminError> {
        let cleanup_policies = self.topic_cleanup_policy(topic)?;

        if cleanup_policies.is_empty() {
            log::info!(
                "Unable to use admin client to verify the cleanup policy of '{}' topic is '{}'",
                topic,
                CLEANUP_POLICY_COMPACT
            );
            return Ok(false);
        }

        let expected: HashSet<String> = [CLEANUP_POLICY_COMPACT.to_string()].into_iter().collect();
        if cleanup_policies != expected {
            let expected_str = expected.iter().cloned().collect::<Vec<_>>().join(",");
            let actual_str = cleanup_policies
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(",");
            return Err(TopicAdminError::InvalidConfig(format!(
                "Topic '{}' supplied via '{}' property requires '{}'='{}' to guarantee consistency and durability of {}, but found '{}={}'.",
                topic, worker_topic_config, CLEANUP_POLICY_CONFIG, expected_str, topic_purpose,
                CLEANUP_POLICY_CONFIG, actual_str
            )));
        }

        Ok(true)
    }

    /// Get the cleanup policy for a topic.
    pub fn topic_cleanup_policy(&self, topic: &str) -> Result<HashSet<String>, TopicAdminError> {
        let configs = self.admin.describe_topic_configs(&[topic.to_string()]);

        match configs.get(topic) {
            Some(Some(config)) => {
                if let Some(policy) = config.get(CLEANUP_POLICY_CONFIG) {
                    log::debug!("Found cleanup.policy={} for topic '{}'", policy, topic);
                    let policies = policy
                        .split(',')
                        .map(|s| s.trim().to_lowercase())
                        .filter(|s| !s.is_empty())
                        .collect();
                    return Ok(policies);
                }
                log::debug!("Found no cleanup.policy for topic '{}'", topic);
                Ok(HashSet::new())
            }
            Some(None) => {
                log::debug!(
                    "Unable to find topic '{}' when getting cleanup policy",
                    topic
                );
                Ok(HashSet::new())
            }
            None => Ok(HashSet::new()),
        }
    }

    /// Fetch the most recent offset for each of the supplied TopicPartition objects.
    pub fn end_offsets(
        &self,
        partitions: &HashSet<TopicPartition>,
    ) -> Result<HashMap<TopicPartition, i64>, TopicAdminError> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let results = self.admin.list_offsets(
            partitions.iter().cloned().collect::<Vec<_>>(),
            kafka_clients_mock::OffsetSpec::Latest,
        );

        let mut offsets = HashMap::new();
        for (tp, result) in results {
            match result {
                Ok(offset) => {
                    offsets.insert(tp, offset);
                }
                Err(e) => {
                    let topic = tp.topic();
                    match e {
                        kafka_clients_mock::TopicError::Authorization { .. } => {
                            return Err(TopicAdminError::Authorization(format!(
                                "Not authorized to get end offsets for topic '{}' on brokers at {}",
                                topic, self.bootstrap_servers
                            )));
                        }
                        kafka_clients_mock::TopicError::UnsupportedVersion { .. } => {
                            return Err(TopicAdminError::UnsupportedVersion(format!(
                                "API to get end offsets for topic '{}' is unsupported on brokers at {}",
                                topic,
                                self.bootstrap_servers
                            )));
                        }
                        kafka_clients_mock::TopicError::Timeout { .. } => {
                            return Err(TopicAdminError::Timeout(format!(
                                "Timed out while waiting to get end offsets for topic '{}' on brokers at {}",
                                topic,
                                self.bootstrap_servers
                            )));
                        }
                        kafka_clients_mock::TopicError::LeaderNotAvailable {
                            topic: _,
                            message,
                        } => {
                            return Err(TopicAdminError::LeaderNotAvailable(format!(
                                "Unable to get end offsets during leader election for topic '{}' on brokers at {}: {}",
                                topic,
                                self.bootstrap_servers,
                                message
                            )));
                        }
                        _ => {
                            return Err(TopicAdminError::Other(format!(
                                "Error while getting end offsets for topic '{}' on brokers at {}",
                                topic, self.bootstrap_servers
                            )));
                        }
                    }
                }
            }
        }

        Ok(offsets)
    }

    /// Fetch end offsets with retry logic.
    pub fn retry_end_offsets(
        &self,
        partitions: &HashSet<TopicPartition>,
        timeout_duration: Duration,
        retry_backoff_ms: i64,
        time: Arc<dyn Time>,
    ) -> Result<HashMap<TopicPartition, i64>, TopicAdminError> {
        let end = time.milliseconds() + timeout_duration.as_millis() as i64;
        let mut attempt = 0;

        loop {
            attempt += 1;
            match self.end_offsets(partitions) {
                Ok(offsets) => return Ok(offsets),
                Err(e) => {
                    if time.milliseconds() < end {
                        log::warn!(
                            "Attempt {} to get end offsets failed; retrying. Reason: {}",
                            attempt,
                            e
                        );
                        time.sleep(retry_backoff_ms);
                    } else {
                        return Err(TopicAdminError::Timeout(format!(
                            "Failed to get end offsets after {} attempts: {}",
                            attempt, e
                        )));
                    }
                }
            }
        }
    }

    /// Returns the bootstrap servers string.
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }
}

impl Drop for TopicAdmin {
    fn drop(&mut self) {
        // Admin is closed via Arc's drop mechanism
        log::debug!(
            "Closing TopicAdmin for brokers at {}",
            self.bootstrap_servers
        );
    }
}

/// Errors that can occur during topic admin operations.
#[derive(Debug)]
pub enum TopicAdminError {
    /// Topic already exists.
    AlreadyExists(String),
    /// Topic does not exist.
    DoesNotExist(String),
    /// Invalid topic configuration.
    InvalidConfig(String),
    /// Invalid replication factor.
    InvalidReplicationFactor(String),
    /// Authorization error.
    Authorization(String),
    /// Unsupported version/API.
    UnsupportedVersion(String),
    /// Timeout error.
    Timeout(String),
    /// Leader not available.
    LeaderNotAvailable(String),
    /// Other error.
    Other(String),
}

impl std::fmt::Display for TopicAdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicAdminError::AlreadyExists(msg) => write!(f, "Topic already exists: {}", msg),
            TopicAdminError::DoesNotExist(msg) => write!(f, "Topic does not exist: {}", msg),
            TopicAdminError::InvalidConfig(msg) => write!(f, "Invalid configuration: {}", msg),
            TopicAdminError::InvalidReplicationFactor(msg) => {
                write!(f, "Invalid replication factor: {}", msg)
            }
            TopicAdminError::Authorization(msg) => write!(f, "Authorization error: {}", msg),
            TopicAdminError::UnsupportedVersion(msg) => write!(f, "Unsupported version: {}", msg),
            TopicAdminError::Timeout(msg) => write!(f, "Timeout: {}", msg),
            TopicAdminError::LeaderNotAvailable(msg) => write!(f, "Leader not available: {}", msg),
            TopicAdminError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for TopicAdminError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_topic_builder() {
        let topic = TopicAdmin::define_topic("test-topic".to_string())
            .partitions(3)
            .replication_factor(2)
            .compacted()
            .build();

        assert_eq!(topic.name(), "test-topic");
        assert_eq!(topic.num_partitions(), 3);
        assert_eq!(topic.replication_factor(), 2);
    }

    #[test]
    fn test_topic_creation_response() {
        let created: HashSet<String> = ["topic1".to_string()].into_iter().collect();
        let existing: HashSet<String> = ["topic2".to_string()].into_iter().collect();

        let response = TopicCreationResponse::new(created, existing);
        assert!(response.is_created("topic1"));
        assert!(response.is_existing("topic2"));
        assert!(response.is_created_or_existing("topic1"));
        assert!(response.is_created_or_existing("topic2"));
        assert!(!response.is_empty());
        assert_eq!(response.created_topics_count(), 1);
        assert_eq!(response.existing_topics_count(), 1);
    }

    #[test]
    fn test_empty_creation_response() {
        let response = TopicCreationResponse::empty();
        assert!(response.is_empty());
        assert_eq!(response.created_or_existing_topics_count(), 0);
    }
}
