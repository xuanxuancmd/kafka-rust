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

//! Mock implementation of external system Admin API for end-to-end testing.
//!
//! This corresponds to simulating external Kafka cluster Admin API calls
//! for MirrorMaker 2 integration tests. Provides predefined responses
//! without implementing real network calls.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Topic listing returned by list_topics operation.
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

/// Result of list_topics operation.
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

/// Resource type for configuration operations.
///
/// This corresponds to `org.apache.kafka.common.config.ConfigResource.Type` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConfigResourceType {
    /// Broker resource.
    Broker,
    /// Topic resource.
    Topic,
}

/// Resource identifier for configuration operations.
///
/// This corresponds to `org.apache.kafka.common.config.ConfigResource` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigResource {
    /// The resource type.
    type_: ConfigResourceType,
    /// The resource name (topic name or broker ID).
    name: String,
}

impl ConfigResource {
    /// Creates a new config resource.
    pub fn new(type_: ConfigResourceType, name: impl Into<String>) -> Self {
        ConfigResource {
            type_,
            name: name.into(),
        }
    }

    /// Creates a topic config resource.
    pub fn topic(name: impl Into<String>) -> Self {
        ConfigResource::new(ConfigResourceType::Topic, name)
    }

    /// Creates a broker config resource.
    pub fn broker(id: impl Into<String>) -> Self {
        ConfigResource::new(ConfigResourceType::Broker, id)
    }

    /// Returns the resource type.
    pub fn type_(&self) -> ConfigResourceType {
        self.type_
    }

    /// Returns the resource name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Configuration entry returned by describe_configs operation.
///
/// This corresponds to `org.apache.kafka.clients.admin.ConfigEntry` in Java.
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    name: String,
    value: String,
    source: ConfigSource,
    is_read_only: bool,
    is_sensitive: bool,
    is_default: bool,
}

impl ConfigEntry {
    /// Creates a new config entry.
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        ConfigEntry {
            name: name.into(),
            value: value.into(),
            source: ConfigSource::DefaultConfig,
            is_read_only: false,
            is_sensitive: false,
            is_default: true,
        }
    }

    /// Creates a config entry with specific value.
    pub fn with_value(name: impl Into<String>, value: impl Into<String>, is_default: bool) -> Self {
        ConfigEntry {
            name: name.into(),
            value: value.into(),
            source: if is_default {
                ConfigSource::DefaultConfig
            } else {
                ConfigSource::DynamicTopicConfig
            },
            is_read_only: false,
            is_sensitive: false,
            is_default,
        }
    }

    /// Returns the config name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the config value.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Returns the source of the config.
    pub fn source(&self) -> ConfigSource {
        self.source
    }

    /// Returns whether the config is read-only.
    pub fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    /// Returns whether the config is sensitive.
    pub fn is_sensitive(&self) -> bool {
        self.is_sensitive
    }

    /// Returns whether the config is default.
    pub fn is_default(&self) -> bool {
        self.is_default
    }
}

/// Source of configuration entry.
///
/// This corresponds to `org.apache.kafka.clients.admin.ConfigSource` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    /// Dynamic broker config.
    DynamicBrokerConfig,
    /// Dynamic broker logger config.
    DynamicBrokerLoggerConfig,
    /// Dynamic topic config.
    DynamicTopicConfig,
    /// Default config.
    DefaultConfig,
    /// Static broker config.
    StaticBrokerConfig,
}

/// Configuration returned by describe_configs operation.
///
/// This corresponds to `org.apache.kafka.clients.admin.Config` in Java.
#[derive(Debug, Clone)]
pub struct Config {
    entries: HashMap<String, ConfigEntry>,
}

impl Config {
    /// Creates a new config with entries.
    pub fn new(entries: HashMap<String, ConfigEntry>) -> Self {
        Config { entries }
    }

    /// Creates an empty config.
    pub fn empty() -> Self {
        Config {
            entries: HashMap::new(),
        }
    }

    /// Returns all config entries.
    pub fn entries(&self) -> &HashMap<String, ConfigEntry> {
        &self.entries
    }

    /// Returns a config entry by name.
    pub fn get(&self, name: &str) -> Option<&ConfigEntry> {
        self.entries.get(name)
    }

    /// Returns config as a simple key-value map.
    pub fn as_map(&self) -> HashMap<String, String> {
        self.entries
            .iter()
            .map(|(k, v)| (k.clone(), v.value().to_string()))
            .collect()
    }
}

/// Result of describe_configs operation.
#[derive(Debug)]
pub struct DescribeConfigsResult {
    configs: HashMap<ConfigResource, Result<Config, ConfigError>>,
}

impl DescribeConfigsResult {
    fn new() -> Self {
        DescribeConfigsResult {
            configs: HashMap::new(),
        }
    }

    fn add_success(&mut self, resource: ConfigResource, config: Config) {
        self.configs.insert(resource, Ok(config));
    }

    fn add_error(&mut self, resource: ConfigResource, error: ConfigError) {
        self.configs.insert(resource, Err(error));
    }

    /// Returns the config for a resource.
    pub fn config(&self, resource: &ConfigResource) -> Option<&Result<Config, ConfigError>> {
        self.configs.get(resource)
    }

    /// Returns all results.
    pub fn all(&self) -> &HashMap<ConfigResource, Result<Config, ConfigError>> {
        &self.configs
    }

    /// Returns successful config descriptions.
    pub fn successes(&self) -> HashMap<&ConfigResource, &Config> {
        self.configs
            .iter()
            .filter_map(|(r, c)| c.as_ref().ok().map(|c| (r, c)))
            .collect()
    }

    /// Returns failed config descriptions.
    pub fn errors(&self) -> HashMap<&ConfigResource, &ConfigError> {
        self.configs
            .iter()
            .filter_map(|(r, c)| c.as_ref().err().map(|e| (r, e)))
            .collect()
    }
}

/// Error for configuration operations.
#[derive(Debug, Clone)]
pub enum ConfigError {
    /// Resource does not exist.
    DoesNotExist { resource: String, message: String },
    /// Invalid configuration value.
    InvalidValue {
        resource: String,
        key: String,
        message: String,
    },
    /// Operation failed.
    Failed { resource: String, message: String },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::DoesNotExist { resource, message } => {
                write!(f, "Resource '{}' does not exist: {}", resource, message)
            }
            ConfigError::InvalidValue {
                resource,
                key,
                message,
            } => {
                write!(
                    f,
                    "Invalid value for config '{}' on resource '{}': {}",
                    key, resource, message
                )
            }
            ConfigError::Failed { resource, message } => {
                write!(
                    f,
                    "Operation failed for resource '{}': {}",
                    resource, message
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Internal topic representation for the mock.
#[derive(Debug, Clone)]
struct InternalTopic {
    name: String,
    is_internal: bool,
    configs: HashMap<String, String>,
}

impl InternalTopic {
    fn new(name: impl Into<String>, is_internal: bool) -> Self {
        InternalTopic {
            name: name.into(),
            is_internal,
            configs: HashMap::new(),
        }
    }

    fn with_configs(
        name: impl Into<String>,
        is_internal: bool,
        configs: HashMap<String, String>,
    ) -> Self {
        InternalTopic {
            name: name.into(),
            is_internal,
            configs,
        }
    }

    fn to_listing(&self) -> TopicListing {
        TopicListing::new(&self.name, self.is_internal)
    }

    fn to_config(&self) -> Config {
        let entries: HashMap<String, ConfigEntry> = self
            .configs
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    ConfigEntry::with_value(k.clone(), v.clone(), false),
                )
            })
            .collect();
        Config::new(entries)
    }
}

/// Mock implementation of external system Admin API for end-to-end testing.
///
/// This provides a simplified in-memory implementation that simulates
/// external Kafka cluster Admin API calls. Used for testing MirrorMaker 2
/// integration without requiring a real Kafka cluster.
///
/// Thread-safe via internal `Arc<Mutex<...>` wrapper.
///
/// Supports:
/// - Predefined topic listings
/// - Predefined topic configurations
/// - Optional simulated network delay
pub struct ExternalAdminMock {
    topics: Arc<Mutex<HashMap<String, InternalTopic>>>,
    simulated_delay: Arc<Mutex<Option<Duration>>>,
}

impl ExternalAdminMock {
    /// Creates a new ExternalAdminMock with empty state.
    pub fn new() -> Self {
        ExternalAdminMock {
            topics: Arc::new(Mutex::new(HashMap::new())),
            simulated_delay: Arc::new(Mutex::new(None)),
        }
    }

    /// Creates a new ExternalAdminMock with predefined topics.
    ///
    /// # Arguments
    /// * `topics` - List of topic names to create
    pub fn with_topics(topics: Vec<String>) -> Self {
        let mut mock = ExternalAdminMock::new();
        for topic in topics {
            mock.add_topic(topic, false);
        }
        mock
    }

    /// Creates a new ExternalAdminMock with predefined topics and configs.
    ///
    /// # Arguments
    /// * `topic_configs` - Map of topic name to configuration key-value pairs
    pub fn with_topic_configs(topic_configs: HashMap<String, HashMap<String, String>>) -> Self {
        let mut mock = ExternalAdminMock::new();
        for (topic_name, configs) in topic_configs {
            mock.add_topic_with_configs(topic_name, false, configs);
        }
        mock
    }

    /// Sets simulated network delay for all operations.
    ///
    /// This can be used to test timeout handling and network latency scenarios.
    pub fn set_simulated_delay(&self, delay: Duration) {
        *self.simulated_delay.lock().unwrap() = Some(delay);
    }

    /// Removes simulated network delay.
    pub fn remove_simulated_delay(&self) {
        *self.simulated_delay.lock().unwrap() = None;
    }

    /// Applies simulated delay if configured.
    fn apply_delay(&self) {
        let delay = self.simulated_delay.lock().unwrap();
        if let Some(d) = *delay {
            // In real tests, this would use tokio::time::sleep
            // For synchronous mock, we just note the delay simulation
            std::thread::sleep(d);
        }
    }

    // ===== Topic Management =====

    /// Adds a topic to the mock.
    ///
    /// # Arguments
    /// * `name` - Topic name
    /// * `is_internal` - Whether this is an internal topic
    pub fn add_topic(&self, name: impl Into<String>, is_internal: bool) {
        let name_str = name.into();
        let mut topics = self.topics.lock().unwrap();
        topics.insert(name_str.clone(), InternalTopic::new(name_str, is_internal));
    }

    /// Adds a topic with configuration to the mock.
    ///
    /// # Arguments
    /// * `name` - Topic name
    /// * `is_internal` - Whether this is an internal topic
    /// * `configs` - Topic configuration key-value pairs
    pub fn add_topic_with_configs(
        &self,
        name: impl Into<String>,
        is_internal: bool,
        configs: HashMap<String, String>,
    ) {
        let name_str = name.into();
        let mut topics = self.topics.lock().unwrap();
        topics.insert(
            name_str.clone(),
            InternalTopic::with_configs(name_str, is_internal, configs),
        );
    }

    /// Removes a topic from the mock.
    pub fn remove_topic(&self, name: &str) {
        self.topics.lock().unwrap().remove(name);
    }

    /// Lists all topics.
    ///
    /// This corresponds to `Admin.listTopics()` in Java.
    /// Returns a `ListTopicsResult` containing all topic listings.
    pub fn list_topics(&self) -> ListTopicsResult {
        self.apply_delay();
        let topics = self.topics.lock().unwrap();
        let listings: Vec<TopicListing> = topics.values().map(|t| t.to_listing()).collect();
        ListTopicsResult::new(listings)
    }

    /// Lists topics matching a pattern.
    ///
    /// # Arguments
    /// * `pattern` - Topic name pattern (supports prefix matching)
    pub fn list_topics_matching(&self, pattern: &str) -> ListTopicsResult {
        self.apply_delay();
        let topics = self.topics.lock().unwrap();
        let listings: Vec<TopicListing> = topics
            .values()
            .filter(|t| t.name.starts_with(pattern))
            .map(|t| t.to_listing())
            .collect();
        ListTopicsResult::new(listings)
    }

    /// Describes configurations for resources.
    ///
    /// This corresponds to `Admin.describeConfigs()` in Java.
    /// Returns configurations for the requested resources.
    ///
    /// # Arguments
    /// * `resources` - List of ConfigResource to describe
    pub fn describe_configs(&self, resources: Vec<ConfigResource>) -> DescribeConfigsResult {
        self.apply_delay();
        let mut result = DescribeConfigsResult::new();
        let topics = self.topics.lock().unwrap();

        for resource in resources {
            if resource.type_() == ConfigResourceType::Topic {
                let topic_name = resource.name().to_string();
                if let Some(topic) = topics.get(&topic_name) {
                    result.add_success(resource, topic.to_config());
                } else {
                    result.add_error(
                        resource,
                        ConfigError::DoesNotExist {
                            resource: topic_name,
                            message: "Topic does not exist".to_string(),
                        },
                    );
                }
            } else {
                // Broker configs return empty for mock
                result.add_success(resource, Config::empty());
            }
        }

        result
    }

    /// Gets configuration for a specific topic.
    ///
    /// # Arguments
    /// * `topic_name` - Topic name
    /// * `config_keys` - Specific config keys to retrieve (empty for all)
    pub fn get_topic_config(
        &self,
        topic_name: &str,
        config_keys: &[String],
    ) -> Result<HashMap<String, String>, ConfigError> {
        self.apply_delay();
        let topics = self.topics.lock().unwrap();

        if let Some(topic) = topics.get(topic_name) {
            if config_keys.is_empty() {
                Ok(topic.configs.clone())
            } else {
                let result: HashMap<String, String> = config_keys
                    .iter()
                    .filter_map(|k| topic.configs.get(k).map(|v| (k.clone(), v.clone())))
                    .collect();
                Ok(result)
            }
        } else {
            Err(ConfigError::DoesNotExist {
                resource: topic_name.to_string(),
                message: "Topic does not exist".to_string(),
            })
        }
    }

    // ===== Utility methods =====

    /// Clears all topics.
    pub fn clear(&self) {
        self.topics.lock().unwrap().clear();
    }

    /// Returns the number of topics.
    pub fn topic_count(&self) -> usize {
        self.topics.lock().unwrap().len()
    }

    /// Returns whether a topic exists.
    pub fn topic_exists(&self, topic_name: &str) -> bool {
        self.topics.lock().unwrap().contains_key(topic_name)
    }

    /// Returns all topic names.
    pub fn topic_names(&self) -> Vec<String> {
        self.topics.lock().unwrap().keys().cloned().collect()
    }
}

impl Default for ExternalAdminMock {
    fn default() -> Self {
        ExternalAdminMock::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let mock = ExternalAdminMock::new();
        assert_eq!(mock.topic_count(), 0);
    }

    #[test]
    fn test_with_topics() {
        let mock = ExternalAdminMock::with_topics(vec!["topic1".to_string(), "topic2".to_string()]);
        assert_eq!(mock.topic_count(), 2);
        assert!(mock.topic_exists("topic1"));
        assert!(mock.topic_exists("topic2"));
    }

    #[test]
    fn test_add_topic() {
        let mock = ExternalAdminMock::new();
        mock.add_topic("test-topic", false);
        assert_eq!(mock.topic_count(), 1);
        assert!(mock.topic_exists("test-topic"));
    }

    #[test]
    fn test_remove_topic() {
        let mock = ExternalAdminMock::with_topics(vec!["topic1".to_string()]);
        mock.remove_topic("topic1");
        assert_eq!(mock.topic_count(), 0);
        assert!(!mock.topic_exists("topic1"));
    }

    #[test]
    fn test_list_topics() {
        let mock = ExternalAdminMock::with_topics(vec![
            "topic1".to_string(),
            "topic2".to_string(),
            "internal-topic".to_string(),
        ]);
        mock.add_topic("__internal", true);

        let result = mock.list_topics();
        assert_eq!(result.count(), 4);

        let names = result.names();
        assert!(names.contains(&"topic1".to_string()));
        assert!(names.contains(&"topic2".to_string()));
        assert!(names.contains(&"internal-topic".to_string()));
        assert!(names.contains(&"__internal".to_string()));
    }

    #[test]
    fn test_list_topics_matching() {
        let mock = ExternalAdminMock::with_topics(vec![
            "source.topic1".to_string(),
            "source.topic2".to_string(),
            "target.topic1".to_string(),
        ]);

        let result = mock.list_topics_matching("source.");
        assert_eq!(result.count(), 2);

        let names = result.names();
        assert!(names.contains(&"source.topic1".to_string()));
        assert!(names.contains(&"source.topic2".to_string()));
        assert!(!names.contains(&"target.topic1".to_string()));
    }

    #[test]
    fn test_describe_configs() {
        let mut configs = HashMap::new();
        configs.insert("cleanup.policy".to_string(), "compact".to_string());
        configs.insert("retention.ms".to_string(), "86400000".to_string());

        let mock = ExternalAdminMock::new();
        mock.add_topic_with_configs("test-topic", false, configs);

        let resource = ConfigResource::topic("test-topic");
        let result = mock.describe_configs(vec![resource.clone()]);

        let config_result = result.config(&resource).unwrap();
        assert!(config_result.is_ok());

        let config = config_result.as_ref().unwrap();
        assert_eq!(config.entries().len(), 2);

        let config_map = config.as_map();
        assert_eq!(config_map.get("cleanup.policy").unwrap(), "compact");
        assert_eq!(config_map.get("retention.ms").unwrap(), "86400000");
    }

    #[test]
    fn test_describe_configs_nonexistent() {
        let mock = ExternalAdminMock::new();
        let resource = ConfigResource::topic("nonexistent");
        let result = mock.describe_configs(vec![resource.clone()]);

        let config_result = result.config(&resource).unwrap();
        assert!(config_result.is_err());
    }

    #[test]
    fn test_get_topic_config() {
        let mut configs = HashMap::new();
        configs.insert("cleanup.policy".to_string(), "compact".to_string());
        configs.insert("retention.ms".to_string(), "86400000".to_string());

        let mock = ExternalAdminMock::new();
        mock.add_topic_with_configs("test-topic", false, configs);

        // Get all configs
        let all_configs = mock.get_topic_config("test-topic", &[]).unwrap();
        assert_eq!(all_configs.len(), 2);

        // Get specific configs
        let specific_configs = mock
            .get_topic_config("test-topic", &["cleanup.policy".to_string()])
            .unwrap();
        assert_eq!(specific_configs.len(), 1);
        assert_eq!(specific_configs.get("cleanup.policy").unwrap(), "compact");
    }

    #[test]
    fn test_get_topic_config_nonexistent() {
        let mock = ExternalAdminMock::new();
        let result = mock.get_topic_config("nonexistent", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_clear() {
        let mock = ExternalAdminMock::with_topics(vec!["topic1".to_string()]);
        mock.clear();
        assert_eq!(mock.topic_count(), 0);
    }

    #[test]
    fn test_topic_listing() {
        let listing = TopicListing::new("test-topic", false);
        assert_eq!(listing.name(), "test-topic");
        assert!(!listing.is_internal());

        let internal_listing = TopicListing::new("__internal", true);
        assert!(internal_listing.is_internal());
    }

    #[test]
    fn test_config_entry() {
        let entry = ConfigEntry::new("cleanup.policy", "compact");
        assert_eq!(entry.name(), "cleanup.policy");
        assert_eq!(entry.value(), "compact");
        assert!(entry.is_default());

        let custom_entry = ConfigEntry::with_value("retention.ms", "86400000", false);
        assert!(!custom_entry.is_default());
    }

    #[test]
    fn test_config_resource() {
        let topic_resource = ConfigResource::topic("test-topic");
        assert_eq!(topic_resource.type_(), ConfigResourceType::Topic);
        assert_eq!(topic_resource.name(), "test-topic");

        let broker_resource = ConfigResource::broker("0");
        assert_eq!(broker_resource.type_(), ConfigResourceType::Broker);
        assert_eq!(broker_resource.name(), "0");
    }
}
