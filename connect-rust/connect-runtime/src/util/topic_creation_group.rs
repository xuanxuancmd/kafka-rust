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

//! Represents a group of topics defined by inclusion/exclusion regex patterns.
//!
//! This corresponds to `org.apache.kafka.connect.util.TopicCreationGroup` in Java.

use kafka_clients_mock::NewTopic;
use regex::Regex;
use std::collections::HashMap;

use super::topic_admin::{NewTopicBuilder, NO_PARTITIONS, NO_REPLICATION_FACTOR};

/// Default topic creation group name.
/// Corresponds to Java: TopicCreationConfig.DEFAULT_TOPIC_CREATION_GROUP
pub const DEFAULT_TOPIC_CREATION_GROUP: &str = "default";

/// Represents a group of topics defined by inclusion/exclusion regex patterns
/// along with the group's topic creation configurations.
///
/// This corresponds to `org.apache.kafka.connect.util.TopicCreationGroup` in Java.
#[derive(Debug, Clone)]
pub struct TopicCreationGroup {
    /// The name of this group
    name: String,
    /// Pattern for topics to include
    inclusion_pattern: Regex,
    /// Pattern for topics to exclude
    exclusion_pattern: Regex,
    /// Number of partitions for new topics
    num_partitions: i32,
    /// Replication factor for new topics
    replication_factor: i32,
    /// Additional topic configurations
    other_configs: HashMap<String, String>,
}

impl TopicCreationGroup {
    /// Creates a new TopicCreationGroup with the given parameters.
    ///
    /// This corresponds to the protected constructor in Java that takes
    /// a group name and SourceConnectorConfig.
    pub fn new(
        name: String,
        inclusion_patterns: Vec<String>,
        exclusion_patterns: Vec<String>,
        num_partitions: i32,
        replication_factor: i32,
        other_configs: HashMap<String, String>,
    ) -> Self {
        // Join patterns with | for regex alternation
        let inclusion_regex = if inclusion_patterns.is_empty() {
            Regex::new(".*").unwrap() // Match everything if no inclusion patterns
        } else {
            Regex::new(&inclusion_patterns.join("|")).unwrap_or_else(|_| Regex::new(".*").unwrap())
        };

        let exclusion_regex = if exclusion_patterns.is_empty() {
            Regex::new("^$").unwrap() // Match nothing if no exclusion patterns
        } else {
            Regex::new(&exclusion_patterns.join("|")).unwrap_or_else(|_| Regex::new("^$").unwrap())
        };

        TopicCreationGroup {
            name,
            inclusion_pattern: inclusion_regex,
            exclusion_pattern: exclusion_regex,
            num_partitions,
            replication_factor,
            other_configs,
        }
    }

    /// Creates a default TopicCreationGroup with default settings.
    pub fn default_group() -> Self {
        TopicCreationGroup::new(
            DEFAULT_TOPIC_CREATION_GROUP.to_string(),
            vec![], // Empty inclusion means match all
            vec![], // Empty exclusion means exclude none
            NO_PARTITIONS,
            NO_REPLICATION_FACTOR,
            HashMap::new(),
        )
    }

    /// Returns the name of this topic creation group.
    ///
    /// Corresponds to Java: TopicCreationGroup.name()
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Checks whether this topic creation group is configured to allow
    /// the creation of the given topic name.
    ///
    /// Returns true if the topic name matches the inclusion regex and
    /// does not match the exclusion regex.
    ///
    /// Corresponds to Java: TopicCreationGroup.matches(String topic)
    pub fn matches(&self, topic: &str) -> bool {
        !self.exclusion_pattern.is_match(topic) && self.inclusion_pattern.is_match(topic)
    }

    /// Returns the description for a new topic with the given name
    /// using the settings defined for this topic creation group.
    ///
    /// Corresponds to Java: TopicCreationGroup.newTopic(String topic)
    pub fn new_topic(&self, topic: &str) -> NewTopic {
        NewTopicBuilder::new(topic.to_string())
            .partitions(self.num_partitions)
            .replication_factor(self.replication_factor)
            .config(self.other_configs.clone())
            .build()
    }

    /// Returns the inclusion pattern.
    pub fn inclusion_pattern(&self) -> &Regex {
        &self.inclusion_pattern
    }

    /// Returns the exclusion pattern.
    pub fn exclusion_pattern(&self) -> &Regex {
        &self.exclusion_pattern
    }

    /// Returns the number of partitions.
    pub fn num_partitions(&self) -> i32 {
        self.num_partitions
    }

    /// Returns the replication factor.
    pub fn replication_factor(&self) -> i32 {
        self.replication_factor
    }

    /// Returns the other configs.
    pub fn other_configs(&self) -> &HashMap<String, String> {
        &self.other_configs
    }
}

impl std::fmt::Display for TopicCreationGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let configs_str = self
            .other_configs
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",");
        write!(
            f,
            "TopicCreationGroup{{name='{}', inclusionPattern={}, exclusionPattern={}, numPartitions={}, replicationFactor={}, otherConfigs={}}}",
            self.name,
            self.inclusion_pattern,
            self.exclusion_pattern,
            self.num_partitions,
            self.replication_factor,
            configs_str
        )
    }
}

impl PartialEq for TopicCreationGroup {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.num_partitions == other.num_partitions
            && self.replication_factor == other.replication_factor
            && self.inclusion_pattern.as_str() == other.inclusion_pattern.as_str()
            && self.exclusion_pattern.as_str() == other.exclusion_pattern.as_str()
            && self.other_configs == other.other_configs
    }
}

impl Eq for TopicCreationGroup {}

impl std::hash::Hash for TopicCreationGroup {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.num_partitions.hash(state);
        self.replication_factor.hash(state);
        self.inclusion_pattern.as_str().hash(state);
        self.exclusion_pattern.as_str().hash(state);
        // HashMap doesn't implement Hash directly, so we hash entries
        for (k, v) in &self.other_configs {
            k.hash(state);
            v.hash(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_creation_group_creation() {
        let group = TopicCreationGroup::new(
            "test-group".to_string(),
            vec!["topic-.*".to_string()],
            vec!["topic-exclude.*".to_string()],
            3,
            2,
            HashMap::new(),
        );

        assert_eq!(group.name(), "test-group");
        assert_eq!(group.num_partitions(), 3);
        assert_eq!(group.replication_factor(), 2);
    }

    #[test]
    fn test_topic_creation_group_matches() {
        let group = TopicCreationGroup::new(
            "test-group".to_string(),
            vec!["topic-.*".to_string()],
            vec!["topic-exclude.*".to_string()],
            3,
            2,
            HashMap::new(),
        );

        // Should match inclusion
        assert!(group.matches("topic-123"));

        // Should not match exclusion
        assert!(!group.matches("topic-exclude-123"));

        // Should not match neither inclusion nor exclusion
        assert!(!group.matches("other-topic"));
    }

    #[test]
    fn test_topic_creation_group_matches_empty_patterns() {
        let group = TopicCreationGroup::default_group();

        // Default group matches everything
        assert!(group.matches("any-topic"));
        assert!(group.matches("another-topic"));
    }

    #[test]
    fn test_topic_creation_group_new_topic() {
        let mut configs = HashMap::new();
        configs.insert("cleanup.policy".to_string(), "compact".to_string());

        let group = TopicCreationGroup::new(
            "test-group".to_string(),
            vec!["topic-.*".to_string()],
            vec![],
            3,
            2,
            configs,
        );

        let topic = group.new_topic("topic-test");
        assert_eq!(topic.name(), "topic-test");
        assert_eq!(topic.num_partitions(), 3);
        assert_eq!(topic.replication_factor(), 2);
    }

    #[test]
    fn test_topic_creation_group_equality() {
        let group1 = TopicCreationGroup::new(
            "test-group".to_string(),
            vec!["topic-.*".to_string()],
            vec![],
            3,
            2,
            HashMap::new(),
        );

        let group2 = TopicCreationGroup::new(
            "test-group".to_string(),
            vec!["topic-.*".to_string()],
            vec![],
            3,
            2,
            HashMap::new(),
        );

        assert_eq!(group1, group2);
    }

    #[test]
    fn test_topic_creation_group_inequality() {
        let group1 = TopicCreationGroup::new(
            "test-group".to_string(),
            vec!["topic-.*".to_string()],
            vec![],
            3,
            2,
            HashMap::new(),
        );

        let group2 = TopicCreationGroup::new(
            "other-group".to_string(),
            vec!["topic-.*".to_string()],
            vec![],
            3,
            2,
            HashMap::new(),
        );

        assert_ne!(group1, group2);
    }

    #[test]
    fn test_topic_creation_group_display() {
        let group = TopicCreationGroup::new(
            "test-group".to_string(),
            vec!["topic-.*".to_string()],
            vec![],
            3,
            2,
            HashMap::new(),
        );

        let display = format!("{}", group);
        assert!(display.contains("test-group"));
        assert!(display.contains("3"));
        assert!(display.contains("2"));
    }
}
