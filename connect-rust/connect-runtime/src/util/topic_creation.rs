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

//! Utility to be used by worker source tasks in order to create topics, if topic creation is
//! enabled for source connectors at the worker and the connector configurations.
//!
//! This corresponds to `org.apache.kafka.connect.util.TopicCreation` in Java.

use std::collections::HashSet;
use std::sync::OnceLock;

use indexmap::IndexMap;

use super::topic_creation_group::{TopicCreationGroup, DEFAULT_TOPIC_CREATION_GROUP};
use crate::config::worker_config::WorkerConfig;

/// Utility to be used by worker source tasks in order to create topics, if topic creation is
/// enabled for source connectors at the worker and the connector configurations.
///
/// This corresponds to `org.apache.kafka.connect.util.TopicCreation` in Java.
#[derive(Debug, Clone)]
pub struct TopicCreation {
    /// Whether topic creation is enabled
    is_topic_creation_enabled: bool,
    /// The default topic creation group (None if topic creation is disabled)
    default_topic_group: Option<TopicCreationGroup>,
    /// Map of topic creation groups (excluding the default group).
    /// Uses IndexMap to preserve insertion order, matching Java's LinkedHashMap semantics
    /// for deterministic find_first_group behavior.
    topic_groups: IndexMap<String, TopicCreationGroup>,
    /// Cache of topics that have been created/added
    topic_cache: HashSet<String>,
}

/// Singleton EMPTY instance representing the state when topic creation is disabled.
/// Corresponds to Java: TopicCreation.EMPTY
static EMPTY: OnceLock<TopicCreation> = OnceLock::new();

impl TopicCreation {
    /// Creates a new TopicCreation instance.
    ///
    /// This corresponds to the protected constructor in Java:
    /// `TopicCreation(boolean isTopicCreationEnabled, TopicCreationGroup defaultTopicGroup,
    ///                Map<String, TopicCreationGroup> topicGroups, Set<String> topicCache)`
    fn new(
        is_topic_creation_enabled: bool,
        default_topic_group: Option<TopicCreationGroup>,
        topic_groups: IndexMap<String, TopicCreationGroup>,
        topic_cache: HashSet<String>,
    ) -> Self {
        TopicCreation {
            is_topic_creation_enabled,
            default_topic_group,
            topic_groups,
            topic_cache,
        }
    }

    /// Creates a new TopicCreation instance from WorkerConfig and topic groups.
    ///
    /// This corresponds to Java: `TopicCreation.newTopicCreation(WorkerConfig, Map<String, TopicCreationGroup>)`
    ///
    /// Returns EMPTY if:
    /// - `workerConfig.topicCreationEnable()` is false
    /// - `topicGroups` is None
    ///
    /// Otherwise, creates a TopicCreation with:
    /// - The default topic group extracted from topicGroups
    /// - Remaining groups (excluding default) stored in topic_groups in insertion order
    /// - An empty topic_cache
    pub fn new_topic_creation(
        worker_config: &WorkerConfig,
        topic_groups: Option<IndexMap<String, TopicCreationGroup>>,
    ) -> TopicCreation {
        if !worker_config.topic_creation_enable() || topic_groups.is_none() {
            return TopicCreation::empty();
        }

        let groups = topic_groups.unwrap();
        let default_topic_group = groups.get(DEFAULT_TOPIC_CREATION_GROUP).cloned();

        // Remove the default group from the groups map while preserving insertion order
        // This matches Java's LinkedHashMap behavior where order is preserved after removal
        let remaining_groups: IndexMap<String, TopicCreationGroup> = groups
            .iter()
            .filter(|(name, _)| *name != DEFAULT_TOPIC_CREATION_GROUP)
            .map(|(name, group)| (name.clone(), group.clone()))
            .collect();

        TopicCreation::new(true, default_topic_group, remaining_groups, HashSet::new())
    }

    /// Returns an instance of this utility that represents what the state of the internal data
    /// structures should be when topic creation is disabled.
    ///
    /// This corresponds to Java: `TopicCreation.empty()`
    ///
    /// # Returns
    /// The utility when topic creation is disabled (singleton EMPTY instance)
    pub fn empty() -> TopicCreation {
        EMPTY
            .get_or_init(|| TopicCreation::new(false, None, IndexMap::new(), HashSet::new()))
            .clone()
    }

    /// Check whether topic creation is enabled for this utility instance.
    /// This state is set at instantiation time and remains unchanged for the lifetime
    /// of every TopicCreation object.
    ///
    /// This corresponds to Java: `TopicCreation.isTopicCreationEnabled()`
    ///
    /// # Returns
    /// true if topic creation is enabled; false otherwise
    pub fn is_topic_creation_enabled(&self) -> bool {
        self.is_topic_creation_enabled
    }

    /// Check whether topic creation may be required for a specific topic name.
    ///
    /// This corresponds to Java: `TopicCreation.isTopicCreationRequired(String topic)`
    ///
    /// # Returns
    /// true if topic creation is enabled and the topic name is not in the topic cache;
    /// false otherwise
    pub fn is_topic_creation_required(&self, topic: &str) -> bool {
        self.is_topic_creation_enabled && !self.topic_cache.contains(topic)
    }

    /// Return the default topic creation group.
    /// This group is always defined when topic creation is enabled but is `None` if
    /// topic creation is disabled.
    ///
    /// This corresponds to Java: `TopicCreation.defaultTopicGroup()`
    ///
    /// # Returns
    /// The default topic creation group if topic creation is enabled; None otherwise
    pub fn default_topic_group(&self) -> Option<&TopicCreationGroup> {
        self.default_topic_group.as_ref()
    }

    /// Return the topic creation groups defined for a source connector as a map of
    /// topic creation group name to topic creation group instance.
    /// This map maintains all the optionally defined groups besides the default group
    /// which is defined for any connector when topic creation is enabled.
    ///
    /// This corresponds to Java: `TopicCreation.topicGroups()`
    ///
    /// # Returns
    /// The map of all the topic creation groups besides the default group; may be empty
    /// The order is preserved (insertion order, matching Java LinkedHashMap semantics)
    pub fn topic_groups(&self) -> &IndexMap<String, TopicCreationGroup> {
        &self.topic_groups
    }

    /// Inform this utility instance that a topic has been created and its creation will no
    /// longer be required. After this method is called for a given `topic`, any subsequent
    /// calls to `is_topic_creation_required` will return `false` for the same topic.
    ///
    /// This corresponds to Java: `TopicCreation.addTopic(String topic)`
    ///
    /// # Parameters
    /// - `topic`: The topic name to mark as created
    pub fn add_topic(&mut self, topic: String) {
        if self.is_topic_creation_enabled {
            self.topic_cache.insert(topic);
        }
    }

    /// Get the first topic creation group that is configured to match the given `topic`
    /// name. If topic creation is enabled, any topic should match at least the default
    /// topic creation group.
    ///
    /// This corresponds to Java: `TopicCreation.findFirstGroup(String topic)`
    ///
    /// # Parameters
    /// - `topic`: The topic name to match against group configurations
    ///
    /// # Returns
    /// The first group that matches the given topic, or the default group if no match found
    pub fn find_first_group(&self, topic: &str) -> Option<&TopicCreationGroup> {
        // Iterate through topic_groups values and find first matching group
        for group in self.topic_groups.values() {
            if group.matches(topic) {
                return Some(group);
            }
        }
        // Fall back to default topic group
        self.default_topic_group.as_ref()
    }
}

impl PartialEq for TopicCreation {
    fn eq(&self, other: &Self) -> bool {
        self.is_topic_creation_enabled == other.is_topic_creation_enabled
            && self.default_topic_group == other.default_topic_group
            && self.topic_groups == other.topic_groups
            && self.topic_cache == other.topic_cache
    }
}

impl Eq for TopicCreation {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::worker_config::WorkerConfig;
    use serde_json::Value;
    use std::collections::HashMap;

    fn create_test_worker_config_with_topic_creation(enabled: bool) -> WorkerConfig {
        let mut props = HashMap::new();
        props.insert(
            crate::config::worker_config::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            Value::String("localhost:9092".to_string()),
        );
        props.insert(
            crate::config::worker_config::KEY_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("JsonConverter".to_string()),
        );
        props.insert(
            crate::config::worker_config::VALUE_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("JsonConverter".to_string()),
        );
        props.insert(
            crate::config::worker_config::TOPIC_CREATION_ENABLE_CONFIG.to_string(),
            Value::Bool(enabled),
        );
        WorkerConfig::new_unvalidated(props)
    }

    fn create_topic_groups() -> IndexMap<String, TopicCreationGroup> {
        let mut groups = IndexMap::new();

        // Default group
        groups.insert(
            DEFAULT_TOPIC_CREATION_GROUP.to_string(),
            TopicCreationGroup::default_group(),
        );

        // Custom group for specific topics
        groups.insert(
            "logs".to_string(),
            TopicCreationGroup::new(
                "logs".to_string(),
                vec!["log-.*".to_string()],
                vec![],
                3,
                2,
                HashMap::new(),
            ),
        );

        groups
    }

    /// Creates topic groups with multiple groups that can match the same topic,
    /// ordered so we can test deterministic first-match behavior.
    fn create_ordered_topic_groups_for_first_match() -> IndexMap<String, TopicCreationGroup> {
        let mut groups = IndexMap::new();

        // Default group (matches everything)
        groups.insert(
            DEFAULT_TOPIC_CREATION_GROUP.to_string(),
            TopicCreationGroup::default_group(),
        );

        // Group1: matches "test-" prefix (inserted first among custom groups)
        groups.insert(
            "group1".to_string(),
            TopicCreationGroup::new(
                "group1".to_string(),
                vec!["test-.*".to_string()],
                vec![],
                3,
                2,
                HashMap::new(),
            ),
        );

        // Group2: matches "test-special-" prefix (inserted second)
        groups.insert(
            "group2".to_string(),
            TopicCreationGroup::new(
                "group2".to_string(),
                vec!["test-special-.*".to_string()],
                vec![],
                5,
                3,
                HashMap::new(),
            ),
        );

        // Group3: matches "test-" prefix (inserted third - same pattern as group1)
        groups.insert(
            "group3".to_string(),
            TopicCreationGroup::new(
                "group3".to_string(),
                vec!["test-.*".to_string()],
                vec![],
                7,
                4,
                HashMap::new(),
            ),
        );

        groups
    }

    #[test]
    fn test_empty_singleton() {
        let empty1 = TopicCreation::empty();
        let empty2 = TopicCreation::empty();

        assert!(!empty1.is_topic_creation_enabled());
        assert!(!empty2.is_topic_creation_enabled());

        // Both should have same disabled state
        assert_eq!(
            empty1.is_topic_creation_enabled,
            empty2.is_topic_creation_enabled
        );
        assert!(empty1.default_topic_group().is_none());
        assert!(empty1.topic_groups().is_empty());
    }

    #[test]
    fn test_new_topic_creation_disabled() {
        let worker_config = create_test_worker_config_with_topic_creation(false);
        let topic_groups = create_topic_groups();

        let creation = TopicCreation::new_topic_creation(&worker_config, Some(topic_groups));

        assert!(!creation.is_topic_creation_enabled());
        assert!(creation.default_topic_group().is_none());
        assert!(creation.topic_groups().is_empty());
    }

    #[test]
    fn test_new_topic_creation_none_groups() {
        let worker_config = create_test_worker_config_with_topic_creation(true);

        let creation = TopicCreation::new_topic_creation(&worker_config, None);

        assert!(!creation.is_topic_creation_enabled());
        assert!(creation.default_topic_group().is_none());
    }

    #[test]
    fn test_new_topic_creation_enabled() {
        let worker_config = create_test_worker_config_with_topic_creation(true);
        let topic_groups = create_topic_groups();

        let creation = TopicCreation::new_topic_creation(&worker_config, Some(topic_groups));

        assert!(creation.is_topic_creation_enabled());
        assert!(creation.default_topic_group().is_some());

        // The default group should be extracted and stored separately
        let default_group = creation.default_topic_group().unwrap();
        assert_eq!(default_group.name(), DEFAULT_TOPIC_CREATION_GROUP);

        // The remaining groups should exclude the default
        assert_eq!(creation.topic_groups().len(), 1);
        assert!(creation.topic_groups().contains_key("logs"));
        assert!(!creation
            .topic_groups()
            .contains_key(DEFAULT_TOPIC_CREATION_GROUP));
    }

    #[test]
    fn test_is_topic_creation_required() {
        let worker_config = create_test_worker_config_with_topic_creation(true);
        let topic_groups = create_topic_groups();

        let mut creation = TopicCreation::new_topic_creation(&worker_config, Some(topic_groups));

        // Initially, topic creation is required for any topic
        assert!(creation.is_topic_creation_required("new-topic"));
        assert!(creation.is_topic_creation_required("another-topic"));

        // After adding a topic, it's no longer required
        creation.add_topic("new-topic".to_string());
        assert!(!creation.is_topic_creation_required("new-topic"));
        assert!(creation.is_topic_creation_required("another-topic"));
    }

    #[test]
    fn test_is_topic_creation_required_disabled() {
        let creation = TopicCreation::empty();

        // When disabled, topic creation is never required
        assert!(!creation.is_topic_creation_required("any-topic"));

        // Adding topic should not affect cache when disabled
        let mut creation_mut = creation;
        creation_mut.add_topic("some-topic".to_string());
        assert!(!creation_mut.is_topic_creation_required("some-topic"));
    }

    #[test]
    fn test_add_topic_only_when_enabled() {
        let worker_config = create_test_worker_config_with_topic_creation(true);
        let topic_groups = create_topic_groups();

        let mut creation = TopicCreation::new_topic_creation(&worker_config, Some(topic_groups));

        creation.add_topic("topic1".to_string());
        creation.add_topic("topic2".to_string());

        assert!(!creation.is_topic_creation_required("topic1"));
        assert!(!creation.is_topic_creation_required("topic2"));
        assert!(creation.is_topic_creation_required("topic3"));
    }

    #[test]
    fn test_find_first_group_matching_custom() {
        let worker_config = create_test_worker_config_with_topic_creation(true);
        let topic_groups = create_topic_groups();

        let creation = TopicCreation::new_topic_creation(&worker_config, Some(topic_groups));

        // Topic matching the 'logs' group pattern
        let group = creation.find_first_group("log-app");
        assert!(group.is_some());
        assert_eq!(group.unwrap().name(), "logs");

        // Another topic matching the logs pattern
        let group = creation.find_first_group("log-system");
        assert!(group.is_some());
        assert_eq!(group.unwrap().name(), "logs");
    }

    /// Test that find_first_group returns the first matching group in insertion order,
    /// matching Java's LinkedHashMap semantics.
    #[test]
    fn test_find_first_group_ordered_first_match() {
        let worker_config = create_test_worker_config_with_topic_creation(true);
        let topic_groups = create_ordered_topic_groups_for_first_match();

        let creation = TopicCreation::new_topic_creation(&worker_config, Some(topic_groups));

        // Topic "test-abc" matches both group1 and group3 (both have pattern "test-.*")
        // Should return group1 because it was inserted first (IndexMap preserves order)
        let group = creation.find_first_group("test-abc");
        assert!(group.is_some());
        assert_eq!(group.unwrap().name(), "group1");
        assert_eq!(group.unwrap().num_partitions(), 3);

        // Topic "test-special-xyz" matches all three: group1, group2, and group3
        // group2 pattern is "test-special-.*" which also matches this topic
        // But group1 is first in order, so it should be returned
        let group = creation.find_first_group("test-special-xyz");
        assert!(group.is_some());
        assert_eq!(group.unwrap().name(), "group1");
        assert_eq!(group.unwrap().num_partitions(), 3);

        // Topic "random-topic" doesn't match any custom group
        // Should fallback to default group
        let group = creation.find_first_group("random-topic");
        assert!(group.is_some());
        assert_eq!(group.unwrap().name(), DEFAULT_TOPIC_CREATION_GROUP);
    }

    #[test]
    fn test_find_first_group_fallback_to_default() {
        let worker_config = create_test_worker_config_with_topic_creation(true);
        let topic_groups = create_topic_groups();

        let creation = TopicCreation::new_topic_creation(&worker_config, Some(topic_groups));

        // Topic not matching any custom group - should fallback to default
        let group = creation.find_first_group("random-topic");
        assert!(group.is_some());
        assert_eq!(group.unwrap().name(), DEFAULT_TOPIC_CREATION_GROUP);
    }

    #[test]
    fn test_find_first_group_disabled() {
        let creation = TopicCreation::empty();

        // When disabled, find_first_group returns None
        let group = creation.find_first_group("any-topic");
        assert!(group.is_none());
    }

    #[test]
    fn test_topic_groups_returns_reference() {
        let worker_config = create_test_worker_config_with_topic_creation(true);
        let topic_groups = create_topic_groups();

        let creation = TopicCreation::new_topic_creation(&worker_config, Some(topic_groups));

        let groups_ref = creation.topic_groups();
        assert!(groups_ref.contains_key("logs"));
    }

    #[test]
    fn test_default_topic_group_when_enabled() {
        let worker_config = create_test_worker_config_with_topic_creation(true);
        let topic_groups = create_topic_groups();

        let creation = TopicCreation::new_topic_creation(&worker_config, Some(topic_groups));

        let default = creation.default_topic_group();
        assert!(default.is_some());
        assert_eq!(default.unwrap().name(), DEFAULT_TOPIC_CREATION_GROUP);
    }

    #[test]
    fn test_default_topic_group_when_disabled() {
        let creation = TopicCreation::empty();

        let default = creation.default_topic_group();
        assert!(default.is_none());
    }

    #[test]
    fn test_empty_stable_behavior() {
        // Test that empty() returns consistent behavior across multiple calls
        let empty1 = TopicCreation::empty();
        let empty2 = TopicCreation::empty();

        // Same state
        assert!(!empty1.is_topic_creation_enabled());
        assert!(!empty2.is_topic_creation_enabled());

        // Both return None for default group
        assert!(empty1.default_topic_group().is_none());
        assert!(empty2.default_topic_group().is_none());

        // Both return empty topic_groups
        assert!(empty1.topic_groups().is_empty());
        assert!(empty2.topic_groups().is_empty());

        // is_topic_creation_required always returns false
        assert!(!empty1.is_topic_creation_required("any-topic"));
        assert!(!empty2.is_topic_creation_required("any-topic"));

        // find_first_group returns None
        assert!(empty1.find_first_group("any-topic").is_none());
        assert!(empty2.find_first_group("any-topic").is_none());
    }

    #[test]
    fn test_eq() {
        let worker_config = create_test_worker_config_with_topic_creation(true);
        let topic_groups = create_topic_groups();

        let creation1 =
            TopicCreation::new_topic_creation(&worker_config, Some(topic_groups.clone()));
        let creation2 = TopicCreation::new_topic_creation(&worker_config, Some(topic_groups));

        // Two TopicCreation instances with same config should be equal
        assert_eq!(creation1, creation2);

        // Empty instances should be equal
        let empty1 = TopicCreation::empty();
        let empty2 = TopicCreation::empty();
        assert_eq!(empty1, empty2);

        // Enabled and disabled should not be equal
        assert_ne!(creation1, empty1);
    }
}
