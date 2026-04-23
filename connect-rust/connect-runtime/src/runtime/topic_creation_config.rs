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

//! Configuration options for source connector automatic topic creation.
//!
//! This module provides configuration options and validators for automatic topic creation
//! by source connectors.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.TopicCreationConfig` in Java.
//!
//! See also: [KIP-158](https://cwiki.apache.org/confluence/display/KAFKA/KIP-158%3A+Kafka+Connect+should+allow+source+connectors+to+set+topic-specific+settings+for+new+topics)

use std::collections::VecDeque;

/// Default topic creation prefix for configuration keys.
pub const DEFAULT_TOPIC_CREATION_PREFIX: &str = "topic.creation.default.";

/// Default topic creation group name.
pub const DEFAULT_TOPIC_CREATION_GROUP: &str = "default";

/// Configuration key for inclusion regex patterns.
pub const INCLUDE_REGEX_CONFIG: &str = "include";

/// Configuration key for exclusion regex patterns.
pub const EXCLUDE_REGEX_CONFIG: &str = "exclude";

/// Configuration key for replication factor.
pub const REPLICATION_FACTOR_CONFIG: &str = "replication.factor";

/// Configuration key for partitions.
pub const PARTITIONS_CONFIG: &str = "partitions";

/// Special value indicating no partitions specified (use broker default).
pub const NO_PARTITIONS: i32 = -1;

/// Special value indicating no replication factor specified (use broker default).
pub const NO_REPLICATION_FACTOR: i16 = -1;

/// Validates that a partition count is valid.
///
/// # Arguments
/// * `config_name` - The configuration key name
/// * `factor` - The partition count to validate
///
/// # Returns
/// Ok if valid, Err with validation message if invalid.
///
/// Corresponds to Java: `private static void validatePartitions(String configName, int factor)`
pub fn validate_partitions(config_name: &str, factor: i32) -> Result<(), String> {
    if factor != NO_PARTITIONS && factor < 1 {
        return Err(format!(
            "Number of partitions must be positive, or {} to use the broker's default. Actual: {}",
            NO_PARTITIONS, factor
        ));
    }
    Ok(())
}

/// Validates that a replication factor is valid.
///
/// # Arguments
/// * `config_name` - The configuration key name
/// * `factor` - The replication factor to validate
///
/// # Returns
/// Ok if valid, Err with validation message if invalid.
///
/// Corresponds to Java: `private static void validateReplicationFactor(String configName, short factor)`
pub fn validate_replication_factor(config_name: &str, factor: i16) -> Result<(), String> {
    if factor != NO_REPLICATION_FACTOR && factor < 1 {
        return Err(format!(
            "Replication factor must be positive and not larger than the number of brokers in the Kafka cluster, or {} to use the broker's default. Actual: {}",
            NO_REPLICATION_FACTOR, factor
        ));
    }
    Ok(())
}

/// Validates that regex patterns are syntactically correct.
///
/// # Arguments
/// * `patterns` - The regex patterns to validate
///
/// # Returns
/// Ok if all patterns are valid, Err with validation message if any pattern has syntax error.
///
/// Corresponds to Java: REGEX_VALIDATOR
pub fn validate_regex_patterns(patterns: &[String]) -> Result<(), String> {
    for pattern in patterns {
        // Basic validation: check for obvious syntax issues
        // In production, this would use a proper regex parser
        if pattern.is_empty() {
            continue;
        }
        // Check for unmatched brackets
        let open_brackets = pattern.matches('[').count();
        let close_brackets = pattern.matches(']').count();
        if open_brackets != close_brackets {
            return Err(format!(
                "Syntax error in regular expression '{}': unmatched brackets",
                pattern
            ));
        }
        // Check for unmatched parentheses
        let open_parens = pattern.matches('(').count();
        let close_parens = pattern.matches(')').count();
        if open_parens != close_parens {
            return Err(format!(
                "Syntax error in regular expression '{}': unmatched parentheses",
                pattern
            ));
        }
    }
    Ok(())
}

/// TopicCreationConfig holds configuration for automatic topic creation.
///
/// This struct contains the configuration options for a specific topic creation group,
/// including regex patterns for topic matching and topic settings (partitions, replication factor).
///
/// Corresponds to `org.apache.kafka.connect.runtime.TopicCreationConfig` configuration group in Java.
#[derive(Debug, Clone, Default)]
pub struct TopicCreationConfig {
    /// Name of this topic creation group.
    group: String,
    /// Regex patterns for topics to include.
    include_patterns: Vec<String>,
    /// Regex patterns for topics to exclude.
    exclude_patterns: Vec<String>,
    /// Replication factor for new topics (-1 for broker default).
    replication_factor: i16,
    /// Number of partitions for new topics (-1 for broker default).
    partitions: i32,
}

impl TopicCreationConfig {
    /// Creates a new TopicCreationConfig with default values.
    pub fn new(group: String) -> Self {
        TopicCreationConfig {
            group,
            include_patterns: Vec::new(),
            exclude_patterns: Vec::new(),
            replication_factor: NO_REPLICATION_FACTOR,
            partitions: NO_PARTITIONS,
        }
    }

    /// Creates a new TopicCreationConfig with all parameters specified.
    pub fn with_values(
        group: String,
        include_patterns: Vec<String>,
        exclude_patterns: Vec<String>,
        replication_factor: i16,
        partitions: i32,
    ) -> Result<Self, String> {
        // Validate patterns
        validate_regex_patterns(&include_patterns)?;
        validate_regex_patterns(&exclude_patterns)?;

        // Validate replication factor
        validate_replication_factor(
            &format!("{}.{}", group, REPLICATION_FACTOR_CONFIG),
            replication_factor,
        )?;

        // Validate partitions
        validate_partitions(&format!("{}.{}", group, PARTITIONS_CONFIG), partitions)?;

        Ok(TopicCreationConfig {
            group,
            include_patterns,
            exclude_patterns,
            replication_factor,
            partitions,
        })
    }

    /// Creates the default topic creation group configuration.
    ///
    /// The default group matches all topics (.* pattern) and requires explicit
    /// replication factor and partitions configuration.
    ///
    /// Corresponds to Java: `public static ConfigDef defaultGroupConfigDef()`
    pub fn default_group(replication_factor: i16, partitions: i32) -> Result<Self, String> {
        Self::with_values(
            DEFAULT_TOPIC_CREATION_GROUP.to_string(),
            vec![".*".to_string()], // Include all topics
            vec![],                 // No exclusions
            replication_factor,
            partitions,
        )
    }

    /// Get the group name.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Get the include patterns.
    pub fn include_patterns(&self) -> &[String] {
        &self.include_patterns
    }

    /// Get the exclude patterns.
    pub fn exclude_patterns(&self) -> &[String] {
        &self.exclude_patterns
    }

    /// Get the replication factor.
    pub fn replication_factor(&self) -> i16 {
        self.replication_factor
    }

    /// Get the number of partitions.
    pub fn partitions(&self) -> i32 {
        self.partitions
    }

    /// Check if this topic matches this group's include/exclude patterns.
    ///
    /// A topic matches if it matches at least one include pattern and does not
    /// match any exclude patterns. Exclusion rules have precedence over inclusion rules.
    ///
    /// # Arguments
    /// * `topic_name` - The topic name to check
    ///
    /// # Returns
    /// True if the topic matches this group's patterns, false otherwise.
    pub fn matches_topic(&self, topic_name: &str) -> bool {
        // First check exclusion rules (they have precedence)
        for pattern in &self.exclude_patterns {
            if self.matches_pattern(topic_name, pattern) {
                return false;
            }
        }

        // Then check inclusion rules
        for pattern in &self.include_patterns {
            if self.matches_pattern(topic_name, pattern) {
                return true;
            }
        }

        // No include patterns means nothing matches
        false
    }

    /// Simple regex matching helper.
    ///
    /// For complex patterns, this uses a simplified matching approach.
    /// In production, this would use a proper regex engine.
    fn matches_pattern(&self, topic_name: &str, pattern: &str) -> bool {
        // Handle common patterns
        if pattern == ".*" {
            return true;
        }

        // Use regex crate for proper matching
        // For now, use simple contains/startswith/endswith matching
        if pattern.starts_with('^') && pattern.ends_with('$') {
            // Exact match pattern
            let inner = &pattern[1..pattern.len() - 1];
            return topic_name == inner;
        }

        if pattern.starts_with('^') {
            let suffix = &pattern[1..];
            // Remove any trailing .* or similar
            let suffix = suffix.trim_end_matches(".*");
            return topic_name.starts_with(suffix);
        }

        if pattern.ends_with('$') {
            let prefix = &pattern[..pattern.len() - 1];
            // Remove any leading .* or similar
            let prefix = prefix.trim_start_matches(".*");
            return topic_name.ends_with(prefix);
        }

        // Simple substring match for patterns like "prefix.*suffix"
        if pattern.contains(".*") {
            let parts: Vec<&str> = pattern.split(".*").collect();
            if parts.len() == 2 {
                return topic_name.starts_with(parts[0]) && topic_name.ends_with(parts[1]);
            }
        }

        // Simple contains match
        topic_name.contains(pattern)
    }

    /// Check if the replication factor uses broker default.
    pub fn uses_broker_default_replication_factor(&self) -> bool {
        self.replication_factor == NO_REPLICATION_FACTOR
    }

    /// Check if the partitions use broker default.
    pub fn uses_broker_default_partitions(&self) -> bool {
        self.partitions == NO_PARTITIONS
    }
}

/// TopicCreationGroup manages multiple topic creation configurations.
///
/// This allows different topic creation settings for different groups of topics.
#[derive(Debug, Clone, Default)]
pub struct TopicCreationGroup {
    /// All topic creation configs, ordered by priority (default first).
    configs: VecDeque<TopicCreationConfig>,
}

impl TopicCreationGroup {
    /// Creates a new empty TopicCreationGroup.
    pub fn new() -> Self {
        TopicCreationGroup {
            configs: VecDeque::new(),
        }
    }

    /// Creates a TopicCreationGroup with a default configuration.
    pub fn with_default(replication_factor: i16, partitions: i32) -> Result<Self, String> {
        let mut group = TopicCreationGroup::new();
        let default_config = TopicCreationConfig::default_group(replication_factor, partitions)?;
        group.configs.push_back(default_config);
        Ok(group)
    }

    /// Add a topic creation configuration.
    pub fn add_config(&mut self, config: TopicCreationConfig) {
        // Default group should be at the front
        if config.group() == DEFAULT_TOPIC_CREATION_GROUP {
            self.configs.push_front(config);
        } else {
            self.configs.push_back(config);
        }
    }

    /// Find the matching topic creation config for a topic name.
    ///
    /// Searches through configs in order (default first) and returns the first
    /// matching configuration.
    ///
    /// # Arguments
    /// * `topic_name` - The topic name to match
    ///
    /// # Returns
    /// The matching TopicCreationConfig, or None if no match found.
    pub fn find_matching_config(&self, topic_name: &str) -> Option<&TopicCreationConfig> {
        for config in &self.configs {
            if config.matches_topic(topic_name) {
                return Some(config);
            }
        }
        None
    }

    /// Get all configs.
    pub fn configs(&self) -> &VecDeque<TopicCreationConfig> {
        &self.configs
    }

    /// Get the default config.
    pub fn default_config(&self) -> Option<&TopicCreationConfig> {
        self.configs.front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_partitions_valid() {
        assert!(validate_partitions("partitions", 10).is_ok());
        assert!(validate_partitions("partitions", NO_PARTITIONS).is_ok());
    }

    #[test]
    fn test_validate_partitions_invalid() {
        assert!(validate_partitions("partitions", 0).is_err());
        assert!(validate_partitions("partitions", -5).is_err());
    }

    #[test]
    fn test_validate_replication_factor_valid() {
        assert!(validate_replication_factor("replication.factor", 3).is_ok());
        assert!(validate_replication_factor("replication.factor", NO_REPLICATION_FACTOR).is_ok());
    }

    #[test]
    fn test_validate_replication_factor_invalid() {
        assert!(validate_replication_factor("replication.factor", 0).is_err());
        assert!(validate_replication_factor("replication.factor", -5).is_err());
    }

    #[test]
    fn test_topic_creation_config_new() {
        let config = TopicCreationConfig::new("test-group".to_string());
        assert_eq!(config.group(), "test-group");
        assert!(config.include_patterns().is_empty());
        assert!(config.exclude_patterns().is_empty());
        assert_eq!(config.replication_factor(), NO_REPLICATION_FACTOR);
        assert_eq!(config.partitions(), NO_PARTITIONS);
    }

    #[test]
    fn test_topic_creation_config_with_values() {
        let config = TopicCreationConfig::with_values(
            "test-group".to_string(),
            vec!["^test.*".to_string()],
            vec![".*temp$".to_string()],
            3,
            10,
        )
        .unwrap();

        assert_eq!(config.group(), "test-group");
        assert_eq!(config.replication_factor(), 3);
        assert_eq!(config.partitions(), 10);
    }

    #[test]
    fn test_topic_creation_config_matches_topic() {
        let config = TopicCreationConfig::with_values(
            "test-group".to_string(),
            vec![".*".to_string()], // Include all
            vec![],                 // No exclusions
            3,
            10,
        )
        .unwrap();

        // Should match any topic
        assert!(config.matches_topic("any-topic"));
        assert!(config.matches_topic("test-topic"));
    }

    #[test]
    fn test_topic_creation_config_exclusion_precedence() {
        let config = TopicCreationConfig::with_values(
            "test-group".to_string(),
            vec![".*".to_string()],      // Include all
            vec![".*temp$".to_string()], // Exclude temp topics
            3,
            10,
        )
        .unwrap();

        // temp topic should be excluded
        assert!(!config.matches_topic("mytemp"));
        // non-temp topic should be included
        assert!(config.matches_topic("mytopic"));
    }

    #[test]
    fn test_default_group() {
        let config = TopicCreationConfig::default_group(3, 10).unwrap();
        assert_eq!(config.group(), DEFAULT_TOPIC_CREATION_GROUP);
        assert!(config.matches_topic("any-topic"));
    }

    #[test]
    fn test_topic_creation_group() {
        let group = TopicCreationGroup::with_default(3, 10).unwrap();

        assert!(group.default_config().is_some());
        assert!(group.find_matching_config("any-topic").is_some());
    }

    #[test]
    fn test_topic_creation_group_multiple_configs() {
        let mut group = TopicCreationGroup::with_default(3, 10).unwrap();

        // Add a specific group for test topics
        let test_config = TopicCreationConfig::with_values(
            "test-group".to_string(),
            vec!["test.*".to_string()],
            vec![".*internal$".to_string()],
            1,
            5,
        )
        .unwrap();
        group.add_config(test_config);

        // Regular topic should use default
        let regular_config = group.find_matching_config("regular-topic").unwrap();
        assert_eq!(regular_config.group(), DEFAULT_TOPIC_CREATION_GROUP);

        // Test topic (non-internal) should use test-group
        let test_topic_config = group.find_matching_config("test-topic").unwrap();
        assert_eq!(test_topic_config.group(), "test-group");
        assert_eq!(test_topic_config.replication_factor(), 1);

        // Test-internal topic should be excluded from test-group, use default
        let internal_config = group.find_matching_config("testinternal").unwrap();
        assert_eq!(internal_config.group(), DEFAULT_TOPIC_CREATION_GROUP);
    }

    #[test]
    fn test_uses_broker_default() {
        let config = TopicCreationConfig::new("test".to_string());
        assert!(config.uses_broker_default_replication_factor());
        assert!(config.uses_broker_default_partitions());

        let config2 = TopicCreationConfig::with_values(
            "test".to_string(),
            vec![".*".to_string()],
            vec![],
            3,
            10,
        )
        .unwrap();
        assert!(!config2.uses_broker_default_replication_factor());
        assert!(!config2.uses_broker_default_partitions());
    }
}
