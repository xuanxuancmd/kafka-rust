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

//! TopicFilter trait for MirrorMaker.
//!
//! Defines which topics should be replicated.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.TopicFilter

use std::collections::HashMap;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java DefaultTopicFilter
// ============================================================================

/// Configuration key for topics to include.
/// Corresponds to Java: DefaultTopicFilter.TOPICS_INCLUDE_CONFIG
pub const TOPICS_INCLUDE_CONFIG: &str = "topics";

/// Default value for topics to include - match all topics.
/// Corresponds to Java: DefaultTopicFilter.TOPICS_INCLUDE_DEFAULT
pub const TOPICS_INCLUDE_DEFAULT: &str = ".*";

/// Configuration key for topics to exclude.
/// Corresponds to Java: DefaultTopicFilter.TOPICS_EXCLUDE_CONFIG
pub const TOPICS_EXCLUDE_CONFIG: &str = "topics.exclude";

/// Default value for topics to exclude - exclude internal topics.
/// Corresponds to Java: DefaultTopicFilter.TOPICS_EXCLUDE_DEFAULT
pub const TOPICS_EXCLUDE_DEFAULT: &str =
    ".*\\.internal$,.*\\.replicas$,(__consumer_offsets|__transaction_state|__consumer_timestamps|__cluster_metadata).*";

// ============================================================================
// TopicFilter Trait
// ============================================================================

/// Trait for filtering topics to replicate.
///
/// Defines which topics should be replicated from the source cluster
/// to the target cluster.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.TopicFilter
pub trait TopicFilter: Send + Sync {
    /// Determines whether a given topic should be replicated.
    ///
    /// Corresponds to Java: TopicFilter.shouldReplicateTopic(String topic)
    fn should_replicate_topic(&self, topic: &str) -> bool;

    /// Configures the filter with the given properties.
    ///
    /// Corresponds to Java: TopicFilter.configure(Map<String, ?> props)
    fn configure(&mut self, props: &HashMap<String, String>);

    /// Closes the filter and releases any resources.
    ///
    /// Corresponds to Java: TopicFilter.close()
    fn close(&mut self) {}
}

// ============================================================================
// DefaultTopicFilter
// ============================================================================

/// Default implementation of TopicFilter using regex patterns.
///
/// This filter uses regex patterns to determine which topics should be
/// replicated. Topics matching the include pattern and not matching the
/// exclude pattern will be replicated.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.DefaultTopicFilter
pub struct DefaultTopicFilter {
    /// Regex pattern for topics to include
    include_pattern: String,
    /// Regex pattern for topics to exclude
    exclude_pattern: String,
}

impl Default for DefaultTopicFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultTopicFilter {
    /// Creates a new DefaultTopicFilter with default patterns.
    pub fn new() -> Self {
        DefaultTopicFilter {
            include_pattern: TOPICS_INCLUDE_DEFAULT.to_string(),
            exclude_pattern: TOPICS_EXCLUDE_DEFAULT.to_string(),
        }
    }

    /// Creates a new DefaultTopicFilter with custom patterns.
    pub fn with_patterns(include: String, exclude: String) -> Self {
        DefaultTopicFilter {
            include_pattern: include,
            exclude_pattern: exclude,
        }
    }

    /// Checks if a topic matches the include pattern.
    fn matches_include(&self, topic: &str) -> bool {
        // Simplified regex matching: check if topic matches the pattern
        // For the default pattern ".*", all topics match
        if self.include_pattern == ".*" {
            return true;
        }

        // Simple pattern matching for common cases
        // Handle comma-separated list of patterns
        for pattern in self.include_pattern.split(',') {
            let trimmed = pattern.trim();
            if Self::matches_pattern(topic, trimmed) {
                return true;
            }
        }
        false
    }

    /// Checks if a topic matches the exclude pattern.
    fn matches_exclude(&self, topic: &str) -> bool {
        // Handle comma-separated list of patterns
        for pattern in self.exclude_pattern.split(',') {
            let trimmed = pattern.trim();
            if Self::matches_pattern(topic, trimmed) {
                return true;
            }
        }
        false
    }

    /// Simple pattern matching function.
    /// Supports basic regex patterns like .* and literal matches.
    fn matches_pattern(text: &str, pattern: &str) -> bool {
        // Handle exact match
        if pattern == text {
            return true;
        }

        // Handle .* wildcard at end (prefix match)
        if pattern.ends_with(".*") {
            let prefix = &pattern[..pattern.len() - 2];
            if text.starts_with(prefix) {
                return true;
            }
        }

        // Handle .* at start (suffix match)
        if pattern.starts_with(".*") {
            let suffix = &pattern[2..];
            if text.ends_with(suffix) {
                return true;
            }
        }

        // Handle .* on both sides (contains match)
        if pattern.starts_with(".*") && pattern.ends_with(".*") {
            let middle = &pattern[2..pattern.len() - 2];
            if text.contains(middle) {
                return true;
            }
        }

        // Handle patterns ending with $ (exact suffix match for regex)
        if pattern.ends_with('$') {
            let suffix = &pattern[..pattern.len() - 1];
            if suffix.ends_with(".*") {
                let prefix = &suffix[..suffix.len() - 2];
                if text.starts_with(prefix) {
                    return text.len() >= prefix.len();
                }
            } else if text == suffix {
                return true;
            }
        }

        // Handle .*\.(pattern)$ for internal topics
        if pattern.contains(".*\\.") && pattern.ends_with("$") {
            // Extract the suffix pattern (e.g., "internal" from ".*\\.internal$")
            let dot_idx = pattern.find(".*\\.");
            if let Some(idx) = dot_idx {
                let suffix_pattern = &pattern[idx + 4..pattern.len() - 1]; // Skip ".*\."
                if text.ends_with(suffix_pattern) && text.len() > suffix_pattern.len() {
                    return true;
                }
            }
        }

        // Handle patterns like "(topic1|topic2).*"
        if pattern.starts_with('(') && pattern.contains(").*") {
            let end_paren = pattern.find(')');
            if let Some(idx) = end_paren {
                let alternatives = &pattern[1..idx];
                for alt in alternatives.split('|') {
                    if text.starts_with(alt) {
                        return true;
                    }
                }
            }
        }

        // Handle patterns like "__topic.*" for internal topics
        if pattern.starts_with("__") && pattern.ends_with(".*") {
            let prefix = &pattern[..pattern.len() - 2];
            if text.starts_with(prefix) {
                return true;
            }
        }

        false
    }
}

impl TopicFilter for DefaultTopicFilter {
    fn should_replicate_topic(&self, topic: &str) -> bool {
        // Topic should be replicated if it matches include and does not match exclude
        self.matches_include(topic) && !self.matches_exclude(topic)
    }

    fn configure(&mut self, props: &HashMap<String, String>) {
        if let Some(include) = props.get(TOPICS_INCLUDE_CONFIG) {
            self.include_pattern = include.clone();
        }
        if let Some(exclude) = props.get(TOPICS_EXCLUDE_CONFIG) {
            self.exclude_pattern = exclude.clone();
        }
    }
}

impl std::fmt::Debug for DefaultTopicFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultTopicFilter")
            .field("include_pattern", &self.include_pattern)
            .field("exclude_pattern", &self.exclude_pattern)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_filter_all_topics() {
        let filter = DefaultTopicFilter::new();
        // Default include pattern ".*" matches everything
        assert!(filter.matches_include("test-topic"));
        assert!(filter.matches_include("any-topic"));
        assert!(filter.matches_include("my-app-events"));
    }

    #[test]
    fn test_default_exclude_internal_topics() {
        let filter = DefaultTopicFilter::new();
        // Internal topics should be excluded
        assert!(filter.matches_exclude("__consumer_offsets"));
        assert!(filter.matches_exclude("__transaction_state"));
        assert!(filter.matches_exclude("test.internal"));
        assert!(filter.matches_exclude("topic.replicas"));
    }

    #[test]
    fn test_should_replicate_topic() {
        let filter = DefaultTopicFilter::new();
        // Regular topics should be replicated
        assert!(filter.should_replicate_topic("my-topic"));
        assert!(filter.should_replicate_topic("test-topic"));
        assert!(filter.should_replicate_topic("app-events"));
        // Internal topics should not be replicated
        assert!(!filter.should_replicate_topic("__consumer_offsets"));
        assert!(!filter.should_replicate_topic("__transaction_state"));
        assert!(!filter.should_replicate_topic("__cluster_metadata"));
    }

    #[test]
    fn test_configure_filter() {
        let mut filter = DefaultTopicFilter::new();
        let mut props = HashMap::new();
        props.insert(TOPICS_INCLUDE_CONFIG.to_string(), "test-.*".to_string());
        props.insert(
            TOPICS_EXCLUDE_CONFIG.to_string(),
            "test-exclude".to_string(),
        );
        filter.configure(&props);

        assert!(filter.matches_include("test-topic"));
        assert!(!filter.matches_include("other-topic"));
        assert!(filter.matches_exclude("test-exclude"));
    }

    #[test]
    fn test_pattern_exact_match() {
        assert!(DefaultTopicFilter::matches_pattern("exact", "exact"));
        assert!(!DefaultTopicFilter::matches_pattern("exact", "other"));
    }

    #[test]
    fn test_pattern_prefix_wildcard() {
        assert!(DefaultTopicFilter::matches_pattern("test-topic", "test.*"));
        assert!(DefaultTopicFilter::matches_pattern("test123", "test.*"));
        assert!(!DefaultTopicFilter::matches_pattern("other-test", "test.*"));
    }

    #[test]
    fn test_pattern_suffix_wildcard() {
        assert!(DefaultTopicFilter::matches_pattern("my-topic", ".*topic"));
        assert!(DefaultTopicFilter::matches_pattern(
            "another-topic",
            ".*topic"
        ));
        assert!(!DefaultTopicFilter::matches_pattern("my-other", ".*topic"));
    }

    #[test]
    fn test_with_patterns() {
        let filter = DefaultTopicFilter::with_patterns(
            "production-.*".to_string(),
            "production-test.*".to_string(),
        );
        assert!(filter.should_replicate_topic("production-events"));
        assert!(!filter.should_replicate_topic("production-test-data"));
        assert!(!filter.should_replicate_topic("dev-events"));
    }

    #[test]
    fn test_empty_topic() {
        let filter = DefaultTopicFilter::new();
        // Empty topic should match .* include pattern
        assert!(filter.matches_include(""));
        // Empty topic should not match internal exclude patterns
        assert!(!filter.matches_exclude(""));
    }

    #[test]
    fn test_close_no_error() {
        let mut filter = DefaultTopicFilter::new();
        filter.close();
        // Should not panic or error
    }
}
