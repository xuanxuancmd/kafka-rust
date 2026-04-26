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

//! ConfigPropertyFilter trait for MirrorMaker.
//!
//! Defines which topic configuration properties should be replicated.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.ConfigPropertyFilter

use std::collections::HashMap;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java DefaultConfigPropertyFilter
// ============================================================================

/// Configuration key for config properties to exclude.
/// Corresponds to Java: DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_CONFIG
pub const CONFIG_PROPERTIES_EXCLUDE_CONFIG: &str = "config.properties.exclude";

/// Default value for config properties to exclude.
/// Corresponds to Java: DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_DEFAULT
pub const CONFIG_PROPERTIES_EXCLUDE_DEFAULT: &str =
    "follower\\.replication\\.throttled\\.replicas,leader\\.replication\\.throttled\\.replicas,message\\.timestamp\\.difference\\.max\\.ms,message\\.timestamp\\.type,unclean\\.leader\\.election\\.enable,min\\.insync\\.replicas";

// ============================================================================
// ConfigPropertyFilter Trait
// ============================================================================

/// Trait for filtering topic configuration properties to replicate.
///
/// Defines which topic configuration properties should be replicated
/// from the source cluster to the target cluster.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.ConfigPropertyFilter
pub trait ConfigPropertyFilter: Send + Sync {
    /// Determines whether a given configuration property should be replicated.
    ///
    /// Note that if a property has a default value on the source cluster,
    /// should_replicate_source_default() will also be called to determine
    /// how that property should be synced.
    ///
    /// Corresponds to Java: ConfigPropertyFilter.shouldReplicateConfigProperty(String prop)
    fn should_replicate_config_property(&self, prop: &str) -> bool;

    /// Determines how to replicate a configuration property that has a default
    /// value on the source cluster.
    ///
    /// Only invoked for properties that should_replicate_config_property() has
    /// returned true for.
    ///
    /// Returns true if the default value from the source topic should be synced
    /// to the target topic, and false if the default value for the target topic
    /// should be used instead.
    ///
    /// Corresponds to Java: ConfigPropertyFilter.shouldReplicateSourceDefault(String prop)
    fn should_replicate_source_default(&self, prop: &str) -> bool {
        false
    }

    /// Configures the filter with the given properties.
    ///
    /// Corresponds to Java: ConfigPropertyFilter.configure(Map<String, ?> props)
    fn configure(&mut self, props: &HashMap<String, String>);

    /// Closes the filter and releases any resources.
    ///
    /// Corresponds to Java: ConfigPropertyFilter.close()
    fn close(&mut self) {}
}

// ============================================================================
// DefaultConfigPropertyFilter
// ============================================================================

/// Default implementation of ConfigPropertyFilter using regex patterns.
///
/// This filter excludes certain sensitive or cluster-specific properties
/// that should not be replicated across clusters.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.DefaultConfigPropertyFilter
pub struct DefaultConfigPropertyFilter {
    /// Regex patterns for properties to exclude
    exclude_patterns: Vec<String>,
}

impl Default for DefaultConfigPropertyFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultConfigPropertyFilter {
    /// Creates a new DefaultConfigPropertyFilter with default exclude patterns.
    pub fn new() -> Self {
        DefaultConfigPropertyFilter {
            exclude_patterns: Self::parse_exclude_patterns(CONFIG_PROPERTIES_EXCLUDE_DEFAULT),
        }
    }

    /// Creates a new DefaultConfigPropertyFilter with custom exclude patterns.
    pub fn with_exclude_patterns(exclude_patterns: Vec<String>) -> Self {
        DefaultConfigPropertyFilter { exclude_patterns }
    }

    /// Parse exclude patterns from comma-separated string.
    fn parse_exclude_patterns(patterns_str: &str) -> Vec<String> {
        patterns_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    }

    /// Checks if a property matches any exclude pattern.
    fn matches_exclude(&self, prop: &str) -> bool {
        for pattern in &self.exclude_patterns {
            if Self::matches_pattern(prop, pattern) {
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

        // Handle patterns with escaped dots (e.g., "follower\\.replication")
        let unescaped = pattern.replace("\\.", ".");
        if text == unescaped {
            return true;
        }

        // Handle .* wildcard
        if pattern.contains(".*") {
            let parts: Vec<&str> = pattern.split(".*").collect();
            if parts.len() == 2 {
                let prefix = parts[0].replace("\\.", ".");
                let suffix = parts[1].replace("\\.", ".");
                if prefix.is_empty() && suffix.is_empty() {
                    return true;
                }
                if !prefix.is_empty() && !suffix.is_empty() {
                    return text.starts_with(&prefix) && text.ends_with(&suffix);
                }
                if !prefix.is_empty() {
                    return text.starts_with(&prefix);
                }
                if !suffix.is_empty() {
                    return text.ends_with(&suffix);
                }
            }
        }

        // Handle wildcard at end (prefix match)
        if pattern.ends_with(".*") {
            let prefix = pattern[..pattern.len() - 2].replace("\\.", ".");
            return text.starts_with(&prefix);
        }

        false
    }
}

impl ConfigPropertyFilter for DefaultConfigPropertyFilter {
    fn should_replicate_config_property(&self, prop: &str) -> bool {
        // Property should be replicated if it does not match exclude patterns
        !self.matches_exclude(prop)
    }

    fn configure(&mut self, props: &HashMap<String, String>) {
        if let Some(exclude) = props.get(CONFIG_PROPERTIES_EXCLUDE_CONFIG) {
            self.exclude_patterns = Self::parse_exclude_patterns(exclude);
        }
    }
}

impl std::fmt::Debug for DefaultConfigPropertyFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultConfigPropertyFilter")
            .field("exclude_patterns", &self.exclude_patterns)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_filter_excludes_cluster_specific() {
        let filter = DefaultConfigPropertyFilter::new();
        // Cluster-specific properties should be excluded
        assert!(!filter.should_replicate_config_property("min.insync.replicas"));
        assert!(!filter.should_replicate_config_property("follower.replication.throttled.replicas"));
        assert!(!filter.should_replicate_config_property("leader.replication.throttled.replicas"));
        assert!(!filter.should_replicate_config_property("message.timestamp.type"));
        assert!(!filter.should_replicate_config_property("unclean.leader.election.enable"));
    }

    #[test]
    fn test_default_filter_includes_general_properties() {
        let filter = DefaultConfigPropertyFilter::new();
        // General properties should be included
        assert!(filter.should_replicate_config_property("cleanup.policy"));
        assert!(filter.should_replicate_config_property("retention.ms"));
        assert!(filter.should_replicate_config_property("compression.type"));
        assert!(filter.should_replicate_config_property("max.message.bytes"));
    }

    #[test]
    fn test_configure_filter() {
        let mut filter = DefaultConfigPropertyFilter::new();
        let mut props = HashMap::new();
        props.insert(
            CONFIG_PROPERTIES_EXCLUDE_CONFIG.to_string(),
            "custom\\.property,test.*".to_string(),
        );
        filter.configure(&props);

        assert!(!filter.should_replicate_config_property("custom.property"));
        assert!(!filter.should_replicate_config_property("test.value"));
        assert!(filter.should_replicate_config_property("other.property"));
    }

    #[test]
    fn test_should_replicate_source_default() {
        let filter = DefaultConfigPropertyFilter::new();
        // By default, source defaults are not replicated
        assert!(!filter.should_replicate_source_default("cleanup.policy"));
        assert!(!filter.should_replicate_source_default("retention.ms"));
    }

    #[test]
    fn test_pattern_exact_match() {
        assert!(DefaultConfigPropertyFilter::matches_pattern(
            "min.insync.replicas",
            "min\\.insync\\.replicas"
        ));
    }

    #[test]
    fn test_pattern_with_escaped_dots() {
        assert!(DefaultConfigPropertyFilter::matches_pattern(
            "follower.replication.throttled.replicas",
            "follower\\.replication\\.throttled\\.replicas"
        ));
    }

    #[test]
    fn test_pattern_prefix_wildcard() {
        assert!(DefaultConfigPropertyFilter::matches_pattern(
            "test.property",
            "test.*"
        ));
        assert!(!DefaultConfigPropertyFilter::matches_pattern(
            "other.test",
            "test.*"
        ));
    }

    #[test]
    fn test_with_exclude_patterns() {
        let filter = DefaultConfigPropertyFilter::with_exclude_patterns(vec![
            "custom\\.property".to_string(),
            "sensitive.*".to_string(),
        ]);
        assert!(!filter.should_replicate_config_property("custom.property"));
        assert!(!filter.should_replicate_config_property("sensitive.data"));
        assert!(filter.should_replicate_config_property("normal.property"));
    }

    #[test]
    fn test_empty_property() {
        let filter = DefaultConfigPropertyFilter::new();
        // Empty property should not match any exclude patterns
        assert!(filter.should_replicate_config_property(""));
    }

    #[test]
    fn test_close_no_error() {
        let mut filter = DefaultConfigPropertyFilter::new();
        filter.close();
        // Should not panic or error
    }
}
