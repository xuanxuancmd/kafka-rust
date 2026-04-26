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

//! GroupFilter trait for MirrorMaker.
//!
//! Defines which consumer groups should be replicated.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.GroupFilter

use std::collections::HashMap;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java DefaultGroupFilter
// ============================================================================

/// Configuration key for consumer groups to include.
/// Corresponds to Java: DefaultGroupFilter.GROUPS_INCLUDE_CONFIG
pub const GROUPS_INCLUDE_CONFIG: &str = "groups";

/// Default value for groups to include - match all groups.
/// Corresponds to Java: DefaultGroupFilter.GROUPS_INCLUDE_DEFAULT
pub const GROUPS_INCLUDE_DEFAULT: &str = ".*";

/// Configuration key for consumer groups to exclude.
/// Corresponds to Java: DefaultGroupFilter.GROUPS_EXCLUDE_CONFIG
pub const GROUPS_EXCLUDE_CONFIG: &str = "groups.exclude";

/// Default value for groups to exclude - exclude console consumer groups.
/// Corresponds to Java: DefaultGroupFilter.GROUPS_EXCLUDE_DEFAULT
pub const GROUPS_EXCLUDE_DEFAULT: &str = "console-consumer-.*,connect-.*,__.*";

// ============================================================================
// GroupFilter Trait
// ============================================================================

/// Trait for filtering consumer groups to replicate.
///
/// Defines which consumer groups should be replicated from the source cluster
/// to the target cluster for checkpoint offset synchronization.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.GroupFilter
pub trait GroupFilter: Send + Sync {
    /// Determines whether a given consumer group should be replicated.
    ///
    /// Corresponds to Java: GroupFilter.shouldReplicateGroup(String group)
    fn should_replicate_group(&self, group: &str) -> bool;

    /// Configures the filter with the given properties.
    ///
    /// Corresponds to Java: GroupFilter.configure(Map<String, ?> props)
    fn configure(&mut self, props: &HashMap<String, String>);

    /// Closes the filter and releases any resources.
    ///
    /// Corresponds to Java: GroupFilter.close()
    fn close(&mut self) {}
}

// ============================================================================
// DefaultGroupFilter
// ============================================================================

/// Default implementation of GroupFilter using regex patterns.
///
/// This filter uses regex patterns to determine which consumer groups should be
/// replicated. Groups matching the include pattern and not matching the
/// exclude pattern will be replicated.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.DefaultGroupFilter
pub struct DefaultGroupFilter {
    /// Regex pattern for groups to include
    include_pattern: String,
    /// Regex pattern for groups to exclude
    exclude_pattern: String,
}

impl Default for DefaultGroupFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultGroupFilter {
    /// Creates a new DefaultGroupFilter with default patterns.
    pub fn new() -> Self {
        DefaultGroupFilter {
            include_pattern: GROUPS_INCLUDE_DEFAULT.to_string(),
            exclude_pattern: GROUPS_EXCLUDE_DEFAULT.to_string(),
        }
    }

    /// Creates a new DefaultGroupFilter with custom patterns.
    pub fn with_patterns(include: String, exclude: String) -> Self {
        DefaultGroupFilter {
            include_pattern: include,
            exclude_pattern: exclude,
        }
    }

    /// Checks if a group matches the include pattern.
    fn matches_include(&self, group: &str) -> bool {
        // For the default pattern ".*", all groups match
        if self.include_pattern == ".*" {
            return true;
        }

        // Handle comma-separated list of patterns
        for pattern in self.include_pattern.split(',') {
            let trimmed = pattern.trim();
            if Self::matches_pattern(group, trimmed) {
                return true;
            }
        }
        false
    }

    /// Checks if a group matches the exclude pattern.
    fn matches_exclude(&self, group: &str) -> bool {
        // Handle comma-separated list of patterns
        for pattern in self.exclude_pattern.split(',') {
            let trimmed = pattern.trim();
            if Self::matches_pattern(group, trimmed) {
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

        // Handle __.* pattern for internal groups
        if pattern == "__.*" {
            return text.starts_with("__");
        }

        false
    }
}

impl GroupFilter for DefaultGroupFilter {
    fn should_replicate_group(&self, group: &str) -> bool {
        // Group should be replicated if it matches include and does not match exclude
        self.matches_include(group) && !self.matches_exclude(group)
    }

    fn configure(&mut self, props: &HashMap<String, String>) {
        if let Some(include) = props.get(GROUPS_INCLUDE_CONFIG) {
            self.include_pattern = include.clone();
        }
        if let Some(exclude) = props.get(GROUPS_EXCLUDE_CONFIG) {
            self.exclude_pattern = exclude.clone();
        }
    }
}

impl std::fmt::Debug for DefaultGroupFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultGroupFilter")
            .field("include_pattern", &self.include_pattern)
            .field("exclude_pattern", &self.exclude_pattern)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_filter_all_groups() {
        let filter = DefaultGroupFilter::new();
        // Default include pattern ".*" matches everything
        assert!(filter.matches_include("my-group"));
        assert!(filter.matches_include("any-group"));
        assert!(filter.matches_include("app-consumers"));
    }

    #[test]
    fn test_default_exclude_console_consumer() {
        let filter = DefaultGroupFilter::new();
        // Console consumer groups should be excluded
        assert!(filter.matches_exclude("console-consumer-12345"));
        assert!(filter.matches_exclude("connect-cluster"));
        assert!(filter.matches_exclude("__consumer_offsets"));
        assert!(filter.matches_exclude("__transaction_state"));
    }

    #[test]
    fn test_should_replicate_group() {
        let filter = DefaultGroupFilter::new();
        // Regular groups should be replicated
        assert!(filter.should_replicate_group("my-consumer-group"));
        assert!(filter.should_replicate_group("app-consumers"));
        assert!(filter.should_replicate_group("production-events"));
        // Console consumer groups should not be replicated
        assert!(!filter.should_replicate_group("console-consumer-12345"));
        assert!(!filter.should_replicate_group("connect-mirror"));
        assert!(!filter.should_replicate_group("__internal"));
    }

    #[test]
    fn test_configure_filter() {
        let mut filter = DefaultGroupFilter::new();
        let mut props = HashMap::new();
        props.insert(GROUPS_INCLUDE_CONFIG.to_string(), "app-.*".to_string());
        props.insert(GROUPS_EXCLUDE_CONFIG.to_string(), "app-test".to_string());
        filter.configure(&props);

        assert!(filter.matches_include("app-consumers"));
        assert!(!filter.matches_include("other-group"));
        assert!(filter.matches_exclude("app-test"));
    }

    #[test]
    fn test_pattern_exact_match() {
        assert!(DefaultGroupFilter::matches_pattern("exact", "exact"));
        assert!(!DefaultGroupFilter::matches_pattern("exact", "other"));
    }

    #[test]
    fn test_pattern_prefix_wildcard() {
        assert!(DefaultGroupFilter::matches_pattern(
            "app-consumers",
            "app.*"
        ));
        assert!(DefaultGroupFilter::matches_pattern("app123", "app.*"));
        assert!(!DefaultGroupFilter::matches_pattern("other-app", "app.*"));
    }

    #[test]
    fn test_pattern_suffix_wildcard() {
        assert!(DefaultGroupFilter::matches_pattern("my-group", ".*group"));
        assert!(DefaultGroupFilter::matches_pattern(
            "another-group",
            ".*group"
        ));
        assert!(!DefaultGroupFilter::matches_pattern("my-other", ".*group"));
    }

    #[test]
    fn test_with_patterns() {
        let filter =
            DefaultGroupFilter::with_patterns("prod-.*".to_string(), "prod-test.*".to_string());
        assert!(filter.should_replicate_group("prod-events"));
        assert!(!filter.should_replicate_group("prod-test-data"));
        assert!(!filter.should_replicate_group("dev-events"));
    }

    #[test]
    fn test_empty_group() {
        let filter = DefaultGroupFilter::new();
        // Empty group should match .* include pattern
        assert!(filter.matches_include(""));
        // Empty group should not match exclude patterns
        assert!(!filter.matches_exclude(""));
    }

    #[test]
    fn test_close_no_error() {
        let mut filter = DefaultGroupFilter::new();
        filter.close();
        // Should not panic or error
    }
}
