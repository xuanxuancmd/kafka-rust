//! Utility functions and traits
//!
//! This module provides utility traits and helper functions for mirror operations.

use anyhow::Result;
use std::collections::HashMap;

/// Topic name utility
///
/// Provides utilities for working with topic names.
pub trait TopicNameUtil {
    /// Check if a topic is an internal topic
    fn is_internal_topic(&self, topic: String) -> bool;

    /// Get the source cluster alias from a topic name
    fn topic_source(&self, topic: String) -> Option<String>;

    /// Get the original topic name from a replicated topic
    fn original_topic(&self, topic: String) -> Option<String>;

    /// Format a remote topic name
    fn format_remote_topic(&self, topic: String, cluster_alias: String) -> String;
}

/// Offset utility
///
/// Provides utilities for working with offsets.
pub trait OffsetUtil {
    /// Convert upstream offset to downstream offset
    fn convert_offset(&self, upstream_offset: i64) -> i64;

    /// Check if an offset is valid
    fn is_valid_offset(&self, offset: i64) -> bool;

    /// Get the earliest offset
    fn earliest_offset(&self) -> i64;

    /// Get the latest offset
    fn latest_offset(&self) -> i64;
}

/// Configuration utility
///
/// Provides utilities for working with configurations.
pub trait ConfigUtil {
    /// Merge two configuration maps
    fn merge_configs(
        &self,
        base: HashMap<String, String>,
        override_config: HashMap<String, String>,
    ) -> HashMap<String, String>;

    /// Get a required configuration value
    fn get_required_config(&self, config: HashMap<String, String>, key: String) -> Result<String>;

    /// Get an optional configuration value
    fn get_optional_config(&self, config: HashMap<String, String>, key: String) -> Option<String>;

    /// Validate configuration
    fn validate_config(&self, config: HashMap<String, String>) -> Result<()>;
}

/// Default topic name utility implementation
pub struct DefaultTopicNameUtil;

impl TopicNameUtil for DefaultTopicNameUtil {
    fn is_internal_topic(&self, topic: String) -> bool {
        topic.starts_with("__") || topic.contains(".heartbeats") || topic.contains(".checkpoints")
    }

    fn topic_source(&self, topic: String) -> Option<String> {
        if topic.contains('.') {
            let parts: Vec<&str> = topic.split('.').collect();
            if parts.len() >= 2 {
                Some(parts[0].to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn original_topic(&self, topic: String) -> Option<String> {
        if topic.contains('.') {
            let parts: Vec<&str> = topic.splitn(2, '.').collect();
            if parts.len() == 2 {
                Some(parts[1].to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn format_remote_topic(&self, topic: String, cluster_alias: String) -> String {
        format!("{}.{}", cluster_alias, topic)
    }
}

/// Default offset utility implementation
pub struct DefaultOffsetUtil;

impl OffsetUtil for DefaultOffsetUtil {
    fn convert_offset(&self, upstream_offset: i64) -> i64 {
        upstream_offset
    }

    fn is_valid_offset(&self, offset: i64) -> bool {
        offset >= 0
    }

    fn earliest_offset(&self) -> i64 {
        0
    }

    fn latest_offset(&self) -> i64 {
        i64::MAX
    }
}

/// Default configuration utility implementation
pub struct DefaultConfigUtil;

impl ConfigUtil for DefaultConfigUtil {
    fn merge_configs(
        &self,
        base: HashMap<String, String>,
        override_config: HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut result = base;
        for (key, value) in override_config {
            result.insert(key, value);
        }
        result
    }

    fn get_required_config(&self, config: HashMap<String, String>, key: String) -> Result<String> {
        config
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Required configuration key '{}' not found", key))
    }

    fn get_optional_config(&self, config: HashMap<String, String>, key: String) -> Option<String> {
        config.get(&key).cloned()
    }

    fn validate_config(&self, config: HashMap<String, String>) -> Result<()> {
        // Basic validation - ensure config is not empty
        if config.is_empty() {
            return Err(anyhow::anyhow!("Configuration cannot be empty"));
        }
        Ok(())
    }
}
