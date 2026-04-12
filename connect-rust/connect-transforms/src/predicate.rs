//! Predicate implementations for Kafka Connect transforms
//!
//! This module provides predicate implementations for filtering records based on various criteria.

pub use connect_api::Predicate;

use connect_api::{Closeable, ConfigDef, ConfigValue, Configurable, ConnectRecordTrait};
use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

// ============================================================================================
// TopicNameMatches predicate
// ============================================================================================

/// A predicate which is true for records with a topic name that matches the configured regular expression.
pub struct TopicNameMatches {
    pattern: Regex,
}

impl TopicNameMatches {
    /// Create a new TopicNameMatches predicate with the specified pattern.
    pub fn new(pattern: Regex) -> Self {
        Self { pattern }
    }

    /// Create a new TopicNameMatches predicate from a pattern string.
    pub fn from_pattern(pattern: &str) -> Result<Self, Box<dyn Error>> {
        let regex = Regex::new(pattern)?;
        Ok(Self::new(regex))
    }

    /// Get the configuration definition for this predicate.
    pub fn config_def() -> ConfigDef {
        let mut config_def = ConfigDef::new();
        config_def.add_config(
            "pattern".to_string(),
            ConfigValue::String(
                "A Java regular expression for matching against the name of a record's topic."
                    .to_string(),
            ),
        );
        config_def
    }
}

impl Default for TopicNameMatches {
    fn default() -> Self {
        Self::new(Regex::new(".*").unwrap())
    }
}

impl Predicate for TopicNameMatches {
    fn test(&self, record: &dyn ConnectRecordTrait) -> bool {
        let topic = record.topic();
        self.pattern.is_match(topic)
    }
}

impl Configurable for TopicNameMatches {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(pattern_any) = configs.get("pattern") {
            if let Some(pattern_str) = pattern_any.downcast_ref::<String>() {
                if let Ok(regex) = Regex::new(pattern_str) {
                    self.pattern = regex;
                }
            }
        }
    }
}

impl Closeable for TopicNameMatches {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

impl std::fmt::Debug for TopicNameMatches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TopicNameMatches{{ pattern: {:?} }}",
            self.pattern.as_str()
        )
    }
}

// ============================================================================================
// RecordIsTombstone predicate
// ============================================================================================

/// A predicate which is true for records which are tombstones (i.e. have null value).
pub struct RecordIsTombstone;

impl RecordIsTombstone {
    /// Create a new RecordIsTombstone predicate.
    pub fn new() -> Self {
        Self
    }

    /// Get the configuration definition for this predicate.
    pub fn config_def() -> ConfigDef {
        ConfigDef::new()
    }
}

impl Default for RecordIsTombstone {
    fn default() -> Self {
        Self::new()
    }
}

impl Predicate for RecordIsTombstone {
    fn test(&self, record: &dyn ConnectRecordTrait) -> bool {
        record.value().is_none()
    }
}

impl Configurable for RecordIsTombstone {
    fn configure(&mut self, _configs: HashMap<String, Box<dyn std::any::Any>>) {
        // No configuration needed
    }
}

impl Closeable for RecordIsTombstone {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

impl std::fmt::Debug for RecordIsTombstone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordIsTombstone{{}}")
    }
}

// ============================================================================================
// HasHeaderKey predicate
// ============================================================================================

/// A predicate which is true for records with at least one header with the configured name.
pub struct HasHeaderKey {
    name: String,
}

impl HasHeaderKey {
    /// Create a new HasHeaderKey predicate with the specified header name.
    pub fn new(name: String) -> Self {
        Self { name }
    }

    /// Get the configuration definition for this predicate.
    pub fn config_def() -> ConfigDef {
        let mut config_def = ConfigDef::new();
        config_def.add_config(
            "name".to_string(),
            ConfigValue::String("The header name.".to_string()),
        );
        config_def
    }
}

impl Default for HasHeaderKey {
    fn default() -> Self {
        Self::new(String::new())
    }
}

impl Predicate for HasHeaderKey {
    fn test(&self, record: &dyn ConnectRecordTrait) -> bool {
        let headers = record.headers();
        // Check if there's at least one header with the specified name
        !headers.all_with_name(&self.name).is_empty()
    }
}

impl Configurable for HasHeaderKey {
    fn configure(&mut self, configs: HashMap<String, Box<dyn std::any::Any>>) {
        if let Some(name_any) = configs.get("name") {
            if let Some(name_str) = name_any.downcast_ref::<String>() {
                self.name = name_str.clone();
            }
        }
    }
}

impl Closeable for HasHeaderKey {
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

impl std::fmt::Debug for HasHeaderKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HasHeaderKey{{ name: {:?} }}", self.name)
    }
}
