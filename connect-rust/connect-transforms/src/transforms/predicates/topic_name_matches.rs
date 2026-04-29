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

//! TopicNameMatches Predicate - matches records by topic name using regular expressions.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.predicates.TopicNameMatches` in Java.
//!
//! A predicate which is true for records with a topic name that matches the configured
//! regular expression.
//!
//! # Configuration
//!
//! * `pattern` - A Java regular expression for matching against the name of a record's topic.
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::predicates::TopicNameMatches;
//! use connect_api::transforms::predicates::Predicate;
//! use connect_api::connector::ConnectRecord;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Configure the predicate with a pattern
//! let mut configs = HashMap::new();
//! configs.insert("pattern".to_string(), json!("foo-.*"));
//!
//! let mut predicate = TopicNameMatches::new();
//! predicate.configure(configs);
//!
//! // The predicate would match records with topic names like "foo-1", "foo-bar", etc.
//! ```

use crate::transforms::util::{RegexValidator, SimpleConfig};
use common_trait::config::{
    CompositeValidator, ConfigDefBuilder, ConfigDefImportance, ConfigDefType,
    NonEmptyStringValidator,
};
use common_trait::ConfigException;
use connect_api::connector::ConnectRecord;
use connect_api::transforms::predicates::Predicate;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Configuration key for the pattern.
const PATTERN_CONFIG: &str = "pattern";

/// Overview documentation for TopicNameMatches.
pub const OVERVIEW_DOC: &str = "A predicate which is true for records with a topic name that matches the configured regular expression.";

/// A predicate which is true for records with a topic name that matches the configured regular expression.
///
/// This corresponds to `org.apache.kafka.connect.transforms.predicates.TopicNameMatches<R>` in Java.
///
/// # Type Parameters
///
/// * `R` - The type of connect record (must implement ConnectRecord trait)
///
/// # Configuration
///
/// The predicate requires a `pattern` configuration which is a Java-compatible regular expression.
/// The pattern is used to match against the topic name of records.
///
/// # Example
///
/// ```
/// use connect_transforms::transforms::predicates::TopicNameMatches;
/// use connect_api::transforms::predicates::Predicate;
/// use serde_json::json;
/// use std::collections::HashMap;
///
/// let mut predicate = TopicNameMatches::new();
/// let mut configs = HashMap::new();
/// configs.insert("pattern".to_string(), json!("test.*"));
/// predicate.configure(configs);
/// ```
#[derive(Debug)]
pub struct TopicNameMatches {
    /// The compiled regex pattern for matching topic names.
    pattern: Option<Regex>,
}

impl TopicNameMatches {
    /// Creates a new TopicNameMatches predicate.
    ///
    /// This corresponds to the constructor in Java.
    pub fn new() -> Self {
        TopicNameMatches { pattern: None }
    }

    /// Returns the version of this predicate.
    ///
    /// This corresponds to `Predicate.version()` in Java, which returns AppInfoParser.getVersion().
    /// In the Rust implementation, we return the version string directly.
    pub fn version() -> &'static str {
        "3.9.0"
    }

    /// Returns the configuration definition for this predicate.
    ///
    /// This corresponds to `TopicNameMatches.CONFIG_DEF` in Java.
    ///
    /// The configuration includes:
    /// - `pattern`: A Java regular expression for matching topic names (required, non-empty string)
    pub fn config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        let composite_validator = CompositeValidator::new(vec![
            Box::new(NonEmptyStringValidator::new()),
            Box::new(RegexValidator::new()),
        ]);

        ConfigDefBuilder::new()
            .define_with_validator(
                PATTERN_CONFIG,
                ConfigDefType::String,
                None, // No default value - required configuration
                Some(Box::new(composite_validator)),
                ConfigDefImportance::Medium,
                "A Java regular expression for matching against the name of a record's topic.",
            )
            .build()
    }
}

impl Default for TopicNameMatches {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: ConnectRecord> Predicate<R> for TopicNameMatches {
    /// Configures this predicate with the given configuration map.
    ///
    /// This corresponds to `TopicNameMatches.configure(Map<String, ?> configs)` in Java.
    ///
    /// # Arguments
    ///
    /// * `configs` - A map of configuration key-value pairs
    ///
    /// # Configuration Keys
    ///
    /// * `pattern` - Required. A Java-compatible regular expression pattern.
    ///
    /// # Errors
    ///
    /// The predicate will fail to configure if:
    /// - The `pattern` key is missing
    /// - The pattern is not a valid regular expression
    /// - The pattern is an empty string
    ///
    /// This corresponds to how Java throws ConfigException for invalid configuration.
    fn configure(&mut self, configs: HashMap<String, Value>) {
        let config_def = Self::config_def();
        let simple_config = SimpleConfig::new(config_def, configs)
            .expect("Failed to parse configuration for TopicNameMatches");

        let pattern_str = simple_config
            .get_string(PATTERN_CONFIG)
            .expect("pattern configuration is required");

        // Wrap pattern with anchors to simulate Java's matcher.matches() behavior
        // Java's matches() requires the entire string to match the pattern
        let anchored_pattern = format!("^{}$", pattern_str);
        let compiled_pattern = Regex::new(&anchored_pattern)
            .expect("Failed to compile regex pattern for TopicNameMatches");

        self.pattern = Some(compiled_pattern);
    }

    /// Tests if the given record's topic name matches the configured pattern.
    ///
    /// This corresponds to `TopicNameMatches.test(R record)` in Java.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to test
    ///
    /// # Returns
    ///
    /// Returns `true` if:
    /// - The record has a topic name (not null)
    /// - The topic name matches the configured regular expression
    ///
    /// Returns `false` otherwise.
    ///
    /// The matching uses full match semantics (Pattern.matcher(topic).matches() in Java),
    /// not partial match semantics. This means the entire topic name must match the pattern.
    fn test(&self, record: &R) -> bool {
        match &self.pattern {
            Some(pattern) => {
                let topic = record.topic();
                // In Java: record.topic() != null && pattern.matcher(record.topic()).matches()
                // The pattern was already wrapped with anchors during configure
                !topic.is_empty() && pattern.is_match(topic)
            }
            None => false,
        }
    }

    /// Closes this predicate and releases any resources.
    ///
    /// This corresponds to `TopicNameMatches.close()` in Java.
    /// In this implementation, there are no resources to release.
    fn close(&mut self) {
        self.pattern = None;
    }
}

impl fmt::Display for TopicNameMatches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TopicNameMatches{{pattern={}}}",
            self.pattern
                .as_ref()
                .map(|p| p.to_string())
                .unwrap_or_else(|| "null".to_string())
        )
    }
}

