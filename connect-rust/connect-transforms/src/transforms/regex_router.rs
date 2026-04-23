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

//! RegexRouter Transformation - updates the record topic using a configured regular expression.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.RegexRouter` in Java.
//!
//! This transformation allows you to rename topics using regular expression replacement.
//! It updates the record's topic by applying a configured regex and replacement string.
//!
//! # Configuration
//!
//! * `regex` - A Java regular expression for matching the topic name (required)
//! * `replacement` - The replacement string for the matched topic portion (required)
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::RegexRouter;
//! use connect_api::Transformation;
//! use connect_api::source::SourceRecord;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Configure the transformation to prefix all topics
//! let mut configs = HashMap::new();
//! configs.insert("regex".to_string(), json!("(.*)"));
//! configs.insert("replacement".to_string(), json!("prefix-$1"));
//!
//! let mut router = RegexRouter::new();
//! router.configure(configs);
//!
//! // Create a test record
//! let record = SourceRecord::new(
//!     HashMap::new(),
//!     HashMap::new(),
//!     "topic",
//!     None,
//!     None,
//!     json!({"test": "value"}),
//! );
//!
//! // Transform the record - topic will be renamed to "prefix-topic"
//! let transformed = router.transform(record).unwrap();
//! assert_eq!(transformed.topic(), "prefix-topic");
//! ```

use crate::transforms::util::{RegexValidator, SimpleConfig};
use common_trait::config::{
    CompositeValidator, ConfigDefBuilder, ConfigDefImportance, ConfigDefType,
    NonEmptyStringValidator,
};
use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::Transformation;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Configuration key for the regex pattern.
const REGEX_CONFIG: &str = "regex";

/// Configuration key for the replacement string.
const REPLACEMENT_CONFIG: &str = "replacement";

/// Overview documentation for RegexRouter.
pub const OVERVIEW_DOC: &str =
    "Update the record topic using a configured regular expression and replacement string.";

/// RegexRouter transformation - updates the record topic using a configured regular expression.
///
/// This corresponds to `org.apache.kafka.connect.transforms.RegexRouter<R>` in Java.
///
/// The transformation works as follows:
/// 1. The configured regex pattern is matched against the record's topic
/// 2. If the pattern matches the entire topic name (using Java's `matches()` semantics),
///    the topic is replaced using `replaceFirst()` semantics
/// 3. If no match is found, the original record is returned unchanged
///
/// # Configuration
///
/// The transformation requires two configuration options:
/// - `regex`: A Java-compatible regular expression pattern (required)
/// - `replacement`: The replacement string (required)
///
/// The replacement string can reference captured groups using `$1`, `$2`, etc.
///
/// # Example
///
/// ```
/// use connect_transforms::transforms::RegexRouter;
/// use connect_api::transforms::transformation::Transformation;
/// use serde_json::json;
/// use std::collections::HashMap;
///
/// let mut router = RegexRouter::new();
/// let mut configs = HashMap::new();
/// configs.insert("regex".to_string(), json!("(.*)"));
/// configs.insert("replacement".to_string(), json!("new-$1"));
/// router.configure(configs);
/// ```
#[derive(Debug)]
pub struct RegexRouter {
    /// The compiled regex pattern.
    regex: Option<Regex>,
    /// The replacement string for matched topic portions.
    replacement: Option<String>,
}

impl RegexRouter {
    /// Creates a new RegexRouter transformation.
    ///
    /// This corresponds to the constructor in Java.
    pub fn new() -> Self {
        RegexRouter {
            regex: None,
            replacement: None,
        }
    }

    /// Returns the version of this transformation.
    ///
    /// This corresponds to `Transformation.version()` in Java, which returns AppInfoParser.getVersion().
    /// In the Rust implementation, we return the version string directly.
    pub fn version() -> &'static str {
        "3.9.0"
    }

    /// Returns the configuration definition for this transformation.
    ///
    /// This corresponds to `RegexRouter.CONFIG_DEF` in Java.
    ///
    /// The configuration includes:
    /// - `regex`: A Java regular expression for matching topic names (required, must be valid regex)
    /// - `replacement`: The replacement string for matched topic portions (required, non-empty)
    pub fn config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        let regex_validator = CompositeValidator::new(vec![
            Box::new(NonEmptyStringValidator::new()),
            Box::new(RegexValidator::new()),
        ]);

        let replacement_validator = NonEmptyStringValidator::new();

        ConfigDefBuilder::new()
            .define_with_validator(
                REGEX_CONFIG,
                ConfigDefType::String,
                None,
                Some(Box::new(regex_validator)),
                ConfigDefImportance::High,
                "Regular expression to use for matching the topic name.",
            )
            .define_with_validator(
                REPLACEMENT_CONFIG,
                ConfigDefType::String,
                None,
                Some(Box::new(replacement_validator)),
                ConfigDefImportance::High,
                "Replacement string for the matched topic portion.",
            )
            .build()
    }

    /// Checks if the regex matches the entire topic string.
    ///
    /// This replicates Java's `Pattern.matcher(topic).matches()` behavior,
    /// which requires the entire input to match the pattern.
    ///
    /// # Arguments
    ///
    /// * `regex` - The compiled regex pattern
    /// * `topic` - The topic string to match
    ///
    /// # Returns
    ///
    /// Returns `true` if the regex matches the entire topic, `false` otherwise.
    fn matches_entire_string(regex: &Regex, topic: &str) -> bool {
        // Find the first match and check if it covers the entire string
        match regex.find(topic) {
            Some(match_range) => {
                // Check if the match covers the entire string (from start to end)
                match_range.start() == 0 && match_range.end() == topic.len()
            }
            None => false,
        }
    }
}

impl Default for RegexRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl Transformation<SourceRecord> for RegexRouter {
    /// Configures this transformation with the given configuration map.
    ///
    /// This corresponds to `RegexRouter.configure(Map<String, ?> configs)` in Java.
    ///
    /// # Arguments
    ///
    /// * `configs` - A map of configuration key-value pairs
    ///
    /// # Configuration Keys
    ///
    /// * `regex` - Required. A Java-compatible regular expression pattern.
    /// * `replacement` - Required. The replacement string for matched topic portions.
    ///
    /// # Errors
    ///
    /// The transformation will fail to configure if:
    /// - Either `regex` or `replacement` key is missing
    /// - The regex is not a valid regular expression
    /// - Either configuration is an empty string
    fn configure(&mut self, configs: HashMap<String, Value>) {
        let config_def = Self::config_def();
        let simple_config = SimpleConfig::new(config_def, configs)
            .expect("Failed to parse configuration for RegexRouter");

        let regex_str = simple_config
            .get_string(REGEX_CONFIG)
            .expect("regex configuration is required");

        let replacement_str = simple_config
            .get_string(REPLACEMENT_CONFIG)
            .expect("replacement configuration is required");

        let compiled_regex =
            Regex::new(regex_str).expect("Failed to compile regex pattern for RegexRouter");

        self.regex = Some(compiled_regex);
        self.replacement = Some(replacement_str.to_string());
    }

    /// Transforms the given record by updating its topic name.
    ///
    /// This corresponds to `RegexRouter.apply(R record)` in Java.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns the transformed record with an updated topic name if the regex matches.
    /// Returns the original record unchanged if no match is found.
    ///
    /// # Transformation Logic
    ///
    /// 1. Matches the regex pattern against the record's topic
    /// 2. If the pattern matches the entire topic (Java `matches()` semantics),
    ///    uses `replaceFirst()` semantics to generate the new topic name
    /// 3. Returns a new record with the updated topic, preserving all other fields
    ///
    /// # Example
    ///
    /// If configured with regex `(.*)` and replacement `prefix-$1`:
    /// - Topic "topic" -> "prefix-topic"
    /// - Topic "foo" -> "prefix-foo"
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        match (&self.regex, &self.replacement) {
            (Some(regex), Some(replacement)) => {
                let topic = record.topic();

                // Check if the regex matches the entire topic (Java matches() semantics)
                if Self::matches_entire_string(regex, topic) {
                    // Apply replaceFirst semantics - in Rust, Regex::replace() replaces first match
                    let new_topic = regex.replace(topic, replacement.as_str());
                    Ok(Some(record.with_topic(new_topic)))
                } else {
                    // No match, return original record unchanged
                    Ok(Some(record))
                }
            }
            (None, _) | (_, None) => {
                // Not configured, return original record
                Ok(Some(record))
            }
        }
    }

    /// Closes this transformation and releases any resources.
    ///
    /// This corresponds to `RegexRouter.close()` in Java.
    /// In this implementation, we clear the regex and replacement.
    fn close(&mut self) {
        self.regex = None;
        self.replacement = None;
    }
}

impl fmt::Display for RegexRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RegexRouter{{regex={}, replacement={}}}",
            self.regex
                .as_ref()
                .map(|r| r.to_string())
                .unwrap_or_else(|| "null".to_string()),
            self.replacement
                .as_ref()
                .map(|r| r.to_string())
                .unwrap_or_else(|| "null".to_string())
        )
    }
}

