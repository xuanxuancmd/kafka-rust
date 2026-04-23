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

//! TimestampRouter Transformation - routes records to different topics based on timestamps.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.TimestampRouter` in Java.
//!
//! A transformation that modifies the topic name of a record based on its timestamp
//! and configurable format strings. This is particularly useful for sink connectors
//! that need to route records to different topics (or equivalent entities in a
//! destination system) based on the record's timestamp.
//!
//! # Configuration
//!
//! * `topic.format` - A format string for the topic name, which can include `${topic}`
//!   and `${timestamp}` as placeholders. Default: `${topic}-${timestamp}`
//! * `timestamp.format` - A format string for the timestamp using Java SimpleDateFormat
//!   compatible patterns (which are converted to chrono format internally).
//!   Default: `yyyyMMdd`
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::TimestampRouter;
//! use connect_api::transforms::Transformation;
//! use connect_api::source::SourceRecord;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Configure the transformation with custom format
//! let mut configs = HashMap::new();
//! configs.insert("topic.format".to_string(), json!("topic-${timestamp}"));
//! configs.insert("timestamp.format".to_string(), json!("yyyyMMdd"));
//!
//! let mut transform = TimestampRouter::new();
//! transform.configure(configs);
//!
//! // Records with timestamps will be routed to appropriately named topics
//! ```

use crate::transforms::util::SimpleConfig;
use chrono::{TimeZone, Utc};
use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType};
use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Configuration key for the topic format.
const TOPIC_FORMAT_CONFIG: &str = "topic.format";

/// Configuration key for the timestamp format.
const TIMESTAMP_FORMAT_CONFIG: &str = "timestamp.format";

/// Default topic format pattern.
const DEFAULT_TOPIC_FORMAT: &str = "${topic}-${timestamp}";

/// Default timestamp format (Java SimpleDateFormat style).
/// In chrono, this corresponds to "%Y%m%d".
const DEFAULT_TIMESTAMP_FORMAT: &str = "yyyyMMdd";

/// Overview documentation for TimestampRouter.
pub const OVERVIEW_DOC: &str =
    "Update the record's topic field as a function of the epoch timestamp recorded in the record.";

/// A transformation that routes records to different topics based on their timestamp.
///
/// This corresponds to `org.apache.kafka.connect.transforms.TimestampRouter<R>` in Java.
///
/// The TimestampRouter modifies the topic name of a record by applying a configurable
/// format string that can incorporate the original topic name and a formatted timestamp.
///
/// # Type Parameters
///
/// This transformation works on `SourceRecord` types that implement `ConnectRecord`.
///
/// # Configuration
///
/// - `topic.format`: Format string for the resulting topic name. Supports `${topic}`
///   placeholder for the original topic and `${timestamp}` for the formatted timestamp.
/// - `timestamp.format`: Format string for the timestamp using SimpleDateFormat patterns.
///
/// # Thread Safety
///
/// In Java, this transformation uses ThreadLocal<SimpleDateFormat> for thread safety.
/// In Rust, we use chrono which is thread-safe by default, so no special handling needed.
#[derive(Debug)]
pub struct TimestampRouter {
    /// The topic format string with ${topic} and ${timestamp} placeholders.
    topic_format: String,
    /// The timestamp format string (SimpleDateFormat style, converted to chrono format).
    timestamp_format: String,
}

impl TimestampRouter {
    /// Creates a new TimestampRouter transformation.
    ///
    /// This corresponds to the constructor in Java.
    pub fn new() -> Self {
        TimestampRouter {
            topic_format: DEFAULT_TOPIC_FORMAT.to_string(),
            timestamp_format: DEFAULT_TIMESTAMP_FORMAT.to_string(),
        }
    }

    /// Returns the version of this transformation.
    ///
    /// This corresponds to `Transformation.version()` in Java, which returns AppInfoParser.getVersion().
    /// In the Rust implementation, we return the version string directly.
    /// Version "3.9.0" corresponds to Kafka 3.9.0.
    pub fn version() -> &'static str {
        "3.9.0"
    }

    /// Returns the configuration definition for this transformation.
    ///
    /// This corresponds to `TimestampRouter.CONFIG_DEF` in Java.
    ///
    /// The configuration includes:
    /// - `topic.format`: Format string for the topic name (default: `${topic}-${timestamp}`)
    /// - `timestamp.format`: SimpleDateFormat string for timestamp formatting (default: `yyyyMMdd`)
    pub fn config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                TOPIC_FORMAT_CONFIG,
                ConfigDefType::String,
                Some(Value::String(DEFAULT_TOPIC_FORMAT.to_string())),
                ConfigDefImportance::High,
                "Format string for the topic that can include '${topic}' and '${timestamp}' as placeholders.",
            )
            .define(
                TIMESTAMP_FORMAT_CONFIG,
                ConfigDefType::String,
                Some(Value::String(DEFAULT_TIMESTAMP_FORMAT.to_string())),
                ConfigDefImportance::High,
                "Format string for the timestamp that follows Java SimpleDateFormat syntax.",
            )
            .build()
    }

    /// Converts a Java SimpleDateFormat pattern to a chrono format string.
    ///
    /// Java SimpleDateFormat uses different pattern syntax than chrono. This function
    /// performs a simple conversion for common patterns.
    ///
    /// # Java to chrono conversion rules (common patterns)
    ///
    /// - `yyyy` -> `%Y` (4-digit year)
    /// - `yy` -> `%y` (2-digit year)
    /// - `MM` -> `%m` (2-digit month)
    /// - `dd` -> `%d` (2-digit day)
    /// - `HH` -> `%H` (24-hour format)
    /// - `hh` -> `%I` (12-hour format)
    /// - `mm` -> `%M` (minutes)
    /// - `ss` -> `%S` (seconds)
    /// - `SSS` -> `%f` (milliseconds -> microseconds in chrono, truncated)
    /// - `'...'` (literal text) -> preserved as-is
    ///
    /// Note: This is a simplified conversion. For complex patterns, users may need
    /// to manually adjust the format string.
    fn convert_simple_date_format_to_chrono(java_format: &str) -> String {
        let mut chrono_format = String::new();
        let mut chars = java_format.chars().peekable();

        while let Some(c) = chars.next() {
            // Handle quoted literal text in SimpleDateFormat
            if c == '\'' {
                // In SimpleDateFormat, text between single quotes is literal
                // In chrono, we just output it directly
                while let Some(next) = chars.next() {
                    if next == '\'' {
                        break;
                    }
                    chrono_format.push(next);
                }
                continue;
            }

            // Handle pattern letters
            match c {
                'y' => {
                    // Count consecutive 'y' characters
                    let mut count = 1;
                    while chars.peek() == Some(&'y') {
                        chars.next();
                        count += 1;
                    }
                    if count >= 4 {
                        chrono_format.push_str("%Y"); // 4-digit year
                    } else if count >= 2 {
                        chrono_format.push_str("%y"); // 2-digit year
                    } else {
                        // Single 'y' is unusual, treat as year
                        chrono_format.push_str("%Y");
                    }
                }
                'M' => {
                    let mut count = 1;
                    while chars.peek() == Some(&'M') {
                        chars.next();
                        count += 1;
                    }
                    // Month: %m for numeric, %b for abbreviated, %B for full name
                    if count <= 2 {
                        chrono_format.push_str("%m"); // Numeric month
                    } else if count == 3 {
                        chrono_format.push_str("%b"); // Abbreviated month name
                    } else {
                        chrono_format.push_str("%B"); // Full month name
                    }
                }
                'd' => {
                    while chars.peek() == Some(&'d') {
                        chars.next();
                    }
                    chrono_format.push_str("%d"); // Day of month
                }
                'H' => {
                    while chars.peek() == Some(&'H') {
                        chars.next();
                    }
                    chrono_format.push_str("%H"); // 24-hour
                }
                'h' => {
                    while chars.peek() == Some(&'h') {
                        chars.next();
                    }
                    chrono_format.push_str("%I"); // 12-hour
                }
                'm' => {
                    while chars.peek() == Some(&'m') {
                        chars.next();
                    }
                    chrono_format.push_str("%M"); // Minutes
                }
                's' => {
                    while chars.peek() == Some(&'s') {
                        chars.next();
                    }
                    chrono_format.push_str("%S"); // Seconds
                }
                'S' => {
                    // Milliseconds in Java, microseconds in chrono
                    while chars.peek() == Some(&'S') {
                        chars.next();
                    }
                    // Chrono uses %f for fractional seconds (microseconds)
                    // For milliseconds (3 digits), we use %3f in chrono or just %f
                    chrono_format.push_str("%f");
                }
                'E' => {
                    // Day of week
                    let mut count = 1;
                    while chars.peek() == Some(&'E') {
                        chars.next();
                        count += 1;
                    }
                    if count <= 3 {
                        chrono_format.push_str("%a"); // Abbreviated day name
                    } else {
                        chrono_format.push_str("%A"); // Full day name
                    }
                }
                'a' => {
                    // AM/PM marker
                    chrono_format.push_str("%P"); // AM/PM lowercase
                }
                'z' | 'Z' => {
                    // Time zone - chrono uses %Z for timezone name
                    chrono_format.push_str("%Z");
                }
                '-' | '.' | '/' | ':' | ' ' | ',' => {
                    // These are literal separators, preserve them
                    chrono_format.push(c);
                }
                _ => {
                    // Unknown pattern character, preserve as literal
                    chrono_format.push(c);
                }
            }
        }

        chrono_format
    }

    /// Formats the timestamp using the configured format string.
    ///
    /// This corresponds to the timestamp formatting in `TimestampRouter.apply()` in Java.
    /// The timestamp is expected to be in milliseconds since epoch.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp in milliseconds since Unix epoch
    ///
    /// # Returns
    ///
    /// Returns the formatted timestamp string.
    fn format_timestamp(&self, timestamp: i64) -> String {
        // Convert milliseconds to DateTime<Utc>
        // Chrono expects seconds, so we need to handle milliseconds separately
        let seconds = timestamp / 1000;
        let millis = (timestamp % 1000) * 1_000_000; // Convert to nanoseconds for chrono

        // Create DateTime from timestamp (milliseconds since epoch)
        let datetime = Utc
            .timestamp_opt(seconds, millis as u32)
            .single()
            .unwrap_or_else(|| {
                // Fallback: use the current time if the timestamp is invalid
                Utc::now()
            });

        // Convert Java SimpleDateFormat to chrono format and apply
        let chrono_format = Self::convert_simple_date_format_to_chrono(&self.timestamp_format);
        datetime.format(&chrono_format).to_string()
    }

    /// Applies the topic format to create the new topic name.
    ///
    /// This corresponds to the topic name construction in `TimestampRouter.apply()` in Java.
    ///
    /// # Arguments
    ///
    /// * `original_topic` - The original topic name
    /// * `formatted_timestamp` - The formatted timestamp string
    ///
    /// # Returns
    ///
    /// Returns the new topic name with placeholders replaced.
    fn apply_topic_format(&self, original_topic: &str, formatted_timestamp: &str) -> String {
        self.topic_format
            .replace("${topic}", original_topic)
            .replace("${timestamp}", formatted_timestamp)
    }
}

impl Default for TimestampRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl Transformation<SourceRecord> for TimestampRouter {
    /// Configures this transformation with the given configuration map.
    ///
    /// This corresponds to `TimestampRouter.configure(Map<String, ?> configs)` in Java.
    ///
    /// # Arguments
    ///
    /// * `configs` - A map of configuration key-value pairs
    ///
    /// # Configuration Keys
    ///
    /// * `topic.format` - Format string for the topic name (optional, default: `${topic}-${timestamp}`)
    /// * `timestamp.format` - SimpleDateFormat string for timestamp formatting (optional, default: `yyyyMMdd`)
    ///
    /// # Errors
    ///
    /// The transformation will fail to configure if the format strings are invalid.
    fn configure(&mut self, configs: HashMap<String, Value>) {
        let config_def = Self::config_def();
        let simple_config = SimpleConfig::new(config_def, configs)
            .expect("Failed to parse configuration for TimestampRouter");

        self.topic_format = simple_config
            .get_string(TOPIC_FORMAT_CONFIG)
            .unwrap_or(DEFAULT_TOPIC_FORMAT)
            .to_string();

        self.timestamp_format = simple_config
            .get_string(TIMESTAMP_FORMAT_CONFIG)
            .unwrap_or(DEFAULT_TIMESTAMP_FORMAT)
            .to_string();
    }

    /// Transforms the given record by updating its topic based on the timestamp.
    ///
    /// This corresponds to `TimestampRouter.apply(R record)` in Java.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with an updated topic name, or a DataException if
    /// the record's timestamp is null.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if the record's timestamp is null.
    /// In Java, this throws a DataException with the message "Timestamp missing on record".
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the timestamp from the record
        let timestamp = record.timestamp();

        // If timestamp is null (None), throw DataException
        // In Java: if (record.timestamp() == null) { throw new DataException("Timestamp missing on record"); }
        let timestamp_millis = match timestamp {
            Some(ts) => ts,
            None => {
                return Err(ConnectError::data("Timestamp missing on record"));
            }
        };

        // Format the timestamp
        let formatted_timestamp = self.format_timestamp(timestamp_millis);

        // Apply the topic format to create new topic name
        let original_topic = record.topic();
        let updated_topic = self.apply_topic_format(original_topic, &formatted_timestamp);

        // Create a new record with the updated topic
        // In Java: record.newRecord(updatedTopic, record.kafkaPartition(), record.key(), record.value(), record.timestamp())
        Ok(Some(record.with_topic(updated_topic)))
    }

    /// Closes this transformation and releases any resources.
    ///
    /// This corresponds to `TimestampRouter.close()` in Java.
    /// In the Java implementation, this cleans up the ThreadLocal SimpleDateFormat.
    /// In Rust, we just reset the format strings to defaults.
    fn close(&mut self) {
        self.topic_format = DEFAULT_TOPIC_FORMAT.to_string();
        self.timestamp_format = DEFAULT_TIMESTAMP_FORMAT.to_string();
    }
}

impl fmt::Display for TimestampRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TimestampRouter{{topic_format='{}', timestamp_format='{}'}}",
            self.topic_format, self.timestamp_format
        )
    }
}

