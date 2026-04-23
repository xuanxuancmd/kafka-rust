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

//! Filter Transformation - drops all records (returns null).
//!
//! This corresponds to `org.apache.kafka.connect.transforms.Filter` in Java.
//!
//! The Filter transformation is the simplest transformation - it drops all records
//! by returning null from `apply()`. It has no configuration options.
//!
//! # Key/Value Pattern
//!
//! In Java, Filter has two inner classes `Filter.Key` and `Filter.Value`.
//! In Rust, we use a `FilterTarget` enum to achieve the same pattern.

use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::Transformation;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Overview documentation for Filter.
pub const OVERVIEW_DOC: &str = "Drops all records.";

/// Filter transformation that drops all records.
///
/// This corresponds to `org.apache.kafka.connect.transforms.Filter<R>` in Java.
///
/// The Filter transformation has no configuration and simply returns null
/// for all records, effectively dropping them from the pipeline.
///
/// # Type Parameters
///
/// * `R` - The type of connect record
///
/// # Example
///
/// ```
/// use connect_transforms::transforms::filter::{Filter, FilterTarget};
/// use connect_api::transforms::transformation::Transformation;
/// use connect_api::source::SourceRecord;
/// use std::collections::HashMap;
///
/// // Create a Filter for Key
/// let filter_key = Filter::new(FilterTarget::Key);
///
/// // Create a Filter for Value
/// let filter_value = Filter::new(FilterTarget::Value);
///
/// // Configure (no configuration needed)
/// let mut configs = HashMap::new();
/// filter_key.configure(configs);
///
/// // Transform will return None (filtered)
/// let record = SourceRecord::new(
///     HashMap::new(),
///     HashMap::new(),
///     "test-topic".to_string(),
///     None,
///     None,
///     serde_json::json!({"key": "value"}),
/// );
///
/// let result = filter_key.transform(record);
/// assert!(result.is_ok());
/// assert!(result.unwrap().is_none());
/// ```
#[derive(Debug, Clone)]
pub struct Filter {
    /// The target (Key or Value) - determines usage pattern.
    target: FilterTarget,
}

/// FilterTarget enum representing Key or Value operation.
///
/// This replaces Java's inner classes `Filter.Key` and `Filter.Value`.
/// In Kafka Connect configuration, the target is determined by the alias:
/// - "Filter$Key" → FilterTarget::Key
/// - "Filter$Value" → FilterTarget::Value
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterTarget {
    /// Operate on record key (Filter.Key in Java).
    Key,
    /// Operate on record value (Filter.Value in Java).
    Value,
}

impl Filter {
    /// Creates a new Filter transformation for the specified target.
    ///
    /// This corresponds to the constructor in Java.
    ///
    /// # Arguments
    ///
    /// * `target` - The target (Key or Value)
    pub fn new(target: FilterTarget) -> Self {
        Filter { target }
    }

    /// Returns the version of this transformation.
    ///
    /// This corresponds to `Transformation.version()` in Java, which returns
    /// `AppInfoParser.getVersion()` from connect-transforms module.
    ///
    /// In Rust, we hard-code the version as "3.9.0" with a comment explaining
    /// it comes from AppInfoParser.
    pub fn version() -> &'static str {
        // Version from AppInfoParser.getVersion() for connect-transforms module
        "3.9.0"
    }

    /// Returns the configuration definition for this transformation.
    ///
    /// This corresponds to `Filter.CONFIG_DEF` in Java.
    /// Filter has no configuration, so this returns an empty HashMap.
    pub fn config_def() -> HashMap<String, common_trait::config::ConfigKeyDef> {
        HashMap::new()
    }

    /// Returns the target of this filter.
    pub fn target(&self) -> FilterTarget {
        self.target
    }
}

impl Default for Filter {
    /// Creates a Filter with default target (Value).
    fn default() -> Self {
        Filter::new(FilterTarget::Value)
    }
}

impl Transformation<SourceRecord> for Filter {
    /// Configures this transformation.
    ///
    /// This corresponds to `Filter.configure(Map<String, ?> configs)` in Java.
    ///
    /// Filter has no configuration options, so this method does nothing.
    ///
    /// # Arguments
    ///
    /// * `configs` - Configuration map (ignored)
    fn configure(&mut self, _configs: HashMap<String, Value>) {
        // No configuration for Filter
    }

    /// Transforms the given record.
    ///
    /// This corresponds to `Filter.apply(R record)` in Java.
    ///
    /// Filter drops all records by returning `Ok(None)`.
    /// This signals to the transformation chain that the record should be filtered out.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to transform (will be dropped)
    ///
    /// # Returns
    ///
    /// Always returns `Ok(None)` to indicate the record should be filtered.
    fn transform(&self, _record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Drop all records by returning None
        Ok(None)
    }

    /// Closes this transformation.
    ///
    /// This corresponds to `Filter.close()` in Java.
    /// Filter has no resources to release, so this does nothing.
    fn close(&mut self) {
        // No resources to release
    }
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Filter{{target={}}}",
            match self.target {
                FilterTarget::Key => "Key",
                FilterTarget::Value => "Value",
            }
        )
    }
}

/// Filter$Key - convenience constructor for Key target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.Filter$Key` in Java.
pub fn filter_key() -> Filter {
    Filter::new(FilterTarget::Key)
}

/// Filter$Value - convenience constructor for Value target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.Filter$Value` in Java.
pub fn filter_value() -> Filter {
    Filter::new(FilterTarget::Value)
}
