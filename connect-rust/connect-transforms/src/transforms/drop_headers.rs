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

//! DropHeaders transformation for removing specified headers from records.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.DropHeaders` in Java.

use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::header::{ConnectHeaders, Headers};
use connect_api::source::SourceRecord;
use connect_api::Transformation;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Configuration key for the list of header keys to drop.
pub const HEADERS_CONFIG: &str = "headers";

/// Version of the transformation.
pub const VERSION: &str = "3.9.0";

/// DropHeaders is a transformation that removes specified headers from records.
///
/// This transformation removes all headers whose keys match any of the configured
/// header keys. If a configured header key does not exist in the record, it is
/// silently ignored.
///
/// # Configuration
///
/// - `headers`: A list of header keys to remove (required, non-empty)
///
/// # Example
///
/// ```ignore
/// let mut transform = DropHeaders::new();
/// let mut config = HashMap::new();
/// config.insert("headers".to_string(), Value::Array(vec![
///     Value::String("unwanted-header".to_string()),
///     Value::String("another-header".to_string()),
/// ]));
/// transform.configure(config);
///
/// // After transformation, headers "unwanted-header" and "another-header" are removed
/// let result = transform.transform(record);
/// ```
#[derive(Debug, Clone, Default)]
pub struct DropHeaders {
    /// The set of header keys to drop.
    headers_to_drop: HashSet<String>,
}

impl DropHeaders {
    /// Creates a new DropHeaders transformation.
    pub fn new() -> Self {
        DropHeaders {
            headers_to_drop: HashSet::new(),
        }
    }

    /// Returns the version of this transformation.
    pub fn version() -> &'static str {
        VERSION
    }

    /// Returns the configuration key for headers.
    pub fn headers_config() -> &'static str {
        HEADERS_CONFIG
    }
}

impl Transformation<SourceRecord> for DropHeaders {
    /// Configures this transformation with the header keys to drop.
    ///
    /// # Arguments
    ///
    /// * `configs` - Configuration map containing the `headers` key with a list of header names
    ///
    /// # Configuration Format
    ///
    /// The `headers` configuration must be a JSON array of strings:
    /// ```json
    /// {"headers": ["header1", "header2", ...]}
    /// ```
    fn configure(&mut self, configs: HashMap<String, Value>) {
        if let Some(value) = configs.get(HEADERS_CONFIG) {
            match value {
                Value::Array(arr) => {
                    if arr.is_empty() {
                        panic!("Headers list must not be empty");
                    }
                    for item in arr {
                        match item {
                            Value::String(s) => {
                                if s.is_empty() {
                                    panic!("Header key must not be empty string");
                                }
                                self.headers_to_drop.insert(s.clone());
                            }
                            _ => panic!("Each header key must be a string"),
                        }
                    }
                }
                _ => panic!("Headers configuration must be an array"),
            }
        } else {
            panic!("Missing required configuration: {}", HEADERS_CONFIG);
        }
    }

    /// Transforms the given record by removing specified headers.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to transform
    ///
    /// # Returns
    ///
    /// A new record with all headers except those matching the configured keys.
    /// If no headers match the configured keys, the original headers are preserved.
    ///
    /// # Behavior
    ///
    /// - Headers with keys in the configured set are removed
    /// - Headers with keys not in the configured set are retained
    /// - If a configured header key does not exist, it is silently ignored
    /// - Multiple headers with the same key are all removed if the key is configured
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Create new ConnectHeaders to hold retained headers
        let mut updated_headers = ConnectHeaders::new();

        // Iterate through all headers and retain those not in the drop set
        for header in record.headers().iter() {
            let key = header.key();
            if !self.headers_to_drop.contains(key) {
                // Retain this header by adding it to updated_headers
                updated_headers.add(key, header.value().clone());
            }
        }

        // Create new record with updated headers
        Ok(Some(record.with_headers(updated_headers)))
    }

    /// Closes this transformation.
    ///
    /// This implementation does nothing as there are no resources to release.
    fn close(&mut self) {
        // No resources to release
    }
}
