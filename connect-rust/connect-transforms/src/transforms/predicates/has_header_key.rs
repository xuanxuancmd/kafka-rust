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

//! HasHeaderKey predicate for checking if a record has a specific header key.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.predicates.HasHeaderKey` in Java.

use connect_api::connector::ConnectRecord;
use connect_api::header::Headers;
use connect_api::transforms::predicates::Predicate;
use serde_json::Value;
use std::collections::HashMap;

/// Configuration key for the header name.
pub const HEADER_NAME_CONFIG: &str = "name";

/// Version of the predicate.
pub const VERSION: &str = "3.9.0";

/// HasHeaderKey is a predicate that checks if a record has a header with a specific key.
///
/// This predicate can be used with transformations like `Filter` to conditionally
/// process records based on the presence of a specific header key.
///
/// # Configuration
///
/// - `name`: The header key to check for (required, non-empty string)
///
/// # Example
///
/// ```ignore
/// let mut predicate = HasHeaderKey::new();
/// let mut config = HashMap::new();
/// config.insert("name".to_string(), Value::String("my-header".to_string()));
/// predicate.configure(config);
///
/// // Returns true if the record has a header with key "my-header"
/// let result = predicate.test(&record);
/// ```
#[derive(Debug, Clone, Default)]
pub struct HasHeaderKey {
    /// The header key name to check for.
    name: String,
}

impl HasHeaderKey {
    /// Creates a new HasHeaderKey predicate.
    pub fn new() -> Self {
        HasHeaderKey {
            name: String::new(),
        }
    }

    /// Returns the version of this predicate.
    pub fn version() -> &'static str {
        VERSION
    }

    /// Returns the configuration key for the header name.
    pub fn header_name_config() -> &'static str {
        HEADER_NAME_CONFIG
    }
}

impl<R> Predicate<R> for HasHeaderKey
where
    R: ConnectRecord,
{
    /// Configures this predicate with the header name to check for.
    ///
    /// # Arguments
    ///
    /// * `configs` - Configuration map containing the `name` key
    ///
    /// # Panics
    ///
    /// Panics if the `name` configuration is missing or empty.
    fn configure(&mut self, configs: HashMap<String, Value>) {
        if let Some(value) = configs.get(HEADER_NAME_CONFIG) {
            if let Value::String(name) = value {
                if name.is_empty() {
                    panic!("Header name must not be empty");
                }
                self.name = name.clone();
            } else {
                panic!("Header name must be a string");
            }
        } else {
            panic!("Missing required configuration: {}", HEADER_NAME_CONFIG);
        }
    }

    /// Tests if the given record has a header with the configured key.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to test
    ///
    /// # Returns
    ///
    /// `true` if the record has at least one header with the configured key,
    /// `false` otherwise.
    fn test(&self, record: &R) -> bool {
        !record.headers().headers_with_key(&self.name).is_empty()
    }

    /// Closes this predicate.
    ///
    /// This implementation does nothing as there are no resources to release.
    fn close(&mut self) {
        // No resources to release
    }
}

