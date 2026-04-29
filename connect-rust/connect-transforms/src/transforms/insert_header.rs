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

//! InsertHeader transformation for inserting a static header into records.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.InsertHeader` in Java.

use connect_api::connector::ConnectRecord;
use connect_api::data::Values;
use connect_api::errors::ConnectError;
use connect_api::header::{ConnectHeaders, Headers};
use connect_api::Transformation;
use serde_json::Value;
use std::collections::HashMap;

/// Configuration key for the header key name.
pub const HEADER_KEY_CONFIG: &str = "header";

/// Configuration key for the header value literal.
pub const VALUE_LITERAL_CONFIG: &str = "valueLiteral";

/// Version of the transformation.
pub const VERSION: &str = "3.9.0";

/// InsertHeader is a transformation that inserts a static header into each record.
///
/// This transformation adds a header with a configured name and literal value to
/// each record. The value is parsed using `Values::parse_string()` which can
/// interpret various types (boolean, number, string, null, etc.).
///
/// # Configuration
///
/// - `header`: The name of the header to insert (required, non-null, non-empty)
/// - `valueLiteral`: The literal value to set for the header (required, non-null)
///
/// # Example
///
/// ```ignore
/// let mut transform = InsertHeader::new();
/// let mut config = HashMap::new();
/// config.insert("header".to_string(), Value::String("my-header".to_string()));
/// config.insert("valueLiteral".to_string(), Value::String("my-value".to_string()));
/// transform.configure(config);
///
/// // After transformation, the record will have a header named "my-header" with value "my-value"
/// let result = transform.transform(record);
/// ```
#[derive(Debug, Clone, Default)]
pub struct InsertHeader {
    /// The name of the header to insert.
    header_key: String,
    /// The parsed value to insert as header value.
    header_value: Value,
}

impl InsertHeader {
    /// Creates a new InsertHeader transformation.
    pub fn new() -> Self {
        InsertHeader {
            header_key: String::new(),
            header_value: Value::Null,
        }
    }

    /// Returns the version of this transformation.
    pub fn version() -> &'static str {
        VERSION
    }

    /// Returns the configuration key for header name.
    pub fn header_key_config() -> &'static str {
        HEADER_KEY_CONFIG
    }

    /// Returns the configuration key for header value literal.
    pub fn value_literal_config() -> &'static str {
        VALUE_LITERAL_CONFIG
    }
}

impl<R> Transformation<R> for InsertHeader
where
    R: ConnectRecord,
{
    /// Configures this transformation with the header name and value literal.
    ///
    /// # Arguments
    ///
    /// * `configs` - Configuration map containing:
    ///   - `header`: The header name (required, must be non-empty string)
    ///   - `valueLiteral`: The literal value string (required, will be parsed)
    ///
    /// # Configuration Format
    ///
    /// ```json
    /// {"header": "header-name", "valueLiteral": "value-string"}
    /// ```
    ///
    /// # Value Parsing
    ///
    /// The `valueLiteral` is parsed using `Values::parse_string()` which can
    /// interpret:
    /// - "null" -> null value
    /// - "true"/"false" -> boolean
    /// - Numbers (integers, floats) -> numeric values
    /// - Dates, times, timestamps in ISO format
    /// - JSON arrays and maps
    /// - Any other string -> string value
    fn configure(&mut self, configs: HashMap<String, Value>) {
        // Get header key
        let header_key = configs
            .get(HEADER_KEY_CONFIG)
            .expect("Missing required configuration: header");

        match header_key {
            Value::String(s) => {
                if s.is_empty() {
                    panic!("Header key must not be empty string");
                }
                self.header_key = s.clone();
            }
            _ => panic!("Header configuration must be a string"),
        }

        // Get value literal
        let value_literal = configs
            .get(VALUE_LITERAL_CONFIG)
            .expect("Missing required configuration: valueLiteral");

        match value_literal {
            Value::String(s) => {
                // Parse the string literal using Values::parse_string
                let parsed = Values::parse_string(s).expect("Failed to parse valueLiteral");
                self.header_value = parsed.value().cloned().unwrap_or(Value::Null);
            }
            Value::Null => {
                panic!("valueLiteral must not be null");
            }
            _ => {
                // For non-string values, use directly
                self.header_value = value_literal.clone();
            }
        }
    }

    /// Transforms the given record by inserting the configured header.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to transform
    ///
    /// # Returns
    ///
    /// A new record with the configured header added. The header is added to
    /// the existing headers, preserving any existing headers.
    ///
    /// # Behavior
    ///
    /// - If a header with the same key already exists, this adds another header
    ///   with the same key (headers can have duplicate keys in Kafka)
    /// - The value is the parsed value from the configuration
    /// - All other record fields (topic, key, value, timestamp, etc.) are preserved
    fn transform(&self, record: R) -> Result<Option<R>, ConnectError> {
        // Create new headers by copying existing ones and adding the new header
        let mut updated_headers = ConnectHeaders::new();

        // Copy all existing headers
        for header in record.headers().iter() {
            updated_headers.add(header.key(), header.value().clone());
        }

        // Add the new header
        updated_headers.add(&self.header_key, self.header_value.clone());

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

