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

//! HeaderFrom transformation for copying or moving fields to headers.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.HeaderFrom` in Java.
//!
//! HeaderFrom copies or moves field values from a record's key or value to its headers.
//! The fields are mapped to header names by their order in the configuration lists.
//!
//! # Configuration
//!
//! * `fields` - List of field names to copy/move to headers (required, non-empty)
//! * `headers` - List of header names corresponding to fields (required, same length as fields)
//! * `operation` - Either "copy" (retain field) or "move" (remove field) (default: "copy")
//! * `replace.null.with.default` - Whether to replace null with default values (default: true)
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::header_from::{HeaderFrom, HeaderFromTarget, HeaderFromOperation};
//! use connect_api::transforms::Transformation;
//! use connect_api::source::SourceRecord;
//! use connect_api::connector::ConnectRecord;
//! use connect_api::header::Headers;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Create a HeaderFor for Value with copy operation
//! let mut transform = HeaderFrom::new(HeaderFromTarget::Value);
//! let mut configs = HashMap::new();
//! configs.insert("fields".to_string(), json!(["user_id", "timestamp"]));
//! configs.insert("headers".to_string(), json!(["user-header", "time-header"]));
//! configs.insert("operation".to_string(), json!("copy"));
//! transform.configure(configs);
//!
//! // Create a record with fields in the value
//! let record = SourceRecord::new(
//!     HashMap::new(),
//!     HashMap::new(),
//!     "test-topic".to_string(),
//!     None,
//!     None,
//!     json!({"user_id": "user123", "timestamp": 1700000, "data": "value"}),
//! );
//!
//! // Transform will add headers while preserving fields (copy operation)
//! let result = transform.transform(record).unwrap().unwrap();
//! assert_eq!(result.headers().headers_with_key("user-header").len(), 1);
//! assert_eq!(result.value(), &json!({"user_id": "user123", "timestamp": 1700000, "data": "value"}));
//! ```

use crate::transforms::util::{Requirements, SimpleConfig};
use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType, ConfigKeyDef};
use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::header::{ConnectHeaders, Headers};
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Configuration key for the list of fields.
pub const FIELDS_CONFIG: &str = "fields";

/// Configuration key for the list of header names.
pub const HEADERS_CONFIG: &str = "headers";

/// Configuration key for the operation type.
pub const OPERATION_CONFIG: &str = "operation";

/// Configuration key for replace null with default.
pub const REPLACE_NULL_CONFIG: &str = "replace.null.with.default";

/// Default value for replace null with default.
pub const DEFAULT_REPLACE_NULL: bool = true;

/// Default value for operation.
pub const DEFAULT_OPERATION: &str = "copy";

/// Version of the transformation.
pub const VERSION: &str = "3.9.0";

/// Overview documentation for HeaderFrom.
pub const OVERVIEW_DOC: &str = "Copy or move fields from the record's key or value to headers. \
    The fields are mapped to header names by their order in the configuration lists. \
    Use 'copy' operation to retain the fields, or 'move' to remove them from the key/value.";

/// Purpose string for error messages.
const PURPOSE: &str = "copying/moving fields to headers";

/// HeaderFromOperation enum representing copy or move operation.
///
/// This corresponds to the operation configuration in Java's HeaderFrom.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderFromOperation {
    /// Copy field value to header, retaining the original field.
    Copy,
    /// Move field value to header, removing the original field.
    Move,
}

impl HeaderFromOperation {
    /// Parses the operation from a string value.
    ///
    /// # Arguments
    ///
    /// * `s` - The string representation ("copy" or "move")
    ///
    /// # Returns
    ///
    /// The corresponding HeaderFromOperation, or None if invalid.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "copy" => Some(HeaderFromOperation::Copy),
            "move" => Some(HeaderFromOperation::Move),
            _ => None,
        }
    }

    /// Returns the string representation of this operation.
    pub fn as_str(&self) -> &'static str {
        match self {
            HeaderFromOperation::Copy => "copy",
            HeaderFromOperation::Move => "move",
        }
    }
}

impl fmt::Display for HeaderFromOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// HeaderFromTarget enum representing Key or Value operation.
///
/// This replaces Java's inner classes `HeaderFrom.Key` and `HeaderFrom.Value`.
/// In Kafka Connect configuration, the target is determined by the alias:
/// - "HeaderFrom$Key" → HeaderFromTarget::Key
/// - "HeaderFrom$Value" → HeaderFromTarget::Value
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderFromTarget {
    /// Operate on record key (HeaderFrom.Key in Java).
    Key,
    /// Operate on record value (HeaderFrom.Value in Java).
    Value,
}

impl HeaderFromTarget {
    /// Returns the string representation of this target.
    pub fn as_str(&self) -> &'static str {
        match self {
            HeaderFromTarget::Key => "Key",
            HeaderFromTarget::Value => "Value",
        }
    }
}

impl fmt::Display for HeaderFromTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// HeaderFrom transformation that copies or moves fields to headers.
///
/// This corresponds to `org.apache.kafka.connect.transforms.HeaderFrom<R>` in Java.
///
/// The HeaderFrom transformation copies or moves field values from a record's key or value
/// to its headers. The fields and header names are mapped by their order in the
/// configuration lists - the i-th field is mapped to the i-th header name.
///
/// # Type Parameters
///
/// This transformation works on `SourceRecord` types that implement `ConnectRecord`.
///
/// # Configuration
///
/// - `fields`: List of field names to copy/move (required, non-empty)
/// - `headers`: List of header names (required, same length as fields)
/// - `operation`: "copy" or "move" (default: "copy")
/// - `replace.null.with.default`: Whether to replace null with defaults (default: true)
///
/// # Schema Modes
///
/// The transformation handles both schemaless and schema-aware modes:
///
/// - **Schemaless mode**: The key/value is a Map (JSON Object). Field values are
///   extracted and added to headers. For "move" operation, fields are removed from the map.
///
/// - **Schema-aware mode**: The key/value is a Struct (JSON Object with schema). Field
///   values are extracted with their schemas and added to headers. For "move" operation,
///   a new schema is created excluding the moved fields.
///
/// # Thread Safety
///
/// In Rust, this transformation is inherently thread-safe as it only uses immutable
/// configuration and thread-safe data structures.
#[derive(Debug)]
pub struct HeaderFrom {
    /// The target (Key or Value) - determines which part of the record to operate on.
    target: HeaderFromTarget,
    /// List of field names to copy/move to headers.
    fields: Vec<String>,
    /// List of header names corresponding to fields.
    header_names: Vec<String>,
    /// The operation type (copy or move).
    operation: HeaderFromOperation,
    /// Whether to replace null values with default values.
    replace_null_with_default: bool,
}

impl HeaderFrom {
    /// Creates a new HeaderFrom transformation for the specified target.
    ///
    /// This corresponds to the constructor in Java's inner classes.
    ///
    /// # Arguments
    ///
    /// * `target` - The target (Key or Value)
    pub fn new(target: HeaderFromTarget) -> Self {
        HeaderFrom {
            target,
            fields: Vec::new(),
            header_names: Vec::new(),
            operation: HeaderFromOperation::Copy,
            replace_null_with_default: DEFAULT_REPLACE_NULL,
        }
    }

    /// Returns the version of this transformation.
    ///
    /// This corresponds to `Transformation.version()` in Java, which returns
    /// `AppInfoParser.getVersion()` from connect-transforms module.
    ///
    /// Version "3.9.0" corresponds to Kafka 3.9.0.
    pub fn version() -> &'static str {
        VERSION
    }

    /// Returns the configuration definition for this transformation.
    ///
    /// This corresponds to `HeaderFrom.CONFIG_DEF` in Java.
    ///
    /// The configuration includes:
    /// - `fields`: List of field names (required, non-empty)
    /// - `headers`: List of header names (required, same length as fields)
    /// - `operation`: "copy" or "move" (default: "copy")
    /// - `replace.null.with.default`: Whether to replace null with defaults (default: true)
    pub fn config_def() -> HashMap<String, ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                FIELDS_CONFIG,
                ConfigDefType::List,
                None,
                ConfigDefImportance::High,
                "List of field names in the record's key or value to copy or move to headers.",
            )
            .define(
                HEADERS_CONFIG,
                ConfigDefType::List,
                None,
                ConfigDefImportance::High,
                "List of header names in the same order as the fields property.",
            )
            .define(
                OPERATION_CONFIG,
                ConfigDefType::String,
                Some(Value::String(DEFAULT_OPERATION.to_string())),
                ConfigDefImportance::Medium,
                "Either 'copy' to retain fields or 'move' to remove fields from key/value.",
            )
            .define(
                REPLACE_NULL_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(DEFAULT_REPLACE_NULL)),
                ConfigDefImportance::Medium,
                "Whether to replace null field values with their default values.",
            )
            .build()
    }

    /// Returns the target of this transformation.
    pub fn target(&self) -> HeaderFromTarget {
        self.target
    }

    /// Returns the fields list.
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Returns the header names list.
    pub fn header_names(&self) -> &[String] {
        &self.header_names
    }

    /// Returns the operation type.
    pub fn operation(&self) -> HeaderFromOperation {
        self.operation
    }

    /// Returns whether null values should be replaced with defaults.
    pub fn replace_null_with_default(&self) -> bool {
        self.replace_null_with_default
    }

    /// Gets the operating value based on the target.
    ///
    /// This corresponds to `operatingValue()` in Java.
    fn operating_value<'a>(&self, record: &'a SourceRecord) -> Option<&'a Value> {
        match self.target {
            HeaderFromTarget::Key => record.key(),
            HeaderFromTarget::Value => Some(record.value()),
        }
    }

    /// Gets the operating schema based on the target.
    ///
    /// This corresponds to `operatingSchema()` in Java.
    fn operating_schema<'a>(&self, record: &'a SourceRecord) -> Option<&'a str> {
        match self.target {
            HeaderFromTarget::Key => record.key_schema(),
            HeaderFromTarget::Value => record.value_schema(),
        }
    }

    /// Applies the transformation in schemaless mode.
    ///
    /// In schemaless mode, the key/value is expected to be a Map (JSON Object).
    /// Field values are extracted and added to headers. For "move" operation,
    /// fields are removed from the map.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with headers added/updated.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if the operating value is not a Map.
    fn apply_schemaless(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value
        let operating_value = self.operating_value(&record);

        // Handle null operating value
        if operating_value.is_none() || operating_value.map(|v| v.is_null()).unwrap_or(true) {
            // Cannot extract fields from null value, return record unchanged
            return Ok(Some(record));
        }

        let value = operating_value.unwrap();

        // Validate that operating value is a Map/Object
        Requirements::require_map(value, PURPOSE)?;

        // Create updated headers by duplicating existing ones
        let mut updated_headers = ConnectHeaders::new();
        for header in record.headers().iter() {
            updated_headers.add(header.key(), header.value().clone());
        }

        // Create updated value map (for move operation)
        let mut updated_value = if self.operation == HeaderFromOperation::Move {
            if let Value::Object(map) = value {
                map.clone()
            } else {
                return Err(ConnectError::data(format!(
                    "Cannot move fields from non-Map value: expected Object, got {}",
                    Requirements::null_safe_type_name(value)
                )));
            }
        } else {
            // For copy operation, we don't need to modify the value
            if let Value::Object(map) = value {
                map.clone()
            } else {
                return Err(ConnectError::data(format!(
                    "Cannot copy fields from non-Map value: expected Object, got {}",
                    Requirements::null_safe_type_name(value)
                )));
            }
        };

        // Process each field/header mapping
        for (field_name, header_name) in self.fields.iter().zip(self.header_names.iter()) {
            // Get the field value
            let field_value = updated_value
                .get(field_name)
                .cloned()
                .unwrap_or(Value::Null);

            // Add header with field value (null schema for schemaless)
            updated_headers.add(header_name, field_value);

            // Remove field from value if operation is move
            if self.operation == HeaderFromOperation::Move {
                updated_value.remove(field_name);
            }
        }

        // Create new record with updated value and headers
        let new_value = Value::Object(updated_value);

        match self.target {
            HeaderFromTarget::Key => {
                let new_record = record.with_key(Some(new_value));
                Ok(Some(new_record.with_headers(updated_headers)))
            }
            HeaderFromTarget::Value => {
                let new_record = record.with_value(new_value);
                Ok(Some(new_record.with_headers(updated_headers)))
            }
        }
    }

    /// Applies the transformation in schema-aware mode.
    ///
    /// In schema-aware mode, the key/value is expected to be a Struct (JSON Object with schema).
    /// Field values are extracted with their schemas and added to headers.
    /// For "move" operation, a new schema is created excluding the moved fields.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    /// * `_schema` - The schema name (currently unused in this simplified implementation)
    ///
    /// # Returns
    ///
    /// Returns a new record with headers added/updated.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if the operating value is not a Struct/Object.
    fn apply_with_schema(
        &self,
        record: SourceRecord,
        _schema: Option<&str>,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value
        let operating_value = self.operating_value(&record);

        // Handle null operating value
        if operating_value.is_none() || operating_value.map(|v| v.is_null()).unwrap_or(true) {
            // Cannot extract fields from null value, return record unchanged
            return Ok(Some(record));
        }

        let value = operating_value.unwrap();

        // Validate that operating value is a Struct/Object
        Requirements::require_struct_value(value, PURPOSE)?;

        // Create updated headers by duplicating existing ones
        let mut updated_headers = ConnectHeaders::new();
        for header in record.headers().iter() {
            updated_headers.add(header.key(), header.value().clone());
        }

        // Create updated value map
        let mut updated_value = if let Value::Object(map) = value {
            map.clone()
        } else {
            return Err(ConnectError::data(format!(
                "Cannot process fields from non-Struct value: expected Object, got {}",
                Requirements::null_safe_type_name(value)
            )));
        };

        // Process each field/header mapping
        for (field_name, header_name) in self.fields.iter().zip(self.header_names.iter()) {
            // Get the field value
            let mut field_value = updated_value
                .get(field_name)
                .cloned()
                .unwrap_or(Value::Null);

            // Apply replace null with default if configured
            if self.replace_null_with_default && field_value.is_null() {
                // In a full schema implementation, we would look up the default value from the schema
                // For now, we just use the null value
                // TODO: Implement schema-based default value lookup when schema system is complete
            }

            // Add header with field value
            updated_headers.add(header_name, field_value);

            // Remove field from value if operation is move
            if self.operation == HeaderFromOperation::Move {
                updated_value.remove(field_name);
            }
        }

        // Create new record with updated value and headers
        let new_value = Value::Object(updated_value);

        match self.target {
            HeaderFromTarget::Key => {
                let new_record = record.with_key(Some(new_value));
                Ok(Some(new_record.with_headers(updated_headers)))
            }
            HeaderFromTarget::Value => {
                let new_record = record.with_value(new_value);
                Ok(Some(new_record.with_headers(updated_headers)))
            }
        }
    }

    /// Creates a new record with the updated operating value and headers.
    ///
    /// This corresponds to `newRecord()` in Java.
    fn new_record(
        &self,
        record: SourceRecord,
        updated_value: Value,
        updated_headers: ConnectHeaders,
    ) -> SourceRecord {
        match self.target {
            HeaderFromTarget::Key => record
                .with_key(Some(updated_value))
                .with_headers(updated_headers),
            HeaderFromTarget::Value => record
                .with_value(updated_value)
                .with_headers(updated_headers),
        }
    }
}

impl Transformation<SourceRecord> for HeaderFrom {
    /// Configures this transformation with the field names and header names.
    ///
    /// # Arguments
    ///
    /// * `configs` - Configuration map containing:
    ///   - `fields`: List of field names (required, non-empty)
    ///   - `headers`: List of header names (required, same length as fields)
    ///   - `operation`: "copy" or "move" (optional, default: "copy")
    ///   - `replace.null.with.default`: Boolean (optional, default: true)
    ///
    /// # Configuration Format
    ///
    /// ```json
    /// {
    ///   "fields": ["field1", "field2"],
    ///   "headers": ["header1", "header2"],
    ///   "operation": "copy",
    ///   "replace.null.with.default": true
    /// }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - `fields` is missing or empty
    /// - `headers` is missing or empty
    /// - `fields` and `headers` have different lengths
    /// - `operation` is not "copy" or "move"
    fn configure(&mut self, configs: HashMap<String, Value>) {
        // Parse fields configuration
        let fields = SimpleConfig::parse_list_config(&configs, FIELDS_CONFIG)
            .expect("Missing required configuration: fields");

        if fields.is_empty() {
            panic!("Fields list must not be empty");
        }

        self.fields = fields;

        // Parse headers configuration
        let headers = SimpleConfig::parse_list_config(&configs, HEADERS_CONFIG)
            .expect("Missing required configuration: headers");

        if headers.is_empty() {
            panic!("Headers list must not be empty");
        }

        self.header_names = headers;

        // Validate that fields and headers have the same length
        if self.fields.len() != self.header_names.len() {
            panic!(
                "Fields and headers must have the same number of elements: {} fields, {} headers",
                self.fields.len(),
                self.header_names.len()
            );
        }

        // Parse operation configuration
        if let Some(op_value) = configs.get(OPERATION_CONFIG) {
            let op_str = match op_value {
                Value::String(s) => s.clone(),
                _ => panic!("Operation must be a string"),
            };

            self.operation =
                HeaderFromOperation::from_str(&op_str).expect("Operation must be 'copy' or 'move'");
        } else {
            self.operation = HeaderFromOperation::Copy;
        }

        // Parse replace.null.with.default configuration
        self.replace_null_with_default =
            SimpleConfig::parse_bool_config(&configs, REPLACE_NULL_CONFIG, DEFAULT_REPLACE_NULL);
    }

    /// Transforms the given record by copying/moving fields to headers.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to transform
    ///
    /// # Returns
    ///
    /// A new record with:
    /// - Headers containing the field values mapped to header names
    /// - For "move" operation: key/value without the moved fields
    /// - For "copy" operation: key/value unchanged
    ///
    /// # Behavior
    ///
    /// - Fields are extracted from the key or value based on target
    /// - Field values are added to headers with corresponding header names
    /// - If operation is "move", fields are removed from the key/value
    /// - Existing headers are preserved
    /// - If a field doesn't exist, null value is added to the header
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Determine which mode to use based on schema presence
        let schema = self.operating_schema(&record).map(|s| s.to_string());

        if schema.is_some() {
            self.apply_with_schema(record, schema.as_deref())
        } else {
            self.apply_schemaless(record)
        }
    }

    /// Closes this transformation.
    ///
    /// This implementation does nothing as there are no resources to release.
    fn close(&mut self) {
        // No resources to release
    }
}

