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

//! ValueToKey Transformation - extracts fields from value as new key.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.ValueToKey` in Java.
//!
//! ValueToKey replaces a record's key with a new key constructed from a subset
//! of fields present in the record's value.
//!
//! # Configuration
//!
//! * `fields` - A list of field names from the record value that will be used
//!   to form the new key. Required configuration.
//! * `replace.null.with.default` - A boolean flag indicating whether null fields
//!   with default values should be replaced by their defaults when creating the
//!   new key. Default: true.
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::ValueToKey;
//! use connect_api::transforms::Transformation;
//! use connect_api::source::SourceRecord;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Configure the transformation
//! let mut configs = HashMap::new();
//! configs.insert("fields".to_string(), json!(["id", "name"]));
//!
//! let mut transform = ValueToKey::new();
//! transform.configure(configs);
//!
//! // Create a record with value containing the fields
//! let record = SourceRecord::new(
//!     HashMap::new(),
//!     HashMap::new(),
//!     "test-topic".to_string(),
//!     None,
//!     Some(json!("original-key")),
//!     json!({"id": 123, "name": "test", "other": "value"}),
//! );
//!
//! // Transform will extract id and name from value as new key
//! let result = transform.transform(record).unwrap().unwrap();
//! // New key will be {"id": 123, "name": "test"}
//! ```

use crate::transforms::util::{Requirements, SimpleConfig};
use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType, ConfigKeyDef};
use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Configuration key for the fields list.
const FIELDS_CONFIG: &str = "fields";

/// Configuration key for replace null with default.
const REPLACE_NULL_CONFIG: &str = "replace.null.with.default";

/// Default value for replace null with default.
const DEFAULT_REPLACE_NULL: bool = true;

/// Overview documentation for ValueToKey.
pub const OVERVIEW_DOC: &str =
    "Replace the record's key with a new key formed from a subset of fields in the record's value.";

/// ValueToKey transformation that extracts fields from value as new key.
///
/// This corresponds to `org.apache.kafka.connect.transforms.ValueToKey<R>` in Java.
///
/// The ValueToKey transformation replaces a record's key with a new key constructed
/// from a subset of fields present in the record's value. This is useful for cases
/// where the key should be derived from specific fields within the value.
///
/// # Type Parameters
///
/// This transformation works on `SourceRecord` types that implement `ConnectRecord`.
///
/// # Configuration
///
/// - `fields`: A list of field names from the record value that will form the new key.
/// - `replace.null.with.default`: Whether to replace null values with defaults (default: true).
///
/// # Schema Modes
///
/// The transformation handles both schemaless and schema-aware modes:
///
/// - **Schemaless mode**: The value is a Map (JSON Object), and the key is created
///   as a new Map containing only the specified fields.
///
/// - **Schema-aware mode**: The value is a Struct (JSON Object with schema), and the
///   key is created as a new Struct with a schema derived from the value schema.
///   Field validation ensures specified fields exist in the value schema.
///
/// # Thread Safety
///
/// In Rust, this transformation is inherently thread-safe as it only uses immutable
/// configuration and thread-safe data structures.
#[derive(Debug)]
pub struct ValueToKey {
    /// The list of field names to extract from value as key.
    fields: Vec<String>,
    /// Whether to replace null values with default values (for schema-aware mode).
    replace_null_with_default: bool,
}

impl ValueToKey {
    /// Creates a new ValueToKey transformation.
    ///
    /// This corresponds to the constructor in Java.
    pub fn new() -> Self {
        ValueToKey {
            fields: Vec::new(),
            replace_null_with_default: DEFAULT_REPLACE_NULL,
        }
    }

    /// Returns the version of this transformation.
    ///
    /// This corresponds to `Transformation.version()` in Java, which returns AppInfoParser.getVersion().
    /// Version "3.9.0" corresponds to Kafka 3.9.0.
    pub fn version() -> &'static str {
        "3.9.0"
    }

    /// Returns the configuration definition for this transformation.
    ///
    /// This corresponds to `ValueToKey.CONFIG_DEF` in Java.
    ///
    /// The configuration includes:
    /// - `fields`: List of field names to extract as key (required)
    /// - `replace.null.with.default`: Whether to replace null with defaults (default: true)
    pub fn config_def() -> HashMap<String, ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                FIELDS_CONFIG,
                ConfigDefType::List,
                None,
                ConfigDefImportance::High,
                "Field names to extract from value to form the new key.",
            )
            .define(
                REPLACE_NULL_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(DEFAULT_REPLACE_NULL)),
                ConfigDefImportance::Medium,
                "Whether to replace null field values with their default values from the schema.",
            )
            .build()
    }

    /// Returns the fields to extract.
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Returns whether null values should be replaced with defaults.
    pub fn replace_null_with_default(&self) -> bool {
        self.replace_null_with_default
    }

    /// Returns the type name of a value, or "null" if the value is null.
    ///
    /// # Arguments
    /// * `x` - The value to get the type name for
    fn null_safe_type_name(x: &Value) -> String {
        match x {
            Value::Null => "null",
            Value::Bool(_) => "Boolean",
            Value::Number(_) => "Number",
            Value::String(_) => "String",
            Value::Array(_) => "Array",
            Value::Object(_) => "Map",
        }
        .to_string()
    }

    /// Applies the transformation in schemaless mode.
    ///
    /// In schemaless mode, the value is expected to be a Map (JSON Object).
    /// A new key Map is created containing only the specified fields.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the key replaced by the extracted fields.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if the value is not a Map.
    fn apply_schemaless(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // In Java: requireMap(record.value(), PURPOSE)
        // Extract fields from the value Map
        let value = record.value();

        // Validate that value is a Map (JSON Object)
        let value_map = Requirements::require_map(value, "extracting fields for key")?;

        // Create new key as a Map with only the specified fields
        let mut new_key_map = serde_json::Map::new();

        for field_name in &self.fields {
            // In schemaless mode, missing fields result in null values
            // No error is thrown for missing fields
            if let Some(field_value) = value_map.get(field_name) {
                new_key_map.insert(field_name.clone(), field_value.clone());
            } else {
                // Field not found, insert null
                new_key_map.insert(field_name.clone(), Value::Null);
            }
        }

        // Create new key Value
        let new_key = Value::Object(new_key_map);

        // Return new record with updated key and null key schema
        // In Java: record.newRecord(topic, partition, null, newKey, value, timestamp)
        Ok(Some(SourceRecord::new(
            record.source_partition().clone(),
            record.source_offset().clone(),
            record.topic().to_string(),
            record.kafka_partition(),
            Some(new_key),
            record.value().clone(),
        )))
    }

    /// Applies the transformation in schema-aware mode.
    ///
    /// In schema-aware mode, the value is expected to be a Struct (JSON Object with schema).
    /// A new key Struct is created with a schema derived from the value schema.
    ///
    /// Note: In the current Rust implementation, schemas are represented as strings
    /// (schema names), not full schema objects. This implementation treats schema-aware
    /// mode similarly to schemaless mode, but validates that specified fields exist
    /// in the value structure.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the key replaced by the extracted fields.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if a specified field does not exist in the value.
    fn apply_with_schema(
        &self,
        record: SourceRecord,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        // In Java: requireStruct(record.value(), PURPOSE)
        // In our Rust implementation with serde_json::Value, we treat schema-aware
        // mode similarly but validate field existence

        let value = record.value();

        // Validate that value is a Map/Object
        if !value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Struct objects supported for [extracting fields for key with schema], found: {}",
                Self::null_safe_type_name(value)
            )));
        }

        let value_obj = value.as_object().unwrap();

        // In schema-aware mode, validate that all specified fields exist in the value
        // This mirrors Java's behavior where missing fields in Struct throw DataException
        for field_name in &self.fields {
            if !value_obj.contains_key(field_name) {
                return Err(ConnectError::data(format!(
                    "Field {} does not exist in value schema",
                    field_name
                )));
            }
        }

        // Create new key as a Map/Object with only the specified fields
        let mut new_key_map = serde_json::Map::new();

        for field_name in &self.fields {
            // Get the field value
            let field_value = value_obj.get(field_name).unwrap();

            // Handle null value replacement based on configuration
            // Note: In our current implementation without full schema objects,
            // we cannot determine default values, so this primarily affects
            // how null values are handled
            if field_value.is_null() && self.replace_null_with_default {
                // In Java: value.get(field) returns default if null
                // In our implementation, we keep null (no default value info available)
                new_key_map.insert(field_name.clone(), Value::Null);
            } else {
                new_key_map.insert(field_name.clone(), field_value.clone());
            }
        }

        // Create new key Value
        let new_key = Value::Object(new_key_map);

        // Return new record with updated key and null key schema
        // In Java: keySchema is derived from valueSchema, but we set null
        Ok(Some(SourceRecord::new(
            record.source_partition().clone(),
            record.source_offset().clone(),
            record.topic().to_string(),
            record.kafka_partition(),
            Some(new_key),
            record.value().clone(),
        )))
    }
}

impl Default for ValueToKey {
    fn default() -> Self {
        Self::new()
    }
}

impl Transformation<SourceRecord> for ValueToKey {
    /// Configures this transformation with the given configuration map.
    ///
    /// This corresponds to `ValueToKey.configure(Map<String, ?> configs)` in Java.
    ///
    /// # Arguments
    ///
    /// * `configs` - A map of configuration key-value pairs
    ///
    /// # Configuration Keys
    ///
    /// * `fields` - A list of field names to extract from value (required, JSON array)
    /// * `replace.null.with.default` - Whether to replace null with defaults (optional, default: true)
    ///
    /// # Errors
    ///
    /// The configuration will fail if `fields` is not provided or is not a valid list.
    fn configure(&mut self, configs: HashMap<String, Value>) {
        let config_def = Self::config_def();
        let simple_config = SimpleConfig::new(config_def, configs)
            .expect("Failed to parse configuration for ValueToKey");

        // Parse fields list - expecting a JSON array of strings
        self.fields = simple_config.get_list(FIELDS_CONFIG).unwrap_or(Vec::new());

        // Parse replace.null.with.default boolean
        self.replace_null_with_default = simple_config
            .get_bool(REPLACE_NULL_CONFIG)
            .unwrap_or(DEFAULT_REPLACE_NULL);
    }

    /// Transforms the given record by replacing the key with fields from the value.
    ///
    /// This corresponds to `ValueToKey.apply(R record)` in Java.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the key replaced by extracted fields from the value.
    /// Returns `Ok(Some(record))` with the transformed record.
    /// Returns `Err(ConnectError)` on transformation errors.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if:
    /// - The value is not a Map/Object (schemaless mode)
    /// - The value is not a Struct/Object (schema-aware mode)
    /// - A specified field does not exist in the value (schema-aware mode only)
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Determine whether to use schemaless or schema-aware mode
        // In Java: if (operatingSchema(record.valueSchema()) != null)
        // In our implementation: check if value_schema is present
        let has_schema = record.value_schema().is_some();

        if has_schema {
            // Schema-aware mode
            self.apply_with_schema(record)
        } else {
            // Schemaless mode
            self.apply_schemaless(record)
        }
    }

    /// Closes this transformation and releases any resources.
    ///
    /// This corresponds to `ValueToKey.close()` in Java.
    /// In the Java implementation, this clears the schema cache.
    /// In Rust, we reset the fields and configuration to defaults.
    fn close(&mut self) {
        self.fields.clear();
        self.replace_null_with_default = DEFAULT_REPLACE_NULL;
    }
}

impl fmt::Display for ValueToKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ValueToKey{{fields={:?}, replace_null_with_default={}}}",
            self.fields, self.replace_null_with_default
        )
    }
}

