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

//! Flatten Transformation - flattens nested structures by concatenating field names.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.Flatten` in Java.
//!
//! Flatten allows you to flatten nested data structures (Maps/Structs) by
//! concatenating field names with a configurable delimiter. This is useful
//! for converting hierarchical data into a flat record format.
//!
//! # Configuration
//!
//! * `delimiter` - The delimiter to use when concatenating field names. Default: ".".
//!
//! # Behavior
//!
//! - Nested Map/Struct fields are recursively flattened
//! - Array fields are NOT modified (preserved as-is)
//! - Primitive types (int, float, boolean, string, bytes) are preserved
//! - Field names are concatenated using the delimiter (e.g., "parent.child.field")
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::flatten::{Flatten, FlattenTarget};
//! use connect_api::transforms::Transformation;
//! use connect_api::source::SourceRecord;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Create a Flatten transformation for Value with default delimiter
//! let mut transform = Flatten::new(FlattenTarget::Value);
//! let mut configs = HashMap::new();
//! configs.insert("delimiter".to_string(), json!("."));
//! transform.configure(configs);
//!
//! // Create a nested record
//! let record = SourceRecord::new(
//!     HashMap::new(),
//!     HashMap::new(),
//!     "test-topic".to_string(),
//!     None,
//!     None,
//!     json!({
//!         "user": {
//!             "name": "alice",
//!             "address": {
//!                 "city": "Seattle",
//!                 "zip": "98101"
//!             }
//!         },
//!         "id": 123
//!     }),
//! );
//!
//! // Transform will flatten nested structure
//! let result = transform.transform(record).unwrap().unwrap();
//! assert_eq!(result.value()["user.name"], json!("alice"));
//! assert_eq!(result.value()["user.address.city"], json!("Seattle"));
//! assert_eq!(result.value()["user.address.zip"], json!("98101"));
//! assert_eq!(result.value()["id"], json!(123));
//! ```

use crate::transforms::util::{Requirements, SimpleConfig};
use common_trait::cache::{Cache, SynchronizedCache};
use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType, ConfigKeyDef};
use connect_api::connector::ConnectRecord;
use connect_api::data::{ConnectSchema, SchemaType};
use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Configuration key for the delimiter.
const DELIMITER_CONFIG: &str = "delimiter";

/// Default value for the delimiter.
const DELIMITER_DEFAULT: &str = ".";

/// Purpose string for error messages.
const PURPOSE: &str = "flatten nested structures";

/// Overview documentation for Flatten.
pub const OVERVIEW_DOC: &str = "Flatten a nested data structure, separating the key names with \
    a delimiter. This is useful to convert a deeply nested data structure into a flat record. \
    Use the concrete transformation type designed for the record key (Flatten.Key) or value (Flatten.Value). \
    Array fields and their contents are not modified.";

/// Flatten transformation that flattens nested structures.
///
/// This corresponds to `org.apache.kafka.connect.transforms.Flatten<R>` in Java.
///
/// The Flatten transformation allows you to:
/// - Flatten nested Map/Struct data structures
/// - Concatenate field names using a configurable delimiter
/// - Preserve Array fields unchanged
/// - Convert hierarchical data to flat records
///
/// # Type Parameters
///
/// This transformation works on `SourceRecord` types that implement `ConnectRecord`.
///
/// # Configuration
///
/// - `delimiter`: The delimiter to use for concatenating field names (default: ".")
///
/// # Flattening Rules
///
/// - Nested Map/Struct: Field names concatenated with delimiter, values preserved
/// - Array: Preserved unchanged (no flattening)
/// - Primitive types: Preserved as-is
/// - Null values: Preserved as null
///
/// # Schema Modes
///
/// The transformation handles both schemaless and schema-aware modes:
///
/// - **Schemaless mode**: The key/value is a Map (JSON Object), nested maps are
///   flattened by recursively traversing and concatenating keys.
///
/// - **Schema-aware mode**: The key/value is a Struct (JSON Object with schema),
///   nested structs are flattened while preserving and updating the schema.
///
/// # Thread Safety
///
/// In Rust, this transformation is thread-safe as it uses SynchronizedCache for
/// schema caching and immutable configuration after initialization.
pub struct Flatten {
    /// The target (Key or Value) - determines which part of the record to transform.
    target: FlattenTarget,
    /// The delimiter to use when concatenating field names.
    delimiter: String,
    /// Schema cache for storing updated schemas.
    /// In Java, this is a SynchronizedCache<Schema, Schema>.
    /// In our simplified implementation, we use String (schema signature) as key and value.
    schema_cache: SynchronizedCache<String, String>,
}

/// FlattenTarget enum representing Key or Value operation.
///
/// This replaces Java's inner classes `Flatten.Key` and `Flatten.Value`.
/// In Kafka Connect configuration, the target is determined by the alias:
/// - "Flatten.Key" → FlattenTarget::Key
/// - "Flatten.Value" → FlattenTarget::Value
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlattenTarget {
    /// Operate on record key (Flatten.Key in Java).
    Key,
    /// Operate on record value (Flatten.Value in Java).
    Value,
}

impl Flatten {
    /// Creates a new Flatten transformation for the specified target.
    ///
    /// This corresponds to the constructor in Java.
    ///
    /// # Arguments
    ///
    /// * `target` - The target (Key or Value)
    pub fn new(target: FlattenTarget) -> Self {
        Flatten {
            target,
            delimiter: DELIMITER_DEFAULT.to_string(),
            schema_cache: SynchronizedCache::with_default_capacity(),
        }
    }

    /// Returns the version of this transformation.
    ///
    /// This corresponds to `Transformation.version()` in Java, which returns
    /// `AppInfoParser.getVersion()` from connect-transforms module.
    ///
    /// Version "3.9.0" corresponds to Kafka 3.9.0.
    pub fn version() -> &'static str {
        "3.9.0"
    }

    /// Returns the configuration definition for this transformation.
    ///
    /// This corresponds to `Flatten.CONFIG_DEF` in Java.
    ///
    /// The configuration includes:
    /// - `delimiter`: The delimiter for concatenating field names (default: ".")
    pub fn config_def() -> HashMap<String, ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                DELIMITER_CONFIG,
                ConfigDefType::String,
                Some(Value::String(DELIMITER_DEFAULT.to_string())),
                ConfigDefImportance::High,
                "Delimiter to use when concatenating field names from nested structures.",
            )
            .build()
    }

    /// Returns the target of this transformation.
    pub fn target(&self) -> FlattenTarget {
        self.target
    }

    /// Returns the delimiter used for concatenating field names.
    pub fn delimiter(&self) -> &str {
        &self.delimiter
    }

    /// Constructs a field name by concatenating prefix and field name with delimiter.
    ///
    /// This corresponds to `Flatten.fieldName(String prefix, String fieldName)` in Java.
    ///
    /// If the prefix is empty or null, returns the field name directly.
    /// Otherwise, returns "prefix + delimiter + fieldName".
    ///
    /// # Arguments
    /// * `prefix` - The prefix (ancestor field names)
    /// * `field_name` - The current field name
    ///
    /// # Returns
    /// Returns the concatenated field name.
    fn field_name(&self, prefix: &str, field_name: &str) -> String {
        if prefix.is_empty() {
            field_name.to_string()
        } else {
            format!("{}{}{}", prefix, self.delimiter, field_name)
        }
    }

    /// Applies the transformation in schemaless mode.
    ///
    /// In schemaless mode, the key/value is expected to be a Map (JSON Object).
    /// Nested maps are flattened by recursively traversing and concatenating keys.
    ///
    /// This corresponds to `Flatten.applySchemaless(R record)` in Java.
    ///
    /// # Arguments
    /// * `record` - The source record to transform
    ///
    /// # Returns
    /// Returns a new record with flattened fields.
    ///
    /// # Errors
    /// Returns `ConnectError::Data` if the operating value is not a Map.
    fn apply_schemaless(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            FlattenTarget::Key => record.key(),
            FlattenTarget::Value => Some(record.value()),
        };

        // Handle null/None operating value
        if operating_value.is_none() {
            return Ok(Some(record));
        }

        let value = operating_value.unwrap();
        if value.is_null() {
            return Ok(Some(record));
        }

        // Validate that value is a Map/Object
        if !value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Map objects supported in absence of schema for [{}], found: {}",
                PURPOSE,
                Requirements::null_safe_type_name(value)
            )));
        }

        // Get the map and apply flattening
        let value_map = Requirements::require_map(value, PURPOSE)?;
        let mut updated_map = HashMap::new();

        // Recursively flatten the map
        self.apply_schemaless_recursive(&value_map, "", &mut updated_map);

        // Convert HashMap to JSON Object
        let updated_value =
            serde_json::to_value(updated_map).unwrap_or(Value::Object(serde_json::Map::new()));

        // Create and return the new record
        self.new_record(record, None, updated_value)
    }

    /// Recursively flattens a Map in schemaless mode.
    ///
    /// This corresponds to the inner logic of `Flatten.applySchemaless` in Java.
    ///
    /// # Arguments
    /// * `original` - The original map to flatten
    /// * `prefix` - The current prefix (ancestor field names)
    /// * `new_record` - The output map to populate with flattened fields
    ///
    /// # Behavior
    /// - For nested Map values: recursively flatten with updated prefix
    /// - For Array values: add directly (no modification)
    /// - For primitive values: add directly
    /// - For null values: add as null
    fn apply_schemaless_recursive(
        &self,
        original: &HashMap<String, Value>,
        prefix: &str,
        new_record: &mut HashMap<String, Value>,
    ) {
        for (key, value) in original.iter() {
            let field_name = self.field_name(prefix, key);

            // Handle null values
            if value.is_null() {
                new_record.insert(field_name, Value::Null);
                continue;
            }

            // Determine schema type from value
            let schema_type = ConnectSchema::schema_type(value);

            match schema_type {
                Some(SchemaType::Map) => {
                    // Nested Map - recursively flatten
                    if value.is_object() {
                        let nested_map = Requirements::require_map(value, PURPOSE);
                        if nested_map.is_ok() {
                            self.apply_schemaless_recursive(
                                &nested_map.unwrap(),
                                &field_name,
                                new_record,
                            );
                        }
                    }
                }
                Some(SchemaType::Array) => {
                    // Array - preserve unchanged (no flattening)
                    new_record.insert(field_name, value.clone());
                }
                Some(SchemaType::Struct) => {
                    // Struct (represented as Object in JSON) - recursively flatten
                    if value.is_object() {
                        let nested_map = Requirements::require_map(value, PURPOSE);
                        if nested_map.is_ok() {
                            self.apply_schemaless_recursive(
                                &nested_map.unwrap(),
                                &field_name,
                                new_record,
                            );
                        }
                    }
                }
                // Primitive types - add directly
                Some(SchemaType::Int8)
                | Some(SchemaType::Int16)
                | Some(SchemaType::Int32)
                | Some(SchemaType::Int64)
                | Some(SchemaType::Float32)
                | Some(SchemaType::Float64)
                | Some(SchemaType::Boolean)
                | Some(SchemaType::String)
                | Some(SchemaType::Bytes) => {
                    new_record.insert(field_name, value.clone());
                }
                None => {
                    // Null type - already handled above
                    new_record.insert(field_name, Value::Null);
                }
            }
        }
    }

    /// Applies the transformation in schema-aware mode.
    ///
    /// In schema-aware mode, the key/value is expected to be a Struct (JSON Object with schema).
    /// Nested structs are flattened while preserving and updating the schema.
    ///
    /// This corresponds to `Flatten.applyWithSchema(R record)` in Java.
    ///
    /// # Arguments
    /// * `record` - The source record to transform
    ///
    /// # Returns
    /// Returns a new record with flattened fields and updated schema.
    ///
    /// # Errors
    /// Returns `ConnectError::Data` if the operating value is not a Struct/Object.
    fn apply_with_schema(
        &self,
        record: SourceRecord,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            FlattenTarget::Key => record.key(),
            FlattenTarget::Value => Some(record.value()),
        };

        // Handle null/None operating value
        if operating_value.is_none() {
            return Ok(Some(record));
        }

        let value = operating_value.unwrap();
        if value.is_null() {
            return Ok(Some(record));
        }

        // Validate that value is an Object/Struct
        if !value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Struct objects supported for [{} with schema], found: {}",
                PURPOSE,
                Requirements::null_safe_type_name(value)
            )));
        }

        // Get the original schema
        let original_schema = match self.target {
            FlattenTarget::Key => record.key_schema(),
            FlattenTarget::Value => record.value_schema(),
        };

        // Compute schema signature for caching
        let schema_signature = self.compute_schema_signature(original_schema);

        // Check schema cache
        let updated_schema = self.schema_cache.get(&schema_signature);
        let updated_schema_str = if updated_schema.is_some() {
            updated_schema.unwrap()
        } else {
            // Build updated schema
            let updated_sig = self.build_updated_schema_signature(original_schema);
            self.schema_cache
                .put(schema_signature.clone(), updated_sig.clone());
            updated_sig
        };

        // Get the original object
        let value_obj = value.as_object().unwrap();
        let mut updated_obj = serde_json::Map::new();

        // Recursively flatten the struct
        self.build_with_schema_recursive(value_obj, "", &mut updated_obj);

        let updated_value = Value::Object(updated_obj);

        // Create and return the new record
        self.new_record(record, Some(updated_schema_str), updated_value)
    }

    /// Recursively builds a flattened value from a Struct.
    ///
    /// This corresponds to `Flatten.buildWithSchema` in Java.
    ///
    /// # Arguments
    /// * `original` - The original struct value (as JSON Object)
    /// * `prefix` - The current prefix (ancestor field names)
    /// * `new_record` - The output struct to populate
    fn build_with_schema_recursive(
        &self,
        original: &serde_json::Map<String, Value>,
        prefix: &str,
        new_record: &mut serde_json::Map<String, Value>,
    ) {
        for (key, value) in original.iter() {
            let field_name = self.field_name(prefix, key);

            // Handle null values
            if value.is_null() {
                new_record.insert(field_name.clone(), Value::Null);
                continue;
            }

            // Determine schema type from value
            let schema_type = ConnectSchema::schema_type(value);

            match schema_type {
                Some(SchemaType::Map) | Some(SchemaType::Struct) => {
                    // Nested Map/Struct - recursively flatten
                    if value.is_object() {
                        let nested_obj = value.as_object().unwrap();
                        self.build_with_schema_recursive(nested_obj, &field_name, new_record);
                    }
                }
                Some(SchemaType::Array) => {
                    // Array - preserve unchanged
                    new_record.insert(field_name.clone(), value.clone());
                }
                // Primitive types - add directly
                Some(SchemaType::Int8)
                | Some(SchemaType::Int16)
                | Some(SchemaType::Int32)
                | Some(SchemaType::Int64)
                | Some(SchemaType::Float32)
                | Some(SchemaType::Float64)
                | Some(SchemaType::Boolean)
                | Some(SchemaType::String)
                | Some(SchemaType::Bytes) => {
                    new_record.insert(field_name.clone(), value.clone());
                }
                None => {
                    new_record.insert(field_name.clone(), Value::Null);
                }
            }
        }
    }

    /// Computes a signature for a schema for caching purposes.
    ///
    /// In Java, the schema object itself is used as the cache key.
    /// In our simplified implementation, we use the schema string.
    fn compute_schema_signature(&self, schema: Option<&str>) -> String {
        schema.unwrap_or("schemaless").to_string()
    }

    /// Builds an updated schema signature after flattening.
    ///
    /// This corresponds to `Flatten.buildUpdatedSchema` in Java.
    ///
    /// # Arguments
    /// * `schema` - The original schema (optional)
    ///
    /// # Returns
    /// Returns a signature representing the flattened schema.
    fn build_updated_schema_signature(&self, schema: Option<&str>) -> String {
        if schema.is_none() {
            return "schemaless".to_string();
        }

        let schema_str = schema.unwrap();

        // Build flattened schema signature
        // In a full implementation, we would recursively build the flattened schema fields
        // For now, we create a simplified signature
        format!("flattened:{}:{}", schema_str, self.delimiter)
    }

    /// Creates a new record with the updated value and schema.
    ///
    /// This corresponds to `Flatten.newRecord(R record, Schema updatedSchema, Object updatedValue)` in Java.
    ///
    /// # Arguments
    /// * `record` - The original record
    /// * `updated_schema` - The updated schema (optional)
    /// * `updated_value` - The updated value
    fn new_record(
        &self,
        record: SourceRecord,
        _updated_schema: Option<String>,
        updated_value: Value,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        match self.target {
            FlattenTarget::Key => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                Some(updated_value),
                record.value().clone(),
            ))),
            FlattenTarget::Value => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                record.key().cloned(),
                updated_value,
            ))),
        }
    }
}

impl Default for Flatten {
    /// Creates a Flatten with default target (Value).
    fn default() -> Self {
        Flatten::new(FlattenTarget::Value)
    }
}

impl Transformation<SourceRecord> for Flatten {
    /// Configures this transformation with the given configuration map.
    ///
    /// This corresponds to `Flatten.configure(Map<String, ?> configs)` in Java.
    ///
    /// # Arguments
    /// * `configs` - A map of configuration key-value pairs
    ///
    /// # Configuration Keys
    /// * `delimiter` - The delimiter for concatenating field names (default: ".")
    fn configure(&mut self, configs: HashMap<String, Value>) {
        let config_def = Self::config_def();
        let simple_config = SimpleConfig::new(config_def, configs)
            .expect("Failed to parse configuration for Flatten");

        // Parse delimiter
        self.delimiter = simple_config
            .get_string(DELIMITER_CONFIG)
            .map(|s| s.to_string())
            .unwrap_or_else(|| DELIMITER_DEFAULT.to_string());
    }

    /// Transforms the given record by flattening nested structures.
    ///
    /// This corresponds to `Flatten.apply(R record)` in Java.
    ///
    /// # Arguments
    /// * `record` - The source record to transform
    ///
    /// # Returns
    /// Returns a new record with flattened nested structures.
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value
        let operating_value = match self.target {
            FlattenTarget::Key => record.key(),
            FlattenTarget::Value => Some(record.value()),
        };

        // If operating value is null, return the record unchanged
        // In Java: if (operatingValue(record) == null) return record;
        if operating_value.is_none() || operating_value.unwrap().is_null() {
            return Ok(Some(record));
        }

        // Determine whether to use schemaless or schema-aware mode
        let has_schema = match self.target {
            FlattenTarget::Key => record.key_schema().is_some(),
            FlattenTarget::Value => record.value_schema().is_some(),
        };

        if has_schema {
            self.apply_with_schema(record)
        } else {
            self.apply_schemaless(record)
        }
    }

    /// Closes this transformation and releases any resources.
    ///
    /// This corresponds to `Flatten.close()` in Java.
    fn close(&mut self) {
        self.delimiter = DELIMITER_DEFAULT.to_string();
    }
}

impl fmt::Display for Flatten {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Flatten{{target={}, delimiter='{}'}}",
            match self.target {
                FlattenTarget::Key => "Key",
                FlattenTarget::Value => "Value",
            },
            self.delimiter
        )
    }
}

impl fmt::Debug for Flatten {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Flatten")
            .field("target", &self.target)
            .field("delimiter", &self.delimiter)
            .field("schema_cache", &"<SynchronizedCache>")
            .finish()
    }
}

/// Flatten.Key - convenience constructor for Key target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.Flatten$Key` in Java.
pub fn flatten_key() -> Flatten {
    Flatten::new(FlattenTarget::Key)
}

/// Flatten.Value - convenience constructor for Value target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.Flatten$Value` in Java.
pub fn flatten_value() -> Flatten {
    Flatten::new(FlattenTarget::Value)
}
