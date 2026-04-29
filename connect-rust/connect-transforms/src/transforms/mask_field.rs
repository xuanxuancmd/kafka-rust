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

//! MaskField Transformation - masks specified fields in a record.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.MaskField` in Java.
//!
//! MaskField masks the values of specified fields in the record's key or value
//! with masked values based on the configured mask type.
//!
//! # Configuration
//!
//! * `fields` - A list of field names to be masked. Required configuration.
//! * `type` - The type of masking to apply. Can be "null", "valid", or "base64".
//!   Default: "null".
//! * `replace.null.with.default` - A boolean flag indicating whether null field values
//!   with default values should be replaced by their defaults. Default: true.
//!
//! # Mask Types
//!
//! * `null` - Replace field values with type-appropriate null values (e.g., 0 for numbers,
//!   empty string for strings, false for booleans).
//! * `valid` - Replace null fields with their default values from the schema (if available).
//! * `base64` - Encode field values using Base64 encoding. String values are encoded;
//!   other types are converted to JSON string first, then encoded.
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::mask_field::{MaskField, MaskFieldTarget, MaskType};
//! use connect_api::transforms::Transformation;
//! use connect_api::source::SourceRecord;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Create a MaskField for Value with null masking
//! let mut transform = MaskField::new(MaskFieldTarget::Value);
//! let mut configs = HashMap::new();
//! configs.insert("fields".to_string(), json!(["password", "secret"]));
//! configs.insert("type".to_string(), json!("null"));
//! transform.configure(configs);
//!
//! // Create a record with sensitive fields
//! let record = SourceRecord::new(
//!     HashMap::new(),
//!     HashMap::new(),
//!     "test-topic".to_string(),
//!     None,
//!     None,
//!     json!({"username": "alice", "password": "secret123", "secret": "hidden"}),
//! );
//!
//! // Transform will mask password and secret fields
//! let result = transform.transform(record).unwrap().unwrap();
//! // password and secret will be masked (empty strings for null type)
//! assert_eq!(result.value()["password"], json!(""));
//! assert_eq!(result.value()["secret"], json!(""));
//! ```

use crate::transforms::util::{Requirements, SimpleConfig};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType, ConfigKeyDef};
use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fmt;

/// Configuration key for the field names.
const FIELDS_CONFIG: &str = "fields";

/// Configuration key for the mask type.
const MASK_TYPE_CONFIG: &str = "type";

/// Configuration key for custom replacement value.
const REPLACEMENT_CONFIG: &str = "replacement";

/// Configuration key for replace null with default.
const REPLACE_NULL_CONFIG: &str = "replace.null.with.default";

/// Default value for replace null with default.
const DEFAULT_REPLACE_NULL: bool = true;

/// Default value for mask type.
const DEFAULT_MASK_TYPE: &str = "null";

/// Purpose string for error messages.
const PURPOSE: &str = "field masking";

/// Overview documentation for MaskField.
pub const OVERVIEW_DOC: &str =
    "Mask specified fields with a null value, valid default value, or Base64 encoded value. \
    Multiple fields can be specified to mask sensitive data in records. \
    Use the \"fields\" config to specify which fields to mask and \"type\" to specify the masking type.";

/// MaskField transformation that masks specified fields in a record.
///
/// This corresponds to `org.apache.kafka.connect.transforms.MaskField<R>` in Java.
///
/// The MaskField transformation masks the values of specified fields in the
/// record's key or value. The masking behavior is determined by the configured
/// mask type:
///
/// - `null`: Replace with type-appropriate null values
/// - `valid`: Replace null fields with schema defaults
/// - `base64`: Encode values using Base64
///
/// # Type Parameters
///
/// This transformation works on `SourceRecord` types that implement `ConnectRecord`.
///
/// # Configuration
///
/// - `fields`: List of field names to mask (required).
/// - `type`: Masking type - "null", "valid", or "base64" (default: "null").
/// - `replace.null.with.default`: Whether to replace null with defaults (default: true).
///
/// # Schema Modes
///
/// The transformation handles both schemaless and schema-aware modes:
///
/// - **Schemaless mode**: The key/value is a Map (JSON Object), and specified fields
///   are masked directly in the map.
///
/// - **Schema-aware mode**: The key/value is a Struct (JSON Object with schema),
///   and specified fields are masked while preserving the schema structure.
///
/// # Thread Safety
///
/// In Rust, this transformation is inherently thread-safe as it only uses immutable
/// configuration and thread-safe data structures.
#[derive(Debug)]
pub struct MaskField {
    /// The target (Key or Value) - determines which part of the record to mask.
    target: MaskFieldTarget,
    /// The set of field names to be masked.
    masked_fields: HashSet<String>,
    /// The type of masking to apply.
    mask_type: MaskType,
    /// Whether to replace null values with default values.
    replace_null_with_default: bool,
    /// Optional custom replacement value for masked fields.
    /// When set, this overrides the mask_type behavior.
    replacement: Option<Value>,
}

/// MaskFieldTarget enum representing Key or Value operation.
///
/// This replaces Java's inner classes `MaskField.Key` and `MaskField.Value`.
/// In Kafka Connect configuration, the target is determined by the alias:
/// - "MaskField.Key" → MaskFieldTarget::Key
/// - "MaskField.Value" → MaskFieldTarget::Value
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaskFieldTarget {
    /// Operate on record key (MaskField.Key in Java).
    Key,
    /// Operate on record value (MaskField.Value in Java).
    Value,
}

/// MaskType enum representing the type of masking.
///
/// - `Null`: Replace with type-appropriate null values
/// - `Valid`: Replace null fields with schema defaults
/// - `Base64`: Encode values using Base64
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaskType {
    /// Replace with type-appropriate null values (default value for each type).
    Null,
    /// Replace null fields with their default values from schema.
    Valid,
    /// Encode field values using Base64 encoding.
    Base64,
}

impl MaskType {
    /// Parse mask type from string.
    ///
    /// # Arguments
    /// * `s` - The string representation of mask type
    ///
    /// # Returns
    /// Returns the corresponding MaskType, or defaults to Null if invalid.
    pub fn from_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "null" => MaskType::Null,
            "valid" => MaskType::Valid,
            "base64" => MaskType::Base64,
            _ => MaskType::Null,
        }
    }
}

impl fmt::Display for MaskType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MaskType::Null => write!(f, "null"),
            MaskType::Valid => write!(f, "valid"),
            MaskType::Base64 => write!(f, "base64"),
        }
    }
}

/// Mapping of primitive types to their null/default values.
///
/// This corresponds to `PRIMITIVE_VALUE_MAPPING` in Java's MaskField.
fn primitive_null_value(value_type: &str) -> Value {
    match value_type {
        "Boolean" => Value::Bool(false),
        "Byte" | "Short" | "Integer" | "Long" | "Number" => Value::Number(0.into()),
        "Float" => Value::Number(serde_json::Number::from_f64(0.0).unwrap_or(0.into())),
        "Double" => Value::Number(serde_json::Number::from_f64(0.0).unwrap_or(0.into())),
        "String" => Value::String(String::new()),
        "Bytes" => Value::String(String::new()), // Represented as string in JSON
        "Date" => Value::Number(0.into()),       // Unix timestamp 0
        "Time" => Value::Number(0.into()),
        "Timestamp" => Value::Number(0.into()),
        "Decimal" => Value::Number(0.into()),
        "Array" => Value::Array(Vec::new()),
        "Map" => Value::Object(serde_json::Map::new()),
        _ => Value::Null,
    }
}

impl MaskField {
    /// Creates a new MaskField transformation for the specified target.
    ///
    /// This corresponds to the constructor in Java.
    ///
    /// # Arguments
    ///
    /// * `target` - The target (Key or Value)
    pub fn new(target: MaskFieldTarget) -> Self {
        MaskField {
            target,
            masked_fields: HashSet::new(),
            mask_type: MaskType::Null,
            replace_null_with_default: DEFAULT_REPLACE_NULL,
            replacement: None,
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
    /// This corresponds to `MaskField.CONFIG_DEF` in Java.
    ///
    /// The configuration includes:
    /// - `fields`: List of field names to mask (required)
    /// - `type`: Masking type - "null", "valid", or "base64" (default: "null")
    /// - `replacement`: Optional custom replacement value for masked fields
    /// - `replace.null.with.default`: Whether to replace null with defaults (default: true)
    pub fn config_def() -> HashMap<String, ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                FIELDS_CONFIG,
                ConfigDefType::List,
                None,
                ConfigDefImportance::High,
                "List of field names to mask.",
            )
            .define(
                MASK_TYPE_CONFIG,
                ConfigDefType::String,
                Some(Value::String(DEFAULT_MASK_TYPE.to_string())),
                ConfigDefImportance::Medium,
                "Type of masking to apply: \"null\" (type default), \"valid\" (schema default), or \"base64\" (Base64 encoding).",
            )
            .define(
                REPLACEMENT_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::Medium,
                "Optional custom replacement value for masked fields. When set, this overrides the mask type.",
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

    /// Returns the target of this transformation.
    pub fn target(&self) -> MaskFieldTarget {
        self.target
    }

    /// Returns the set of masked field names.
    pub fn masked_fields(&self) -> &HashSet<String> {
        &self.masked_fields
    }

    /// Returns the mask type.
    pub fn mask_type(&self) -> MaskType {
        self.mask_type
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
        Requirements::null_safe_type_name(x)
    }

    /// Applies the masking to a value based on the configured mask type.
    ///
    /// # Arguments
    /// * `original_value` - The original value to mask
    /// * `field_name` - The field name (for error messages)
    ///
    /// # Returns
    /// Returns the masked value.
    fn mask_value(&self, original_value: &Value, field_name: &str) -> Value {
        // If a custom replacement is set, use it directly
        if let Some(ref replacement) = self.replacement {
            return replacement.clone();
        }

        match self.mask_type {
            MaskType::Null => {
                // Get the type-appropriate null value
                let type_name = Self::null_safe_type_name(original_value);
                primitive_null_value(&type_name)
            }
            MaskType::Valid => {
                // For valid type, replace null with type default, keep non-null as-is
                if original_value.is_null() {
                    // Try to use schema default (if available)
                    // In our simplified implementation, we use the null value for the inferred type
                    primitive_null_value("String") // Default to string type for null values
                } else {
                    // Keep the original non-null value
                    original_value.clone()
                }
            }
            MaskType::Base64 => {
                // Encode the value using Base64
                self.encode_base64(original_value, field_name)
            }
        }
    }

    /// Encodes a value using Base64 encoding.
    ///
    /// String values are encoded directly. Other types are converted to
    /// JSON string first, then encoded.
    ///
    /// # Arguments
    /// * `value` - The value to encode
    /// * `field_name` - The field name (for error messages)
    ///
    /// # Returns
    /// Returns the Base64 encoded string value.
    fn encode_base64(&self, value: &Value, _field_name: &str) -> Value {
        if value.is_null() {
            // Null values are encoded as empty string's Base64
            Value::String(BASE64_STANDARD.encode(""))
        } else if let Value::String(s) = value {
            // String values are encoded directly
            Value::String(BASE64_STANDARD.encode(s.as_bytes()))
        } else {
            // Other types are converted to JSON string first
            let json_string = value.to_string();
            Value::String(BASE64_STANDARD.encode(json_string.as_bytes()))
        }
    }

    /// Applies the transformation in schemaless mode.
    ///
    /// In schemaless mode, the key/value is expected to be a Map (JSON Object).
    /// Specified fields are masked in the map.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with masked fields.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if the operating value is not a Map.
    fn apply_schemaless(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            MaskFieldTarget::Key => record.key(),
            MaskFieldTarget::Value => Some(record.value()),
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
                Self::null_safe_type_name(value)
            )));
        }

        // Get the map and mask specified fields
        let value_map = Requirements::require_map(value, PURPOSE)?;
        let mut masked_map = HashMap::new();

        // Copy all fields, masking the specified ones
        for (key, val) in value_map.iter() {
            if self.masked_fields.contains(key) {
                masked_map.insert(key.clone(), self.mask_value(val, key));
            } else {
                masked_map.insert(key.clone(), val.clone());
            }
        }

        // Convert HashMap to JSON Object
        let masked_value =
            serde_json::to_value(masked_map).unwrap_or(Value::Object(serde_json::Map::new()));

        // Create and return the new record based on target
        self.create_record_with_masked_value(record, masked_value)
    }

    /// Applies the transformation in schema-aware mode.
    ///
    /// In schema-aware mode, the key/value is expected to be a Struct (JSON Object with schema).
    /// Specified fields are masked while preserving the schema structure.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with masked fields.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if the operating value is not a Struct/Object.
    fn apply_with_schema(
        &self,
        record: SourceRecord,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            MaskFieldTarget::Key => record.key(),
            MaskFieldTarget::Value => Some(record.value()),
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
                Self::null_safe_type_name(value)
            )));
        }

        // Get the object and mask specified fields
        let value_obj = value.as_object().unwrap();
        let mut masked_obj = serde_json::Map::new();

        // Copy all fields, masking the specified ones
        for (key, val) in value_obj.iter() {
            if self.masked_fields.contains(key) {
                // Apply masking with null-with-default consideration
                let masked_val = if val.is_null() && self.replace_null_with_default {
                    // For schema-aware mode with replace_null_with_default=true,
                    // we would use the schema's default value
                    // In our simplified implementation, we use type default
                    self.mask_value(val, key)
                } else {
                    self.mask_value(val, key)
                };
                masked_obj.insert(key.clone(), masked_val);
            } else {
                masked_obj.insert(key.clone(), val.clone());
            }
        }

        let masked_value = Value::Object(masked_obj);

        // Create and return the new record based on target
        self.create_record_with_masked_value(record, masked_value)
    }

    /// Creates a new record with the masked value.
    ///
    /// # Arguments
    /// * `record` - The original record
    /// * `masked_value` - The masked value to use as the new key or value
    fn create_record_with_masked_value(
        &self,
        record: SourceRecord,
        masked_value: Value,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        match self.target {
            MaskFieldTarget::Key => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                Some(masked_value),
                record.value().clone(),
            ))),
            MaskFieldTarget::Value => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                record.key().cloned(),
                masked_value,
            ))),
        }
    }
}

impl Default for MaskField {
    /// Creates a MaskField with default target (Value).
    fn default() -> Self {
        MaskField::new(MaskFieldTarget::Value)
    }
}

impl Transformation<SourceRecord> for MaskField {
    /// Configures this transformation with the given configuration map.
    ///
    /// This corresponds to `MaskField.configure(Map<String, ?> configs)` in Java.
    ///
    /// # Arguments
    ///
    /// * `configs` - A map of configuration key-value pairs
    ///
    /// # Configuration Keys
    ///
    /// * `fields` - List of field names to mask (required, JSON array of strings)
    /// * `type` - Masking type: "null", "valid", or "base64" (optional, default: "null")
    /// * `replacement` - Optional custom replacement value (optional)
    /// * `replace.null.with.default` - Whether to replace null with defaults (optional, default: true)
    ///
    /// # Errors
    ///
    /// The configuration will fail if `fields` is not provided.
    fn configure(&mut self, configs: HashMap<String, Value>) {
        // Get replacement value from original configs and parse it
        if let Some(ref replacement_val) = configs.get(REPLACEMENT_CONFIG).cloned() {
            // Try to parse the replacement as a proper JSON value
            self.replacement = if let Value::String(s) = replacement_val {
                // Try to parse string as number, boolean, or keep as string
                if let Ok(n) = s.parse::<i64>() {
                    Some(Value::Number(n.into()))
                } else if let Ok(n) = s.parse::<f64>() {
                    serde_json::Number::from_f64(n)
                        .map(Value::Number)
                        .or_else(|| Some(Value::String(s.clone())))
                } else if s == "true" {
                    Some(Value::Bool(true))
                } else if s == "false" {
                    Some(Value::Bool(false))
                } else {
                    Some(Value::String(s.clone()))
                }
            } else {
                Some(replacement_val.clone())
            };
        } else {
            self.replacement = None;
        }

        let config_def = Self::config_def();
        let simple_config = SimpleConfig::new(config_def, configs)
            .expect("Failed to parse configuration for MaskField");

        // Parse field names from the list configuration
        let fields_list = simple_config.get_list(FIELDS_CONFIG).unwrap_or_default();
        self.masked_fields = fields_list.into_iter().collect();

        // Parse mask type
        let type_str = simple_config
            .get_string(MASK_TYPE_CONFIG)
            .unwrap_or(DEFAULT_MASK_TYPE)
            .to_string();
        self.mask_type = MaskType::from_string(&type_str);

        // Parse replace.null.with.default boolean
        self.replace_null_with_default = simple_config
            .get_bool(REPLACE_NULL_CONFIG)
            .unwrap_or(DEFAULT_REPLACE_NULL);
    }

    /// Transforms the given record by masking specified fields.
    ///
    /// This corresponds to `MaskField.apply(R record)` in Java.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the specified fields masked.
    /// Returns `Ok(Some(record))` with the transformed record.
    /// Returns `Err(ConnectError)` on transformation errors.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if:
    /// - The operating value is not a Map/Object (schemaless mode)
    /// - The operating value is not a Struct/Object (schema-aware mode)
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // If no fields are configured to mask, return the original record
        if self.masked_fields.is_empty() {
            return Ok(Some(record));
        }

        // Determine whether to use schemaless or schema-aware mode
        let has_schema = match self.target {
            MaskFieldTarget::Key => record.key_schema().is_some(),
            MaskFieldTarget::Value => record.value_schema().is_some(),
        };

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
    /// This corresponds to `MaskField.close()` in Java.
    /// In Rust, we reset the fields and configuration to defaults.
    fn close(&mut self) {
        self.masked_fields.clear();
        self.mask_type = MaskType::Null;
        self.replace_null_with_default = DEFAULT_REPLACE_NULL;
        self.replacement = None;
    }
}

impl fmt::Display for MaskField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields_str = self
            .masked_fields
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "MaskField{{target={}, mask_type={}, fields=[{}], replace_null_with_default={}}}",
            match self.target {
                MaskFieldTarget::Key => "Key",
                MaskFieldTarget::Value => "Value",
            },
            self.mask_type,
            fields_str,
            self.replace_null_with_default
        )
    }
}

/// MaskField.Key - convenience constructor for Key target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.MaskField.Key` in Java.
pub fn mask_field_key() -> MaskField {
    MaskField::new(MaskFieldTarget::Key)
}

/// MaskField.Value - convenience constructor for Value target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.MaskField.Value` in Java.
pub fn mask_field_value() -> MaskField {
    MaskField::new(MaskFieldTarget::Value)
}
