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

//! ExtractField Transformation - extracts a field as the entire record key or value.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.ExtractField` in Java.
//!
//! ExtractField extracts the value of a specified field from the record's key or value
//! and replaces the entire key or value with that extracted field value.
//!
//! # Configuration
//!
//! * `field` - The name of the field to extract. Supports nested field paths using
//!   dot notation (e.g., `magic.foo`). Required configuration.
//! * `replace.null.with.default` - A boolean flag indicating whether null field values
//!   with default values should be replaced by their defaults. Default: true.
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::extract_field::{ExtractField, ExtractFieldTarget};
//! use connect_api::transforms::Transformation;
//! use connect_api::source::SourceRecord;
//! use connect_api::connector::ConnectRecord;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Create an ExtractField for Value
//! let mut transform = ExtractField::new(ExtractFieldTarget::Value);
//! let mut configs = HashMap::new();
//! configs.insert("field".to_string(), json!("id"));
//! transform.configure(configs);
//!
//! // Create a record with value containing the field
//! let record = SourceRecord::new(
//!     HashMap::new(),
//!     HashMap::new(),
//!     "test-topic".to_string(),
//!     None,
//!     Some(json!("original-key")),
//!     json!({"id": 123, "name": "test"}),
//! );
//!
//! // Transform will extract id field as the entire value
//! let result = transform.transform(record).unwrap().unwrap();
//! // Value will be just the extracted field: 123
//! assert_eq!(result.value(), &json!(123));
//! ```

use crate::transforms::field::{FieldSyntaxVersion, SingleFieldPath};
use crate::transforms::util::{Requirements, SimpleConfig};
use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType, ConfigKeyDef};
use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Configuration key for the field name.
const FIELD_CONFIG: &str = "field";

/// Configuration key for replace null with default.
const REPLACE_NULL_CONFIG: &str = "replace.null.with.default";

/// Default value for replace null with default.
const DEFAULT_REPLACE_NULL: bool = true;

/// Configuration key for field syntax version.
const FIELD_SYNTAX_VERSION_CONFIG: &str = "field.syntax.version";

/// Default value for field syntax version.
const FIELD_SYNTAX_VERSION_DEFAULT_VALUE: &str = "V1";

/// Purpose string for error messages.
const PURPOSE: &str = "field extraction";

/// Overview documentation for ExtractField.
pub const OVERVIEW_DOC: &str =
    "Extract the value of a specified field from the record's key or value and replace \
    the entire key or value with that extracted field value. Supports nested fields using \
    dot notation (e.g., \"magic.foo\") when using field syntax version V2.";

/// ExtractField transformation that extracts a field as the entire record key or value.
///
/// This corresponds to `org.apache.kafka.connect.transforms.ExtractField<R>` in Java.
///
/// The ExtractField transformation extracts the value of a specified field from the
/// record's key or value (Struct or Map) and replaces the entire key or value with
/// that extracted field value. This is useful when you want to extract a single field
/// from a complex structure and use it as the key or value.
///
/// # Type Parameters
///
/// This transformation works on `SourceRecord` types that implement `ConnectRecord`.
///
/// # Configuration
///
/// - `field`: The name of the field to extract (required).
///   Supports nested field paths using dot notation (e.g., "magic.foo").
/// - `replace.null.with.default`: Whether to replace null values with defaults (default: true).
/// - `field.syntax.version`: V1 for backward compatibility (single field only),
///   V2 for nested field support (default: V1).
///
/// # Schema Modes
///
/// The transformation handles both schemaless and schema-aware modes:
///
/// - **Schemaless mode**: The key/value is a Map (JSON Object), and the specified field
///   value is extracted and becomes the new key/value. If the field doesn't exist,
///   the result is null.
///
/// - **Schema-aware mode**: The key/value is a Struct (JSON Object with schema), and the
///   specified field value is extracted and becomes the new key/value. The schema of
///   the extracted field becomes the new schema. If the field doesn't exist in the schema,
///   an error is thrown.
///
/// # Nested Fields (V2)
///
/// When using `field.syntax.version=V2`, nested field paths are supported:
/// - `magic.foo` extracts the value at path magic -> foo
/// - ``magic.foo`` extracts field named "magic.foo" (literal dot)
///
/// # Thread Safety
///
/// In Rust, this transformation is inherently thread-safe as it only uses immutable
/// configuration and thread-safe data structures.
#[derive(Debug)]
pub struct ExtractField {
    /// The target (Key or Value) - determines which part of the record to extract from.
    target: ExtractFieldTarget,
    /// The parsed field path for accessing nested field values.
    field_path: SingleFieldPath,
    /// Whether to replace null values with default values (for schema-aware mode).
    replace_null_with_default: bool,
    /// Field syntax version (V1 or V2).
    field_syntax_version: FieldSyntaxVersion,
}

/// ExtractFieldTarget enum representing Key or Value operation.
///
/// This replaces Java's inner classes `ExtractField$Key` and `ExtractField$Value`.
/// In Kafka Connect configuration, the target is determined by the alias:
/// - "ExtractField$Key" → ExtractFieldTarget::Key
/// - "ExtractField$Value" → ExtractFieldTarget::Value
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractFieldTarget {
    /// Operate on record key (ExtractField.Key in Java).
    Key,
    /// Operate on record value (ExtractField.Value in Java).
    Value,
}

impl ExtractField {
    /// Creates a new ExtractField transformation for the specified target.
    ///
    /// This corresponds to the constructor in Java.
    ///
    /// # Arguments
    ///
    /// * `target` - The target (Key or Value)
    pub fn new(target: ExtractFieldTarget) -> Self {
        // Create default field path (empty)
        let default_field_path = SingleFieldPath::new("", FieldSyntaxVersion::V1)
            .expect("Empty field path should always be valid");

        ExtractField {
            target,
            field_path: default_field_path,
            replace_null_with_default: DEFAULT_REPLACE_NULL,
            field_syntax_version: FieldSyntaxVersion::V1,
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
    /// This corresponds to `ExtractField.CONFIG_DEF` in Java.
    ///
    /// The configuration includes:
    /// - `field`: Field name to extract (required)
    /// - `replace.null.with.default`: Whether to replace null with defaults (default: true)
    /// - `field.syntax.version`: V1 for single fields, V2 for nested (default: V1)
    pub fn config_def() -> HashMap<String, ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                FIELD_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The name of the field to extract. Supports nested field paths using dot notation \
                (e.g., \"magic.foo\") when using field syntax version V2.",
            )
            .define(
                REPLACE_NULL_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(DEFAULT_REPLACE_NULL)),
                ConfigDefImportance::Medium,
                "Whether to replace null field values with their default values from the schema.",
            )
            .define(
                FIELD_SYNTAX_VERSION_CONFIG,
                ConfigDefType::String,
                Some(Value::String(
                    FIELD_SYNTAX_VERSION_DEFAULT_VALUE.to_string(),
                )),
                ConfigDefImportance::Low,
                crate::transforms::field::FIELD_SYNTAX_VERSION_DOC,
            )
            .build()
    }

    /// Returns the target of this transformation.
    pub fn target(&self) -> ExtractFieldTarget {
        self.target
    }

    /// Returns the field path (parsed from field configuration).
    pub fn field_path(&self) -> &SingleFieldPath {
        &self.field_path
    }

    /// Returns whether null values should be replaced with defaults.
    pub fn replace_null_with_default(&self) -> bool {
        self.replace_null_with_default
    }

    /// Returns the field syntax version.
    pub fn field_syntax_version(&self) -> FieldSyntaxVersion {
        self.field_syntax_version
    }

    /// Returns the type name of a value, or "null" if the value is null.
    ///
    /// # Arguments
    /// * `x` - The value to get the type name for
    fn null_safe_type_name(x: &Value) -> String {
        Requirements::null_safe_type_name(x)
    }

    /// Applies the transformation in schemaless mode.
    ///
    /// In schemaless mode, the key/value is expected to be a Map (JSON Object).
    /// The specified field value is extracted and becomes the new key/value.
    /// If the field doesn't exist, the result is null.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the key or value replaced by the extracted field.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if the operating value is not a Map.
    fn apply_schemaless(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            ExtractFieldTarget::Key => record.key(),
            ExtractFieldTarget::Value => Some(record.value()),
        };

        // Validate that operating value is a Map (JSON Object)
        if operating_value.is_none() {
            // If operating value is null (None), return null as the result
            return self.create_record_with_extracted_value(record, Value::Null);
        }

        let value = operating_value.unwrap();
        if value.is_null() {
            // Null value results in null extracted field
            return self.create_record_with_extracted_value(record, Value::Null);
        }

        // Validate that value is a Map/Object
        if !value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Map objects supported in absence of schema for [{}], found: {}",
                PURPOSE,
                Self::null_safe_type_name(value)
            )));
        }

        // Extract the field value using SingleFieldPath
        let value_map = Requirements::require_map(value, PURPOSE)?;
        let extracted_value = self.extract_from_map(&value_map);

        self.create_record_with_extracted_value(record, extracted_value)
    }

    /// Applies the transformation in schema-aware mode.
    ///
    /// In schema-aware mode, the key/value is expected to be a Struct (JSON Object with schema).
    /// The specified field value is extracted and becomes the new key/value.
    /// If the field doesn't exist in the schema, an error is thrown.
    ///
    /// Note: In the current Rust implementation, schemas are represented as strings
    /// (schema names), not full schema objects. This implementation treats schema-aware
    /// mode similarly to schemaless mode, but validates field existence.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the key or value replaced by the extracted field.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if:
    /// - The operating value is not a Struct/Object
    /// - A specified field does not exist in the value
    fn apply_with_schema(
        &self,
        record: SourceRecord,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            ExtractFieldTarget::Key => record.key(),
            ExtractFieldTarget::Value => Some(record.value()),
        };

        // If operating value is null, result is null
        if operating_value.is_none() {
            return self.create_record_with_extracted_value(record, Value::Null);
        }

        let value = operating_value.unwrap();
        if value.is_null() {
            return self.create_record_with_extracted_value(record, Value::Null);
        }

        // Validate that value is an Object/Struct
        if !value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Struct objects supported for [{} with schema], found: {}",
                PURPOSE,
                Self::null_safe_type_name(value)
            )));
        }

        let value_obj = value.as_object().unwrap();

        // In schema-aware mode, validate that all fields in path exist
        // For nested paths, validate each segment exists
        self.validate_field_path_in_schema(value_obj)?;

        // Extract the field value
        let extracted_value = self.extract_from_schema_object(value_obj);

        self.create_record_with_extracted_value(record, extracted_value)
    }

    /// Extracts a field value from a Map (schemaless mode).
    ///
    /// Uses the SingleFieldPath to navigate nested structures.
    /// If the field doesn't exist, returns null.
    ///
    /// # Arguments
    /// * `map` - The HashMap to extract from
    fn extract_from_map(&self, map: &HashMap<String, Value>) -> Value {
        // Use SingleFieldPath to get the value
        let field_names = self.field_path.field_names();

        if field_names.is_empty() {
            return Value::Null;
        }

        // Navigate through nested maps
        let mut current: Option<&Value> = None;
        for (i, field_name) in field_names.iter().enumerate() {
            if i == 0 {
                current = map.get(field_name);
            } else if let Some(Value::Object(obj)) = current {
                current = obj.get(field_name);
            } else {
                // Not an object, cannot traverse further
                return Value::Null;
            }

            if current.is_none() {
                return Value::Null;
            }
        }

        current.cloned().unwrap_or(Value::Null)
    }

    /// Extracts a field value from a JSON Object (schema-aware mode).
    ///
    /// Uses the SingleFieldPath to navigate nested structures.
    ///
    /// # Arguments
    /// * `obj` - The serde_json::Map to extract from
    fn extract_from_schema_object(&self, obj: &serde_json::Map<String, Value>) -> Value {
        let field_names = self.field_path.field_names();

        if field_names.is_empty() {
            return Value::Null;
        }

        // Navigate through nested objects
        let mut current: Option<&Value> = None;
        for (i, field_name) in field_names.iter().enumerate() {
            if i == 0 {
                current = obj.get(field_name);
            } else if let Some(Value::Object(inner_obj)) = current {
                current = inner_obj.get(field_name);
            } else {
                // Not an object, cannot traverse further
                return Value::Null;
            }

            if current.is_none() {
                // In schema mode, this shouldn't happen as we validated earlier
                return Value::Null;
            }
        }

        // Handle null value replacement based on configuration
        let extracted = current.unwrap();
        if extracted.is_null() && self.replace_null_with_default {
            // In Java: value.get(field) returns default if null
            // In our implementation without full schema objects, we keep null
            // (no default value info available)
            Value::Null
        } else {
            extracted.clone()
        }
    }

    /// Validates that all field path segments exist in the schema/object.
    ///
    /// In schema-aware mode, all fields in the path must exist.
    ///
    /// # Arguments
    /// * `obj` - The JSON Object to validate against
    ///
    /// # Errors
    /// Returns `ConnectError::Data` if any field in the path does not exist.
    fn validate_field_path_in_schema(
        &self,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<(), ConnectError> {
        let field_names = self.field_path.field_names();

        if field_names.is_empty() {
            return Err(ConnectError::data("Field path is empty"));
        }

        // Validate each segment in the path
        let mut current: Option<&serde_json::Map<String, Value>> = Some(obj);
        for (i, field_name) in field_names.iter().enumerate() {
            if current.is_none() {
                // Parent was not an object
                return Err(ConnectError::data(format!(
                    "Field {} at path segment {} is not an object",
                    field_names.get(i - 1).unwrap_or(&String::new()),
                    i - 1
                )));
            }

            let current_obj = current.unwrap();
            if !current_obj.contains_key(field_name) {
                return Err(ConnectError::data(format!(
                    "Field {} does not exist in {} schema",
                    field_name,
                    if i == 0 {
                        match self.target {
                            ExtractFieldTarget::Key => "key",
                            ExtractFieldTarget::Value => "value",
                        }
                    } else {
                        "nested"
                    }
                )));
            }

            // If this is not the last segment, check if it's an object for nesting
            if i < field_names.len() - 1 {
                let next_val = current_obj.get(field_name).unwrap();
                if !next_val.is_object() && !next_val.is_null() {
                    return Err(ConnectError::data(format!(
                        "Field {} at path segment {} is not a Struct/Object",
                        field_name, i
                    )));
                }
                // Continue traversal if it's an object
                current = next_val.as_object();
            }
        }

        Ok(())
    }

    /// Creates a new record with the extracted field value.
    ///
    /// # Arguments
    /// * `record` - The original record
    /// * `extracted_value` - The extracted field value to use as the new key or value
    fn create_record_with_extracted_value(
        &self,
        record: SourceRecord,
        extracted_value: Value,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        match self.target {
            ExtractFieldTarget::Key => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                Some(extracted_value),
                record.value().clone(),
            ))),
            ExtractFieldTarget::Value => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                record.key().cloned(),
                extracted_value,
            ))),
        }
    }
}

impl Default for ExtractField {
    /// Creates an ExtractField with default target (Value).
    fn default() -> Self {
        ExtractField::new(ExtractFieldTarget::Value)
    }
}

impl Transformation<SourceRecord> for ExtractField {
    /// Configures this transformation with the given configuration map.
    ///
    /// This corresponds to `ExtractField.configure(Map<String, ?> configs)` in Java.
    ///
    /// # Arguments
    ///
    /// * `configs` - A map of configuration key-value pairs
    ///
    /// # Configuration Keys
    ///
    /// * `field` - The name of the field to extract (required, JSON string)
    /// * `replace.null.with.default` - Whether to replace null with defaults (optional, default: true)
    /// * `field.syntax.version` - V1 for single fields, V2 for nested (optional, default: V1)
    ///
    /// # Errors
    ///
    /// The configuration will fail if `field` is not provided or if the field path
    /// is invalid for V2 syntax.
    fn configure(&mut self, configs: HashMap<String, Value>) {
        let config_def = Self::config_def();
        let simple_config = SimpleConfig::new(config_def, configs)
            .expect("Failed to parse configuration for ExtractField");

        // Parse field syntax version first (needed for parsing field path)
        let version_str = simple_config
            .get_string(FIELD_SYNTAX_VERSION_CONFIG)
            .unwrap_or(FIELD_SYNTAX_VERSION_DEFAULT_VALUE);
        self.field_syntax_version =
            FieldSyntaxVersion::from_string(version_str).unwrap_or(FieldSyntaxVersion::V1);

        // Parse field name and create SingleFieldPath
        let field_name = simple_config
            .get_string(FIELD_CONFIG)
            .unwrap_or("")
            .to_string();

        // Create the field path based on syntax version
        self.field_path = SingleFieldPath::new(&field_name, self.field_syntax_version)
            .expect("Field path should be valid for given syntax version");

        // Parse replace.null.with.default boolean
        self.replace_null_with_default = simple_config
            .get_bool(REPLACE_NULL_CONFIG)
            .unwrap_or(DEFAULT_REPLACE_NULL);
    }

    /// Transforms the given record by extracting a field as the new key or value.
    ///
    /// This corresponds to `ExtractField.apply(R record)` in Java.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the key or value replaced by the extracted field.
    /// Returns `Ok(Some(record))` with the transformed record.
    /// Returns `Err(ConnectError)` on transformation errors.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if:
    /// - The operating value is not a Map/Object (schemaless mode)
    /// - The operating value is not a Struct/Object (schema-aware mode)
    /// - A specified field does not exist in the value (schema-aware mode only)
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Determine whether to use schemaless or schema-aware mode
        // In Java: if (operatingSchema(record) != null)
        // In our implementation: check if operating schema is present
        let has_schema = match self.target {
            ExtractFieldTarget::Key => record.key_schema().is_some(),
            ExtractFieldTarget::Value => record.value_schema().is_some(),
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
    /// This corresponds to `ExtractField.close()` in Java.
    /// In Rust, we reset the fields and configuration to defaults.
    fn close(&mut self) {
        self.field_path = SingleFieldPath::new("", FieldSyntaxVersion::V1)
            .expect("Empty field path should always be valid");
        self.replace_null_with_default = DEFAULT_REPLACE_NULL;
        self.field_syntax_version = FieldSyntaxVersion::V1;
    }
}

impl fmt::Display for ExtractField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExtractField{{target={}, field={}, replace_null_with_default={}, field_syntax_version={}}}",
            match self.target {
                ExtractFieldTarget::Key => "Key",
                ExtractFieldTarget::Value => "Value",
            },
            self.field_path.field_names().join("."),
            self.replace_null_with_default,
            self.field_syntax_version
        )
    }
}

/// ExtractField$Key - convenience constructor for Key target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.ExtractField$Key` in Java.
pub fn extract_field_key() -> ExtractField {
    ExtractField::new(ExtractFieldTarget::Key)
}

/// ExtractField$Value - convenience constructor for Value target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.ExtractField$Value` in Java.
pub fn extract_field_value() -> ExtractField {
    ExtractField::new(ExtractFieldTarget::Value)
}
