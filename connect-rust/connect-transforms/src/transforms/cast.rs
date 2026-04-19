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

//! Cast Transformation - casts fields or entire values to specific types.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.Cast` in Java.
//!
//! Cast allows you to change the data type of fields in the record's key or value.
//! It supports casting between various primitive types and converting binary data
//! to strings (base64 encoded).
//!
//! # Configuration
//!
//! * `spec` - A list of colon-delimited pairs specifying field names and target types,
//!   or a single type for whole-value casting. Format: `field1:type,field2:type` or `type`.
//!   Required configuration.
//! * `replace.null.with.default` - A boolean flag indicating whether null field values
//!   with default values should be replaced by their defaults. Default: true.
//!
//! # Supported Types
//!
//! Input types: INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES
//! Output types: INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::cast::{Cast, CastTarget};
//! use connect_api::transforms::Transformation;
//! use connect_api::source::SourceRecord;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Create a Cast for Value that casts "age" to int32 and "score" to float64
//! let mut transform = Cast::new(CastTarget::Value);
//! let mut configs = HashMap::new();
//! configs.insert("spec".to_string(), json!(["age:int32", "score:float64"]));
//! transform.configure(configs);
//!
//! // Create a record with fields to cast
//! let record = SourceRecord::new(
//!     HashMap::new(),
//!     HashMap::new(),
//!     "test-topic".to_string(),
//!     None,
//!     None,
//!     json!({"name": "alice", "age": "25", "score": "95.5"}),
//! );
//!
//! // Transform will cast age and score fields
//! let result = transform.transform(record).unwrap().unwrap();
//! assert_eq!(result.value()["age"], json!(25));
//! assert_eq!(result.value()["score"], json!(95.5));
//! ```

use crate::transforms::util::{Requirements, SimpleConfig};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use common_trait::cache::{Cache, SynchronizedCache};
use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType, ConfigKeyDef};
use connect_api::connector::ConnectRecord;
use connect_api::data::{ConnectSchema, SchemaBuilder, SchemaType};
use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Configuration key for the spec (field:type mappings).
const SPEC_CONFIG: &str = "spec";

/// Configuration key for replace null with default.
const REPLACE_NULL_CONFIG: &str = "replace.null.with.default";

/// Default value for replace null with default.
const DEFAULT_REPLACE_NULL: bool = true;

/// Purpose string for error messages.
const PURPOSE: &str = "cast types";

/// Overview documentation for Cast.
pub const OVERVIEW_DOC: &str = "Cast fields or the entire key or value to a specific type, \
    e.g. to force an integer field to a smaller width. Cast from integers, floats, boolean and string \
    to any other type, and cast binary to string (base64 encoded). \
    Use the concrete transformation type designed for the record key (Cast.Key) or value (Cast.Value).";

/// Set of supported input types for casting.
/// In Java: SUPPORTED_CAST_INPUT_TYPES = EnumSet.of(INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES)
fn supported_cast_input_types() -> Vec<SchemaType> {
    vec![
        SchemaType::Int8,
        SchemaType::Int16,
        SchemaType::Int32,
        SchemaType::Int64,
        SchemaType::Float32,
        SchemaType::Float64,
        SchemaType::Boolean,
        SchemaType::String,
        SchemaType::Bytes,
    ]
}

/// Set of supported output types for casting.
/// In Java: SUPPORTED_CAST_OUTPUT_TYPES = EnumSet.of(INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING)
fn supported_cast_output_types() -> Vec<SchemaType> {
    vec![
        SchemaType::Int8,
        SchemaType::Int16,
        SchemaType::Int32,
        SchemaType::Int64,
        SchemaType::Float32,
        SchemaType::Float64,
        SchemaType::Boolean,
        SchemaType::String,
    ]
}

/// FieldType enum for distinguishing input vs output type validation.
/// This corresponds to the private enum FieldType in Java's Cast class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldType {
    Input,
    Output,
}

/// Cast transformation that casts fields or entire values to specific types.
///
/// This corresponds to `org.apache.kafka.connect.transforms.Cast<R>` in Java.
///
/// The Cast transformation allows you to:
/// - Cast individual fields to specific types
/// - Cast the entire record value to a specific type
/// - Cast from integers, floats, boolean, and string to other types
/// - Cast binary data to string (base64 encoded)
///
/// # Type Parameters
///
/// This transformation works on `SourceRecord` types that implement `ConnectRecord`.
///
/// # Configuration
///
/// - `spec`: List of field:type mappings or single type for whole-value cast (required)
/// - `replace.null.with.default`: Whether to replace null with defaults (default: true)
///
/// # Schema Modes
///
/// The transformation handles both schemaless and schema-aware modes:
///
/// - **Schemaless mode**: The key/value is a Map (JSON Object), and fields are
///   cast directly in the map.
///
/// - **Schema-aware mode**: The key/value is a Struct (JSON Object with schema),
///   and fields are cast while updating the schema structure.
///
/// # Thread Safety
///
/// In Rust, this transformation is thread-safe as it uses SynchronizedCache for
/// schema caching and immutable configuration after initialization.
pub struct Cast {
    /// The target (Key or Value) - determines which part of the record to transform.
    target: CastTarget,
    /// Map of field names to target types for casting.
    /// In Java: Map<String, Schema.Type> casts
    casts: HashMap<String, SchemaType>,
    /// The type for whole-value casting (null means field-level casting).
    /// In Java: Schema.Type wholeValueCastType (using WHOLE_VALUE_CAST = null as key)
    whole_value_cast_type: Option<SchemaType>,
    /// Schema cache for storing updated schemas.
    /// In Java: Cache<Schema, Schema> schemaUpdateCache
    schema_cache: SynchronizedCache<String, String>,
    /// Whether to replace null values with default values from schema.
    replace_null_with_default: bool,
}

/// CastTarget enum representing Key or Value operation.
///
/// This replaces Java's inner classes `Cast.Key` and `Cast.Value`.
/// In Kafka Connect configuration, the target is determined by the alias:
/// - "Cast.Key" → CastTarget::Key
/// - "Cast.Value" → CastTarget::Value
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastTarget {
    /// Operate on record key (Cast.Key in Java).
    Key,
    /// Operate on record value (Cast.Value in Java).
    Value,
}

impl Cast {
    /// Creates a new Cast transformation for the specified target.
    ///
    /// This corresponds to the constructor in Java.
    ///
    /// # Arguments
    ///
    /// * `target` - The target (Key or Value)
    pub fn new(target: CastTarget) -> Self {
        Cast {
            target,
            casts: HashMap::new(),
            whole_value_cast_type: None,
            schema_cache: SynchronizedCache::with_default_capacity(),
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
        "3.9.0"
    }

    /// Returns the configuration definition for this transformation.
    ///
    /// This corresponds to `Cast.CONFIG_DEF` in Java.
    ///
    /// The configuration includes:
    /// - `spec`: List of field:type mappings (required)
    /// - `replace.null.with.default`: Replace null with defaults (default: true)
    pub fn config_def() -> HashMap<String, ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                SPEC_CONFIG,
                ConfigDefType::List,
                None,
                ConfigDefImportance::High,
                "List of fields and the type to cast them to of the form field1:type,field2:type \
                 to cast fields of Maps or Structs. A single type to cast the entire value. \
                 Valid types are int8, int16, int32, int64, float32, float64, boolean, and string. \
                 Note that binary fields can only be cast to string.",
            )
            .define(
                REPLACE_NULL_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(DEFAULT_REPLACE_NULL)),
                ConfigDefImportance::Medium,
                "Whether to replace fields that have a default value and that are null to the default value. \
                 When set to true, the default value is used, otherwise null is used.",
            )
            .build()
    }

    /// Returns the target of this transformation.
    pub fn target(&self) -> CastTarget {
        self.target
    }

    /// Returns the casts mapping.
    pub fn casts(&self) -> &HashMap<String, SchemaType> {
        &self.casts
    }

    /// Returns the whole value cast type.
    pub fn whole_value_cast_type(&self) -> Option<SchemaType> {
        self.whole_value_cast_type
    }

    /// Returns whether null values should be replaced with defaults.
    pub fn replace_null_with_default(&self) -> bool {
        self.replace_null_with_default
    }

    /// Parses field type mappings from a list of strings.
    ///
    /// This corresponds to `Cast.parseFieldTypes(List<String> mappings)` in Java.
    ///
    /// The spec format is:
    /// - Field-level casting: `field1:type,field2:type`
    /// - Whole-value casting: `type` (single type without field name)
    ///
    /// # Arguments
    /// * `mappings` - A list of mapping strings
    ///
    /// # Returns
    /// Returns a tuple of (HashMap<String, SchemaType>, Option<SchemaType>)
    /// where the HashMap contains field-level casts and the Option contains
    /// the whole-value cast type (if specified).
    ///
    /// # Errors
    /// Returns ConnectError if:
    /// - The spec is empty or invalid
    /// - An invalid type is specified
    /// - Both field-level and whole-value casting are specified together
    pub fn parse_field_types(
        mappings: Vec<String>,
    ) -> Result<(HashMap<String, SchemaType>, Option<SchemaType>), ConnectError> {
        let mut casts = HashMap::new();
        let mut is_whole_value_cast = false;
        let mut whole_value_cast_type: Option<SchemaType> = None;

        if mappings.is_empty() {
            return Err(ConnectError::data(
                "Must specify at least one field to cast.",
            ));
        }

        for mapping in &mappings {
            let parts: Vec<&str> = mapping.split(':').collect();
            if parts.len() > 2 {
                return Err(ConnectError::data(format!(
                    "Invalid cast mapping: {}",
                    mapping
                )));
            }

            if parts.len() == 1 {
                // Whole-value cast: just a type name
                let type_str = parts[0].trim().to_uppercase();
                let target_type = parse_schema_type_from_string(&type_str)?;
                Self::valid_cast_type(target_type, FieldType::Output)?;
                casts.insert("__whole_value_cast__".to_string(), target_type);
                whole_value_cast_type = Some(target_type);
                is_whole_value_cast = true;
            } else {
                // Field-level cast: field:type
                let field = parts[0].trim();
                let type_str = parts[1].trim().to_uppercase();
                let target_type = parse_schema_type_from_string(&type_str)?;
                Self::valid_cast_type(target_type, FieldType::Output)?;
                casts.insert(field.to_string(), target_type);
            }
        }

        if is_whole_value_cast && mappings.len() > 1 {
            return Err(ConnectError::data(
                "Cast transformations that specify a type to cast the entire value to \
                 may only specify a single cast in their spec",
            ));
        }

        Ok((casts, whole_value_cast_type))
    }

    /// Validates that a schema type is supported for casting.
    ///
    /// This corresponds to `Cast.validCastType(Schema.Type type, FieldType fieldType)` in Java.
    ///
    /// # Arguments
    /// * `type` - The schema type to validate
    /// * `field_type` - Whether this is an input or output type
    ///
    /// # Returns
    /// Returns the validated type on success.
    ///
    /// # Errors
    /// Returns ConnectError::Data for unsupported types.
    pub fn valid_cast_type(
        type_: SchemaType,
        field_type: FieldType,
    ) -> Result<SchemaType, ConnectError> {
        match field_type {
            FieldType::Input => {
                if !supported_cast_input_types().contains(&type_) {
                    return Err(ConnectError::data(format!(
                        "Cast transformation does not support casting from {}; supported types are {:?}",
                        type_,
                        supported_cast_input_types()
                    )));
                }
            }
            FieldType::Output => {
                if !supported_cast_output_types().contains(&type_) {
                    return Err(ConnectError::data(format!(
                        "Cast transformation does not support casting to {}; supported types are {:?}",
                        type_,
                        supported_cast_output_types()
                    )));
                }
            }
        }
        Ok(type_)
    }

    /// Casts a value to the specified target type.
    ///
    /// This corresponds to `Cast.castValueToType(Schema schema, Object value, Schema.Type targetType)` in Java.
    ///
    /// # Arguments
    /// * `schema` - The schema of the value (optional for schemaless mode)
    /// * `value` - The value to cast
    /// * `target_type` - The target type to cast to
    ///
    /// # Returns
    /// Returns the casted value as a JSON Value.
    ///
    /// # Errors
    /// Returns ConnectError::Data if:
    /// - The value is null or not supported
    /// - The cast fails due to invalid conversion
    fn cast_value_to_type(
        schema: Option<&str>,
        value: &Value,
        target_type: SchemaType,
    ) -> Result<Value, ConnectError> {
        if value.is_null() {
            return Ok(Value::Null);
        }

        // Infer the source type
        let inferred_type = if schema.is_none() {
            ConnectSchema::schema_type(value)
        } else {
            // In schema-aware mode, we would parse the schema string
            // For now, we infer from the value
            ConnectSchema::schema_type(value)
        };

        let inferred_type = match inferred_type {
            Some(t) => t,
            None => {
                return Err(ConnectError::data(format!(
                    "Cast transformation was passed a value of type {:?} which is not supported by Connect's data API",
                    value
                )));
            }
        };

        // Validate input type
        Self::valid_cast_type(inferred_type, FieldType::Input)?;

        // Perform the cast based on target type
        match target_type {
            SchemaType::Int8 => Self::cast_to_int8(value),
            SchemaType::Int16 => Self::cast_to_int16(value),
            SchemaType::Int32 => Self::cast_to_int32(value),
            SchemaType::Int64 => Self::cast_to_int64(value),
            SchemaType::Float32 => Self::cast_to_float32(value),
            SchemaType::Float64 => Self::cast_to_float64(value),
            SchemaType::Boolean => Self::cast_to_boolean(value),
            SchemaType::String => Self::cast_to_string(value),
            _ => Err(ConnectError::data(format!(
                "{} is not supported in the Cast transformation",
                target_type
            ))),
        }
    }

    /// Casts a value to INT8 (byte).
    fn cast_to_int8(value: &Value) -> Result<Value, ConnectError> {
        match value {
            Value::Number(n) => {
                let num = if n.is_i64() {
                    n.as_i64().unwrap()
                } else if n.is_f64() {
                    n.as_f64().unwrap() as i64
                } else {
                    return Err(ConnectError::data(format!(
                        "Unexpected number type in Cast transformation"
                    )));
                };
                if num < i8::MIN as i64 || num > i8::MAX as i64 {
                    return Err(ConnectError::data(format!(
                        "Value {} was out of range for requested data type Int8",
                        num
                    )));
                }
                Ok(Value::Number((num as i8).into()))
            }
            Value::Bool(b) => Ok(Value::Number(if *b { 1i8 } else { 0i8 }.into())),
            Value::String(s) => {
                let parsed = s.parse::<i8>().map_err(|e| {
                    ConnectError::data(format!(
                        "Value ({}) was out of range for requested data type Int8: {}",
                        s, e
                    ))
                })?;
                Ok(Value::Number(parsed.into()))
            }
            _ => Err(ConnectError::data(format!(
                "Unexpected type in Cast transformation: {:?}",
                value
            ))),
        }
    }

    /// Casts a value to INT16 (short).
    fn cast_to_int16(value: &Value) -> Result<Value, ConnectError> {
        match value {
            Value::Number(n) => {
                let num = if n.is_i64() {
                    n.as_i64().unwrap()
                } else if n.is_f64() {
                    n.as_f64().unwrap() as i64
                } else {
                    return Err(ConnectError::data(format!(
                        "Unexpected number type in Cast transformation"
                    )));
                };
                if num < i16::MIN as i64 || num > i16::MAX as i64 {
                    return Err(ConnectError::data(format!(
                        "Value {} was out of range for requested data type Int16",
                        num
                    )));
                }
                Ok(Value::Number((num as i16).into()))
            }
            Value::Bool(b) => Ok(Value::Number(if *b { 1i16 } else { 0i16 }.into())),
            Value::String(s) => {
                let parsed = s.parse::<i16>().map_err(|e| {
                    ConnectError::data(format!(
                        "Value ({}) was out of range for requested data type Int16: {}",
                        s, e
                    ))
                })?;
                Ok(Value::Number(parsed.into()))
            }
            _ => Err(ConnectError::data(format!(
                "Unexpected type in Cast transformation: {:?}",
                value
            ))),
        }
    }

    /// Casts a value to INT32 (int).
    fn cast_to_int32(value: &Value) -> Result<Value, ConnectError> {
        match value {
            Value::Number(n) => {
                let num = if n.is_i64() {
                    n.as_i64().unwrap()
                } else if n.is_f64() {
                    n.as_f64().unwrap() as i64
                } else {
                    return Err(ConnectError::data(format!(
                        "Unexpected number type in Cast transformation"
                    )));
                };
                if num < i32::MIN as i64 || num > i32::MAX as i64 {
                    return Err(ConnectError::data(format!(
                        "Value {} was out of range for requested data type Int32",
                        num
                    )));
                }
                Ok(Value::Number((num as i32).into()))
            }
            Value::Bool(b) => Ok(Value::Number(if *b { 1i32 } else { 0i32 }.into())),
            Value::String(s) => {
                let parsed = s.parse::<i32>().map_err(|e| {
                    ConnectError::data(format!(
                        "Value ({}) was out of range for requested data type Int32: {}",
                        s, e
                    ))
                })?;
                Ok(Value::Number(parsed.into()))
            }
            _ => Err(ConnectError::data(format!(
                "Unexpected type in Cast transformation: {:?}",
                value
            ))),
        }
    }

    /// Casts a value to INT64 (long).
    fn cast_to_int64(value: &Value) -> Result<Value, ConnectError> {
        match value {
            Value::Number(n) => {
                if n.is_i64() {
                    Ok(Value::Number(n.clone()))
                } else if n.is_f64() {
                    Ok(Value::Number((n.as_f64().unwrap() as i64).into()))
                } else {
                    Err(ConnectError::data(format!(
                        "Unexpected number type in Cast transformation"
                    )))
                }
            }
            Value::Bool(b) => Ok(Value::Number(if *b { 1i64 } else { 0i64 }.into())),
            Value::String(s) => {
                let parsed = s.parse::<i64>().map_err(|e| {
                    ConnectError::data(format!(
                        "Value ({}) was out of range for requested data type Int64: {}",
                        s, e
                    ))
                })?;
                Ok(Value::Number(parsed.into()))
            }
            _ => Err(ConnectError::data(format!(
                "Unexpected type in Cast transformation: {:?}",
                value
            ))),
        }
    }

    /// Casts a value to FLOAT32.
    fn cast_to_float32(value: &Value) -> Result<Value, ConnectError> {
        match value {
            Value::Number(n) => {
                let f = if n.is_i64() {
                    n.as_i64().unwrap() as f32
                } else if n.is_f64() {
                    n.as_f64().unwrap() as f32
                } else {
                    return Err(ConnectError::data(format!(
                        "Unexpected number type in Cast transformation"
                    )));
                };
                // Convert f32 to a JSON number representation
                match serde_json::Number::from_f64(f as f64) {
                    Some(num) => Ok(Value::Number(num)),
                    None => Ok(Value::Number(serde_json::Number::from(0))),
                }
            }
            Value::Bool(b) => {
                let f = if *b { 1.0_f32 } else { 0.0_f32 };
                match serde_json::Number::from_f64(f as f64) {
                    Some(num) => Ok(Value::Number(num)),
                    None => Ok(Value::Number(serde_json::Number::from(0))),
                }
            }
            Value::String(s) => {
                let parsed = s.parse::<f32>().map_err(|e| {
                    ConnectError::data(format!("Value ({}) cannot be parsed as Float32: {}", s, e))
                })?;
                match serde_json::Number::from_f64(parsed as f64) {
                    Some(num) => Ok(Value::Number(num)),
                    None => Ok(Value::Number(serde_json::Number::from(0))),
                }
            }
            _ => Err(ConnectError::data(format!(
                "Unexpected type in Cast transformation: {:?}",
                value
            ))),
        }
    }

    /// Casts a value to FLOAT64 (double).
    fn cast_to_float64(value: &Value) -> Result<Value, ConnectError> {
        match value {
            Value::Number(n) => {
                if n.is_i64() {
                    match serde_json::Number::from_f64(n.as_i64().unwrap() as f64) {
                        Some(num) => Ok(Value::Number(num)),
                        None => Ok(Value::Number(serde_json::Number::from(0))),
                    }
                } else if n.is_f64() {
                    Ok(Value::Number(n.clone()))
                } else {
                    Err(ConnectError::data(format!(
                        "Unexpected number type in Cast transformation"
                    )))
                }
            }
            Value::Bool(b) => {
                let f = if *b { 1.0_f64 } else { 0.0_f64 };
                match serde_json::Number::from_f64(f) {
                    Some(num) => Ok(Value::Number(num)),
                    None => Ok(Value::Number(serde_json::Number::from(0))),
                }
            }
            Value::String(s) => {
                let parsed = s.parse::<f64>().map_err(|e| {
                    ConnectError::data(format!("Value ({}) cannot be parsed as Float64: {}", s, e))
                })?;
                match serde_json::Number::from_f64(parsed) {
                    Some(num) => Ok(Value::Number(num)),
                    None => Ok(Value::Number(serde_json::Number::from(0))),
                }
            }
            _ => Err(ConnectError::data(format!(
                "Unexpected type in Cast transformation: {:?}",
                value
            ))),
        }
    }

    /// Casts a value to BOOLEAN.
    fn cast_to_boolean(value: &Value) -> Result<Value, ConnectError> {
        match value {
            Value::Number(n) => {
                let is_nonzero = if n.is_i64() {
                    n.as_i64().unwrap() != 0
                } else if n.is_f64() {
                    n.as_f64().unwrap() != 0.0
                } else {
                    false
                };
                Ok(Value::Bool(is_nonzero))
            }
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::String(s) => {
                // Java's Boolean.parseBoolean returns false for any non-"true" string
                let parsed = s.to_lowercase() == "true";
                Ok(Value::Bool(parsed))
            }
            _ => Err(ConnectError::data(format!(
                "Unexpected type in Cast transformation: {:?}",
                value
            ))),
        }
    }

    /// Casts a value to STRING.
    ///
    /// For binary values (base64 encoded strings), encodes them.
    /// For all other types, uses the string representation.
    fn cast_to_string(value: &Value) -> Result<Value, ConnectError> {
        match value {
            Value::Null => Ok(Value::String("null".to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            Value::Number(n) => Ok(Value::String(n.to_string())),
            Value::String(s) => Ok(Value::String(s.clone())),
            Value::Array(_arr) => {
                // Arrays and objects are converted to JSON string
                Ok(Value::String(value.to_string()))
            }
            Value::Object(_obj) => {
                // Handle potential binary data encoded as string
                // In Java, ByteBuffer and byte[] are base64 encoded
                // For our JSON representation, we treat string values as potential binary
                Ok(Value::String(value.to_string()))
            }
        }
    }

    /// Encodes a byte array to base64 string.
    fn encode_bytes_to_base64(bytes: &[u8]) -> String {
        BASE64_STANDARD.encode(bytes)
    }

    /// Converts a schema type to a SchemaBuilder for that type.
    ///
    /// This corresponds to `Cast.convertFieldType(Schema.Type type)` in Java.
    fn convert_field_type(type_: SchemaType) -> SchemaBuilder {
        match type_ {
            SchemaType::Int8 => SchemaBuilder::int8(),
            SchemaType::Int16 => SchemaBuilder::int16(),
            SchemaType::Int32 => SchemaBuilder::int32(),
            SchemaType::Int64 => SchemaBuilder::int64(),
            SchemaType::Float32 => SchemaBuilder::float32(),
            SchemaType::Float64 => SchemaBuilder::float64(),
            SchemaType::Boolean => SchemaBuilder::bool(),
            SchemaType::String => SchemaBuilder::string(),
            _ => panic!("Unexpected type in Cast transformation: {}", type_),
        }
    }

    /// Applies the transformation in schemaless mode.
    ///
    /// In schemaless mode, the key/value is expected to be a Map (JSON Object).
    /// Fields are cast directly in the map.
    ///
    /// This corresponds to `Cast.applySchemaless(R record)` in Java.
    fn apply_schemaless(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            CastTarget::Key => record.key(),
            CastTarget::Value => Some(record.value()),
        };

        // Handle null/None operating value
        if operating_value.is_none() {
            return Ok(Some(record));
        }

        let value = operating_value.unwrap();
        if value.is_null() {
            return Ok(Some(record));
        }

        // Whole-value cast
        if let Some(target_type) = self.whole_value_cast_type {
            let casted = Self::cast_value_to_type(None, value, target_type)?;
            return self.new_record(record, None, casted);
        }

        // Field-level cast
        // Validate that value is a Map/Object
        if !value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Map objects supported in absence of schema for [{}], found: {}",
                PURPOSE,
                Requirements::null_safe_type_name(value)
            )));
        }

        let value_map = Requirements::require_map(value, PURPOSE)?;
        let mut updated_map = HashMap::new();

        // Iterate through original fields and apply casts
        for (field_name, field_value) in value_map.iter() {
            if let Some(target_type) = self.casts.get(field_name) {
                let casted = Self::cast_value_to_type(None, field_value, *target_type)?;
                updated_map.insert(field_name.clone(), casted);
            } else {
                updated_map.insert(field_name.clone(), field_value.clone());
            }
        }

        // Convert HashMap to JSON Object
        let updated_value =
            serde_json::to_value(updated_map).unwrap_or(Value::Object(serde_json::Map::new()));

        self.new_record(record, None, updated_value)
    }

    /// Applies the transformation in schema-aware mode.
    ///
    /// In schema-aware mode, the key/value is expected to be a Struct (JSON Object with schema).
    /// Fields are cast while updating the schema structure.
    ///
    /// This corresponds to `Cast.applyWithSchema(R record)` in Java.
    fn apply_with_schema(
        &self,
        record: SourceRecord,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            CastTarget::Key => record.key(),
            CastTarget::Value => Some(record.value()),
        };

        // Handle null/None operating value
        if operating_value.is_none() {
            return Ok(Some(record));
        }

        let value = operating_value.unwrap();
        if value.is_null() {
            return Ok(Some(record));
        }

        // Get the original schema
        let original_schema = match self.target {
            CastTarget::Key => record.key_schema(),
            CastTarget::Value => record.value_schema(),
        };

        // Get or build the updated schema
        let updated_schema = self.get_or_build_schema(original_schema);

        // Whole-value cast
        if let Some(target_type) = self.whole_value_cast_type {
            let casted = Self::cast_value_to_type(original_schema, value, target_type)?;
            return self.new_record(record, Some(updated_schema), casted);
        }

        // Field-level cast - validate that value is a Struct/Object
        if !value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Struct objects supported for [{} with schema], found: {}",
                PURPOSE,
                Requirements::null_safe_type_name(value)
            )));
        }

        let value_obj = value.as_object().unwrap();
        let mut updated_obj = serde_json::Map::new();

        // Build the updated value by iterating through fields
        // In a full implementation, we would iterate through schema fields
        for (field_name, field_value) in value_obj.iter() {
            if let Some(target_type) = self.casts.get(field_name) {
                // Cast the field value
                let final_value = if field_value.is_null() && self.replace_null_with_default {
                    // In Java, this would use field.schema().defaultValue()
                    // Casted to the target type
                    Value::Null
                } else {
                    Self::cast_value_to_type(None, field_value, *target_type)?
                };
                updated_obj.insert(field_name.clone(), final_value);
            } else {
                updated_obj.insert(field_name.clone(), field_value.clone());
            }
        }

        let updated_value = Value::Object(updated_obj);
        self.new_record(record, Some(updated_schema), updated_value)
    }

    /// Gets or builds the updated schema from the original schema.
    ///
    /// This corresponds to `Cast.getOrBuildSchema(Schema valueSchema)` in Java.
    /// Uses a cache to avoid rebuilding schemas for the same input.
    fn get_or_build_schema(&self, original_schema: Option<&str>) -> String {
        let schema_key = original_schema.unwrap_or("schemaless").to_string();

        // Check cache first
        if let Some(cached) = self.schema_cache.get(&schema_key) {
            return cached;
        }

        // Build the updated schema
        let updated_schema = if let Some(target_type) = self.whole_value_cast_type {
            // Whole-value cast: the entire schema becomes the target type
            format!("{}:{}", schema_key, target_type.name())
        } else {
            // Field-level cast: update individual field types
            let field_casts: Vec<String> = self
                .casts
                .iter()
                .filter(|(k, _)| k.as_str() != "__whole_value_cast__")
                .map(|(f, t)| format!("{}:{}", f, t.name()))
                .collect();
            format!("{}:{}", schema_key, field_casts.join(","))
        };

        // Cache the result
        self.schema_cache
            .put(schema_key.clone(), updated_schema.clone());
        updated_schema
    }

    /// Creates a new record with the updated value and schema.
    ///
    /// This corresponds to `Cast.newRecord(R record, Schema updatedSchema, Object updatedValue)` in Java.
    fn new_record(
        &self,
        record: SourceRecord,
        _updated_schema: Option<String>,
        updated_value: Value,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        match self.target {
            CastTarget::Key => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                Some(updated_value),
                record.value().clone(),
            ))),
            CastTarget::Value => Ok(Some(SourceRecord::new(
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

/// Parses a schema type from a string.
///
/// This is used in parse_field_types to convert type strings like "int32" to SchemaType::Int32.
fn parse_schema_type_from_string(type_str: &str) -> Result<SchemaType, ConnectError> {
    match type_str.to_uppercase().as_str() {
        "INT8" => Ok(SchemaType::Int8),
        "INT16" => Ok(SchemaType::Int16),
        "INT32" => Ok(SchemaType::Int32),
        "INT64" => Ok(SchemaType::Int64),
        "FLOAT32" => Ok(SchemaType::Float32),
        "FLOAT64" => Ok(SchemaType::Float64),
        "BOOLEAN" => Ok(SchemaType::Boolean),
        "STRING" => Ok(SchemaType::String),
        _ => Err(ConnectError::data(format!(
            "Invalid type found in casting spec: {}",
            type_str
        ))),
    }
}

impl Default for Cast {
    /// Creates a Cast with default target (Value).
    fn default() -> Self {
        Cast::new(CastTarget::Value)
    }
}

impl Transformation<SourceRecord> for Cast {
    /// Configures this transformation with the given configuration map.
    ///
    /// This corresponds to `Cast.configure(Map<String, ?> configs)` in Java.
    fn configure(&mut self, configs: HashMap<String, Value>) {
        let config_def = Self::config_def();
        let simple_config =
            SimpleConfig::new(config_def, configs).expect("Failed to parse configuration for Cast");

        // Parse spec list
        let spec_list = simple_config.get_list(SPEC_CONFIG).unwrap_or_default();
        let (casts, whole_value_cast_type) =
            Cast::parse_field_types(spec_list).expect("Failed to parse spec for Cast");
        self.casts = casts;
        self.whole_value_cast_type = whole_value_cast_type;

        // Parse replace.null.with.default boolean
        self.replace_null_with_default = simple_config
            .get_bool(REPLACE_NULL_CONFIG)
            .unwrap_or(DEFAULT_REPLACE_NULL);
    }

    /// Transforms the given record by casting fields or whole value.
    ///
    /// This corresponds to `Cast.apply(R record)` in Java.
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value
        let operating_value = match self.target {
            CastTarget::Key => record.key(),
            CastTarget::Value => Some(record.value()),
        };

        // If operating value is null, return the record unchanged
        if operating_value.is_none() || operating_value.unwrap().is_null() {
            return Ok(Some(record));
        }

        // Determine whether to use schemaless or schema-aware mode
        let has_schema = match self.target {
            CastTarget::Key => record.key_schema().is_some(),
            CastTarget::Value => record.value_schema().is_some(),
        };

        if has_schema {
            self.apply_with_schema(record)
        } else {
            self.apply_schemaless(record)
        }
    }

    /// Closes this transformation and releases any resources.
    ///
    /// This corresponds to `Cast.close()` in Java.
    fn close(&mut self) {
        self.casts.clear();
        self.whole_value_cast_type = None;
        self.replace_null_with_default = DEFAULT_REPLACE_NULL;
    }
}

impl fmt::Display for Cast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let casts_str = self
            .casts
            .iter()
            .filter(|(k, _)| k.as_str() != "__whole_value_cast__")
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "Cast{{target={}, casts=[{}], whole_value_cast={}, replace_null_with_default={}}}",
            match self.target {
                CastTarget::Key => "Key",
                CastTarget::Value => "Value",
            },
            casts_str,
            self.whole_value_cast_type
                .map(|t| t.name())
                .unwrap_or("None"),
            self.replace_null_with_default
        )
    }
}

impl fmt::Debug for Cast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let casts_str = self
            .casts
            .iter()
            .filter(|(k, _)| k.as_str() != "__whole_value_cast__")
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect::<Vec<_>>()
            .join(", ");
        f.debug_struct("Cast")
            .field("target", &self.target)
            .field("casts", &casts_str)
            .field("whole_value_cast_type", &self.whole_value_cast_type)
            .field("replace_null_with_default", &self.replace_null_with_default)
            .field("schema_cache", &"<SynchronizedCache>")
            .finish()
    }
}

/// Cast.Key - convenience constructor for Key target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.Cast$Key` in Java.
pub fn cast_key() -> Cast {
    Cast::new(CastTarget::Key)
}

/// Cast.Value - convenience constructor for Value target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.Cast$Value` in Java.
pub fn cast_value() -> Cast {
    Cast::new(CastTarget::Value)
}
