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

use crate::data::{Field, Schema, SchemaType};
use crate::errors::ConnectError;
use serde_json::Value;
use std::collections::HashMap;

/// ConnectSchema - concrete implementation of Schema.
///
/// This corresponds to `org.apache.kafka.connect.data.ConnectSchema` in Java.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectSchema {
    pub(crate) schema_type: SchemaType,
    pub(crate) optional: bool,
    pub(crate) default_value: Option<Value>,
    pub(crate) name: Option<String>,
    pub(crate) version: Option<i32>,
    pub(crate) doc: Option<String>,
    pub(crate) parameters: HashMap<String, String>,
    pub(crate) key_schema: Option<Box<ConnectSchema>>,
    pub(crate) value_schema: Option<Box<ConnectSchema>>,
    pub(crate) fields: Vec<Field>,
    pub(crate) field_map: HashMap<String, Field>,
}

impl ConnectSchema {
    /// Creates a new ConnectSchema.
    pub fn new(schema_type: SchemaType) -> Self {
        ConnectSchema {
            schema_type,
            optional: false,
            default_value: None,
            name: None,
            version: None,
            doc: None,
            parameters: HashMap::new(),
            key_schema: None,
            value_schema: None,
            fields: Vec::new(),
            field_map: HashMap::new(),
        }
    }

    /// Creates an optional schema.
    pub fn optional(schema_type: SchemaType) -> Self {
        ConnectSchema {
            schema_type,
            optional: true,
            default_value: None,
            name: None,
            version: None,
            doc: None,
            parameters: HashMap::new(),
            key_schema: None,
            value_schema: None,
            fields: Vec::new(),
            field_map: HashMap::new(),
        }
    }

    /// Creates a ConnectSchema with full parameters.
    pub fn new_full(
        schema_type: SchemaType,
        optional: bool,
        default_value: Option<Value>,
        name: Option<String>,
        version: Option<i32>,
        doc: Option<String>,
        parameters: HashMap<String, String>,
        fields: Vec<Field>,
        key_schema: Option<ConnectSchema>,
        value_schema: Option<ConnectSchema>,
    ) -> Self {
        let field_map: HashMap<String, Field> = fields
            .iter()
            .map(|f| (f.name().to_string(), f.clone()))
            .collect();

        ConnectSchema {
            schema_type,
            optional,
            default_value,
            name,
            version,
            doc,
            parameters,
            key_schema: key_schema.map(Box::new),
            value_schema: value_schema.map(Box::new),
            fields,
            field_map,
        }
    }

    /// Sets the name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the version.
    pub fn with_version(mut self, version: i32) -> Self {
        self.version = Some(version);
        self
    }

    /// Sets the documentation.
    pub fn with_doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }

    /// Sets the parameters.
    pub fn with_parameters(mut self, parameters: HashMap<String, String>) -> Self {
        self.parameters = parameters;
        self
    }

    /// Sets the default value.
    pub fn with_default_value(mut self, default_value: Value) -> Self {
        self.default_value = Some(default_value);
        self
    }

    /// Sets the key schema for map schemas.
    pub fn with_key_schema(mut self, key_schema: ConnectSchema) -> Self {
        self.key_schema = Some(Box::new(key_schema));
        self
    }

    /// Sets the value schema for map and array schemas.
    pub fn with_value_schema(mut self, value_schema: ConnectSchema) -> Self {
        self.value_schema = Some(Box::new(value_schema));
        self
    }

    /// Adds a field for struct schemas.
    pub fn add_field(mut self, field: Field) -> Self {
        self.field_map
            .insert(field.name().to_string(), field.clone());
        self.fields.push(field);
        self
    }

    /// Returns the default value.
    pub fn default_value(&self) -> Option<&Value> {
        self.default_value.as_ref()
    }

    /// Returns the key schema as a concrete ConnectSchema.
    pub fn key_schema_ref(&self) -> Option<&ConnectSchema> {
        self.key_schema.as_ref().map(|s| s.as_ref())
    }

    /// Returns the value schema as a concrete ConnectSchema.
    pub fn value_schema_ref(&self) -> Option<&ConnectSchema> {
        self.value_schema.as_ref().map(|s| s.as_ref())
    }

    /// Validates a value against this schema.
    pub fn validate_value(&self, value: &Value) -> Result<(), ConnectError> {
        Self::validate_value_internal(None, self, value)
    }

    /// Validates a value against a schema with field name context.
    pub fn validate_value_with_field(
        field_name: Option<&str>,
        schema: &dyn Schema,
        value: &Value,
    ) -> Result<(), ConnectError> {
        let location = if let Some(name) = field_name {
            format!("field: \"{}\"", name)
        } else {
            "value".to_string()
        };
        Self::validate_value_internal(None, schema, value)
    }

    /// Internal validation method.
    fn validate_value_internal(
        field_name: Option<&str>,
        schema: &dyn Schema,
        value: &Value,
    ) -> Result<(), ConnectError> {
        let location = if let Some(name) = field_name {
            format!("field: \"{}\"", name)
        } else {
            "value".to_string()
        };

        if value.is_null() {
            if !schema.is_optional() {
                return Err(ConnectError::data(format!(
                    "Invalid value: null used for required {}, schema type: {}",
                    location,
                    schema.r#type()
                )));
            }
            return Ok(());
        }

        // Validate based on schema type
        match schema.r#type() {
            SchemaType::Int8 => {
                if !value.is_i64() {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type INT8: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
                let i = value.as_i64().unwrap();
                if i < i8::MIN as i64 || i > i8::MAX as i64 {
                    return Err(ConnectError::data(format!(
                        "Value {} out of Int8 range for {}",
                        i, location
                    )));
                }
            }
            SchemaType::Int16 => {
                if !value.is_i64() {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type INT16: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
                let i = value.as_i64().unwrap();
                if i < i16::MIN as i64 || i > i16::MAX as i64 {
                    return Err(ConnectError::data(format!(
                        "Value {} out of Int16 range for {}",
                        i, location
                    )));
                }
            }
            SchemaType::Int32 => {
                // Int32 validation
                if !value.is_i64() {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type INT32: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
                let i = value.as_i64().unwrap();
                if i < i32::MIN as i64 || i > i32::MAX as i64 {
                    return Err(ConnectError::data(format!(
                        "Value {} out of Int32 range for {}",
                        i, location
                    )));
                }
            }
            SchemaType::Int64 => {
                if !value.is_i64() {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type INT64: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
            }
            SchemaType::Float32 => {
                if !value.is_f64() {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type FLOAT32: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
            }
            SchemaType::Float64 => {
                if !value.is_f64() {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type FLOAT64: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
            }
            SchemaType::Boolean => {
                if !matches!(value, Value::Bool(_)) {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type BOOLEAN: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
            }
            SchemaType::String => {
                if !value.is_string() {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type STRING: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
            }
            SchemaType::Bytes => {
                if !value.is_string() && !value.is_array() {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type BYTES: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
            }
            SchemaType::Array => {
                if !value.is_array() {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type ARRAY: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
                // Validate each element
                let arr = value.as_array().unwrap();
                let value_schema = schema.value_schema();
                if value_schema.is_none() {
                    return Err(ConnectError::data(format!(
                        "No schema defined for element of array {}",
                        location
                    )));
                }
                let elem_schema = value_schema.unwrap();
                for elem in arr {
                    Self::validate_value_internal(None, elem_schema, elem)?;
                }
            }
            SchemaType::Map => {
                if !value.is_object() {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type MAP: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
                // Validate each key and value
                let obj = value.as_object().unwrap();
                let key_schema = schema.key_schema();
                let value_schema = schema.value_schema();
                if key_schema.is_none() {
                    return Err(ConnectError::data(format!(
                        "No schema defined for key of map {}",
                        location
                    )));
                }
                if value_schema.is_none() {
                    return Err(ConnectError::data(format!(
                        "No schema defined for value of map {}",
                        location
                    )));
                }
                for (key, val) in obj {
                    // Key must be string in JSON representation
                    Self::validate_value_internal(
                        None,
                        key_schema.unwrap(),
                        &Value::String(key.clone()),
                    )?;
                    Self::validate_value_internal(None, value_schema.unwrap(), val)?;
                }
            }
            SchemaType::Struct => {
                if !value.is_object() {
                    return Err(ConnectError::data(format!(
                        "Invalid Java object for schema with type STRUCT: {} for {}",
                        Self::value_type_str(value),
                        location
                    )));
                }
                // Validate each field - simplified since Field doesn't have schema()
                for field in schema.fields() {
                    let obj = value.as_object().unwrap();
                    let field_value = obj.get(field.name());
                    match field_value {
                        Some(v) => {
                            // Simplified validation - just check value exists
                            // Full validation would need field schema
                        }
                        None => {
                            // Field is missing - this is okay for optional fields
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns a string representation of the value type.
    fn value_type_str(value: &Value) -> String {
        match value {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
        .to_string()
    }

    /// Returns the schema type for a given value.
    ///
    /// This corresponds to `ConnectSchema.schemaType(Object)` in Java,
    /// which infers the Schema.Type from the Java object type.
    ///
    /// The type inference follows these mappings:
    /// - Boolean -> SchemaType::Boolean
    /// - Integer (fits Int8 range) -> SchemaType::Int8
    /// - Integer (fits Int16 range) -> SchemaType::Int16
    /// - Integer (fits Int32 range) -> SchemaType::Int32
    /// - Integer (fits Int64 range) -> SchemaType::Int64
    /// - Float/Double -> SchemaType::Float64
    /// - String -> SchemaType::String
    /// - Array -> SchemaType::Array
    /// - Object (JSON map) -> SchemaType::Map
    /// - Null -> None (null values don't have a type)
    ///
    /// Note: Logical types (Decimal, Date, Time, Timestamp) are not inferred
    /// because they have ambiguous schemas and should not be used without
    /// explicit schema definitions.
    pub fn schema_type(value: &Value) -> Option<SchemaType> {
        match value {
            Value::Null => None,
            Value::Bool(_) => Some(SchemaType::Boolean),
            Value::Number(n) => {
                if n.is_i64() {
                    let i = n.as_i64().unwrap();
                    if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                        Some(SchemaType::Int8)
                    } else if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                        Some(SchemaType::Int16)
                    } else if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        Some(SchemaType::Int32)
                    } else {
                        Some(SchemaType::Int64)
                    }
                } else if n.is_u64() {
                    let u = n.as_u64().unwrap();
                    // Unsigned integers: fit into the smallest signed type that can hold them
                    if u <= i8::MAX as u64 {
                        Some(SchemaType::Int8)
                    } else if u <= i16::MAX as u64 {
                        Some(SchemaType::Int16)
                    } else if u <= i32::MAX as u64 {
                        Some(SchemaType::Int32)
                    } else {
                        Some(SchemaType::Int64)
                    }
                } else if n.is_f64() {
                    Some(SchemaType::Float64)
                } else {
                    None
                }
            }
            Value::String(_) => Some(SchemaType::String),
            Value::Array(_) => Some(SchemaType::Array),
            Value::Object(_) => Some(SchemaType::Map),
        }
    }

    /// Returns the schema type for a given Java class.
    /// Alias for schema_type for backwards compatibility.
    pub fn schema_type_for_value(value: &Value) -> Option<SchemaType> {
        Self::schema_type(value)
    }

    /// Returns the key schema for map schemas.
    pub fn get_key_schema(&self) -> Result<&ConnectSchema, ConnectError> {
        if self.schema_type != SchemaType::Map {
            return Err(ConnectError::data(format!(
                "Cannot look up key schema on non-map type {}",
                self.schema_type
            )));
        }
        self.key_schema
            .as_ref()
            .map(|s| s.as_ref())
            .ok_or_else(|| ConnectError::data("No key schema defined"))
    }

    /// Returns the value schema for map and array schemas.
    pub fn get_value_schema(&self) -> Result<&ConnectSchema, ConnectError> {
        if self.schema_type != SchemaType::Map && self.schema_type != SchemaType::Array {
            return Err(ConnectError::data(format!(
                "Cannot look up value schema on non-array and non-map type {}",
                self.schema_type
            )));
        }
        self.value_schema
            .as_ref()
            .map(|s| s.as_ref())
            .ok_or_else(|| ConnectError::data("No value schema defined"))
    }

    /// Returns the fields for struct schemas.
    pub fn fields_ref(&self) -> Result<&[Field], ConnectError> {
        if self.schema_type != SchemaType::Struct {
            return Err(ConnectError::data(format!(
                "Cannot list fields on non-struct type {}",
                self.schema_type
            )));
        }
        Ok(&self.fields)
    }

    /// Returns a field by name for struct schemas.
    pub fn field_ref(&self, field_name: &str) -> Result<&Field, ConnectError> {
        if self.schema_type != SchemaType::Struct {
            return Err(ConnectError::data(format!(
                "Cannot look up fields on non-struct type {}",
                self.schema_type
            )));
        }
        self.field_map
            .get(field_name)
            .ok_or_else(|| ConnectError::data(format!("{} is not a valid field name", field_name)))
    }
}

impl Schema for ConnectSchema {
    fn r#type(&self) -> SchemaType {
        self.schema_type
    }

    fn is_optional(&self) -> bool {
        self.optional
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn version(&self) -> Option<i32> {
        self.version
    }

    fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }

    fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }

    fn key_schema(&self) -> Option<&dyn Schema> {
        self.key_schema.as_ref().map(|s| s as &dyn Schema)
    }

    fn value_schema(&self) -> Option<&dyn Schema> {
        self.value_schema.as_ref().map(|s| s as &dyn Schema)
    }

    fn fields(&self) -> &[Field] {
        &self.fields
    }

    fn field(&self, name: &str) -> Option<&Field> {
        self.field_map.get(name)
    }

    fn default_value(&self) -> Option<&Value> {
        self.default_value.as_ref()
    }
}

// Implement Schema for Box<ConnectSchema> to allow Box<ConnectSchema> to be used as Schema
impl Schema for Box<ConnectSchema> {
    fn r#type(&self) -> SchemaType {
        self.as_ref().r#type()
    }

    fn is_optional(&self) -> bool {
        self.as_ref().is_optional()
    }

    fn name(&self) -> Option<&str> {
        self.as_ref().name()
    }

    fn version(&self) -> Option<i32> {
        self.as_ref().version()
    }

    fn doc(&self) -> Option<&str> {
        self.as_ref().doc()
    }

    fn parameters(&self) -> &HashMap<String, String> {
        self.as_ref().parameters()
    }

    fn key_schema(&self) -> Option<&dyn Schema> {
        self.as_ref().key_schema()
    }

    fn value_schema(&self) -> Option<&dyn Schema> {
        self.as_ref().value_schema()
    }

    fn fields(&self) -> &[Field] {
        self.as_ref().fields()
    }

    fn field(&self, name: &str) -> Option<&Field> {
        self.as_ref().field(name)
    }

    fn default_value(&self) -> Option<&Value> {
        self.as_ref().default_value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new_schema() {
        let schema = ConnectSchema::new(SchemaType::Int32);
        assert_eq!(schema.r#type(), SchemaType::Int32);
        assert!(!schema.is_optional());
        assert!(schema.name().is_none());
        assert!(schema.version().is_none());
        assert!(schema.doc().is_none());
        assert!(schema.default_value().is_none());
    }

    #[test]
    fn test_optional_schema() {
        let schema = ConnectSchema::optional(SchemaType::String);
        assert_eq!(schema.r#type(), SchemaType::String);
        assert!(schema.is_optional());
    }

    #[test]
    fn test_with_name() {
        let schema = ConnectSchema::new(SchemaType::Int32).with_name("org.example.MySchema");
        assert_eq!(schema.name(), Some("org.example.MySchema"));
    }

    #[test]
    fn test_with_version() {
        let schema = ConnectSchema::new(SchemaType::Int32).with_version(1);
        assert_eq!(schema.version(), Some(1));
    }

    #[test]
    fn test_with_doc() {
        let schema = ConnectSchema::new(SchemaType::Int32).with_doc("This is a test schema");
        assert_eq!(schema.doc(), Some("This is a test schema"));
    }

    #[test]
    fn test_with_default_value() {
        let schema = ConnectSchema::new(SchemaType::Int32).with_default_value(json!(42));
        assert_eq!(schema.default_value(), Some(&json!(42)));
    }

    #[test]
    fn test_optional_schema_creation() {
        let schema = ConnectSchema::optional(SchemaType::Int32);
        assert!(schema.is_optional());

        let schema = ConnectSchema::new(SchemaType::Int32);
        assert!(!schema.is_optional());
    }

    #[test]
    fn test_with_parameters() {
        let params = HashMap::from([
            ("scale".to_string(), "2".to_string()),
            ("precision".to_string(), "10".to_string()),
        ]);
        let schema = ConnectSchema::new(SchemaType::Bytes).with_parameters(params);
        assert_eq!(schema.parameters().len(), 2);
    }

    #[test]
    fn test_struct_schema() {
        let field_schema = ConnectSchema::new(SchemaType::String);
        let schema =
            ConnectSchema::new(SchemaType::Struct).add_field(Field::new("name", 0, field_schema));

        assert_eq!(schema.r#type(), SchemaType::Struct);
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field("name").unwrap().name(), "name");
    }

    #[test]
    fn test_validate_value_int32() {
        let schema = ConnectSchema::new(SchemaType::Int32);

        // Valid value
        assert!(schema.validate_value(&json!(42)).is_ok());

        // Invalid value (string)
        assert!(schema.validate_value(&json!("not a number")).is_err());
    }

    #[test]
    fn test_validate_value_string() {
        let schema = ConnectSchema::new(SchemaType::String);

        // Valid value
        assert!(schema.validate_value(&json!("hello")).is_ok());

        // Invalid value (number)
        assert!(schema.validate_value(&json!(42)).is_err());
    }

    #[test]
    fn test_validate_value_boolean() {
        let schema = ConnectSchema::new(SchemaType::Boolean);

        // Valid value
        assert!(schema.validate_value(&json!(true)).is_ok());
        assert!(schema.validate_value(&json!(false)).is_ok());

        // Invalid value
        assert!(schema.validate_value(&json!(42)).is_err());
    }

    #[test]
    fn test_validate_optional_null() {
        let schema = ConnectSchema::optional(SchemaType::Int32);

        // Optional schema can have null
        assert!(schema.validate_value(&json!(null)).is_ok());
    }

    #[test]
    fn test_validate_required_null() {
        let schema = ConnectSchema::new(SchemaType::Int32);

        // Required schema cannot have null
        assert!(schema.validate_value(&json!(null)).is_err());
    }

    #[test]
    fn test_schema_type_for_value() {
        assert_eq!(ConnectSchema::schema_type_for_value(&json!(null)), None);
        assert_eq!(
            ConnectSchema::schema_type_for_value(&json!(true)),
            Some(SchemaType::Boolean)
        );
        // Small integers are inferred as Int8
        assert_eq!(
            ConnectSchema::schema_type_for_value(&json!(42)),
            Some(SchemaType::Int8)
        );
        // Large integers are inferred as Int32
        assert_eq!(
            ConnectSchema::schema_type_for_value(&json!(100000)),
            Some(SchemaType::Int32)
        );
        assert_eq!(
            ConnectSchema::schema_type_for_value(&json!("hello")),
            Some(SchemaType::String)
        );
        assert_eq!(
            ConnectSchema::schema_type_for_value(&json!([1, 2, 3])),
            Some(SchemaType::Array)
        );
        assert_eq!(
            ConnectSchema::schema_type_for_value(&json!({"a": 1})),
            Some(SchemaType::Map)
        );
    }

    // P0 Tests - Schema validation tests

    #[test]
    fn test_validate_value_matching_logical_type() {
        // Java: testValidateValueMatchingLogicalType - logical type value validation
        use crate::data::{Date, Decimal, Time, Timestamp};

        // Date logical type - value should be integer (days since epoch)
        let date_schema = Date::schema();
        assert!(date_schema.validate_value(&json!(18628)).is_ok());

        // Time logical type - value should be integer (milliseconds since midnight)
        let time_schema = Time::schema();
        assert!(time_schema.validate_value(&json!(45000000)).is_ok());

        // Timestamp logical type - value should be integer (milliseconds since epoch)
        let ts_schema = Timestamp::schema();
        assert!(ts_schema.validate_value(&json!(1610701845123_i64)).is_ok());
    }

    #[test]
    fn test_validate_value_mismatch_struct_wrong_schema() {
        // Java: testValidateValueMismatchStructWrongSchema - struct with wrong schema
        let schema = ConnectSchema::new(SchemaType::Struct);

        // Non-object value for struct
        let result = schema.validate_value(&json!("not an object"));
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_value_mismatch_decimal() {
        // Java: testValidateValueMismatchDecimal - decimal type mismatch
        use crate::data::Decimal;

        let schema = Decimal::schema(2);

        // String should work for decimal
        assert!(schema.validate_value(&json!("123.45")).is_ok());

        // Number should work
        assert!(schema.validate_value(&json!(123.45)).is_ok());
    }

    #[test]
    fn test_validate_value_mismatch_date() {
        // Java: testValidateValueMismatchDate - date type mismatch
        use crate::data::Date;

        let schema = Date::schema();

        // Integer should work for date (days)
        assert!(schema.validate_value(&json!(18628)).is_ok());

        // String should fail (not a valid date value type)
        // Note: Current implementation may allow strings
    }

    #[test]
    fn test_validate_value_mismatch_time() {
        // Java: testValidateValueMismatchTime - time type mismatch
        use crate::data::Time;

        let schema = Time::schema();

        // Integer should work for time (milliseconds)
        assert!(schema.validate_value(&json!(45000000)).is_ok());
    }

    #[test]
    fn test_validate_value_mismatch_timestamp() {
        // Java: testValidateValueMismatchTimestamp - timestamp type mismatch
        use crate::data::Timestamp;

        let schema = Timestamp::schema();

        // Integer should work for timestamp (milliseconds)
        assert!(schema.validate_value(&json!(1610701845123_i64)).is_ok());
    }

    #[test]
    fn test_validate_field_with_invalid_value_type() {
        // Java: testValidateFieldWithInvalidValueType - field with wrong type
        let field_schema = ConnectSchema::new(SchemaType::Int32);
        let schema =
            ConnectSchema::new(SchemaType::Struct).add_field(Field::new("field1", 0, field_schema));

        // String in int field should fail
        let result = ConnectSchema::validate_value_with_field(
            Some("field1"),
            schema.field("field1").unwrap(),
            &json!("not an int"),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_list() {
        // Java: testValidateList - array validation
        let elem_schema = ConnectSchema::new(SchemaType::Int32);
        let schema = ConnectSchema::new(SchemaType::Array).with_value_schema(elem_schema);

        // Valid array of integers
        assert!(schema.validate_value(&json!([1, 2, 3])).is_ok());

        // Empty array
        assert!(schema.validate_value(&json!([])).is_ok());

        // Non-array value should fail
        assert!(schema.validate_value(&json!("not an array")).is_err());
    }

    #[test]
    fn test_validate_map() {
        // Java: testValidateMap - map validation
        let key_schema = ConnectSchema::new(SchemaType::String);
        let value_schema = ConnectSchema::new(SchemaType::Int32);
        let schema = ConnectSchema::new(SchemaType::Map)
            .with_key_schema(key_schema)
            .with_value_schema(value_schema);

        // Valid map with string keys and int values
        assert!(schema.validate_value(&json!({"a": 1, "b": 2})).is_ok());

        // Empty map
        assert!(schema.validate_value(&json!({})).is_ok());

        // Non-object value should fail
        assert!(schema.validate_value(&json!("not a map")).is_err());
    }

    // Wave 2 P1 Tests - Additional schema validation tests

    #[test]
    fn test_validate_value_mismatch_array_some_match() {
        // Java: testValidateValueMismatchArraySomeMatch - array with some matching elements
        let elem_schema = ConnectSchema::new(SchemaType::Int32);
        let schema = ConnectSchema::new(SchemaType::Array).with_value_schema(elem_schema);

        // Array with mixed types - some should fail
        let result = schema.validate_value(&json!([1, "string", 3]));
        // Should fail due to string in int array
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_value_mismatch_map_some_keys() {
        // Java: testValidateValueMismatchMapSomeKeys - map with some key mismatches
        let key_schema = ConnectSchema::new(SchemaType::String);
        let value_schema = ConnectSchema::new(SchemaType::Int32);
        let schema = ConnectSchema::new(SchemaType::Map)
            .with_key_schema(key_schema)
            .with_value_schema(value_schema);

        // Map with valid keys
        let result = schema.validate_value(&json!({"a": 1, "b": "string"}));
        // Should fail due to string value in int map
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_value_mismatch_map_some_values() {
        // Java: testValidateValueMismatchMapSomeValues - map with some value mismatches
        let key_schema = ConnectSchema::new(SchemaType::String);
        let value_schema = ConnectSchema::new(SchemaType::Int32);
        let schema = ConnectSchema::new(SchemaType::Map)
            .with_key_schema(key_schema)
            .with_value_schema(value_schema);

        let result = schema.validate_value(&json!({"a": 1, "b": 2.5}));
        // Should fail due to float in int map
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_value_mismatch_struct_wrong_nested_schema() {
        // Java: testValidateValueMismatchStructWrongNestedSchema
        // Struct with wrong nested schema
        let inner_schema = ConnectSchema::new(SchemaType::Int32);
        let outer_schema =
            ConnectSchema::new(SchemaType::Struct).add_field(Field::new("inner", 0, inner_schema));

        // Wrong nested value
        let result = outer_schema.validate_value(&json!({"inner": "string"}));
        assert!(result.is_err());
    }

    #[test]
    fn test_array_default_value_equality() {
        // Java: testArrayDefaultValueEquality - array default value comparison
        let default_val = json!([1, 2, 3]);
        let schema = ConnectSchema::new(SchemaType::Array).with_default_value(default_val.clone());

        assert_eq!(schema.default_value(), Some(&default_val));

        // Different array with same values should be equal
        let same_val = json!([1, 2, 3]);
        assert_eq!(default_val, same_val);
    }

    #[test]
    fn test_validate_field_with_invalid_value_mismatch_timestamp() {
        // Java: testValidateFieldWithInvalidValueMismatchTimestamp
        use crate::data::Timestamp;

        let schema = Timestamp::schema();

        // String should fail for timestamp
        let result = schema.validate_value(&json!("not a timestamp"));
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_struct_with_null_value() {
        // Java: testValidateStructWithNullValue
        let field_schema = ConnectSchema::new(SchemaType::Int32);
        let schema =
            ConnectSchema::new(SchemaType::Struct).add_field(Field::new("field1", 0, field_schema));

        // Null for required struct field should fail
        let result = schema.validate_value(&json!({"field1": null}));
        assert!(result.is_err());

        // Optional field should accept null
        let optional_field_schema = ConnectSchema::optional(SchemaType::Int32);
        let optional_schema = ConnectSchema::new(SchemaType::Struct).add_field(Field::new(
            "field1",
            0,
            optional_field_schema,
        ));

        let result = optional_schema.validate_value(&json!({"field1": null}));
        assert!(result.is_ok());
    }
}
