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

use crate::connector_types::ConnectRecord;
use crate::error::{ConnectException, DataException};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Schema type enumeration defining the basic data types supported by Connect schemas.
pub type SchemaType = Type;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Type {
    /// 8-bit signed integer
    Int8,
    /// 16-bit signed integer
    Int16,
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 32-bit floating point
    Float32,
    /// 64-bit floating point
    Float64,
    /// Boolean
    Boolean,
    /// String
    String,
    /// Bytes
    Bytes,
    /// Array
    Array,
    /// Map
    Map,
    /// Struct
    Struct,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Int8 => write!(f, "INT8"),
            Type::Int16 => write!(f, "INT16"),
            Type::Int32 => write!(f, "INT32"),
            Type::Int64 => write!(f, "INT64"),
            Type::Float32 => write!(f, "FLOAT32"),
            Type::Float64 => write!(f, "FLOAT64"),
            Type::Boolean => write!(f, "BOOLEAN"),
            Type::String => write!(f, "STRING"),
            Type::Bytes => write!(f, "BYTES"),
            Type::Array => write!(f, "ARRAY"),
            Type::Map => write!(f, "MAP"),
            Type::Struct => write!(f, "STRUCT"),
        }
    }
}

/// Schema trait defining the structure of data in Kafka Connect.
pub trait Schema: fmt::Debug + Send + Sync {
    /// Returns the type of the schema.
    fn type_(&self) -> Type;

    /// Returns whether the schema is optional.
    fn is_optional(&self) -> bool;

    /// Returns the default value for the schema.
    fn default_value(&self) -> Option<&dyn Any>;

    /// Returns the name of the schema.
    fn name(&self) -> Option<&str>;

    /// Returns the version of the schema.
    fn version(&self) -> Option<i32>;

    /// Returns the documentation string for the schema.
    fn doc(&self) -> Option<&str>;

    /// Returns a map of schema parameters.
    fn parameters(&self) -> Option<&HashMap<String, String>>;

    /// Returns a list of fields if the schema is a STRUCT type.
    fn fields(&self) -> Result<Vec<Field>, DataException>;

    /// Returns a specific field by name if the schema is a STRUCT type.
    fn field(&self, field_name: &str) -> Result<Field, DataException>;

    /// Returns the key schema if the schema is a MAP type.
    fn key_schema(&self) -> Result<Arc<dyn Schema>, DataException>;

    /// Returns the value schema if the schema is a MAP or ARRAY type.
    fn value_schema(&self) -> Result<Arc<dyn Schema>, DataException>;

    /// Validates if a given value conforms to this schema.
    fn validate_value(&self, value: &dyn Any) -> Result<(), DataException>;

    /// Returns `self` if this is a `ConnectSchema`, otherwise `None`.
    /// This allows downcasting from `dyn Schema` to `ConnectSchema`.
    fn as_connect_schema(&self) -> Option<&ConnectSchema> {
        None
    }
}

/// Field represents a field within a STRUCT schema.
#[derive(Debug, Clone)]
pub struct Field {
    index: i32,
    name: String,
    schema: Arc<ConnectSchema>,
    doc: Option<String>,
}

impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.name == other.name
    }
}

impl Eq for Field {}

impl Field {
    pub fn new(index: i32, name: String, schema: Arc<ConnectSchema>) -> Self {
        Field {
            index,
            name,
            schema,
            doc: None,
        }
    }

    pub fn with_doc(mut self, doc: String) -> Self {
        self.doc = Some(doc);
        self
    }

    pub fn index(&self) -> i32 {
        self.index
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> Arc<ConnectSchema> {
        Arc::clone(&self.schema)
    }

    pub fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }
}

/// ConnectSchema is the primary implementation of the Schema interface.
#[derive(Debug)]
pub struct ConnectSchema {
    type_: Type,
    optional: bool,
    default_value: Option<Box<dyn Any + Send + Sync>>,
    fields: Option<Vec<Field>>,
    fields_by_name: Option<HashMap<String, Field>>,
    key_schema: Option<Arc<ConnectSchema>>,
    value_schema: Option<Arc<ConnectSchema>>,
    name: Option<String>,
    version: Option<i32>,
    doc: Option<String>,
    parameters: Option<HashMap<String, String>>,
}

impl Clone for ConnectSchema {
    fn clone(&self) -> Self {
        ConnectSchema {
            type_: self.type_,
            optional: self.optional,
            default_value: None, // Cannot clone Box<dyn Any>
            fields: self.fields.clone(),
            fields_by_name: self.fields_by_name.clone(),
            key_schema: self.key_schema.clone(),
            value_schema: self.value_schema.clone(),
            name: self.name.clone(),
            version: self.version,
            doc: self.doc.clone(),
            parameters: self.parameters.clone(),
        }
    }
}

impl ConnectSchema {
    /// Creates a new primitive schema with the given type.
    pub fn new(type_: Type) -> Self {
        ConnectSchema {
            type_,
            optional: false,
            default_value: None,
            fields: None,
            fields_by_name: None,
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    /// Creates a new array schema.
    pub fn array(value_schema: Arc<ConnectSchema>) -> Self {
        ConnectSchema {
            type_: Type::Array,
            optional: false,
            default_value: None,
            fields: None,
            fields_by_name: None,
            key_schema: None,
            value_schema: Some(value_schema),
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    /// Creates a new map schema.
    pub fn map(key_schema: Arc<ConnectSchema>, value_schema: Arc<ConnectSchema>) -> Self {
        ConnectSchema {
            type_: Type::Map,
            optional: false,
            default_value: None,
            fields: None,
            fields_by_name: None,
            key_schema: Some(key_schema),
            value_schema: Some(value_schema),
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    /// Creates a new struct schema.
    pub fn struct_(fields: Vec<Field>) -> Self {
        let mut fields_by_name = HashMap::new();
        for field in &fields {
            fields_by_name.insert(field.name().to_string(), field.clone());
        }

        ConnectSchema {
            type_: Type::Struct,
            optional: false,
            default_value: None,
            fields: Some(fields),
            fields_by_name: Some(fields_by_name),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn with_optional(mut self, optional: bool) -> Self {
        self.optional = optional;
        self
    }

    pub fn with_default_value(mut self, value: Box<dyn Any + Send + Sync>) -> Self {
        self.default_value = Some(value);
        self
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_version(mut self, version: i32) -> Self {
        self.version = Some(version);
        self
    }

    pub fn with_doc(mut self, doc: String) -> Self {
        self.doc = Some(doc);
        self
    }

    pub fn with_parameters(mut self, parameters: HashMap<String, String>) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Validates a value against a schema.
    pub fn validate_value_static(
        schema: &dyn Schema,
        value: &dyn Any,
    ) -> Result<(), DataException> {
        if value.is::<()>() {
            if !schema.is_optional() {
                return Err(DataException::new(
                    "Value cannot be null for non-optional schema".to_string(),
                ));
            }
            return Ok(());
        }

        let type_ = schema.type_();
        match type_ {
            Type::Int8 => {
                if !value.is::<i8>() {
                    return Err(DataException::new(format!(
                        "Expected i8 for schema type {}, got different type",
                        type_
                    )));
                }
            }
            Type::Int16 => {
                if !value.is::<i16>() {
                    return Err(DataException::new(format!(
                        "Expected i16 for schema type {}, got different type",
                        type_
                    )));
                }
            }
            Type::Int32 => {
                if !value.is::<i32>() {
                    return Err(DataException::new(format!(
                        "Expected i32 for schema type {}, got different type",
                        type_
                    )));
                }
            }
            Type::Int64 => {
                if !value.is::<i64>() {
                    return Err(DataException::new(format!(
                        "Expected i64 for schema type {}, got different type",
                        type_
                    )));
                }
            }
            Type::Float32 => {
                if !value.is::<f32>() {
                    return Err(DataException::new(format!(
                        "Expected f32 for schema type {}, got different type",
                        type_
                    )));
                }
            }
            Type::Float64 => {
                if !value.is::<f64>() {
                    return Err(DataException::new(format!(
                        "Expected f64 for schema type {}, got different type",
                        type_
                    )));
                }
            }
            Type::Boolean => {
                if !value.is::<bool>() {
                    return Err(DataException::new(format!(
                        "Expected bool for schema type {}, got different type",
                        type_
                    )));
                }
            }
            Type::String => {
                if !value.is::<String>() {
                    return Err(DataException::new(format!(
                        "Expected String for schema type {}, got different type",
                        type_
                    )));
                }
            }
            Type::Bytes => {
                if !value.is::<Vec<u8>>() {
                    return Err(DataException::new(format!(
                        "Expected Vec<u8> for schema type {}, got different type",
                        type_
                    )));
                }
            }
            Type::Array => {
                if !value.is::<Vec<Box<dyn Any + Send + Sync>>>() {
                    return Err(DataException::new(format!(
                        "Expected Vec for schema type {}, got different type",
                        type_
                    )));
                }
            }
            Type::Map => {
                if !value.is::<HashMap<String, Box<dyn Any + Send + Sync>>>() {
                    return Err(DataException::new(format!(
                        "Expected HashMap for schema type {}, got different type",
                        type_
                    )));
                }
            }
            Type::Struct => {
                if !value.is::<Struct>() {
                    return Err(DataException::new(format!(
                        "Expected Struct for schema type {}, got different type",
                        type_
                    )));
                }
            }
        }

        Ok(())
    }
}

impl Schema for ConnectSchema {
    fn type_(&self) -> Type {
        self.type_
    }

    fn is_optional(&self) -> bool {
        self.optional
    }

    fn default_value(&self) -> Option<&dyn Any> {
        self.default_value.as_ref().map(|v| v.as_ref() as &dyn Any)
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

    fn parameters(&self) -> Option<&HashMap<String, String>> {
        self.parameters.as_ref()
    }

    fn fields(&self) -> Result<Vec<Field>, DataException> {
        if self.type_ != Type::Struct {
            return Err(DataException::new(format!(
                "Cannot get fields for schema of type {}",
                self.type_
            )));
        }
        self.fields
            .clone()
            .ok_or_else(|| DataException::new("Fields not available for STRUCT schema".to_string()))
    }

    fn field(&self, field_name: &str) -> Result<Field, DataException> {
        if self.type_ != Type::Struct {
            return Err(DataException::new(format!(
                "Cannot get field for schema of type {}",
                self.type_
            )));
        }
        self.fields_by_name
            .as_ref()
            .and_then(|map| map.get(field_name).cloned())
            .ok_or_else(|| {
                DataException::new(format!("Field '{}' not found in schema", field_name))
            })
    }

    fn key_schema(&self) -> Result<Arc<dyn Schema>, DataException> {
        if self.type_ != Type::Map {
            return Err(DataException::new(format!(
                "Cannot get key schema for schema of type {}",
                self.type_
            )));
        }
        self.key_schema
            .as_ref()
            .map(|s| Arc::clone(s) as Arc<dyn Schema>)
            .ok_or_else(|| {
                DataException::new("Key schema not available for MAP schema".to_string())
            })
    }

    fn value_schema(&self) -> Result<Arc<dyn Schema>, DataException> {
        if self.type_ != Type::Array && self.type_ != Type::Map {
            return Err(DataException::new(format!(
                "Cannot get value schema for schema of type {}",
                self.type_
            )));
        }
        self.value_schema
            .as_ref()
            .map(|s| Arc::clone(s) as Arc<dyn Schema>)
            .ok_or_else(|| {
                DataException::new("Value schema not available for ARRAY/MAP schema".to_string())
            })
    }

    fn validate_value(&self, value: &dyn Any) -> Result<(), DataException> {
        ConnectSchema::validate_value_static(self, value)
    }

    fn as_connect_schema(&self) -> Option<&ConnectSchema> {
        Some(self)
    }
}

/// SchemaBuilder is a utility class for constructing Schema objects in a fluent way.
pub struct SchemaBuilder {
    type_: Option<Type>,
    optional: bool,
    default_value: Option<Box<dyn Any + Send + Sync>>,
    fields: Vec<Field>,
    key_schema: Option<Arc<ConnectSchema>>,
    value_schema: Option<Arc<ConnectSchema>>,
    name: Option<String>,
    version: Option<i32>,
    doc: Option<String>,
    parameters: Option<HashMap<String, String>>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        SchemaBuilder {
            type_: None,
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn r#struct() -> Self {
        SchemaBuilder {
            type_: None,
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn struct_() -> Self {
        SchemaBuilder {
            type_: Some(Type::Struct),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn int8() -> Self {
        SchemaBuilder {
            type_: Some(Type::Int8),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn int16() -> Self {
        SchemaBuilder {
            type_: Some(Type::Int16),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn int32() -> Self {
        SchemaBuilder {
            type_: Some(Type::Int32),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn int64() -> Self {
        SchemaBuilder {
            type_: Some(Type::Int64),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn float32() -> Self {
        SchemaBuilder {
            type_: Some(Type::Float32),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn float64() -> Self {
        SchemaBuilder {
            type_: Some(Type::Float64),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn boolean() -> Self {
        SchemaBuilder {
            type_: Some(Type::Boolean),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn string() -> Self {
        SchemaBuilder {
            type_: Some(Type::String),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn bytes() -> Self {
        SchemaBuilder {
            type_: Some(Type::Bytes),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn array(value_schema: Arc<ConnectSchema>) -> Self {
        SchemaBuilder {
            type_: Some(Type::Array),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: None,
            value_schema: Some(value_schema),
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn map(key_schema: Arc<ConnectSchema>, value_schema: Arc<ConnectSchema>) -> Self {
        SchemaBuilder {
            type_: Some(Type::Map),
            optional: false,
            default_value: None,
            fields: Vec::new(),
            key_schema: Some(key_schema),
            value_schema: Some(value_schema),
            name: None,
            version: None,
            doc: None,
            parameters: None,
        }
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }

    pub fn required(mut self) -> Self {
        self.optional = false;
        self
    }

    pub fn version(mut self, version: i32) -> Self {
        self.version = Some(version);
        self
    }

    pub fn doc(mut self, doc: String) -> Self {
        self.doc = Some(doc);
        self
    }

    pub fn parameter(mut self, key: String, value: String) -> Self {
        if self.parameters.is_none() {
            self.parameters = Some(HashMap::new());
        }
        self.parameters.as_mut().unwrap().insert(key, value);
        self
    }

    pub fn parameters(mut self, params: HashMap<String, String>) -> Self {
        self.parameters = Some(params);
        self
    }

    pub fn field(mut self, name: String, schema: Arc<ConnectSchema>) -> Self {
        let index = self.fields.len() as i32;
        self.fields.push(Field::new(index, name, schema));
        self
    }

    pub fn field_with_doc(mut self, name: String, schema: Arc<ConnectSchema>, doc: String) -> Self {
        let index = self.fields.len() as i32;
        self.fields
            .push(Field::new(index, name, schema).with_doc(doc));
        self
    }

    /// Sets the default value for this schema.
    ///
    /// # Arguments
    /// * `value` - The default value to set
    ///
    /// # Returns
    /// * Self for method chaining
    ///
    /// # Errors
    /// * Returns ConnectException if:
    ///   - Default value has already been set
    ///   - Schema type has not been specified
    ///   - The value does not match the schema type
    pub fn default_value(
        mut self,
        value: Box<dyn Any + Send + Sync>,
    ) -> Result<Self, ConnectException> {
        // Check if default value has already been set
        if self.default_value.is_some() {
            return Err(ConnectException::new(
                "Invalid SchemaBuilder call: default value has already been set".to_string(),
            ));
        }

        // Check that type has been specified
        if self.type_.is_none() {
            return Err(ConnectException::new(
                "Invalid SchemaBuilder call: type must be specified to set default value"
                    .to_string(),
            ));
        }

        // Validate the value against the schema
        // We need to create a temporary schema to validate
        let type_ = self.type_.unwrap();
        let temp_schema = ConnectSchema::new(type_);

        if let Err(e) = ConnectSchema::validate_value_static(&temp_schema, value.as_ref()) {
            return Err(ConnectException::new(format!(
                "Invalid default value: {}",
                e
            )));
        }

        self.default_value = Some(value);
        Ok(self)
    }

    /// Private helper method to check if a field can be set.
    ///
    /// # Arguments
    /// * `field_name` - Name of the field being set
    /// * `is_set` - Whether the field has already been set
    ///
    /// # Errors
    /// * Returns ConnectException if the field has already been set
    fn check_can_set(field_name: &str, is_set: bool) -> Result<(), ConnectException> {
        if is_set {
            return Err(ConnectException::new(format!(
                "Invalid SchemaBuilder call: {} has already been set",
                field_name
            )));
        }
        Ok(())
    }

    /// Private helper method to check if a required field is not null.
    ///
    /// # Arguments
    /// * `field_name` - Name of the field being checked
    /// * `is_some` - Whether the field has a value
    /// * `field_to_set` - Name of the field being set
    ///
    /// # Errors
    /// * Returns ConnectException if the required field is null
    fn check_not_null(
        field_name: &str,
        is_some: bool,
        field_to_set: &str,
    ) -> Result<(), ConnectException> {
        if !is_some {
            return Err(ConnectException::new(format!(
                "Invalid SchemaBuilder call: {} must be specified to set {}",
                field_name, field_to_set
            )));
        }
        Ok(())
    }

    pub fn build(self) -> Result<Arc<ConnectSchema>, ConnectException> {
        let type_ = self
            .type_
            .ok_or_else(|| ConnectException::new("Schema type not specified"))?;

        let schema = match type_ {
            Type::Struct => {
                let mut schema = ConnectSchema::struct_(self.fields);
                schema.optional = self.optional;
                schema.default_value = self.default_value;
                schema.name = self.name;
                schema.version = self.version;
                schema.doc = self.doc;
                schema.parameters = self.parameters;
                schema
            }
            Type::Array => {
                let value_schema = self
                    .value_schema
                    .ok_or_else(|| ConnectException::new("Value schema required for ARRAY"))?;
                let mut schema = ConnectSchema::array(value_schema);
                schema.optional = self.optional;
                schema.default_value = self.default_value;
                schema.name = self.name;
                schema.version = self.version;
                schema.doc = self.doc;
                schema.parameters = self.parameters;
                schema
            }
            Type::Map => {
                let key_schema = self
                    .key_schema
                    .ok_or_else(|| ConnectException::new("Key schema required for MAP"))?;
                let value_schema = self
                    .value_schema
                    .ok_or_else(|| ConnectException::new("Value schema required for MAP"))?;
                let mut schema = ConnectSchema::map(key_schema, value_schema);
                schema.optional = self.optional;
                schema.default_value = self.default_value;
                schema.name = self.name;
                schema.version = self.version;
                schema.doc = self.doc;
                schema.parameters = self.parameters;
                schema
            }
            _ => {
                let mut schema = ConnectSchema::new(type_);
                schema.optional = self.optional;
                schema.default_value = self.default_value;
                schema.name = self.name;
                schema.version = self.version;
                schema.doc = self.doc;
                schema.parameters = self.parameters;
                schema
            }
        };

        Ok(Arc::new(schema))
    }
}

/// Struct represents an instance of a STRUCT schema.
pub struct Struct {
    schema: Arc<ConnectSchema>,
    values: HashMap<String, Box<dyn Any + Send + Sync>>,
}

impl fmt::Debug for Struct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Struct{{")?;
        let mut first = true;
        if let Ok(fields) = self.schema.fields() {
            for field in &fields {
                if let Some(value) = self.get(field.name()) {
                    if !first {
                        write!(f, ", ")?;
                    }
                    first = false;
                    write!(f, "{}=", field.name())?;
                    format_value(value, f)?;
                }
            }
        }
        write!(f, "}}")
    }
}

impl fmt::Display for Struct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Struct{{")?;
        let mut first = true;
        if let Ok(fields) = self.schema.fields() {
            for field in &fields {
                if let Some(value) = self.get(field.name()) {
                    if !first {
                        write!(f, ",")?;
                    }
                    first = false;
                    write!(f, "{}=", field.name())?;
                    format_value(value, f)?;
                }
            }
        }
        write!(f, "}}")
    }
}

fn format_value(value: &dyn Any, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if let Some(v) = value.downcast_ref::<i8>() {
        write!(f, "{}", v)
    } else if let Some(v) = value.downcast_ref::<i16>() {
        write!(f, "{}", v)
    } else if let Some(v) = value.downcast_ref::<i32>() {
        write!(f, "{}", v)
    } else if let Some(v) = value.downcast_ref::<i64>() {
        write!(f, "{}", v)
    } else if let Some(v) = value.downcast_ref::<f32>() {
        write!(f, "{}", v)
    } else if let Some(v) = value.downcast_ref::<f64>() {
        write!(f, "{}", v)
    } else if let Some(v) = value.downcast_ref::<bool>() {
        write!(f, "{}", v)
    } else if let Some(v) = value.downcast_ref::<String>() {
        write!(f, "{}", v)
    } else if let Some(v) = value.downcast_ref::<Vec<u8>>() {
        write!(f, "{:?}", v)
    } else if let Some(v) = value.downcast_ref::<Struct>() {
        write!(f, "{}", v)
    } else {
        write!(f, "{{unknown}}")
    }
}

impl Clone for Struct {
    fn clone(&self) -> Self {
        Struct {
            schema: Arc::clone(&self.schema),
            values: HashMap::new(), // Cannot clone Box<dyn Any>
        }
    }
}

impl Struct {
    pub fn new(schema: Arc<ConnectSchema>) -> Result<Self, DataException> {
        if schema.type_() != Type::Struct {
            return Err(DataException::new(format!(
                "Cannot create Struct with schema of type {}",
                schema.type_()
            )));
        }
        Ok(Struct {
            schema,
            values: HashMap::new(),
        })
    }

    pub fn schema(&self) -> Arc<ConnectSchema> {
        Arc::clone(&self.schema)
    }

    pub fn put(
        &mut self,
        field_name: &str,
        value: Box<dyn Any + Send + Sync>,
    ) -> Result<(), DataException> {
        let field = self.schema.field(field_name)?;
        field.schema().validate_value(value.as_ref())?;
        self.values.insert(field_name.to_string(), value);
        Ok(())
    }

    pub fn put_without_validation(&mut self, field_name: &str, value: Box<dyn Any + Send + Sync>) {
        self.values.insert(field_name.to_string(), value);
    }

    pub fn get(&self, field_name: &str) -> Option<&dyn Any> {
        self.values.get(field_name).map(|v| v.as_ref() as &dyn Any)
    }

    pub fn get_without_default(&self, field_name: &str) -> Option<&dyn Any> {
        self.values.get(field_name).map(|v| v.as_ref() as &dyn Any)
    }

    pub fn get_int8(&self, field_name: &str) -> Result<i8, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<i8>())
            .copied()
            .ok_or_else(|| DataException::new(format!("Field {} is not an i8", field_name)))
    }

    pub fn get_int16(&self, field_name: &str) -> Result<i16, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<i16>())
            .copied()
            .ok_or_else(|| DataException::new(format!("Field {} is not an i16", field_name)))
    }

    pub fn get_int32(&self, field_name: &str) -> Result<i32, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<i32>())
            .copied()
            .ok_or_else(|| DataException::new(format!("Field {} is not an i32", field_name)))
    }

    pub fn get_int64(&self, field_name: &str) -> Result<i64, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<i64>())
            .copied()
            .ok_or_else(|| DataException::new(format!("Field {} is not an i64", field_name)))
    }

    pub fn get_float32(&self, field_name: &str) -> Result<f32, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<f32>())
            .copied()
            .ok_or_else(|| DataException::new(format!("Field {} is not an f32", field_name)))
    }

    pub fn get_float64(&self, field_name: &str) -> Result<f64, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<f64>())
            .copied()
            .ok_or_else(|| DataException::new(format!("Field {} is not an f64", field_name)))
    }

    pub fn get_boolean(&self, field_name: &str) -> Result<bool, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<bool>())
            .copied()
            .ok_or_else(|| DataException::new(format!("Field {} is not a bool", field_name)))
    }

    pub fn get_string(&self, field_name: &str) -> Result<String, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<String>())
            .cloned()
            .ok_or_else(|| DataException::new(format!("Field {} is not a String", field_name)))
    }

    pub fn get_bytes(&self, field_name: &str) -> Result<Vec<u8>, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<Vec<u8>>())
            .cloned()
            .ok_or_else(|| DataException::new(format!("Field {} is not Vec<u8>", field_name)))
    }

    pub fn get_array(
        &self,
        field_name: &str,
    ) -> Result<&Vec<Box<dyn Any + Send + Sync>>, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<Vec<Box<dyn Any + Send + Sync>>>())
            .ok_or_else(|| DataException::new(format!("Field {} is not Vec", field_name)))
    }

    pub fn get_map(
        &self,
        field_name: &str,
    ) -> Result<&HashMap<String, Box<dyn Any + Send + Sync>>, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<HashMap<String, Box<dyn Any + Send + Sync>>>())
            .ok_or_else(|| DataException::new(format!("Field {} is not HashMap", field_name)))
    }

    pub fn get_struct(&self, field_name: &str) -> Result<Struct, DataException> {
        self.get(field_name)
            .and_then(|v| v.downcast_ref::<Struct>())
            .cloned()
            .ok_or_else(|| DataException::new(format!("Field {} is not Struct", field_name)))
    }

    pub fn validate(&self) -> Result<(), DataException> {
        for field in self.schema.fields()? {
            if let Some(value) = self.get(field.name()) {
                field.schema().validate_value(value)?;
            } else if !field.schema().is_optional() {
                return Err(DataException::new(format!(
                    "Required field {} is missing",
                    field.name()
                )));
            }
        }
        Ok(())
    }

    pub fn equals(&self, other: &Struct) -> bool {
        // Compare schema references
        if Arc::ptr_eq(&self.schema, &other.schema) {
            // Same schema, compare values
            return self.compare_values(&other.values);
        }

        // Different schemas - compare schema type and fields
        if self.schema.type_() != other.schema.type_() {
            return false;
        }

        // Compare schema fields
        let self_fields = match self.schema.fields() {
            Ok(fields) => fields,
            Err(_) => return false,
        };
        let other_fields = match other.schema.fields() {
            Ok(fields) => fields,
            Err(_) => return false,
        };

        if self_fields.len() != other_fields.len() {
            return false;
        }

        for (self_field, other_field) in self_fields.iter().zip(other_fields.iter()) {
            if self_field.name() != other_field.name() {
                return false;
            }
            if self_field.index() != other_field.index() {
                return false;
            }
        }

        // Compare values
        self.compare_values(&other.values)
    }

    fn compare_values(&self, other_values: &HashMap<String, Box<dyn Any + Send + Sync>>) -> bool {
        // Compare number of fields
        if self.values.len() != other_values.len() {
            return false;
        }

        // Compare each field value
        for (field_name, self_value) in &self.values {
            if let Some(other_value) = other_values.get(field_name) {
                if !Self::compare_any_values(self_value.as_ref(), other_value.as_ref()) {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }

    fn compare_any_values(a: &dyn Any, b: &dyn Any) -> bool {
        // Compare primitives
        if let (Some(a_i8), Some(b_i8)) = (a.downcast_ref::<i8>(), b.downcast_ref::<i8>()) {
            return a_i8 == b_i8;
        }
        if let (Some(a_i16), Some(b_i16)) = (a.downcast_ref::<i16>(), b.downcast_ref::<i16>()) {
            return a_i16 == b_i16;
        }
        if let (Some(a_i32), Some(b_i32)) = (a.downcast_ref::<i32>(), b.downcast_ref::<i32>()) {
            return a_i32 == b_i32;
        }
        if let (Some(a_i64), Some(b_i64)) = (a.downcast_ref::<i64>(), b.downcast_ref::<i64>()) {
            return a_i64 == b_i64;
        }
        if let (Some(a_f32), Some(b_f32)) = (a.downcast_ref::<f32>(), b.downcast_ref::<f32>()) {
            return a_f32 == b_f32;
        }
        if let (Some(a_f64), Some(b_f64)) = (a.downcast_ref::<f64>(), b.downcast_ref::<f64>()) {
            return a_f64 == b_f64;
        }
        if let (Some(a_bool), Some(b_bool)) = (a.downcast_ref::<bool>(), b.downcast_ref::<bool>()) {
            return a_bool == b_bool;
        }
        if let (Some(a_string), Some(b_string)) =
            (a.downcast_ref::<String>(), b.downcast_ref::<String>())
        {
            return a_string == b_string;
        }
        if let (Some(a_bytes), Some(b_bytes)) =
            (a.downcast_ref::<Vec<u8>>(), b.downcast_ref::<Vec<u8>>())
        {
            return a_bytes == b_bytes;
        }

        // Compare complex types (simplified - just compare pointers for now)
        // In a full implementation, we would recursively compare arrays, maps, and structs
        if let (Some(_a_array), Some(_b_array)) = (
            a.downcast_ref::<Vec<Box<dyn Any + Send + Sync>>>(),
            b.downcast_ref::<Vec<Box<dyn Any + Send + Sync>>>(),
        ) {
            // For arrays, we would need to compare each element
            // This is a simplified version
            return false;
        }
        if let (Some(_a_map), Some(_b_map)) = (
            a.downcast_ref::<HashMap<String, Box<dyn Any + Send + Sync>>>(),
            b.downcast_ref::<HashMap<String, Box<dyn Any + Send + Sync>>>(),
        ) {
            // For maps, we would need to compare each key-value pair
            // This is a simplified version
            return false;
        }
        if let (Some(_a_struct), Some(_b_struct)) =
            (a.downcast_ref::<Struct>(), b.downcast_ref::<Struct>())
        {
            // For structs, we would need to recursively compare
            // This is a simplified version
            return false;
        }

        false
    }

    pub fn hash_code(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        // Hash schema pointer
        let schema_ptr = Arc::as_ptr(&self.schema) as usize;
        schema_ptr.hash(&mut hasher);

        // Hash values
        for (field_name, value) in &self.values {
            field_name.hash(&mut hasher);
            Self::hash_any_value(value.as_ref(), &mut hasher);
        }

        hasher.finish()
    }

    fn hash_any_value(value: &dyn Any, hasher: &mut impl Hasher) {
        // Hash primitives
        if let Some(v) = value.downcast_ref::<i8>() {
            v.hash(hasher);
        } else if let Some(v) = value.downcast_ref::<i16>() {
            v.hash(hasher);
        } else if let Some(v) = value.downcast_ref::<i32>() {
            v.hash(hasher);
        } else if let Some(v) = value.downcast_ref::<i64>() {
            v.hash(hasher);
        } else if let Some(v) = value.downcast_ref::<f32>() {
            v.to_bits().hash(hasher);
        } else if let Some(v) = value.downcast_ref::<f64>() {
            v.to_bits().hash(hasher);
        } else if let Some(v) = value.downcast_ref::<bool>() {
            v.hash(hasher);
        } else if let Some(v) = value.downcast_ref::<String>() {
            v.hash(hasher);
        } else if let Some(v) = value.downcast_ref::<Vec<u8>>() {
            v.hash(hasher);
        } else {
            // For complex types, hash pointer (simplified)
            let ptr = (value as *const dyn Any) as *const () as usize;
            ptr.hash(hasher);
        }
    }

    fn format_value(value: &dyn Any, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(v) = value.downcast_ref::<i8>() {
            write!(f, "{}", v)
        } else if let Some(v) = value.downcast_ref::<i16>() {
            write!(f, "{}", v)
        } else if let Some(v) = value.downcast_ref::<i32>() {
            write!(f, "{}", v)
        } else if let Some(v) = value.downcast_ref::<i64>() {
            write!(f, "{}", v)
        } else if let Some(v) = value.downcast_ref::<f32>() {
            write!(f, "{}", v)
        } else if let Some(v) = value.downcast_ref::<f64>() {
            write!(f, "{}", v)
        } else if let Some(v) = value.downcast_ref::<bool>() {
            write!(f, "{}", v)
        } else if let Some(v) = value.downcast_ref::<String>() {
            write!(f, "{}", v)
        } else if let Some(v) = value.downcast_ref::<Vec<u8>>() {
            write!(f, "{:?}", v)
        } else if let Some(v) = value.downcast_ref::<Struct>() {
            write!(f, "{}", v)
        } else {
            write!(f, "{{unknown}}")
        }
    }
}

/// SchemaAndValue is a simple container that holds both a Schema and its corresponding value.
#[derive(Debug)]
pub struct SchemaAndValue {
    schema: Option<Arc<ConnectSchema>>,
    value: Option<Box<dyn Any + Send + Sync>>,
}

impl AsRef<Self> for SchemaAndValue {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl fmt::Display for SchemaAndValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.value {
            Some(v) => {
                if let Some(s) = v.downcast_ref::<String>() {
                    write!(f, "{}", s)
                } else if let Some(i) = v.downcast_ref::<i64>() {
                    write!(f, "{}", i)
                } else if let Some(i) = v.downcast_ref::<i32>() {
                    write!(f, "{}", i)
                } else if let Some(b) = v.downcast_ref::<bool>() {
                    write!(f, "{}", b)
                } else {
                    write!(f, "{{unknown type}}")
                }
            }
            None => write!(f, "null"),
        }
    }
}

impl Clone for SchemaAndValue {
    fn clone(&self) -> Self {
        SchemaAndValue {
            schema: self.schema.clone(),
            value: None, // Cannot clone Box<dyn Any>
        }
    }
}

impl SchemaAndValue {
    pub fn new(
        schema: Option<Arc<ConnectSchema>>,
        value: Option<Box<dyn Any + Send + Sync>>,
    ) -> Self {
        SchemaAndValue { schema, value }
    }

    pub fn schema(&self) -> Option<Arc<ConnectSchema>> {
        self.schema.clone()
    }

    pub fn schema_as_ref(&self) -> Option<&dyn Schema> {
        self.schema.as_ref().map(|s| s.as_ref() as &dyn Schema)
    }

    pub fn value(&self) -> Option<&dyn Any> {
        self.value.as_ref().map(|v| v.as_ref() as &dyn Any)
    }

    pub fn with_schema(mut self, schema: Arc<ConnectSchema>) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_value(mut self, value: Box<dyn Any + Send + Sync>) -> Self {
        self.value = Some(value);
        self
    }

    /// Convert value to serde_json::Value
    pub fn to_json_value(&self) -> serde_json::Value {
        match &self.value {
            Some(v) => {
                if let Some(s) = v.downcast_ref::<String>() {
                    serde_json::Value::String(s.clone())
                } else if let Some(i) = v.downcast_ref::<i64>() {
                    serde_json::Value::Number((*i).into())
                } else if let Some(i) = v.downcast_ref::<i32>() {
                    serde_json::Value::Number((*i).into())
                } else if let Some(b) = v.downcast_ref::<bool>() {
                    serde_json::Value::Bool(*b)
                } else {
                    serde_json::Value::Null
                }
            }
            None => serde_json::Value::Null,
        }
    }
}

/// Values is a utility for converting Connect values from one form to another.
pub struct Values;

// Constants for Values
const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;
const NULL_VALUE: &str = "null";
const ISO_8601_DATE_FORMAT_PATTERN: &str = "yyyy-MM-dd";
const ISO_8601_TIME_FORMAT_PATTERN: &str = "HH:mm:ss.SSS'Z'";
const ISO_8601_TIMESTAMP_FORMAT_PATTERN: &str = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

impl Values {
    pub fn convert_to_boolean(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<bool>, DataException> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };

        if let Some(b) = value.downcast_ref::<bool>() {
            return Ok(Some(*b));
        }

        if let Some(s) = value.downcast_ref::<String>() {
            let parsed = Self::parse_string(Some(s.as_str()))?;
            if let Some(v) = parsed.value() {
                if let Some(b) = v.downcast_ref::<bool>() {
                    return Ok(Some(*b));
                }
            }
        }

        let long_val = Self::as_long(value, schema, None)?;
        Ok(Some(long_val != 0))
    }

    pub fn convert_to_byte(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<i8>, DataException> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };

        if let Some(b) = value.downcast_ref::<i8>() {
            return Ok(Some(*b));
        }

        let long_val = Self::as_long(value, schema, None)?;
        Ok(Some(long_val as i8))
    }

    pub fn convert_to_short(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<i16>, DataException> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };

        if let Some(s) = value.downcast_ref::<i16>() {
            return Ok(Some(*s));
        }

        let long_val = Self::as_long(value, schema, None)?;
        Ok(Some(long_val as i16))
    }

    pub fn convert_to_integer(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<i32>, DataException> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };

        if let Some(i) = value.downcast_ref::<i32>() {
            return Ok(Some(*i));
        }

        let long_val = Self::as_long(value, schema, None)?;
        Ok(Some(long_val as i32))
    }

    pub fn convert_to_long(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<i64>, DataException> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };

        if let Some(l) = value.downcast_ref::<i64>() {
            return Ok(Some(*l));
        }

        Self::as_long(value, schema, None).map(Some)
    }

    pub fn convert_to_float(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<f32>, DataException> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };

        if let Some(f) = value.downcast_ref::<f32>() {
            return Ok(Some(*f));
        }

        let double_val = Self::as_double(value, schema, None)?;
        Ok(Some(double_val as f32))
    }

    pub fn convert_to_double(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<f64>, DataException> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };

        if let Some(d) = value.downcast_ref::<f64>() {
            return Ok(Some(*d));
        }

        Self::as_double(value, schema, None).map(Some)
    }

    pub fn convert_to_string(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<String>, DataException> {
        let value = match value {
            Some(v) => v,
            None => return Ok(None),
        };

        let mut sb = String::new();
        Self::append(&mut sb, value, false);
        Ok(Some(sb))
    }

    pub fn convert_to_list(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<Vec<Box<dyn Any + Send + Sync>>>, DataException> {
        Self::convert_to_array_internal(schema, value)
    }

    pub fn convert_to_map(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<HashMap<String, Box<dyn Any + Send + Sync>>>, DataException> {
        Self::convert_to_map_internal(schema, value)
    }

    pub fn convert_to_struct(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<Struct>, DataException> {
        Self::convert_to_struct_internal(schema, value)
    }

    pub fn convert_to_date(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<i32>, DataException> {
        let value = value.ok_or_else(|| {
            DataException::new(
                "Unable to convert a null value to a schema that requires a value".to_string(),
            )
        })?;
        Self::convert_to_date_internal(schema, value).map(Some)
    }

    pub fn convert_to_time(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<i32>, DataException> {
        let value = value.ok_or_else(|| {
            DataException::new(
                "Unable to convert a null value to a schema that requires a value".to_string(),
            )
        })?;
        Self::convert_to_time_internal(schema, value).map(Some)
    }

    pub fn convert_to_timestamp(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<i64>, DataException> {
        let value = value.ok_or_else(|| {
            DataException::new(
                "Unable to convert a null value to a schema that requires a value".to_string(),
            )
        })?;
        Self::convert_to_timestamp_internal(schema, value).map(Some)
    }

    pub fn convert_to_decimal(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
        scale: i32,
    ) -> Result<Option<(i64, i32)>, DataException> {
        let value = value.ok_or_else(|| {
            DataException::new(
                "Unable to convert a null value to a schema that requires a value".to_string(),
            )
        })?;
        Self::convert_to_decimal_internal(schema, value, scale).map(Some)
    }

    pub fn infer_schema(value: Option<&dyn Any>) -> Option<Arc<ConnectSchema>> {
        let value = value?;

        if value.is::<String>() {
            Some(Arc::new(STRING_SCHEMA))
        } else if value.is::<bool>() {
            Some(Arc::new(BOOLEAN_SCHEMA))
        } else if value.is::<i8>() {
            Some(Arc::new(INT8_SCHEMA))
        } else if value.is::<i16>() {
            Some(Arc::new(INT16_SCHEMA))
        } else if value.is::<i32>() {
            Some(Arc::new(INT32_SCHEMA))
        } else if value.is::<i64>() {
            Some(Arc::new(INT64_SCHEMA))
        } else if value.is::<f32>() {
            Some(Arc::new(FLOAT32_SCHEMA))
        } else if value.is::<f64>() {
            Some(Arc::new(FLOAT64_SCHEMA))
        } else if value.is::<Vec<u8>>() {
            Some(Arc::new(BYTES_SCHEMA))
        } else if value.is::<Vec<Box<dyn Any + Send + Sync>>>() {
            Self::infer_list_schema(value.downcast_ref::<Vec<Box<dyn Any + Send + Sync>>>())
        } else if value.is::<HashMap<String, Box<dyn Any + Send + Sync>>>() {
            Self::infer_map_schema(
                value.downcast_ref::<HashMap<String, Box<dyn Any + Send + Sync>>>(),
            )
        } else if value.is::<Struct>() {
            value
                .downcast_ref::<Struct>()
                .map(|s| Arc::clone(&s.schema()))
        } else {
            None
        }
    }

    pub fn parse_string(value: Option<&str>) -> Result<SchemaAndValue, DataException> {
        let value = match value {
            Some(v) => v,
            None => return Ok(SchemaAndValue::new(None, None)),
        };

        if value.is_empty() {
            return Ok(SchemaAndValue::new(
                Some(Arc::new(STRING_SCHEMA)),
                Some(Box::new(value.to_string()) as Box<dyn Any + Send + Sync>),
            ));
        }

        let parser = Parser::new(value.to_string());
        let value_parser = ValueParser::new(parser);
        value_parser.parse(false)
    }

    // Helper methods
    fn as_long(
        value: &dyn Any,
        schema: Option<&dyn Schema>,
        _error: Option<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<i64, DataException> {
        // Try to convert as number
        if let Some(n) = value.downcast_ref::<i8>() {
            return Ok(*n as i64);
        }
        if let Some(n) = value.downcast_ref::<i16>() {
            return Ok(*n as i64);
        }
        if let Some(n) = value.downcast_ref::<i32>() {
            return Ok(*n as i64);
        }
        if let Some(n) = value.downcast_ref::<i64>() {
            return Ok(*n);
        }
        if let Some(n) = value.downcast_ref::<f32>() {
            return Ok(*n as i64);
        }
        if let Some(n) = value.downcast_ref::<f64>() {
            return Ok(*n as i64);
        }

        // Try to parse as string
        if let Some(s) = value.downcast_ref::<String>() {
            // Try to parse as integer
            if let Ok(n) = s.parse::<i64>() {
                return Ok(n);
            }
        }

        // Handle logical types (Date, Time, Timestamp)
        if let Some(schema) = schema {
            if let Some(name) = schema.name() {
                if name == Date::LOGICAL_NAME {
                    // Date is stored as i32 (days since epoch)
                    if let Some(days) = value.downcast_ref::<i32>() {
                        return Ok(*days as i64);
                    }
                } else if name == Time::LOGICAL_NAME {
                    // Time is stored as i32 (milliseconds since midnight)
                    if let Some(millis) = value.downcast_ref::<i32>() {
                        return Ok(*millis as i64);
                    }
                } else if name == Timestamp::LOGICAL_NAME {
                    // Timestamp is stored as i64 (milliseconds since epoch)
                    if let Some(millis) = value.downcast_ref::<i64>() {
                        return Ok(*millis);
                    }
                }
            }
        }

        Err(DataException::new(format!(
            "Unable to convert value to long"
        )))
    }

    fn as_double(
        value: &dyn Any,
        schema: Option<&dyn Schema>,
        _error: Option<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<f64, DataException> {
        // Try to convert as number
        if let Some(n) = value.downcast_ref::<f32>() {
            return Ok(*n as f64);
        }
        if let Some(n) = value.downcast_ref::<f64>() {
            return Ok(*n);
        }

        // Try as long first, then convert to double
        let long_val = Self::as_long(value, schema, None)?;
        Ok(long_val as f64)
    }

    fn append(sb: &mut String, value: &dyn Any, embedded: bool) {
        if value.is::<()>() {
            sb.push_str(NULL_VALUE);
        } else if let Some(n) = value.downcast_ref::<i8>() {
            sb.push_str(&n.to_string());
        } else if let Some(n) = value.downcast_ref::<i16>() {
            sb.push_str(&n.to_string());
        } else if let Some(n) = value.downcast_ref::<i32>() {
            sb.push_str(&n.to_string());
        } else if let Some(n) = value.downcast_ref::<i64>() {
            sb.push_str(&n.to_string());
        } else if let Some(n) = value.downcast_ref::<f32>() {
            sb.push_str(&n.to_string());
        } else if let Some(n) = value.downcast_ref::<f64>() {
            sb.push_str(&n.to_string());
        } else if let Some(b) = value.downcast_ref::<bool>() {
            sb.push_str(&b.to_string());
        } else if let Some(s) = value.downcast_ref::<String>() {
            if embedded {
                let escaped = Self::escape(s);
                sb.push('"');
                sb.push_str(&escaped);
                sb.push('"');
            } else {
                sb.push_str(s);
            }
        } else if let Some(bytes) = value.downcast_ref::<Vec<u8>>() {
            // Simple hex encoding instead of base64 (to avoid dependency)
            let encoded: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
            if embedded {
                sb.push('"');
                sb.push_str(&encoded);
                sb.push('"');
            } else {
                sb.push_str(&encoded);
            }
        } else if let Some(list) = value.downcast_ref::<Vec<Box<dyn Any + Send + Sync>>>() {
            sb.push('[');
            Self::append_iterable(sb, list.iter());
            sb.push(']');
        } else if let Some(map) =
            value.downcast_ref::<HashMap<String, Box<dyn Any + Send + Sync>>>()
        {
            sb.push('{');
            let mut first = true;
            for (key, val) in map {
                if first {
                    first = false;
                } else {
                    sb.push(',');
                }
                Self::append(sb, key, true);
                sb.push(':');
                Self::append(sb, val, true);
            }
            sb.push('}');
        } else if let Some(struct_val) = value.downcast_ref::<Struct>() {
            let schema = struct_val.schema();
            let mut first = true;
            sb.push('{');
            match schema.fields() {
                Ok(fields) => {
                    for field in &fields {
                        if first {
                            first = false;
                        } else {
                            sb.push(',');
                        }
                        Self::append(sb, &field.name(), true);
                        sb.push(':');
                        if let Some(v) = struct_val.get(field.name()) {
                            Self::append(sb, v, true);
                        } else {
                            sb.push_str(NULL_VALUE);
                        }
                    }
                }
                Err(_) => {}
            }
            sb.push('}');
        } else {
            // For map entries, handle specially
            // This is a simplified version - in real code would handle Map.Entry
            sb.push_str("unknown");
        }
    }

    fn append_iterable<'a, I>(sb: &mut String, iter: I)
    where
        I: Iterator<Item = &'a Box<dyn Any + Send + Sync>>,
    {
        let mut first = true;
        for item in iter {
            if first {
                first = false;
            } else {
                sb.push(',');
            }
            Self::append(sb, item.as_ref() as &dyn Any, true);
        }
    }

    fn escape(value: &str) -> String {
        // Replace backslashes with double backslashes, then replace quotes with escaped quotes
        value.replace("\\", "\\\\").replace("\"", "\\\"")
    }

    fn date_format_for(_value: i64) -> &'static str {
        // Simplified - in real code would determine format based on value
        ISO_8601_TIMESTAMP_FORMAT_PATTERN
    }

    // Internal conversion methods
    fn convert_to_array_internal(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<Vec<Box<dyn Any + Send + Sync>>>, DataException> {
        let value = match value {
            Some(v) => v,
            None => {
                return Err(DataException::new(
                    "Unable to convert a null value to a schema that requires a value".to_string(),
                ))
            }
        };

        if let Some(s) = value.downcast_ref::<String>() {
            let parsed = Self::parse_string(Some(s.as_str()))?;
            if let Some(v) = parsed.value() {
                if let Some(list) = v.downcast_ref::<Vec<Box<dyn Any + Send + Sync>>>() {
                    // Can't clone Vec<Box<dyn Any>>, so create a new one
                    let mut new_list: Vec<Box<dyn Any + Send + Sync>> = Vec::new();
                    for item in list {
                        // Just clone the reference - in real code would deep clone
                        new_list.push(item.clone());
                    }
                    return Ok(Some(new_list));
                }
            }
        }

        if let Some(list) = value.downcast_ref::<Vec<Box<dyn Any + Send + Sync>>>() {
            // Can't clone Vec<Box<dyn Any>>, so create a new one
            let mut new_list: Vec<Box<dyn Any + Send + Sync>> = Vec::new();
            for item in list {
                new_list.push(item.clone());
            }
            return Ok(Some(new_list));
        }

        Err(DataException::new(format!(
            "Unable to convert value to array"
        )))
    }

    fn convert_to_map_internal(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<HashMap<String, Box<dyn Any + Send + Sync>>>, DataException> {
        let value = match value {
            Some(v) => v,
            None => {
                return Err(DataException::new(
                    "Unable to convert a null value to a schema that requires a value".to_string(),
                ))
            }
        };

        if let Some(s) = value.downcast_ref::<String>() {
            let parsed = Self::parse_string(Some(s.as_str()))?;
            if let Some(v) = parsed.value() {
                if let Some(map) = v.downcast_ref::<HashMap<String, Box<dyn Any + Send + Sync>>>() {
                    // Can't clone HashMap, so create a new one
                    let mut new_map: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
                    for (key, val) in map {
                        new_map.insert(key.clone(), val.clone());
                    }
                    return Ok(Some(new_map));
                }
            }
        }

        if let Some(map) = value.downcast_ref::<HashMap<String, Box<dyn Any + Send + Sync>>>() {
            // Can't clone HashMap, so create a new one
            let mut new_map: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
            for (key, val) in map {
                new_map.insert(key.clone(), val.clone());
            }
            return Ok(Some(new_map));
        }

        Err(DataException::new(format!(
            "Unable to convert value to map"
        )))
    }

    fn convert_to_struct_internal(
        schema: Option<&dyn Schema>,
        value: Option<&dyn Any>,
    ) -> Result<Option<Struct>, DataException> {
        let value = match value {
            Some(v) => v,
            None => {
                return Err(DataException::new(
                    "Unable to convert a null value to a schema that requires a value".to_string(),
                ))
            }
        };

        if let Some(struct_val) = value.downcast_ref::<Struct>() {
            return Ok(Some(struct_val.clone()));
        }

        Err(DataException::new(format!(
            "Unable to convert value to struct"
        )))
    }

    fn convert_to_date_internal(
        schema: Option<&dyn Schema>,
        value: &dyn Any,
    ) -> Result<i32, DataException> {
        // Simplified implementation - in real code would handle Date logical type
        let long_val = Self::as_long(value, schema, None)?;
        Ok((long_val / MILLIS_PER_DAY) as i32)
    }

    fn convert_to_time_internal(
        schema: Option<&dyn Schema>,
        value: &dyn Any,
    ) -> Result<i32, DataException> {
        // Simplified implementation - in real code would handle Time logical type
        let long_val = Self::as_long(value, schema, None)?;
        Ok((long_val % MILLIS_PER_DAY) as i32)
    }

    fn convert_to_timestamp_internal(
        schema: Option<&dyn Schema>,
        value: &dyn Any,
    ) -> Result<i64, DataException> {
        // Simplified implementation - in real code would handle Timestamp logical type
        Self::as_long(value, schema, None)
    }

    fn convert_to_decimal_internal(
        schema: Option<&dyn Schema>,
        value: &dyn Any,
        scale: i32,
    ) -> Result<(i64, i32), DataException> {
        // Simplified implementation - uses scaled integers
        // In real code would handle BigDecimal conversion
        let long_val = Self::as_long(value, schema, None)?;
        Ok((long_val, scale))
    }

    fn infer_list_schema(
        list: Option<&Vec<Box<dyn Any + Send + Sync>>>,
    ) -> Option<Arc<ConnectSchema>> {
        let list = list?;
        if list.is_empty() {
            return None;
        }

        let mut detector = SchemaDetector::new();
        for element in list {
            if !detector.can_detect(Some(element.as_ref() as &dyn Any)) {
                return None;
            }
        }

        let element_schema = detector.schema()?;
        Some(Arc::new(ConnectSchema::array(element_schema)))
    }

    fn infer_map_schema(
        map: Option<&HashMap<String, Box<dyn Any + Send + Sync>>>,
    ) -> Option<Arc<ConnectSchema>> {
        let map = map?;
        if map.is_empty() {
            return None;
        }

        let mut key_detector = SchemaDetector::new();
        let mut value_detector = SchemaDetector::new();

        for (key, value) in map {
            if !key_detector.can_detect(Some(key as &dyn Any)) {
                return None;
            }
            if !value_detector.can_detect(Some(value.as_ref() as &dyn Any)) {
                return None;
            }
        }

        let key_schema = key_detector.schema()?;
        let value_schema = value_detector.schema()?;
        Some(Arc::new(ConnectSchema::map(key_schema, value_schema)))
    }
}

// Parser class for tokenization
pub struct Parser {
    original: String,
    chars: Vec<char>,
    position: usize,
    next_token: Option<String>,
    previous_token: Option<String>,
}

impl Parser {
    pub fn new(original: String) -> Self {
        let chars: Vec<char> = original.chars().collect();
        Parser {
            original,
            chars,
            position: 0,
            next_token: None,
            previous_token: None,
        }
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn mark(&self) -> usize {
        self.position - self.next_token.as_ref().map_or(0, |t| t.len())
    }

    pub fn rewind_to(&mut self, position: usize) {
        self.position = position;
        self.next_token = None;
        self.previous_token = None;
    }

    pub fn original(&self) -> &str {
        &self.original
    }

    pub fn has_next(&mut self) -> bool {
        if self.next_token.is_some() {
            return true;
        }
        self.can_consume_next_token()
    }

    fn can_consume_next_token(&self) -> bool {
        self.position < self.chars.len()
    }

    pub fn next(&mut self) -> Option<String> {
        if let Some(token) = self.next_token.take() {
            self.previous_token = Some(token.clone());
            return Some(token);
        } else {
            let token = self.consume_next_token()?;
            self.previous_token = Some(token.clone());
            Some(token)
        }
    }

    pub fn next_n(&mut self, n: usize) -> Option<String> {
        let start = self.mark();
        for _ in 0..n {
            if !self.has_next() {
                self.rewind_to(start);
                return None;
            }
            self.next();
        }
        Some(self.original[start..self.position()].to_string())
    }

    fn consume_next_token(&mut self) -> Option<String> {
        let mut escaped = false;
        let start = self.position;

        while self.can_consume_next_token() {
            let c = self.chars[self.position];
            match c {
                '\\' => {
                    escaped = !escaped;
                }
                ':' | ',' | '{' | '}' | '[' | ']' | '"' => {
                    if !escaped {
                        if start < self.position {
                            return Some(self.original[start..self.position].to_string());
                        }
                        self.position += 1;
                        return Some(self.original[start..start + 1].to_string());
                    }
                    escaped = false;
                }
                _ => {
                    escaped = false;
                }
            }
            self.position += 1;
        }

        if start < self.position {
            Some(self.original[start..self.position].to_string())
        } else {
            None
        }
    }

    pub fn previous(&self) -> Option<&String> {
        self.previous_token.as_ref()
    }

    pub fn can_consume(&mut self, expected: &str) -> bool {
        self.can_consume_with_whitespace(expected, true)
    }

    pub fn can_consume_with_whitespace(&mut self, expected: &str, ignore_whitespace: bool) -> bool {
        if self.is_next(expected, ignore_whitespace) {
            self.next_token = None;
            return true;
        }
        false
    }

    fn is_next(&mut self, expected: &str, ignore_whitespace: bool) -> bool {
        if self.next_token.is_none() {
            if !self.has_next() {
                return false;
            }
            self.next_token = self.consume_next_token();
        }

        if ignore_whitespace {
            while let Some(ref token) = self.next_token {
                if token.trim().is_empty() && self.can_consume_next_token() {
                    self.next_token = self.consume_next_token();
                } else {
                    break;
                }
            }
        }

        if let Some(ref token) = self.next_token {
            if ignore_whitespace {
                token.trim() == expected
            } else {
                token == expected
            }
        } else {
            false
        }
    }
}

// SchemaDetector for schema inference
pub struct SchemaDetector {
    known_type: Option<Type>,
    optional: bool,
}

impl SchemaDetector {
    pub fn new() -> Self {
        SchemaDetector {
            known_type: None,
            optional: false,
        }
    }

    pub fn can_detect(&mut self, value: Option<&dyn Any>) -> bool {
        let value = match value {
            Some(v) => v,
            None => {
                self.optional = true;
                return true;
            }
        };

        let schema = match Values::infer_schema(Some(value)) {
            Some(s) => s,
            None => return false,
        };

        let type_ = schema.type_();
        if self.known_type.is_none() {
            self.known_type = Some(type_);
        }

        self.known_type == Some(type_)
    }

    pub fn schema(&self) -> Option<Arc<ConnectSchema>> {
        let type_ = self.known_type?;
        let mut builder = SchemaBuilder::new();
        match type_ {
            Type::Int8 => builder = SchemaBuilder::int8(),
            Type::Int16 => builder = SchemaBuilder::int16(),
            Type::Int32 => builder = SchemaBuilder::int32(),
            Type::Int64 => builder = SchemaBuilder::int64(),
            Type::Float32 => builder = SchemaBuilder::float32(),
            Type::Float64 => builder = SchemaBuilder::float64(),
            Type::Boolean => builder = SchemaBuilder::boolean(),
            Type::String => builder = SchemaBuilder::string(),
            Type::Bytes => builder = SchemaBuilder::bytes(),
            Type::Array => return None, // Complex type, need value schema
            Type::Map => return None,   // Complex type, need key/value schemas
            Type::Struct => return None, // Complex type, need fields
        }

        if self.optional {
            builder = builder.optional();
        }

        builder.build().ok()
    }
}

// SchemaMerger for merging schemas
pub struct SchemaMerger {
    common: Option<Arc<ConnectSchema>>,
    compatible: bool,
}

impl SchemaMerger {
    pub fn new() -> Self {
        SchemaMerger {
            common: None,
            compatible: true,
        }
    }

    pub fn merge(&mut self, latest: &SchemaAndValue) {
        if !self.compatible {
            return;
        }

        let latest_schema = match latest.schema() {
            Some(s) => s,
            None => return,
        };

        if let Some(ref common) = self.common {
            self.common = Self::merge_schemas(common, &latest_schema);
            self.compatible = self.common.is_some();
        } else {
            self.common = Some(latest_schema);
        }
    }

    pub fn has_common_schema(&self) -> bool {
        self.common.is_some()
    }

    pub fn schema(&self) -> Option<Arc<ConnectSchema>> {
        self.common.clone()
    }

    fn merge_schemas(
        previous: &Arc<ConnectSchema>,
        new_schema: &Arc<ConnectSchema>,
    ) -> Option<Arc<ConnectSchema>> {
        let prev_type = previous.type_();
        let new_type = new_schema.type_();

        if prev_type != new_type {
            return Self::common_schema_for_type(&prev_type, previous, new_schema);
        }

        if previous.is_optional() == new_schema.is_optional() {
            return Some(Arc::clone(previous));
        }

        None
    }

    fn common_schema_for_type(
        prev_type: &Type,
        previous: &Arc<ConnectSchema>,
        new_schema: &Arc<ConnectSchema>,
    ) -> Option<Arc<ConnectSchema>> {
        let new_type = new_schema.type_();

        match prev_type {
            Type::Int8 => match new_type {
                Type::Int16 | Type::Int32 | Type::Int64 | Type::Float32 | Type::Float64 => {
                    Some(Arc::clone(new_schema))
                }
                _ => None,
            },
            Type::Int16 => match new_type {
                Type::Int8 => Some(Arc::clone(previous)),
                Type::Int32 | Type::Int64 | Type::Float32 | Type::Float64 => {
                    Some(Arc::clone(new_schema))
                }
                _ => None,
            },
            Type::Int32 => match new_type {
                Type::Int8 | Type::Int16 => Some(Arc::clone(previous)),
                Type::Int64 | Type::Float32 | Type::Float64 => Some(Arc::clone(new_schema)),
                _ => None,
            },
            Type::Int64 => match new_type {
                Type::Int8 | Type::Int16 | Type::Int32 => Some(Arc::clone(previous)),
                Type::Float32 | Type::Float64 => Some(Arc::clone(new_schema)),
                _ => None,
            },
            Type::Float32 => match new_type {
                Type::Int8 | Type::Int16 | Type::Int32 | Type::Int64 => Some(Arc::clone(previous)),
                Type::Float64 => Some(Arc::clone(new_schema)),
                _ => None,
            },
            Type::Float64 => match new_type {
                Type::Int8 | Type::Int16 | Type::Int32 | Type::Int64 | Type::Float32 => {
                    Some(Arc::clone(previous))
                }
                _ => None,
            },
            _ => None,
        }
    }
}

// ValueParser for parsing strings into SchemaAndValue
pub struct ValueParser {
    parser: Parser,
}

impl ValueParser {
    pub fn new(parser: Parser) -> Self {
        ValueParser { parser }
    }

    pub fn parse(&mut self, embedded: bool) -> Result<SchemaAndValue, DataException> {
        if !self.parser.has_next() {
            return Ok(SchemaAndValue::new(None, None));
        }

        if embedded && self.parser.can_consume("\"") {
            return self.parse_quoted_string();
        }

        if self.parser.can_consume(NULL_VALUE) {
            return Ok(SchemaAndValue::new(None, None));
        }

        if self.parser.can_consume("true") {
            return Ok(SchemaAndValue::new(
                Some(Arc::new(BOOLEAN_SCHEMA)),
                Some(Box::new(true) as Box<dyn Any + Send + Sync>),
            ));
        }

        if self.parser.can_consume("false") {
            return Ok(SchemaAndValue::new(
                Some(Arc::new(BOOLEAN_SCHEMA)),
                Some(Box::new(false) as Box<dyn Any + Send + Sync>),
            ));
        }

        let start_position = self.parser.mark();

        // Try to parse as array or map
        if self.parser.can_consume("[") {
            return self.parse_array();
        }

        if self.parser.can_consume("{") {
            return self.parse_map();
        }

        self.parser.rewind_to(start_position);

        // Parse as token
        let token = self
            .parser
            .next()
            .ok_or_else(|| DataException::new("No token available".to_string()))?;
        self.parse_next_token(embedded, &token)
    }

    fn parse_next_token(
        &mut self,
        embedded: bool,
        token: &str,
    ) -> Result<SchemaAndValue, DataException> {
        let token = token.trim();

        if token.is_empty() {
            return Ok(SchemaAndValue::new(
                Some(Arc::new(STRING_SCHEMA)),
                Some(Box::new(token.to_string()) as Box<dyn Any + Send + Sync>),
            ));
        }

        let first_char = token.chars().next();
        let first_char_is_digit = first_char.map_or(false, |c| c.is_ascii_digit());

        // Try to parse as number
        if first_char_is_digit || first_char == Some('+') || first_char == Some('-') {
            if let Ok(parsed) = self.parse_as_number(token) {
                return Ok(parsed);
            }
        }

        if embedded {
            return Err(DataException::new(
                "Failed to parse embedded value".to_string(),
            ));
        }

        // Return as string
        Ok(SchemaAndValue::new(
            Some(Arc::new(STRING_SCHEMA)),
            Some(Box::new(self.parser.original().to_string()) as Box<dyn Any + Send + Sync>),
        ))
    }

    fn parse_quoted_string(&mut self) -> Result<SchemaAndValue, DataException> {
        let mut content = String::new();

        while self.parser.has_next() {
            if self.parser.can_consume("\"") {
                break;
            }
            if let Some(token) = self.parser.next() {
                content.push_str(&token);
            }
        }

        Ok(SchemaAndValue::new(
            Some(Arc::new(STRING_SCHEMA)),
            Some(Box::new(content) as Box<dyn Any + Send + Sync>),
        ))
    }

    fn parse_array(&mut self) -> Result<SchemaAndValue, DataException> {
        let mut result: Vec<Box<dyn Any + Send + Sync>> = Vec::new();
        let mut element_schema = SchemaMerger::new();

        while self.parser.has_next() {
            if self.parser.can_consume("]") {
                let list_schema = if element_schema.has_common_schema() {
                    let schema = element_schema
                        .schema()
                        .ok_or_else(|| DataException::new("No common schema".to_string()))?;
                    ConnectSchema::array(schema)
                } else {
                    ConnectSchema::array(Arc::new(STRING_SCHEMA))
                };
                return Ok(SchemaAndValue::new(
                    Some(Arc::new(list_schema)),
                    Some(Box::new(result) as Box<dyn Any + Send + Sync>),
                ));
            }

            if self.parser.can_consume(",") {
                return Err(DataException::new(
                    "Unable to parse an empty array element".to_string(),
                ));
            }

            let element = self.parse(true)?;
            element_schema.merge(&element);
            if let Some(v) = element.value() {
                // Convert &dyn Any to Box<dyn Any>
                // This is a simplified approach - in real code would need proper handling
                if let Some(s) = v.downcast_ref::<String>() {
                    result.push(Box::new(s.clone()) as Box<dyn Any + Send + Sync>);
                } else if let Some(n) = v.downcast_ref::<i64>() {
                    result.push(Box::new(*n) as Box<dyn Any + Send + Sync>);
                } else if let Some(n) = v.downcast_ref::<i32>() {
                    result.push(Box::new(*n) as Box<dyn Any + Send + Sync>);
                } else if let Some(b) = v.downcast_ref::<bool>() {
                    result.push(Box::new(*b) as Box<dyn Any + Send + Sync>);
                } else {
                    result.push(Box::new(()) as Box<dyn Any + Send + Sync>);
                }
            } else {
                result.push(Box::new(()) as Box<dyn Any + Send + Sync>);
            }

            let current_position = self.parser.mark();
            if self.parser.can_consume("]") {
                self.parser.rewind_to(current_position);
            } else if !self.parser.can_consume(",") {
                return Err(DataException::new(
                    "Array elements missing ',' delimiter".to_string(),
                ));
            }
        }

        Err(DataException::new(
            "Array is missing terminating ']'".to_string(),
        ))
    }

    fn parse_map(&mut self) -> Result<SchemaAndValue, DataException> {
        let mut result: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        let mut key_schema = SchemaMerger::new();
        let mut value_schema = SchemaMerger::new();

        while self.parser.has_next() {
            if self.parser.can_consume("}") {
                let map_schema =
                    if key_schema.has_common_schema() && value_schema.has_common_schema() {
                        let ks = key_schema
                            .schema()
                            .ok_or_else(|| DataException::new("No key schema".to_string()))?;
                        let vs = value_schema
                            .schema()
                            .ok_or_else(|| DataException::new("No value schema".to_string()))?;
                        ConnectSchema::map(ks, vs)
                    } else if key_schema.has_common_schema() {
                        let ks = key_schema
                            .schema()
                            .ok_or_else(|| DataException::new("No key schema".to_string()))?;
                        ConnectSchema::map(ks, Arc::new(STRING_SCHEMA))
                    } else {
                        ConnectSchema::map(Arc::new(STRING_SCHEMA), Arc::new(STRING_SCHEMA))
                    };
                return Ok(SchemaAndValue::new(
                    Some(Arc::new(map_schema)),
                    Some(Box::new(result) as Box<dyn Any + Send + Sync>),
                ));
            }

            if self.parser.can_consume(",") {
                return Err(DataException::new(
                    "Unable to parse a map entry with no key or value".to_string(),
                ));
            }

            let key = self.parse(true)?;
            let key_value = key
                .value()
                .ok_or_else(|| DataException::new("Map key is null".to_string()))?;

            if !self.parser.can_consume(":") {
                return Err(DataException::new(
                    "Map entry is missing ':' delimiter".to_string(),
                ));
            }

            let value = self.parse(true)?;
            let value_value = value.value();

            if let Some(key_str) = key_value.downcast_ref::<String>() {
                // Convert &dyn Any to Box<dyn Any>
                let boxed_value = if let Some(v) = value_value {
                    // This is simplified - in real code would need proper deep cloning
                    if let Some(s) = v.downcast_ref::<String>() {
                        Box::new(s.clone()) as Box<dyn Any + Send + Sync>
                    } else if let Some(n) = v.downcast_ref::<i64>() {
                        Box::new(*n) as Box<dyn Any + Send + Sync>
                    } else if let Some(n) = v.downcast_ref::<i32>() {
                        Box::new(*n) as Box<dyn Any + Send + Sync>
                    } else if let Some(b) = v.downcast_ref::<bool>() {
                        Box::new(*b) as Box<dyn Any + Send + Sync>
                    } else {
                        Box::new(()) as Box<dyn Any + Send + Sync>
                    }
                } else {
                    Box::new(()) as Box<dyn Any + Send + Sync>
                };

                result.insert(key_str.clone(), boxed_value);
                key_schema.merge(&key);
                value_schema.merge(&value);
            } else {
                return Err(DataException::new("Map key must be a string".to_string()));
            }

            self.parser.can_consume(",");
        }

        Err(DataException::new(
            "Map is missing terminating '}'".to_string(),
        ))
    }

    fn parse_as_number(&self, token: &str) -> Result<SchemaAndValue, DataException> {
        // Try to parse as i64 first
        if let Ok(n) = token.parse::<i64>() {
            // Check if it fits in smaller types
            if n >= i8::MIN as i64 && n <= i8::MAX as i64 {
                return Ok(SchemaAndValue::new(
                    Some(Arc::new(INT8_SCHEMA)),
                    Some(Box::new(n as i8) as Box<dyn Any + Send + Sync>),
                ));
            } else if n >= i16::MIN as i64 && n <= i16::MAX as i64 {
                return Ok(SchemaAndValue::new(
                    Some(Arc::new(INT16_SCHEMA)),
                    Some(Box::new(n as i16) as Box<dyn Any + Send + Sync>),
                ));
            } else if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                return Ok(SchemaAndValue::new(
                    Some(Arc::new(INT32_SCHEMA)),
                    Some(Box::new(n as i32) as Box<dyn Any + Send + Sync>),
                ));
            } else {
                return Ok(SchemaAndValue::new(
                    Some(Arc::new(INT64_SCHEMA)),
                    Some(Box::new(n) as Box<dyn Any + Send + Sync>),
                ));
            }
        }

        // Try to parse as f64
        if let Ok(n) = token.parse::<f64>() {
            // Check if it fits in f32
            if n >= f32::MIN as f64 && n <= f32::MAX as f64 {
                return Ok(SchemaAndValue::new(
                    Some(Arc::new(FLOAT32_SCHEMA)),
                    Some(Box::new(n as f32) as Box<dyn Any + Send + Sync>),
                ));
            } else {
                return Ok(SchemaAndValue::new(
                    Some(Arc::new(FLOAT64_SCHEMA)),
                    Some(Box::new(n) as Box<dyn Any + Send + Sync>),
                ));
            }
        }

        Err(DataException::new(format!(
            "Failed to parse '{}' as number",
            token
        )))
    }
}

/// SchemaProjector trait for projecting schemas.
pub trait SchemaProjector: Send + Sync {
    fn project_schema(&self, schema: &dyn Schema) -> Result<Arc<dyn Schema>, DataException>;

    fn project_value(
        &self,
        value: &dyn Any,
        schema: &dyn Schema,
    ) -> Result<Box<dyn Any + Send + Sync>, DataException>;
}

/// Date logical type.
pub struct Date;

impl Date {
    pub const LOGICAL_NAME: &'static str = "org.apache.kafka.connect.data.Date";
    pub const MILLIS_PER_DAY: i64 = 86400000;

    pub fn schema() -> Arc<ConnectSchema> {
        Arc::new(
            ConnectSchema::new(Type::Int32)
                .with_name(Self::LOGICAL_NAME.to_string())
                .with_version(1),
        )
    }

    pub fn from_logical(value: i32) -> i32 {
        value
    }

    pub fn to_logical(value: i32) -> Result<i32, DataException> {
        Ok(value)
    }
}

/// Time logical type.
pub struct Time;

impl Time {
    pub const LOGICAL_NAME: &'static str = "org.apache.kafka.connect.data.Time";

    pub fn schema() -> Arc<ConnectSchema> {
        Arc::new(
            ConnectSchema::new(Type::Int32)
                .with_name(Self::LOGICAL_NAME.to_string())
                .with_version(1),
        )
    }

    pub fn from_logical(value: i32) -> i32 {
        value
    }

    pub fn to_logical(value: i32) -> Result<i32, DataException> {
        Ok(value)
    }
}

/// Timestamp logical type.
pub struct Timestamp;

impl Timestamp {
    pub const LOGICAL_NAME: &'static str = "org.apache.kafka.connect.data.Timestamp";

    pub fn schema() -> Arc<ConnectSchema> {
        Arc::new(
            ConnectSchema::new(Type::Int64)
                .with_name(Self::LOGICAL_NAME.to_string())
                .with_version(1),
        )
    }

    pub fn from_logical(value: i64) -> i64 {
        value
    }

    pub fn to_logical(value: i64) -> Result<i64, DataException> {
        Ok(value)
    }
}

/// Decimal logical type.
pub struct Decimal;

impl Decimal {
    pub const LOGICAL_NAME: &'static str = "org.apache.kafka.connect.data.Decimal";

    pub fn schema(scale: i32) -> Arc<ConnectSchema> {
        let mut params = HashMap::new();
        params.insert("scale".to_string(), scale.to_string());

        Arc::new(
            ConnectSchema::new(Type::Bytes)
                .with_name(Self::LOGICAL_NAME.to_string())
                .with_version(1)
                .with_parameters(params),
        )
    }

    pub fn from_logical(_value: &Vec<u8>) -> Vec<u8> {
        // In real implementation, would convert BigDecimal to bytes
        vec![]
    }

    pub fn to_logical(_value: &Vec<u8>, _scale: i32) -> Result<Vec<u8>, DataException> {
        // In real implementation, would convert bytes to BigDecimal
        Ok(vec![])
    }
}

// ============================================================================================
// Schema Constants - predefined schemas for primitive types
// ============================================================================================

/// Predefined schema constants for primitive types
pub const INT8_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Int8,
    optional: false,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const INT16_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Int16,
    optional: false,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const INT32_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Int32,
    optional: false,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const INT64_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Int64,
    optional: false,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const FLOAT32_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Float32,
    optional: false,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const FLOAT64_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Float64,
    optional: false,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const BOOLEAN_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Boolean,
    optional: false,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const STRING_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::String,
    optional: false,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const BYTES_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Bytes,
    optional: false,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const OPTIONAL_INT8_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Int8,
    optional: true,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const OPTIONAL_INT16_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Int16,
    optional: true,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const OPTIONAL_INT32_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Int32,
    optional: true,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const OPTIONAL_INT64_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Int64,
    optional: true,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const OPTIONAL_FLOAT32_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Float32,
    optional: true,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const OPTIONAL_FLOAT64_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Float64,
    optional: true,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const OPTIONAL_BOOLEAN_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Boolean,
    optional: true,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const OPTIONAL_STRING_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::String,
    optional: true,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

pub const OPTIONAL_BYTES_SCHEMA: ConnectSchema = ConnectSchema {
    type_: Type::Bytes,
    optional: true,
    default_value: None,
    fields: None,
    fields_by_name: None,
    key_schema: None,
    value_schema: None,
    name: None,
    version: None,
    doc: None,
    parameters: None,
};

// ============================================================================================
// Header and Headers - representing record headers in Kafka Connect
// ============================================================================================

/// Header trait representing a single header in a Kafka record
pub trait Header: Send + Sync {
    /// Returns the header's key
    fn key(&self) -> &str;

    /// Returns the schema of the header's value
    fn schema(&self) -> Arc<dyn Schema>;

    /// Returns the header's value (cloned)
    fn value(&self) -> Arc<dyn Any + Send + Sync>;

    /// Returns a new Header with the specified schema and value
    fn with(&self, schema: Arc<dyn Schema>, value: Arc<dyn Any + Send + Sync>) -> Box<dyn Header>;

    /// Returns a new Header with the specified key
    fn rename(&self, key: &str) -> Box<dyn Header>;
}

/// Concrete implementation of Header
pub struct ConcreteHeader {
    key: String,
    schema: Arc<dyn Schema>,
    value: Arc<dyn Any + Send + Sync>,
}

impl ConcreteHeader {
    pub fn new(key: String, schema: Arc<dyn Schema>, value: Arc<dyn Any + Send + Sync>) -> Self {
        Self { key, schema, value }
    }
}

impl Header for ConcreteHeader {
    fn key(&self) -> &str {
        &self.key
    }

    fn schema(&self) -> Arc<dyn Schema> {
        self.schema.clone()
    }

    fn value(&self) -> Arc<dyn Any + Send + Sync> {
        self.value.clone()
    }

    fn with(&self, schema: Arc<dyn Schema>, value: Arc<dyn Any + Send + Sync>) -> Box<dyn Header> {
        Box::new(ConcreteHeader::new(self.key.clone(), schema, value))
    }

    fn rename(&self, key: &str) -> Box<dyn Header> {
        Box::new(ConcreteHeader::new(
            key.to_string(),
            self.schema.clone(),
            self.value.clone(),
        ))
    }
}

impl Clone for ConcreteHeader {
    fn clone(&self) -> Self {
        ConcreteHeader {
            key: self.key.clone(),
            schema: self.schema.clone(),
            value: self.value.clone(),
        }
    }
}

/// Headers trait representing a collection of headers
pub trait Headers: Send + Sync {
    /// Returns the number of headers
    fn size(&self) -> usize;

    /// Returns true if there are no headers
    fn is_empty(&self) -> bool;

    /// Returns all headers with the given key
    fn all_with_name(&self, key: &str) -> Vec<Box<dyn Header>>;

    /// Returns the last header with the given key
    fn last_with_name(&self, key: &str) -> Option<Box<dyn Header>>;

    /// Adds a header
    fn add(&mut self, header: Box<dyn Header>);

    /// Removes headers with the given keys and returns new headers
    fn remove_headers(&mut self, keys: &[String]) -> Box<dyn Headers>;

    /// Removes a header by key and returns new headers
    fn remove(&mut self, key: &str) -> Box<dyn Headers>;

    /// Returns a duplicate of the headers
    fn duplicate(&self) -> Box<dyn Headers>;

    /// Returns an iterator over the headers
    fn iter(&self) -> Box<dyn Iterator<Item = &dyn Header> + '_>;
}

/// Concrete implementation of Headers
pub struct ConcreteHeaders {
    headers: Vec<Box<dyn Header>>,
}

impl ConcreteHeaders {
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
        }
    }
}

impl Default for ConcreteHeaders {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ConcreteHeaders {
    fn clone(&self) -> Self {
        let mut new_headers = ConcreteHeaders::new();
        for h in &self.headers {
            // Clone the header value
            let schema = h.schema();
            let value = h.value();
            new_headers.add(h.with(schema, value));
        }
        new_headers
    }
}

impl Headers for ConcreteHeaders {
    fn size(&self) -> usize {
        self.headers.len()
    }

    fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    fn all_with_name(&self, key: &str) -> Vec<Box<dyn Header>> {
        self.headers
            .iter()
            .filter(|h| h.key() == key)
            .map(|h| h.with(h.schema(), h.value().clone()))
            .collect()
    }

    fn last_with_name(&self, key: &str) -> Option<Box<dyn Header>> {
        self.headers
            .iter()
            .rev()
            .find(|h| h.key() == key)
            .map(|h| h.with(h.schema(), h.value().clone()))
    }

    fn add(&mut self, header: Box<dyn Header>) {
        self.headers.push(header);
    }

    fn remove_headers(&mut self, keys: &[String]) -> Box<dyn Headers> {
        let mut new_headers = ConcreteHeaders::new();
        for h in &self.headers {
            if !keys.contains(&h.key().to_string()) {
                new_headers.add(h.with(h.schema(), h.value().clone()));
            }
        }
        Box::new(new_headers)
    }

    fn remove(&mut self, key: &str) -> Box<dyn Headers> {
        let mut new_headers = ConcreteHeaders::new();
        for h in &self.headers {
            if h.key() != key {
                new_headers.add(h.with(h.schema(), h.value().clone()));
            }
        }
        Box::new(new_headers)
    }

    fn duplicate(&self) -> Box<dyn Headers> {
        let mut new_headers = ConcreteHeaders::new();
        for h in &self.headers {
            new_headers.add(h.with(h.schema(), h.value().clone()));
        }
        Box::new(new_headers)
    }

    fn iter(&self) -> Box<dyn Iterator<Item = &dyn Header> + '_> {
        Box::new(self.headers.iter().map(|h| h.as_ref()))
    }
}

// ============================================================================================
// SourceRecord - record from a source system
// ============================================================================================

/// SourceRecord represents a record read from a source system
pub struct SourceRecord {
    topic: String,
    kafka_partition: Option<i32>,
    key_schema: Option<Arc<dyn Schema>>,
    key: Option<Arc<dyn Any + Send + Sync>>,
    value_schema: Option<Arc<dyn Schema>>,
    value: Option<Arc<dyn Any + Send + Sync>>,
    timestamp: Option<i64>,
    headers: Box<dyn Headers>,
    source_partition: HashMap<String, i64>,
    source_offset: HashMap<String, i64>,
}

impl SourceRecord {
    pub fn new(
        topic: String,
        source_partition: HashMap<String, i64>,
        source_offset: HashMap<String, i64>,
    ) -> Self {
        Self {
            topic,
            kafka_partition: None,
            key_schema: None,
            key: None,
            value_schema: None,
            value: None,
            timestamp: None,
            headers: Box::new(ConcreteHeaders::new()),
            source_partition,
            source_offset,
        }
    }

    pub fn builder() -> SourceRecordBuilder {
        SourceRecordBuilder::new()
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn kafka_partition(&self) -> Option<i32> {
        self.kafka_partition
    }

    pub fn key_schema(&self) -> Option<Arc<dyn Schema>> {
        self.key_schema.clone()
    }

    pub fn key(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.key.clone()
    }

    pub fn value_schema(&self) -> Option<Arc<dyn Schema>> {
        self.value_schema.clone()
    }

    pub fn value(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.value.clone()
    }

    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    pub fn headers(&self) -> &dyn Headers {
        self.headers.as_ref()
    }

    pub fn source_partition(&self) -> &HashMap<String, i64> {
        &self.source_partition
    }

    pub fn source_offset(&self) -> &HashMap<String, i64> {
        &self.source_offset
    }
}

impl ConnectRecord<SourceRecord> for SourceRecord {
    fn topic(&self) -> &str {
        self.topic()
    }

    fn kafka_partition(&self) -> Option<i32> {
        self.kafka_partition()
    }

    fn key_schema(&self) -> Option<Arc<dyn Schema>> {
        self.key_schema()
    }

    fn key(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.key()
    }

    fn value_schema(&self) -> Option<Arc<dyn Schema>> {
        self.value_schema()
    }

    fn value(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.value()
    }

    fn timestamp(&self) -> Option<i64> {
        self.timestamp()
    }

    fn headers(&self) -> &dyn Headers {
        self.headers()
    }

    fn new_record(
        self,
        topic: Option<&str>,
        partition: Option<i32>,
        key_schema: Option<Arc<dyn Schema>>,
        key: Option<Arc<dyn Any + Send + Sync>>,
        value_schema: Option<Arc<dyn Schema>>,
        value: Option<Arc<dyn Any + Send + Sync>>,
        timestamp: Option<i64>,
        headers: Option<Box<dyn Headers>>,
    ) -> SourceRecord {
        let headers_box = if let Some(h) = headers {
            h.duplicate()
        } else {
            Box::new(ConcreteHeaders::new())
        };

        SourceRecord {
            topic: topic.unwrap_or(&self.topic).to_string(),
            kafka_partition: partition.or(self.kafka_partition),
            key_schema: key_schema.or(self.key_schema),
            key: key.or(self.key),
            value_schema: value_schema.or(self.value_schema),
            value: value.or(self.value),
            timestamp: timestamp.or(self.timestamp),
            headers: headers_box,
            source_partition: self.source_partition,
            source_offset: self.source_offset,
        }
    }
}

/// Builder for SourceRecord
pub struct SourceRecordBuilder {
    topic: Option<String>,
    kafka_partition: Option<i32>,
    key_schema: Option<Arc<dyn Schema>>,
    key: Option<Arc<dyn Any + Send + Sync>>,
    value_schema: Option<Arc<dyn Schema>>,
    value: Option<Arc<dyn Any + Send + Sync>>,
    timestamp: Option<i64>,
    headers: Option<Box<dyn Headers>>,
    source_partition: HashMap<String, i64>,
    source_offset: HashMap<String, i64>,
}

impl SourceRecordBuilder {
    pub fn new() -> Self {
        Self {
            topic: None,
            kafka_partition: None,
            key_schema: None,
            key: None,
            value_schema: None,
            value: None,
            timestamp: None,
            headers: None,
            source_partition: HashMap::new(),
            source_offset: HashMap::new(),
        }
    }

    pub fn topic(mut self, topic: String) -> Self {
        self.topic = Some(topic);
        self
    }

    pub fn partition(mut self, partition: Option<i32>) -> Self {
        self.kafka_partition = partition;
        self
    }

    pub fn key_schema(mut self, schema: Arc<dyn Schema>) -> Self {
        self.key_schema = Some(schema);
        self
    }

    pub fn key(mut self, key: Arc<dyn Any + Send + Sync>) -> Self {
        self.key = Some(key);
        self
    }

    pub fn value_schema(mut self, schema: Arc<dyn Schema>) -> Self {
        self.value_schema = Some(schema);
        self
    }

    pub fn value(mut self, value: Arc<dyn Any + Send + Sync>) -> Self {
        self.value = Some(value);
        self
    }

    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn headers(mut self, headers: Box<dyn Headers>) -> Self {
        self.headers = Some(headers);
        self
    }

    pub fn source_partition(mut self, partition: HashMap<String, i64>) -> Self {
        self.source_partition = partition;
        self
    }

    pub fn source_offset(mut self, offset: HashMap<String, i64>) -> Self {
        self.source_offset = offset;
        self
    }

    pub fn build(self) -> SourceRecord {
        SourceRecord {
            topic: self.topic.unwrap_or_default(),
            kafka_partition: self.kafka_partition,
            key_schema: self.key_schema,
            key: self.key,
            value_schema: self.value_schema,
            value: self.value,
            timestamp: self.timestamp,
            headers: self
                .headers
                .unwrap_or_else(|| Box::new(ConcreteHeaders::new())),
            source_partition: self.source_partition,
            source_offset: self.source_offset,
        }
    }
}

impl Default for SourceRecordBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================================
// SinkRecord - record destined for a sink system
// ============================================================================================

/// SinkRecord represents a record read from Kafka and destined for a sink system
pub struct SinkRecord {
    topic: String,
    kafka_partition: i32,
    key_schema: Option<Arc<dyn Schema>>,
    key: Option<Arc<dyn Any + Send + Sync>>,
    value_schema: Option<Arc<dyn Schema>>,
    value: Option<Arc<dyn Any + Send + Sync>>,
    timestamp: Option<i64>,
    headers: Box<dyn Headers>,
    kafka_offset: i64,
}

impl SinkRecord {
    pub fn new(topic: String, kafka_partition: i32, kafka_offset: i64) -> Self {
        Self {
            topic,
            kafka_partition,
            key_schema: None,
            key: None,
            value_schema: None,
            value: None,
            timestamp: None,
            headers: Box::new(ConcreteHeaders::new()),
            kafka_offset,
        }
    }

    pub fn builder() -> SinkRecordBuilder {
        SinkRecordBuilder::new()
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn kafka_partition(&self) -> i32 {
        self.kafka_partition
    }

    pub fn key_schema(&self) -> Option<Arc<dyn Schema>> {
        self.key_schema.clone()
    }

    pub fn key(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.key.clone()
    }

    pub fn value_schema(&self) -> Option<Arc<dyn Schema>> {
        self.value_schema.clone()
    }

    pub fn value(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.value.clone()
    }

    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    pub fn headers(&self) -> &dyn Headers {
        self.headers.as_ref()
    }

    pub fn kafka_offset(&self) -> i64 {
        self.kafka_offset
    }
}

impl ConnectRecord<SinkRecord> for SinkRecord {
    fn topic(&self) -> &str {
        self.topic()
    }

    fn kafka_partition(&self) -> Option<i32> {
        Some(self.kafka_partition())
    }

    fn key_schema(&self) -> Option<Arc<dyn Schema>> {
        self.key_schema()
    }

    fn key(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.key()
    }

    fn value_schema(&self) -> Option<Arc<dyn Schema>> {
        self.value_schema()
    }

    fn value(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.value()
    }

    fn timestamp(&self) -> Option<i64> {
        self.timestamp()
    }

    fn headers(&self) -> &dyn Headers {
        self.headers()
    }

    fn new_record(
        self,
        topic: Option<&str>,
        partition: Option<i32>,
        key_schema: Option<Arc<dyn Schema>>,
        key: Option<Arc<dyn Any + Send + Sync>>,
        value_schema: Option<Arc<dyn Schema>>,
        value: Option<Arc<dyn Any + Send + Sync>>,
        timestamp: Option<i64>,
        headers: Option<Box<dyn Headers>>,
    ) -> SinkRecord {
        let headers_box = if let Some(h) = headers {
            h.duplicate()
        } else {
            Box::new(ConcreteHeaders::new())
        };

        SinkRecord {
            topic: topic.unwrap_or(&self.topic).to_string(),
            kafka_partition: partition.unwrap_or(self.kafka_partition),
            key_schema: key_schema.or(self.key_schema),
            key: key.or(self.key),
            value_schema: value_schema.or(self.value_schema),
            value: value.or(self.value),
            timestamp: timestamp.or(self.timestamp),
            headers: headers_box,
            kafka_offset: self.kafka_offset,
        }
    }
}

/// Builder for SinkRecord
pub struct SinkRecordBuilder {
    topic: Option<String>,
    kafka_partition: Option<i32>,
    key_schema: Option<Arc<dyn Schema>>,
    key: Option<Arc<dyn Any + Send + Sync>>,
    value_schema: Option<Arc<dyn Schema>>,
    value: Option<Arc<dyn Any + Send + Sync>>,
    timestamp: Option<i64>,
    headers: Option<Box<dyn Headers>>,
    kafka_offset: Option<i64>,
}

impl SinkRecordBuilder {
    pub fn new() -> Self {
        Self {
            topic: None,
            kafka_partition: None,
            key_schema: None,
            key: None,
            value_schema: None,
            value: None,
            timestamp: None,
            headers: None,
            kafka_offset: None,
        }
    }

    pub fn topic(mut self, topic: String) -> Self {
        self.topic = Some(topic);
        self
    }

    pub fn partition(mut self, partition: i32) -> Self {
        self.kafka_partition = Some(partition);
        self
    }

    pub fn key_schema(mut self, schema: Arc<dyn Schema>) -> Self {
        self.key_schema = Some(schema);
        self
    }

    pub fn key(mut self, key: Arc<dyn Any + Send + Sync>) -> Self {
        self.key = Some(key);
        self
    }

    pub fn value_schema(mut self, schema: Arc<dyn Schema>) -> Self {
        self.value_schema = Some(schema);
        self
    }

    pub fn value(mut self, value: Arc<dyn Any + Send + Sync>) -> Self {
        self.value = Some(value);
        self
    }

    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn headers(mut self, headers: Box<dyn Headers>) -> Self {
        self.headers = Some(headers);
        self
    }

    pub fn offset(mut self, offset: i64) -> Self {
        self.kafka_offset = Some(offset);
        self
    }

    pub fn build(self) -> SinkRecord {
        SinkRecord {
            topic: self.topic.unwrap_or_default(),
            kafka_partition: self.kafka_partition.unwrap_or(0),
            key_schema: self.key_schema,
            key: self.key,
            value_schema: self.value_schema,
            value: self.value,
            timestamp: self.timestamp,
            headers: self
                .headers
                .unwrap_or_else(|| Box::new(ConcreteHeaders::new())),
            kafka_offset: self.kafka_offset.unwrap_or(0),
        }
    }
}

impl Default for SinkRecordBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_schema() {
        let schema = ConnectSchema::new(Type::Int32);
        assert_eq!(schema.type_(), Type::Int32);
        assert!(!schema.is_optional());
    }

    #[test]
    fn test_struct_schema() {
        let fields = vec![
            Field::new(
                0,
                "name".to_string(),
                Arc::new(ConnectSchema::new(Type::String)),
            ),
            Field::new(
                1,
                "age".to_string(),
                Arc::new(ConnectSchema::new(Type::Int32)),
            ),
        ];
        let schema = ConnectSchema::struct_(fields);
        assert_eq!(schema.type_(), Type::Struct);
        assert_eq!(schema.fields().unwrap().len(), 2);
    }

    #[test]
    fn test_array_schema() {
        let value_schema = Arc::new(ConnectSchema::new(Type::Int32));
        let schema = ConnectSchema::array(value_schema);
        assert_eq!(schema.type_(), Type::Array);
        assert_eq!(schema.value_schema().unwrap().type_(), Type::Int32);
    }

    #[test]
    fn test_map_schema() {
        let key_schema = Arc::new(ConnectSchema::new(Type::String));
        let value_schema = Arc::new(ConnectSchema::new(Type::Int32));
        let schema = ConnectSchema::map(key_schema, value_schema);
        assert_eq!(schema.type_(), Type::Map);
        assert_eq!(schema.key_schema().unwrap().type_(), Type::String);
        assert_eq!(schema.value_schema().unwrap().type_(), Type::Int32);
    }

    #[test]
    fn test_schema_builder() {
        let schema = SchemaBuilder::int32()
            .name("test".to_string())
            .optional()
            .version(1)
            .doc("Test schema".to_string())
            .build()
            .unwrap();

        assert_eq!(schema.type_(), Type::Int32);
        assert!(schema.is_optional());
        assert_eq!(schema.name(), Some("test"));
        assert_eq!(schema.version(), Some(1));
        assert_eq!(schema.doc(), Some("Test schema"));
    }

    #[test]
    fn test_struct_builder() {
        let schema = SchemaBuilder::struct_()
            .name("Person".to_string())
            .field(
                "name".to_string(),
                Arc::new(ConnectSchema::new(Type::String)),
            )
            .field("age".to_string(), Arc::new(ConnectSchema::new(Type::Int32)))
            .build()
            .unwrap();

        assert_eq!(schema.type_(), Type::Struct);
        assert_eq!(schema.fields().unwrap().len(), 2);
    }

    #[test]
    fn test_struct_operations() {
        let schema = SchemaBuilder::struct_()
            .name("Person".to_string())
            .field(
                "name".to_string(),
                Arc::new(ConnectSchema::new(Type::String)),
            )
            .field("age".to_string(), Arc::new(ConnectSchema::new(Type::Int32)))
            .build()
            .unwrap();

        let mut person = Struct::new(schema).unwrap();
        person.put("name", Box::new("Alice".to_string())).unwrap();
        person.put("age", Box::new(30i32)).unwrap();

        assert_eq!(person.get_string("name").unwrap(), "Alice");
        assert_eq!(person.get_int32("age").unwrap(), 30);
    }

    #[test]
    fn test_field() {
        let schema = Arc::new(ConnectSchema::new(Type::String));
        let field = Field::new(0, "test".to_string(), schema).with_doc("Test field".to_string());

        assert_eq!(field.index(), 0);
        assert_eq!(field.name(), "test");
        assert_eq!(field.doc(), Some("Test field"));
    }

    #[test]
    fn test_logical_types() {
        let date_schema = Date::schema();
        assert_eq!(date_schema.name(), Some(Date::LOGICAL_NAME));

        let time_schema = Time::schema();
        assert_eq!(time_schema.name(), Some(Time::LOGICAL_NAME));

        let timestamp_schema = Timestamp::schema();
        assert_eq!(timestamp_schema.name(), Some(Timestamp::LOGICAL_NAME));

        let decimal_schema = Decimal::schema(2);
        assert_eq!(decimal_schema.name(), Some(Decimal::LOGICAL_NAME));
    }
}
