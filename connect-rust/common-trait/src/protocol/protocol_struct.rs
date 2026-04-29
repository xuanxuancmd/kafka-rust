/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use super::BoundField;
use super::ProtocolValue;
use super::Schema;
use super::SchemaError;

/// A record that can be serialized and deserialized according to a pre-defined schema
/// Corresponds to Java: org.apache.kafka.common.protocol.types.Struct
pub struct Struct {
    schema: *const Schema,
    values: Vec<Option<ProtocolValue>>,
}

impl Struct {
    /// Create a new struct with the given schema
    pub fn new(schema: &Schema) -> Self {
        Self {
            schema: schema as *const Schema,
            values: vec![None; schema.num_fields()],
        }
    }

    /// Create a struct with pre-populated values
    pub fn new_with_values(schema: &Schema, values: Vec<Option<ProtocolValue>>) -> Self {
        Self {
            schema: schema as *const Schema,
            values,
        }
    }

    /// Get the schema for this struct
    pub fn schema(&self) -> &Schema {
        // SAFETY: schema pointer is always valid as long as the Schema exists
        unsafe { &*self.schema }
    }

    /// Get the value for a field by index
    pub fn get_by_index(&self, index: usize) -> Result<&ProtocolValue, SchemaError> {
        let schema = self.schema();
        let field = schema.get_by_index(index);

        match &self.values[index] {
            Some(value) => Ok(value),
            None => {
                if field.def.has_default_value {
                    Err(SchemaError::new(format!(
                        "Missing value for field '{}' which has no default value.",
                        field.def.name
                    )))
                } else if field.def.type_ref.is_nullable() {
                    Err(SchemaError::new(
                        "Null value not supported in this implementation",
                    ))
                } else {
                    Err(SchemaError::new(format!(
                        "Missing value for field '{}' which has no default value.",
                        field.def.name
                    )))
                }
            }
        }
    }

    /// Get the value for a field by name
    pub fn get(&self, name: &str) -> Result<&ProtocolValue, SchemaError> {
        let schema = self.schema();
        let field = schema
            .get_by_name(name)
            .ok_or_else(|| SchemaError::new(format!("No such field: {}", name)))?;
        self.get_by_index(field.index)
    }

    /// Check if the struct contains a field
    pub fn has_field(&self, name: &str) -> bool {
        self.schema().get_by_name(name).is_some()
    }

    /// Get a Short value by name
    pub fn get_short(&self, name: &str) -> Result<i16, SchemaError> {
        let value = self.get(name)?;
        value
            .as_int16()
            .ok_or_else(|| SchemaError::new(format!("Field '{}' is not a Short", name)))
    }

    /// Get an Integer value by name
    pub fn get_int(&self, name: &str) -> Result<i32, SchemaError> {
        let value = self.get(name)?;
        value
            .as_int32()
            .ok_or_else(|| SchemaError::new(format!("Field '{}' is not an Integer", name)))
    }

    /// Get a Long value by name
    pub fn get_long(&self, name: &str) -> Result<i64, SchemaError> {
        let value = self.get(name)?;
        value
            .as_int64()
            .ok_or_else(|| SchemaError::new(format!("Field '{}' is not a Long", name)))
    }

    /// Get a String value by name
    pub fn get_string(&self, name: &str) -> Result<&str, SchemaError> {
        let value = self.get(name)?;
        value
            .as_string()
            .ok_or_else(|| SchemaError::new(format!("Field '{}' is not a String", name)))
    }

    /// Set a field by index
    pub fn set_by_index(&mut self, index: usize, value: ProtocolValue) -> Result<(), SchemaError> {
        if index >= self.values.len() {
            return Err(SchemaError::new(format!("Invalid field index: {}", index)));
        }
        self.values[index] = Some(value);
        Ok(())
    }

    /// Set a field by name
    pub fn set(&mut self, name: &str, value: ProtocolValue) -> Result<(), SchemaError> {
        let schema = self.schema();
        let field = schema
            .get_by_name(name)
            .ok_or_else(|| SchemaError::new(format!("Unknown field: {}", name)))?;
        self.set_by_index(field.index, value)?;
        Ok(())
    }

    /// Set a Short value
    pub fn set_short(&mut self, name: &str, value: i16) -> Result<(), SchemaError> {
        self.set(name, ProtocolValue::Int16(value))
    }

    /// Set an Integer value
    pub fn set_int(&mut self, name: &str, value: i32) -> Result<(), SchemaError> {
        self.set(name, ProtocolValue::Int32(value))
    }

    /// Set a Long value
    pub fn set_long(&mut self, name: &str, value: i64) -> Result<(), SchemaError> {
        self.set(name, ProtocolValue::Int64(value))
    }

    /// Set a String value
    pub fn set_string(&mut self, name: &str, value: impl Into<String>) -> Result<(), SchemaError> {
        self.set(name, ProtocolValue::String(value.into()))
    }

    /// Get the serialized size of this object
    pub fn size_of(&self) -> Result<usize, SchemaError> {
        self.schema().size_of(self)
    }

    /// Write this struct to a buffer
    pub fn write_to(&self, buffer: &mut Vec<u8>) -> Result<(), SchemaError> {
        self.schema().write(buffer, self)
    }

    /// Validate the contents of this struct against its schema
    pub fn validate(&self) -> Result<(), SchemaError> {
        self.schema().validate(self)
    }

    /// Clear all values from this struct
    pub fn clear(&mut self) {
        for value in &mut self.values {
            *value = None;
        }
    }
}

impl std::fmt::Display for Struct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        let schema = self.schema();
        for (i, value_opt) in self.values.iter().enumerate() {
            let field = schema.get_by_index(i);
            write!(f, "{}=", field.def.name)?;
            if let Some(value) = value_opt {
                write!(f, "{}", value)?;
            } else {
                write!(f, "null")?;
            }
            if i < self.values.len() - 1 {
                write!(f, ",")?;
            }
        }
        write!(f, "}}")?;
        Ok(())
    }
}

impl std::fmt::Debug for Struct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Struct({})", self)
    }
}
