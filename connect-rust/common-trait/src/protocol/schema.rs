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

use std::collections::HashMap;
use std::io::Cursor;

use super::BoundField;
use super::Field;
use super::ProtocolValue;
use super::SchemaError;
use super::Struct;
use super::Type;

/// Helper function to get remaining bytes in cursor
fn remaining(cursor: &Cursor<&[u8]>) -> usize {
    cursor.get_ref().len() - cursor.position() as usize
}

/// The schema for a compound record definition - corresponds to Java: org.apache.kafka.common.protocol.types.Schema
pub struct Schema {
    fields: Vec<BoundField>,
    fields_by_name: HashMap<String, usize>,
    tolerate_missing_fields_with_defaults: bool,
}

impl Schema {
    /// Construct the schema with a given list of its field values
    pub fn new(fields: Vec<Field>) -> Result<Self, SchemaError> {
        Self::new_with_tolerance(false, fields)
    }

    /// Construct the schema with tolerance for missing optional fields with defaults
    pub fn new_with_tolerance(
        tolerate_missing_fields_with_defaults: bool,
        fields: Vec<Field>,
    ) -> Result<Self, SchemaError> {
        let mut bound_fields = Vec::with_capacity(fields.len());
        let mut fields_by_name = HashMap::new();

        for (i, def) in fields.into_iter().enumerate() {
            if fields_by_name.contains_key(&def.name) {
                return Err(SchemaError::new(format!(
                    "Schema contains a duplicate field: {}",
                    def.name
                )));
            }
            bound_fields.push(BoundField::new(def, i));
            fields_by_name.insert(bound_fields[i].def.name.clone(), i);
        }

        Ok(Self {
            fields: bound_fields,
            fields_by_name,
            tolerate_missing_fields_with_defaults,
        })
    }

    /// Write a struct to the buffer
    pub fn write(&self, buffer: &mut Vec<u8>, struct_ref: &Struct) -> Result<(), SchemaError> {
        for field in &self.fields {
            let value = struct_ref.get_by_index(field.index)?;
            field.def.type_ref.validate(value)?;
            field.def.type_ref.write(buffer, value)?;
        }
        Ok(())
    }

    /// Read a struct from the buffer
    pub fn read(&self, buffer: &mut Cursor<&[u8]>) -> Result<Struct, SchemaError> {
        let mut values: Vec<Option<ProtocolValue>> = Vec::with_capacity(self.fields.len());

        for field in &self.fields {
            if self.tolerate_missing_fields_with_defaults {
                if remaining(buffer) > 0 {
                    let value = field.def.type_ref.read(buffer)?;
                    values.push(Some(value));
                } else if field.def.has_default_value {
                    // Use default value - for simplicity, we use Null
                    values.push(Some(ProtocolValue::Null));
                } else {
                    return Err(SchemaError::new(format!(
                        "Missing value for field '{}' which has no default value.",
                        field.def.name
                    )));
                }
            } else {
                let value = field.def.type_ref.read(buffer)?;
                values.push(Some(value));
            }
        }

        Ok(Struct::new_with_values(self, values))
    }

    /// The size of the given record
    pub fn size_of(&self, struct_ref: &Struct) -> Result<usize, SchemaError> {
        let mut size = 0;
        for field in &self.fields {
            let value = struct_ref.get_by_index(field.index)?;
            size += field.def.type_ref.size_of(value);
        }
        Ok(size)
    }

    /// The number of fields in this schema
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Get a field by its slot in the record array
    pub fn get_by_index(&self, slot: usize) -> &BoundField {
        &self.fields[slot]
    }

    /// Get a field by its name
    pub fn get_by_name(&self, name: &str) -> Option<&BoundField> {
        self.fields_by_name.get(name).map(|i| &self.fields[*i])
    }

    /// Get all the fields in this schema
    pub fn fields(&self) -> &[BoundField] {
        &self.fields
    }

    /// Validate the struct
    pub fn validate(&self, struct_ref: &Struct) -> Result<(), SchemaError> {
        for field in &self.fields {
            let value = struct_ref.get_by_index(field.index)?;
            field.def.type_ref.validate(value)?;
        }
        Ok(())
    }
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        for (i, field) in self.fields.iter().enumerate() {
            write!(f, "{}", field)?;
            if i < self.fields.len() - 1 {
                write!(f, ",")?;
            }
        }
        write!(f, "}}")?;
        Ok(())
    }
}
