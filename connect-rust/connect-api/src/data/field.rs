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

use crate::data::{ConnectSchema, Schema, SchemaType};
use crate::errors::ConnectError;
use serde_json::Value;
use std::collections::HashMap;

/// Field represents a field in a struct schema.
///
/// This corresponds to `org.apache.kafka.connect.data.Field` in Java.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    name: String,
    index: i32,
    schema: ConnectSchema,
}

impl Field {
    /// Creates a new Field with the given name, index, and schema.
    pub fn new(name: impl Into<String>, index: i32, schema: ConnectSchema) -> Self {
        Field {
            name: name.into(),
            index,
            schema,
        }
    }

    /// Returns the name of this field.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the index of this field.
    pub fn index(&self) -> i32 {
        self.index
    }

    /// Returns the schema of this field.
    pub fn schema(&self) -> &ConnectSchema {
        &self.schema
    }

    /// Returns the schema type (alias for schema().type()).
    pub fn r#type(&self) -> SchemaType {
        self.schema.r#type()
    }
}

impl Schema for Field {
    fn r#type(&self) -> SchemaType {
        self.schema.r#type()
    }

    fn is_optional(&self) -> bool {
        self.schema.is_optional()
    }

    fn name(&self) -> Option<&str> {
        self.schema.name()
    }

    fn version(&self) -> Option<i32> {
        self.schema.version()
    }

    fn doc(&self) -> Option<&str> {
        self.schema.doc()
    }

    fn parameters(&self) -> &HashMap<String, String> {
        self.schema.parameters()
    }

    fn key_schema(&self) -> Option<&dyn Schema> {
        self.schema.key_schema()
    }

    fn value_schema(&self) -> Option<&dyn Schema> {
        self.schema.value_schema()
    }

    fn fields(&self) -> &[Field] {
        self.schema.fields()
    }

    fn field(&self, name: &str) -> Option<&Field> {
        self.schema.field(name)
    }

    fn default_value(&self) -> Option<&Value> {
        self.schema.default_value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::SchemaBuilder;

    #[test]
    fn test_new() {
        let schema = SchemaBuilder::int32().build();
        let field = Field::new("test", 0, schema);
        assert_eq!(field.name(), "test");
        assert_eq!(field.index(), 0);
        assert_eq!(field.r#type(), SchemaType::Int32);
    }
}
