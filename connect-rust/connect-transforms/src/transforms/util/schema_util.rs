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

use connect_api::data::{ConnectSchema, Schema, SchemaBuilder, SchemaType};
use std::collections::HashMap;

/// SchemaUtil provides utility methods for copying and manipulating schemas.
///
/// This corresponds to `org.apache.kafka.connect.transforms.util.SchemaUtil` in Java.
pub struct SchemaUtil;

impl SchemaUtil {
    /// Copies the basic properties from a source schema into a new SchemaBuilder.
    ///
    /// This method creates a new SchemaBuilder with the type from the source schema,
    /// then copies name, version, doc, and parameters.
    ///
    /// For ARRAY schemas, the value schema is also copied.
    /// Note: This method does NOT copy key_schema (for Map), value_schema (for non-Array),
    /// fields (for Struct), or default_value - only basic metadata properties.
    ///
    /// # Arguments
    /// * `source` - The source schema to copy from
    ///
    /// # Returns
    /// A SchemaBuilder with the copied basic properties
    pub fn copy_schema_basics(source: &dyn Schema) -> SchemaBuilder {
        let builder = if source.r#type() == SchemaType::Array {
            // For ARRAY type, we need to include the value schema
            let value_schema = source.value_schema();
            if let Some(vs) = value_schema {
                // Convert dyn Schema to ConnectSchema
                let value_connect_schema = deep_copy_schema(vs);
                SchemaBuilder::array(value_connect_schema)
            } else {
                // If no value schema, create a type builder
                SchemaBuilder::type_builder(source.r#type())
            }
        } else {
            SchemaBuilder::type_builder(source.r#type())
        };
        Self::copy_schema_basics_to(source, builder)
    }

    /// Copies the basic properties from a source schema into an existing SchemaBuilder.
    ///
    /// This method copies name, version, doc, and parameters into the provided builder.
    /// It does NOT modify the type, optional status, value/key schemas, or fields.
    ///
    /// # Arguments
    /// * `source` - The source schema to copy from
    /// * `builder` - The SchemaBuilder to copy properties into
    ///
    /// # Returns
    /// The modified SchemaBuilder with the copied basic properties
    pub fn copy_schema_basics_to(source: &dyn Schema, builder: SchemaBuilder) -> SchemaBuilder {
        let mut result = builder;

        // Copy name
        if let Some(name) = source.name() {
            result = result.name(name);
        }

        // Copy version
        if let Some(version) = source.version() {
            result = result.version(version);
        }

        // Copy doc
        if let Some(doc) = source.doc() {
            result = result.doc(doc);
        }

        // Copy parameters
        let params = source.parameters();
        if !params.is_empty() {
            let params_copy: HashMap<String, String> = params.clone();
            result = result.parameters(params_copy);
        }

        result
    }
}

/// Deep copies a schema, recursively copying all nested schemas.
///
/// This creates a complete copy of the schema including:
/// - All basic properties (type, optional, name, version, doc, parameters)
/// - Key schema for Map types
/// - Value schema for Map and Array types
/// - Fields for Struct types
/// - Default value
///
/// # Arguments
/// * `schema` - The schema to deep copy
///
/// # Returns
/// A new ConnectSchema that is a complete copy of the original
pub fn deep_copy_schema(schema: &dyn Schema) -> ConnectSchema {
    let schema_type = schema.r#type();
    let optional = schema.is_optional();

    // Copy basic properties
    let name = schema.name().map(|s| s.to_string());
    let version = schema.version();
    let doc = schema.doc().map(|s| s.to_string());
    let parameters: HashMap<String, String> = schema.parameters().clone();

    // Copy default value
    let default_value = schema.default_value().cloned();

    // Handle nested schemas based on type
    let (key_schema, value_schema, fields) = match schema_type {
        SchemaType::Map => {
            let key_schema = schema.key_schema().map(|ks| deep_copy_schema(ks));
            let value_schema = schema.value_schema().map(|vs| deep_copy_schema(vs));
            (key_schema, value_schema, Vec::new())
        }
        SchemaType::Array => {
            let value_schema = schema.value_schema().map(|vs| deep_copy_schema(vs));
            (None, value_schema, Vec::new())
        }
        SchemaType::Struct => {
            // Deep copy each field's schema
            let fields: Vec<connect_api::data::Field> = schema
                .fields()
                .iter()
                .map(|f| {
                    let field_schema = deep_copy_schema(f.schema());
                    connect_api::data::Field::new(f.name(), f.index(), field_schema)
                })
                .collect();
            (None, None, fields)
        }
        _ => (None, None, Vec::new()),
    };

    ConnectSchema::new_full(
        schema_type,
        optional,
        default_value,
        name,
        version,
        doc,
        parameters,
        fields,
        key_schema,
        value_schema,
    )
}

