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

//! JSON schema constants and envelope structure.
//!
//! Corresponds to: `connect/json/src/main/java/org/apache/kafka/connect/json/JsonSchema.java`

use serde_json::{json, Map, Value};

// ============================================================================
// Envelope field names
// ============================================================================

/// Field name for the schema in an envelope.
/// Java: `ENVELOPE_SCHEMA_FIELD_NAME = "schema"`
pub const ENVELOPE_SCHEMA_FIELD_NAME: &str = "schema";

/// Field name for the payload in an envelope.
/// Java: `ENVELOPE_PAYLOAD_FIELD_NAME = "payload"`
pub const ENVELOPE_PAYLOAD_FIELD_NAME: &str = "payload";

// ============================================================================
// Schema field names
// ============================================================================

/// Field name for the type in a schema.
/// Java: `SCHEMA_TYPE_FIELD_NAME = "type"`
pub const SCHEMA_TYPE_FIELD_NAME: &str = "type";

/// Field name for the optional flag in a schema.
/// Java: `SCHEMA_OPTIONAL_FIELD_NAME = "optional"`
pub const SCHEMA_OPTIONAL_FIELD_NAME: &str = "optional";

/// Field name for the name in a schema.
/// Java: `SCHEMA_NAME_FIELD_NAME = "name"`
pub const SCHEMA_NAME_FIELD_NAME: &str = "name";

/// Field name for the version in a schema.
/// Java: `SCHEMA_VERSION_FIELD_NAME = "version"`
pub const SCHEMA_VERSION_FIELD_NAME: &str = "version";

/// Field name for the documentation in a schema.
/// Java: `SCHEMA_DOC_FIELD_NAME = "doc"`
pub const SCHEMA_DOC_FIELD_NAME: &str = "doc";

/// Field name for the parameters in a schema.
/// Java: `SCHEMA_PARAMETERS_FIELD_NAME = "parameters"`
pub const SCHEMA_PARAMETERS_FIELD_NAME: &str = "parameters";

/// Field name for the default value in a schema.
/// Java: `SCHEMA_DEFAULT_FIELD_NAME = "default"`
pub const SCHEMA_DEFAULT_FIELD_NAME: &str = "default";

// ============================================================================
// Complex type field names
// ============================================================================

/// Field name for array items.
/// Java: `ARRAY_ITEMS_FIELD_NAME = "items"`
pub const ARRAY_ITEMS_FIELD_NAME: &str = "items";

/// Field name for map keys.
/// Java: `MAP_KEY_FIELD_NAME = "keys"`
pub const MAP_KEY_FIELD_NAME: &str = "keys";

/// Field name for map values.
/// Java: `MAP_VALUE_FIELD_NAME = "values"`
pub const MAP_VALUE_FIELD_NAME: &str = "values";

/// Field name for struct fields.
/// Java: `STRUCT_FIELDS_FIELD_NAME = "fields"`
pub const STRUCT_FIELDS_FIELD_NAME: &str = "fields";

/// Field name for struct field name.
/// Java: `STRUCT_FIELD_NAME_FIELD_NAME = "field"`
pub const STRUCT_FIELD_NAME_FIELD_NAME: &str = "field";

// ============================================================================
// Type names
// ============================================================================

/// Type name for boolean.
/// Java: `BOOLEAN_TYPE_NAME = "boolean"`
pub const BOOLEAN_TYPE_NAME: &str = "boolean";

/// Type name for int8.
/// Java: `INT8_TYPE_NAME = "int8"`
pub const INT8_TYPE_NAME: &str = "int8";

/// Type name for int16.
/// Java: `INT16_TYPE_NAME = "int16"`
pub const INT16_TYPE_NAME: &str = "int16";

/// Type name for int32.
/// Java: `INT32_TYPE_NAME = "int32"`
pub const INT32_TYPE_NAME: &str = "int32";

/// Type name for int64.
/// Java: `INT64_TYPE_NAME = "int64"`
pub const INT64_TYPE_NAME: &str = "int64";

/// Type name for float.
/// Java: `FLOAT_TYPE_NAME = "float"`
pub const FLOAT_TYPE_NAME: &str = "float";

/// Type name for double.
/// Java: `DOUBLE_TYPE_NAME = "double"`
pub const DOUBLE_TYPE_NAME: &str = "double";

/// Type name for bytes.
/// Java: `BYTES_TYPE_NAME = "bytes"`
pub const BYTES_TYPE_NAME: &str = "bytes";

/// Type name for string.
/// Java: `STRING_TYPE_NAME = "string"`
pub const STRING_TYPE_NAME: &str = "string";

/// Type name for array.
/// Java: `ARRAY_TYPE_NAME = "array"`
pub const ARRAY_TYPE_NAME: &str = "array";

/// Type name for map.
/// Java: `MAP_TYPE_NAME = "map"`
pub const MAP_TYPE_NAME: &str = "map";

/// Type name for struct.
/// Java: `STRUCT_TYPE_NAME = "struct"`
pub const STRUCT_TYPE_NAME: &str = "struct";

// ============================================================================
// Predefined schemas
// ============================================================================

/// Predefined schema for boolean type.
/// Java: `BOOLEAN_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, BOOLEAN_TYPE_NAME)`
pub fn boolean_schema() -> Value {
    json!({
        SCHEMA_TYPE_FIELD_NAME: BOOLEAN_TYPE_NAME
    })
}

/// Predefined schema for int8 type.
/// Java: `INT8_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, INT8_TYPE_NAME)`
pub fn int8_schema() -> Value {
    json!({
        SCHEMA_TYPE_FIELD_NAME: INT8_TYPE_NAME
    })
}

/// Predefined schema for int16 type.
/// Java: `INT16_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, INT16_TYPE_NAME)`
pub fn int16_schema() -> Value {
    json!({
        SCHEMA_TYPE_FIELD_NAME: INT16_TYPE_NAME
    })
}

/// Predefined schema for int32 type.
/// Java: `INT32_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, INT32_TYPE_NAME)`
pub fn int32_schema() -> Value {
    json!({
        SCHEMA_TYPE_FIELD_NAME: INT32_TYPE_NAME
    })
}

/// Predefined schema for int64 type.
/// Java: `INT64_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, INT64_TYPE_NAME)`
pub fn int64_schema() -> Value {
    json!({
        SCHEMA_TYPE_FIELD_NAME: INT64_TYPE_NAME
    })
}

/// Predefined schema for float type.
/// Java: `FLOAT_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, FLOAT_TYPE_NAME)`
pub fn float_schema() -> Value {
    json!({
        SCHEMA_TYPE_FIELD_NAME: FLOAT_TYPE_NAME
    })
}

/// Predefined schema for double type.
/// Java: `DOUBLE_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, DOUBLE_TYPE_NAME)`
pub fn double_schema() -> Value {
    json!({
        SCHEMA_TYPE_FIELD_NAME: DOUBLE_TYPE_NAME
    })
}

/// Predefined schema for bytes type.
/// Java: `BYTES_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, BYTES_TYPE_NAME)`
pub fn bytes_schema() -> Value {
    json!({
        SCHEMA_TYPE_FIELD_NAME: BYTES_TYPE_NAME
    })
}

/// Predefined schema for string type.
/// Java: `STRING_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, STRING_TYPE_NAME)`
pub fn string_schema() -> Value {
    json!({
        SCHEMA_TYPE_FIELD_NAME: STRING_TYPE_NAME
    })
}

// ============================================================================
// Envelope function
// ============================================================================

/// Creates an envelope JSON object with schema and payload.
///
/// Java: `public static ObjectNode envelope(JsonNode schema, JsonNode payload)`
///
/// # Arguments
///
/// * `schema` - The schema JSON value
/// * `payload` - The payload JSON value
///
/// # Returns
///
/// A JSON object containing the schema and payload fields.
pub fn envelope(schema: Value, payload: Value) -> Value {
    let mut result = Map::new();
    result.insert(ENVELOPE_SCHEMA_FIELD_NAME.to_string(), schema);
    result.insert(ENVELOPE_PAYLOAD_FIELD_NAME.to_string(), payload);
    Value::Object(result)
}

// ============================================================================
// Envelope struct
// ============================================================================

/// Represents an envelope containing schema and payload.
///
/// Java: `static class Envelope { public JsonNode schema; public JsonNode payload; }`
#[derive(Debug, Clone)]
pub struct Envelope {
    /// The schema part of the envelope.
    /// Java: `public JsonNode schema`
    pub schema: Value,

    /// The payload part of the envelope.
    /// Java: `public JsonNode payload`
    pub payload: Value,
}

impl Envelope {
    /// Creates a new Envelope with the given schema and payload.
    ///
    /// Java: `public Envelope(JsonNode schema, JsonNode payload)`
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema JSON value
    /// * `payload` - The payload JSON value
    pub fn new(schema: Value, payload: Value) -> Self {
        Envelope { schema, payload }
    }

    /// Converts this envelope to a JSON object.
    ///
    /// Java: `public ObjectNode toJsonNode() { return envelope(schema, payload); }`
    ///
    /// # Returns
    ///
    /// A JSON object containing the schema and payload fields.
    pub fn to_json_node(&self) -> Value {
        envelope(self.schema.clone(), self.payload.clone())
    }
}
