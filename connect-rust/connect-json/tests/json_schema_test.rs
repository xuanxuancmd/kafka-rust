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

//! Tests for json_schema module.

use connect_json::*;
use serde_json::json;

#[test]
fn test_envelope_schema_field_name() {
    assert_eq!(ENVELOPE_SCHEMA_FIELD_NAME, "schema");
}

#[test]
fn test_envelope_payload_field_name() {
    assert_eq!(ENVELOPE_PAYLOAD_FIELD_NAME, "payload");
}

#[test]
fn test_schema_type_field_name() {
    assert_eq!(SCHEMA_TYPE_FIELD_NAME, "type");
}

#[test]
fn test_boolean_type_name() {
    assert_eq!(BOOLEAN_TYPE_NAME, "boolean");
}

#[test]
fn test_int8_type_name() {
    assert_eq!(INT8_TYPE_NAME, "int8");
}

#[test]
fn test_boolean_schema() {
    let schema = boolean_schema();
    assert_eq!(schema["type"], "boolean");
}

#[test]
fn test_int8_schema() {
    let schema = int8_schema();
    assert_eq!(schema["type"], "int8");
}

#[test]
fn test_string_schema() {
    let schema = string_schema();
    assert_eq!(schema["type"], "string");
}

#[test]
fn test_envelope_function() {
    let schema = json!({"type": "string"});
    let payload = json!("test value");
    let result = envelope(schema.clone(), payload.clone());

    assert_eq!(result["schema"], schema);
    assert_eq!(result["payload"], payload);
}

#[test]
fn test_envelope_struct() {
    let schema = json!({"type": "int32"});
    let payload = json!(42);
    let envelope = Envelope::new(schema.clone(), payload.clone());

    assert_eq!(envelope.schema, schema);
    assert_eq!(envelope.payload, payload);

    let json_node = envelope.to_json_node();
    assert_eq!(json_node["schema"], schema);
    assert_eq!(json_node["payload"], payload);
}

#[test]
fn test_all_type_names() {
    assert_eq!(BOOLEAN_TYPE_NAME, "boolean");
    assert_eq!(INT8_TYPE_NAME, "int8");
    assert_eq!(INT16_TYPE_NAME, "int16");
    assert_eq!(INT32_TYPE_NAME, "int32");
    assert_eq!(INT64_TYPE_NAME, "int64");
    assert_eq!(FLOAT_TYPE_NAME, "float");
    assert_eq!(DOUBLE_TYPE_NAME, "double");
    assert_eq!(BYTES_TYPE_NAME, "bytes");
    assert_eq!(STRING_TYPE_NAME, "string");
    assert_eq!(ARRAY_TYPE_NAME, "array");
    assert_eq!(MAP_TYPE_NAME, "map");
    assert_eq!(STRUCT_TYPE_NAME, "struct");
}

#[test]
fn test_all_field_names() {
    assert_eq!(ENVELOPE_SCHEMA_FIELD_NAME, "schema");
    assert_eq!(ENVELOPE_PAYLOAD_FIELD_NAME, "payload");
    assert_eq!(SCHEMA_TYPE_FIELD_NAME, "type");
    assert_eq!(SCHEMA_OPTIONAL_FIELD_NAME, "optional");
    assert_eq!(SCHEMA_NAME_FIELD_NAME, "name");
    assert_eq!(SCHEMA_VERSION_FIELD_NAME, "version");
    assert_eq!(SCHEMA_DOC_FIELD_NAME, "doc");
    assert_eq!(SCHEMA_PARAMETERS_FIELD_NAME, "parameters");
    assert_eq!(SCHEMA_DEFAULT_FIELD_NAME, "default");
    assert_eq!(ARRAY_ITEMS_FIELD_NAME, "items");
    assert_eq!(MAP_KEY_FIELD_NAME, "keys");
    assert_eq!(MAP_VALUE_FIELD_NAME, "values");
    assert_eq!(STRUCT_FIELDS_FIELD_NAME, "fields");
    assert_eq!(STRUCT_FIELD_NAME_FIELD_NAME, "field");
}

#[test]
fn test_all_schemas() {
    assert_eq!(boolean_schema()["type"], BOOLEAN_TYPE_NAME);
    assert_eq!(int8_schema()["type"], INT8_TYPE_NAME);
    assert_eq!(int16_schema()["type"], INT16_TYPE_NAME);
    assert_eq!(int32_schema()["type"], INT32_TYPE_NAME);
    assert_eq!(int64_schema()["type"], INT64_TYPE_NAME);
    assert_eq!(float_schema()["type"], FLOAT_TYPE_NAME);
    assert_eq!(double_schema()["type"], DOUBLE_TYPE_NAME);
    assert_eq!(bytes_schema()["type"], BYTES_TYPE_NAME);
    assert_eq!(string_schema()["type"], STRING_TYPE_NAME);
}
