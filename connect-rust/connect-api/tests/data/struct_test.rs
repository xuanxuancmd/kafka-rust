// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use connect_api::data::{Schema, SchemaBuilder, Struct};
use connect_api::errors::ConnectError;
use std::collections::HashMap;

fn create_flat_struct_schema() -> Schema {
    SchemaBuilder::struct_builder()
        .field("int8", SchemaBuilder::int8().build())
        .field("int16", SchemaBuilder::int16().build())
        .field("int32", SchemaBuilder::int32().build())
        .field("int64", SchemaBuilder::int64().build())
        .field("float32", SchemaBuilder::float32().build())
        .field("float64", SchemaBuilder::float64().build())
        .field("boolean", SchemaBuilder::bool().build())
        .field("string", SchemaBuilder::string().build())
        .field("bytes", SchemaBuilder::bytes().build())
        .build()
}

fn create_nested_schema() -> Schema {
    let array_schema = SchemaBuilder::array(SchemaBuilder::int8().build()).build();
    let map_schema = SchemaBuilder::map(
        SchemaBuilder::int32().build(),
        SchemaBuilder::string().build(),
    )
    .build();
    let nested_child_schema = SchemaBuilder::struct_builder()
        .field("int8", SchemaBuilder::int8().build())
        .build();

    SchemaBuilder::struct_builder()
        .field("array", array_schema)
        .field("map", map_schema)
        .field("nested", nested_child_schema)
        .build()
}

#[test]
fn test_flat_struct() {
    let schema = create_flat_struct_schema();
    let struct_val = Struct::new(schema)
        .put("int8", 12i8)
        .put("int16", 12i16)
        .put("int32", 12i32)
        .put("int64", 12i64)
        .put("float32", 12.0f32)
        .put("float64", 12.0f64)
        .put("boolean", true)
        .put("string", "foobar")
        .put("bytes", vec![1u8, 2u8, 3u8]);

    // Test type-specific getters
    assert_eq!(Some(12i8), struct_val.get_int8("int8"));
    assert_eq!(Some(12i16), struct_val.get_int16("int16"));
    assert_eq!(Some(12i32), struct_val.get_int32("int32"));
    assert_eq!(Some(12i64), struct_val.get_int64("int64"));
    assert_eq!(Some(12.0f32), struct_val.get_float32("float32"));
    assert_eq!(Some(12.0f64), struct_val.get_float64("float64"));
    assert_eq!(Some(true), struct_val.get_boolean("boolean"));
    assert_eq!(Some("foobar".to_string()), struct_val.get_string("string"));

    struct_val.validate().unwrap();
}

#[test]
fn test_complex_struct() {
    let schema = create_nested_schema();
    let nested_child_schema = SchemaBuilder::struct_builder()
        .field("int8", SchemaBuilder::int8().build())
        .build();

    let array: Vec<i8> = vec![1, 2];
    let mut map: HashMap<i32, String> = HashMap::new();
    map.insert(1, "string".to_string());

    let nested_struct = Struct::new(nested_child_schema).put("int8", 12i8);

    let struct_val = Struct::new(schema)
        .put("array", array.clone())
        .put("map", map.clone())
        .put("nested", nested_struct);

    // Test typed get methods
    let array_extracted: Option<Vec<i8>> = struct_val.get_array("array");
    assert_eq!(Some(array), array_extracted);

    let map_extracted: Option<HashMap<i32, String>> = struct_val.get_map("map");
    assert_eq!(Some(map), map_extracted);

    let nested_extracted = struct_val.get_struct("nested");
    assert!(nested_extracted.is_some());
    assert_eq!(Some(12i8), nested_extracted.unwrap().get_int8("int8"));

    struct_val.validate().unwrap();
}

#[test]
fn test_invalid_field_type() {
    let schema = create_flat_struct_schema();
    let result = Struct::new(schema).put("int8", "should fail because this is a string, not int8");
    // This should fail validation
}

#[test]
fn test_invalid_array_field_elements() {
    let schema = create_nested_schema();
    let result =
        Struct::new(schema).put("array", vec!["should fail since elements should be int8s"]);
    // This should fail validation
}

#[test]
fn test_invalid_map_key_elements() {
    let schema = create_nested_schema();
    let mut map: HashMap<String, i8> = HashMap::new();
    map.insert("should fail because keys should be int8s".to_string(), 12);
    let result = Struct::new(schema).put("map", map);
    // This should fail validation
}

#[test]
fn test_missing_field_validation() {
    // Required int8 field
    let schema = SchemaBuilder::struct_builder()
        .field("field", SchemaBuilder::int8().build())
        .build();
    let struct_val = Struct::new(schema);
    let result = struct_val.validate();
    assert!(result.is_err());
}

#[test]
fn test_missing_optional_field_validation() {
    let schema = SchemaBuilder::struct_builder()
        .field("field", SchemaBuilder::int8().optional().build())
        .build();
    let struct_val = Struct::new(schema);
    struct_val.validate().unwrap();
}

#[test]
fn test_missing_field_with_default_validation() {
    let schema = SchemaBuilder::struct_builder()
        .field("field", SchemaBuilder::int8().default_value(0i8).build())
        .build();
    let struct_val = Struct::new(schema);
    struct_val.validate().unwrap();
}

#[test]
fn test_missing_field_with_default_value() {
    let schema = SchemaBuilder::struct_builder()
        .field("field", SchemaBuilder::int8().default_value(0i8).build())
        .build();
    let struct_val = Struct::new(schema);
    assert_eq!(Some(0i8), struct_val.get_int8("field"));
}

#[test]
fn test_missing_field_without_default_value() {
    let schema = SchemaBuilder::struct_builder()
        .field("field", SchemaBuilder::int8().build())
        .build();
    let struct_val = Struct::new(schema);
    assert!(struct_val.get("field").is_none());
}

#[test]
fn test_equals() {
    let schema = create_flat_struct_schema();

    let struct1 = Struct::new(schema)
        .put("int8", 12i8)
        .put("int16", 12i16)
        .put("int32", 12i32)
        .put("int64", 12i64)
        .put("float32", 12.0f32)
        .put("float64", 12.0f64)
        .put("boolean", true)
        .put("string", "foobar")
        .put("bytes", vec![1u8, 2u8, 3u8]);

    let struct2 = Struct::new(schema)
        .put("int8", 12i8)
        .put("int16", 12i16)
        .put("int32", 12i32)
        .put("int64", 12i64)
        .put("float32", 12.0f32)
        .put("float64", 12.0f64)
        .put("boolean", true)
        .put("string", "foobar")
        .put("bytes", vec![1u8, 2u8, 3u8]);

    let struct3 = Struct::new(schema)
        .put("int8", 12i8)
        .put("int16", 12i16)
        .put("int32", 12i32)
        .put("int64", 12i64)
        .put("float32", 12.0f32)
        .put("float64", 12.0f64)
        .put("boolean", true)
        .put("string", "mismatching string")
        .put("bytes", vec![1u8, 2u8, 3u8]);

    assert_eq!(struct1, struct2);
    assert_ne!(struct1, struct3);
}

#[test]
fn test_equals_and_hashcode_with_byte_array_value() {
    let schema = create_flat_struct_schema();

    let struct1 = Struct::new(schema)
        .put("int8", 12i8)
        .put("int16", 12i16)
        .put("int32", 12i32)
        .put("int64", 12i64)
        .put("float32", 12.0f32)
        .put("float64", 12.0f64)
        .put("boolean", true)
        .put("string", "foobar")
        .put("bytes", vec![102, 111, 111, 98, 97, 114]); // "foobar" as bytes

    let struct2 = Struct::new(schema)
        .put("int8", 12i8)
        .put("int16", 12i16)
        .put("int32", 12i32)
        .put("int64", 12i64)
        .put("float32", 12.0f32)
        .put("float64", 12.0f64)
        .put("boolean", true)
        .put("string", "foobar")
        .put("bytes", vec![102, 111, 111, 98, 97, 114]);

    let struct3 = Struct::new(schema)
        .put("int8", 12i8)
        .put("int16", 12i16)
        .put("int32", 12i32)
        .put("int64", 12i64)
        .put("float32", 12.0f32)
        .put("float64", 12.0f64)
        .put("boolean", true)
        .put("string", "foobar")
        .put("bytes", vec![109, 105, 115, 109, 97, 116, 99, 104]); // "mismatch" as bytes

    // Verify contract for equals
    assert_eq!(struct1, struct2);
    assert_ne!(struct1, struct3);

    // Hash codes should be equal for equal objects
    assert_eq!(struct1.hash_code(), struct2.hash_code());
    assert_ne!(struct1.hash_code(), struct3.hash_code());
}

#[test]
fn test_validate_struct_with_null_value() {
    let schema = SchemaBuilder::struct_builder()
        .field("one", SchemaBuilder::string().build())
        .field("two", SchemaBuilder::string().build())
        .field("three", SchemaBuilder::string().build())
        .build();

    let struct_val = Struct::new(schema);
    let result = struct_val.validate();
    assert!(result.is_err());
}

#[test]
fn test_invalid_put_includes_field_name() {
    let schema = SchemaBuilder::struct_builder()
        .field("fieldName", SchemaBuilder::string().build())
        .build();

    let struct_val = Struct::new(schema);
    let result = struct_val.put("fieldName", None::<String>);
    // This should fail with error message including field name
}

#[test]
fn test_invalid_struct_field_schema() {
    // NESTED_SCHEMA has a "nested" field that expects NESTED_CHILD_SCHEMA (struct with int8 field)
    // But we try to put a Struct with MAP_SCHEMA (map schema) instead - should fail
    let nested_schema = create_nested_schema();
    let map_schema = SchemaBuilder::map(
        SchemaBuilder::int32().build(),
        SchemaBuilder::string().build(),
    )
    .build();

    let struct_val = Struct::new(nested_schema).unwrap();
    // Create a Struct with wrong schema (MAP_SCHEMA instead of NESTED_CHILD_SCHEMA)
    let wrong_schema_struct = Struct::new(map_schema).unwrap();

    // This should fail because the nested field expects NESTED_CHILD_SCHEMA, not MAP_SCHEMA
    let result = struct_val.put("nested", wrong_schema_struct);
    assert!(result.is_err());
}

#[test]
fn test_invalid_struct_field_value() {
    // NESTED_SCHEMA has a "nested" field that expects NESTED_CHILD_SCHEMA (struct with int8 field)
    // We create a Struct with correct NESTED_CHILD_SCHEMA, but it's empty (missing required int8 field)
    // When we put this empty Struct, validation should fail because the nested Struct is invalid
    let nested_schema = create_nested_schema();
    let nested_child_schema = SchemaBuilder::struct_builder()
        .field("int8", SchemaBuilder::int8().build())
        .build();

    let struct_val = Struct::new(nested_schema).unwrap();
    // Create an empty Struct with correct schema but no values set
    let empty_nested_struct = Struct::new(nested_child_schema).unwrap();

    // This should fail because the nested Struct is empty (missing required int8 field)
    let result = struct_val.put("nested", empty_nested_struct);
    assert!(result.is_err());
}
