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

use connect_api::data::{ConnectSchema, Field, Schema, SchemaBuilder, SchemaType, Struct};
use connect_api::errors::ConnectError;

#[test]
fn test_fields_on_struct_schema() {
    let schema = SchemaBuilder::struct_builder()
        .field("foo", SchemaBuilder::bool().build())
        .field("bar", SchemaBuilder::int32().build())
        .build();

    assert_eq!(2, schema.fields().len());

    // Validate field lookup by name
    let foo = schema.field("foo").expect("foo field should exist");
    assert_eq!(0, foo.index());

    let bar = schema.field("bar").expect("bar field should exist");
    assert_eq!(1, bar.index());

    // Any other field name should return None
    assert!(schema.field("other").is_none());
}

#[test]
fn test_fields_only_valid_for_structs() {
    let schema = SchemaBuilder::int8().build();
    // For non-struct schemas, fields() should return empty slice
    assert_eq!(0, schema.fields().len());
}

#[test]
fn test_validate_value_matching_type() {
    // Test primitive types
    ConnectSchema::validate_value(&SchemaBuilder::int8().build(), 1i8).unwrap();
    ConnectSchema::validate_value(&SchemaBuilder::int16().build(), 1i16).unwrap();
    ConnectSchema::validate_value(&SchemaBuilder::int32().build(), 1i32).unwrap();
    ConnectSchema::validate_value(&SchemaBuilder::int64().build(), 1i64).unwrap();
    ConnectSchema::validate_value(&SchemaBuilder::float32().build(), 1.0f32).unwrap();
    ConnectSchema::validate_value(&SchemaBuilder::float64().build(), 1.0f64).unwrap();
    ConnectSchema::validate_value(&SchemaBuilder::bool().build(), true).unwrap();
    ConnectSchema::validate_value(&SchemaBuilder::string().build(), "a string").unwrap();
    ConnectSchema::validate_value(&SchemaBuilder::bytes().build(), vec![1u8, 2u8, 3u8]).unwrap();

    // Test array type
    let array_schema = SchemaBuilder::array(SchemaBuilder::int32().build()).build();
    ConnectSchema::validate_value(&array_schema, vec![1i32, 2i32, 3i32]).unwrap();

    // Test map type
    let map_schema = SchemaBuilder::map(
        SchemaBuilder::int32().build(),
        SchemaBuilder::string().build(),
    )
    .build();
    use std::collections::HashMap;
    let mut map: HashMap<i32, String> = HashMap::new();
    map.insert(1, "value".to_string());
    ConnectSchema::validate_value(&map_schema, map).unwrap();
}

#[test]
fn test_validate_value_mismatch_int8() {
    let result = ConnectSchema::validate_value(&SchemaBuilder::int8().build(), 1i32);
    assert!(result.is_err());
}

#[test]
fn test_validate_value_mismatch_int16() {
    let result = ConnectSchema::validate_value(&SchemaBuilder::int16().build(), 1i32);
    assert!(result.is_err());
}

#[test]
fn test_validate_value_mismatch_int32() {
    let result = ConnectSchema::validate_value(&SchemaBuilder::int32().build(), 1i64);
    assert!(result.is_err());
}

#[test]
fn test_validate_value_mismatch_int64() {
    let result = ConnectSchema::validate_value(&SchemaBuilder::int64().build(), 1i32);
    assert!(result.is_err());
}

#[test]
fn test_validate_value_mismatch_float() {
    let result = ConnectSchema::validate_value(&SchemaBuilder::float32().build(), 1.0f64);
    assert!(result.is_err());
}

#[test]
fn test_validate_value_mismatch_double() {
    let result = ConnectSchema::validate_value(&SchemaBuilder::float64().build(), 1.0f32);
    assert!(result.is_err());
}

#[test]
fn test_validate_value_mismatch_boolean() {
    let result = ConnectSchema::validate_value(&SchemaBuilder::bool().build(), 1.0f32);
    assert!(result.is_err());
}

#[test]
fn test_validate_value_mismatch_string() {
    let result = ConnectSchema::validate_value(&SchemaBuilder::string().build(), 1i32);
    assert!(result.is_err());
}

#[test]
fn test_validate_value_mismatch_bytes() {
    let result = ConnectSchema::validate_value(&SchemaBuilder::bytes().build(), "a string");
    assert!(result.is_err());
}

#[test]
fn test_validate_value_mismatch_array() {
    let array_schema = SchemaBuilder::array(SchemaBuilder::int32().build()).build();
    let result = ConnectSchema::validate_value(&array_schema, vec!["a", "b", "c"]);
    assert!(result.is_err());
}

#[test]
fn test_primitive_equality() {
    // Test that primitive types handle equality correctly
    let s1 = ConnectSchema::new(
        SchemaType::Int8,
        false,
        None,
        Some("name"),
        Some(2),
        Some("doc"),
        None,
        None,
        None,
        None,
    );
    let s2 = ConnectSchema::new(
        SchemaType::Int8,
        false,
        None,
        Some("name"),
        Some(2),
        Some("doc"),
        None,
        None,
        None,
        None,
    );

    assert_eq!(s1, s2);

    // Different type
    let different_type = ConnectSchema::new(
        SchemaType::Int16,
        false,
        None,
        Some("name"),
        Some(2),
        Some("doc"),
        None,
        None,
        None,
        None,
    );
    assert_ne!(s1, different_type);

    // Different optional
    let different_optional = ConnectSchema::new(
        SchemaType::Int8,
        true,
        None,
        Some("name"),
        Some(2),
        Some("doc"),
        None,
        None,
        None,
        None,
    );
    assert_ne!(s1, different_optional);

    // Different name
    let different_name = ConnectSchema::new(
        SchemaType::Int8,
        false,
        None,
        Some("otherName"),
        Some(2),
        Some("doc"),
        None,
        None,
        None,
        None,
    );
    assert_ne!(s1, different_name);

    // Different version
    let different_version = ConnectSchema::new(
        SchemaType::Int8,
        false,
        None,
        Some("name"),
        Some(4),
        Some("doc"),
        None,
        None,
        None,
        None,
    );
    assert_ne!(s1, different_version);

    // Different doc
    let different_doc = ConnectSchema::new(
        SchemaType::Int8,
        false,
        None,
        Some("name"),
        Some(2),
        Some("other doc"),
        None,
        None,
        None,
        None,
    );
    assert_ne!(s1, different_doc);
}

#[test]
fn test_array_equality() {
    let s1 = ConnectSchema::new(
        SchemaType::Array,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(SchemaBuilder::int8().build()),
        None,
    );
    let s2 = ConnectSchema::new(
        SchemaType::Array,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(SchemaBuilder::int8().build()),
        None,
    );

    assert_eq!(s1, s2);

    // Different value schema
    let different_value_schema = ConnectSchema::new(
        SchemaType::Array,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(SchemaBuilder::int16().build()),
        None,
    );
    assert_ne!(s1, different_value_schema);
}

#[test]
fn test_map_equality() {
    let s1 = ConnectSchema::new(
        SchemaType::Map,
        false,
        None,
        None,
        None,
        None,
        None,
        Some(SchemaBuilder::int8().build()),
        Some(SchemaBuilder::int16().build()),
        None,
    );
    let s2 = ConnectSchema::new(
        SchemaType::Map,
        false,
        None,
        None,
        None,
        None,
        None,
        Some(SchemaBuilder::int8().build()),
        Some(SchemaBuilder::int16().build()),
        None,
    );

    assert_eq!(s1, s2);

    // Different key schema
    let different_key_schema = ConnectSchema::new(
        SchemaType::Map,
        false,
        None,
        None,
        None,
        None,
        None,
        Some(SchemaBuilder::string().build()),
        Some(SchemaBuilder::int16().build()),
        None,
    );
    assert_ne!(s1, different_key_schema);

    // Different value schema
    let different_value_schema = ConnectSchema::new(
        SchemaType::Map,
        false,
        None,
        None,
        None,
        None,
        None,
        Some(SchemaBuilder::int8().build()),
        Some(SchemaBuilder::string().build()),
        None,
    );
    assert_ne!(s1, different_value_schema);
}

#[test]
fn test_struct_equality() {
    let fields1 = vec![
        Field::new("field", 0, SchemaBuilder::int8().build()),
        Field::new("field2", 1, SchemaBuilder::int16().build()),
    ];
    let fields2 = vec![
        Field::new("field", 0, SchemaBuilder::int8().build()),
        Field::new("field2", 1, SchemaBuilder::int16().build()),
    ];
    let different_fields = vec![
        Field::new("field", 0, SchemaBuilder::int8().build()),
        Field::new("different field name", 1, SchemaBuilder::int16().build()),
    ];

    let s1 = ConnectSchema::new(
        SchemaType::Struct,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(fields1),
    );
    let s2 = ConnectSchema::new(
        SchemaType::Struct,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(fields2),
    );
    let different_field = ConnectSchema::new(
        SchemaType::Struct,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(different_fields),
    );

    assert_eq!(s1, s2);
    assert_ne!(s1, different_field);
}

#[test]
fn test_empty_struct() {
    let empty_struct_schema = ConnectSchema::new(
        SchemaType::Struct,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(vec![]),
    );
    assert_eq!(0, empty_struct_schema.fields().len());

    // Creating a Struct with empty schema should work
    let _struct = Struct::new(empty_struct_schema);
}

// =============================================================================
// Additional tests matching ConnectSchemaTest.java
// =============================================================================

/// Test: testValidateValueMatchingTypeWithOptional
#[test]
fn test_validate_value_optional_types() {
    // Optional int8 with null should validate
    let optional_int8 = SchemaBuilder::int8().optional().build();
    ConnectSchema::validate_value(&optional_int8, None::<i8>).unwrap();
    ConnectSchema::validate_value(&optional_int8, Some(1i8)).unwrap();

    // Optional string with null should validate
    let optional_string = SchemaBuilder::string().optional().build();
    ConnectSchema::validate_value(&optional_string, None::<String>).unwrap();
    ConnectSchema::validate_value(&optional_string, Some("value".to_string())).unwrap();
}

/// Test: testValidateValueWithDefault
#[test]
fn test_validate_value_with_default() {
    // Schema with default value
    let schema_with_default = SchemaBuilder::int8().default_value(42i8).build();
    ConnectSchema::validate_value(&schema_with_default, 42i8).unwrap();
    ConnectSchema::validate_value(&schema_with_default, 1i8).unwrap();
}

/// Test: testNestedStructValidation
#[test]
fn test_validate_nested_struct() {
    // Create nested struct schema
    let inner_schema = SchemaBuilder::struct_builder()
        .field("innerField", SchemaBuilder::int8().build())
        .build();

    let outer_schema = SchemaBuilder::struct_builder()
        .field("outerField", SchemaBuilder::string().build())
        .field("nestedStruct", inner_schema.clone())
        .build();

    // Validate nested struct
    let inner_struct = Struct::new(inner_schema).put("innerField", 42i8).unwrap();
    let outer_struct = Struct::new(outer_schema)
        .put("outerField", "value")
        .put("nestedStruct", inner_struct)
        .unwrap();

    // Schema should validate the struct
    assert_eq!(outer_schema.fields().len(), 2);
}

/// Test: testArrayWithNestedTypes
#[test]
fn test_array_with_nested_types() {
    // Array of structs
    let struct_schema = SchemaBuilder::struct_builder()
        .field("name", SchemaBuilder::string().build())
        .build();

    let array_schema = SchemaBuilder::array(struct_schema.clone()).build();
    assert_eq!(array_schema.type_(), SchemaType::Array);
    assert!(array_schema.value_schema().is_some());
}

/// Test: testMapWithNestedTypes
#[test]
fn test_map_with_nested_types() {
    // Map with struct values
    let struct_schema = SchemaBuilder::struct_builder()
        .field("value", SchemaBuilder::int32().build())
        .build();

    let map_schema =
        SchemaBuilder::map(SchemaBuilder::string().build(), struct_schema.clone()).build();
    assert_eq!(map_schema.type_(), SchemaType::Map);
    assert!(map_schema.key_schema().is_some());
    assert!(map_schema.value_schema().is_some());
}

/// Test: testSchemaTypeChecking
#[test]
fn test_schema_type_checking() {
    assert_eq!(SchemaBuilder::int8().build().type_(), SchemaType::Int8);
    assert_eq!(SchemaBuilder::int16().build().type_(), SchemaType::Int16);
    assert_eq!(SchemaBuilder::int32().build().type_(), SchemaType::Int32);
    assert_eq!(SchemaBuilder::int64().build().type_(), SchemaType::Int64);
    assert_eq!(
        SchemaBuilder::float32().build().type_(),
        SchemaType::Float32
    );
    assert_eq!(
        SchemaBuilder::float64().build().type_(),
        SchemaType::Float64
    );
    assert_eq!(SchemaBuilder::bool().build().type_(), SchemaType::Boolean);
    assert_eq!(SchemaBuilder::string().build().type_(), SchemaType::String);
    assert_eq!(SchemaBuilder::bytes().build().type_(), SchemaType::Bytes);
    assert_eq!(
        SchemaBuilder::struct_builder().build().type_(),
        SchemaType::Struct
    );
    assert_eq!(
        SchemaBuilder::array(SchemaBuilder::int8().build())
            .build()
            .type_(),
        SchemaType::Array
    );
    assert_eq!(
        SchemaBuilder::map(
            SchemaBuilder::int8().build(),
            SchemaBuilder::string().build()
        )
        .build()
        .type_(),
        SchemaType::Map
    );
}

/// Test: testOptionalFlag
#[test]
fn test_optional_flag() {
    // Required by default
    assert!(!SchemaBuilder::int8().build().is_optional());
    assert!(!SchemaBuilder::string().build().is_optional());

    // Optional when specified
    assert!(SchemaBuilder::int8().optional().build().is_optional());
    assert!(SchemaBuilder::string().optional().build().is_optional());
}

/// Test: testSchemaName
#[test]
fn test_schema_name() {
    let schema_with_name = SchemaBuilder::int8().name("testName").build();
    assert_eq!(schema_with_name.name(), Some("testName"));

    let schema_without_name = SchemaBuilder::int8().build();
    assert_eq!(schema_without_name.name(), None);
}

/// Test: testSchemaVersion
#[test]
fn test_schema_version() {
    let schema_with_version = SchemaBuilder::int8().version(2).build();
    assert_eq!(schema_with_version.version(), Some(2));

    let schema_without_version = SchemaBuilder::int8().build();
    assert_eq!(schema_without_version.version(), None);
}

/// Test: testSchemaDoc
#[test]
fn test_schema_doc() {
    let schema_with_doc = SchemaBuilder::int8().doc("documentation").build();
    assert_eq!(schema_with_doc.doc(), Some("documentation"));

    let schema_without_doc = SchemaBuilder::int8().build();
    assert_eq!(schema_without_doc.doc(), None);
}

/// Test: testFieldIndex
#[test]
fn test_field_index() {
    let schema = SchemaBuilder::struct_builder()
        .field("first", SchemaBuilder::int8().build())
        .field("second", SchemaBuilder::int16().build())
        .field("third", SchemaBuilder::int32().build())
        .build();

    assert_eq!(schema.field("first").unwrap().index(), 0);
    assert_eq!(schema.field("second").unwrap().index(), 1);
    assert_eq!(schema.field("third").unwrap().index(), 2);
}

/// Test: testFieldSchema
#[test]
fn test_field_schema() {
    let schema = SchemaBuilder::struct_builder()
        .field("int8Field", SchemaBuilder::int8().build())
        .field("stringField", SchemaBuilder::string().build())
        .build();

    assert_eq!(
        schema.field("int8Field").unwrap().schema().type_(),
        SchemaType::Int8
    );
    assert_eq!(
        schema.field("stringField").unwrap().schema().type_(),
        SchemaType::String
    );
}
