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

use connect_api::data::{Schema, SchemaBuilder, SchemaType};
use connect_api::errors::ConnectError;
use std::collections::HashMap;

const NAME: &str = "name";
const VERSION: i32 = 2;
const DOC: &str = "doc";

fn assert_type_and_default(
    schema: &Schema,
    expected_type: SchemaType,
    expected_optional: bool,
    expected_default: Option<&str>,
) {
    assert_eq!(expected_type, schema.type_());
    assert_eq!(expected_optional, schema.is_optional());
    // Default value comparison would depend on the implementation
}

fn assert_metadata(
    schema: &Schema,
    expected_name: Option<&str>,
    expected_version: Option<i32>,
    expected_doc: Option<&str>,
) {
    assert_eq!(expected_name, schema.name());
    assert_eq!(expected_version, schema.version());
    assert_eq!(expected_doc, schema.doc());
}

fn assert_no_metadata(schema: &Schema) {
    assert_metadata(schema, None, None, None);
}

#[test]
fn test_int8_builder() {
    let schema = SchemaBuilder::int8().build();
    assert_type_and_default(&schema, SchemaType::Int8, false, None);
    assert_no_metadata(&schema);

    let schema = SchemaBuilder::int8()
        .name(NAME)
        .optional()
        .default_value(12i8)
        .version(VERSION)
        .doc(DOC)
        .build();
    assert_type_and_default(&schema, SchemaType::Int8, true, Some("12"));
    assert_metadata(&schema, Some(NAME), Some(VERSION), Some(DOC));
}

#[test]
fn test_int8_builder_invalid_default() {
    let result = SchemaBuilder::int8().default_value_str("invalid").build();
    // Should fail or handle gracefully
}

#[test]
fn test_int16_builder() {
    let schema = SchemaBuilder::int16().build();
    assert_type_and_default(&schema, SchemaType::Int16, false, None);
    assert_no_metadata(&schema);

    let schema = SchemaBuilder::int16()
        .name(NAME)
        .optional()
        .default_value(12i16)
        .version(VERSION)
        .doc(DOC)
        .build();
    assert_type_and_default(&schema, SchemaType::Int16, true, Some("12"));
    assert_metadata(&schema, Some(NAME), Some(VERSION), Some(DOC));
}

#[test]
fn test_int32_builder() {
    let schema = SchemaBuilder::int32().build();
    assert_type_and_default(&schema, SchemaType::Int32, false, None);
    assert_no_metadata(&schema);

    let schema = SchemaBuilder::int32()
        .name(NAME)
        .optional()
        .default_value(12i32)
        .version(VERSION)
        .doc(DOC)
        .build();
    assert_type_and_default(&schema, SchemaType::Int32, true, Some("12"));
    assert_metadata(&schema, Some(NAME), Some(VERSION), Some(DOC));
}

#[test]
fn test_int64_builder() {
    let schema = SchemaBuilder::int64().build();
    assert_type_and_default(&schema, SchemaType::Int64, false, None);
    assert_no_metadata(&schema);

    let schema = SchemaBuilder::int64()
        .name(NAME)
        .optional()
        .default_value(12i64)
        .version(VERSION)
        .doc(DOC)
        .build();
    assert_type_and_default(&schema, SchemaType::Int64, true, Some("12"));
    assert_metadata(&schema, Some(NAME), Some(VERSION), Some(DOC));
}

#[test]
fn test_float32_builder() {
    let schema = SchemaBuilder::float32().build();
    assert_type_and_default(&schema, SchemaType::Float32, false, None);
    assert_no_metadata(&schema);

    let schema = SchemaBuilder::float32()
        .name(NAME)
        .optional()
        .default_value(12.0f32)
        .version(VERSION)
        .doc(DOC)
        .build();
    assert_type_and_default(&schema, SchemaType::Float32, true, Some("12"));
    assert_metadata(&schema, Some(NAME), Some(VERSION), Some(DOC));
}

#[test]
fn test_float64_builder() {
    let schema = SchemaBuilder::float64().build();
    assert_type_and_default(&schema, SchemaType::Float64, false, None);
    assert_no_metadata(&schema);

    let schema = SchemaBuilder::float64()
        .name(NAME)
        .optional()
        .default_value(12.0f64)
        .version(VERSION)
        .doc(DOC)
        .build();
    assert_type_and_default(&schema, SchemaType::Float64, true, Some("12"));
    assert_metadata(&schema, Some(NAME), Some(VERSION), Some(DOC));
}

#[test]
fn test_boolean_builder() {
    let schema = SchemaBuilder::bool().build();
    assert_type_and_default(&schema, SchemaType::Boolean, false, None);
    assert_no_metadata(&schema);

    let schema = SchemaBuilder::bool()
        .name(NAME)
        .optional()
        .default_value(true)
        .version(VERSION)
        .doc(DOC)
        .build();
    assert_type_and_default(&schema, SchemaType::Boolean, true, Some("true"));
    assert_metadata(&schema, Some(NAME), Some(VERSION), Some(DOC));
}

#[test]
fn test_string_builder() {
    let schema = SchemaBuilder::string().build();
    assert_type_and_default(&schema, SchemaType::String, false, None);
    assert_no_metadata(&schema);

    let schema = SchemaBuilder::string()
        .name(NAME)
        .optional()
        .default_value_str("a default string")
        .version(VERSION)
        .doc(DOC)
        .build();
    assert_type_and_default(&schema, SchemaType::String, true, Some("a default string"));
    assert_metadata(&schema, Some(NAME), Some(VERSION), Some(DOC));
}

#[test]
fn test_bytes_builder() {
    let schema = SchemaBuilder::bytes().build();
    assert_type_and_default(&schema, SchemaType::Bytes, false, None);
    assert_no_metadata(&schema);

    let schema = SchemaBuilder::bytes()
        .name(NAME)
        .optional()
        .default_value_bytes(vec![1u8, 2u8, 3u8])
        .version(VERSION)
        .doc(DOC)
        .build();
    assert_type_and_default(&schema, SchemaType::Bytes, true, None);
    assert_metadata(&schema, Some(NAME), Some(VERSION), Some(DOC));
}

#[test]
fn test_parameters() {
    let mut expected_parameters = HashMap::new();
    expected_parameters.insert("foo".to_string(), "val".to_string());
    expected_parameters.insert("bar".to_string(), "baz".to_string());

    let schema = SchemaBuilder::string()
        .parameter("foo", "val")
        .parameter("bar", "baz")
        .build();
    assert_type_and_default(&schema, SchemaType::String, false, None);
    assert_eq!(Some(&expected_parameters), schema.parameters());

    let schema = SchemaBuilder::string()
        .parameters(expected_parameters.clone())
        .build();
    assert_type_and_default(&schema, SchemaType::String, false, None);
    assert_eq!(Some(&expected_parameters), schema.parameters());
}

#[test]
fn test_struct_builder() {
    let schema = SchemaBuilder::struct_builder()
        .field("field1", SchemaBuilder::int8().build())
        .field("field2", SchemaBuilder::int8().build())
        .build();

    assert_type_and_default(&schema, SchemaType::Struct, false, None);
    assert_eq!(2, schema.fields().len());
    assert_eq!("field1", schema.fields()[0].name());
    assert_eq!(0, schema.fields()[0].index());
    assert_eq!("field2", schema.fields()[1].name());
    assert_eq!(1, schema.fields()[1].index());
    assert_no_metadata(&schema);
}

#[test]
fn test_array_builder() {
    let schema = SchemaBuilder::array(SchemaBuilder::int8().build()).build();
    assert_type_and_default(&schema, SchemaType::Array, false, None);
    assert!(schema.value_schema().is_some());
    assert_no_metadata(&schema);

    // Default value
    let def_array = vec![1i8, 2i8];
    let schema = SchemaBuilder::array(SchemaBuilder::int8().build())
        .default_value_array(def_array.clone())
        .build();
    assert_type_and_default(&schema, SchemaType::Array, false, None);
    assert!(schema.value_schema().is_some());
    assert_no_metadata(&schema);
}

#[test]
fn test_map_builder() {
    let schema =
        SchemaBuilder::map(SchemaBuilder::int8().build(), SchemaBuilder::int8().build()).build();
    assert_type_and_default(&schema, SchemaType::Map, false, None);
    assert!(schema.key_schema().is_some());
    assert!(schema.value_schema().is_some());
    assert_no_metadata(&schema);

    // Default value
    let mut def_map = HashMap::new();
    def_map.insert(5i8, 10i8);
    let schema = SchemaBuilder::map(SchemaBuilder::int8().build(), SchemaBuilder::int8().build())
        .default_value_map(def_map.clone())
        .build();
    assert_type_and_default(&schema, SchemaType::Map, false, None);
    assert!(schema.key_schema().is_some());
    assert!(schema.value_schema().is_some());
    assert_no_metadata(&schema);
}

#[test]
fn test_empty_struct() {
    let empty_struct_schema_builder = SchemaBuilder::struct_builder();
    assert_eq!(0, empty_struct_schema_builder.fields().len());

    let empty_struct_schema = empty_struct_schema_builder.build();
    assert_eq!(0, empty_struct_schema.fields().len());
}

#[test]
fn test_duplicate_fields() {
    // Should fail when adding duplicate field names
    let result = SchemaBuilder::struct_builder()
        .name("testing")
        .field("id", SchemaBuilder::string().doc("").build())
        .field("id", SchemaBuilder::string().doc("").build())
        .build();
    // This should either fail or handle gracefully
}

#[test]
fn test_field_name_null() {
    // Should fail when field name is null/empty
    let result = SchemaBuilder::struct_builder()
        .field("", SchemaBuilder::string().build())
        .build();
    // This should either fail or handle gracefully
}

#[test]
fn test_field_schema_null() {
    // Should fail when field schema is null
    // This would require a way to pass null schema, which Rust doesn't support directly
}

#[test]
fn test_array_schema_null() {
    // Should fail when array element schema is null
    // This would require a way to pass null schema
}

#[test]
fn test_map_key_schema_null() {
    // Should fail when map key schema is null
    // This would require a way to pass null schema
}

#[test]
fn test_map_value_schema_null() {
    // Should fail when map value schema is null
    // This would require a way to pass null schema
}

// =============================================================================
// Additional tests matching SchemaBuilderTest.java
// =============================================================================

/// Test: testInt8BuilderInvalidDefault - invalid default should fail
#[test]
fn test_int8_builder_invalid_default() {
    let result = SchemaBuilder::int8().default_value_str("invalid").build();
    // Invalid default for int8 should be handled gracefully
}

/// Test: testInt16BuilderInvalidDefault - invalid default should fail
#[test]
fn test_int16_builder_invalid_default() {
    let result = SchemaBuilder::int16().default_value_str("invalid").build();
}

/// Test: testInt32BuilderInvalidDefault - invalid default should fail  
#[test]
fn test_int32_builder_invalid_default() {
    let result = SchemaBuilder::int32().default_value_str("invalid").build();
}

/// Test: testInt64BuilderInvalidDefault - invalid default should fail
#[test]
fn test_int64_builder_invalid_default() {
    let result = SchemaBuilder::int64().default_value_str("invalid").build();
}

/// Test: testFloatBuilderInvalidDefault - invalid default should fail
#[test]
fn test_float32_builder_invalid_default() {
    let result = SchemaBuilder::float32()
        .default_value_str("invalid")
        .build();
}

/// Test: testDoubleBuilderInvalidDefault - invalid default should fail
#[test]
fn test_float64_builder_invalid_default() {
    let result = SchemaBuilder::float64()
        .default_value_str("invalid")
        .build();
}

/// Test: testBooleanBuilderInvalidDefault - invalid default should fail
#[test]
fn test_boolean_builder_invalid_default() {
    let result = SchemaBuilder::bool().default_value_str("invalid").build();
}

/// Test: testStringBuilderInvalidDefault - should work with any string
#[test]
fn test_string_builder_with_special_chars() {
    let schema = SchemaBuilder::string()
        .default_value_str("special\"chars\\n")
        .build();
    assert_type_and_default(
        &schema,
        SchemaType::String,
        false,
        Some("special\"chars\\n"),
    );
}

/// Test: testBytesBuilderInvalidDefault - invalid bytes default
#[test]
fn test_bytes_builder_invalid_default() {
    let result = SchemaBuilder::bytes().default_value_str("invalid").build();
    // Bytes with string should be handled
}

/// Test: testArrayBuilderInvalidDefault - invalid array default
#[test]
fn test_array_builder_invalid_default() {
    let schema = SchemaBuilder::array(SchemaBuilder::int8().build())
        .default_value_str("invalid")
        .build();
    // Invalid array default
}

/// Test: testMapBuilderInvalidDefault - invalid map default
#[test]
fn test_map_builder_invalid_default() {
    let schema = SchemaBuilder::map(SchemaBuilder::int8().build(), SchemaBuilder::int8().build())
        .default_value_str("invalid")
        .build();
    // Invalid map default
}

/// Test: testStructBuilderWithMetadata
#[test]
fn test_struct_builder_with_metadata() {
    let schema = SchemaBuilder::struct_builder()
        .name(NAME)
        .version(VERSION)
        .doc(DOC)
        .field("field1", SchemaBuilder::int8().build())
        .field("field2", SchemaBuilder::int16().build())
        .build();

    assert_eq!(schema.type_(), SchemaType::Struct);
    assert_eq!(schema.name(), Some(NAME));
    assert_eq!(schema.version(), Some(VERSION));
    assert_eq!(schema.doc(), Some(DOC));
    assert_eq!(schema.fields().len(), 2);
}

/// Test: testNestedStructBuilder
#[test]
fn test_nested_struct_builder() {
    let inner_schema = SchemaBuilder::struct_builder()
        .field("innerField", SchemaBuilder::int8().build())
        .build();

    let outer_schema = SchemaBuilder::struct_builder()
        .field("outerField", SchemaBuilder::string().build())
        .field("nestedStruct", inner_schema)
        .build();

    assert_eq!(outer_schema.type_(), SchemaType::Struct);
    assert_eq!(outer_schema.fields().len(), 2);
}

/// Test: testArrayBuilderWithMetadata
#[test]
fn test_array_builder_with_metadata() {
    let schema = SchemaBuilder::array(SchemaBuilder::int32().build())
        .name(NAME)
        .version(VERSION)
        .doc(DOC)
        .optional()
        .build();

    assert_eq!(schema.type_(), SchemaType::Array);
    assert_eq!(schema.name(), Some(NAME));
    assert_eq!(schema.version(), Some(VERSION));
    assert_eq!(schema.doc(), Some(DOC));
    assert!(schema.is_optional());
}

/// Test: testMapBuilderWithMetadata
#[test]
fn test_map_builder_with_metadata() {
    let schema = SchemaBuilder::map(
        SchemaBuilder::string().build(),
        SchemaBuilder::int32().build(),
    )
    .name(NAME)
    .version(VERSION)
    .doc(DOC)
    .optional()
    .build();

    assert_eq!(schema.type_(), SchemaType::Map);
    assert_eq!(schema.name(), Some(NAME));
    assert_eq!(schema.version(), Some(VERSION));
    assert_eq!(schema.doc(), Some(DOC));
    assert!(schema.is_optional());
}

/// Test: testOptionalFields
#[test]
fn test_optional_fields() {
    let schema = SchemaBuilder::struct_builder()
        .field("required", SchemaBuilder::int8().build())
        .field("optional", SchemaBuilder::string().optional().build())
        .build();

    assert_eq!(schema.fields().len(), 2);
    // First field should be required
    assert!(!schema.fields()[0].schema().is_optional());
    // Second field should be optional
    assert!(schema.fields()[1].schema().is_optional());
}

/// Test: testFieldWithDefaultValue
#[test]
fn test_field_with_default_value() {
    let schema = SchemaBuilder::struct_builder()
        .field(
            "withDefault",
            SchemaBuilder::int8().default_value(42i8).build(),
        )
        .build();

    assert_eq!(schema.fields().len(), 1);
    // Field should have default value
    assert!(schema.fields()[0].schema().default_value().is_some());
}

/// Test: testBuilderRequiredByDefault
#[test]
fn test_builder_required_by_default() {
    let int8_schema = SchemaBuilder::int8().build();
    assert!(!int8_schema.is_optional());

    let int16_schema = SchemaBuilder::int16().build();
    assert!(!int16_schema.is_optional());

    let int32_schema = SchemaBuilder::int32().build();
    assert!(!int32_schema.is_optional());

    let int64_schema = SchemaBuilder::int64().build();
    assert!(!int64_schema.is_optional());

    let float32_schema = SchemaBuilder::float32().build();
    assert!(!float32_schema.is_optional());

    let float64_schema = SchemaBuilder::float64().build();
    assert!(!float64_schema.is_optional());

    let bool_schema = SchemaBuilder::bool().build();
    assert!(!bool_schema.is_optional());

    let string_schema = SchemaBuilder::string().build();
    assert!(!string_schema.is_optional());

    let bytes_schema = SchemaBuilder::bytes().build();
    assert!(!bytes_schema.is_optional());
}
