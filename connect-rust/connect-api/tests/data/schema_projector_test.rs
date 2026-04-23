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

use connect_api::data::{Schema, SchemaBuilder, SchemaProjector, SchemaType, Struct};
use connect_api::errors::{DataException, SchemaProjectorException};
use serde_json::{json, Value};
use std::collections::HashMap;

// =============================================================================
// Test 1: testPrimitiveTypeProjection - Primitive type projection tests
// =============================================================================
#[test]
fn test_primitive_type_projection() {
    // Boolean projection
    let bool_schema = SchemaBuilder::bool().build();
    let optional_bool_schema = SchemaBuilder::bool().optional().build();

    let projected = SchemaProjector::project(&bool_schema, &bool_schema, json!(false));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(false));

    let projected = SchemaProjector::project(&optional_bool_schema, &bool_schema, json!(false));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(false));

    // String projection
    let string_schema = SchemaBuilder::string().build();
    let optional_string_schema = SchemaBuilder::string().optional().build();

    let projected = SchemaProjector::project(&string_schema, &string_schema, json!("abc"));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!("abc"));

    let projected = SchemaProjector::project(&optional_string_schema, &string_schema, json!("abc"));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!("abc"));

    // Optional to required without default should fail
    let result = SchemaProjector::project(&bool_schema, &optional_bool_schema, json!(null));
    assert!(result.is_err());
}

// =============================================================================
// Test 2: testNumericTypeProjection - Numeric type promotion tests
// =============================================================================
#[test]
fn test_numeric_type_projection() {
    // INT8 to INT16 promotion
    let int8_schema = SchemaBuilder::int8().build();
    let int16_schema = SchemaBuilder::int16().build();

    let projected = SchemaProjector::project(&int16_schema, &int8_schema, json!(127));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(127));

    // INT8 to INT32 promotion
    let int32_schema = SchemaBuilder::int32().build();
    let projected = SchemaProjector::project(&int32_schema, &int8_schema, json!(127));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(127));

    // INT8 to INT64 promotion
    let int64_schema = SchemaBuilder::int64().build();
    let projected = SchemaProjector::project(&int64_schema, &int8_schema, json!(127));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(127));

    // INT8 to FLOAT32 promotion
    let float32_schema = SchemaBuilder::float32().build();
    let projected = SchemaProjector::project(&float32_schema, &int8_schema, json!(127));
    assert!(projected.is_ok());

    // INT8 to FLOAT64 promotion
    let float64_schema = SchemaBuilder::float64().build();
    let projected = SchemaProjector::project(&float64_schema, &int8_schema, json!(127));
    assert!(projected.is_ok());

    // INT16 to INT32 promotion
    let projected = SchemaProjector::project(&int32_schema, &int16_schema, json!(255));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(255));

    // INT32 to INT64 promotion
    let projected = SchemaProjector::project(&int64_schema, &int32_schema, json!(32767));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(32767));

    // FLOAT32 to FLOAT64 promotion
    let projected = SchemaProjector::project(&float64_schema, &float32_schema, json!(1.2));
    assert!(projected.is_ok());

    // Non-promotable types should fail
    let result = SchemaProjector::project(&bool_schema, &int8_schema, json!(127));
    assert!(result.is_err());
}

// =============================================================================
// Test 3: testPrimitiveOptionalProjection - Optional primitive projection
// =============================================================================
#[test]
fn test_primitive_optional_projection() {
    let optional_int8_schema = SchemaBuilder::int8().optional().build();
    let int32_with_default = SchemaBuilder::int32().default_value(json!(12789)).build();

    // Project optional INT8 to INT32 with default value
    let projected = SchemaProjector::project(&int32_with_default, &optional_int8_schema, json!(12));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(12));

    // Project null from optional to schema with default
    let projected =
        SchemaProjector::project(&int32_with_default, &optional_int8_schema, json!(null));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(12789));

    // Optional INT8 to optional INT32
    let optional_int32_schema = SchemaBuilder::int32().optional().build();
    let projected =
        SchemaProjector::project(&optional_int32_schema, &optional_int8_schema, json!(12));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(12));

    let projected =
        SchemaProjector::project(&optional_int32_schema, &optional_int8_schema, json!(null));
    assert!(projected.is_ok());
    assert!(projected.unwrap().is_null());
}

// =============================================================================
// Test 4: testStructAddField - Add field to struct during projection
// =============================================================================
#[test]
fn test_struct_add_field() {
    let source_schema = SchemaBuilder::struct_()
        .field("field", SchemaBuilder::int32().build())
        .build();

    let source_struct = Struct::new(source_schema.clone()).put("field", 1);

    let target_schema = SchemaBuilder::struct_()
        .field("field", SchemaBuilder::int32().build())
        .field(
            "field2",
            SchemaBuilder::int32().default_value(json!(123)).build(),
        )
        .build();

    let result = SchemaProjector::project(&target_schema, &source_schema, source_struct.to_json());
    assert!(result.is_ok());
    let projected = result.unwrap();

    // Verify field1 is preserved
    if let Value::Object(obj) = projected {
        assert_eq!(obj.get("field"), Some(&json!(1)));
        assert_eq!(obj.get("field2"), Some(&json!(123)));
    } else {
        panic!("Expected object");
    }

    // Incompatible schema (no default value for new field) should fail
    let incompatible_target = SchemaBuilder::struct_()
        .field("field", SchemaBuilder::int32().build())
        .field("field2", SchemaBuilder::int32().build())
        .build();

    let result = SchemaProjector::project(
        &incompatible_target,
        &source_schema,
        source_struct.to_json(),
    );
    assert!(result.is_err());
}

// =============================================================================
// Test 5: testStructRemoveField - Remove field from struct during projection
// =============================================================================
#[test]
fn test_struct_remove_field() {
    let source_schema = SchemaBuilder::struct_()
        .field("field", SchemaBuilder::int32().build())
        .field("field2", SchemaBuilder::int32().build())
        .build();

    let source_struct = Struct::new(source_schema.clone())
        .put("field", 1)
        .put("field2", 234);

    let target_schema = SchemaBuilder::struct_()
        .field("field", SchemaBuilder::int32().build())
        .build();

    let result = SchemaProjector::project(&target_schema, &source_schema, source_struct.to_json());
    assert!(result.is_ok());
    let projected = result.unwrap();

    // Verify only field1 is present
    if let Value::Object(obj) = projected {
        assert_eq!(obj.get("field"), Some(&json!(1)));
        assert!(obj.get("field2").is_none());
    } else {
        panic!("Expected object");
    }
}

// =============================================================================
// Test 6: testStructDefaultValue - Struct with default value
// =============================================================================
#[test]
fn test_struct_default_value() {
    let source_schema = SchemaBuilder::struct_()
        .optional()
        .field("field", SchemaBuilder::int32().build())
        .field("field2", SchemaBuilder::int32().build())
        .build();

    let default_struct = json!({"field": 12, "field2": 345});
    let target_schema = SchemaBuilder::struct_()
        .field("field", SchemaBuilder::int32().build())
        .field("field2", SchemaBuilder::int32().build())
        .default_value(default_struct.clone())
        .build();

    // Project null to struct with default
    let projected = SchemaProjector::project(&target_schema, &source_schema, json!(null));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), default_struct);

    // Project actual struct
    let source_struct = json!({"field": 45, "field2": 678});
    let projected = SchemaProjector::project(&target_schema, &source_schema, source_struct.clone());
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), source_struct);
}

// =============================================================================
// Test 7: testNestedSchemaProjection - Nested struct projection
// =============================================================================
#[test]
fn test_nested_schema_projection() {
    let source_flat_schema = SchemaBuilder::struct_()
        .field("field", SchemaBuilder::int32().build())
        .build();

    let target_flat_schema = SchemaBuilder::struct_()
        .field("field", SchemaBuilder::int32().build())
        .field(
            "field2",
            SchemaBuilder::int32().default_value(json!(123)).build(),
        )
        .build();

    let source_nested_schema = SchemaBuilder::struct_()
        .field("first", SchemaBuilder::int32().build())
        .field("second", SchemaBuilder::string().build())
        .field(
            "array",
            SchemaBuilder::array(SchemaBuilder::int32().build()).build(),
        )
        .field(
            "map",
            SchemaBuilder::map(
                SchemaBuilder::int32().build(),
                SchemaBuilder::string().build(),
            )
            .build(),
        )
        .field("nested", source_flat_schema.clone())
        .build();

    let target_nested_schema = SchemaBuilder::struct_()
        .field("first", SchemaBuilder::int32().build())
        .field("second", SchemaBuilder::string().build())
        .field(
            "array",
            SchemaBuilder::array(SchemaBuilder::int32().build()).build(),
        )
        .field(
            "map",
            SchemaBuilder::map(
                SchemaBuilder::int32().build(),
                SchemaBuilder::string().build(),
            )
            .build(),
        )
        .field("nested", target_flat_schema.clone())
        .build();

    let source_nested_struct = json!({
        "first": 1,
        "second": "abc",
        "array": [1, 2],
        "map": {"5": "def"},
        "nested": {"field": 113}
    });

    let result = SchemaProjector::project(
        &target_nested_schema,
        &source_nested_schema,
        source_nested_struct,
    );
    assert!(result.is_ok());
    let projected = result.unwrap();

    if let Value::Object(obj) = projected {
        assert_eq!(obj.get("first"), Some(&json!(1)));
        assert_eq!(obj.get("second"), Some(&json!("abc")));

        // Check nested struct has field2 added with default value
        if let Some(Value::Object(nested)) = obj.get("nested") {
            assert_eq!(nested.get("field"), Some(&json!(113)));
            assert_eq!(nested.get("field2"), Some(&json!(123)));
        } else {
            panic!("Expected nested object");
        }
    } else {
        panic!("Expected object");
    }
}

// =============================================================================
// Test 8: testLogicalTypeProjection - Logical type projection
// =============================================================================
#[test]
fn test_logical_type_projection() {
    use connect_api::data::{Date, Decimal, Time, Timestamp};

    // Decimal projection
    let decimal_schema = Decimal::schema(2);
    let projected = SchemaProjector::project(&decimal_schema, &decimal_schema, json!("1.56"));
    assert!(projected.is_ok());

    // Date projection
    let date_schema = Date::schema();
    let projected = SchemaProjector::project(&date_schema, &date_schema, json!(1000));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(1000));

    // Time projection
    let time_schema = Time::schema();
    let projected = SchemaProjector::project(&time_schema, &time_schema, json!(231));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(231));

    // Timestamp projection
    let timestamp_schema = Timestamp::schema();
    let projected = SchemaProjector::project(&timestamp_schema, &timestamp_schema, json!(34567));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!(34567));

    // Cannot project logical types to non-logical types
    let bool_schema = SchemaBuilder::bool().build();
    let result = SchemaProjector::project(&bool_schema, &date_schema, json!(1000));
    assert!(result.is_err());
}

// =============================================================================
// Test 9: testArrayProjection - Array projection
// =============================================================================
#[test]
fn test_array_projection() {
    let source_schema = SchemaBuilder::array(SchemaBuilder::int32().build()).build();

    let projected = SchemaProjector::project(&source_schema, &source_schema, json!([1, 2, 3]));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!([1, 2, 3]));

    // Optional array to array with default
    let optional_source = SchemaBuilder::array(SchemaBuilder::int32().build())
        .optional()
        .build();
    let target_with_default = SchemaBuilder::array(SchemaBuilder::int32().build())
        .default_value(json!([1, 2, 3]))
        .build();

    let projected = SchemaProjector::project(&target_with_default, &optional_source, json!([4, 5]));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!([4, 5]));

    let projected = SchemaProjector::project(&target_with_default, &optional_source, json!(null));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!([1, 2, 3]));

    // Promoted array element type
    let promoted_target = SchemaBuilder::array(SchemaBuilder::int64().build())
        .default_value(json!([1, 2, 3]))
        .build();

    let projected = SchemaProjector::project(&promoted_target, &optional_source, json!([4, 5]));
    assert!(projected.is_ok());
    // Elements should be promoted to INT64

    // No default value should fail for null
    let no_default_target = SchemaBuilder::array(SchemaBuilder::int32().build()).build();
    let result = SchemaProjector::project(&no_default_target, &optional_source, json!(null));
    assert!(result.is_err());
}

// =============================================================================
// Test 10: testMapProjection - Map projection
// =============================================================================
#[test]
fn test_map_projection() {
    let source_schema = SchemaBuilder::map(
        SchemaBuilder::int32().build(),
        SchemaBuilder::int32().build(),
    )
    .optional()
    .build();

    let target_schema = SchemaBuilder::map(
        SchemaBuilder::int32().build(),
        SchemaBuilder::int32().build(),
    )
    .default_value(json!({"1": 2}))
    .build();

    let projected = SchemaProjector::project(&target_schema, &source_schema, json!({"3": 4}));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!({"3": 4}));

    let projected = SchemaProjector::project(&target_schema, &source_schema, json!(null));
    assert!(projected.is_ok());
    assert_eq!(projected.unwrap(), json!({"1": 2}));

    // Promoted key/value types
    let promoted_target = SchemaBuilder::map(
        SchemaBuilder::int64().build(),
        SchemaBuilder::float32().build(),
    )
    .default_value(json!({"3": 4.5}))
    .build();

    let projected = SchemaProjector::project(&promoted_target, &source_schema, json!({"3": 4}));
    assert!(projected.is_ok());

    // No default value should fail for null
    let no_default_target = SchemaBuilder::map(
        SchemaBuilder::int32().build(),
        SchemaBuilder::int32().build(),
    )
    .build();

    let result = SchemaProjector::project(&no_default_target, &source_schema, json!(null));
    assert!(result.is_err());
}

// =============================================================================
// Test 11: testMaybeCompatible - Schema compatibility check
// =============================================================================
#[test]
fn test_maybe_compatible() {
    let source_schema = SchemaBuilder::int32().name("source").build();
    let target_schema = SchemaBuilder::int32().name("target").build();

    // Name mismatch should fail
    let result = SchemaProjector::project(&target_schema, &source_schema, json!(12));
    assert!(result.is_err());

    // Parameters mismatch
    let mut params = HashMap::new();
    params.insert("key", "value");

    let target_with_params = SchemaBuilder::int32().parameters(params).build();
    let result = SchemaProjector::project(&target_with_params, &source_schema, json!(34));
    assert!(result.is_err());
}

// =============================================================================
// Test 12: testProjectMissingDefaultValuedStructField
// =============================================================================
#[test]
fn test_project_missing_default_valued_struct_field() {
    let source_schema = SchemaBuilder::struct_().build();
    let target_schema = SchemaBuilder::struct_()
        .field(
            "id",
            SchemaBuilder::int64().default_value(json!(42)).build(),
        )
        .build();

    let result = SchemaProjector::project(&target_schema, &source_schema, json!({}));
    assert!(result.is_ok());
    let projected = result.unwrap();

    if let Value::Object(obj) = projected {
        assert_eq!(obj.get("id"), Some(&json!(42)));
    } else {
        panic!("Expected object");
    }
}

// =============================================================================
// Test 13: testProjectMissingOptionalStructField
// =============================================================================
#[test]
fn test_project_missing_optional_struct_field() {
    let source_schema = SchemaBuilder::struct_().build();
    let target_schema = SchemaBuilder::struct_()
        .field("id", SchemaBuilder::int64().optional().build())
        .build();

    let result = SchemaProjector::project(&target_schema, &source_schema, json!({}));
    assert!(result.is_ok());
    let projected = result.unwrap();

    if let Value::Object(obj) = projected {
        assert!(obj.get("id").is_none() || obj.get("id") == Some(&json!(null)));
    } else {
        panic!("Expected object");
    }
}

// =============================================================================
// Test 14: testProjectMissingRequiredField
// =============================================================================
#[test]
fn test_project_missing_required_field() {
    let source_schema = SchemaBuilder::struct_().build();
    let target_schema = SchemaBuilder::struct_()
        .field("id", SchemaBuilder::int64().build())
        .build();

    // Required field missing should fail
    let result = SchemaProjector::project(&target_schema, &source_schema, json!({}));
    assert!(result.is_err());
}
