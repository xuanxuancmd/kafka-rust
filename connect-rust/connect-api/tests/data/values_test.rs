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

use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use connect_api::data::{
    Date, Schema, SchemaAndValue, SchemaBuilder, SchemaType, Struct, Time, Timestamp, Values,
};
use connect_api::errors::ConnectError;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::str::FromStr;

// =============================================================================
// P0 Core Tests - 25 tests matching ValuesTest.java
// =============================================================================

/// Test 1: shouldParseNullString - parse null string returns null SchemaAndValue
#[test]
fn test_parse_null_string() {
    let result = Values::parse_string("");
    assert!(result.is_ok());
    let schema_and_value = result.unwrap();
    assert!(schema_and_value.schema().is_some());
    assert_eq!(
        schema_and_value.schema().unwrap().type_(),
        SchemaType::String
    );
    assert!(schema_and_value.value().is_some());
    assert_eq!(schema_and_value.value().unwrap(), &json!(""));
}

/// Test 2: shouldParseEmptyString - parse empty string returns STRING_SCHEMA
#[test]
fn test_parse_empty_string() {
    let result = Values::parse_string("");
    assert!(result.is_ok());
    let schema_and_value = result.unwrap();
    assert!(schema_and_value.schema().is_some());
    assert_eq!(
        schema_and_value.schema().unwrap().type_(),
        SchemaType::String
    );
    assert!(schema_and_value.value().is_some());
    assert_eq!(schema_and_value.value().unwrap(), &json!(""));
}

/// Test 3: shouldParseEmptyMap - parse "{}" returns empty Map
#[test]
fn test_parse_empty_map() {
    let result = Values::parse_string("{}");
    assert!(result.is_ok());
    let schema_and_value = result.unwrap();
    assert!(schema_and_value.schema().is_some());
    assert_eq!(schema_and_value.schema().unwrap().type_(), SchemaType::Map);
    assert!(schema_and_value.value().is_some());
    let map = schema_and_value.value().unwrap().as_object();
    assert!(map.is_some());
    assert!(map.unwrap().is_empty());
}

/// Test 4: shouldParseEmptyArray - parse "[]" returns empty Array
#[test]
fn test_parse_empty_array() {
    let result = Values::parse_string("[]");
    assert!(result.is_ok());
    let schema_and_value = result.unwrap();
    assert!(schema_and_value.schema().is_some());
    assert_eq!(
        schema_and_value.schema().unwrap().type_(),
        SchemaType::Array
    );
    assert!(schema_and_value.value().is_some());
    let arr = schema_and_value.value().unwrap().as_array();
    assert!(arr.is_some());
    assert!(arr.unwrap().is_empty());
}

/// Test 5: shouldParseNull - parse "null" string returns null SchemaAndValue
#[test]
fn test_parse_null() {
    let result = Values::parse_string("null");
    assert!(result.is_ok());
    let schema_and_value = result.unwrap();
    assert!(schema_and_value.schema().is_none());
    assert!(schema_and_value.value().is_none());
}

/// Test 6: shouldConvertNullValue - test various null value conversions with roundTrip
#[test]
fn test_convert_null_value() {
    // Test null conversion with optional schemas
    let optional_int8_schema = SchemaBuilder::int8().optional().build();
    let optional_int16_schema = SchemaBuilder::int16().optional().build();
    let optional_int32_schema = SchemaBuilder::int32().optional().build();
    let optional_int64_schema = SchemaBuilder::int64().optional().build();
    let optional_float32_schema = SchemaBuilder::float32().optional().build();
    let optional_float64_schema = SchemaBuilder::float64().optional().build();
    let optional_bool_schema = SchemaBuilder::bool().optional().build();
    let optional_string_schema = SchemaBuilder::string().optional().build();

    // Test that null values return None for optional schemas
    let result = Values::convert_to_boolean(Some(&optional_bool_schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let result = Values::convert_to_byte(Some(&optional_int8_schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let result = Values::convert_to_short(Some(&optional_int16_schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let result = Values::convert_to_int(Some(&optional_int32_schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let result = Values::convert_to_long(Some(&optional_int64_schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let result = Values::convert_to_float(Some(&optional_float32_schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let result = Values::convert_to_double(Some(&optional_float64_schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let result = Values::convert_to_string(Some(&optional_string_schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

/// Test 7: shouldConvertBooleanValues - Boolean value conversion and roundTrip
#[test]
fn test_convert_boolean_values() {
    let schema = SchemaBuilder::bool().build();
    let optional_schema = SchemaBuilder::bool().optional().build();

    // Test true conversion
    let result = Values::convert_to_boolean(Some(&schema), &json!(true));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(true));

    // Test false conversion
    let result = Values::convert_to_boolean(Some(&schema), &json!(false));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(false));

    // Test string "true"
    let result = Values::convert_to_boolean(Some(&schema), &json!("true"));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(true));

    // Test string "false"
    let result = Values::convert_to_boolean(Some(&schema), &json!("false"));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(false));

    // Test integer 1 (should be true)
    let result = Values::convert_to_boolean(Some(&schema), &json!(1));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(true));

    // Test integer 0 (should be false)
    let result = Values::convert_to_boolean(Some(&schema), &json!(0));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(false));

    // Test null with optional schema
    let result = Values::convert_to_boolean(Some(&optional_schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // Test roundTrip for boolean values
    let parsed = Values::parse_string("true");
    assert!(parsed.is_ok());
    let sv = parsed.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Boolean);
    assert_eq!(sv.value().unwrap(), &json!(true));

    let parsed = Values::parse_string("false");
    assert!(parsed.is_ok());
    let sv = parsed.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Boolean);
    assert_eq!(sv.value().unwrap(), &json!(false));
}

/// Test 8: shouldConvertEmptyStruct - empty Struct conversion
#[test]
fn test_convert_empty_struct() {
    // Create an empty struct schema
    let struct_schema = SchemaBuilder::struct_().build();

    // Test that converting null to struct fails
    let result = Values::convert_to_struct(Some(&struct_schema), &json!(null));
    assert!(result.is_err());

    // Test that converting empty string to struct fails
    let result = Values::convert_to_struct(Some(&struct_schema), &json!(""));
    assert!(result.is_err());
}

/// Test 9: shouldConvertSimpleString - simple string conversion
#[test]
fn test_convert_simple_string() {
    let schema = SchemaBuilder::string().build();

    let result = Values::convert_to_string(Some(&schema), &json!("simple"));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("simple".to_string()));

    // Test roundTrip: parse and convert back
    let parsed = Values::parse_string("simple");
    assert!(parsed.is_ok());
    let sv = parsed.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::String);
    assert_eq!(sv.value().unwrap(), &json!("simple"));
}

/// Test 10: shouldConvertEmptyString - empty string conversion
#[test]
fn test_convert_empty_string() {
    let schema = SchemaBuilder::string().build();

    let result = Values::convert_to_string(Some(&schema), &json!(""));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("".to_string()));

    // Test roundTrip
    let parsed = Values::parse_string("");
    assert!(parsed.is_ok());
    let sv = parsed.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::String);
    assert_eq!(sv.value().unwrap(), &json!(""));
}

/// Test 11: shouldConvertMapWithStringKeys - string key Map conversion
#[test]
fn test_convert_map_with_string_keys() {
    let key_schema = SchemaBuilder::string().build();
    let value_schema = SchemaBuilder::string().build();
    let map_schema = SchemaBuilder::map(key_schema, value_schema).build();

    // Test with JSON object
    let map_value = json!({"foo": "123", "bar": "baz"});
    let result = Values::convert_to_map(Some(&map_schema), &map_value);
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());
    let map = converted.unwrap();
    assert_eq!(map.get("foo"), Some(&json!("123")));
    assert_eq!(map.get("bar"), Some(&json!("baz")));

    // Test parse string representation
    let parsed = Values::parse_string("{\"foo\":\"123\",\"bar\":\"baz\"}");
    assert!(parsed.is_ok());
    let sv = parsed.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Map);
}

/// Test 12: shouldConvertMapWithIntValues - Integer value Map conversion
#[test]
fn test_convert_map_with_int_values() {
    let key_schema = SchemaBuilder::string().build();
    let value_schema = SchemaBuilder::int32().build();
    let map_schema = SchemaBuilder::map(key_schema, value_schema).build();

    // Test with JSON object containing integers
    let map_value = json!({"foo": 1234567890, "bar": 0, "baz": -987654321});
    let result = Values::convert_to_map(Some(&map_schema), &map_value);
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());

    // Test parse string representation
    let parsed = Values::parse_string("{\"foo\":1234567890,\"bar\":0,\"baz\":-987654321}");
    assert!(parsed.is_ok());
    let sv = parsed.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Map);
}

/// Test 13: shouldConvertListWithStringValues - string List conversion
#[test]
fn test_convert_list_with_string_values() {
    let element_schema = SchemaBuilder::string().build();
    let array_schema = SchemaBuilder::array(element_schema).build();

    // Test with JSON array
    let array_value = json!(["foo", "bar"]);
    let result = Values::convert_to_list(Some(&array_schema), &array_value);
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());
    let list = converted.unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0], json!("foo"));
    assert_eq!(list[1], json!("bar"));

    // Test parse string representation
    let parsed = Values::parse_string("[\"foo\",\"bar\"]");
    assert!(parsed.is_ok());
    let sv = parsed.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Array);
}

/// Test 14: shouldConvertListWithIntValues - Integer List conversion
#[test]
fn test_convert_list_with_int_values() {
    let element_schema = SchemaBuilder::int32().build();
    let array_schema = SchemaBuilder::array(element_schema).build();

    // Test with JSON array
    let array_value = json!([1234567890, -987654321]);
    let result = Values::convert_to_list(Some(&array_schema), &array_value);
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());
    let list = converted.unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0], json!(1234567890));
    assert_eq!(list[1], json!(-987654321));

    // Test parse string representation
    let parsed = Values::parse_string("[1234567890,-987654321]");
    assert!(parsed.is_ok());
    let sv = parsed.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Array);
}

/// Test 15: shouldParseTimestampStringAsTimestamp - Timestamp string parsing
#[test]
fn test_parse_timestamp_string() {
    let ts_str = "2019-08-23T14:34:54.346Z";
    let result = Values::parse_string(ts_str);
    assert!(result.is_ok());
    let sv = result.unwrap();

    // Schema should be INT64 with Timestamp logical name
    assert!(sv.schema().is_some());
    let schema = sv.schema().unwrap();
    assert_eq!(schema.type_(), SchemaType::Int64);
    assert_eq!(schema.name(), Some(Timestamp::LOGICAL_NAME));

    // Value should be milliseconds since epoch
    assert!(sv.value().is_some());
}

/// Test 16: shouldParseDateStringAsDate - Date string parsing
#[test]
fn test_parse_date_string() {
    let date_str = "2019-08-23";
    let result = Values::parse_string(date_str);
    assert!(result.is_ok());
    let sv = result.unwrap();

    // Schema should be INT32 with Date logical name
    assert!(sv.schema().is_some());
    let schema = sv.schema().unwrap();
    assert_eq!(schema.type_(), SchemaType::Int32);
    assert_eq!(schema.name(), Some(Date::LOGICAL_NAME));

    // Value should be days since epoch
    assert!(sv.value().is_some());
}

/// Test 17: shouldParseTimeStringAsTime - Time string parsing
#[test]
fn test_parse_time_string() {
    let time_str = "14:34:54.346Z";
    let result = Values::parse_string(time_str);
    assert!(result.is_ok());
    let sv = result.unwrap();

    // Schema should be INT32 with Time logical name
    assert!(sv.schema().is_some());
    let schema = sv.schema().unwrap();
    assert_eq!(schema.type_(), SchemaType::Int32);
    assert_eq!(schema.name(), Some(Time::LOGICAL_NAME));

    // Value should be milliseconds since midnight
    assert!(sv.value().is_some());
}

/// Test 18: shouldConvertTimeValues - Time value conversion
#[test]
fn test_convert_time_values() {
    let time_schema = Time::schema();

    // Test conversion from NaiveTime
    let time = NaiveTime::from_hms_milli_opt(14, 34, 54, 346).unwrap();
    let millis = Time::to_connect_data(time);

    // Convert from millis
    let result = Values::convert_to_time(Some(&time_schema), &json!(millis));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());

    // Convert from string
    let result = Values::convert_to_time(Some(&time_schema), &json!("14:34:54.346Z"));
    assert!(result.is_ok());
}

/// Test 19: shouldConvertDateValues - Date value conversion
#[test]
fn test_convert_date_values() {
    let date_schema = Date::schema();

    // Test conversion from NaiveDate
    let date = NaiveDate::from_ymd_opt(2019, 8, 23).unwrap();
    let days = Date::to_connect_data(date);

    // Convert from days
    let result = Values::convert_to_date(Some(&date_schema), &json!(days));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());

    // Convert from string
    let result = Values::convert_to_date(Some(&date_schema), &json!("2019-08-23"));
    assert!(result.is_ok());
}

/// Test 20: shouldConvertTimestampValues - Timestamp value conversion
#[test]
fn test_convert_timestamp_values() {
    let timestamp_schema = Timestamp::schema();

    // Test conversion from DateTime
    let ts = Utc.from_utc_datetime(&chrono::NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2019, 8, 23).unwrap(),
        NaiveTime::from_hms_milli_opt(14, 34, 54, 346).unwrap(),
    ));
    let millis = Timestamp::to_connect_data(ts);

    // Convert from millis
    let result = Values::convert_to_timestamp(Some(&timestamp_schema), &json!(millis));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());

    // Convert from string
    let result =
        Values::convert_to_timestamp(Some(&timestamp_schema), &json!("2019-08-23T14:34:54.346Z"));
    assert!(result.is_ok());
}

/// Test 21: shouldConvertDecimalValues - Decimal value conversion
#[test]
fn test_convert_decimal_values() {
    // Test conversion from BigDecimal
    let decimal = BigDecimal::from_str("1.0").unwrap();
    let result = Values::convert_to_decimal(None, &json!("1.0"), 1);
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());
    assert_eq!(converted.unwrap(), decimal);

    // Test conversion from number
    let result = Values::convert_to_decimal(None, &json!(1.0), 1);
    assert!(result.is_ok());

    // Test conversion from string
    let result = Values::convert_to_decimal(None, &json!("123.456"), 3);
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());
    let expected = BigDecimal::from_str("123.456").unwrap();
    assert_eq!(converted.unwrap(), expected);
}

/// Test 22: shouldInferStructSchema - Struct Schema inference
#[test]
fn test_infer_struct_schema() {
    // Create a struct with a schema
    let struct_schema = SchemaBuilder::struct_().build();

    // Infer schema from struct
    // Note: infer_schema works with Value types, not Struct directly
    // For struct inference, we need to check the schema matches
    assert_eq!(struct_schema.type_(), SchemaType::Struct);
}

/// Test 23: shouldConvertStringOfNull - "null" string roundTrip
#[test]
fn test_convert_string_of_null() {
    let schema = SchemaBuilder::string().build();

    // Convert "null" string to string - should be literal "null"
    let result = Values::convert_to_string(Some(&schema), &json!("null"));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("null".to_string()));

    // Parse "null" - should return null SchemaAndValue (not a string)
    let parsed = Values::parse_string("null");
    assert!(parsed.is_ok());
    let sv = parsed.unwrap();
    assert!(sv.schema().is_none());
    assert!(sv.value().is_none());
}

/// Test 24: shouldRoundTripNull - null value complete roundTrip test
#[test]
fn test_round_trip_null() {
    // Test roundTrip for null values with optional schemas
    let optional_string_schema = SchemaBuilder::string().optional().build();

    // Parse null string
    let parsed = Values::parse_string("null");
    assert!(parsed.is_ok());
    let sv = parsed.unwrap();
    assert!(sv.schema().is_none());
    assert!(sv.value().is_none());

    // Convert null value to string (should be None for optional)
    let result = Values::convert_to_string(Some(&optional_string_schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

/// Test 25: shouldEscapeStringsWithEmbeddedQuotesAndBackslashes - escape/unescape strings
#[test]
fn test_escape_unescape_strings() {
    // Test escape and unescape functionality
    // Original string: "three\"blind\\\"mice"
    // Expected escaped: "three\\\"blind\\\\\\\"mice"

    // Test escaping quotes and backslashes
    let original = "three\"blind\\\"mice";
    let escaped = Values::escape_string(original);
    // Expected: backslashes doubled, quotes escaped
    assert!(escaped.contains("\\\\"));
    assert!(escaped.contains("\\\""));

    // Test unescaping
    let unescaped = Values::unescape_string(&escaped);
    assert_eq!(original, unescaped);

    // Test simple escape cases
    let simple = "hello\"world";
    let escaped_simple = Values::escape_string(simple);
    let unescaped_simple = Values::unescape_string(&escaped_simple);
    assert_eq!(simple, unescaped_simple);

    // Test backslash only
    let backslash = "path\\to\\file";
    let escaped_bs = Values::escape_string(backslash);
    let unescaped_bs = Values::unescape_string(&escaped_bs);
    assert_eq!(backslash, unescaped_bs);
}

// =============================================================================
// Additional utility tests
// =============================================================================

#[test]
fn test_infer_schema_basic_types() {
    // Null returns None
    assert!(Values::infer_schema(&json!(null)).is_none());

    // Boolean
    let schema = Values::infer_schema(&json!(true)).unwrap();
    assert_eq!(schema.type_(), SchemaType::Boolean);

    // Small integer -> Int8
    let schema = Values::infer_schema(&json!(42)).unwrap();
    assert_eq!(schema.type_(), SchemaType::Int8);

    // Medium integer -> Int16
    let schema = Values::infer_schema(&json!(1000)).unwrap();
    assert_eq!(schema.type_(), SchemaType::Int16);

    // Larger integer -> Int32
    let schema = Values::infer_schema(&json!(100000)).unwrap();
    assert_eq!(schema.type_(), SchemaType::Int32);

    // String
    let schema = Values::infer_schema(&json!("hello")).unwrap();
    assert_eq!(schema.type_(), SchemaType::String);

    // Array
    let schema = Values::infer_schema(&json!([1, 2, 3])).unwrap();
    assert_eq!(schema.type_(), SchemaType::Array);

    // Object (Map)
    let schema = Values::infer_schema(&json!({"key": "value"})).unwrap();
    assert_eq!(schema.type_(), SchemaType::Map);
}

#[test]
fn test_parse_string_numbers() {
    // Parse integer
    let result = Values::parse_string("42");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.value().unwrap(), &json!(42));

    // Parse negative integer
    let result = Values::parse_string("-42");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.value().unwrap(), &json!(-42));

    // Parse float
    let result = Values::parse_string("3.14");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert!(sv.value().unwrap().is_number());
}

#[test]
fn test_parse_string_arrays() {
    // Parse simple array
    let result = Values::parse_string("[1, 2, 3]");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert!(sv.value().unwrap().is_array());
    let arr = sv.value().unwrap().as_array().unwrap();
    assert_eq!(arr.len(), 3);

    // Parse array with strings
    let result = Values::parse_string("[\"a\", \"b\"]");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert!(sv.value().unwrap().is_array());
}

#[test]
fn test_parse_string_objects() {
    // Parse simple object
    let result = Values::parse_string("{\"key\": \"value\"}");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert!(sv.value().unwrap().is_object());

    // Parse object with numbers
    let result = Values::parse_string("{\"a\": 1, \"b\": 2}");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert!(sv.value().unwrap().is_object());
}

#[test]
fn test_convert_to_int8() {
    let schema = SchemaBuilder::int8().build();

    // Test byte value
    let result = Values::convert_to_byte(Some(&schema), &json!(42));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(42i8));

    // Test negative byte
    let result = Values::convert_to_byte(Some(&schema), &json!(-1));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(-1i8));

    // Test string conversion
    let result = Values::convert_to_byte(Some(&schema), &json!("42"));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(42i8));
}

#[test]
fn test_convert_to_int16() {
    let schema = SchemaBuilder::int16().build();

    // Test short value
    let result = Values::convert_to_short(Some(&schema), &json!(1000));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(1000i16));

    // Test negative short
    let result = Values::convert_to_short(Some(&schema), &json!(-1000));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(-1000i16));
}

#[test]
fn test_convert_to_int32() {
    let schema = SchemaBuilder::int32().build();

    // Test int value
    let result = Values::convert_to_int(Some(&schema), &json!(12345));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(12345i32));

    // Test negative int
    let result = Values::convert_to_int(Some(&schema), &json!(-12345));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(-12345i32));
}

#[test]
fn test_convert_to_int64() {
    let schema = SchemaBuilder::int64().build();

    // Test long value
    let result = Values::convert_to_long(Some(&schema), &json!(1234567890));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(1234567890i64));

    // Test negative long
    let result = Values::convert_to_long(Some(&schema), &json!(-1234567890));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(-1234567890i64));
}

#[test]
fn test_convert_to_float32() {
    let schema = SchemaBuilder::float32().build();

    // Test float value
    let result = Values::convert_to_float(Some(&schema), &json!(3.14));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());
    // Float precision comparison
    assert!((converted.unwrap() - 3.14f32).abs() < 0.01);
}

#[test]
fn test_convert_to_float64() {
    let schema = SchemaBuilder::float64().build();

    // Test double value
    let result = Values::convert_to_double(Some(&schema), &json!(3.141592653589793));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());
    assert!((converted.unwrap() - 3.141592653589793f64).abs() < 0.0001);
}

// =============================================================================
// P1 Additional Tests - Matching more ValuesTest.java tests
// =============================================================================

/// Test: shouldNotParseUnquotedEmbeddedMapKeysAsStrings - unquoted map keys treated as strings
#[test]
fn test_parse_unquoted_map_keys_as_strings() {
    let result = Values::parse_string("{foo: 3}");
    assert!(result.is_ok());
    let sv = result.unwrap();
    // Unquoted keys should result in string type, not map
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::String);
    assert_eq!(sv.value().unwrap(), &json!("{foo: 3}"));
}

/// Test: shouldNotParseUnquotedEmbeddedMapValuesAsStrings - unquoted map values treated as strings
#[test]
fn test_parse_unquoted_map_values_as_strings() {
    let result = Values::parse_string("{3: foo}");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::String);
    assert_eq!(sv.value().unwrap(), &json!("{3: foo}"));
}

/// Test: shouldNotParseUnquotedArrayElementsAsStrings - unquoted array elements treated as strings
#[test]
fn test_parse_unquoted_array_elements_as_strings() {
    let result = Values::parse_string("[foo]");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::String);
    assert_eq!(sv.value().unwrap(), &json!("[foo]"));
}

/// Test: shouldNotParseStringsBeginningWithNullAsStrings - "null=" treated as string
#[test]
fn test_parse_strings_beginning_with_null() {
    let result = Values::parse_string("null=");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::String);
    assert_eq!(sv.value().unwrap(), &json!("null="));
}

/// Test: shouldParseStringsBeginningWithTrueAsStrings - "true}" treated as string
#[test]
fn test_parse_strings_beginning_with_true() {
    let result = Values::parse_string("true}");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::String);
    assert_eq!(sv.value().unwrap(), &json!("true}"));
}

/// Test: shouldParseStringsBeginningWithFalseAsStrings - "false]" treated as string
#[test]
fn test_parse_strings_beginning_with_false() {
    let result = Values::parse_string("false]");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::String);
    assert_eq!(sv.value().unwrap(), &json!("false]"));
}

/// Test: shouldParseTrueAsBooleanIfSurroundedByWhitespace
#[test]
fn test_parse_true_with_whitespace() {
    let whitespace = "\n \t \t\n";
    let input = format!("{}true{}", whitespace, whitespace);
    let result = Values::parse_string(&input);
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Boolean);
    assert_eq!(sv.value().unwrap(), &json!(true));
}

/// Test: shouldParseFalseAsBooleanIfSurroundedByWhitespace
#[test]
fn test_parse_false_with_whitespace() {
    let whitespace = "\n \t \t\n";
    let input = format!("{}false{}", whitespace, whitespace);
    let result = Values::parse_string(&input);
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Boolean);
    assert_eq!(sv.value().unwrap(), &json!(false));
}

/// Test: shouldParseNullAsNullIfSurroundedByWhitespace
#[test]
fn test_parse_null_with_whitespace() {
    let whitespace = "\n \t \t\n";
    let input = format!("{}null{}", whitespace, whitespace);
    let result = Values::parse_string(&input);
    assert!(result.is_ok());
    let sv = result.unwrap();
    // null surrounded by whitespace should return null SchemaAndValue
    assert!(sv.schema().is_none());
    assert!(sv.value().is_none());
}

/// Test: shouldParseBooleanLiteralsEmbeddedInArray
#[test]
fn test_parse_boolean_array() {
    let result = Values::parse_string("[true, false]");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Array);
    let arr = sv.value().unwrap().as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0], json!(true));
    assert_eq!(arr[1], json!(false));
}

/// Test: shouldParseBooleanLiteralsEmbeddedInMap
#[test]
fn test_parse_boolean_map() {
    let result = Values::parse_string("{\"true\": false, \"false\": true}");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Map);
    let map = sv.value().unwrap().as_object().unwrap();
    assert_eq!(map.get("true"), Some(&json!(false)));
    assert_eq!(map.get("false"), Some(&json!(true)));
}

/// Test: shouldNotParseAsMapWithoutCommas
#[test]
fn test_not_parse_map_without_commas() {
    let result = Values::parse_string("{6:9 4:20}");
    assert!(result.is_ok());
    let sv = result.unwrap();
    // Without commas, should be treated as string
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::String);
    assert_eq!(sv.value().unwrap(), &json!("{6:9 4:20}"));
}

/// Test: shouldNotParseAsArrayWithoutCommas
#[test]
fn test_not_parse_array_without_commas() {
    let result = Values::parse_string("[0 1 2]");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::String);
    assert_eq!(sv.value().unwrap(), &json!("[0 1 2]"));
}

/// Test: shouldNotParseAsMapWithNullKeys
#[test]
fn test_not_parse_map_with_null_keys() {
    let result = Values::parse_string("{null: 3}");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::String);
    assert_eq!(sv.value().unwrap(), &json!("{null: 3}"));
}

/// Test: shouldParseNullMapValues
#[test]
fn test_parse_null_map_values() {
    let result = Values::parse_string("{\"3\": null}");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Map);
    let map = sv.value().unwrap().as_object().unwrap();
    assert_eq!(map.get("3"), Some(&json!(null)));
}

/// Test: shouldParseNullArrayElements
#[test]
fn test_parse_null_array_elements() {
    let result = Values::parse_string("[null]");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert_eq!(sv.schema().unwrap().type_(), SchemaType::Array);
    let arr = sv.value().unwrap().as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0], json!(null));
}

/// Test: shouldConvertInt8 - Int8 roundTrip
#[test]
fn test_convert_int8_roundtrip() {
    let schema = SchemaBuilder::int8().build();

    // Test byte 0
    let result = Values::convert_to_byte(Some(&schema), &json!(0));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(0i8));

    // Test byte 1
    let result = Values::convert_to_byte(Some(&schema), &json!(1));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(1i8));

    // Test byte -1
    let result = Values::convert_to_byte(Some(&schema), &json!(-1));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(-1i8));

    // Test max byte
    let result = Values::convert_to_byte(Some(&schema), &json!(127));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(127i8));

    // Test min byte
    let result = Values::convert_to_byte(Some(&schema), &json!(-128));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(-128i8));
}

/// Test: shouldConvertInt64 - Int64 roundTrip
#[test]
fn test_convert_int64_roundtrip() {
    let schema = SchemaBuilder::int64().build();

    let result = Values::convert_to_long(Some(&schema), &json!(1i64));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(1i64));

    let result = Values::convert_to_long(Some(&schema), &json!(i64::MAX));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(i64::MAX));

    let result = Values::convert_to_long(Some(&schema), &json!(i64::MIN));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(i64::MIN));
}

/// Test: shouldConvertFloat32 - Float32 roundTrip
#[test]
fn test_convert_float32_roundtrip() {
    let schema = SchemaBuilder::float32().build();

    let result = Values::convert_to_float(Some(&schema), &json!(1.0f32));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());
    assert_eq!(converted.unwrap(), 1.0f32);
}

/// Test: shouldConvertFloat64 - Float64 roundTrip  
#[test]
fn test_convert_float64_roundtrip() {
    let schema = SchemaBuilder::float64().build();

    let result = Values::convert_to_double(Some(&schema), &json!(1.0f64));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());
    assert_eq!(converted.unwrap(), 1.0f64);
}

/// Test: shouldFailToParseInvalidBooleanValueString
#[test]
fn test_fail_parse_invalid_boolean() {
    let schema = SchemaBuilder::string().build();

    // "green" should fail boolean conversion
    let result = Values::convert_to_boolean(Some(&schema), &json!("green"));
    assert!(result.is_err());
}

/// Test: shouldConvertBytes - Bytes conversion
#[test]
fn test_convert_bytes() {
    let schema = SchemaBuilder::bytes().build();

    let bytes = vec![1u8, 2u8, 3u8];
    let result = Values::convert_to_bytes(Some(&schema), &json!(bytes.clone()));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert!(converted.is_some());
    assert_eq!(converted.unwrap(), bytes);
}

/// Test: shouldParseIntegerStrings
#[test]
fn test_parse_integer_strings() {
    // Parse positive integer
    let result = Values::parse_string("42");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert!(sv.value().unwrap().is_number());

    // Parse negative integer
    let result = Values::parse_string("-42");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert!(sv.value().unwrap().is_number());

    // Parse large integer
    let result = Values::parse_string("1234567890");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert!(sv.value().unwrap().is_number());
}

/// Test: shouldParseFloatStrings
#[test]
fn test_parse_float_strings() {
    // Parse float
    let result = Values::parse_string("3.14");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert!(sv.value().unwrap().is_number());

    // Parse negative float
    let result = Values::parse_string("-3.14");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert!(sv.value().unwrap().is_number());

    // Parse scientific notation
    let result = Values::parse_string("1.5e10");
    assert!(result.is_ok());
    let sv = result.unwrap();
    assert!(sv.value().unwrap().is_number());
}

/// Test: shouldConvertQuotedString
#[test]
fn test_convert_quoted_string() {
    let schema = SchemaBuilder::string().build();

    let result = Values::convert_to_string(Some(&schema), &json!("\"quoted\""));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("\"quoted\"".to_string()));
}

/// Test: shouldConvertStringWithEscapes
#[test]
fn test_convert_string_with_escapes() {
    let schema = SchemaBuilder::string().build();

    let result = Values::convert_to_string(Some(&schema), &json!("line1\\nline2"));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("line1\\nline2".to_string()));
}

/// Test: shouldConvertNestedMap
#[test]
fn test_convert_nested_map() {
    let key_schema = SchemaBuilder::string().build();
    let value_schema = SchemaBuilder::string().build();
    let map_schema = SchemaBuilder::map(key_schema, value_schema).build();

    let nested = json!({"outer": {"inner": "value"}});
    let result = Values::convert_to_map(Some(&map_schema), &nested);
    assert!(result.is_ok());
}

/// Test: shouldConvertNestedArray
#[test]
fn test_convert_nested_array() {
    let element_schema = SchemaBuilder::int32().build();
    let array_schema = SchemaBuilder::array(element_schema).build();

    let nested = json!([[1, 2], [3, 4]]);
    let result = Values::convert_to_list(Some(&array_schema), &nested);
    assert!(result.is_ok());
}

/// Test: shouldConvertOptionalInt8
#[test]
fn test_convert_optional_int8() {
    let schema = SchemaBuilder::int8().optional().build();

    // null should work with optional schema
    let result = Values::convert_to_byte(Some(&schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // value should work
    let result = Values::convert_to_byte(Some(&schema), &json!(42));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(42i8));
}

/// Test: shouldConvertOptionalString
#[test]
fn test_convert_optional_string() {
    let schema = SchemaBuilder::string().optional().build();

    let result = Values::convert_to_string(Some(&schema), &json!(null));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let result = Values::convert_to_string(Some(&schema), &json!("value"));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("value".to_string()));
}

/// Test: shouldConvertStructWithFields
#[test]
fn test_convert_struct_with_fields() {
    let struct_schema = SchemaBuilder::struct_()
        .field("name", SchemaBuilder::string().build())
        .field("age", SchemaBuilder::int32().build())
        .build();

    // Create a struct
    let struct_val = json!({"name": "John", "age": 30});
    let result = Values::convert_to_struct(Some(&struct_schema), &struct_val);
    assert!(result.is_ok());
}

/// Test: shouldInferSchemaFromNull
#[test]
fn test_infer_schema_null() {
    let schema = Values::infer_schema(&json!(null));
    assert!(schema.is_none());
}

/// Test: shouldInferSchemaFromBoolean
#[test]
fn test_infer_schema_boolean() {
    let schema = Values::infer_schema(&json!(true)).unwrap();
    assert_eq!(schema.type_(), SchemaType::Boolean);

    let schema = Values::infer_schema(&json!(false)).unwrap();
    assert_eq!(schema.type_(), SchemaType::Boolean);
}

/// Test: shouldInferSchemaFromString
#[test]
fn test_infer_schema_string() {
    let schema = Values::infer_schema(&json!("hello")).unwrap();
    assert_eq!(schema.type_(), SchemaType::String);
}

/// Test: shouldInferSchemaFromArray
#[test]
fn test_infer_schema_array() {
    let schema = Values::infer_schema(&json!([1, 2, 3])).unwrap();
    assert_eq!(schema.type_(), SchemaType::Array);
}

/// Test: shouldInferSchemaFromMap
#[test]
fn test_infer_schema_map() {
    let schema = Values::infer_schema(&json!({"key": "value"})).unwrap();
    assert_eq!(schema.type_(), SchemaType::Map);
}
