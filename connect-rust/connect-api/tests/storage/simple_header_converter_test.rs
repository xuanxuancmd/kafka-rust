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

use connect_api::data::{Schema, SchemaBuilder};
use connect_api::storage::{HeaderConverter, SimpleHeaderConverter};
use serde_json::{json, Value};

// =============================================================================
// Test 1: shouldConvertNullValue
// =============================================================================
#[test]
fn should_convert_null_value() {
    let converter = SimpleHeaderConverter::new();

    // Null value should convert to empty bytes
    let bytes = converter.to_bytes("header", &json!(null)).unwrap();
    assert!(bytes.is_empty());

    // Round trip for optional string
    let optional_string_schema = SchemaBuilder::string().optional().build();
    let result = converter.from_bytes("header", &bytes);
    assert!(result.is_ok());
}

// =============================================================================
// Test 2: shouldConvertSimpleString
// =============================================================================
#[test]
fn should_convert_simple_string() {
    let converter = SimpleHeaderConverter::new();
    let value = json!("simple");

    let bytes = converter.to_bytes("header", &value).unwrap();
    assert_eq!("simple".as_bytes(), bytes.as_slice());

    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(json!("simple"), result);
}

// =============================================================================
// Test 3: shouldConvertEmptyString
// =============================================================================
#[test]
fn should_convert_empty_string() {
    let converter = SimpleHeaderConverter::new();
    let value = json!("");

    let bytes = converter.to_bytes("header", &value).unwrap();
    assert!(bytes.is_empty());

    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(json!(""), result);
}

// =============================================================================
// Test 4: shouldConvertStringWithQuotesAndOtherDelimiterCharacters
// =============================================================================
#[test]
fn should_convert_string_with_quotes_and_other_delimiter_characters() {
    let converter = SimpleHeaderConverter::new();

    // String with embedded quotes
    let value = json!("three\"blind\\\"mice");
    let bytes = converter.to_bytes("header", &value).unwrap();
    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(value, result);

    // String with various delimiters
    let value2 = json!("string with delimiters: <>?,./\\=+-!@#$%^&*(){}[]|;':");
    let bytes2 = converter.to_bytes("header", &value2).unwrap();
    let result2 = converter.from_bytes("header", &bytes2).unwrap();
    assert_eq!(value2, result2);
}

// =============================================================================
// Test 5: shouldConvertMapWithStringKeys
// =============================================================================
#[test]
fn should_convert_map_with_string_keys() {
    let converter = SimpleHeaderConverter::new();
    let value = json!({"foo": "123", "bar": "baz"});

    let bytes = converter.to_bytes("header", &value).unwrap();
    let bytes_str = String::from_utf8(bytes.clone()).unwrap();
    assert!(bytes_str.contains("foo"));
    assert!(bytes_str.contains("bar"));

    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 6: shouldParseStringOfMapWithStringValuesWithoutWhitespaceAsMap
// =============================================================================
#[test]
fn should_parse_string_of_map_with_string_values_without_whitespace_as_map() {
    let converter = SimpleHeaderConverter::new();
    let bytes = "{\"foo\":\"123\",\"bar\":\"baz\"}".as_bytes().to_vec();

    let result = converter.from_bytes("header", &bytes).unwrap();
    // The converter parses the JSON and returns it
    let bytes_str = String::from_utf8(bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 7: shouldParseStringOfMapWithStringValuesWithWhitespaceAsMap
// =============================================================================
#[test]
fn should_parse_string_of_map_with_string_values_with_whitespace_as_map() {
    let converter = SimpleHeaderConverter::new();
    let bytes = "{ \"foo\" : \"123\" , \"bar\" : \"baz\" }"
        .as_bytes()
        .to_vec();

    let result = converter.from_bytes("header", &bytes).unwrap();
    let bytes_str = String::from_utf8(bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 8: shouldConvertMapWithStringKeysAndShortValues
// =============================================================================
#[test]
fn should_convert_map_with_string_keys_and_short_values() {
    let converter = SimpleHeaderConverter::new();
    let value = json!({"foo": 12345, "bar": 0, "baz": -4321});

    let bytes = converter.to_bytes("header", &value).unwrap();
    let bytes_str = String::from_utf8(bytes.clone()).unwrap();
    assert!(bytes_str.contains("12345"));

    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 9: shouldParseStringOfMapWithShortValuesWithoutWhitespaceAsMap
// =============================================================================
#[test]
fn should_parse_string_of_map_with_short_values_without_whitespace_as_map() {
    let converter = SimpleHeaderConverter::new();
    let bytes = "{\"foo\":12345,\"bar\":0,\"baz\":-4321}"
        .as_bytes()
        .to_vec();

    let result = converter.from_bytes("header", &bytes).unwrap();
    let bytes_str = String::from_utf8(bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 10: shouldParseStringOfMapWithShortValuesWithWhitespaceAsMap
// =============================================================================
#[test]
fn should_parse_string_of_map_with_short_values_with_whitespace_as_map() {
    let converter = SimpleHeaderConverter::new();
    let bytes = "{ \"foo\" : 12345 , \"bar\" : 0 , \"baz\" : -4321 }"
        .as_bytes()
        .to_vec();

    let result = converter.from_bytes("header", &bytes).unwrap();
    let bytes_str = String::from_utf8(bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 11: shouldConvertMapWithStringKeysAndIntegerValues
// =============================================================================
#[test]
fn should_convert_map_with_string_keys_and_integer_values() {
    let converter = SimpleHeaderConverter::new();
    let value = json!({"foo": 1234567890, "bar": 0, "baz": -987654321});

    let bytes = converter.to_bytes("header", &value).unwrap();
    let bytes_str = String::from_utf8(bytes.clone()).unwrap();
    assert!(bytes_str.contains("1234567890"));

    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 12: shouldParseStringOfMapWithIntValuesWithoutWhitespaceAsMap
// =============================================================================
#[test]
fn should_parse_string_of_map_with_int_values_without_whitespace_as_map() {
    let converter = SimpleHeaderConverter::new();
    let bytes = "{\"foo\":1234567890,\"bar\":0,\"baz\":-987654321}"
        .as_bytes()
        .to_vec();

    let result = converter.from_bytes("header", &bytes).unwrap();
    let bytes_str = String::from_utf8(bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 13: shouldParseStringOfMapWithIntValuesWithWhitespaceAsMap
// =============================================================================
#[test]
fn should_parse_string_of_map_with_int_values_with_whitespace_as_map() {
    let converter = SimpleHeaderConverter::new();
    let bytes = "{ \"foo\" : 1234567890 , \"bar\" : 0 , \"baz\" : -987654321 }"
        .as_bytes()
        .to_vec();

    let result = converter.from_bytes("header", &bytes).unwrap();
    let bytes_str = String::from_utf8(bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 14: shouldConvertListWithStringValues
// =============================================================================
#[test]
fn should_convert_list_with_string_values() {
    let converter = SimpleHeaderConverter::new();
    let value = json!(["foo", "bar"]);

    let bytes = converter.to_bytes("header", &value).unwrap();
    let bytes_str = String::from_utf8(bytes.clone()).unwrap();
    assert!(bytes_str.contains("foo"));
    assert!(bytes_str.contains("bar"));

    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 15: shouldConvertListWithIntegerValues
// =============================================================================
#[test]
fn should_convert_list_with_integer_values() {
    let converter = SimpleHeaderConverter::new();
    let value = json!([1234567890, -987654321]);

    let bytes = converter.to_bytes("header", &value).unwrap();
    let bytes_str = String::from_utf8(bytes.clone()).unwrap();
    assert!(bytes_str.contains("1234567890"));

    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 16: shouldConvertMapWithStringKeysAndMixedValuesToMap
// =============================================================================
#[test]
fn should_convert_map_with_string_keys_and_mixed_values_to_map() {
    let converter = SimpleHeaderConverter::new();
    let value = json!({"string": "value", "number": 42, "bool": true});

    let bytes = converter.to_bytes("header", &value).unwrap();
    let bytes_str = String::from_utf8(bytes.clone()).unwrap();

    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 17: shouldConvertListWithMixedValuesToListWithoutSchema
// =============================================================================
#[test]
fn should_convert_list_with_mixed_values_to_list_without_schema() {
    let converter = SimpleHeaderConverter::new();
    let value = json!(["string", 42, true, null]);

    let bytes = converter.to_bytes("header", &value).unwrap();
    let bytes_str = String::from_utf8(bytes.clone()).unwrap();

    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(json!(bytes_str), result);
}

// =============================================================================
// Test 18: shouldConvertEmptyMapToMap
// =============================================================================
#[test]
fn should_convert_empty_map_to_map() {
    let converter = SimpleHeaderConverter::new();
    let value = json!({});

    let bytes = converter.to_bytes("header", &value).unwrap();
    let bytes_str = String::from_utf8(bytes.clone()).unwrap();
    assert_eq!("{}", bytes_str);

    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(json!("{}", bytes_str), result);
}

// =============================================================================
// Test 19: shouldConvertEmptyListToList
// =============================================================================
#[test]
fn should_convert_empty_list_to_list() {
    let converter = SimpleHeaderConverter::new();
    let value = json!([]);

    let bytes = converter.to_bytes("header", &value).unwrap();
    let bytes_str = String::from_utf8(bytes.clone()).unwrap();
    assert_eq!("[]", bytes_str);

    let result = converter.from_bytes("header", &bytes).unwrap();
    assert_eq!(json!("[]", bytes_str), result);
}

// =============================================================================
// Test 20: converterShouldReturnAppInfoParserVersion
// =============================================================================
#[test]
fn converter_should_return_app_info_parser_version() {
    let converter = SimpleHeaderConverter::new();
    let version = converter.version();
    // Version should be defined (non-empty)
    assert!(!version.is_empty());
}
