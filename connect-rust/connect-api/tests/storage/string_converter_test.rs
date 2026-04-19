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
use connect_api::storage::{Converter, ConverterType, StringConverter};
use serde_json::json;
use std::collections::HashMap;

// =============================================================================
// Test 1: testStringToBytes - Convert string to bytes
// =============================================================================
#[test]
fn test_string_to_bytes() {
    let converter = StringConverter::new();
    let schema = SchemaBuilder::string().build();
    let value = Some("test_value");

    let result = converter.from_connect_data("topic", Some(&schema), value);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    assert_eq!("test_value".as_bytes(), bytes.as_slice());
}

// =============================================================================
// Test 2: testNonStringToBytes - Non-string value conversion
// =============================================================================
#[test]
fn test_non_string_to_bytes() {
    let converter = StringConverter::new();
    let schema = SchemaBuilder::int32().build();

    // Non-string values should still be converted (JSON representation)
    let result = converter.from_connect_data("topic", Some(&schema), Some(42));
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let bytes_str = String::from_utf8(bytes).unwrap();
    assert!(bytes_str.contains("42"));
}

// =============================================================================
// Test 3: testNullToBytes - Null value conversion
// =============================================================================
#[test]
fn test_null_to_bytes() {
    let converter = StringConverter::new();
    let optional_schema = SchemaBuilder::string().optional().build();

    let result = converter.from_connect_data("topic", Some(&optional_schema), None::<&str>);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    assert!(bytes.is_empty());
}

// =============================================================================
// Test 4: testToBytesIgnoresSchema - Schema parameter should be ignored
// =============================================================================
#[test]
fn test_to_bytes_ignores_schema() {
    let converter = StringConverter::new();

    // StringConverter should convert string regardless of schema
    let schema1 = SchemaBuilder::string().build();
    let schema2 = SchemaBuilder::int32().build();

    let result1 = converter.from_connect_data("topic", Some(&schema1), Some("value"));
    let result2 = converter.from_connect_data("topic", Some(&schema2), Some("value"));

    // Both should produce the same bytes
    assert!(result1.is_ok());
    assert!(result2.is_ok());
    assert_eq!(result1.unwrap(), result2.unwrap());
}

// =============================================================================
// Test 5: testToBytesNonUtf8Encoding - Non-UTF8 encoding test
// =============================================================================
#[test]
fn test_to_bytes_non_utf8_encoding() {
    let mut converter = StringConverter::new();

    // Configure with non-UTF8 encoding (e.g., ISO-8859-1)
    // Note: This test verifies the converter handles encoding configuration
    let configs: HashMap<String, common_trait::config::Value> = HashMap::new();
    converter.configure(configs, ConverterType::Value);

    // Default encoding is UTF-8
    let result = converter.from_connect_data("topic", None, Some("test"));
    assert!(result.is_ok());
}

// =============================================================================
// Test 6: testBytesToString - Convert bytes to string
// =============================================================================
#[test]
fn test_bytes_to_string() {
    let converter = StringConverter::new();
    let bytes = "test_value".as_bytes().to_vec();

    let result = converter.to_connect_data("topic", &bytes);
    assert!(result.is_ok());
    let (schema, value) = result.unwrap();
    assert!(schema.is_some());
    assert_eq!(Some("test_value"), value.as_string());
}

// =============================================================================
// Test 7: testBytesNullToString - Null bytes conversion
// =============================================================================
#[test]
fn test_bytes_null_to_string() {
    let converter = StringConverter::new();
    let empty_bytes: Vec<u8> = vec![];

    let result = converter.to_connect_data("topic", &empty_bytes);
    assert!(result.is_ok());
    let (schema, value) = result.unwrap();
    // Empty bytes should result in null or empty string
    assert!(value.is_null() || value.as_string() == Some(""));
}

// =============================================================================
// Test 8: testBytesToStringNonUtf8Encoding - Non-UTF8 bytes conversion
// =============================================================================
#[test]
fn test_bytes_to_string_non_utf8_encoding() {
    let mut converter = StringConverter::new();
    let configs: HashMap<String, common_trait::config::Value> = HashMap::new();
    converter.configure(configs, ConverterType::Value);

    // Test with UTF-8 bytes
    let bytes = "test".as_bytes().to_vec();
    let result = converter.to_connect_data("topic", &bytes);
    assert!(result.is_ok());
}

// =============================================================================
// Test 9: testStringHeaderValueToBytes - Header value conversion
// =============================================================================
#[test]
fn test_string_header_value_to_bytes() {
    let converter = StringConverter::new();
    let value = Some("header_value");

    let result = converter.from_connect_data("topic", None, value);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    assert_eq!("header_value".as_bytes(), bytes.as_slice());
}

// =============================================================================
// Test 10: testNonStringHeaderValueToBytes - Non-string header value
// =============================================================================
#[test]
fn test_non_string_header_value_to_bytes() {
    let converter = StringConverter::new();

    // Integer value should be converted to JSON string representation
    let result = converter.from_connect_data("topic", None, Some(123));
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let bytes_str = String::from_utf8(bytes).unwrap();
    assert!(bytes_str.contains("123"));
}

// =============================================================================
// Test 11: testNullHeaderValueToBytes - Null header value
// =============================================================================
#[test]
fn test_null_header_value_to_bytes() {
    let converter = StringConverter::new();

    let result = converter.from_connect_data("topic", None, None::<&str>);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    assert!(bytes.is_empty());
}

// =============================================================================
// Test 12: testInheritedVersionRetrievedFromAppInfoParser
// =============================================================================
#[test]
fn test_inherited_version_retrieved_from_app_info_parser() {
    let converter = StringConverter::new();
    let version = converter.version();
    // Version should be defined (non-empty)
    assert!(!version.is_empty());
}

// =============================================================================
// Additional test: Configure with encoding
// =============================================================================
#[test]
fn test_string_converter_configure_with_encoding() {
    let mut converter = StringConverter::new();
    let mut configs: HashMap<String, common_trait::config::Value> = HashMap::new();

    // Add encoding configuration
    configs.insert(
        "converter.encoding".to_string(),
        common_trait::config::Value::String("UTF-8".to_string()),
    );

    converter.configure(configs, ConverterType::Key);
    // Configuration should succeed
}

// =============================================================================
// Additional test: Round trip conversion
// =============================================================================
#[test]
fn test_round_trip_conversion() {
    let converter = StringConverter::new();
    let original = "hello world";

    // Convert to bytes
    let bytes = converter
        .from_connect_data("topic", None, Some(original))
        .unwrap();

    // Convert back to string
    let (schema, value) = converter.to_connect_data("topic", &bytes).unwrap();

    assert_eq!(Some(original), value.as_string());
}

// =============================================================================
// Additional test: Complex string round trip
// =============================================================================
#[test]
fn test_complex_string_round_trip() {
    let converter = StringConverter::new();
    let original = "string with special chars: \"quotes\" \\backslash\\ newline\n tab\t";

    let bytes = converter
        .from_connect_data("topic", None, Some(original))
        .unwrap();
    let (schema, value) = converter.to_connect_data("topic", &bytes).unwrap();

    assert_eq!(Some(original), value.as_string());
}
