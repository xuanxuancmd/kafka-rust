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

//! Tests for json_converter_config module.

use connect_json::*;
use serde_json::Value;
use std::collections::HashMap;

#[test]
fn test_config_def_contains_all_keys() {
    let config_def = JsonConverterConfig::config_def();

    assert!(config_def.contains_key(SCHEMAS_ENABLE_CONFIG));
    assert!(config_def.contains_key(SCHEMAS_CACHE_SIZE_CONFIG));
    assert!(config_def.contains_key(SCHEMA_CONTENT_CONFIG));
    assert!(config_def.contains_key(DECIMAL_FORMAT_CONFIG));
    assert!(config_def.contains_key(REPLACE_NULL_WITH_DEFAULT_CONFIG));
}

#[test]
fn test_config_def_defaults() {
    let config_def = JsonConverterConfig::config_def();

    let schemas_enable = config_def.get(SCHEMAS_ENABLE_CONFIG).unwrap();
    assert_eq!(schemas_enable.default_value(), Some(&Value::Bool(true)));

    let schemas_cache_size = config_def.get(SCHEMAS_CACHE_SIZE_CONFIG).unwrap();
    assert_eq!(
        schemas_cache_size.default_value(),
        Some(&Value::Number(1000.into()))
    );

    let schema_content = config_def.get(SCHEMA_CONTENT_CONFIG).unwrap();
    assert!(schema_content.default_value().is_none());

    let decimal_format = config_def.get(DECIMAL_FORMAT_CONFIG).unwrap();
    assert_eq!(
        decimal_format.default_value(),
        Some(&Value::String("BASE64".to_string()))
    );

    let replace_null = config_def.get(REPLACE_NULL_WITH_DEFAULT_CONFIG).unwrap();
    assert_eq!(replace_null.default_value(), Some(&Value::Bool(true)));
}

#[test]
fn test_new_with_empty_props() {
    let props = HashMap::new();
    let config = JsonConverterConfig::new(props);

    // Should use default values
    assert_eq!(config.schemas_enabled(), SCHEMAS_ENABLE_DEFAULT);
    assert_eq!(config.schema_cache_size(), SCHEMAS_CACHE_SIZE_DEFAULT);
    assert_eq!(config.decimal_format(), DecimalFormat::Base64);
    assert_eq!(
        config.replace_null_with_default(),
        REPLACE_NULL_WITH_DEFAULT_DEFAULT
    );
    assert!(config.schema_content().is_none());
}

#[test]
fn test_new_with_custom_values() {
    let mut props = HashMap::new();
    props.insert(SCHEMAS_ENABLE_CONFIG.to_string(), Value::Bool(false));
    props.insert(
        SCHEMAS_CACHE_SIZE_CONFIG.to_string(),
        Value::Number(500.into()),
    );
    props.insert(
        DECIMAL_FORMAT_CONFIG.to_string(),
        Value::String("NUMERIC".to_string()),
    );
    props.insert(
        REPLACE_NULL_WITH_DEFAULT_CONFIG.to_string(),
        Value::Bool(false),
    );
    props.insert(
        SCHEMA_CONTENT_CONFIG.to_string(),
        Value::String("{\"type\":\"string\"}".to_string()),
    );

    let config = JsonConverterConfig::new(props);

    assert!(!config.schemas_enabled());
    assert_eq!(config.schema_cache_size(), 500);
    assert_eq!(config.decimal_format(), DecimalFormat::Numeric);
    assert!(!config.replace_null_with_default());
    assert_eq!(
        config.schema_content(),
        Some("{\"type\":\"string\"}".as_bytes())
    );
}

#[test]
fn test_decimal_format_case_insensitive() {
    let mut props = HashMap::new();
    props.insert(
        DECIMAL_FORMAT_CONFIG.to_string(),
        Value::String("numeric".to_string()),
    );

    let config = JsonConverterConfig::new(props);
    assert_eq!(config.decimal_format(), DecimalFormat::Numeric);

    // Test with lowercase
    let mut props2 = HashMap::new();
    props2.insert(
        DECIMAL_FORMAT_CONFIG.to_string(),
        Value::String("base64".to_string()),
    );
    let config2 = JsonConverterConfig::new(props2);
    assert_eq!(config2.decimal_format(), DecimalFormat::Base64);
}

#[test]
fn test_schema_content_empty_string_is_none() {
    let mut props = HashMap::new();
    props.insert(
        SCHEMA_CONTENT_CONFIG.to_string(),
        Value::String(String::new()),
    );

    let config = JsonConverterConfig::new(props);
    assert!(config.schema_content().is_none());
}

#[test]
fn test_schema_content_null_is_none() {
    let mut props = HashMap::new();
    props.insert(SCHEMA_CONTENT_CONFIG.to_string(), Value::Null);

    let config = JsonConverterConfig::new(props);
    assert!(config.schema_content().is_none());
}

#[test]
fn test_constants_match_java() {
    // Verify constants match Java exactly
    assert_eq!(SCHEMAS_ENABLE_CONFIG, "schemas.enable");
    assert_eq!(SCHEMAS_CACHE_SIZE_CONFIG, "schemas.cache.size");
    assert_eq!(SCHEMA_CONTENT_CONFIG, "schema.content");
    assert_eq!(DECIMAL_FORMAT_CONFIG, "decimal.format");
    assert_eq!(
        REPLACE_NULL_WITH_DEFAULT_CONFIG,
        "replace.null.with.default"
    );

    // Verify default values match Java
    assert_eq!(SCHEMAS_ENABLE_DEFAULT, true);
    assert_eq!(SCHEMAS_CACHE_SIZE_DEFAULT, 1000);
    assert_eq!(DECIMAL_FORMAT_DEFAULT, "BASE64");
    assert_eq!(REPLACE_NULL_WITH_DEFAULT_DEFAULT, true);
}
