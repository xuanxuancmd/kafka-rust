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

use connect_transforms::transforms::extract_field::*;
use connect_transforms::transforms::field::FieldSyntaxVersion;
use connect_api::connector::ConnectRecord;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::json;
use std::collections::HashMap;

// ==================== Helper Functions ====================

fn create_test_record_with_key_value(key: Option<Value>, value: Value) -> SourceRecord {
    SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test-topic".to_string(),
        None,
        key,
        value,
    )
}

fn create_test_record_with_schema(
    key: Option<Value>,
    key_schema: Option<&str>,
    value: Value,
    value_schema: Option<&str>,
) -> SourceRecord {
    let mut record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test-topic".to_string(),
        None,
        key,
        value,
    );
    if let Some(schema) = key_schema {
        record = record.with_key_and_schema(record.key().cloned(), Some(schema.to_string()));
    }
    if let Some(schema) = value_schema {
        record = record.with_value_and_schema(record.value().clone(), Some(schema.to_string()));
    }
    record
}

// ==================== Basic Creation Tests ====================

#[test]
fn test_version() {
    assert_eq!(ExtractField::version(), "3.9.0");
}

#[test]
fn test_config_def() {
    let config_def = ExtractField::config_def();
    assert!(config_def.contains_key("field"));
    assert!(config_def.contains_key("replace.null.with.default"));
    assert!(config_def.contains_key("field.syntax.version"));
}

#[test]
fn test_extract_field_key_creation() {
    let transform = ExtractField::new(ExtractFieldTarget::Key);
    assert_eq!(transform.target(), ExtractFieldTarget::Key);
}

#[test]
fn test_extract_field_value_creation() {
    let transform = ExtractField::new(ExtractFieldTarget::Value);
    assert_eq!(transform.target(), ExtractFieldTarget::Value);
}

#[test]
fn test_default() {
    let transform = ExtractField::default();
    assert_eq!(transform.target(), ExtractFieldTarget::Value);
}

#[test]
fn test_extract_field_key_convenience() {
    let transform = extract_field_key();
    assert_eq!(transform.target(), ExtractFieldTarget::Key);
}

#[test]
fn test_extract_field_value_convenience() {
    let transform = extract_field_value();
    assert_eq!(transform.target(), ExtractFieldTarget::Value);
}

// ==================== Configuration Tests ====================

#[test]
fn test_configure_with_field() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);
    assert_eq!(transform.field_path().field_names(), &["magic"]);
}

#[test]
fn test_configure_with_replace_null_with_default_true() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    configs.insert("replace.null.with.default".to_string(), json!(true));
    transform.configure(configs);
    assert!(transform.replace_null_with_default());
}

#[test]
fn test_configure_with_replace_null_with_default_false() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    configs.insert("replace.null.with.default".to_string(), json!(false));
    transform.configure(configs);
    assert!(!transform.replace_null_with_default());
}

#[test]
fn test_configure_with_v2_syntax() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic.foo"));
    configs.insert("field.syntax.version".to_string(), json!("V2"));
    transform.configure(configs);
    assert_eq!(transform.field_syntax_version(), FieldSyntaxVersion::V2);
    assert_eq!(transform.field_path().field_names(), &["magic", "foo"]);
}

#[test]
fn test_configure_default_values() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);
    assert!(transform.replace_null_with_default());
    assert_eq!(transform.field_syntax_version(), FieldSyntaxVersion::V1);
}

// ==================== Schemaless Key Extraction Tests ====================

#[test]
fn test_schemaless_key_extraction() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);

    let record =
        create_test_record_with_key_value(Some(json!({"magic": 42})), json!({"data": "value"}));
    let result = transform.transform(record).unwrap().unwrap();

    assert!(result.key_schema().is_none());
    assert_eq!(result.key(), Some(&json!(42)));
    assert_eq!(result.value(), &json!({"data": "value"}));
}

#[test]
fn test_schemaless_key_extraction_string_field() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("name"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(Some(json!({"name": "test"})), json!("value"));
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.key(), Some(&json!("test")));
}

#[test]
fn test_schemaless_key_extraction_nested_path_v2() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic.foo"));
    configs.insert("field.syntax.version".to_string(), json!("V2"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(
        Some(json!({"magic": {"foo": 42}})),
        json!({"data": "value"}),
    );
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.key(), Some(&json!(42)));
}

#[test]
fn test_schemaless_key_null_input() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(None, json!({"data": "value"}));
    let result = transform.transform(record).unwrap().unwrap();

    assert!(result.key().is_none() || result.key().map(|k| k.is_null()).unwrap_or(true));
}

// ==================== Schemaless Value Extraction Tests ====================

#[test]
fn test_schemaless_value_extraction() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("id"));
    transform.configure(configs);

    let record =
        create_test_record_with_key_value(Some(json!("key")), json!({"id": 123, "name": "test"}));
    let result = transform.transform(record).unwrap().unwrap();

    assert!(result.value_schema().is_none());
    assert_eq!(result.value(), &json!(123));
    assert_eq!(result.key(), Some(&json!("key")));
}

#[test]
fn test_schemaless_value_extraction_nested_path_v2() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("user.id"));
    configs.insert("field.syntax.version".to_string(), json!("V2"));
    transform.configure(configs);

    let record =
        create_test_record_with_key_value(None, json!({"user": {"id": 100, "name": "Alice"}}));
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.value(), &json!(100));
}

#[test]
fn test_schemaless_value_null_input() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(Some(json!("key")), json!(null));
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.value(), &json!(null));
}

// ==================== Non-Existent Field Tests (Schemaless) ====================

#[test]
fn test_schemaless_non_existent_field_returns_null() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("nonexistent"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(Some(json!({"magic": 42})), json!("value"));
    let result = transform.transform(record).unwrap().unwrap();

    assert!(result.key_schema().is_none());
    assert!(result.key().map(|k| k.is_null()).unwrap_or(true));
}

#[test]
fn test_schemaless_non_existent_nested_field_returns_null() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic.nonexistent"));
    configs.insert("field.syntax.version".to_string(), json!("V2"));
    transform.configure(configs);

    let record =
        create_test_record_with_key_value(Some(json!({"magic": {"foo": 42}})), json!("value"));
    let result = transform.transform(record).unwrap().unwrap();

    assert!(result.key().map(|k| k.is_null()).unwrap_or(true));
}

#[test]
fn test_schemaless_non_existent_parent_field_returns_null() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("nonexistent.field"));
    configs.insert("field.syntax.version".to_string(), json!("V2"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(None, json!({"other": "data"}));
    let result = transform.transform(record).unwrap().unwrap();

    assert!(result.value().is_null());
}

// ==================== Schema-Aware Extraction Tests ====================

#[test]
fn test_with_schema_key_extraction() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);

    let record = create_test_record_with_schema(
        Some(json!({"magic": 42})),
        Some("key-schema"),
        json!({"data": "value"}),
        None,
    );
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.key(), Some(&json!(42)));
}

#[test]
fn test_with_schema_value_extraction() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("id"));
    transform.configure(configs);

    let record = create_test_record_with_schema(
        Some(json!("key")),
        None,
        json!({"id": 123, "name": "test"}),
        Some("value-schema"),
    );
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.value(), &json!(123));
}

#[test]
fn test_with_schema_nested_extraction_v2() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("nested.field"));
    configs.insert("field.syntax.version".to_string(), json!("V2"));
    transform.configure(configs);

    let record = create_test_record_with_schema(
        None,
        None,
        json!({"nested": {"field": "data"}}),
        Some("value-schema"),
    );
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.value(), &json!("data"));
}

#[test]
fn test_with_schema_null_record() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);

    let record = create_test_record_with_schema(None, Some("key-schema"), json!("value"), None);
    let result = transform.transform(record).unwrap().unwrap();

    assert!(result.key().is_none() || result.key().map(|k| k.is_null()).unwrap_or(true));
}

// ==================== Non-Existent Field Tests (With Schema) ====================

#[test]
fn test_with_schema_non_existent_field_fails() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("nonexistent"));
    transform.configure(configs);

    let record = create_test_record_with_schema(
        Some(json!({"magic": 42})),
        Some("key-schema"),
        json!("value"),
        None,
    );

    let result = transform.transform(record);
    assert!(result.is_err());
}

#[test]
fn test_with_schema_non_existent_nested_field_fails() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic.nonexistent"));
    configs.insert("field.syntax.version".to_string(), json!("V2"));
    transform.configure(configs);

    let record = create_test_record_with_schema(
        Some(json!({"magic": {"foo": 42}})),
        Some("key-schema"),
        json!("value"),
        None,
    );

    let result = transform.transform(record);
    assert!(result.is_err());
}

// ==================== Null Value Handling Tests ====================

#[test]
fn test_null_field_value_in_record() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("optional"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(None, json!({"optional": null, "other": 1}));
    let result = transform.transform(record).unwrap().unwrap();

    assert!(result.value().is_null());
}

#[test]
fn test_whole_record_key_null() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(None, json!({"data": "value"}));
    let result = transform.transform(record).unwrap().unwrap();

    assert!(result.key().is_none() || result.key().map(|k| k.is_null()).unwrap_or(true));
}

#[test]
fn test_whole_record_value_null() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(Some(json!("key")), json!(null));
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.value(), &json!(null));
}

// ==================== Invalid Input Tests ====================

#[test]
fn test_schemaless_non_object_key_fails() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(Some(json!("not-an-object")), json!("value"));
    let result = transform.transform(record);

    assert!(result.is_err());
}

#[test]
fn test_schemaless_non_object_value_fails() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(None, json!("not-an-object"));
    let result = transform.transform(record);

    assert!(result.is_err());
}

#[test]
fn test_schemaless_array_value_fails() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(None, json!([1, 2, 3]));
    let result = transform.transform(record);

    assert!(result.is_err());
}

// ==================== Close and Display Tests ====================

#[test]
fn test_close() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    configs.insert("replace.null.with.default".to_string(), json!(false));
    configs.insert("field.syntax.version".to_string(), json!("V2"));
    transform.configure(configs);

    transform.close();

    // After close, field_path is reset to empty string (default)
    assert_eq!(transform.field_path().field_names(), &[""]);
    assert!(transform.replace_null_with_default());
    assert_eq!(transform.field_syntax_version(), FieldSyntaxVersion::V1);
}

#[test]
fn test_display() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("magic"));
    transform.configure(configs);

    let display = format!("{}", transform);
    assert!(display.contains("ExtractField"));
    assert!(display.contains("Value"));
    assert!(display.contains("magic"));
}

#[test]
fn test_display_key() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("id"));
    transform.configure(configs);

    let display = format!("{}", transform);
    assert!(display.contains("Key"));
    assert!(display.contains("id"));
}

// ==================== Transformation Trait Tests ====================

#[test]
fn test_transformation_trait_value() {
    let mut transform = extract_field_value();
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("id"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(None, json!({"id": 42}));
    let result = Transformation::<SourceRecord>::transform(&transform, record);

    assert!(result.is_ok());
    let transformed = result.unwrap().unwrap();
    assert_eq!(transformed.value(), &json!(42));
}

#[test]
fn test_transformation_trait_key() {
    let mut transform = extract_field_key();
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("key_id"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(Some(json!({"key_id": "abc"})), json!("value"));
    let result = Transformation::<SourceRecord>::transform(&transform, record);

    assert!(result.is_ok());
    let transformed = result.unwrap().unwrap();
    assert_eq!(transformed.key(), Some(&json!("abc")));
}

// ==================== Edge Cases Tests ====================

#[test]
fn test_empty_field_value() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!(""));
    transform.configure(configs);

    let record = create_test_record_with_key_value(None, json!({"data": 1}));
    let result = transform.transform(record).unwrap().unwrap();

    assert!(result.value().is_null());
}

#[test]
fn test_multiple_nested_levels_v2() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("level1.level2.level3"));
    configs.insert("field.syntax.version".to_string(), json!("V2"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(
        None,
        json!({"level1": {"level2": {"level3": "deep-value"}}}),
    );
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.value(), &json!("deep-value"));
}

#[test]
fn test_extract_boolean_field() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("active"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(None, json!({"active": true, "name": "test"}));
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.value(), &json!(true));
}

#[test]
fn test_extract_array_field() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("items"));
    transform.configure(configs);

    let record =
        create_test_record_with_key_value(None, json!({"items": [1, 2, 3], "name": "test"}));
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.value(), &json!([1, 2, 3]));
}

#[test]
fn test_extract_nested_object_field() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("metadata"));
    transform.configure(configs);

    let record = create_test_record_with_key_value(
        None,
        json!({"metadata": {"created": "2023-01-01", "updated": "2023-06-01"}}),
    );
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(
        result.value(),
        &json!({"created": "2023-01-01", "updated": "2023-06-01"})
    );
}

#[test]
fn test_topic_preserved_after_transform() {
    let mut transform = ExtractField::new(ExtractFieldTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("field".to_string(), json!("id"));
    transform.configure(configs);

    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "my-custom-topic".to_string(),
        Some(5),
        None,
        json!({"id": 100}),
    );
    let result = transform.transform(record).unwrap().unwrap();

    assert_eq!(result.topic(), "my-custom-topic");
    assert_eq!(result.kafka_partition(), Some(5));
}
