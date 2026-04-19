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

use connect_api::connector::ConnectRecord;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use connect_transforms::transforms::flatten::{
    flatten_key, flatten_value, Flatten, FlattenTarget, DELIMITER_CONFIG,
};
use serde_json::json;
use std::collections::HashMap;

fn create_test_record_with_value(value: serde_json::Value) -> SourceRecord {
    SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test-topic".to_string(),
        None,
        None,
        value,
    )
}

fn create_test_record_with_key_value(
    key: serde_json::Value,
    value: serde_json::Value,
) -> SourceRecord {
    SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test-topic".to_string(),
        None,
        Some(key),
        value,
    )
}

// ==================== Basic Creation Tests ====================

#[test]
fn test_version() {
    assert_eq!(Flatten::version(), "3.9.0");
}

#[test]
fn test_config_def() {
    let config_def = Flatten::config_def();
    assert!(config_def.contains_key(DELIMITER_CONFIG));
}

#[test]
fn test_flatten_key_creation() {
    let transform = Flatten::new(FlattenTarget::Key);
    assert_eq!(transform.target(), FlattenTarget::Key);
}

#[test]
fn test_flatten_value_creation() {
    let transform = Flatten::new(FlattenTarget::Value);
    assert_eq!(transform.target(), FlattenTarget::Value);
}

#[test]
fn test_default() {
    let transform = Flatten::default();
    assert_eq!(transform.target(), FlattenTarget::Value);
    assert_eq!(transform.delimiter(), ".");
}

#[test]
fn test_default_delimiter() {
    let transform = Flatten::new(FlattenTarget::Value);
    assert_eq!(transform.delimiter(), ".");
}

#[test]
fn test_flatten_key_convenience() {
    let transform = flatten_key();
    assert_eq!(transform.target(), FlattenTarget::Key);
}

#[test]
fn test_flatten_value_convenience() {
    let transform = flatten_value();
    assert_eq!(transform.target(), FlattenTarget::Value);
}

// ==================== Delimiter Configuration Tests ====================

#[test]
fn test_configure_default_delimiter() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    let configs = HashMap::new();
    transform.configure(configs);
    assert_eq!(transform.delimiter(), ".");
}

#[test]
fn test_configure_custom_delimiter_dot() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("delimiter".to_string(), json!("."));
    transform.configure(configs);
    assert_eq!(transform.delimiter(), ".");
}

#[test]
fn test_configure_custom_delimiter_underscore() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("delimiter".to_string(), json!("_"));
    transform.configure(configs);
    assert_eq!(transform.delimiter(), "_");
}

#[test]
fn test_configure_custom_delimiter_dash() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("delimiter".to_string(), json!("-"));
    transform.configure(configs);
    assert_eq!(transform.delimiter(), "-");
}

#[test]
fn test_configure_custom_delimiter_slash() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("delimiter".to_string(), json!("/"));
    transform.configure(configs);
    assert_eq!(transform.delimiter(), "/");
}

#[test]
fn test_configure_custom_delimiter_empty() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("delimiter".to_string(), json!(""));
    transform.configure(configs);
    assert_eq!(transform.delimiter(), "");
}

// ==================== Nested Structure Flatten Tests ====================

#[test]
fn test_flatten_single_level_nested() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!({
        "user": {
            "name": "alice",
            "age": 30
        },
        "id": 123
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["user.name"], json!("alice"));
    assert_eq!(result.value()["user.age"], json!(30));
    assert_eq!(result.value()["id"], json!(123));
}

#[test]
fn test_flatten_multi_level_nested() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!({
        "user": {
            "profile": {
                "name": "bob",
                "email": "bob@example.com"
            },
            "id": 456
        },
        "status": "active"
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["user.profile.name"], json!("bob"));
    assert_eq!(
        result.value()["user.profile.email"],
        json!("bob@example.com")
    );
    assert_eq!(result.value()["user.id"], json!(456));
    assert_eq!(result.value()["status"], json!("active"));
}

#[test]
fn test_flatten_deeply_nested() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!({
        "level1": {
            "level2": {
                "level3": {
                    "level4": {
                        "value": "deep"
                    }
                }
            }
        }
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(
        result.value()["level1.level2.level3.level4.value"],
        json!("deep")
    );
}

#[test]
fn test_flatten_nested_with_underscore_delimiter() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("delimiter".to_string(), json!("_"));
    transform.configure(configs);
    let record = create_test_record_with_value(json!({
        "user": {
            "address": {
                "city": "Seattle"
            }
        }
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["user_address_city"], json!("Seattle"));
}

#[test]
fn test_flatten_nested_with_dash_delimiter() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("delimiter".to_string(), json!("-"));
    transform.configure(configs);
    let record = create_test_record_with_value(json!({
        "user": {
            "address": {
                "city": "Seattle"
            }
        }
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["user-address-city"], json!("Seattle"));
}

#[test]
fn test_flatten_nested_with_empty_delimiter() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("delimiter".to_string(), json!(""));
    transform.configure(configs);
    let record = create_test_record_with_value(json!({
        "user": {
            "name": "alice"
        }
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["username"], json!("alice"));
}

// ==================== Map Flatten Tests ====================

#[test]
fn test_flatten_simple_map() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!({
        "key1": "value1",
        "key2": "value2"
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["key1"], json!("value1"));
    assert_eq!(result.value()["key2"], json!("value2"));
}

#[test]
fn test_flatten_map_with_nested_map() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!({
        "outer": {
            "inner": {
                "key": "value"
            }
        }
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["outer.inner.key"], json!("value"));
}

#[test]
fn test_flatten_map_preserves_array() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!({
        "user": {
            "name": "alice",
            "tags": ["tag1", "tag2", "tag3"]
        }
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["user.name"], json!("alice"));
    assert_eq!(result.value()["user.tags"], json!(["tag1", "tag2", "tag3"]));
}

#[test]
fn test_flatten_map_with_mixed_types() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!({
        "string_field": "text",
        "number_field": 42,
        "bool_field": true,
        "nested": {
            "inner_string": "inner_text",
            "inner_number": 100
        }
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["string_field"], json!("text"));
    assert_eq!(result.value()["number_field"], json!(42));
    assert_eq!(result.value()["bool_field"], json!(true));
    assert_eq!(result.value()["nested.inner_string"], json!("inner_text"));
    assert_eq!(result.value()["nested.inner_number"], json!(100));
}

// ==================== Null Value Handling Tests ====================

#[test]
fn test_flatten_null_value() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!(null));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value(), &json!(null));
}

#[test]
fn test_flatten_nested_null_field() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!({
        "user": {
            "name": "alice",
            "email": null
        }
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["user.name"], json!("alice"));
    assert_eq!(result.value()["user.email"], json!(null));
}

#[test]
fn test_flatten_null_nested_object() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!({
        "user": null,
        "id": 123
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["user"], json!(null));
    assert_eq!(result.value()["id"], json!(123));
}

#[test]
fn test_flatten_multiple_null_fields() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!({
        "field1": null,
        "nested": {
            "field2": null,
            "field3": "value"
        }
    }));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["field1"], json!(null));
    assert_eq!(result.value()["nested.field2"], json!(null));
    assert_eq!(result.value()["nested.field3"], json!("value"));
}

// ==================== Key Operation Tests ====================

#[test]
fn test_flatten_key_target() {
    let mut transform = Flatten::new(FlattenTarget::Key);
    transform.configure(HashMap::new());
    let record = create_test_record_with_key_value(
        json!({
            "user": {
                "id": 123
            }
        }),
        json!("value"),
    );
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.key().unwrap()["user.id"], json!(123));
    assert_eq!(result.value(), &json!("value"));
}

#[test]
fn test_flatten_key_with_delimiter() {
    let mut transform = Flatten::new(FlattenTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("delimiter".to_string(), json!("_"));
    transform.configure(configs);
    let record = create_test_record_with_key_value(
        json!({
            "partition": {
                "region": "us-west",
                "zone": "1a"
            }
        }),
        json!("data"),
    );
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.key().unwrap()["partition_region"], json!("us-west"));
    assert_eq!(result.key().unwrap()["partition_zone"], json!("1a"));
}

#[test]
fn test_flatten_key_null() {
    let mut transform = Flatten::new(FlattenTarget::Key);
    transform.configure(HashMap::new());
    let record = create_test_record_with_key_value(json!(null), json!("value"));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.key().unwrap(), &json!(null));
    assert_eq!(result.value(), &json!("value"));
}

#[test]
fn test_flatten_key_nested_structure() {
    let mut transform = Flatten::new(FlattenTarget::Key);
    transform.configure(HashMap::new());
    let record = create_test_record_with_key_value(
        json!({
            "tenant": {
                "project": {
                    "env": "prod"
                }
            }
        }),
        json!("payload"),
    );
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.key().unwrap()["tenant.project.env"], json!("prod"));
}

#[test]
fn test_transformation_trait_flatten_value() {
    let mut transform = flatten_value();
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!({
        "nested": {
            "field": "value"
        }
    }));
    let result = Transformation::<SourceRecord>::transform(&transform, record);
    assert!(result.is_ok());
    let transformed = result.unwrap().unwrap();
    assert_eq!(transformed.value()["nested.field"], json!("value"));
}

#[test]
fn test_transformation_trait_flatten_key() {
    let mut transform = flatten_key();
    transform.configure(HashMap::new());
    let record = create_test_record_with_key_value(
        json!({
            "nested": {
                "field": "value"
            }
        }),
        json!("data"),
    );
    let result = Transformation::<SourceRecord>::transform(&transform, record);
    assert!(result.is_ok());
    let transformed = result.unwrap().unwrap();
    assert_eq!(transformed.key().unwrap()["nested.field"], json!("value"));
}

// ==================== Field Name Tests ====================

#[test]
fn test_field_name_empty_prefix() {
    let transform = Flatten::new(FlattenTarget::Value);
    assert_eq!(transform.field_name("", "field"), "field");
}

#[test]
fn test_field_name_with_prefix() {
    let transform = Flatten::new(FlattenTarget::Value);
    assert_eq!(transform.field_name("parent", "child"), "parent.child");
}

#[test]
fn test_field_name_custom_delimiter() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("delimiter".to_string(), json!("_"));
    transform.configure(configs);
    assert_eq!(transform.field_name("parent", "child"), "parent_child");
}

// ==================== Close and Display Tests ====================

#[test]
fn test_close() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("delimiter".to_string(), json!("_"));
    transform.configure(configs);
    transform.close();
    assert_eq!(transform.delimiter(), ".");
}

#[test]
fn test_display() {
    let transform = Flatten::new(FlattenTarget::Value);
    let display = format!("{}", transform);
    assert!(display.contains("Flatten"));
    assert!(display.contains("Value"));
    assert!(display.contains("delimiter"));
}

#[test]
fn test_display_key_target() {
    let transform = Flatten::new(FlattenTarget::Key);
    let display = format!("{}", transform);
    assert!(display.contains("Key"));
}

#[test]
fn test_debug() {
    let transform = Flatten::new(FlattenTarget::Value);
    let debug = format!("{:?}", transform);
    assert!(debug.contains("Flatten"));
    assert!(debug.contains("target"));
    assert!(debug.contains("delimiter"));
}

// ==================== Error Handling Tests ====================

#[test]
fn test_flatten_non_object_value_error() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!("not_an_object"));
    let result = transform.transform(record);
    assert!(result.is_err());
}

#[test]
fn test_flatten_array_value_error() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!(["item1", "item2"]));
    let result = transform.transform(record);
    assert!(result.is_err());
}

#[test]
fn test_flatten_number_value_error() {
    let mut transform = Flatten::new(FlattenTarget::Value);
    transform.configure(HashMap::new());
    let record = create_test_record_with_value(json!(42));
    let result = transform.transform(record);
    assert!(result.is_err());
}
