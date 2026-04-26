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
use connect_transforms::transforms::ValueToKey;
use serde_json::json;
use std::collections::HashMap;

fn create_record_with_key_value(key: serde_json::Value, value: serde_json::Value) -> SourceRecord {
    SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test-topic",
        None,
        Some(key),
        value,
    )
}

#[test]
fn test_version() {
    assert_eq!(ValueToKey::version(), "3.9.0");
}

#[test]
fn test_creation() {
    let transform = ValueToKey::new();
    assert!(transform.fields().is_empty());
}

#[test]
fn test_default() {
    let transform = ValueToKey::default();
    assert!(transform.fields().is_empty());
}

#[test]
fn test_configure_with_fields() {
    let mut transform = ValueToKey::new();
    let mut config = HashMap::new();
    config.insert("fields".to_string(), json!(["id"]));
    transform.configure(config);
    assert_eq!(transform.fields(), &["id"]);
}

#[test]
fn test_transform_extract_single_field() {
    let mut transform = ValueToKey::new();
    let mut config = HashMap::new();
    config.insert("fields".to_string(), json!(["keyField"]));
    transform.configure(config);
    let record = create_record_with_key_value(
        json!("oldKey"),
        json!({"keyField": "newKey", "other": "data"}),
    );
    let result = transform.transform(record).unwrap().unwrap();
    let new_key = result.key().unwrap();
    assert_eq!(new_key.get("keyField"), Some(&json!("newKey")));
}

#[test]
fn test_transform_multiple_fields() {
    let mut transform = ValueToKey::new();
    let mut config = HashMap::new();
    config.insert("fields".to_string(), json!(["id", "name"]));
    transform.configure(config);
    let record = create_record_with_key_value(
        json!("oldKey"),
        json!({"id": 123, "name": "test", "other": "data"}),
    );
    let result = transform.transform(record).unwrap().unwrap();
    let new_key = result.key().unwrap();
    assert_eq!(new_key.get("id"), Some(&json!(123)));
    assert_eq!(new_key.get("name"), Some(&json!("test")));
}

#[test]
fn test_transform_missing_field() {
    let mut transform = ValueToKey::new();
    let mut config = HashMap::new();
    config.insert("fields".to_string(), json!(["missing"]));
    transform.configure(config);
    let record = create_record_with_key_value(json!("oldKey"), json!({"other": "data"}));
    let result = transform.transform(record);
    assert!(result.is_ok());
}

#[test]
fn test_transform_null_field() {
    let mut transform = ValueToKey::new();
    let mut config = HashMap::new();
    config.insert("fields".to_string(), json!(["field"]));
    transform.configure(config);
    let record = create_record_with_key_value(json!("oldKey"), json!({"field": null}));
    let result = transform.transform(record).unwrap().unwrap();
    let new_key = result.key().unwrap();
    assert!(new_key.get("field").unwrap().is_null());
}

#[test]
fn test_close() {
    let mut transform = ValueToKey::new();
    let mut config = HashMap::new();
    config.insert("fields".to_string(), json!(["id"]));
    transform.configure(config);
    transform.close();
}

#[test]
fn test_transformation_trait() {
    let mut transform = ValueToKey::new();
    let mut config = HashMap::new();
    config.insert("fields".to_string(), json!(["key"]));
    transform.configure(config);
    let record = create_record_with_key_value(json!("old"), json!({"key": "new"}));
    let result = Transformation::<SourceRecord>::transform(&transform, record);
    assert!(result.is_ok());
}

#[test]
fn test_replace_null_with_default_config() {
    let mut transform = ValueToKey::new();
    let mut config = HashMap::new();
    config.insert("fields".to_string(), json!(["id"]));
    config.insert("replace.null.with.default".to_string(), json!(false));
    transform.configure(config);
    assert!(!transform.replace_null_with_default());
}
