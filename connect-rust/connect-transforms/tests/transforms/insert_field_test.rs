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
use connect_transforms::transforms::insert_field::*;
use serde_json::json;
use std::collections::HashMap;

fn create_record(value: serde_json::Value) -> SourceRecord {
    SourceRecord::new(HashMap::new(), HashMap::new(), "test", Some(0), None, value)
}

fn create_record_with_topic(topic: &str, value: serde_json::Value) -> SourceRecord {
    SourceRecord::new(HashMap::new(), HashMap::new(), topic, Some(0), None, value)
}

#[test]
fn test_version() {
    assert_eq!(InsertField::version(), "3.9.0");
}

#[test]
fn test_insert_topic_field_schemaless() {
    let mut xform = InsertField::new(InsertFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("topic.field".to_string(), json!("topic_field"));
    xform.configure(config);

    let record = create_record_with_topic("test-topic", json!({"magic": 42}));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("magic"), Some(&json!(42)));
    assert_eq!(value.get("topic_field"), Some(&json!("test-topic")));
}

#[test]
fn test_insert_partition_field_schemaless() {
    let mut xform = InsertField::new(InsertFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("partition.field".to_string(), json!("partition_field"));
    xform.configure(config);

    let record = create_record(json!({"magic": 42}));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("magic"), Some(&json!(42)));
    assert_eq!(value.get("partition_field"), Some(&json!(0)));
}

#[test]
fn test_insert_static_field_schemaless() {
    let mut xform = InsertField::new(InsertFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("static.field".to_string(), json!("instance_id"));
    config.insert("static.value".to_string(), json!("my-instance-id"));
    xform.configure(config);

    let record = create_record(json!({"magic": 42}));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("magic"), Some(&json!(42)));
    assert_eq!(value.get("instance_id"), Some(&json!("my-instance-id")));
}

#[test]
fn test_insert_multiple_fields_schemaless() {
    let mut xform = InsertField::new(InsertFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("topic.field".to_string(), json!("topic_field"));
    config.insert("partition.field".to_string(), json!("partition_field"));
    config.insert("static.field".to_string(), json!("instance_id"));
    config.insert("static.value".to_string(), json!("my-instance-id"));
    xform.configure(config);

    let record = create_record_with_topic("test-topic", json!({"magic": 42}));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("magic"), Some(&json!(42)));
    assert_eq!(value.get("topic_field"), Some(&json!("test-topic")));
    assert_eq!(value.get("partition_field"), Some(&json!(0)));
    assert_eq!(value.get("instance_id"), Some(&json!("my-instance-id")));
}

#[test]
fn test_tombstone_schemaless_returns_null() {
    let mut xform = InsertField::new(InsertFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("topic.field".to_string(), json!("topic_field"));
    xform.configure(config);

    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test",
        Some(0),
        None,
        json!(null),
    );
    let transformed = xform.transform(record).unwrap().unwrap();

    assert!(transformed.value().is_null());
}

#[test]
fn test_insert_field_key() {
    let mut xform = InsertField::new(InsertFieldTarget::Key);
    let mut config = HashMap::new();
    config.insert("topic.field".to_string(), json!("topic_field"));
    xform.configure(config);

    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test-topic",
        Some(0),
        Some(json!({"magic": 42})),
        json!("value"),
    );
    let transformed = xform.transform(record).unwrap().unwrap();

    let key = transformed.key().unwrap();
    assert_eq!(key.get("magic"), Some(&json!(42)));
    assert_eq!(key.get("topic_field"), Some(&json!("test-topic")));
}

#[test]
fn test_close() {
    let mut xform = InsertField::new(InsertFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("topic.field".to_string(), json!("topic_field"));
    xform.configure(config);
    xform.close();
}
