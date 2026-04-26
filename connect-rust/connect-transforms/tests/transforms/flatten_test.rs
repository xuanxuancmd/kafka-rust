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
use connect_transforms::transforms::flatten::*;
use serde_json::json;
use std::collections::HashMap;

fn create_record(value: serde_json::Value) -> SourceRecord {
    SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test-topic",
        None,
        None,
        value,
    )
}

#[test]
fn test_version() {
    assert_eq!(Flatten::version(), "3.9.0");
}

#[test]
fn test_flatten_key_creation() {
    let flatten = Flatten::new(FlattenTarget::Key);
    assert_eq!(flatten.target(), FlattenTarget::Key);
}

#[test]
fn test_flatten_value_creation() {
    let flatten = Flatten::new(FlattenTarget::Value);
    assert_eq!(flatten.target(), FlattenTarget::Value);
}

#[test]
fn test_default() {
    let flatten = Flatten::default();
    assert_eq!(flatten.target(), FlattenTarget::Value);
}

#[test]
fn test_convenience_constructors() {
    assert_eq!(flatten_key().target(), FlattenTarget::Key);
    assert_eq!(flatten_value().target(), FlattenTarget::Value);
}

#[test]
fn test_configure_default_delimiter() {
    let mut flatten = Flatten::new(FlattenTarget::Value);
    flatten.configure(HashMap::new());
    assert_eq!(flatten.delimiter(), ".");
}

#[test]
fn test_configure_custom_delimiter() {
    let mut flatten = Flatten::new(FlattenTarget::Value);
    let mut config = HashMap::new();
    config.insert("delimiter".to_string(), json!("_"));
    flatten.configure(config);
    assert_eq!(flatten.delimiter(), "_");
}

#[test]
fn test_transform_simple_map() {
    let mut flatten = Flatten::new(FlattenTarget::Value);
    flatten.configure(HashMap::new());
    let record = create_record(json!({"a": 1, "b": 2}));
    let result = flatten.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["a"], json!(1));
    assert_eq!(result.value()["b"], json!(2));
}

#[test]
fn test_transform_nested_map() {
    let mut flatten = Flatten::new(FlattenTarget::Value);
    flatten.configure(HashMap::new());
    let record = create_record(json!({"user": {"name": "alice", "age": 30}}));
    let result = flatten.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["user.name"], json!("alice"));
    assert_eq!(result.value()["user.age"], json!(30));
}

#[test]
fn test_transform_deeply_nested() {
    let mut flatten = Flatten::new(FlattenTarget::Value);
    flatten.configure(HashMap::new());
    let record = create_record(json!({"a": {"b": {"c": "deep"}}}));
    let result = flatten.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["a.b.c"], json!("deep"));
}

#[test]
fn test_transform_with_underscore_delimiter() {
    let mut flatten = Flatten::new(FlattenTarget::Value);
    let mut config = HashMap::new();
    config.insert("delimiter".to_string(), json!("_"));
    flatten.configure(config);
    let record = create_record(json!({"user": {"name": "alice"}}));
    let result = flatten.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["user_name"], json!("alice"));
}

#[test]
fn test_transform_null_value() {
    let mut flatten = Flatten::new(FlattenTarget::Value);
    flatten.configure(HashMap::new());
    let record = create_record(json!(null));
    let result = flatten.transform(record).unwrap().unwrap();
    assert!(result.value().is_null());
}

#[test]
fn test_transform_key_target() {
    let mut flatten = Flatten::new(FlattenTarget::Key);
    flatten.configure(HashMap::new());
    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test-topic",
        None,
        Some(json!({"user": {"id": 123}})),
        json!("value"),
    );
    let result = flatten.transform(record).unwrap().unwrap();
    assert_eq!(result.key().unwrap()["user.id"], json!(123));
}

#[test]
fn test_close() {
    let mut flatten = Flatten::new(FlattenTarget::Value);
    flatten.configure(HashMap::new());
    flatten.close();
}

#[test]
fn test_transformation_trait() {
    let mut flatten = flatten_value();
    flatten.configure(HashMap::new());
    let record = create_record(json!({"a": {"b": 1}}));
    let result = Transformation::<SourceRecord>::transform(&flatten, record);
    assert!(result.is_ok());
}
