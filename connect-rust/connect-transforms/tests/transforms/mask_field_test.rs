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
use connect_transforms::transforms::mask_field::*;
use serde_json::json;
use std::collections::HashMap;

fn create_record(value: serde_json::Value) -> SourceRecord {
    SourceRecord::new(HashMap::new(), HashMap::new(), "test", Some(0), None, value)
}

fn create_transform(fields: Vec<&str>, replacement: Option<&str>) -> MaskField {
    let mut xform = MaskField::new(MaskFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("fields".to_string(), json!(fields));
    if let Some(repl) = replacement {
        config.insert("replacement".to_string(), json!(repl));
    }
    xform.configure(config);
    xform
}

#[test]
fn test_version() {
    assert_eq!(MaskField::version(), "3.9.0");
}

#[test]
fn test_schemaless_mask_int_field() {
    let xform = create_transform(vec!["int"], None);
    let record = create_record(json!({
        "magic": 42,
        "int": 42
    }));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("magic"), Some(&json!(42)));
    assert_eq!(value.get("int"), Some(&json!(0)));
}

#[test]
fn test_schemaless_mask_string_field() {
    let xform = create_transform(vec!["string"], None);
    let record = create_record(json!({
        "magic": 42,
        "string": "55.121.20.20"
    }));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("magic"), Some(&json!(42)));
    assert_eq!(value.get("string"), Some(&json!("")));
}

#[test]
fn test_schemaless_mask_bool_field() {
    let xform = create_transform(vec!["bool"], None);
    let record = create_record(json!({
        "magic": 42,
        "bool": true
    }));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("magic"), Some(&json!(42)));
    assert_eq!(value.get("bool"), Some(&json!(false)));
}

#[test]
fn test_schemaless_mask_multiple_fields() {
    let xform = create_transform(vec!["int", "string", "bool"], None);
    let record = create_record(json!({
        "magic": 42,
        "int": 42,
        "string": "test",
        "bool": true
    }));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("magic"), Some(&json!(42)));
    assert_eq!(value.get("int"), Some(&json!(0)));
    assert_eq!(value.get("string"), Some(&json!("")));
    assert_eq!(value.get("bool"), Some(&json!(false)));
}

#[test]
fn test_schemaless_mask_with_replacement() {
    let xform = create_transform(vec!["int"], Some("123"));
    let record = create_record(json!({
        "magic": 42,
        "int": 42
    }));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("magic"), Some(&json!(42)));
    // Replacement "123" is parsed as a number since it can be parsed as i64
    assert_eq!(value.get("int"), Some(&json!(123)));
}

#[test]
fn test_schemaless_mask_string_with_replacement() {
    let xform = create_transform(vec!["string"], Some("masked"));
    let record = create_record(json!({
        "magic": 42,
        "string": "original"
    }));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("magic"), Some(&json!(42)));
    assert_eq!(value.get("string"), Some(&json!("masked")));
}

#[test]
fn test_mask_field_target_key() {
    let mut xform = MaskField::new(MaskFieldTarget::Key);
    let mut config = HashMap::new();
    config.insert("fields".to_string(), json!(vec!["int"]));
    xform.configure(config);

    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test",
        Some(0),
        Some(json!({"magic": 42, "int": 42})),
        json!("value"),
    );
    let transformed = xform.transform(record).unwrap().unwrap();

    let key = transformed.key().unwrap();
    assert_eq!(key.get("magic"), Some(&json!(42)));
    assert_eq!(key.get("int"), Some(&json!(0)));
}

#[test]
fn test_close() {
    let mut xform = MaskField::new(MaskFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("fields".to_string(), json!(vec!["field"]));
    xform.configure(config);
    xform.close();
}

#[test]
fn test_mask_field_preserves_other_fields() {
    let xform = create_transform(vec!["mask"], None);
    let record = create_record(json!({
        "mask": 100,
        "preserve": "value",
        "another": 42
    }));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("mask"), Some(&json!(0)));
    assert_eq!(value.get("preserve"), Some(&json!("value")));
    assert_eq!(value.get("another"), Some(&json!(42)));
}
