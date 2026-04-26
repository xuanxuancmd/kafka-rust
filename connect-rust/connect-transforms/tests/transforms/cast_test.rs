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
use connect_api::data::SchemaType;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use connect_transforms::transforms::cast::*;
use serde_json::{json, Value};
use std::collections::HashMap;

fn create_test_record_with_value(value: Value) -> SourceRecord {
    SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test-topic".to_string(),
        None,
        None,
        value,
    )
}

fn create_test_record_with_key_value(key: Value, value: Value) -> SourceRecord {
    SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test-topic".to_string(),
        None,
        Some(key),
        value,
    )
}

#[test]
fn test_version() {
    assert_eq!(Cast::version(), "3.9.0");
}

#[test]
fn test_config_def() {
    let config_def = Cast::config_def();
    assert!(config_def.contains_key("spec"));
}

#[test]
fn test_cast_key_creation() {
    let transform = Cast::new(CastTarget::Key);
    assert_eq!(transform.target(), CastTarget::Key);
}

#[test]
fn test_cast_value_creation() {
    let transform = Cast::new(CastTarget::Value);
    assert_eq!(transform.target(), CastTarget::Value);
}

#[test]
fn test_default() {
    let transform = Cast::default();
    assert_eq!(transform.target(), CastTarget::Value);
}

#[test]
fn test_cast_key_convenience() {
    let transform = cast_key();
    assert_eq!(transform.target(), CastTarget::Key);
}

#[test]
fn test_cast_value_convenience() {
    let transform = cast_value();
    assert_eq!(transform.target(), CastTarget::Value);
}

#[test]
fn test_configure_with_field_casts() {
    let mut transform = Cast::new(CastTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("spec".to_string(), json!(["age:int32"]));
    transform.configure(configs);
    assert!(transform.casts().contains_key("age"));
}

#[test]
fn test_configure_with_whole_value_cast() {
    let mut transform = Cast::new(CastTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("spec".to_string(), json!(["int64"]));
    transform.configure(configs);
    assert_eq!(transform.whole_value_cast_type(), Some(SchemaType::Int64));
}

#[test]
fn test_transform_int32_cast() {
    let mut transform = Cast::new(CastTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("spec".to_string(), json!(["age:int32"]));
    transform.configure(configs);
    let record = create_test_record_with_value(json!({"age": "25"}));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["age"], json!(25));
}

#[test]
fn test_transform_int64_whole_value_cast() {
    let mut transform = Cast::new(CastTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("spec".to_string(), json!(["int64"]));
    transform.configure(configs);
    let record = create_test_record_with_value(json!("12345"));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value(), &json!(12345_i64));
}

#[test]
fn test_transform_boolean_cast() {
    let mut transform = Cast::new(CastTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("spec".to_string(), json!(["active:boolean"]));
    transform.configure(configs);
    let record = create_test_record_with_value(json!({"active": "true"}));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["active"], json!(true));
}

#[test]
fn test_transform_string_cast() {
    let mut transform = Cast::new(CastTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("spec".to_string(), json!(["count:string"]));
    transform.configure(configs);
    let record = create_test_record_with_value(json!({"count": 42}));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value()["count"], json!("42"));
}

#[test]
fn test_transform_float64_cast() {
    let mut transform = Cast::new(CastTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("spec".to_string(), json!(["price:float64"]));
    transform.configure(configs);
    let record = create_test_record_with_value(json!({"price": "99.99"}));
    let result = transform.transform(record).unwrap().unwrap();
    assert!(result.value()["price"].is_number());
}

#[test]
fn test_transform_null_value() {
    let mut transform = Cast::new(CastTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("spec".to_string(), json!(["field:int32"]));
    transform.configure(configs);
    let record = create_test_record_with_value(json!(null));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.value(), &json!(null));
}

#[test]
fn test_transform_key_target() {
    let mut transform = Cast::new(CastTarget::Key);
    let mut configs = HashMap::new();
    configs.insert("spec".to_string(), json!(["id:int64"]));
    transform.configure(configs);
    let record = create_test_record_with_key_value(json!({"id": "123"}), json!("value"));
    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.key().unwrap()["id"], json!(123_i64));
}

#[test]
fn test_close() {
    let mut transform = Cast::new(CastTarget::Value);
    let mut configs = HashMap::new();
    configs.insert("spec".to_string(), json!(["field:int32"]));
    transform.configure(configs);
    transform.close();
    assert!(transform.casts().is_empty());
}

#[test]
fn test_transformation_trait() {
    let mut transform = cast_value();
    let mut configs = HashMap::new();
    configs.insert("spec".to_string(), json!(["num:int32"]));
    transform.configure(configs);
    let record = create_test_record_with_value(json!({"num": "100"}));
    let result = Transformation::<SourceRecord>::transform(&transform, record);
    assert!(result.is_ok());
}
