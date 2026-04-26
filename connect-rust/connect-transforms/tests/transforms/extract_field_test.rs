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
use connect_transforms::transforms::extract_field::*;
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
    assert_eq!(ExtractField::version(), "3.9.0");
}

#[test]
fn test_extract_key_creation() {
    let extract = ExtractField::new(ExtractFieldTarget::Key);
    assert_eq!(extract.target(), ExtractFieldTarget::Key);
}

#[test]
fn test_extract_value_creation() {
    let extract = ExtractField::new(ExtractFieldTarget::Value);
    assert_eq!(extract.target(), ExtractFieldTarget::Value);
}

#[test]
fn test_default() {
    let extract = ExtractField::default();
    assert_eq!(extract.target(), ExtractFieldTarget::Value);
}

#[test]
fn test_convenience_constructors() {
    assert_eq!(extract_field_key().target(), ExtractFieldTarget::Key);
    assert_eq!(extract_field_value().target(), ExtractFieldTarget::Value);
}

#[test]
fn test_configure_with_field() {
    let mut extract = ExtractField::new(ExtractFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("field".to_string(), json!("name"));
    extract.configure(config);
}

#[test]
fn test_transform_extract_field() {
    let mut extract = ExtractField::new(ExtractFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("field".to_string(), json!("name"));
    extract.configure(config);
    let record = create_record(json!({"name": "alice", "age": 30}));
    let result = extract.transform(record).unwrap().unwrap();
    assert_eq!(result.value(), &json!("alice"));
}

#[test]
fn test_transform_missing_field() {
    let mut extract = ExtractField::new(ExtractFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("field".to_string(), json!("missing"));
    extract.configure(config);
    let record = create_record(json!({"name": "alice"}));
    let result = extract.transform(record);
    assert!(result.is_ok());
}

#[test]
fn test_transform_null_field() {
    let mut extract = ExtractField::new(ExtractFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("field".to_string(), json!("field"));
    extract.configure(config);
    let record = create_record(json!({"field": null}));
    let result = extract.transform(record).unwrap().unwrap();
    assert!(result.value().is_null());
}

#[test]
fn test_close() {
    let mut extract = ExtractField::new(ExtractFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("field".to_string(), json!("name"));
    extract.configure(config);
    extract.close();
}

#[test]
fn test_transformation_trait() {
    let mut extract = extract_field_value();
    let mut config = HashMap::new();
    config.insert("field".to_string(), json!("name"));
    extract.configure(config);
    let record = create_record(json!({"name": "test"}));
    let result = Transformation::<SourceRecord>::transform(&extract, record);
    assert!(result.is_ok());
}
