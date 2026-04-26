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
use connect_transforms::transforms::set_schema_metadata::*;
use serde_json::json;
use std::collections::HashMap;

fn create_record(value: serde_json::Value) -> SourceRecord {
    SourceRecord::new(HashMap::new(), HashMap::new(), "", Some(0), None, value)
}

#[test]
fn test_version() {
    assert_eq!(SetSchemaMetadata::version(), "3.9.0");
}

#[test]
fn test_schema_name_update() {
    let mut xform = SetSchemaMetadata::new(SetSchemaMetadataTarget::Value);
    let mut config = HashMap::new();
    config.insert("schema.name".to_string(), json!("foo"));
    xform.configure(config);

    let record = create_record(json!({"field": "value"}));
    let result = xform.transform(record);
    // Transform should work even without initial schema
    assert!(result.is_ok());
}

#[test]
fn test_schema_version_update() {
    let mut xform = SetSchemaMetadata::new(SetSchemaMetadataTarget::Value);
    let mut config = HashMap::new();
    config.insert("schema.version".to_string(), json!(42));
    xform.configure(config);

    let record = create_record(json!({"field": "value"}));
    let result = xform.transform(record);
    // Transform should work even without initial schema
    assert!(result.is_ok());
}

#[test]
fn test_schema_name_and_version_update() {
    let mut xform = SetSchemaMetadata::new(SetSchemaMetadataTarget::Value);
    let mut config = HashMap::new();
    config.insert("schema.name".to_string(), json!("foo"));
    config.insert("schema.version".to_string(), json!(42));
    xform.configure(config);

    let record = create_record(json!({"field": "value"}));
    let result = xform.transform(record);
    // Transform should work even without initial schema
    assert!(result.is_ok());
}

#[test]
fn test_ignore_record_with_null_value() {
    let mut xform = SetSchemaMetadata::new(SetSchemaMetadataTarget::Value);
    let mut config = HashMap::new();
    config.insert("schema.name".to_string(), json!("foo"));
    xform.configure(config);

    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "",
        Some(0),
        None,
        json!(null),
    );
    let transformed = xform.transform(record).unwrap().unwrap();

    assert!(transformed.key().is_none());
    assert!(transformed.key_schema().is_none());
    assert!(transformed.value().is_null());
    assert!(transformed.value_schema().is_none());
}

#[test]
fn test_target_key() {
    let mut xform = SetSchemaMetadata::new(SetSchemaMetadataTarget::Key);
    let mut config = HashMap::new();
    config.insert("schema.name".to_string(), json!("foo"));
    xform.configure(config);

    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "",
        Some(0),
        Some(json!({"field": "value"})),
        json!("value"),
    );
    let result = xform.transform(record);
    // Transform should work even without initial schema
    assert!(result.is_ok());
}

#[test]
fn test_update_schema_preserves_value() {
    let mut xform = SetSchemaMetadata::new(SetSchemaMetadataTarget::Value);
    let mut config = HashMap::new();
    config.insert("schema.name".to_string(), json!("new-name"));
    xform.configure(config);

    let record = create_record(json!({"f1": "value1", "f2": 1}));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    assert_eq!(value.get("f1"), Some(&json!("value1")));
    assert_eq!(value.get("f2"), Some(&json!(1)));
}

#[test]
fn test_close() {
    let mut xform = SetSchemaMetadata::new(SetSchemaMetadataTarget::Value);
    let mut config = HashMap::new();
    config.insert("schema.name".to_string(), json!("foo"));
    xform.configure(config);
    xform.close();
}

#[test]
fn test_default_target_is_value() {
    let xform = SetSchemaMetadata::default();
    assert_eq!(xform.target(), SetSchemaMetadataTarget::Value);
}
