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
use connect_transforms::transforms::replace_field::*;
use serde_json::json;
use std::collections::HashMap;

fn create_record(value: serde_json::Value) -> SourceRecord {
    SourceRecord::new(HashMap::new(), HashMap::new(), "test", Some(0), None, value)
}

#[test]
fn test_version() {
    assert_eq!(ReplaceField::version(), "3.9.0");
}

#[test]
fn test_tombstone_schemaless() {
    let mut xform = ReplaceField::new(ReplaceFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("include".to_string(), json!("abc,foo"));
    config.insert("renames".to_string(), json!("abc:xyz,foo:bar"));
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
fn test_schemaless() {
    let mut xform = ReplaceField::new(ReplaceFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("exclude".to_string(), json!(["dont"]));
    config.insert("renames".to_string(), json!(["abc:xyz", "foo:bar"]));
    xform.configure(config);

    let record = create_record(json!({
        "dont": "whatever",
        "abc": 42,
        "foo": true,
        "etc": "etc"
    }));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    // "dont" should be excluded
    assert_eq!(value.get("dont"), None);
    // "abc" renamed to "xyz"
    assert_eq!(value.get("xyz"), Some(&json!(42)));
    // "foo" renamed to "bar"
    assert_eq!(value.get("bar"), Some(&json!(true)));
    // "etc" unchanged
    assert_eq!(value.get("etc"), Some(&json!("etc")));
}

#[test]
fn test_schemaless_include() {
    let mut xform = ReplaceField::new(ReplaceFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("include".to_string(), json!(["abc", "foo"]));
    config.insert("renames".to_string(), json!(["abc:xyz", "foo:bar"]));
    xform.configure(config);

    let record = create_record(json!({
        "dont": "whatever",
        "abc": 42,
        "foo": true,
        "etc": "etc"
    }));
    let transformed = xform.transform(record).unwrap().unwrap();

    let value = transformed.value();
    // Only included fields should remain
    assert_eq!(value.get("dont"), None);
    assert_eq!(value.get("etc"), None);
    assert_eq!(value.get("xyz"), Some(&json!(42)));
    assert_eq!(value.get("bar"), Some(&json!(true)));
}

#[test]
fn test_replace_field_target_key() {
    let mut xform = ReplaceField::new(ReplaceFieldTarget::Key);
    let mut config = HashMap::new();
    config.insert("exclude".to_string(), json!(["dont"]));
    xform.configure(config);

    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test",
        Some(0),
        Some(json!({"dont": "whatever", "keep": "value"})),
        json!("value"),
    );
    let transformed = xform.transform(record).unwrap().unwrap();

    let key = transformed.key().unwrap();
    assert_eq!(key.get("dont"), None);
    assert_eq!(key.get("keep"), Some(&json!("value")));
}

#[test]
fn test_close() {
    let mut xform = ReplaceField::new(ReplaceFieldTarget::Value);
    let mut config = HashMap::new();
    config.insert("exclude".to_string(), json!("field"));
    xform.configure(config);
    xform.close();
}
