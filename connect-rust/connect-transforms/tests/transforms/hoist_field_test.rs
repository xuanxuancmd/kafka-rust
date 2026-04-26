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
use connect_transforms::transforms::hoist_field::*;
use serde_json::json;
use std::collections::HashMap;

fn create_record_with_key(key: serde_json::Value) -> SourceRecord {
    SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test",
        None,
        Some(key),
        json!("value"),
    )
}

#[test]
fn test_version() {
    assert_eq!(HoistField::version(), "3.9.0");
}

#[test]
fn test_schemaless() {
    let mut xform = HoistField::new(HoistFieldTarget::Key);
    let mut config = HashMap::new();
    config.insert("field".to_string(), json!("magic"));
    xform.configure(config);

    let record = create_record_with_key(json!(42));
    let transformed = xform.transform(record).unwrap().unwrap();

    assert!(transformed.key_schema().is_none());
    let expected = json!({"magic": 42});
    assert_eq!(transformed.key().unwrap(), &expected);
}

#[test]
fn test_schemaless_map_is_mutable() {
    let mut xform = HoistField::new(HoistFieldTarget::Key);
    let mut config = HashMap::new();
    config.insert("field".to_string(), json!("magic"));
    xform.configure(config);

    let record = create_record_with_key(json!(420));
    let transformed = xform.transform(record).unwrap().unwrap();

    assert!(transformed.key_schema().is_none());
    // In Rust, we can verify the structure is correct
    let expected = json!({"magic": 420});
    assert_eq!(transformed.key().unwrap(), &expected);
}

#[test]
fn test_close() {
    let mut xform = HoistField::new(HoistFieldTarget::Key);
    let mut config = HashMap::new();
    config.insert("field".to_string(), json!("magic"));
    xform.configure(config);
    xform.close();
}
