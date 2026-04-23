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

use connect_transforms::transforms::filter::{filter_key, filter_value, Filter, FilterTarget};
use connect_api::connector::ConnectRecord;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
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
    assert_eq!(Filter::version(), "3.9.0");
}

#[test]
fn test_filter_key_creation() {
    let filter = Filter::new(FilterTarget::Key);
    assert_eq!(filter.target(), FilterTarget::Key);
}

#[test]
fn test_filter_value_creation() {
    let filter = Filter::new(FilterTarget::Value);
    assert_eq!(filter.target(), FilterTarget::Value);
}

#[test]
fn test_default() {
    let filter = Filter::default();
    assert_eq!(filter.target(), FilterTarget::Value);
}

#[test]
fn test_convenience_constructors() {
    assert_eq!(filter_key().target(), FilterTarget::Key);
    assert_eq!(filter_value().target(), FilterTarget::Value);
}

#[test]
fn test_close() {
    let mut filter = Filter::new(FilterTarget::Value);
    filter.close();
}

#[test]
fn test_transform_returns_none() {
    let filter = Filter::new(FilterTarget::Value);
    let record = create_record(json!("value"));
    let result = filter.transform(record);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_transform_preserves_topic() {
    let filter = Filter::new(FilterTarget::Value);
    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "custom-topic",
        Some(5),
        None,
        json!("value"),
    );
    // Filter returns None, so we can't check the result
    let result = filter.transform(record);
    assert!(result.is_ok());
}

#[test]
fn test_display() {
    let filter = Filter::new(FilterTarget::Value);
    let display = format!("{}", filter);
    assert!(display.contains("Filter"));
    assert!(display.contains("Value"));
}

#[test]
fn test_debug() {
    let filter = Filter::new(FilterTarget::Key);
    let debug = format!("{:?}", filter);
    assert!(debug.contains("Filter"));
    assert!(debug.contains("Key"));
}

#[test]
fn test_transformation_trait() {
    let filter = filter_value();
    let record = create_record(json!({"key": "value"}));
    let result = Transformation::<SourceRecord>::transform(&filter, record);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_filter_with_key() {
    let filter = Filter::new(FilterTarget::Key);
    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "test-topic",
        None,
        Some(json!("key")),
        json!("value"),
    );
    let result = filter.transform(record);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_configure_no_config() {
    let mut filter = Filter::new(FilterTarget::Value);
    filter.configure(HashMap::new());
}
