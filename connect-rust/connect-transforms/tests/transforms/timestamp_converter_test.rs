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
use connect_transforms::transforms::timestamp_converter::*;
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
    assert_eq!(TimestampConverter::version(), "3.9.0");
}

#[test]
fn test_converter_key_creation() {
    let converter = TimestampConverter::new(TimestampConverterTarget::Key);
    assert_eq!(converter.target(), TimestampConverterTarget::Key);
}

#[test]
fn test_converter_value_creation() {
    let converter = TimestampConverter::new(TimestampConverterTarget::Value);
    assert_eq!(converter.target(), TimestampConverterTarget::Value);
}

#[test]
fn test_default() {
    let converter = TimestampConverter::default();
    assert_eq!(converter.target(), TimestampConverterTarget::Value);
}

#[test]
fn test_convenience_constructors() {
    assert_eq!(
        timestamp_converter_key().target(),
        TimestampConverterTarget::Key
    );
    assert_eq!(
        timestamp_converter_value().target(),
        TimestampConverterTarget::Value
    );
}

#[test]
fn test_configure_unix_to_string() {
    let mut converter = TimestampConverter::new(TimestampConverterTarget::Value);
    let mut config = HashMap::new();
    config.insert("target.type".to_string(), json!("string"));
    config.insert("format".to_string(), json!("yyyy-MM-dd HH:mm:ss"));
    config.insert("unix.precision".to_string(), json!("milliseconds"));
    converter.configure(config);
    assert_eq!(converter.target_type(), TargetType::String);
}

#[test]
fn test_configure_unix_to_timestamp() {
    let mut converter = TimestampConverter::new(TimestampConverterTarget::Value);
    let mut config = HashMap::new();
    config.insert("target.type".to_string(), json!("timestamp"));
    converter.configure(config);
    assert_eq!(converter.target_type(), TargetType::Timestamp);
}

#[test]
fn test_transform_unix_millis_to_string() {
    let mut converter = TimestampConverter::new(TimestampConverterTarget::Value);
    let mut config = HashMap::new();
    config.insert("target.type".to_string(), json!("string"));
    config.insert("format".to_string(), json!("yyyy-MM-dd HH:mm:ss"));
    config.insert("unix.precision".to_string(), json!("milliseconds"));
    converter.configure(config);
    let record = create_record(json!(1609459200000_i64));
    let result = converter.transform(record).unwrap().unwrap();
    assert!(result.value().is_string());
}

#[test]
fn test_transform_string_to_unix() {
    let mut converter = TimestampConverter::new(TimestampConverterTarget::Value);
    let mut config = HashMap::new();
    config.insert("target.type".to_string(), json!("unix"));
    config.insert("format".to_string(), json!("yyyy-MM-dd HH:mm:ss"));
    config.insert("unix.precision".to_string(), json!("milliseconds"));
    converter.configure(config);
    let record = create_record(json!("2021-01-01 00:00:00"));
    let result = converter.transform(record).unwrap().unwrap();
    assert!(result.value().is_number());
}

#[test]
fn test_transform_null_value() {
    let mut converter = TimestampConverter::new(TimestampConverterTarget::Value);
    let mut config = HashMap::new();
    config.insert("target.type".to_string(), json!("timestamp"));
    converter.configure(config);
    let record = create_record(json!(null));
    let result = converter.transform(record).unwrap().unwrap();
    assert!(result.value().is_null());
}

#[test]
fn test_close() {
    let mut converter = TimestampConverter::new(TimestampConverterTarget::Value);
    let mut config = HashMap::new();
    config.insert("target.type".to_string(), json!("timestamp"));
    converter.configure(config);
    converter.close();
}

#[test]
fn test_transformation_trait() {
    let mut converter = timestamp_converter_value();
    let mut config = HashMap::new();
    config.insert("target.type".to_string(), json!("timestamp"));
    converter.configure(config);
    let record = create_record(json!(1609459200000_i64));
    let result = Transformation::<SourceRecord>::transform(&converter, record);
    assert!(result.is_ok());
}
