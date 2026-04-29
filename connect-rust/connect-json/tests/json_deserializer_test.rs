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

//! Tests for json_deserializer module.

use common_trait::serialization::Deserializer;
use connect_json::JsonDeserializer;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn test_new() {
    let deserializer = JsonDeserializer::new();
    assert!(deserializer.is_blackbird_enabled());
}

#[test]
fn test_with_blackbird_enabled() {
    let deserializer = JsonDeserializer::with_blackbird(true);
    assert!(deserializer.is_blackbird_enabled());
}

#[test]
fn test_with_blackbird_disabled() {
    let deserializer = JsonDeserializer::with_blackbird(false);
    assert!(!deserializer.is_blackbird_enabled());
}

#[test]
fn test_default() {
    let deserializer = JsonDeserializer::default();
    assert!(deserializer.is_blackbird_enabled());
}

#[test]
fn test_deserialize_null() {
    let deserializer = JsonDeserializer::new();
    let result = deserializer.deserialize("test", &[]);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_deserialize_json_object() {
    let deserializer = JsonDeserializer::new();
    let json_bytes = br#"{"key": "value", "number": 42}"#;
    let result = deserializer.deserialize("test", json_bytes);
    assert!(result.is_ok());
    let value = result.unwrap();
    assert!(value.is_some());
    let json_value = value.unwrap();
    assert_eq!(json_value["key"], "value");
    assert_eq!(json_value["number"], 42);
}

#[test]
fn test_deserialize_json_array() {
    let deserializer = JsonDeserializer::new();
    let json_bytes = br#"[1, 2, 3, "four"]"#;
    let result = deserializer.deserialize("test", json_bytes);
    assert!(result.is_ok());
    let value = result.unwrap();
    assert!(value.is_some());
    let json_value = value.unwrap();
    assert!(json_value.is_array());
    assert_eq!(json_value[0], 1);
    assert_eq!(json_value[3], "four");
}

#[test]
fn test_deserialize_json_string() {
    let deserializer = JsonDeserializer::new();
    let json_bytes = br#""hello world""#;
    let result = deserializer.deserialize("test", json_bytes);
    assert!(result.is_ok());
    let value = result.unwrap();
    assert!(value.is_some());
    let json_value = value.unwrap();
    assert!(json_value.is_string());
    assert_eq!(json_value, "hello world");
}

#[test]
fn test_deserialize_json_number() {
    let deserializer = JsonDeserializer::new();
    let json_bytes = br#"123.45"#;
    let result = deserializer.deserialize("test", json_bytes);
    assert!(result.is_ok());
    let value = result.unwrap();
    assert!(value.is_some());
    let json_value = value.unwrap();
    assert!(json_value.is_number());
    assert_eq!(json_value, json!(123.45));
}

#[test]
fn test_deserialize_json_boolean() {
    let deserializer = JsonDeserializer::new();
    let json_bytes = br#"true"#;
    let result = deserializer.deserialize("test", json_bytes);
    assert!(result.is_ok());
    let value = result.unwrap();
    assert!(value.is_some());
    let json_value = value.unwrap();
    assert!(json_value.is_boolean());
    assert_eq!(json_value, true);
}

#[test]
fn test_deserialize_invalid_json() {
    let deserializer = JsonDeserializer::new();
    let invalid_bytes = br#"{"invalid": json}"#;
    let result = deserializer.deserialize("test", invalid_bytes);
    assert!(result.is_err());
}

#[test]
fn test_deserialize_empty_object() {
    let deserializer = JsonDeserializer::new();
    let json_bytes = br#"{}"#;
    let result = deserializer.deserialize("test", json_bytes);
    assert!(result.is_ok());
    let value = result.unwrap();
    assert!(value.is_some());
    let json_value = value.unwrap();
    assert!(json_value.is_object());
}

#[test]
fn test_configure() {
    let mut deserializer = JsonDeserializer::new();
    let configs = HashMap::new();
    deserializer.configure(configs, false);
    // configure should not change the state
    assert!(deserializer.is_blackbird_enabled());
}

#[test]
fn test_close() {
    let mut deserializer = JsonDeserializer::new();
    deserializer.close();
    // close should not change the state
    assert!(deserializer.is_blackbird_enabled());
}
