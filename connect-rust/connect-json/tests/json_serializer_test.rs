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

//! Tests for json_serializer module.

use connect_json::JsonSerializer;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn test_new() {
    let serializer = JsonSerializer::new();
    // Verify serializer works correctly after construction
    let data = json!("test");
    let result = serializer.serialize("test", Some(&data));
    assert!(result.is_some());
}

#[test]
fn test_with_blackbird_true() {
    let serializer = JsonSerializer::with_blackbird(true);
    // Verify serializer works correctly with blackbird enabled
    let data = json!("test");
    let result = serializer.serialize("test", Some(&data));
    assert!(result.is_some());
}

#[test]
fn test_with_blackbird_false() {
    let serializer = JsonSerializer::with_blackbird(false);
    // Verify serializer works correctly with blackbird disabled
    let data = json!("test");
    let result = serializer.serialize("test", Some(&data));
    assert!(result.is_some());
}

#[test]
fn test_serialize_null() {
    let serializer = JsonSerializer::new();
    let result = serializer.serialize("test", None);
    assert!(result.is_none());
}

#[test]
fn test_serialize_string() {
    let serializer = JsonSerializer::new();
    let data = json!("hello");
    let result = serializer.serialize("test", Some(&data));
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"\"hello\"");
}

#[test]
fn test_serialize_number() {
    let serializer = JsonSerializer::new();
    let data = json!(42);
    let result = serializer.serialize("test", Some(&data));
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"42");
}

#[test]
fn test_serialize_object() {
    let serializer = JsonSerializer::new();
    let data = json!({"key": "value"});
    let result = serializer.serialize("test", Some(&data));
    assert!(result.is_some());
    let bytes = result.unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed["key"], "value");
}

#[test]
fn test_serialize_array() {
    let serializer = JsonSerializer::new();
    let data = json!([1, 2, 3]);
    let result = serializer.serialize("test", Some(&data));
    assert!(result.is_some());
    let bytes = result.unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed, json!([1, 2, 3]));
}

#[test]
fn test_serialize_boolean() {
    let serializer = JsonSerializer::new();
    let data = json!(true);
    let result = serializer.serialize("test", Some(&data));
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"true");
}

#[test]
fn test_serialize_null_value() {
    let serializer = JsonSerializer::new();
    let data = json!(null);
    let result = serializer.serialize("test", Some(&data));
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"null");
}

#[test]
fn test_configure() {
    let mut serializer = JsonSerializer::new();
    let configs = HashMap::new();
    serializer.configure(configs, false);
    // Verify serializer works correctly after configure
    let data = json!("test");
    let result = serializer.serialize("test", Some(&data));
    assert!(result.is_some());
}

#[test]
fn test_close() {
    let mut serializer = JsonSerializer::new();
    serializer.close();
    // No assertion needed - just verify it doesn't panic
}

#[test]
fn test_default() {
    let serializer = JsonSerializer::default();
    // Verify serializer works correctly after default construction
    let data = json!("test");
    let result = serializer.serialize("test", Some(&data));
    assert!(result.is_some());
}
