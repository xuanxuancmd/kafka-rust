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

use connect_transforms::transforms::drop_headers::{DropHeaders, HEADERS_CONFIG};
use connect_api::header::{ConnectHeaders, Headers};
use connect_api::connector::ConnectRecord;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::json;
use std::collections::HashMap;

fn create_record_with_headers(headers: ConnectHeaders) -> SourceRecord {
    SourceRecord::new_with_headers(
        HashMap::new(),
        HashMap::new(),
        "test-topic",
        None,
        Some(json!("key")),
        json!("value"),
        headers,
    )
}

#[test]
fn test_version() {
    assert_eq!(DropHeaders::version(), "3.9.0");
}

#[test]
fn test_drop_single_header() {
    let mut headers = ConnectHeaders::new();
    headers.add("keep", json!("value1"));
    headers.add("drop", json!("value2"));

    let record = create_record_with_headers(headers);

    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert(HEADERS_CONFIG.to_string(), json!(["drop"]));
    transform.configure(config);

    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.headers().headers_with_key("keep").len(), 1);
    assert_eq!(result.headers().headers_with_key("drop").len(), 0);
}

#[test]
fn test_drop_multiple_headers() {
    let mut headers = ConnectHeaders::new();
    headers.add("keep1", json!("v1"));
    headers.add("drop1", json!("v2"));
    headers.add("keep2", json!("v3"));
    headers.add("drop2", json!("v4"));

    let record = create_record_with_headers(headers);

    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert(HEADERS_CONFIG.to_string(), json!(["drop1", "drop2"]));
    transform.configure(config);

    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.headers().headers_with_key("keep1").len(), 1);
    assert_eq!(result.headers().headers_with_key("keep2").len(), 1);
    assert_eq!(result.headers().headers_with_key("drop1").len(), 0);
    assert_eq!(result.headers().headers_with_key("drop2").len(), 0);
}

#[test]
fn test_drop_nonexistent_header() {
    let mut headers = ConnectHeaders::new();
    headers.add("keep", json!("value"));

    let record = create_record_with_headers(headers);

    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert(HEADERS_CONFIG.to_string(), json!(["nonexistent"]));
    transform.configure(config);

    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.headers().headers_with_key("keep").len(), 1);
}

#[test]
fn test_drop_all_headers() {
    let mut headers = ConnectHeaders::new();
    headers.add("h1", json!("v1"));
    headers.add("h2", json!("v2"));

    let record = create_record_with_headers(headers);

    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert(HEADERS_CONFIG.to_string(), json!(["h1", "h2"]));
    transform.configure(config);

    let result = transform.transform(record).unwrap().unwrap();
    assert!(result.headers().is_empty());
}

#[test]
fn test_close() {
    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert(HEADERS_CONFIG.to_string(), json!(["test"]));
    transform.configure(config);
    transform.close();
}

#[test]
fn test_preserves_topic() {
    let mut headers = ConnectHeaders::new();
    headers.add("h", json!("v"));

    let record = SourceRecord::new_with_headers(
        HashMap::new(),
        HashMap::new(),
        "custom-topic",
        Some(5),
        Some(json!("key")),
        json!("value"),
        headers,
    );

    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert(HEADERS_CONFIG.to_string(), json!(["h"]));
    transform.configure(config);

    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.topic(), "custom-topic");
    assert_eq!(result.kafka_partition(), Some(5));
}

#[test]
fn test_duplicate_header_keys() {
    let mut headers = ConnectHeaders::new();
    headers.add("dup", json!("v1"));
    headers.add("dup", json!("v2"));
    headers.add("keep", json!("v3"));

    let record = create_record_with_headers(headers);

    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert(HEADERS_CONFIG.to_string(), json!(["dup"]));
    transform.configure(config);

    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.headers().headers_with_key("dup").len(), 0);
    assert_eq!(result.headers().headers_with_key("keep").len(), 1);
}

#[test]
#[should_panic(expected = "Missing required configuration")]
fn test_configure_missing_headers() {
    let mut transform = DropHeaders::new();
    transform.configure(HashMap::new());
}

#[test]
#[should_panic(expected = "Headers list must not be empty")]
fn test_configure_empty_headers() {
    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert(HEADERS_CONFIG.to_string(), json!([]));
    transform.configure(config);
}

#[test]
#[should_panic(expected = "Header key must not be empty string")]
fn test_configure_empty_header_key() {
    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert(HEADERS_CONFIG.to_string(), json!([""]));
    transform.configure(config);
}

#[test]
#[should_panic(expected = "Headers configuration must be an array")]
fn test_configure_not_array() {
    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert(HEADERS_CONFIG.to_string(), json!("not-array"));
    transform.configure(config);
}

#[test]
#[should_panic(expected = "Each header key must be a string")]
fn test_configure_not_string() {
    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert(HEADERS_CONFIG.to_string(), json!([42]));
    transform.configure(config);
}

#[test]
fn test_default() {
    let transform = DropHeaders::default();
    assert!(transform.headers_to_drop.is_empty());
}

#[test]
fn test_new() {
    let transform = DropHeaders::new();
    assert!(transform.headers_to_drop.is_empty());
}
