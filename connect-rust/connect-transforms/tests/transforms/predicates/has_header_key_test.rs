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
use connect_api::header::{ConnectHeaders, Headers};
use connect_api::source::SourceRecord;
use connect_api::transforms::predicates::Predicate;
use connect_transforms::transforms::predicates::HasHeaderKey;
use serde_json::json;
use std::collections::HashMap;

fn create_predicate() -> HasHeaderKey {
    HasHeaderKey::new()
}

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
    assert_eq!(HasHeaderKey::version(), "3.9.0");
}

#[test]
fn test_creation() {
    let predicate = create_predicate();
    assert!(true); // Predicate created successfully
}

#[test]
fn test_default() {
    let predicate = HasHeaderKey::default();
    assert!(true); // Default predicate created successfully
}

#[test]
fn test_predicate_header_present() {
    let mut headers = ConnectHeaders::new();
    headers.add("X-Request-Id", json!("12345"));
    let record = create_record_with_headers(headers);

    let mut predicate: HasHeaderKey = create_predicate();
    let mut config = HashMap::new();
    config.insert("name".to_string(), json!("X-Request-Id"));
    Predicate::<SourceRecord>::configure(&mut predicate, config);

    let result = Predicate::<SourceRecord>::test(&predicate, &record);
    assert!(result);
}

#[test]
fn test_predicate_header_missing() {
    let headers = ConnectHeaders::new();
    let record = create_record_with_headers(headers);

    let mut predicate: HasHeaderKey = create_predicate();
    let mut config = HashMap::new();
    config.insert("name".to_string(), json!("X-Request-Id"));
    Predicate::<SourceRecord>::configure(&mut predicate, config);

    let result = Predicate::<SourceRecord>::test(&predicate, &record);
    assert!(!result);
}

#[test]
fn test_close() {
    let mut predicate: HasHeaderKey = create_predicate();
    let mut config = HashMap::new();
    config.insert("name".to_string(), json!("test"));
    Predicate::<SourceRecord>::configure(&mut predicate, config);
    Predicate::<SourceRecord>::close(&mut predicate);
}

#[test]
fn test_predicate_header_different_key() {
    let mut headers = ConnectHeaders::new();
    headers.add("other-header", json!("value"));
    let record = create_record_with_headers(headers);

    let mut predicate: HasHeaderKey = create_predicate();
    let mut config = HashMap::new();
    config.insert("name".to_string(), json!("target-header"));
    Predicate::<SourceRecord>::configure(&mut predicate, config);

    let result = Predicate::<SourceRecord>::test(&predicate, &record);
    assert!(!result);
}

#[test]
fn test_predicate_multiple_headers_with_target() {
    let mut headers = ConnectHeaders::new();
    headers.add("header1", json!("value1"));
    headers.add("header2", json!("value2"));
    headers.add("target", json!("target-value"));
    let record = create_record_with_headers(headers);

    let mut predicate: HasHeaderKey = create_predicate();
    let mut config = HashMap::new();
    config.insert("name".to_string(), json!("target"));
    Predicate::<SourceRecord>::configure(&mut predicate, config);

    let result = Predicate::<SourceRecord>::test(&predicate, &record);
    assert!(result);
}
