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
use connect_api::transforms::Transformation;
use connect_transforms::transforms::drop_headers::*;
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
    headers.add("keep", json!("v1"));
    headers.add("drop", json!("v2"));
    let record = create_record_with_headers(headers);

    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert("headers".to_string(), json!(["drop"]));
    transform.configure(config);

    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.headers().headers_with_key("keep").len(), 1);
    assert_eq!(result.headers().headers_with_key("drop").len(), 0);
}

#[test]
fn test_drop_multiple_headers() {
    let mut headers = ConnectHeaders::new();
    headers.add("h1", json!("v1"));
    headers.add("h2", json!("v2"));
    headers.add("h3", json!("v3"));
    let record = create_record_with_headers(headers);

    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert("headers".to_string(), json!(["h1", "h2"]));
    transform.configure(config);

    let result = transform.transform(record).unwrap().unwrap();
    assert_eq!(result.headers().headers_with_key("h3").len(), 1);
    assert_eq!(result.headers().headers_with_key("h1").len(), 0);
}

#[test]
fn test_drop_all_headers() {
    let mut headers = ConnectHeaders::new();
    headers.add("h1", json!("v1"));
    headers.add("h2", json!("v2"));
    let record = create_record_with_headers(headers);

    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert("headers".to_string(), json!(["h1", "h2"]));
    transform.configure(config);

    let result = transform.transform(record).unwrap().unwrap();
    assert!(result.headers().is_empty());
}

#[test]
fn test_close() {
    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert("headers".to_string(), json!(["test"]));
    transform.configure(config);
    transform.close();
}

#[test]
fn test_transformation_trait() {
    let mut headers = ConnectHeaders::new();
    headers.add("drop", json!("v"));
    let record = create_record_with_headers(headers);

    let mut transform = DropHeaders::new();
    let mut config = HashMap::new();
    config.insert("headers".to_string(), json!(["drop"]));
    transform.configure(config);

    let result = Transformation::<SourceRecord>::transform(&transform, record);
    assert!(result.is_ok());
}
