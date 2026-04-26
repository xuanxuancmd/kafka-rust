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
use connect_transforms::transforms::insert_header::*;
use serde_json::json;
use std::collections::HashMap;

fn create_record_with_headers(headers: ConnectHeaders) -> SourceRecord {
    SourceRecord::new_with_headers(
        HashMap::new(),
        HashMap::new(),
        "topic",
        Some(0),
        Some(json!("key")),
        json!("value"),
        headers,
    )
}

#[test]
fn test_version() {
    assert_eq!(InsertHeader::version(), "3.9.0");
}

#[test]
fn test_insertion_with_existing_other_header() {
    let mut xform = InsertHeader::new();
    let mut config = HashMap::new();
    config.insert("header".to_string(), json!("inserted"));
    config.insert("valueLiteral".to_string(), json!("inserted-value"));
    Transformation::<SourceRecord>::configure(&mut xform, config);

    let mut headers = ConnectHeaders::new();
    headers.add("existing", json!("existing-value"));
    let record = create_record_with_headers(headers);

    let transformed = xform.transform(record).unwrap().unwrap();
    let transformed_headers = transformed.headers();

    assert_eq!(transformed_headers.size(), 2);
}

#[test]
fn test_insertion_with_existing_same_header() {
    let mut xform = InsertHeader::new();
    let mut config = HashMap::new();
    config.insert("header".to_string(), json!("existing"));
    config.insert("valueLiteral".to_string(), json!("inserted-value"));
    Transformation::<SourceRecord>::configure(&mut xform, config);

    let mut headers = ConnectHeaders::new();
    headers.add("existing", json!("preexisting-value"));
    let record = create_record_with_headers(headers);

    let transformed = xform.transform(record).unwrap().unwrap();
    let transformed_headers = transformed.headers();

    // Headers with same key should have both values
    assert_eq!(transformed_headers.size(), 2);
}

#[test]
fn test_close() {
    let mut xform = InsertHeader::new();
    let mut config = HashMap::new();
    config.insert("header".to_string(), json!("test"));
    config.insert("valueLiteral".to_string(), json!("value"));
    Transformation::<SourceRecord>::configure(&mut xform, config);
    Transformation::<SourceRecord>::close(&mut xform);
}

#[test]
fn test_transform_preserves_non_headers() {
    let mut xform = InsertHeader::new();
    let mut config = HashMap::new();
    config.insert("header".to_string(), json!("inserted"));
    config.insert("valueLiteral".to_string(), json!("inserted-value"));
    Transformation::<SourceRecord>::configure(&mut xform, config);

    let headers = ConnectHeaders::new();
    let original = create_record_with_headers(headers);

    let transformed = xform.transform(original.clone()).unwrap().unwrap();

    // Verify non-header fields are preserved
    assert_eq!(transformed.topic(), "topic");
    assert_eq!(transformed.kafka_partition(), Some(0));
}

#[test]
fn test_insertion_with_byte_header() {
    let mut xform = InsertHeader::new();
    let mut config = HashMap::new();
    config.insert("header".to_string(), json!("inserted"));
    config.insert("valueLiteral".to_string(), json!("1"));
    Transformation::<SourceRecord>::configure(&mut xform, config);

    let mut headers = ConnectHeaders::new();
    headers.add("existing", json!("existing-value"));
    let record = create_record_with_headers(headers);

    let transformed = xform.transform(record).unwrap().unwrap();
    let transformed_headers = transformed.headers();

    assert_eq!(transformed_headers.size(), 2);
}
