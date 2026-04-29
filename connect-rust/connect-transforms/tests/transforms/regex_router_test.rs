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
use connect_transforms::transforms::regex_router::*;
use serde_json::json;
use std::collections::HashMap;

fn apply(regex: &str, replacement: &str, topic: &str) -> String {
    let mut config = HashMap::new();
    config.insert("regex".to_string(), json!(regex));
    config.insert("replacement".to_string(), json!(replacement));
    let mut router = RegexRouter::new();
    router.configure(config);

    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        topic,
        Some(0),
        None,
        json!("value"),
    );
    let transformed = router.transform(record).unwrap().unwrap();
    router.close();
    transformed.topic().to_string()
}

#[test]
fn test_version() {
    assert_eq!(RegexRouter::version(), "3.9.0");
}

#[test]
fn test_static_replacement() {
    let result = apply("foo", "bar", "foo");
    assert_eq!(result, "bar");
}

#[test]
fn test_doesnt_match() {
    let result = apply("foo", "bar", "orig");
    // When regex doesn't match, original topic should be returned
    assert_eq!(result, "orig");
}

#[test]
fn test_identity() {
    let result = apply("(.*)", "$1", "orig");
    assert_eq!(result, "orig");
}

#[test]
fn test_add_prefix() {
    let result = apply("(.*)", "prefix-$1", "orig");
    assert_eq!(result, "prefix-orig");
}

#[test]
fn test_add_suffix() {
    let result = apply("(.*)", "$1-suffix", "orig");
    assert_eq!(result, "orig-suffix");
}

#[test]
fn test_slice() {
    let result = apply("(.*)-(\\d\\d\\d\\d\\d\\d\\d\\d)", "$1", "index-20160117");
    assert_eq!(result, "index");
}

#[test]
fn test_close() {
    let mut router = RegexRouter::new();
    let mut config = HashMap::new();
    config.insert("regex".to_string(), json!("(.*)"));
    config.insert("replacement".to_string(), json!("prefix-$1"));
    router.configure(config);
    router.close();
}
