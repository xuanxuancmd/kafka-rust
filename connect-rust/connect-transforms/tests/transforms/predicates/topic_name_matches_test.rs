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
use connect_api::transforms::predicates::Predicate;
use connect_transforms::transforms::predicates::topic_name_matches::*;
use serde_json::json;
use std::collections::HashMap;

fn create_record_with_topic(topic: &str) -> SourceRecord {
    SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        topic,
        None,
        None,
        json!("value"),
    )
}

fn create_predicate() -> TopicNameMatches {
    TopicNameMatches::new()
}

#[test]
fn test_version() {
    assert_eq!(TopicNameMatches::version(), "3.9.0");
}

#[test]
fn test_pattern_matches_prefix() {
    let mut predicate: TopicNameMatches = create_predicate();
    let mut config = HashMap::new();
    config.insert("pattern".to_string(), json!("my-prefix-.*"));
    Predicate::<SourceRecord>::configure(&mut predicate, config);

    assert!(Predicate::<SourceRecord>::test(
        &predicate,
        &create_record_with_topic("my-prefix-")
    ));
    assert!(Predicate::<SourceRecord>::test(
        &predicate,
        &create_record_with_topic("my-prefix-foo")
    ));
    assert!(!Predicate::<SourceRecord>::test(
        &predicate,
        &create_record_with_topic("x-my-prefix-")
    ));
    assert!(!Predicate::<SourceRecord>::test(
        &predicate,
        &create_record_with_topic("x-my-prefix-foo")
    ));
}

#[test]
fn test_pattern_doesnt_match() {
    let mut predicate: TopicNameMatches = create_predicate();
    let mut config = HashMap::new();
    config.insert("pattern".to_string(), json!("my-prefix-.*"));
    Predicate::<SourceRecord>::configure(&mut predicate, config);

    assert!(!Predicate::<SourceRecord>::test(
        &predicate,
        &create_record_with_topic("your-prefix-")
    ));
    assert!(!Predicate::<SourceRecord>::test(
        &predicate,
        &create_record_with_topic("your-prefix-foo")
    ));
}

#[test]
fn test_null_topic() {
    let mut predicate: TopicNameMatches = create_predicate();
    let mut config = HashMap::new();
    config.insert("pattern".to_string(), json!("my-prefix-.*"));
    Predicate::<SourceRecord>::configure(&mut predicate, config);

    let record = SourceRecord::new(
        HashMap::new(),
        HashMap::new(),
        "", // Empty topic
        None,
        None,
        json!("value"),
    );
    assert!(!Predicate::<SourceRecord>::test(&predicate, &record));
}

#[test]
fn test_close() {
    let mut predicate: TopicNameMatches = create_predicate();
    let mut config = HashMap::new();
    config.insert("pattern".to_string(), json!("test.*"));
    Predicate::<SourceRecord>::configure(&mut predicate, config);
    Predicate::<SourceRecord>::close(&mut predicate);
}
