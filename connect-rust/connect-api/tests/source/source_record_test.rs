// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use connect_api::connector::ConnectRecord;
use connect_api::data::{Schema, SchemaBuilder};
use connect_api::source::{SourceRecord, SourceTask, SourceTaskContext};
use std::collections::HashMap;

#[test]
fn test_source_record_basic() {
    let key_schema = SchemaBuilder::string().build();
    let value_schema = SchemaBuilder::string().build();

    let record = SourceRecord::new()
        .source_partition(HashMap::new())
        .source_offset(HashMap::new())
        .topic("test_topic")
        .kafka_partition(0)
        .key_schema(key_schema)
        .key("test_key")
        .value_schema(value_schema)
        .value("test_value");

    assert_eq!("test_topic", record.topic());
    assert_eq!(0, record.kafka_partition());
}

#[test]
fn test_source_record_with_null_key() {
    let value_schema = SchemaBuilder::string().build();

    let record = SourceRecord::new()
        .source_partition(HashMap::new())
        .source_offset(HashMap::new())
        .topic("test_topic")
        .kafka_partition(0)
        .value_schema(value_schema)
        .value("test_value");

    assert_eq!("test_topic", record.topic());
    // Key should be null
}

#[test]
fn test_source_record_with_null_value() {
    let key_schema = SchemaBuilder::string().build();

    let record = SourceRecord::new()
        .source_partition(HashMap::new())
        .source_offset(HashMap::new())
        .topic("test_topic")
        .kafka_partition(0)
        .key_schema(key_schema)
        .key("test_key");

    assert_eq!("test_topic", record.topic());
    // Value should be null
}

#[test]
fn test_source_record_source_partition() {
    let mut partition: HashMap<String, String> = HashMap::new();
    partition.insert("partition_key", "partition_value");

    let record = SourceRecord::new()
        .source_partition(partition.clone())
        .source_offset(HashMap::new())
        .topic("test_topic");

    assert_eq!(Some(&partition), record.source_partition());
}

#[test]
fn test_source_record_source_offset() {
    let mut offset: HashMap<String, String> = HashMap::new();
    offset.insert("offset_key", "offset_value");

    let record = SourceRecord::new()
        .source_partition(HashMap::new())
        .source_offset(offset.clone())
        .topic("test_topic");

    assert_eq!(Some(&offset), record.source_offset());
}

#[test]
fn test_source_record_timestamp() {
    let record = SourceRecord::new()
        .source_partition(HashMap::new())
        .source_offset(HashMap::new())
        .topic("test_topic")
        .timestamp(1234567890);

    assert_eq!(Some(1234567890), record.timestamp());
}
