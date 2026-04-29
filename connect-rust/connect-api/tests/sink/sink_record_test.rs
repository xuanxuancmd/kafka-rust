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

use common_trait::record::TimestampType;
use connect_api::connector::ConnectRecord;
use connect_api::data::{Schema, SchemaBuilder};
use connect_api::sink::{SinkRecord, SinkTask, SinkTaskContext};
use std::collections::HashMap;

#[test]
fn test_sink_record_basic() {
    let key_schema = SchemaBuilder::string().build();
    let value_schema = SchemaBuilder::string().build();

    let record = SinkRecord::new()
        .topic("test_topic")
        .kafka_partition(0)
        .kafka_offset(0)
        .key_schema(key_schema)
        .key("test_key")
        .value_schema(value_schema)
        .value("test_value")
        .timestamp_type(TimestampType::CreateTime);

    assert_eq!("test_topic", record.topic());
    assert_eq!(0, record.kafka_partition());
    assert_eq!(0, record.kafka_offset());
}

#[test]
fn test_sink_record_with_null_key() {
    let value_schema = SchemaBuilder::string().build();

    let record = SinkRecord::new()
        .topic("test_topic")
        .kafka_partition(0)
        .kafka_offset(0)
        .value_schema(value_schema)
        .value("test_value");

    assert_eq!("test_topic", record.topic());
    // Key should be null
}

#[test]
fn test_sink_record_with_null_value() {
    let key_schema = SchemaBuilder::string().build();

    let record = SinkRecord::new()
        .topic("test_topic")
        .kafka_partition(0)
        .kafka_offset(0)
        .key_schema(key_schema)
        .key("test_key");

    assert_eq!("test_topic", record.topic());
    // Value should be null
}

#[test]
fn test_sink_record_offset() {
    let record = SinkRecord::new()
        .topic("test_topic")
        .kafka_partition(0)
        .kafka_offset(12345);

    assert_eq!(12345, record.kafka_offset());
}

#[test]
fn test_sink_record_timestamp_type() {
    let record = SinkRecord::new()
        .topic("test_topic")
        .kafka_partition(0)
        .kafka_offset(0)
        .timestamp_type(TimestampType::CreateTime);

    assert_eq!(TimestampType::CreateTime, record.timestamp_type());
}

#[test]
fn test_sink_record_log_append_time() {
    let record = SinkRecord::new()
        .topic("test_topic")
        .kafka_partition(0)
        .kafka_offset(0)
        .timestamp_type(TimestampType::LogAppendTime);

    assert_eq!(TimestampType::LogAppendTime, record.timestamp_type());
}

#[test]
fn test_sink_record_original_topic() {
    let record = SinkRecord::new()
        .topic("test_topic")
        .kafka_partition(0)
        .kafka_offset(0)
        .original_topic("original_topic");

    assert_eq!(Some("original_topic"), record.original_topic());
}
