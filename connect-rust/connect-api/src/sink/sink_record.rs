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

use crate::connector::ConnectRecord;
use crate::header::ConnectHeaders;
use common_trait::record::TimestampType;
use serde_json::Value;

/// SinkRecord represents a record for a sink connector.
///
/// This corresponds to `org.apache.kafka.connect.sink.SinkRecord` in Java.
#[derive(Debug, Clone)]
pub struct SinkRecord {
    topic: String,
    kafka_partition: Option<i32>,
    key: Option<Value>,
    key_schema: Option<String>,
    value: Value,
    value_schema: Option<String>,
    timestamp: Option<i64>,
    timestamp_type: TimestampType,
    headers: ConnectHeaders,
    kafka_offset: i64,
    original_topic: Option<String>,
    original_kafka_partition: Option<i32>,
    original_kafka_offset: Option<i64>,
}

impl SinkRecord {
    /// Creates a new SinkRecord.
    pub fn new(
        topic: impl Into<String>,
        kafka_partition: Option<i32>,
        key: Option<Value>,
        value: Value,
        kafka_offset: i64,
        timestamp_type: TimestampType,
    ) -> Self {
        SinkRecord {
            topic: topic.into(),
            kafka_partition,
            key,
            key_schema: None,
            value,
            value_schema: None,
            timestamp: None,
            timestamp_type,
            headers: ConnectHeaders::new(),
            kafka_offset,
            original_topic: None,
            original_kafka_partition: None,
            original_kafka_offset: None,
        }
    }

    /// Creates a new SinkRecord with original topic/partition/offset for tracking.
    pub fn new_with_original(
        topic: impl Into<String>,
        kafka_partition: Option<i32>,
        key: Option<Value>,
        value: Value,
        kafka_offset: i64,
        timestamp: Option<i64>,
        timestamp_type: TimestampType,
        headers: ConnectHeaders,
        original_topic: &str,
        original_kafka_partition: i32,
        original_kafka_offset: i64,
    ) -> Self {
        SinkRecord {
            topic: topic.into(),
            kafka_partition,
            key,
            key_schema: None,
            value,
            value_schema: None,
            timestamp,
            timestamp_type,
            headers,
            kafka_offset,
            original_topic: Some(original_topic.to_string()),
            original_kafka_partition: Some(original_kafka_partition),
            original_kafka_offset: Some(original_kafka_offset),
        }
    }

    /// Returns the Kafka offset.
    pub fn kafka_offset(&self) -> i64 {
        self.kafka_offset
    }

    /// Returns the timestamp type.
    pub fn timestamp_type(&self) -> TimestampType {
        self.timestamp_type
    }

    /// Returns the original topic.
    pub fn original_topic(&self) -> Option<&str> {
        self.original_topic.as_deref()
    }

    /// Returns the original Kafka partition.
    pub fn original_kafka_partition(&self) -> Option<i32> {
        self.original_kafka_partition
    }

    /// Returns the original Kafka offset.
    pub fn original_kafka_offset(&self) -> Option<i64> {
        self.original_kafka_offset
    }
}

impl ConnectRecord for SinkRecord {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn kafka_partition(&self) -> Option<i32> {
        self.kafka_partition
    }

    fn key(&self) -> Option<&Value> {
        self.key.as_ref()
    }

    fn key_schema(&self) -> Option<&str> {
        self.key_schema.as_deref()
    }

    fn value(&self) -> &Value {
        &self.value
    }

    fn value_schema(&self) -> Option<&str> {
        self.value_schema.as_deref()
    }

    fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    fn headers(&self) -> &ConnectHeaders {
        &self.headers
    }

    fn with_topic(&self, topic: impl Into<String>) -> Self {
        SinkRecord {
            topic: topic.into(),
            ..self.clone()
        }
    }

    fn with_partition(&self, partition: i32) -> Self {
        SinkRecord {
            kafka_partition: Some(partition),
            ..self.clone()
        }
    }

    fn with_timestamp(&self, timestamp: i64) -> Self {
        SinkRecord {
            timestamp: Some(timestamp),
            ..self.clone()
        }
    }

    fn with_headers(&self, headers: ConnectHeaders) -> Self {
        SinkRecord {
            headers,
            ..self.clone()
        }
    }

    fn with_key(&self, key: Option<Value>) -> Self {
        SinkRecord {
            key,
            ..self.clone()
        }
    }

    fn with_key_and_schema(&self, key: Option<Value>, key_schema: Option<String>) -> Self {
        SinkRecord {
            key,
            key_schema,
            ..self.clone()
        }
    }

    fn with_value(&self, value: Value) -> Self {
        SinkRecord {
            value,
            ..self.clone()
        }
    }

    fn with_value_and_schema(&self, value: Value, value_schema: Option<String>) -> Self {
        SinkRecord {
            value,
            value_schema,
            ..self.clone()
        }
    }
}
