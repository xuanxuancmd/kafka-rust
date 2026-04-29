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
use crate::header::{ConnectHeaders, Headers};
use serde_json::Value;
use std::collections::HashMap;

/// SourceRecord represents a record from a source connector.
///
/// This corresponds to `org.apache.kafka.connect.source.SourceRecord` in Java.
#[derive(Debug, Clone)]
pub struct SourceRecord {
    source_partition: HashMap<String, Value>,
    source_offset: HashMap<String, Value>,
    topic: String,
    kafka_partition: Option<i32>,
    key: Option<Value>,
    key_schema: Option<String>,
    value: Value,
    value_schema: Option<String>,
    timestamp: Option<i64>,
    headers: ConnectHeaders,
}

impl PartialEq for SourceRecord {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.kafka_partition == other.kafka_partition
            && self.source_partition == other.source_partition
            && self.source_offset == other.source_offset
    }
}

impl Eq for SourceRecord {}

impl std::hash::Hash for SourceRecord {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.kafka_partition.hash(state);
        // Hash the source_partition and source_offset by iterating through keys
        for (k, v) in &self.source_partition {
            k.hash(state);
            // Hash the value as a string representation for simplicity
            v.to_string().hash(state);
        }
        for (k, v) in &self.source_offset {
            k.hash(state);
            v.to_string().hash(state);
        }
    }
}

impl SourceRecord {
    /// Creates a new SourceRecord.
    pub fn new(
        source_partition: HashMap<String, Value>,
        source_offset: HashMap<String, Value>,
        topic: impl Into<String>,
        kafka_partition: Option<i32>,
        key: Option<Value>,
        value: Value,
    ) -> Self {
        SourceRecord {
            source_partition,
            source_offset,
            topic: topic.into(),
            kafka_partition,
            key,
            key_schema: None,
            value,
            value_schema: None,
            timestamp: None,
            headers: ConnectHeaders::new(),
        }
    }

    /// Creates a new SourceRecord with headers.
    pub fn new_with_headers(
        source_partition: HashMap<String, Value>,
        source_offset: HashMap<String, Value>,
        topic: impl Into<String>,
        kafka_partition: Option<i32>,
        key: Option<Value>,
        value: Value,
        headers: ConnectHeaders,
    ) -> Self {
        SourceRecord {
            source_partition,
            source_offset,
            topic: topic.into(),
            kafka_partition,
            key,
            key_schema: None,
            value,
            value_schema: None,
            timestamp: None,
            headers,
        }
    }

    /// Returns the source partition.
    pub fn source_partition(&self) -> &HashMap<String, Value> {
        &self.source_partition
    }

    /// Returns the source offset.
    pub fn source_offset(&self) -> &HashMap<String, Value> {
        &self.source_offset
    }
}

impl ConnectRecord for SourceRecord {
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
        SourceRecord {
            topic: topic.into(),
            ..self.clone()
        }
    }

    fn with_partition(&self, partition: i32) -> Self {
        SourceRecord {
            kafka_partition: Some(partition),
            ..self.clone()
        }
    }

    fn with_timestamp(&self, timestamp: i64) -> Self {
        SourceRecord {
            timestamp: Some(timestamp),
            ..self.clone()
        }
    }

    fn with_headers(&self, headers: ConnectHeaders) -> Self {
        SourceRecord {
            headers,
            ..self.clone()
        }
    }

    fn with_key(&self, key: Option<Value>) -> Self {
        SourceRecord {
            key,
            ..self.clone()
        }
    }

    fn with_key_and_schema(&self, key: Option<Value>, key_schema: Option<String>) -> Self {
        SourceRecord {
            key,
            key_schema,
            ..self.clone()
        }
    }

    fn with_value(&self, value: Value) -> Self {
        SourceRecord {
            value,
            ..self.clone()
        }
    }

    fn with_value_and_schema(&self, value: Value, value_schema: Option<String>) -> Self {
        SourceRecord {
            value,
            value_schema,
            ..self.clone()
        }
    }
}
