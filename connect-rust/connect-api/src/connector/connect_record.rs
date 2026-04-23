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

use crate::header::ConnectHeaders;
use crate::header::Headers;
use serde_json::Value;

/// ConnectRecord trait for connect records.
///
/// This corresponds to `org.apache.kafka.connect.connector.ConnectRecord` in Java.
pub trait ConnectRecord {
    /// Returns the topic of this record.
    fn topic(&self) -> &str;

    /// Returns the Kafka partition of this record.
    fn kafka_partition(&self) -> Option<i32>;

    /// Returns the key of this record.
    fn key(&self) -> Option<&Value>;

    /// Returns the key schema name of this record.
    fn key_schema(&self) -> Option<&str>;

    /// Returns the value of this record.
    fn value(&self) -> &Value;

    /// Returns the value schema name of this record.
    fn value_schema(&self) -> Option<&str>;

    /// Returns the timestamp of this record.
    fn timestamp(&self) -> Option<i64>;

    /// Returns the headers of this record.
    fn headers(&self) -> &ConnectHeaders;

    /// Creates a new record with the given topic.
    fn with_topic(&self, topic: impl Into<String>) -> Self;

    /// Creates a new record with the given partition.
    fn with_partition(&self, partition: i32) -> Self;

    /// Creates a new record with the given timestamp.
    fn with_timestamp(&self, timestamp: i64) -> Self;

    /// Creates a new record with the given headers.
    fn with_headers(&self, headers: ConnectHeaders) -> Self;

    /// Creates a new record with the given key.
    ///
    /// This corresponds to transformations that modify the record's key.
    fn with_key(&self, key: Option<Value>) -> Self;

    /// Creates a new record with the given key and key schema.
    ///
    /// This corresponds to transformations that modify both the key and its schema.
    fn with_key_and_schema(&self, key: Option<Value>, key_schema: Option<String>) -> Self;

    /// Creates a new record with the given value.
    ///
    /// This corresponds to transformations that modify the record's value.
    fn with_value(&self, value: Value) -> Self;

    /// Creates a new record with the given value and value schema.
    ///
    /// This corresponds to transformations that modify both the value and its schema.
    fn with_value_and_schema(&self, value: Value, value_schema: Option<String>) -> Self;
}
