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

//! Connector types for Kafka Connect API.
//! This module contains type definitions that are shared across connector implementations.

use crate::data::{Headers, Schema};
use std::sync::Arc;

/// ConnectRecord is a base trait for records in Kafka Connect.
pub trait ConnectRecord<T>: Send + Sync {
    /// Returns to topic.
    fn topic(&self) -> &str;

    /// Returns to Kafka partition.
    fn kafka_partition(&self) -> Option<i32>;

    /// Returns to key schema.
    fn key_schema(&self) -> Option<Arc<dyn Schema>>;

    /// Returns to key.
    fn key(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>>;

    /// Returns to value schema.
    fn value_schema(&self) -> Option<Arc<dyn Schema>>;

    /// Returns to value.
    fn value(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>>;

    /// Returns to timestamp.
    fn timestamp(&self) -> Option<i64>;

    /// Returns to headers.
    fn headers(&self) -> &dyn crate::data::Headers;

    /// Creates a new record with specified topic, partition, key, value, timestamp, and headers.
    /// This is used by transformations to create modified versions of records.
    ///
    /// # Arguments
    ///
    /// * `topic` - Optional new topic (defaults to current topic)
    /// * `partition` - Optional new partition (defaults to current partition)
    /// * `key_schema` - Optional new key schema (defaults to current key schema)
    /// * `key` - Optional new key (defaults to current key)
    /// * `value_schema` - Optional new value schema (defaults to current value schema)
    /// * `value` - Optional new value (defaults to current value)
    /// * `timestamp` - Optional new timestamp (defaults to current timestamp)
    /// * `headers` - Optional new headers (defaults to current headers)
    ///
    /// # Returns
    ///
    /// A new record of type T with the specified modifications.
    fn new_record(
        self,
        topic: Option<&str>,
        partition: Option<i32>,
        key_schema: Option<Arc<dyn Schema>>,
        key: Option<Arc<dyn std::any::Any + Send + Sync>>,
        value_schema: Option<Arc<dyn Schema>>,
        value: Option<Arc<dyn std::any::Any + Send + Sync>>,
        timestamp: Option<i64>,
        headers: Option<Box<dyn Headers>>,
    ) -> T;
}
