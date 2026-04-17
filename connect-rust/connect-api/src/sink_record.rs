// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// // License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SinkRecord implementation for Kafka Connect.
//! This module provides the SinkRecord type that implements ConnectRecord trait.

use crate::connector_types::ConnectRecord;
use crate::data::{Schema, SchemaAndValue};
use crate::error::ConnectException;
use std::any::Any;
use std::collections::HashMap;

/// SinkRecord represents a record read from Kafka and destined for a sink system.
#[derive(Debug)]
pub struct SinkRecord {
    /// The topic
    pub topic: String,
    /// The Kafka partition
    pub kafka_partition: i32,
    /// The Kafka offset
    pub kafka_offset: i64,
    /// The record key
    pub key: Option<Box<dyn Any + Send + Sync>>,
    /// The record value
    pub value: Option<Box<dyn Any + Send + Sync>>,
    /// The timestamp
    pub timestamp: Option<i64>,
    /// The Kafka offset
    pub kafka_offset: i64,
    /// The original topic (before transformations)
    pub original_topic: Option<String>,
    /// The original Kafka partition (before transformations)
    pub original_kafka_partition: Option<i32>,
    /// The original Kafka offset (before transformations)
    pub original_kafka_offset: Option<i64>,
}

impl Clone for SinkRecord {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            kafka_partition: self.kafka_partition,
            key: None,   // Cannot clone Box<dyn Any>
            value: None, // Cannot clone Box<dyn Any>
            timestamp: self.timestamp,
            kafka_offset: self.kafka_offset,
            original_topic: self.original_topic.clone(),
            original_kafka_partition: self.original_kafka_partition,
            original_kafka_offset: self.original_kafka_offset,
        }
    }
}

impl ConnectRecord<SinkRecord> for SinkRecord {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn kafka_partition(&self) -> Option<i32> {
        self.kafka_partition
    }

    fn key(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        self.key.as_ref().map(|k| Arc::clone(k))
    }

    fn value(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        self.value.as_ref().map(|v| Arc::clone(v))
    }

    fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    fn kafka_offset(&self) -> i64 {
        self.kafka_offset
    }

    fn headers(&self) -> Arc<dyn crate::data::Headers> {
        self.headers.clone()
    }

    fn new_record(
        &self,
        topic: Option<&str>,
        partition: Option<i32>,
        key_schema: Option<Arc<dyn crate::data::Schema>>,
        key: Option<Arc<dyn std::any::Any + Send + Sync>>,
        value_schema: Option<Arc<dyn crate::data::Schema>>,
        value: Option<Arc<dyn std::any::Any + Send + Sync>>,
        timestamp: Option<i64>,
        headers: Option<Box<dyn crate::data::Headers>>,
    ) -> SinkRecord {
        SinkRecord {
            topic: topic.unwrap_or(&self.topic).map(|s| s.to_string()),
            kafka_partition: partition.or(self.kafka_partition).unwrap_or(0),
            kafka_offset: self.kafka_offset.unwrap_or(0),
            key: key.unwrap_or_else(|| {
                self.key
                    .as_ref()
                    .and_then(|k| k.as_ref().map(|s| s.as_ref()).map(|s| Arc::clone(s)))
            }),
            value: value.unwrap_or_else(|| {
                self.value
                    .as_ref()
                    .and_then(|v| v.as_ref().map(|s| Arc::clone(v)))
            }),
            timestamp: timestamp.or(self.timestamp),
            kafka_offset: self.kafka_offset.unwrap_or(0),
            original_topic: None,
            original_kafka_partition: None,
            original_kafka_offset: None,
            headers: headers.unwrap_or_else(|| Box::new(SimpleHeaders::new())),
        }
    }
}

use common_trait::util::completable_future::CompletableFuture;

/// Component that a SinkTask can use to report problematic records (and their corresponding problems)
/// as it writes them through SinkTask::put.
///
/// This trait provides an asynchronous way to report errors to the dead letter queue (DLQ).
pub trait ErrantRecordReporter {
    /// Report a problematic record and the corresponding error to be written to the sink
    /// connector's dead letter queue (DLQ).
    ///
    /// This call is asynchronous and returns a CompletableFuture. Awaiting on this future
    /// will block until the record has been written or return any error that occurred while
    /// sending the record.
    ///
    /// Connect guarantees that sink records reported through this reporter will be written to
    /// the error topic before the framework calls the pre-commit method and therefore before
    /// committing the consumer offsets.
    ///
    /// # Arguments
    /// * `record` - The problematic record; may not be null
    /// * `error` - The error capturing the problem with the record; may not be null
    ///
    /// # Returns
    /// A future that can be used to block until the record and error are reported to the DLQ
    ///
    /// # Errors
    /// Returns a ConnectException if the error reporter and DLQ fails to write a reported record
    fn report(
        &self,
        record: &SinkRecord,
        error: Box<dyn std::error::Error + Send + Sync>,
    ) -> CompletableFuture<(), Box<dyn std::error::Error + Send + Sync>>;
}
