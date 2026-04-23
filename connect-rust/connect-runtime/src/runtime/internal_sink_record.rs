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

//! A specialization of SinkRecord that allows WorkerSinkTask to track the original ConsumerRecord.
//!
//! It is used internally and not exposed to connectors.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.InternalSinkRecord` in Java.

use common_trait::record::TimestampType;
use connect_api::connector::ConnectRecord;
use connect_api::header::ConnectHeaders;
use connect_api::sink::SinkRecord;
use serde_json::Value;

/// Processing context for tracking the original consumer record.
///
/// This context holds information about the original consumer record
/// that was consumed before transformation.
#[derive(Debug, Clone)]
pub struct ProcessingContext {
    /// Original topic name.
    original_topic: String,
    /// Original partition.
    original_partition: i32,
    /// Original offset.
    original_offset: i64,
}

impl ProcessingContext {
    /// Creates a new ProcessingContext.
    pub fn new(original_topic: String, original_partition: i32, original_offset: i64) -> Self {
        ProcessingContext {
            original_topic,
            original_partition,
            original_offset,
        }
    }

    /// Get the original topic name.
    pub fn original_topic(&self) -> &str {
        &self.original_topic
    }

    /// Get the original partition.
    pub fn original_partition(&self) -> i32 {
        self.original_partition
    }

    /// Get the original offset.
    pub fn original_offset(&self) -> i64 {
        self.original_offset
    }
}

/// A specialization of SinkRecord that allows a WorkerSinkTask to track the
/// original ConsumerRecord for each SinkRecord.
///
/// This is used internally and not exposed to connectors. It provides additional
/// tracking of the original consumer record's topic, partition, and offset for
/// offset commit management.
///
/// Corresponds to `org.apache.kafka.connect.runtime.InternalSinkRecord` in Java.
#[derive(Debug, Clone)]
pub struct InternalSinkRecord {
    /// The underlying sink record.
    record: SinkRecord,
    /// Processing context tracking the original consumer record.
    context: ProcessingContext,
}

impl InternalSinkRecord {
    /// Creates a new InternalSinkRecord from a processing context and sink record.
    ///
    /// # Arguments
    /// * `context` - Processing context tracking the original consumer record
    /// * `record` - The sink record to wrap
    ///
    /// Corresponds to Java: `public InternalSinkRecord(ProcessingContext<ConsumerRecord<byte[], byte[]>> context, SinkRecord record)`
    pub fn new(context: ProcessingContext, record: SinkRecord) -> Self {
        InternalSinkRecord {
            record: SinkRecord::new_with_original(
                record.topic(),
                record.kafka_partition(),
                record.key().cloned(),
                record.value().clone(),
                record.kafka_offset(),
                record.timestamp(),
                record.timestamp_type(),
                record.headers().clone(),
                context.original_topic(),
                context.original_partition(),
                context.original_offset(),
            ),
            context,
        }
    }

    /// Creates a new InternalSinkRecord with all fields specified.
    ///
    /// This is used by the newRecord method to create modified copies.
    ///
    /// # Arguments
    /// * `context` - Processing context
    /// * `topic` - Topic name
    /// * `partition` - Kafka partition
    /// * `key_schema` - Key schema (optional)
    /// * `key` - Key value (optional)
    /// * `value_schema` - Value schema (optional)
    /// * `value` - Value
    /// * `kafka_offset` - Kafka offset
    /// * `timestamp` - Timestamp (optional)
    /// * `timestamp_type` - Timestamp type
    /// * `headers` - Headers
    ///
    /// Corresponds to Java: `protected InternalSinkRecord(ProcessingContext context, String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset, Long timestamp, TimestampType timestampType, Iterable<Header> headers)`
    pub fn with_values(
        context: ProcessingContext,
        topic: String,
        partition: Option<i32>,
        key: Option<Value>,
        value: Value,
        kafka_offset: i64,
        timestamp: Option<i64>,
        timestamp_type: TimestampType,
        headers: ConnectHeaders,
    ) -> Self {
        let record = SinkRecord::new_with_original(
            topic,
            partition,
            key,
            value,
            kafka_offset,
            timestamp,
            timestamp_type,
            headers,
            context.original_topic(),
            context.original_partition(),
            context.original_offset(),
        );

        InternalSinkRecord { record, context }
    }

    /// Create a new record with modified fields.
    ///
    /// # Arguments
    /// * `topic` - New topic name
    /// * `kafka_partition` - New partition (optional)
    /// * `key` - New key (optional)
    /// * `value` - New value
    /// * `timestamp` - New timestamp (optional)
    /// * `headers` - New headers
    ///
    /// # Returns
    /// A new InternalSinkRecord with the modified fields but same context.
    ///
    /// Corresponds to Java: `public SinkRecord newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp, Iterable<Header> headers)`
    pub fn new_record(
        &self,
        topic: String,
        kafka_partition: Option<i32>,
        key: Option<Value>,
        value: Value,
        timestamp: Option<i64>,
        headers: ConnectHeaders,
    ) -> Self {
        InternalSinkRecord::with_values(
            self.context.clone(),
            topic,
            kafka_partition,
            key,
            value,
            self.record.kafka_offset(),
            timestamp,
            self.record.timestamp_type(),
            headers,
        )
    }

    /// Return the context used to process this record.
    ///
    /// # Returns
    /// The processing context; never null
    ///
    /// Corresponds to Java: `public ProcessingContext<ConsumerRecord<byte[], byte[]>> context()`
    pub fn context(&self) -> &ProcessingContext {
        &self.context
    }

    /// Get the underlying sink record.
    pub fn as_sink_record(&self) -> &SinkRecord {
        &self.record
    }

    /// Get the topic name.
    pub fn topic(&self) -> &str {
        self.record.topic()
    }

    /// Get the Kafka partition.
    pub fn kafka_partition(&self) -> Option<i32> {
        self.record.kafka_partition()
    }

    /// Get the Kafka offset.
    pub fn kafka_offset(&self) -> i64 {
        self.record.kafka_offset()
    }

    /// Get the timestamp type.
    pub fn timestamp_type(&self) -> TimestampType {
        self.record.timestamp_type()
    }

    /// Get the original topic.
    pub fn original_topic(&self) -> &str {
        self.record.original_topic().unwrap_or(self.record.topic())
    }

    /// Get the original Kafka partition.
    pub fn original_kafka_partition(&self) -> Option<i32> {
        self.record.original_kafka_partition()
    }

    /// Get the original Kafka offset.
    pub fn original_kafka_offset(&self) -> Option<i64> {
        self.record.original_kafka_offset()
    }
}

impl PartialEq for InternalSinkRecord {
    fn eq(&self, other: &Self) -> bool {
        self.record.topic() == other.record.topic()
            && self.record.kafka_partition() == other.record.kafka_partition()
            && self.record.kafka_offset() == other.record.kafka_offset()
            && self.context.original_topic() == other.context.original_topic()
            && self.context.original_partition() == other.context.original_partition()
            && self.context.original_offset() == other.context.original_offset()
    }
}

impl Eq for InternalSinkRecord {}

impl std::hash::Hash for InternalSinkRecord {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.record.topic().hash(state);
        self.record.kafka_partition().hash(state);
        self.record.kafka_offset().hash(state);
        self.context.original_topic.hash(state);
        self.context.original_partition.hash(state);
        self.context.original_offset.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_internal_sink_record() {
        let context = ProcessingContext::new("original-topic".to_string(), 0, 100);
        let sink_record = SinkRecord::new(
            "target-topic",
            Some(1),
            None,
            Value::String("test".to_string()),
            200,
            TimestampType::CreateTime,
        );

        let internal_record = InternalSinkRecord::new(context, sink_record);

        assert_eq!(internal_record.topic(), "target-topic");
        assert_eq!(internal_record.kafka_partition(), Some(1));
        assert_eq!(internal_record.kafka_offset(), 200);
        assert_eq!(internal_record.context().original_topic(), "original-topic");
        assert_eq!(internal_record.context().original_partition(), 0);
        assert_eq!(internal_record.context().original_offset(), 100);
    }

    #[test]
    fn test_new_record() {
        let context = ProcessingContext::new("original-topic".to_string(), 0, 100);
        let sink_record = SinkRecord::new(
            "target-topic",
            Some(1),
            None,
            Value::String("test".to_string()),
            200,
            TimestampType::CreateTime,
        );

        let internal_record = InternalSinkRecord::new(context, sink_record);

        // Create a new record with different topic
        let new_record = internal_record.new_record(
            "new-topic".to_string(),
            Some(2),
            None,
            Value::String("new-value".to_string()),
            None,
            ConnectHeaders::new(),
        );

        // New record should have new topic but same context
        assert_eq!(new_record.topic(), "new-topic");
        assert_eq!(new_record.kafka_partition(), Some(2));
        assert_eq!(new_record.context().original_topic(), "original-topic");
        assert_eq!(new_record.kafka_offset(), 200); // Offset preserved from original
    }

    #[test]
    fn test_context() {
        let context = ProcessingContext::new("topic".to_string(), 5, 1234);
        let sink_record = SinkRecord::new(
            "topic",
            None,
            None,
            Value::Null,
            0,
            TimestampType::CreateTime,
        );
        let internal_record = InternalSinkRecord::new(context.clone(), sink_record);

        let ctx = internal_record.context();
        assert_eq!(ctx.original_topic(), "topic");
        assert_eq!(ctx.original_partition(), 5);
        assert_eq!(ctx.original_offset(), 1234);
    }

    #[test]
    fn test_equality() {
        let context = ProcessingContext::new("original".to_string(), 0, 100);
        let sink_record1 = SinkRecord::new(
            "topic",
            Some(0),
            None,
            Value::String("value".to_string()),
            50,
            TimestampType::CreateTime,
        );
        let sink_record2 = SinkRecord::new(
            "topic",
            Some(0),
            None,
            Value::String("value".to_string()),
            50,
            TimestampType::CreateTime,
        );

        let internal1 = InternalSinkRecord::new(context.clone(), sink_record1);
        let internal2 = InternalSinkRecord::new(context.clone(), sink_record2);

        assert_eq!(internal1, internal2);
    }

    #[test]
    fn test_hash() {
        let context = ProcessingContext::new("original".to_string(), 0, 100);
        let sink_record = SinkRecord::new(
            "topic",
            Some(0),
            None,
            Value::String("value".to_string()),
            50,
            TimestampType::CreateTime,
        );

        let internal = InternalSinkRecord::new(context, sink_record);

        // Should be able to use in HashSet
        let mut set = std::collections::HashSet::new();
        set.insert(internal.clone());
        assert!(set.contains(&internal));
    }
}
