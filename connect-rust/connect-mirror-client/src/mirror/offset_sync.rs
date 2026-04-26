/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! OffsetSync records for MirrorMaker 2 offset synchronization.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.OffsetSync

use std::fmt;
use std::io::Cursor;

use common_trait::protocol::{Field, Schema, SchemaError, Struct, INT32, INT64, STRING};
use common_trait::TopicPartition;

// Schema constants for OffsetSync
pub const TOPIC_KEY: &str = "topic";
pub const PARTITION_KEY: &str = "partition";
pub const UPSTREAM_OFFSET_KEY: &str = "upstreamOffset";
pub const DOWNSTREAM_OFFSET_KEY: &str = "offset";

/// OffsetSync records emitted by MirrorSourceTask.
///
/// This encapsulates the mapping between an upstream (source) topic-partition's offset
/// and its corresponding downstream (target) offset. Used by MirrorMaker to synchronize
/// consumer group offsets across clusters.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.OffsetSync
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetSync {
    topic_partition: TopicPartition,
    upstream_offset: i64,
    downstream_offset: i64,
}

impl OffsetSync {
    /// Creates a new OffsetSync.
    ///
    /// # Arguments
    /// * `topic_partition` - The source topic and partition
    /// * `upstream_offset` - The offset in the source cluster
    /// * `downstream_offset` - The corresponding offset in the target cluster
    pub fn new(
        topic_partition: TopicPartition,
        upstream_offset: i64,
        downstream_offset: i64,
    ) -> Self {
        Self {
            topic_partition,
            upstream_offset,
            downstream_offset,
        }
    }

    /// Returns the topic partition.
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }

    /// Returns the upstream (source) offset.
    pub fn upstream_offset(&self) -> i64 {
        self.upstream_offset
    }

    /// Returns the downstream (target) offset.
    pub fn downstream_offset(&self) -> i64 {
        self.downstream_offset
    }

    /// Serialize key to byte array.
    ///
    /// The key schema contains:
    /// - topic: String
    /// - partition: Int32
    ///
    /// Corresponds to Java: OffsetSync.recordKey()
    pub fn record_key(&self) -> Result<Vec<u8>, SchemaError> {
        let key_schema = OffsetSync::key_schema();
        let mut struct_ref = Struct::new(&key_schema);
        struct_ref.set_string(TOPIC_KEY, self.topic_partition.topic())?;
        struct_ref.set_int(PARTITION_KEY, self.topic_partition.partition())?;

        let mut buffer = Vec::new();
        key_schema.write(&mut buffer, &struct_ref)?;
        Ok(buffer)
    }

    /// Serialize value to byte array.
    ///
    /// The value schema contains:
    /// - upstreamOffset: Int64
    /// - offset (downstream): Int64
    ///
    /// Corresponds to Java: OffsetSync.recordValue()
    pub fn record_value(&self) -> Result<Vec<u8>, SchemaError> {
        let value_schema = OffsetSync::value_schema();

        let mut value_struct = Struct::new(&value_schema);
        value_struct.set_long(UPSTREAM_OFFSET_KEY, self.upstream_offset)?;
        value_struct.set_long(DOWNSTREAM_OFFSET_KEY, self.downstream_offset)?;

        let mut buffer = Vec::new();
        value_schema.write(&mut buffer, &value_struct)?;
        Ok(buffer)
    }

    /// Deserialize from byte arrays.
    ///
    /// Corresponds to Java: OffsetSync.deserializeRecord(byte[] key, byte[] value)
    pub fn deserialize_record(key: &[u8], value: &[u8]) -> Result<Self, SchemaError> {
        let key_schema = OffsetSync::key_schema();
        let value_schema = OffsetSync::value_schema();

        // Read key
        let mut key_cursor = Cursor::new(key);
        let key_struct = key_schema.read(&mut key_cursor)?;
        let topic = key_struct.get_string(TOPIC_KEY)?.to_string();
        let partition = key_struct.get_int(PARTITION_KEY)?;

        // Read value
        let mut value_cursor = Cursor::new(value);
        let value_struct = value_schema.read(&mut value_cursor)?;
        let upstream_offset = value_struct.get_long(UPSTREAM_OFFSET_KEY)?;
        let downstream_offset = value_struct.get_long(DOWNSTREAM_OFFSET_KEY)?;

        Ok(Self::new(
            TopicPartition::new(topic, partition),
            upstream_offset,
            downstream_offset,
        ))
    }

    /// Key schema for OffsetSync records.
    fn key_schema() -> Schema {
        Schema::new(vec![
            Field::new(TOPIC_KEY, std::sync::Arc::new(STRING)),
            Field::new(PARTITION_KEY, std::sync::Arc::new(INT32)),
        ])
        .expect("Invalid key schema")
    }

    /// Value schema for OffsetSync records.
    fn value_schema() -> Schema {
        Schema::new(vec![
            Field::new(UPSTREAM_OFFSET_KEY, std::sync::Arc::new(INT64)),
            Field::new(DOWNSTREAM_OFFSET_KEY, std::sync::Arc::new(INT64)),
        ])
        .expect("Invalid value schema")
    }
}

impl fmt::Display for OffsetSync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OffsetSync{{topicPartition={}, upstreamOffset={}, downstreamOffset={}}}",
            self.topic_partition, self.upstream_offset, self.downstream_offset
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_sync_new() {
        let tp = TopicPartition::new("test-topic", 0);
        let sync = OffsetSync::new(tp.clone(), 100, 200);

        assert_eq!(sync.topic_partition(), &tp);
        assert_eq!(sync.upstream_offset(), 100);
        assert_eq!(sync.downstream_offset(), 200);
    }

    #[test]
    fn test_offset_sync_display() {
        let tp = TopicPartition::new("test-topic", 5);
        let sync = OffsetSync::new(tp, 1000, 2000);

        let display = format!("{}", sync);
        assert!(display.contains("test-topic"));
        assert!(display.contains("1000"));
        assert!(display.contains("2000"));
        assert!(display.contains("5"));
    }

    #[test]
    fn test_offset_sync_record_key() {
        let tp = TopicPartition::new("source-topic", 3);
        let sync = OffsetSync::new(tp, 100, 200);

        let key = sync.record_key().unwrap();
        assert!(!key.is_empty());

        // Deserialize and verify
        let key_schema = OffsetSync::key_schema();
        let mut cursor = Cursor::new(key.as_slice());
        let key_struct = key_schema.read(&mut cursor).unwrap();

        assert_eq!(key_struct.get_string(TOPIC_KEY).unwrap(), "source-topic");
        assert_eq!(key_struct.get_int(PARTITION_KEY).unwrap(), 3);
    }

    #[test]
    fn test_offset_sync_record_value() {
        let tp = TopicPartition::new("test-topic", 0);
        let sync = OffsetSync::new(tp, 12345, 67890);

        let value = sync.record_value().unwrap();
        assert!(!value.is_empty());

        // Deserialize and verify
        let value_schema = OffsetSync::value_schema();
        let mut cursor = Cursor::new(value.as_slice());
        let value_struct = value_schema.read(&mut cursor).unwrap();

        assert_eq!(value_struct.get_long(UPSTREAM_OFFSET_KEY).unwrap(), 12345);
        assert_eq!(value_struct.get_long(DOWNSTREAM_OFFSET_KEY).unwrap(), 67890);
    }

    #[test]
    fn test_offset_sync_serialize_deserialize_roundtrip() {
        let tp = TopicPartition::new("roundtrip-topic", 7);
        let original = OffsetSync::new(tp.clone(), 1000, 2000);

        let key = original.record_key().unwrap();
        let value = original.record_value().unwrap();

        let deserialized = OffsetSync::deserialize_record(&key, &value).unwrap();

        assert_eq!(deserialized.topic_partition(), &tp);
        assert_eq!(deserialized.upstream_offset(), 1000);
        assert_eq!(deserialized.downstream_offset(), 2000);
    }

    #[test]
    fn test_offset_sync_multiple_partitions() {
        for partition in 0i32..10 {
            let tp = TopicPartition::new("multi-topic", partition);
            let sync = OffsetSync::new(
                tp.clone(),
                (partition * 100) as i64,
                (partition * 200) as i64,
            );

            let key = sync.record_key().unwrap();
            let value = sync.record_value().unwrap();

            let deserialized = OffsetSync::deserialize_record(&key, &value).unwrap();

            assert_eq!(deserialized.topic_partition().partition(), partition);
            assert_eq!(deserialized.upstream_offset(), (partition * 100) as i64);
            assert_eq!(deserialized.downstream_offset(), (partition * 200) as i64);
        }
    }

    #[test]
    fn test_offset_sync_large_offsets() {
        let tp = TopicPartition::new("large-offset-topic", 0);
        let sync = OffsetSync::new(tp.clone(), i64::MAX, i64::MAX - 1);

        let key = sync.record_key().unwrap();
        let value = sync.record_value().unwrap();

        let deserialized = OffsetSync::deserialize_record(&key, &value).unwrap();

        assert_eq!(deserialized.upstream_offset(), i64::MAX);
        assert_eq!(deserialized.downstream_offset(), i64::MAX - 1);
    }

    #[test]
    fn test_offset_sync_zero_offsets() {
        let tp = TopicPartition::new("zero-offset-topic", 0);
        let sync = OffsetSync::new(tp.clone(), 0, 0);

        let key = sync.record_key().unwrap();
        let value = sync.record_value().unwrap();

        let deserialized = OffsetSync::deserialize_record(&key, &value).unwrap();

        assert_eq!(deserialized.upstream_offset(), 0);
        assert_eq!(deserialized.downstream_offset(), 0);
    }

    #[test]
    fn test_offset_sync_equality() {
        let tp1 = TopicPartition::new("topic", 0);
        let tp2 = TopicPartition::new("topic", 0);
        let tp3 = TopicPartition::new("topic", 1);

        let sync1 = OffsetSync::new(tp1, 100, 200);
        let sync2 = OffsetSync::new(tp2, 100, 200);
        let sync3 = OffsetSync::new(tp3, 100, 200);

        assert_eq!(sync1, sync2);
        assert_ne!(sync1, sync3);
    }

    #[test]
    fn test_offset_sync_clone() {
        let tp = TopicPartition::new("clone-topic", 5);
        let sync = OffsetSync::new(tp, 500, 600);

        let cloned = sync.clone();

        assert_eq!(sync, cloned);
    }

    /// Test: Exact Java testSerde correspondence.
    /// Corresponds to Java: OffsetSyncTest.testSerde()
    /// Uses same parameter values as Java test: ("topic-1", partition 2, upstream 3, downstream 4)
    #[test]
    fn test_serde_java_correspondence() {
        // Java: OffsetSync offsetSync = new OffsetSync(new TopicPartition("topic-1", 2), 3, 4);
        let tp = TopicPartition::new("topic-1", 2);
        let sync = OffsetSync::new(tp.clone(), 3, 4);

        // Java: byte[] key = offsetSync.recordKey();
        let key = sync.record_key().expect("record_key should succeed");

        // Java: byte[] value = offsetSync.recordValue();
        let value = sync.record_value().expect("record_value should succeed");

        // Java: ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("any-topic", 6, 7, key, value);
        // Note: ConsumerRecord's topic/partition/offset are carriers, not part of OffsetSync data
        // Rust deserialize_record directly accepts key and value bytes

        // Java: OffsetSync deserialized = OffsetSync.deserializeRecord(record);
        let deserialized = OffsetSync::deserialize_record(&key, &value)
            .expect("deserialize_record should succeed");

        // Java: assertEquals(offsetSync.topicPartition(), deserialized.topicPartition())
        assert_eq!(
            sync.topic_partition(),
            deserialized.topic_partition(),
            "Failure on offset sync topic partition serde"
        );

        // Java implicitly checks upstream and downstream offsets via constructor
        assert_eq!(
            sync.upstream_offset(),
            deserialized.upstream_offset(),
            "Failure on offset sync upstream offset serde"
        );
        assert_eq!(
            sync.downstream_offset(),
            deserialized.downstream_offset(),
            "Failure on offset sync downstream offset serde"
        );
    }

    /// Test: Negative offset handling (-1 is Kafka's NO_OFFSET sentinel).
    /// Kafka uses -1 to indicate undefined or no offset.
    #[test]
    fn test_offset_sync_negative_offsets() {
        let tp = TopicPartition::new("negative-offset-topic", 0);
        // -1 represents NO_OFFSET in Kafka semantics
        let sync = OffsetSync::new(tp.clone(), -1, -1);

        let key = sync.record_key().expect("record_key should succeed");
        let value = sync.record_value().expect("record_value should succeed");

        let deserialized = OffsetSync::deserialize_record(&key, &value)
            .expect("deserialize_record should succeed");

        assert_eq!(deserialized.upstream_offset(), -1);
        assert_eq!(deserialized.downstream_offset(), -1);
    }

    /// Test: Mixed positive and negative offsets.
    #[test]
    fn test_offset_sync_mixed_positive_negative() {
        let tp = TopicPartition::new("mixed-offset-topic", 10);
        // upstream is valid offset, downstream is -1 (NO_OFFSET)
        let sync = OffsetSync::new(tp.clone(), 1000, -1);

        let key = sync.record_key().expect("record_key should succeed");
        let value = sync.record_value().expect("record_value should succeed");

        let deserialized = OffsetSync::deserialize_record(&key, &value)
            .expect("deserialize_record should succeed");

        assert_eq!(deserialized.upstream_offset(), 1000);
        assert_eq!(deserialized.downstream_offset(), -1);
    }

    /// Test: Key bytes are non-empty and correctly sized.
    /// Key schema: topic (String) + partition (Int32)
    #[test]
    fn test_record_key_structure() {
        let tp = TopicPartition::new("structure-test", 42);
        let sync = OffsetSync::new(tp, 100, 200);

        let key = sync.record_key().expect("record_key should succeed");

        // Key must be non-empty
        assert!(!key.is_empty(), "Key bytes should not be empty");

        // Verify key can be deserialized back
        let key_schema = OffsetSync::key_schema();
        let mut cursor = Cursor::new(key.as_slice());
        let key_struct = key_schema
            .read(&mut cursor)
            .expect("Key schema read should succeed");

        // Verify topic string matches
        assert_eq!(
            key_struct
                .get_string(TOPIC_KEY)
                .expect("topic should exist"),
            "structure-test"
        );
        // Verify partition int matches
        assert_eq!(
            key_struct
                .get_int(PARTITION_KEY)
                .expect("partition should exist"),
            42
        );
    }

    /// Test: Value bytes are non-empty and correctly sized.
    /// Value schema: upstreamOffset (Int64) + downstreamOffset (Int64)
    #[test]
    fn test_record_value_structure() {
        let tp = TopicPartition::new("value-structure-test", 0);
        let sync = OffsetSync::new(tp, 12345, 67890);

        let value = sync.record_value().expect("record_value should succeed");

        // Value must be non-empty
        assert!(!value.is_empty(), "Value bytes should not be empty");

        // Verify value can be deserialized back
        let value_schema = OffsetSync::value_schema();
        let mut cursor = Cursor::new(value.as_slice());
        let value_struct = value_schema
            .read(&mut cursor)
            .expect("Value schema read should succeed");

        // Verify upstream offset matches
        assert_eq!(
            value_struct
                .get_long(UPSTREAM_OFFSET_KEY)
                .expect("upstreamOffset should exist"),
            12345
        );
        // Verify downstream offset matches
        assert_eq!(
            value_struct
                .get_long(DOWNSTREAM_OFFSET_KEY)
                .expect("downstreamOffset should exist"),
            67890
        );
    }
}
