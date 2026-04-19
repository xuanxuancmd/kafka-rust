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

use common_trait::TopicPartition;

/// RecordMetadata represents metadata for a produced record.
///
/// This corresponds to `org.apache.kafka.clients.producer.RecordMetadata` in Java.
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    topic_partition: TopicPartition,
    base_offset: i64,
    relative_offset: i64,
    timestamp: i64,
    checksum: Option<i64>,
    serialized_key_size: i32,
    serialized_value_size: i32,
}

impl RecordMetadata {
    /// Creates a new RecordMetadata.
    pub fn new(
        topic_partition: TopicPartition,
        base_offset: i64,
        relative_offset: i64,
        timestamp: i64,
        serialized_key_size: i32,
        serialized_value_size: i32,
    ) -> Self {
        RecordMetadata {
            topic_partition,
            base_offset,
            relative_offset,
            timestamp,
            checksum: None,
            serialized_key_size,
            serialized_value_size,
        }
    }

    /// Returns the topic.
    pub fn topic(&self) -> &str {
        self.topic_partition.topic()
    }

    /// Returns the partition.
    pub fn partition(&self) -> i32 {
        self.topic_partition.partition()
    }

    /// Returns the offset.
    pub fn offset(&self) -> i64 {
        self.base_offset + self.relative_offset
    }

    /// Returns the timestamp.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns the checksum.
    pub fn checksum(&self) -> Option<i64> {
        self.checksum
    }

    /// Returns the serialized key size.
    pub fn serialized_key_size(&self) -> i32 {
        self.serialized_key_size
    }

    /// Returns the serialized value size.
    pub fn serialized_value_size(&self) -> i32 {
        self.serialized_value_size
    }

    /// Returns the topic partition.
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let tp = TopicPartition::new("test-topic", 0);
        let rm = RecordMetadata::new(tp, 100, 5, 1234567890, 10, 20);
        assert_eq!(rm.topic(), "test-topic");
        assert_eq!(rm.partition(), 0);
        assert_eq!(rm.offset(), 105);
        assert_eq!(rm.timestamp(), 1234567890);
        assert_eq!(rm.serialized_key_size(), 10);
        assert_eq!(rm.serialized_value_size(), 20);
    }
}
