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

//! Consumer record types for Kafka mock client.
//!
//! This corresponds to `org.apache.kafka.clients.consumer.ConsumerRecord` and
//! `org.apache.kafka.clients.consumer.ConsumerRecords` in Java.

use common_trait::TopicPartition;
use std::collections::{HashMap, HashSet};

/// A single consumer record.
///
/// This corresponds to `org.apache.kafka.clients.consumer.ConsumerRecord` in Java.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    topic: String,
    partition: i32,
    offset: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    timestamp: i64,
    headers: HashMap<String, Vec<u8>>,
}

impl ConsumerRecord {
    /// Creates a new consumer record.
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        timestamp: i64,
    ) -> Self {
        ConsumerRecord {
            topic: topic.into(),
            partition,
            offset,
            key,
            value,
            timestamp,
            headers: HashMap::new(),
        }
    }

    /// Creates a record with headers.
    pub fn with_headers(
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        timestamp: i64,
        headers: HashMap<String, Vec<u8>>,
    ) -> Self {
        ConsumerRecord {
            topic: topic.into(),
            partition,
            offset,
            key,
            value,
            timestamp,
            headers,
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the key.
    pub fn key(&self) -> Option<&Vec<u8>> {
        self.key.as_ref()
    }

    /// Returns the value.
    pub fn value(&self) -> Option<&Vec<u8>> {
        self.value.as_ref()
    }

    /// Returns the timestamp.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns the headers.
    pub fn headers(&self) -> &HashMap<String, Vec<u8>> {
        &self.headers
    }

    /// Returns the topic partition.
    pub fn topic_partition(&self) -> TopicPartition {
        TopicPartition::new(&self.topic, self.partition)
    }
}

/// A collection of consumer records.
///
/// This corresponds to `org.apache.kafka.clients.consumer.ConsumerRecords` in Java.
#[derive(Debug, Clone)]
pub struct ConsumerRecords {
    records: HashMap<TopicPartition, Vec<ConsumerRecord>>,
}

impl ConsumerRecords {
    /// Creates an empty ConsumerRecords.
    pub fn empty() -> Self {
        ConsumerRecords {
            records: HashMap::new(),
        }
    }

    /// Creates a ConsumerRecords from a map of records.
    pub fn new(records: HashMap<TopicPartition, Vec<ConsumerRecord>>) -> Self {
        ConsumerRecords { records }
    }

    /// Returns whether there are no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty() || self.records.values().all(|v| v.is_empty())
    }

    /// Returns the total count of records.
    pub fn count(&self) -> usize {
        self.records.values().map(|v| v.len()).sum()
    }

    /// Returns records for a specific partition.
    pub fn records(&self, partition: &TopicPartition) -> Option<&Vec<ConsumerRecord>> {
        self.records.get(partition)
    }

    /// Returns all partitions that have records.
    pub fn partitions(&self) -> HashSet<TopicPartition> {
        self.records
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Returns an iterator over all records.
    pub fn iter(&self) -> impl Iterator<Item = &ConsumerRecord> {
        self.records.values().flatten()
    }
}

impl Default for ConsumerRecords {
    fn default() -> Self {
        ConsumerRecords::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_record_new() {
        let record = ConsumerRecord::new(
            "test-topic",
            0,
            100,
            Some(vec![1, 2]),
            Some(vec![3, 4]),
            1234567890,
        );
        assert_eq!(record.topic(), "test-topic");
        assert_eq!(record.partition(), 0);
        assert_eq!(record.offset(), 100);
        assert_eq!(record.key(), Some(&vec![1, 2]));
        assert_eq!(record.value(), Some(&vec![3, 4]));
        assert_eq!(record.timestamp(), 1234567890);
        assert_eq!(record.headers().len(), 0);
    }

    #[test]
    fn test_consumer_record_with_headers() {
        let headers = HashMap::from([
            ("header1".to_string(), vec![1]),
            ("header2".to_string(), vec![2]),
        ]);
        let record = ConsumerRecord::with_headers(
            "test-topic",
            0,
            100,
            Some(vec![1]),
            Some(vec![2]),
            12345,
            headers,
        );
        assert_eq!(record.headers().len(), 2);
        assert_eq!(record.headers().get("header1"), Some(&vec![1]));
    }

    #[test]
    fn test_consumer_record_topic_partition() {
        let record = ConsumerRecord::new("test-topic", 5, 100, None, Some(vec![1]), 12345);
        let tp = record.topic_partition();
        assert_eq!(tp.topic(), "test-topic");
        assert_eq!(tp.partition(), 5);
    }

    #[test]
    fn test_consumer_records_empty() {
        let records = ConsumerRecords::empty();
        assert!(records.is_empty());
        assert_eq!(records.count(), 0);
        assert!(records.partitions().is_empty());
    }

    #[test]
    fn test_consumer_records_default() {
        let records = ConsumerRecords::default();
        assert!(records.is_empty());
    }

    #[test]
    fn test_consumer_records_with_data() {
        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        let r2 = ConsumerRecord::new("topic1", 0, 1, None, Some(vec![2]), 1001);
        let r3 = ConsumerRecord::new("topic1", 1, 0, None, Some(vec![3]), 1002);

        let tp0 = TopicPartition::new("topic1", 0);
        let tp1 = TopicPartition::new("topic1", 1);

        let records = ConsumerRecords::new(HashMap::from([
            (tp0.clone(), vec![r1, r2]),
            (tp1.clone(), vec![r3]),
        ]));

        assert!(!records.is_empty());
        assert_eq!(records.count(), 3);
        assert_eq!(records.partitions().len(), 2);
        assert_eq!(records.records(&tp0).unwrap().len(), 2);
        assert_eq!(records.records(&tp1).unwrap().len(), 1);
    }

    #[test]
    fn test_consumer_records_iter() {
        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);
        let r2 = ConsumerRecord::new("topic1", 1, 0, None, Some(vec![2]), 1001);

        let tp0 = TopicPartition::new("topic1", 0);
        let tp1 = TopicPartition::new("topic1", 1);

        let records = ConsumerRecords::new(HashMap::from([(tp0, vec![r1]), (tp1, vec![r2])]));

        let count = records.iter().count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_consumer_records_empty_partition() {
        let tp0 = TopicPartition::new("topic1", 0);
        let tp1 = TopicPartition::new("topic1", 1);

        let r1 = ConsumerRecord::new("topic1", 0, 0, None, Some(vec![1]), 1000);

        let records = ConsumerRecords::new(HashMap::from([
            (tp0.clone(), vec![r1]),
            (tp1.clone(), vec![]),
        ]));

        assert!(!records.is_empty());
        assert_eq!(records.count(), 1);
        assert_eq!(records.partitions().len(), 1); // Only tp0 has non-empty records
    }
}
