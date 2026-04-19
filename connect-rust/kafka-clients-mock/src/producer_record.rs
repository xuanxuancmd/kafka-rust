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

//! Producer record type for Kafka mock client.
//!
//! This corresponds to `org.apache.kafka.clients.producer.ProducerRecord` in Java.

use std::collections::HashMap;

/// A producer record to be sent to Kafka.
///
/// This corresponds to `org.apache.kafka.clients.producer.ProducerRecord` in Java.
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    topic: String,
    partition: Option<i32>,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    timestamp: Option<i64>,
    headers: HashMap<String, Vec<u8>>,
}

impl ProducerRecord {
    /// Creates a new producer record with topic and value.
    pub fn new(topic: impl Into<String>, value: Option<Vec<u8>>) -> Self {
        ProducerRecord {
            topic: topic.into(),
            partition: None,
            key: None,
            value,
            timestamp: None,
            headers: HashMap::new(),
        }
    }

    /// Creates a producer record with topic, key, and value.
    pub fn with_key_value(
        topic: impl Into<String>,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
    ) -> Self {
        ProducerRecord {
            topic: topic.into(),
            partition: None,
            key,
            value,
            timestamp: None,
            headers: HashMap::new(),
        }
    }

    /// Creates a producer record with explicit partition.
    pub fn with_partition(
        topic: impl Into<String>,
        partition: i32,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
    ) -> Self {
        ProducerRecord {
            topic: topic.into(),
            partition: Some(partition),
            key,
            value,
            timestamp: None,
            headers: HashMap::new(),
        }
    }

    /// Creates a producer record with all fields.
    pub fn with_all(
        topic: impl Into<String>,
        partition: Option<i32>,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        timestamp: Option<i64>,
        headers: HashMap<String, Vec<u8>>,
    ) -> Self {
        ProducerRecord {
            topic: topic.into(),
            partition,
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

    /// Returns the partition (if specified).
    pub fn partition(&self) -> Option<i32> {
        self.partition
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
    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// Returns the headers.
    pub fn headers(&self) -> &HashMap<String, Vec<u8>> {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_record_new() {
        let record = ProducerRecord::new("test-topic", Some(vec![1, 2, 3]));
        assert_eq!(record.topic(), "test-topic");
        assert!(record.partition().is_none());
        assert!(record.key().is_none());
        assert_eq!(record.value(), Some(&vec![1, 2, 3]));
        assert!(record.timestamp().is_none());
        assert_eq!(record.headers().len(), 0);
    }

    #[test]
    fn test_producer_record_with_key_value() {
        let record = ProducerRecord::with_key_value("test-topic", Some(vec![1]), Some(vec![2]));
        assert_eq!(record.key(), Some(&vec![1]));
        assert_eq!(record.value(), Some(&vec![2]));
    }

    #[test]
    fn test_producer_record_with_partition() {
        let record = ProducerRecord::with_partition("test-topic", 5, Some(vec![1]), Some(vec![2]));
        assert_eq!(record.partition(), Some(5));
    }

    #[test]
    fn test_producer_record_with_all() {
        let headers = HashMap::from([("header1".to_string(), vec![1])]);
        let record = ProducerRecord::with_all(
            "test-topic",
            Some(3),
            Some(vec![1]),
            Some(vec![2]),
            Some(1234567890),
            headers,
        );
        assert_eq!(record.topic(), "test-topic");
        assert_eq!(record.partition(), Some(3));
        assert_eq!(record.key(), Some(&vec![1]));
        assert_eq!(record.value(), Some(&vec![2]));
        assert_eq!(record.timestamp(), Some(1234567890));
        assert_eq!(record.headers().len(), 1);
    }

    #[test]
    fn test_producer_record_none_value() {
        let record = ProducerRecord::new("test-topic", None);
        assert!(record.value().is_none());
    }
}
