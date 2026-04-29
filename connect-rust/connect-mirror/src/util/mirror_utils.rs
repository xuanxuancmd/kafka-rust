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

//! MirrorUtils - Internal utility methods for MirrorMaker.
//!
//! This module provides utility functions for wrapping and unwrapping
//! Kafka TopicPartition and offset information into and out of Map objects
//! used by Kafka Connect.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorUtils

use common_trait::TopicPartition;
use kafka_clients_mock::MockKafkaConsumer;
use std::collections::HashMap;

// ============================================================================
// Constants - 1:1 correspondence with Java MirrorUtils
// ============================================================================

/// Key for source cluster alias in wrapped partition map.
/// Corresponds to Java: MirrorUtils.SOURCE_CLUSTER_KEY = "cluster"
pub const SOURCE_CLUSTER_KEY: &str = "cluster";

/// Key for topic name in wrapped partition map.
/// Corresponds to Java: MirrorUtils.TOPIC_KEY = "topic"
pub const TOPIC_KEY: &str = "topic";

/// Key for partition number in wrapped partition map.
/// Corresponds to Java: MirrorUtils.PARTITION_KEY = "partition"
pub const PARTITION_KEY: &str = "partition";

/// Key for offset in wrapped offset map.
/// Corresponds to Java: MirrorUtils.OFFSET_KEY = "offset"
pub const OFFSET_KEY: &str = "offset";

// ============================================================================
// MirrorUtils - Utility Methods
// ============================================================================

/// Wraps a TopicPartition and source cluster alias into a HashMap.
///
/// This creates a map that can be used as the sourcePartition in a Kafka Connect SourceRecord.
/// The map contains:
/// - TOPIC_KEY: the topic name
/// - PARTITION_KEY: the partition number
/// - SOURCE_CLUSTER_KEY: the source cluster alias
///
/// Corresponds to Java: MirrorUtils.wrapPartition(TopicPartition, String)
///
/// # Arguments
/// * `topic_partition` - The TopicPartition to wrap
/// * `source_cluster_alias` - The source cluster alias
///
/// # Returns
/// A HashMap containing the wrapped partition information
pub fn wrap_partition(
    topic_partition: &TopicPartition,
    source_cluster_alias: &str,
) -> HashMap<String, ValueWrapper> {
    let mut wrapped = HashMap::new();
    wrapped.insert(
        TOPIC_KEY.to_string(),
        ValueWrapper::String(topic_partition.topic().to_string()),
    );
    wrapped.insert(
        PARTITION_KEY.to_string(),
        ValueWrapper::Integer(topic_partition.partition()),
    );
    wrapped.insert(
        SOURCE_CLUSTER_KEY.to_string(),
        ValueWrapper::String(source_cluster_alias.to_string()),
    );
    wrapped
}

/// Unwraps a HashMap back into a TopicPartition.
///
/// Extracts the topic name and partition number from the wrapped map
/// using TOPIC_KEY and PARTITION_KEY.
///
/// Corresponds to Java: MirrorUtils.unwrapPartition(Map<String, ?>)
///
/// # Arguments
/// * `wrapped` - The wrapped partition map
///
/// # Returns
/// A TopicPartition containing the topic and partition, or None if the map is invalid
pub fn unwrap_partition(wrapped: &HashMap<String, ValueWrapper>) -> Option<TopicPartition> {
    let topic = match wrapped.get(TOPIC_KEY) {
        Some(ValueWrapper::String(s)) => s.clone(),
        _ => return None,
    };

    let partition = match wrapped.get(PARTITION_KEY) {
        Some(ValueWrapper::Integer(i)) => *i,
        Some(ValueWrapper::Long(l)) => *l as i32,
        _ => return None,
    };

    Some(TopicPartition::new(&topic, partition))
}

/// Wraps a long offset into a HashMap.
///
/// This creates a map that can be used as the sourceOffset in a Kafka Connect SourceRecord.
/// The map contains OFFSET_KEY with the offset value.
///
/// Corresponds to Java: MirrorUtils.wrapOffset(long)
///
/// # Arguments
/// * `offset` - The offset value to wrap
///
/// # Returns
/// A HashMap containing the wrapped offset information
pub fn wrap_offset(offset: i64) -> HashMap<String, ValueWrapper> {
    let mut wrapped = HashMap::new();
    wrapped.insert(OFFSET_KEY.to_string(), ValueWrapper::Long(offset));
    wrapped
}

/// Unwraps a HashMap back into a long offset.
///
/// Extracts the offset value from the wrapped map using OFFSET_KEY.
/// Returns -1 if the map is null or the offset value is null.
///
/// Corresponds to Java: MirrorUtils.unwrapOffset(Map<String, ?>)
///
/// # Arguments
/// * `wrapped` - The wrapped offset map (may be None)
///
/// # Returns
/// The offset value, or -1 if not found
pub fn unwrap_offset(wrapped: Option<&HashMap<String, ValueWrapper>>) -> i64 {
    match wrapped {
        None => -1,
        Some(map) => match map.get(OFFSET_KEY) {
            None => -1,
            Some(ValueWrapper::Long(l)) => *l,
            Some(ValueWrapper::Integer(i)) => *i as i64,
            Some(ValueWrapper::String(s)) => s.parse::<i64>().unwrap_or(-1),
        },
    }
}

/// Creates a new MockKafkaConsumer with byte array deserializers.
///
/// This is a simplified version for testing purposes.
/// In the Java version, this creates a real KafkaConsumer with ByteArrayDeserializer.
/// In our Rust implementation, we return a MockKafkaConsumer configured appropriately.
///
/// Corresponds to Java: MirrorUtils.newConsumer(Map<String, Object>)
///
/// # Arguments
/// * `props` - Consumer configuration properties
///
/// # Returns
/// A new MockKafkaConsumer instance
pub fn new_consumer(props: HashMap<String, String>) -> MockKafkaConsumer {
    // Create MockKafkaConsumer with configuration
    // In the real implementation, this would use ByteArrayDeserializer
    // For mock, we just create a consumer that can be configured
    let consumer = MockKafkaConsumer::new();

    // If bootstrap.servers is provided, we could use it for configuration
    // For mock, we don't actually connect to any server
    if let Some(_bootstrap_servers) = props.get("bootstrap.servers") {
        // In a real implementation, this would configure the consumer
        // For mock, we just acknowledge the configuration exists
    }

    // Apply group.id if present
    if let Some(_group_id) = props.get("group.id") {
        // In a real implementation, this would set the consumer group
        // For mock, we acknowledge the configuration
    }

    consumer
}

/// Creates a new MockKafkaConsumer with byte array serializers.
///
/// This is a producer creation method - in Rust we return a MockKafkaProducer.
/// In the Java version, this creates a real KafkaProducer with ByteArraySerializer.
///
/// Note: This method is included for completeness but uses MockProducer for testing.
/// Corresponds to Java: MirrorUtils.newProducer(Map<String, Object>)
///
/// # Arguments
/// * `props` - Producer configuration properties
///
/// # Returns
/// A new MockKafkaProducer instance (if available in kafka-clients-mock)
pub fn new_producer(props: HashMap<String, String>) -> kafka_clients_mock::MockKafkaProducer {
    // Create MockKafkaProducer with configuration
    let producer = kafka_clients_mock::MockKafkaProducer::new();

    // For mock, we just acknowledge configuration
    if let Some(_bootstrap_servers) = props.get("bootstrap.servers") {
        // Configuration acknowledged
    }

    producer
}

/// Encodes a TopicPartition into a string representation.
///
/// Returns the standard string representation of TopicPartition:
/// "topic-partition"
///
/// Corresponds to Java: MirrorUtils.encodeTopicPartition(TopicPartition)
///
/// # Arguments
/// * `topic_partition` - The TopicPartition to encode
///
/// # Returns
/// A string representation of the TopicPartition
pub fn encode_topic_partition(topic_partition: &TopicPartition) -> String {
    format!(
        "{}-{}",
        topic_partition.topic(),
        topic_partition.partition()
    )
}

/// Decodes a string representation back into a TopicPartition.
///
/// Parses the "topic-partition" format back into a TopicPartition.
///
/// Corresponds to Java: MirrorUtils.decodeTopicPartition(String)
///
/// # Arguments
/// * `topic_partition_string` - The string to decode
///
/// # Returns
/// A TopicPartition, or None if the string is invalid
pub fn decode_topic_partition(topic_partition_string: &str) -> Option<TopicPartition> {
    // Find the last '-' separator (topic names can contain '-')
    let sep = topic_partition_string.rfind('-');

    match sep {
        None => None,
        Some(idx) => {
            let topic = topic_partition_string[..idx].to_string();
            let partition_str = &topic_partition_string[idx + 1..];
            match partition_str.parse::<i32>() {
                Ok(partition) => Some(TopicPartition::new(&topic, partition)),
                Err(_) => None,
            }
        }
    }
}

/// Value wrapper for partition/offset maps.
///
/// This enum represents the different value types that can appear
/// in the wrapped partition/offset maps.
/// Java uses Object type, we use this enum for type safety.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueWrapper {
    /// String value (topic name, cluster alias)
    String(String),
    /// Integer value (partition number, potentially)
    Integer(i32),
    /// Long value (offset, partition number when serialized)
    Long(i64),
}

impl ValueWrapper {
    /// Returns the value as a string, if it is a String type.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            ValueWrapper::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the value as an integer, converting from Long if necessary.
    pub fn as_integer(&self) -> Option<i32> {
        match self {
            ValueWrapper::Integer(i) => Some(*i),
            ValueWrapper::Long(l) => Some(*l as i32),
            _ => None,
        }
    }

    /// Returns the value as a long, converting from Integer if necessary.
    pub fn as_long(&self) -> Option<i64> {
        match self {
            ValueWrapper::Long(l) => Some(*l),
            ValueWrapper::Integer(i) => Some(*i as i64),
            _ => None,
        }
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_partition() {
        let tp = TopicPartition::new("test-topic", 5);
        let wrapped = wrap_partition(&tp, "backup");

        assert_eq!(
            wrapped.get(TOPIC_KEY).unwrap().as_string().unwrap(),
            "test-topic"
        );
        assert_eq!(wrapped.get(PARTITION_KEY).unwrap().as_integer().unwrap(), 5);
        assert_eq!(
            wrapped
                .get(SOURCE_CLUSTER_KEY)
                .unwrap()
                .as_string()
                .unwrap(),
            "backup"
        );
    }

    #[test]
    fn test_unwrap_partition() {
        let mut wrapped = HashMap::new();
        wrapped.insert(
            TOPIC_KEY.to_string(),
            ValueWrapper::String("test-topic".to_string()),
        );
        wrapped.insert(PARTITION_KEY.to_string(), ValueWrapper::Integer(5));
        wrapped.insert(
            SOURCE_CLUSTER_KEY.to_string(),
            ValueWrapper::String("backup".to_string()),
        );

        let tp = unwrap_partition(&wrapped).unwrap();
        assert_eq!(tp.topic(), "test-topic");
        assert_eq!(tp.partition(), 5);
    }

    #[test]
    fn test_unwrap_partition_with_long_partition() {
        // Java may serialize partition as Long in some cases
        let mut wrapped = HashMap::new();
        wrapped.insert(
            TOPIC_KEY.to_string(),
            ValueWrapper::String("test-topic".to_string()),
        );
        wrapped.insert(PARTITION_KEY.to_string(), ValueWrapper::Long(5));

        let tp = unwrap_partition(&wrapped).unwrap();
        assert_eq!(tp.partition(), 5);
    }

    #[test]
    fn test_unwrap_partition_missing_topic() {
        let mut wrapped = HashMap::new();
        wrapped.insert(PARTITION_KEY.to_string(), ValueWrapper::Integer(5));

        assert!(unwrap_partition(&wrapped).is_none());
    }

    #[test]
    fn test_unwrap_partition_missing_partition() {
        let mut wrapped = HashMap::new();
        wrapped.insert(
            TOPIC_KEY.to_string(),
            ValueWrapper::String("test-topic".to_string()),
        );

        assert!(unwrap_partition(&wrapped).is_none());
    }

    #[test]
    fn test_wrap_offset() {
        let wrapped = wrap_offset(12345);
        assert_eq!(wrapped.get(OFFSET_KEY).unwrap().as_long().unwrap(), 12345);
    }

    #[test]
    fn test_unwrap_offset_valid() {
        let wrapped = wrap_offset(12345);
        assert_eq!(unwrap_offset(Some(&wrapped)), 12345);
    }

    #[test]
    fn test_unwrap_offset_none() {
        assert_eq!(unwrap_offset(None), -1);
    }

    #[test]
    fn test_unwrap_offset_missing_key() {
        let wrapped = HashMap::new();
        assert_eq!(unwrap_offset(Some(&wrapped)), -1);
    }

    #[test]
    fn test_unwrap_offset_integer_value() {
        let mut wrapped = HashMap::new();
        wrapped.insert(OFFSET_KEY.to_string(), ValueWrapper::Integer(100));
        assert_eq!(unwrap_offset(Some(&wrapped)), 100);
    }

    #[test]
    fn test_encode_topic_partition() {
        let tp = TopicPartition::new("my-topic", 3);
        let encoded = encode_topic_partition(&tp);
        assert_eq!(encoded, "my-topic-3");
    }

    #[test]
    fn test_decode_topic_partition() {
        let tp = decode_topic_partition("my-topic-3").unwrap();
        assert_eq!(tp.topic(), "my-topic");
        assert_eq!(tp.partition(), 3);
    }

    #[test]
    fn test_decode_topic_partition_with_dash_in_topic() {
        // Topics can contain dashes, so we use last dash as separator
        let tp = decode_topic_partition("my-complex-topic-5").unwrap();
        assert_eq!(tp.topic(), "my-complex-topic");
        assert_eq!(tp.partition(), 5);
    }

    #[test]
    fn test_decode_topic_partition_invalid() {
        assert!(decode_topic_partition("invalidformat").is_none());
        assert!(decode_topic_partition("topic-notanumber").is_none());
    }

    #[test]
    fn test_new_consumer() {
        let mut props = HashMap::new();
        props.insert(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        props.insert("group.id".to_string(), "test-group".to_string());

        let consumer = new_consumer(props);
        // Mock consumer should be created successfully
        assert!(!consumer.is_subscribed());
    }

    #[test]
    fn test_new_producer() {
        let mut props = HashMap::new();
        props.insert(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );

        let producer = new_producer(props);
        // Mock producer should be created successfully
        assert_eq!(producer.sent_records().len(), 0);
    }

    #[test]
    fn test_wrap_unwrap_roundtrip() {
        let original_tp = TopicPartition::new("roundtrip-topic", 10);
        let wrapped = wrap_partition(&original_tp, "source-cluster");
        let unwrapped = unwrap_partition(&wrapped).unwrap();

        assert_eq!(original_tp.topic(), unwrapped.topic());
        assert_eq!(original_tp.partition(), unwrapped.partition());
    }

    #[test]
    fn test_offset_roundtrip() {
        let original_offset: i64 = 9999;
        let wrapped = wrap_offset(original_offset);
        let unwrapped = unwrap_offset(Some(&wrapped));

        assert_eq!(original_offset, unwrapped);
    }

    #[test]
    fn test_value_wrapper_as_string() {
        let v = ValueWrapper::String("test".to_string());
        assert_eq!(v.as_string().unwrap(), "test");
        assert!(v.as_integer().is_none());
        assert!(v.as_long().is_none());
    }

    #[test]
    fn test_value_wrapper_as_integer() {
        let v = ValueWrapper::Integer(42);
        assert!(v.as_string().is_none());
        assert_eq!(v.as_integer().unwrap(), 42);
        assert_eq!(v.as_long().unwrap(), 42);
    }

    #[test]
    fn test_value_wrapper_as_long() {
        let v = ValueWrapper::Long(123456789);
        assert!(v.as_string().is_none());
        assert_eq!(v.as_integer().unwrap(), 123456789 as i32);
        assert_eq!(v.as_long().unwrap(), 123456789);
    }
}
