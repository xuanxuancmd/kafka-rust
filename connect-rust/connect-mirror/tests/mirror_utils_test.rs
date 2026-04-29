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

//! Tests for MirrorUtils utility functions.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorUtils
//!
//! Note: Java MirrorUtilsTest focuses on createCompactedTopic, but Rust
//! implementation handles internal topic creation directly via Admin.create_topics.
//! This test file focuses on wrap/unwrap utility methods which are the core
//! functionality needed across MirrorMaker.
//!
//! Test coverage includes:
//! - wrap_partition / unwrap_partition with various value types
//! - wrap_offset / unwrap_offset with boundary values
//! - encode_topic_partition / decode_topic_partition edge cases
//! - new_consumer / new_producer creation
//! - Roundtrip semantics (wrap then unwrap)
//! - Exception/boundary handling

use std::collections::HashMap;

use common_trait::TopicPartition;
use connect_mirror::util::{
    decode_topic_partition, encode_topic_partition, new_consumer, new_producer, unwrap_offset,
    unwrap_partition, wrap_offset, wrap_partition, ValueWrapper, OFFSET_KEY, PARTITION_KEY,
    SOURCE_CLUSTER_KEY, TOPIC_KEY,
};

// ============================================================================
// wrap_partition Tests
// ============================================================================

/// Test basic wrap_partition functionality.
/// Corresponds to Java: MirrorUtils.wrapPartition(TopicPartition, String)
#[test]
fn test_wrap_partition_basic() {
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

/// Test wrap_partition with empty cluster alias.
#[test]
fn test_wrap_partition_empty_cluster_alias() {
    let tp = TopicPartition::new("topic", 0);
    let wrapped = wrap_partition(&tp, "");

    assert_eq!(
        wrapped.get(TOPIC_KEY).unwrap().as_string().unwrap(),
        "topic"
    );
    assert_eq!(wrapped.get(PARTITION_KEY).unwrap().as_integer().unwrap(), 0);
    assert_eq!(
        wrapped
            .get(SOURCE_CLUSTER_KEY)
            .unwrap()
            .as_string()
            .unwrap(),
        ""
    );
}

/// Test wrap_partition with partition 0 (edge case).
#[test]
fn test_wrap_partition_partition_zero() {
    let tp = TopicPartition::new("my-topic", 0);
    let wrapped = wrap_partition(&tp, "source");

    assert_eq!(wrapped.get(PARTITION_KEY).unwrap().as_integer().unwrap(), 0);
}

/// Test wrap_partition with large partition number.
#[test]
fn test_wrap_partition_large_partition() {
    let tp = TopicPartition::new("large-topic", 10000);
    let wrapped = wrap_partition(&tp, "cluster");

    assert_eq!(
        wrapped.get(PARTITION_KEY).unwrap().as_integer().unwrap(),
        10000
    );
}

/// Test wrap_partition stores partition as Integer (not Long).
/// This ensures type consistency with Java semantics.
#[test]
fn test_wrap_partition_stores_partition_as_integer() {
    let tp = TopicPartition::new("topic", 42);
    let wrapped = wrap_partition(&tp, "alias");

    let partition_value = wrapped.get(PARTITION_KEY).unwrap();
    assert!(partition_value.as_integer().is_some());
    assert_eq!(partition_value.as_integer().unwrap(), 42);
}

// ============================================================================
// unwrap_partition Tests
// ============================================================================

/// Test basic unwrap_partition functionality.
/// Corresponds to Java: MirrorUtils.unwrapPartition(Map<String, ?>)
#[test]
fn test_unwrap_partition_basic() {
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

/// Test unwrap_partition with Long partition value.
/// Java may serialize partition as Long in some cases.
#[test]
fn test_unwrap_partition_with_long_partition() {
    let mut wrapped = HashMap::new();
    wrapped.insert(
        TOPIC_KEY.to_string(),
        ValueWrapper::String("test-topic".to_string()),
    );
    wrapped.insert(PARTITION_KEY.to_string(), ValueWrapper::Long(10));

    let tp = unwrap_partition(&wrapped).unwrap();
    assert_eq!(tp.partition(), 10);
}

/// Test unwrap_partition returns None when TOPIC_KEY missing.
#[test]
fn test_unwrap_partition_missing_topic_key() {
    let mut wrapped = HashMap::new();
    wrapped.insert(PARTITION_KEY.to_string(), ValueWrapper::Integer(5));

    assert!(unwrap_partition(&wrapped).is_none());
}

/// Test unwrap_partition returns None when PARTITION_KEY missing.
#[test]
fn test_unwrap_partition_missing_partition_key() {
    let mut wrapped = HashMap::new();
    wrapped.insert(
        TOPIC_KEY.to_string(),
        ValueWrapper::String("test-topic".to_string()),
    );

    assert!(unwrap_partition(&wrapped).is_none());
}

/// Test unwrap_partition returns None with empty map.
#[test]
fn test_unwrap_partition_empty_map() {
    let wrapped = HashMap::new();
    assert!(unwrap_partition(&wrapped).is_none());
}

/// Test unwrap_partition with wrong value type for topic.
#[test]
fn test_unwrap_partition_wrong_topic_type() {
    let mut wrapped = HashMap::new();
    wrapped.insert(TOPIC_KEY.to_string(), ValueWrapper::Integer(123));
    wrapped.insert(PARTITION_KEY.to_string(), ValueWrapper::Integer(5));

    assert!(unwrap_partition(&wrapped).is_none());
}

/// Test unwrap_partition with wrong value type for partition.
#[test]
fn test_unwrap_partition_wrong_partition_type() {
    let mut wrapped = HashMap::new();
    wrapped.insert(
        TOPIC_KEY.to_string(),
        ValueWrapper::String("topic".to_string()),
    );
    wrapped.insert(
        PARTITION_KEY.to_string(),
        ValueWrapper::String("not-a-number".to_string()),
    );

    assert!(unwrap_partition(&wrapped).is_none());
}

/// Test unwrap_partition ignores SOURCE_CLUSTER_KEY.
/// Source cluster alias is not part of TopicPartition.
#[test]
fn test_unwrap_partition_ignores_source_cluster() {
    let mut wrapped = HashMap::new();
    wrapped.insert(
        TOPIC_KEY.to_string(),
        ValueWrapper::String("topic".to_string()),
    );
    wrapped.insert(PARTITION_KEY.to_string(), ValueWrapper::Integer(0));
    // SOURCE_CLUSTER_KEY present but not needed for unwrapping

    let tp = unwrap_partition(&wrapped).unwrap();
    assert_eq!(tp.topic(), "topic");
    assert_eq!(tp.partition(), 0);
}

// ============================================================================
// wrap_offset Tests
// ============================================================================

/// Test basic wrap_offset functionality.
/// Corresponds to Java: MirrorUtils.wrapOffset(long)
#[test]
fn test_wrap_offset_basic() {
    let wrapped = wrap_offset(12345);
    assert_eq!(wrapped.get(OFFSET_KEY).unwrap().as_long().unwrap(), 12345);
}

/// Test wrap_offset with zero offset.
#[test]
fn test_wrap_offset_zero() {
    let wrapped = wrap_offset(0);
    assert_eq!(wrapped.get(OFFSET_KEY).unwrap().as_long().unwrap(), 0);
}

/// Test wrap_offset with negative offset (NO_OFFSET semantic).
/// Kafka uses -1 to represent undefined/invalid offset.
#[test]
fn test_wrap_offset_negative() {
    let wrapped = wrap_offset(-1);
    assert_eq!(wrapped.get(OFFSET_KEY).unwrap().as_long().unwrap(), -1);
}

/// Test wrap_offset with large offset value.
#[test]
fn test_wrap_offset_large() {
    let wrapped = wrap_offset(i64::MAX);
    assert_eq!(
        wrapped.get(OFFSET_KEY).unwrap().as_long().unwrap(),
        i64::MAX
    );
}

/// Test wrap_offset stores value as Long type.
#[test]
fn test_wrap_offset_stores_as_long() {
    let wrapped = wrap_offset(100);

    let offset_value = wrapped.get(OFFSET_KEY).unwrap();
    assert!(offset_value.as_long().is_some());
    assert_eq!(offset_value.as_long().unwrap(), 100);
}

// ============================================================================
// unwrap_offset Tests
// ============================================================================

/// Test basic unwrap_offset functionality.
/// Corresponds to Java: MirrorUtils.unwrapOffset(Map<String, ?>)
#[test]
fn test_unwrap_offset_basic() {
    let wrapped = wrap_offset(12345);
    assert_eq!(unwrap_offset(Some(&wrapped)), 12345);
}

/// Test unwrap_offset returns -1 when map is None.
/// Java semantics: null map returns -1.
#[test]
fn test_unwrap_offset_none_map() {
    assert_eq!(unwrap_offset(None), -1);
}

/// Test unwrap_offset returns -1 when OFFSET_KEY missing.
#[test]
fn test_unwrap_offset_missing_key() {
    let wrapped = HashMap::new();
    assert_eq!(unwrap_offset(Some(&wrapped)), -1);
}

/// Test unwrap_offset with Integer value (type conversion).
#[test]
fn test_unwrap_offset_integer_value() {
    let mut wrapped = HashMap::new();
    wrapped.insert(OFFSET_KEY.to_string(), ValueWrapper::Integer(100));
    assert_eq!(unwrap_offset(Some(&wrapped)), 100);
}

/// Test unwrap_offset with Long value.
#[test]
fn test_unwrap_offset_long_value() {
    let mut wrapped = HashMap::new();
    wrapped.insert(OFFSET_KEY.to_string(), ValueWrapper::Long(999999));
    assert_eq!(unwrap_offset(Some(&wrapped)), 999999);
}

/// Test unwrap_offset with String value that parses to number.
#[test]
fn test_unwrap_offset_string_parseable() {
    let mut wrapped = HashMap::new();
    wrapped.insert(
        OFFSET_KEY.to_string(),
        ValueWrapper::String("42".to_string()),
    );
    assert_eq!(unwrap_offset(Some(&wrapped)), 42);
}

/// Test unwrap_offset with String value that fails to parse.
/// Returns -1 when parsing fails.
#[test]
fn test_unwrap_offset_string_unparseable() {
    let mut wrapped = HashMap::new();
    wrapped.insert(
        OFFSET_KEY.to_string(),
        ValueWrapper::String("not-a-number".to_string()),
    );
    assert_eq!(unwrap_offset(Some(&wrapped)), -1);
}

/// Test unwrap_offset with negative offset value.
#[test]
fn test_unwrap_offset_negative_value() {
    let wrapped = wrap_offset(-1);
    assert_eq!(unwrap_offset(Some(&wrapped)), -1);
}

// ============================================================================
// Roundtrip Tests (wrap then unwrap)
// ============================================================================

/// Test partition roundtrip: wrap then unwrap returns same values.
#[test]
fn test_partition_roundtrip() {
    let original_tp = TopicPartition::new("roundtrip-topic", 10);
    let wrapped = wrap_partition(&original_tp, "source-cluster");
    let unwrapped = unwrap_partition(&wrapped).unwrap();

    assert_eq!(original_tp.topic(), unwrapped.topic());
    assert_eq!(original_tp.partition(), unwrapped.partition());
}

/// Test offset roundtrip: wrap then unwrap returns same value.
#[test]
fn test_offset_roundtrip() {
    let original_offset: i64 = 9999;
    let wrapped = wrap_offset(original_offset);
    let unwrapped = unwrap_offset(Some(&wrapped));

    assert_eq!(original_offset, unwrapped);
}

/// Test offset roundtrip with negative value.
#[test]
fn test_offset_roundtrip_negative() {
    let original_offset: i64 = -1;
    let wrapped = wrap_offset(original_offset);
    let unwrapped = unwrap_offset(Some(&wrapped));

    assert_eq!(original_offset, unwrapped);
}

/// Test offset roundtrip with zero.
#[test]
fn test_offset_roundtrip_zero() {
    let original_offset: i64 = 0;
    let wrapped = wrap_offset(original_offset);
    let unwrapped = unwrap_offset(Some(&wrapped));

    assert_eq!(original_offset, unwrapped);
}

/// Test partition roundtrip with various cluster aliases.
#[test]
fn test_partition_roundtrip_various_aliases() {
    let aliases = ["source", "target", "backup", "primary", "", "cluster-1"];

    for alias in aliases {
        let tp = TopicPartition::new("test", 5);
        let wrapped = wrap_partition(&tp, alias);
        let unwrapped = unwrap_partition(&wrapped).unwrap();

        assert_eq!(tp.topic(), unwrapped.topic());
        assert_eq!(tp.partition(), unwrapped.partition());
    }
}

// ============================================================================
// encode_topic_partition / decode_topic_partition Tests
// ============================================================================

/// Test basic encode/decode functionality.
#[test]
fn test_encode_decode_basic() {
    let tp = TopicPartition::new("my-topic", 3);
    let encoded = encode_topic_partition(&tp);
    let decoded = decode_topic_partition(&encoded).unwrap();

    assert_eq!(tp.topic(), decoded.topic());
    assert_eq!(tp.partition(), decoded.partition());
}

/// Test encode_topic_partition format.
#[test]
fn test_encode_format() {
    let tp = TopicPartition::new("topic", 5);
    let encoded = encode_topic_partition(&tp);
    assert_eq!(encoded, "topic-5");
}

/// Test decode_topic_partition with dash in topic name.
/// Uses last dash as separator.
#[test]
fn test_decode_with_dash_in_topic() {
    let tp = decode_topic_partition("my-complex-topic-5").unwrap();
    assert_eq!(tp.topic(), "my-complex-topic");
    assert_eq!(tp.partition(), 5);
}

/// Test decode_topic_partition with multiple dashes.
#[test]
fn test_decode_multiple_dashes() {
    let tp = decode_topic_partition("a-b-c-d-10").unwrap();
    assert_eq!(tp.topic(), "a-b-c-d");
    assert_eq!(tp.partition(), 10);
}

/// Test decode_topic_partition returns None for invalid format (no dash).
#[test]
fn test_decode_invalid_no_dash() {
    assert!(decode_topic_partition("invalidformat").is_none());
}

/// Test decode_topic_partition returns None when partition not a number.
#[test]
fn test_decode_invalid_partition_not_number() {
    assert!(decode_topic_partition("topic-notanumber").is_none());
}

/// Test encode/decode roundtrip for various partitions.
#[test]
fn test_encode_decode_roundtrip_partitions() {
    for partition in [0, 1, 10, 100, 1000] {
        let tp = TopicPartition::new("test-topic", partition);
        let encoded = encode_topic_partition(&tp);
        let decoded = decode_topic_partition(&encoded).unwrap();
        assert_eq!(tp.partition(), decoded.partition());
    }
}

// ============================================================================
// new_consumer / new_producer Tests
// ============================================================================

/// Test new_consumer creates MockKafkaConsumer.
#[test]
fn test_new_consumer_basic() {
    let props = HashMap::new();
    let consumer = new_consumer(props);
    assert!(!consumer.is_subscribed());
}

/// Test new_consumer with bootstrap.servers config.
#[test]
fn test_new_consumer_with_bootstrap() {
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    let consumer = new_consumer(props);
    assert!(!consumer.is_subscribed());
}

/// Test new_consumer with group.id config.
#[test]
fn test_new_consumer_with_group_id() {
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    props.insert("group.id".to_string(), "test-group".to_string());
    let consumer = new_consumer(props);
    assert!(!consumer.is_subscribed());
}

/// Test new_producer creates MockKafkaProducer.
#[test]
fn test_new_producer_basic() {
    let props = HashMap::new();
    let producer = new_producer(props);
    assert_eq!(producer.sent_records().len(), 0);
}

/// Test new_producer with bootstrap.servers config.
#[test]
fn test_new_producer_with_bootstrap() {
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    let producer = new_producer(props);
    assert_eq!(producer.sent_records().len(), 0);
}

// ============================================================================
// ValueWrapper Tests
// ============================================================================

/// Test ValueWrapper::String as_string method.
#[test]
fn test_value_wrapper_string_as_string() {
    let v = ValueWrapper::String("test".to_string());
    assert_eq!(v.as_string().unwrap(), "test");
    assert!(v.as_integer().is_none());
    assert!(v.as_long().is_none());
}

/// Test ValueWrapper::Integer as_integer and as_long methods.
#[test]
fn test_value_wrapper_integer_conversions() {
    let v = ValueWrapper::Integer(42);
    assert!(v.as_string().is_none());
    assert_eq!(v.as_integer().unwrap(), 42);
    assert_eq!(v.as_long().unwrap(), 42);
}

/// Test ValueWrapper::Long as_long and as_integer methods.
#[test]
fn test_value_wrapper_long_conversions() {
    let v = ValueWrapper::Long(123456789);
    assert!(v.as_string().is_none());
    assert_eq!(v.as_long().unwrap(), 123456789);
    assert_eq!(v.as_integer().unwrap(), 123456789 as i32);
}

/// Test ValueWrapper equality.
#[test]
fn test_value_wrapper_equality() {
    let v1 = ValueWrapper::String("test".to_string());
    let v2 = ValueWrapper::String("test".to_string());
    let v3 = ValueWrapper::String("other".to_string());

    assert_eq!(v1, v2);
    assert_ne!(v1, v3);
}

/// Test ValueWrapper clone.
#[test]
fn test_value_wrapper_clone() {
    let v1 = ValueWrapper::Long(100);
    let v2 = v1.clone();

    assert_eq!(v1, v2);
}

// ============================================================================
// Constants Tests
// ============================================================================

/// Test constant values match Java MirrorUtils.
#[test]
fn test_constants_match_java() {
    assert_eq!(SOURCE_CLUSTER_KEY, "cluster");
    assert_eq!(TOPIC_KEY, "topic");
    assert_eq!(PARTITION_KEY, "partition");
    assert_eq!(OFFSET_KEY, "offset");
}

// ============================================================================
// Edge Cases and Boundary Tests
// ============================================================================

/// Test wrap_partition with topic containing special characters.
#[test]
fn test_wrap_partition_special_topic_name() {
    let tp = TopicPartition::new("topic-with_special.chars", 1);
    let wrapped = wrap_partition(&tp, "cluster");

    assert_eq!(
        wrapped.get(TOPIC_KEY).unwrap().as_string().unwrap(),
        "topic-with_special.chars"
    );
}

/// Test wrap_offset with i64::MIN.
#[test]
fn test_wrap_offset_min_value() {
    let wrapped = wrap_offset(i64::MIN);
    assert_eq!(unwrap_offset(Some(&wrapped)), i64::MIN);
}

/// Test unwrap_offset handles empty string value.
#[test]
fn test_unwrap_offset_empty_string() {
    let mut wrapped = HashMap::new();
    wrapped.insert(OFFSET_KEY.to_string(), ValueWrapper::String("".to_string()));
    assert_eq!(unwrap_offset(Some(&wrapped)), -1);
}

/// Test decode_topic_partition with partition zero.
#[test]
fn test_decode_partition_zero() {
    let tp = decode_topic_partition("topic-0").unwrap();
    assert_eq!(tp.partition(), 0);
}

/// Test wrap_partition preserves all required keys.
#[test]
fn test_wrap_partition_has_all_keys() {
    let tp = TopicPartition::new("topic", 1);
    let wrapped = wrap_partition(&tp, "alias");

    assert!(wrapped.contains_key(TOPIC_KEY));
    assert!(wrapped.contains_key(PARTITION_KEY));
    assert!(wrapped.contains_key(SOURCE_CLUSTER_KEY));
}
