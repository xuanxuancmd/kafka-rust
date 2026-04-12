//! Checkpoint records emitted by MirrorCheckpointConnector.
//!
//! This module implements the Checkpoint structure and its serialization/deserialization methods,
//! corresponding to Java's Checkpoint.java.

use kafka_clients_trait::consumer::{OffsetAndMetadata, TopicPartition};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Key constants for serialization
pub const TOPIC_KEY: &str = "topic";
pub const PARTITION_KEY: &str = "partition";
pub const CONSUMER_GROUP_ID_KEY: &str = "group";
pub const UPSTREAM_OFFSET_KEY: &str = "upstreamOffset";
pub const DOWNSTREAM_OFFSET_KEY: &str = "offset";
pub const METADATA_KEY: &str = "metadata";
pub const VERSION_KEY: &str = "version";

/// Current version of the checkpoint format
pub const VERSION: i16 = 0;

/// Checkpoint record structure
///
/// Represents a checkpoint emitted by MirrorCheckpointConnector, containing offset information
/// for a consumer group on a specific topic partition.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    consumer_group_id: String,
    topic_partition: TopicPartition,
    upstream_offset: i64,
    downstream_offset: i64,
    metadata: String,
}

impl Checkpoint {
    /// Creates a new Checkpoint instance
    ///
    /// # Arguments
    /// * `consumer_group_id` - The consumer group ID
    /// * `topic_partition` - The topic partition
    /// * `upstream_offset` - The upstream offset
    /// * `downstream_offset` - The downstream offset
    /// * `metadata` - The metadata string
    pub fn new(
        consumer_group_id: String,
        topic_partition: TopicPartition,
        upstream_offset: i64,
        downstream_offset: i64,
        metadata: String,
    ) -> Self {
        Checkpoint {
            consumer_group_id,
            topic_partition,
            upstream_offset,
            downstream_offset,
            metadata,
        }
    }

    /// Returns the consumer group ID
    pub fn consumer_group_id(&self) -> &str {
        &self.consumer_group_id
    }

    /// Returns the topic partition
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }

    /// Returns the upstream offset
    pub fn upstream_offset(&self) -> i64 {
        self.upstream_offset
    }

    /// Returns the downstream offset
    pub fn downstream_offset(&self) -> i64 {
        self.downstream_offset
    }

    /// Returns the metadata
    pub fn metadata(&self) -> &str {
        &self.metadata
    }

    /// Returns the offset and metadata as an OffsetAndMetadata instance
    pub fn offset_and_metadata(&self) -> OffsetAndMetadata {
        OffsetAndMetadata {
            offset: self.downstream_offset,
            metadata: self.metadata.clone(),
            leader_epoch: None,
        }
    }

    /// Serializes the value portion of the checkpoint
    ///
    /// # Arguments
    /// * `version` - The serialization version
    ///
    /// # Returns
    /// A byte vector containing the serialized value
    pub fn serialize_value(&self, version: i16) -> Vec<u8> {
        // For simplicity, use JSON-like format
        // In production, should use proper Kafka protocol serialization
        format!(
            "{{\"{}\":{},\"{}\":{},\"{}\":{},\"{}\":\"{}\"}}}",
            VERSION_KEY,
            version,
            UPSTREAM_OFFSET_KEY,
            self.upstream_offset,
            DOWNSTREAM_OFFSET_KEY,
            self.downstream_offset,
            METADATA_KEY,
            self.metadata
        )
        .as_bytes()
        .to_vec()
    }

    /// Serializes the key portion of the checkpoint
    ///
    /// # Returns
    /// A byte vector containing the serialized key
    pub fn serialize_key(&self) -> Vec<u8> {
        // For simplicity, use JSON-like format
        // In production, should use proper Kafka protocol serialization
        format!(
            "{{\"{}\":\"{}\",\"{}\":\"{}\",\"{}\":{}}}",
            CONSUMER_GROUP_ID_KEY,
            self.consumer_group_id,
            TOPIC_KEY,
            self.topic_partition.topic,
            PARTITION_KEY,
            self.topic_partition.partition
        )
        .as_bytes()
        .to_vec()
    }

    /// Deserializes a checkpoint from a consumer record
    ///
    /// # Arguments
    /// * `key` - The record key bytes
    /// * `value` - The record value bytes
    ///
    /// # Returns
    /// A Checkpoint instance
    pub fn deserialize_record(key: &[u8], value: &[u8]) -> Result<Self, String> {
        // Simplified parsing - in production should use proper Kafka protocol deserialization
        let key_str = String::from_utf8_lossy(key);
        let value_str = String::from_utf8_lossy(value);

        // Parse consumer group ID from key
        let consumer_group_id = Self::parse_string_field(&key_str, CONSUMER_GROUP_ID_KEY);
        let topic = Self::parse_string_field(&key_str, TOPIC_KEY);
        let partition = Self::parse_i32_field(&key_str, PARTITION_KEY);

        // Parse offsets and metadata from value
        let upstream_offset = Self::parse_i64_field(&value_str, UPSTREAM_OFFSET_KEY);
        let downstream_offset = Self::parse_i64_field(&value_str, DOWNSTREAM_OFFSET_KEY);
        let metadata = Self::parse_string_field(&value_str, METADATA_KEY);

        Ok(Checkpoint {
            consumer_group_id,
            topic_partition: TopicPartition { topic, partition },
            upstream_offset,
            downstream_offset,
            metadata,
        })
    }

    /// Parse string field from JSON-like string
    fn parse_string_field(s: &str, field: &str) -> String {
        let pattern = format!("\"{}\":\"", field);
        if let Some(start) = s.find(&pattern) {
            let start = start + pattern.len();
            if let Some(end) = s[start..].find('"') {
                return s[start..start + end].to_string();
            }
        }
        String::new()
    }

    /// Parse i32 field from JSON-like string
    fn parse_i32_field(s: &str, field: &str) -> i32 {
        let pattern = format!("\"{}\":", field);
        if let Some(start) = s.find(&pattern) {
            let start = start + pattern.len();
            if let Some(end) = s[start..].find(|c: char| !c.is_ascii_digit() && c != '-') {
                return s[start..start + end].parse().unwrap_or(0);
            }
        }
        0
    }

    /// Parse i64 field from JSON-like string
    fn parse_i64_field(s: &str, field: &str) -> i64 {
        let pattern = format!("\"{}\":", field);
        if let Some(start) = s.find(&pattern) {
            let start = start + pattern.len();
            if let Some(end) = s[start..].find(|c: char| !c.is_ascii_digit() && c != '-') {
                return s[start..start + end].parse().unwrap_or(0);
            }
        }
        0
    }

    /// Returns the checkpoint as a connect partition map
    pub fn connect_partition(&self) -> HashMap<String, String> {
        let mut partition = HashMap::new();
        partition.insert(
            CONSUMER_GROUP_ID_KEY.to_string(),
            self.consumer_group_id.clone(),
        );
        partition.insert(TOPIC_KEY.to_string(), self.topic_partition.topic.clone());
        partition.insert(
            PARTITION_KEY.to_string(),
            self.topic_partition.partition.to_string(),
        );
        partition
    }

    /// Unwraps the consumer group ID from a connect partition map
    ///
    /// # Arguments
    /// * `connect_partition` - The connect partition map
    ///
    /// # Returns
    /// The consumer group ID string
    pub fn unwrap_group(connect_partition: &HashMap<String, String>) -> String {
        connect_partition
            .get(CONSUMER_GROUP_ID_KEY)
            .cloned()
            .unwrap_or_default()
    }

    /// Returns the record key as bytes
    pub fn record_key(&self) -> Vec<u8> {
        self.serialize_key()
    }

    /// Returns the record value as bytes
    pub fn record_value(&self) -> Vec<u8> {
        self.serialize_value(VERSION)
    }
}

impl std::fmt::Display for Checkpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Checkpoint{{consumerGroupId={}, topicPartition={}, upstreamOffset={}, downstreamOffset={}, metadata={}}}",
            self.consumer_group_id, self.topic_partition, self.upstream_offset, self.downstream_offset, self.metadata
        )
    }
}

impl PartialEq for Checkpoint {
    fn eq(&self, other: &Self) -> bool {
        self.consumer_group_id == other.consumer_group_id
            && self.topic_partition == other.topic_partition
            && self.upstream_offset == other.upstream_offset
            && self.downstream_offset == other.downstream_offset
            && self.metadata == other.metadata
    }
}

impl Eq for Checkpoint {}

impl Hash for Checkpoint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.consumer_group_id.hash(state);
        self.topic_partition.hash(state);
        self.upstream_offset.hash(state);
        self.downstream_offset.hash(state);
        self.metadata.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_creation() {
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint = Checkpoint::new(
            "test-group".to_string(),
            tp.clone(),
            100,
            200,
            "test-metadata".to_string(),
        );

        assert_eq!(checkpoint.consumer_group_id(), "test-group");
        assert_eq!(checkpoint.topic_partition(), &tp);
        assert_eq!(checkpoint.upstream_offset(), 100);
        assert_eq!(checkpoint.downstream_offset(), 200);
        assert_eq!(checkpoint.metadata(), "test-metadata");
    }

    #[test]
    fn test_offset_and_metadata() {
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint = Checkpoint::new(
            "test-group".to_string(),
            tp,
            100,
            200,
            "test-metadata".to_string(),
        );

        let offset_and_metadata = checkpoint.offset_and_metadata();
        assert_eq!(offset_and_metadata.offset, 200);
        assert_eq!(offset_and_metadata.metadata, "test-metadata");
        assert_eq!(offset_and_metadata.leader_epoch, None);
    }

    #[test]
    fn test_serialize_key() {
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint = Checkpoint::new(
            "test-group".to_string(),
            tp,
            100,
            200,
            "test-metadata".to_string(),
        );

        let key_bytes = checkpoint.serialize_key();
        assert!(!key_bytes.is_empty());
        let key_str = String::from_utf8_lossy(&key_bytes);
        assert!(key_str.contains("test-group"));
        assert!(key_str.contains("test-topic"));
        assert!(key_str.contains("0"));
    }

    #[test]
    fn test_serialize_value() {
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint = Checkpoint::new(
            "test-group".to_string(),
            tp,
            100,
            200,
            "test-metadata".to_string(),
        );

        let value_bytes = checkpoint.serialize_value(VERSION);
        assert!(!value_bytes.is_empty());
        let value_str = String::from_utf8_lossy(&value_bytes);
        assert!(value_str.contains("100"));
        assert!(value_str.contains("200"));
        assert!(value_str.contains("test-metadata"));
    }

    #[test]
    fn test_deserialize_record() {
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint = Checkpoint::new(
            "test-group".to_string(),
            tp.clone(),
            100,
            200,
            "test-metadata".to_string(),
        );

        let key_bytes = checkpoint.record_key();
        let value_bytes = checkpoint.record_value();

        let deserialized = Checkpoint::deserialize_record(&key_bytes, &value_bytes).unwrap();

        assert_eq!(deserialized.consumer_group_id(), "test-group");
        assert_eq!(deserialized.topic_partition(), &tp);
        assert_eq!(deserialized.upstream_offset(), 100);
        assert_eq!(deserialized.downstream_offset(), 200);
        assert_eq!(deserialized.metadata(), "test-metadata");
    }

    #[test]
    fn test_connect_partition() {
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint = Checkpoint::new(
            "test-group".to_string(),
            tp,
            100,
            200,
            "test-metadata".to_string(),
        );

        let partition = checkpoint.connect_partition();
        assert_eq!(
            partition.get(CONSUMER_GROUP_ID_KEY),
            Some(&"test-group".to_string())
        );
        assert_eq!(partition.get(TOPIC_KEY), Some(&"test-topic".to_string()));
        assert_eq!(partition.get(PARTITION_KEY), Some(&"0".to_string()));
    }

    #[test]
    fn test_unwrap_group() {
        let mut partition = HashMap::new();
        partition.insert(CONSUMER_GROUP_ID_KEY.to_string(), "test-group".to_string());

        let group_id = Checkpoint::unwrap_group(&partition);
        assert_eq!(group_id, "test-group");
    }

    #[test]
    fn test_equality() {
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint1 = Checkpoint::new(
            "test-group".to_string(),
            tp.clone(),
            100,
            200,
            "test-metadata".to_string(),
        );
        let checkpoint2 = Checkpoint::new(
            "test-group".to_string(),
            tp.clone(),
            100,
            200,
            "test-metadata".to_string(),
        );
        let checkpoint3 = Checkpoint::new(
            "test-group".to_string(),
            tp,
            100,
            300,
            "test-metadata".to_string(),
        );

        assert_eq!(checkpoint1, checkpoint2);
        assert_ne!(checkpoint1, checkpoint3);
    }

    #[test]
    fn test_hash() {
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint1 = Checkpoint::new(
            "test-group".to_string(),
            tp.clone(),
            100,
            200,
            "test-metadata".to_string(),
        );
        let checkpoint2 = Checkpoint::new(
            "test-group".to_string(),
            tp,
            100,
            200,
            "test-metadata".to_string(),
        );

        use std::collections::hash_map::DefaultHasher;
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();

        checkpoint1.hash(&mut hasher1);
        checkpoint2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_display() {
        let tp = TopicPartition {
            topic: "test-topic".to_string(),
            partition: 0,
        };
        let checkpoint = Checkpoint::new(
            "test-group".to_string(),
            tp,
            100,
            200,
            "test-metadata".to_string(),
        );

        let display_str = format!("{}", checkpoint);
        assert!(display_str.contains("test-group"));
        assert!(display_str.contains("test-topic"));
        assert!(display_str.contains("100"));
        assert!(display_str.contains("200"));
        assert!(display_str.contains("test-metadata"));
    }
}

