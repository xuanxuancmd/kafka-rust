//! Tests for connector module

use connect_api::connector::{ExactlyOnceSupport, OffsetAndMetadata, TopicPartition};

#[test]
fn test_topic_partition_equality() {
    let tp1 = TopicPartition {
        topic: "test".to_string(),
        partition: 0,
    };
    let tp2 = TopicPartition {
        topic: "test".to_string(),
        partition: 0,
    };
    let tp3 = TopicPartition {
        topic: "test".to_string(),
        partition: 1,
    };

    assert_eq!(tp1, tp2);
    assert_ne!(tp1, tp3);
}

#[test]
fn test_offset_and_metadata() {
    let offset = OffsetAndMetadata {
        offset: 100,
        metadata: Some("metadata".to_string()),
        leader_epoch: Some(1),
    };

    assert_eq!(offset.offset, 100);
    assert_eq!(offset.metadata, Some("metadata".to_string()));
    assert_eq!(offset.leader_epoch, Some(1));
}

#[test]
fn test_exactly_once_support() {
    let supported = ExactlyOnceSupport::Supported;
    let unsupported = ExactlyOnceSupport::Unsupported;

    assert_eq!(supported, ExactlyOnceSupport::Supported);
    assert_eq!(unsupported, ExactlyOnceSupport::Unsupported);
    assert_ne!(supported, unsupported);
}
