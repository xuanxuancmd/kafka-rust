//! TopicPartition - 主题和分区
//!
//! 用于表示 Kafka 中的主题和分区

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    /// 创建新的 TopicPartition
    pub fn new(topic: String, partition: i32) -> Self {
        TopicPartition { topic, partition }
    }
}

/// OffsetAndMetadata - 偏移量和元数据
///
/// 用于表示 Kafka 中的偏移量和元数据
#[derive(Debug, Clone)]
pub struct OffsetAndMetadata {
    pub offset: i64,
    pub metadata: String,
    pub leader_epoch: Option<i32>,
}

impl OffsetAndMetadata {
    /// 创建新的 OffsetAndMetadata
    pub fn new(offset: i64, metadata: String) -> Self {
        OffsetAndMetadata {
            offset,
            metadata,
            leader_epoch: None,
        }
    }

    /// 创建带有 leader_epoch 的 OffsetAndMetadata
    pub fn with_leader_epoch(offset: i64, metadata: String, leader_epoch: i32) -> Self {
        OffsetAndMetadata {
            offset,
            metadata,
            leader_epoch: Some(leader_epoch),
        }
    }
}
