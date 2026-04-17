//! 数据模型模块
//!
//! 定义MirrorMaker2使用的数据模型

use crate::protocol::{Field, FieldValue, ProtocolError, Schema, Struct, Type};
use common_trait::consumer::{OffsetAndMetadata, TopicPartition};
use std::collections::HashMap;

/// Checkpoint - 检查点数据结构
///
/// 记录消费者组的偏移量检查点信息
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    /// 消费者组ID
    consumer_group_id: String,
    /// 主题分区
    topic_partition: TopicPartition,
    /// 上游偏移量
    upstream_offset: i64,
    /// 下游偏移量
    downstream_offset: i64,
    /// 元数据
    metadata: String,
}

// Checkpoint常量定义
impl Checkpoint {
    pub const TOPIC_KEY: &'static str = "topic";
    pub const PARTITION_KEY: &'static str = "partition";
    pub const CONSUMER_GROUP_ID_KEY: &'static str = "group";
    pub const UPSTREAM_OFFSET_KEY: &'static str = "upstreamOffset";
    pub const DOWNSTREAM_OFFSET_KEY: &'static str = "offset";
    pub const METADATA_KEY: &'static str = "metadata";
    pub const VERSION_KEY: &'static str = "version";
    pub const VERSION: i16 = 0;

    /// Value schema for version 0
    pub fn value_schema_v0() -> Schema {
        Schema::new(vec![
            Field::new(Self::UPSTREAM_OFFSET_KEY, Type::INT64),
            Field::new(Self::DOWNSTREAM_OFFSET_KEY, Type::INT64),
            Field::new(Self::METADATA_KEY, Type::STRING),
        ])
    }

    /// Key schema
    pub fn key_schema() -> Schema {
        Schema::new(vec![
            Field::new(Self::CONSUMER_GROUP_ID_KEY, Type::STRING),
            Field::new(Self::TOPIC_KEY, Type::STRING),
            Field::new(Self::PARTITION_KEY, Type::INT32),
        ])
    }

    /// Header schema
    pub fn header_schema() -> Schema {
        Schema::new(vec![Field::new(Self::VERSION_KEY, Type::INT16)])
    }

    /// Get value schema for a specific version
    fn value_schema(version: i16) -> Schema {
        assert_eq!(version, Self::VERSION);
        Self::value_schema_v0()
    }

    /// 创建新的检查点
    ///
    /// # 参数
    /// - `consumer_group_id`: 消费者组ID
    /// - `topic_partition`: 主题分区
    /// - `upstream_offset`: 上游偏移量
    /// - `downstream_offset`: 下游偏移量
    /// - `metadata`: 元数据
    ///
    /// # 返回
    /// 新的Checkpoint实例
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

    /// 获取消费者组ID
    pub fn consumer_group_id(&self) -> &str {
        &self.consumer_group_id
    }

    /// 获取主题分区
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }

    /// 获取上游偏移量
    pub fn upstream_offset(&self) -> i64 {
        self.upstream_offset
    }

    /// 获取下游偏移量
    pub fn downstream_offset(&self) -> i64 {
        self.downstream_offset
    }

    /// 获取元数据
    pub fn metadata(&self) -> &str {
        &self.metadata
    }

    /// 获取OffsetAndMetadata
    pub fn offset_and_metadata(&self) -> OffsetAndMetadata {
        OffsetAndMetadata {
            offset: self.downstream_offset,
            metadata: self.metadata.clone(),
            leader_epoch: None,
        }
    }

    /// 序列化值（包含头部）
    pub fn serialize_value(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        let header = self.header_struct(version);
        let value_schema = Self::value_schema(version);
        let value_struct = self.value_struct();
        let mut buffer = Vec::new();

        Self::header_schema().write(&mut buffer, &header)?;
        value_schema.write(&mut buffer, &value_struct)?;

        Ok(buffer)
    }

    /// 序列化键
    pub fn serialize_key(&self) -> Result<Vec<u8>, ProtocolError> {
        let key_struct = self.key_struct();
        let mut buffer = Vec::new();
        Self::key_schema().write(&mut buffer, &key_struct)?;
        Ok(buffer)
    }

    /// 从记录反序列化
    pub fn deserialize_record(key: &[u8], value: &[u8]) -> Result<Self, ProtocolError> {
        let mut value_buffer = value;
        let header = Self::header_schema().read(&mut value_buffer)?;
        let version = header
            .get_int16(Self::VERSION_KEY)
            .ok_or_else(|| ProtocolError::InvalidData("Missing version".to_string()))?;
        let value_schema = Self::value_schema(version);
        let value_struct = value_schema.read(&mut value_buffer)?;

        let upstream_offset = value_struct
            .get_int64(Self::UPSTREAM_OFFSET_KEY)
            .ok_or_else(|| ProtocolError::InvalidData("Missing upstream offset".to_string()))?;
        let downstream_offset = value_struct
            .get_int64(Self::DOWNSTREAM_OFFSET_KEY)
            .ok_or_else(|| ProtocolError::InvalidData("Missing downstream offset".to_string()))?;
        let metadata = value_struct
            .get_string(Self::METADATA_KEY)
            .ok_or_else(|| ProtocolError::InvalidData("Missing metadata".to_string()))?;

        let mut key_buffer = key;
        let key_struct = Self::key_schema().read(&mut key_buffer)?;
        let consumer_group_id = key_struct
            .get_string(Self::CONSUMER_GROUP_ID_KEY)
            .ok_or_else(|| ProtocolError::InvalidData("Missing consumer group id".to_string()))?;
        let topic = key_struct
            .get_string(Self::TOPIC_KEY)
            .ok_or_else(|| ProtocolError::InvalidData("Missing topic".to_string()))?;
        let partition = key_struct
            .get_int32(Self::PARTITION_KEY)
            .ok_or_else(|| ProtocolError::InvalidData("Missing partition".to_string()))?;

        Ok(Checkpoint {
            consumer_group_id,
            topic_partition: TopicPartition { topic, partition },
            upstream_offset,
            downstream_offset,
            metadata,
        })
    }

    /// 创建头部结构
    fn header_struct(&self, version: i16) -> Struct {
        let mut header = Struct::new();
        header.set(Self::VERSION_KEY, FieldValue::Int16(version));
        header
    }

    /// 创建值结构
    fn value_struct(&self) -> Struct {
        let mut value = Struct::new();
        value.set(Self::TIMESTAMP_KEY, FieldValue::Int64(self.timestamp));
        value
    }

    /// 创建键结构
    fn key_struct(&self) -> Struct {
        let mut key = Struct::new();
        key.set(
            Self::SOURCE_CLUSTER_ALIAS_KEY,
            FieldValue::String(self.source_cluster_alias.clone()),
        );
        key.set(
            Self::TARGET_CLUSTER_ALIAS_KEY,
            FieldValue::String(self.target_cluster_alias.clone()),
        );
        key
    }

    /// 创建键结构
    fn key_struct(&self) -> Struct {
        let mut key = Struct::new();
        key.set(
            Self::CONSUMER_GROUP_ID_KEY,
            FieldValue::String(self.consumer_group_id.clone()),
        );
        key.set(
            Self::TOPIC_KEY,
            FieldValue::String(self.topic_partition.topic.clone()),
        );
        key.set(
            Self::PARTITION_KEY,
            FieldValue::Int32(self.topic_partition.partition),
        );
        key
    }

    /// 获取连接分区信息
    pub fn connect_partition(&self) -> HashMap<String, FieldValue> {
        let mut partition = HashMap::new();
        partition.insert(
            Self::CONSUMER_GROUP_ID_KEY.to_string(),
            FieldValue::String(self.consumer_group_id.clone()),
        );
        partition.insert(
            Self::TOPIC_KEY.to_string(),
            FieldValue::String(self.topic_partition.topic.clone()),
        );
        partition.insert(
            Self::PARTITION_KEY.to_string(),
            FieldValue::Int32(self.topic_partition.partition),
        );
        partition
    }

    /// 从连接分区信息解包消费者组ID
    pub fn unwrap_group(
        connect_partition: &HashMap<String, FieldValue>,
    ) -> Result<String, ProtocolError> {
        connect_partition
            .get(Self::CONSUMER_GROUP_ID_KEY)
            .and_then(|v| match v {
                FieldValue::String(s) => Some(s.to_string()),
                _ => None,
            })
            .ok_or_else(|| ProtocolError::InvalidData("Missing consumer group id".to_string()))
    }

    /// 获取记录键字节数组
    pub fn record_key(&self) -> Result<Vec<u8>, ProtocolError> {
        self.serialize_key()
    }

    /// 获取记录值字节数组
    pub fn record_value(&self) -> Result<Vec<u8>, ProtocolError> {
        self.serialize_value(Self::VERSION)
    }
}

/// Heartbeat - 心跳数据结构
///
/// 记录集群间的心跳信息
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    /// 源集群别名
    source_cluster_alias: String,
    /// 目标集群别名
    target_cluster_alias: String,
    /// 时间戳
    timestamp: i64,
}

// Heartbeat常量定义
impl Heartbeat {
    pub const SOURCE_CLUSTER_ALIAS_KEY: &'static str = "sourceClusterAlias";
    pub const TARGET_CLUSTER_ALIAS_KEY: &'static str = "targetClusterAlias";
    pub const TIMESTAMP_KEY: &'static str = "timestamp";
    pub const VERSION_KEY: &'static str = "version";
    pub const VERSION: i16 = 0;

    /// Value schema for version 0
    pub fn value_schema_v0() -> Schema {
        Schema::new(vec![Field::new(Self::TIMESTAMP_KEY, Type::INT64)])
    }

    /// Key schema
    pub fn key_schema() -> Schema {
        Schema::new(vec![
            Field::new(Self::SOURCE_CLUSTER_ALIAS_KEY, Type::STRING),
            Field::new(Self::TARGET_CLUSTER_ALIAS_KEY, Type::STRING),
        ])
    }

    /// Header schema
    pub fn header_schema() -> Schema {
        Schema::new(vec![Field::new(Self::VERSION_KEY, Type::INT16)])
    }

    /// Get value schema for a specific version
    fn value_schema(version: i16) -> Schema {
        assert_eq!(version, Self::VERSION);
        Self::value_schema_v0()
    }

    /// 创建新的心跳
    ///
    /// # 参数
    /// - `source_cluster_alias`: 源集群别名
    /// - `target_cluster_alias`: 目标集群别名
    /// - `timestamp`: 时间戳
    ///
    /// # 返回
    ///    新的Heartbeat实例
    pub fn new(source_cluster_alias: String, target_cluster_alias: String, timestamp: i64) -> Self {
        Heartbeat {
            source_cluster_alias,
            target_cluster_alias,
            timestamp,
        }
    }

    /// 获取源集群别名
    pub fn source_cluster_alias(&self) -> &str {
        &self.source_cluster_alias
    }

    /// 获取目标集群别名
    pub fn target_cluster_alias(&self) -> &str {
        &self.target_cluster_alias
    }

    /// 获取时间戳
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// 序列化值（包含头部）
    pub fn serialize_value(&self, version: i16) -> Result<Vec<u8>, ProtocolError> {
        let header = self.header_struct(version);
        let value_schema = Self::value_schema(version);
        let value_struct = self.value_struct();
        let mut buffer = Vec::new();

        Self::header_schema().write(&mut buffer, &header)?;
        value_schema.write(&mut buffer, &value_struct)?;

        Ok(buffer)
    }

    /// 序列化键
    pub fn serialize_key(&self) -> Result<Vec<u8>, ProtocolError> {
        let key_struct = self.key_struct();
        let mut buffer = Vec::new();
        Self::key_schema().write(&mut buffer, &key_struct)?;
        Ok(buffer)
    }

    /// 从记录反序列化
    pub fn deserialize_record(key: &[u8], value: &[u8]) -> Result<Self, ProtocolError> {
        let mut value_buffer = value;
        let header = Self::header_schema().read(&mut value_buffer)?;
        let version = header
            .get_int16(Self::VERSION_KEY)
            .ok_or_else(|| ProtocolError::InvalidData("Missing version".to_string()))?;
        let value_schema = Self::value_schema(version);
        let value_struct = value_schema.read(&mut value_buffer)?;

        let timestamp = value_struct
            .get_int64(Self::TIMESTAMP_KEY)
            .ok_or_else(|| ProtocolError::InvalidData("Missing timestamp".to_string()))?;

        let mut key_buffer = key;
        let key_struct = Self::key_schema().read(&mut key_buffer)?;
        let source_cluster_alias = key_struct
            .get_string(Self::SOURCE_CLUSTER_ALIAS_KEY)
            .ok_or_else(|| {
                ProtocolError::InvalidData("Missing source cluster alias".to_string())
            })?;
        let target_cluster_alias = key_struct
            .get_string(Self::TARGET_CLUSTER_ALIAS_KEY)
            .ok_or_else(|| {
                ProtocolError::InvalidData("Missing target cluster alias".to_string())
            })?;

        Ok(Heartbeat {
            source_cluster_alias,
            target_cluster_alias,
            timestamp,
        })
    }

    /// 创建头部结构
    fn header_struct(&self, version: i16) -> Struct {
        let mut header = Struct::new();
        header.set(Self::VERSION_KEY, FieldValue::Int16(version));
        header
    }

    /// 创建值结构
    fn value_struct(&self) -> Struct {
        let mut value = Struct::new();
        value.set(
            Self::UPSTREAM_OFFSET_KEY,
            FieldValue::Int64(self.upstream_offset),
        );
        value.set(
            Self::DOWNSTREAM_OFFSET_KEY,
            FieldValue::Int64(self.downstream_offset),
        );
        value.set(
            Self::METADATA_KEY,
            FieldValue::String(std::borrow::Cow::Owned(self.metadata.clone())),
        );
        value
    }

    /// 创建键结构
    fn key_struct(&self) -> Struct {
        let mut key = Struct::new();
        key.set(
            Self::SOURCE_CLUSTER_ALIAS_KEY,
            FieldValue::String(std::borrow::Cow::Owned(self.source_cluster_alias.clone())),
        );
        key.set(
            Self::TARGET_CLUSTER_ALIAS_KEY,
            FieldValue::String(std::borrow::Cow::Owned(self.target_cluster_alias.clone())),
        );
        key
    }

    /// 获取连接分区信息
    pub fn connect_partition(&self) -> HashMap<String, FieldValue> {
        let mut partition = HashMap::new();
        partition.insert(
            Self::SOURCE_CLUSTER_ALIAS_KEY.to_string(),
            FieldValue::String(std::borrow::Cow::Owned(self.source_cluster_alias.clone())),
        );
        partition.insert(
            Self::TARGET_CLUSTER_ALIAS_KEY.to_string(),
            FieldValue::String(std::borrow::Cow::Owned(self.target_cluster_alias.clone())),
        );
        partition
    }

    /// 获取记录键字节数组
    pub fn record_key(&self) -> Result<Vec<u8>, ProtocolError> {
        self.serialize_key()
    }

    /// 获取记录值字节数组
    pub fn record_value(&self) -> Result<Vec<u8>, ProtocolError> {
        self.serialize_value(Self::VERSION)
    }
}

impl std::fmt::Display for Heartbeat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Heartbeat{{sourceClusterAlias={}, targetClusterAlias={}, timestamp={}}}",
            self.source_cluster_alias, self.target_cluster_alias, self.timestamp
        )
    }
}

/// SourceAndTarget - 源和目标集群信息
///
/// 记录源集群和目标集群的别名信息
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceAndTarget {
    /// 源集群别名
    source_cluster_alias: String,
    /// 目标集群别名
    target_cluster_alias: String,
}

impl SourceAndTarget {
    /// 创建新的源和目标信息
    ///
    /// # 参数
    /// - `source_cluster_alias`: 源集群别名
    /// - `target_cluster_alias`: 目标集群别名
    ///
    /// # 返回
    /// 新的SourceAndTarget实例
    pub fn new(source_cluster_alias: String, target_cluster_alias: String) -> Self {
        SourceAndTarget {
            source_cluster_alias,
            target_cluster_alias,
        }
    }

    /// 获取源集群别名
    pub fn source_cluster_alias(&self) -> &str {
        &self.source_cluster_alias
    }

    /// 获取目标集群别名
    pub fn target_cluster_alias(&self) -> &str {
        &self.target_cluster_alias
    }
}

impl std::fmt::Display for SourceAndTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}->{}",
            self.source_cluster_alias, self.target_cluster_alias
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_serialization() {
        let checkpoint = Checkpoint::new(
            "test-group".to_string(),
            TopicPartition {
                topic: "test-topic".to_string(),
                partition: 0,
            },
            100,
            90,
            "test-metadata".to_string(),
        );

        let key = checkpoint.serialize_key().unwrap();
        let value = checkpoint.serialize_value(Checkpoint::VERSION).unwrap();

        let deserialized = Checkpoint::deserialize_record(&key, &value).unwrap();
        assert_eq!(deserialized, checkpoint);
    }

    #[test]
    fn test_heartbeat_serialization() {
        let heartbeat = Heartbeat::new("source".to_string(), "target".to_string(), 1234567890);

        let key = heartbeat.serialize_key().unwrap();
        let value = heartbeat.serialize_value(Heartbeat::VERSION).unwrap();

        let deserialized = Heartbeat::deserialize_record(&key, &value).unwrap();
        assert_eq!(deserialized, heartbeat);
    }

    #[test]
    fn test_source_and_target_display() {
        let sat = SourceAndTarget::new("source".to_string(), "target".to_string());
        assert_eq!(format!("{}", sat), "source->target");
    }
}
