//! 数据模型模块
//!
//! 定义MirrorMaker2使用的数据模型

use std::error::Error;

/// Checkpoint - 检查点数据结构
///
/// 记录消费者组的偏移量检查点信息
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    /// 消费者组ID
    consumer_group_id: String,
    /// 主题分区
    topic_partition: String,
    /// 上游偏移量
    upstream_offset: i64,
    /// 下游偏移量
    downstream_offset: i64,
    /// 元数据（可选）
    metadata: Option<String>,
}

impl Checkpoint {
    /// 创建新的检查点
    ///
    /// # 参数
    /// - `consumer_group_id`: 消费者组ID
    /// - `topic_partition`: 主题分区
    /// - `upstream_offset`: 上游偏移量
    /// - `downstream_offset`: 下游偏移量
    ///
    /// # 返回
    /// 新的Checkpoint实例
    pub fn new(
        consumer_group_id: String,
        topic_partition: String,
        upstream_offset: i64,
        downstream_offset: i64,
    ) -> Self {
        Checkpoint {
            consumer_group_id,
            topic_partition,
            upstream_offset,
            downstream_offset,
            metadata: None,
        }
    }

    /// 创建带有元数据的检查点
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
    pub fn with_metadata(
        consumer_group_id: String,
        topic_partition: String,
        upstream_offset: i64,
        downstream_offset: i64,
        metadata: String,
    ) -> Self {
        Checkpoint {
            consumer_group_id,
            topic_partition,
            upstream_offset,
            downstream_offset,
            metadata: Some(metadata),
        }
    }

    /// 获取消费者组ID
    pub fn consumer_group_id(&self) -> &str {
        &self.consumer_group_id
    }

    /// 获取主题分区
    pub fn topic_partition(&self) -> &str {
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
    pub fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    /// 序列化检查点
    ///
    /// # 返回
    /// 序列化后的字节数组
    pub fn serialize(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        // 简化实现，实际应该使用JSON或其他序列化格式
        let data = format!(
            "{}|{}|{}|{}|{}",
            self.consumer_group_id,
            self.topic_partition,
            self.upstream_offset,
            self.downstream_offset,
            self.metadata.as_deref().unwrap_or("")
        );
        Ok(data.into_bytes())
    }

    /// 反序列化检查点
    ///
    /// # 参数
    /// - `data`: 序列化后的字节数组
    ///
    /// # 返回
    /// 反序列化后的Checkpoint实例
    pub fn deserialize(data: Vec<u8>) -> Result<Self, Box<dyn Error>> {
        let data_str = String::from_utf8(data)?;
        let parts: Vec<&str> = data_str.split('|').collect();

        if parts.len() < 4 {
            return Err("Invalid checkpoint data".into());
        }

        let consumer_group_id = parts[0].to_string();
        let topic_partition = parts[1].to_string();
        let upstream_offset = parts[2].parse::<i64>()?;
        let downstream_offset = parts[3].parse::<i64>()?;
        let metadata = if parts.len() > 4 && !parts[4].is_empty() {
            Some(parts[4].to_string())
        } else {
            None
        };

        Ok(Checkpoint {
            consumer_group_id,
            topic_partition,
            upstream_offset,
            downstream_offset,
            metadata,
        })
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

impl Heartbeat {
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

    /// 序列化心跳
    ///
    /// # 返回
    /// 序列化后的字节数组
    pub fn serialize(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        // 简化实现，实际应该使用JSON或其他序列化格式
        let data = format!(
            "{}|{}|{}",
            self.source_cluster_alias, self.target_cluster_alias, self.timestamp
        );
        Ok(data.into_bytes())
    }

    /// 反序列化心跳
    ///
    /// # 参数
    /// - `data`: 序列化后的字节数组
    ///
    /// # 返回
    /// 反序列化后的Heartbeat实例
    pub fn deserialize(data: Vec<u8>) -> Result<Self, Box<dyn Error>> {
        let data_str = String::from_utf8(data)?;
        let parts: Vec<&str> = data_str.split('|').collect();

        if parts.len() < 3 {
            return Err("Invalid heartbeat data".into());
        }

        let source_cluster_alias = parts[0].to_string();
        let target_cluster_alias = parts[1].to_string();
        let timestamp = parts[2].parse::<i64>()?;

        Ok(Heartbeat {
            source_cluster_alias,
            target_cluster_alias,
            timestamp,
        })
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
