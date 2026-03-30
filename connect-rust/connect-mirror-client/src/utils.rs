//! 工具函数模块
//!
//! 提供MirrorMaker2客户端使用的工具函数

use std::collections::HashMap;
use std::error::Error;

use crate::client::{MirrorClient, MirrorClientConfig};

/// 验证主题名称格式
///
/// # 参数
/// - `topic`: 主题名称
///
/// # 返回
/// 如果主题名称格式有效返回true，否则返回false
pub fn is_valid_topic_name(topic: &str) -> bool {
    // 主题名称不能为空
    if topic.is_empty() {
        return false;
    }

    // 主题名称长度限制
    if topic.len() > 249 {
        return false;
    }

    // 主题名称不能以点开头或结尾
    if topic.starts_with('.') || topic.ends_with('.') {
        return false;
    }

    // 主题名称不能包含连续的点
    if topic.contains("..") {
        return false;
    }

    // 主题名称只能包含字母、数字、点、下划线和连字符
    topic
        .chars()
        .all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '-')
}

/// 验证集群别名格式
///
/// # 参数
/// - `alias`: 集群别名
///
/// # 返回
/// 如果集群别名格式有效返回true，否则返回false
pub fn is_valid_cluster_alias(alias: &str) -> bool {
    // 集群别名不能为空
    if alias.is_empty() {
        return false;
    }

    // 集群别名长度限制
    if alias.len() > 100 {
        return false;
    }

    // 集群别名只能包含字母、数字、下划线和连字符
    alias
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
}

/// 验证消费者组ID格式
///
/// # 参数
/// - `group_id`: 消费者组ID
///
/// # 返回
/// 如果消费者组ID格式有效返回true，否则返回false
pub fn is_valid_consumer_group_id(group_id: &str) -> bool {
    // 消费者组ID不能为空
    if group_id.is_empty() {
        return false;
    }

    // 消费者组ID长度限制
    if group_id.len() > 255 {
        return false;
    }

    true
}

/// 提取主题分区信息
///
/// # 参数
/// - `topic_partition`: 主题分区字符串（格式：topic-partition）
///
/// # 返回
/// 包含主题名称和分区号的元组
pub fn parse_topic_partition(topic_partition: &str) -> Option<(String, i32)> {
    let parts: Vec<&str> = topic_partition.rsplitn(2, '-').collect();

    if parts.len() != 2 {
        return None;
    }

    let partition = parts[0].parse::<i32>().ok()?;
    let topic = parts[1].to_string();

    Some((topic, partition))
}

/// 格式化主题分区信息
///
/// # 参数
/// - `topic`: 主题名称
/// - `partition`: 分区号
///
/// # 返回
/// 格式化后的主题分区字符串
pub fn format_topic_partition(topic: &str, partition: i32) -> String {
    format!("{}-{}", topic, partition)
}

/// 计算偏移量差异
///
/// # 参数
/// - `upstream_offset`: 上游偏移量
/// - `downstream_offset`: 下游偏移量
///
/// # 返回
/// 偏移量差异
pub fn calculate_offset_lag(upstream_offset: i64, downstream_offset: i64) -> i64 {
    upstream_offset - downstream_offset
}

/// 合并配置
///
/// # 参数
/// - `base_config`: 基础配置
/// - `override_config`: 覆盖配置
///
/// # 返回
/// 合并后的配置
pub fn merge_configs(
    base_config: HashMap<String, String>,
    override_config: HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = base_config;
    for (key, value) in override_config {
        merged.insert(key, value);
    }
    merged
}

/// 验证配置键
///
/// # 参数
/// - `key`: 配置键
///
/// # 返回
/// 如果配置键有效返回true，否则返回false
pub fn is_valid_config_key(key: &str) -> bool {
    // 配置键不能为空
    if key.is_empty() {
        return false;
    }

    // 配置键长度限制
    if key.len() > 255 {
        return false;
    }

    true
}

/// 验证配置值
///
/// # 参数
/// - `value`: 配置值
///
/// # 返回
/// 如果配置值有效返回true，否则返回false
pub fn is_valid_config_value(value: &str) -> bool {
    // 配置值长度限制
    if value.len() > 4096 {
        return false;
    }

    true
}

/// 获取当前时间戳（毫秒）
///
/// # 返回
/// 当前时间戳（毫秒）
pub fn current_timestamp_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 判断偏移量是否有效
///
/// # 参数
/// - `offset`: 偏移量
///
/// # 返回
/// 如果偏移量有效返回true，否则返回false
pub fn is_valid_offset(offset: i64) -> bool {
    offset >= 0
}

/// 判断偏移量是否需要同步
///
/// # 参数
/// - `upstream_offset`: 上游偏移量
/// - `downstream_offset`: 下游偏移量
/// - `threshold`: 同步阈值
///
/// # 返回
/// 如果需要同步返回true，否则返回false
pub fn needs_sync(upstream_offset: i64, downstream_offset: i64, threshold: i64) -> bool {
    let lag = calculate_offset_lag(upstream_offset, downstream_offset);
    lag > threshold
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_topic_name() {
        assert!(is_valid_topic_name("test-topic"));
        assert!(is_valid_topic_name("test_topic"));
        assert!(is_valid_topic_name("test.topic"));
        assert!(!is_valid_topic_name(""));
        assert!(!is_valid_topic_name(".test"));
        assert!(!is_valid_topic_name("test."));
        assert!(!is_valid_topic_name("test..topic"));
    }

    #[test]
    fn test_is_valid_cluster_alias() {
        assert!(is_valid_cluster_alias("us-west"));
        assert!(is_valid_cluster_alias("us_west"));
        assert!(!is_valid_cluster_alias(""));
        assert!(!is_valid_cluster_alias("us.west"));
    }

    #[test]
    fn test_parse_topic_partition() {
        assert_eq!(
            parse_topic_partition("test-topic-0"),
            Some(("test-topic".to_string(), 0))
        );
        assert_eq!(
            parse_topic_partition("test-topic-10"),
            Some(("test-topic".to_string(), 10))
        );
        assert_eq!(parse_topic_partition("invalid"), None);
    }

    #[test]
    fn test_format_topic_partition() {
        assert_eq!(format_topic_partition("test-topic", 0), "test-topic-0");
        assert_eq!(format_topic_partition("test-topic", 10), "test-topic-10");
    }

    #[test]
    fn test_calculate_offset_lag() {
        assert_eq!(calculate_offset_lag(100, 90), 10);
        assert_eq!(calculate_offset_lag(100, 100), 0);
        assert_eq!(calculate_offset_lag(90, 100), -10);
    }

    #[test]
    fn test_merge_configs() {
        let mut base = HashMap::new();
        base.insert("key1".to_string(), "value1".to_string());
        base.insert("key2".to_string(), "value2".to_string());

        let mut override_config = HashMap::new();
        override_config.insert("key2".to_string(), "new_value2".to_string());
        override_config.insert("key3".to_string(), "value3".to_string());

        let merged = merge_configs(base, override_config);

        assert_eq!(merged.get("key1"), Some(&"value1".to_string()));
        assert_eq!(merged.get("key2"), Some(&"new_value2".to_string()));
        assert_eq!(merged.get("key3"), Some(&"value3".to_string()));
    }

    #[test]
    fn test_is_valid_offset() {
        assert!(is_valid_offset(0));
        assert!(is_valid_offset(100));
        assert!(!is_valid_offset(-1));
    }

    #[test]
    fn test_needs_sync() {
        assert!(needs_sync(100, 90, 5));
        assert!(!needs_sync(100, 95, 5));
        assert!(!needs_sync(100, 100, 5));
    }
}

/// RemoteClusterUtils - 远程集群工具类
///
/// 提供与MirrorMaker内部主题和远程集群交互的便捷静态方法
pub struct RemoteClusterUtils;

impl RemoteClusterUtils {
    /// 计算复制跳数
    ///
    /// # 参数
    /// - `properties`: 配置属性
    /// - `upstream_cluster_alias`: 上游集群别名
    ///
    /// # 返回
    /// 复制跳数
    pub fn replication_hops(
        properties: &HashMap<String, String>,
        upstream_cluster_alias: &str,
    ) -> Result<i32, Box<dyn Error>> {
        let config = MirrorClientConfig::new(properties.clone());
        let mut client = crate::client::BasicMirrorClient::new(config);
        let result = client.replication_hops(upstream_cluster_alias.to_string());
        client.close()?;
        result
    }

    /// 获取心跳主题列表
    ///
    /// # 参数
    /// - `properties`: 配置属性
    ///
    /// # 返回
    /// 心跳主题列表
    pub fn heartbeat_topics(
        properties: &HashMap<String, String>,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let config = MirrorClientConfig::new(properties.clone());
        let mut client = crate::client::BasicMirrorClient::new(config);
        let result = client.heartbeat_topics();
        client.close()?;
        result
    }

    /// 获取检查点主题列表
    ///
    /// # 参数
    /// - `properties`: 配置属性
    ///
    /// # 返回
    /// 检查点主题列表
    pub fn checkpoint_topics(
        properties: &HashMap<String, String>,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let config = MirrorClientConfig::new(properties.clone());
        let mut client = crate::client::BasicMirrorClient::new(config);
        let result = client.checkpoint_topics();
        client.close()?;
        result
    }

    /// 获取上游集群列表
    ///
    /// # 参数
    /// - `properties`: 配置属性
    ///
    /// # 返回
    /// 上游集群别名列表
    pub fn upstream_clusters(
        properties: &HashMap<String, String>,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let config = MirrorClientConfig::new(properties.clone());
        let mut client = crate::client::BasicMirrorClient::new(config);
        let result = client.upstream_clusters();
        client.close()?;
        result
    }

    /// 获取远程主题列表
    ///
    /// # 参数
    /// - `properties`: 配置属性
    ///
    /// # 返回
    /// 远程主题列表
    pub fn remote_topics(
        properties: &HashMap<String, String>,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let config = MirrorClientConfig::new(properties.clone());
        let mut client = crate::client::BasicMirrorClient::new(config);
        let result = client.remote_topics();
        client.close()?;
        result
    }

    /// 获取指定源的远程主题列表
    ///
    /// # 参数
    /// - `properties`: 配置属性
    /// - `source`: 源集群别名
    ///
    /// # 返回
    /// 远程主题列表
    pub fn remote_topics_for_source(
        properties: &HashMap<String, String>,
        source: &str,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let config = MirrorClientConfig::new(properties.clone());
        let mut client = crate::client::BasicMirrorClient::new(config);
        let result = client.remote_topics_for_source(source.to_string());
        client.close()?;
        result
    }

    /// 翻译远程消费者偏移量
    ///
    /// # 参数
    /// - `properties`: 配置属性
    /// - `remote_cluster_alias`: 远程集群别名
    /// - `consumer_group_id`: 消费者组ID
    /// - `timeout`: 超时时间（毫秒）
    ///
    /// # 返回
    /// 消费者组ID到主题偏移量的映射
    pub fn translate_offsets(
        properties: &HashMap<String, String>,
        remote_cluster_alias: &str,
        consumer_group_id: &str,
        timeout: i64,
    ) -> Result<HashMap<String, i64>, Box<dyn Error>> {
        let config = MirrorClientConfig::new(properties.clone());
        let mut client = crate::client::BasicMirrorClient::new(config);
        let result = client.remote_consumer_offsets(
            consumer_group_id.to_string(),
            remote_cluster_alias.to_string(),
            timeout,
        );
        client.close()?;
        result
    }
}
