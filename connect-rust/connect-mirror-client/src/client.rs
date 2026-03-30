//! MirrorClient - MirrorMaker2客户端
//!
//! 提供用于MirrorMaker2的客户端功能

use std::collections::HashMap;
use std::error::Error;

use crate::policy::{DefaultReplicationPolicy, IdentityReplicationPolicy, ReplicationPolicy};

/// 配置键常量
pub mod config_keys {
    /// 复制策略类配置键
    pub const REPLICATION_POLICY_CLASS: &str = "replication.policy.class";
    /// 复制策略分隔符配置键
    pub const REPLICATION_POLICY_SEPARATOR: &str = "replication.policy.separator";
    /// 是否启用内部主题分隔符配置键
    pub const INTERNAL_TOPIC_SEPARATOR_ENABLED: &str =
        "replication.policy.is.internal.topic.separator.enabled";
    /// Admin客户端配置前缀
    pub const ADMIN_CLIENT_PREFIX: &str = "admin.";
    /// Consumer客户端配置前缀
    pub const CONSUMER_CLIENT_PREFIX: &str = "consumer.";
    /// Producer客户端配置前缀
    pub const PRODUCER_CLIENT_CLIENT_PREFIX: &str = "producer.";
    /// 源集群别名配置键
    pub const SOURCE_CLUSTER_ALIAS: &str = "source.cluster.alias";
}

/// MirrorClientConfig - MirrorClient配置
///
/// 配置MirrorClient的各种参数
#[derive(Clone)]
pub struct MirrorClientConfig {
    /// 原始配置属性
    props: HashMap<String, String>,
    /// 复制策略类名
    replication_policy_class: String,
}

impl MirrorClientConfig {
    /// 创建新的MirrorClientConfig
    ///
    /// # 参数
    /// - `props`: 配置属性
    ///
    /// # 返回
    /// 新的MirrorClientConfig实例
    pub fn new(props: HashMap<String, String>) -> Self {
        let replication_policy_class = props
            .get(config_keys::REPLICATION_POLICY_CLASS)
            .cloned()
            .unwrap_or_else(|| {
                "org.apache.kafka.connect.mirror.DefaultReplicationPolicy".to_string()
            });

        MirrorClientConfig {
            props,
            replication_policy_class,
        }
    }

    /// 获取复制策略实例
    ///
    /// # 返回
    /// 复制策略实例
    pub fn replication_policy(&self) -> Box<dyn ReplicationPolicy> {
        // 根据配置创建相应的复制策略
        if self
            .replication_policy_class
            .contains("IdentityReplicationPolicy")
        {
            let mut policy = IdentityReplicationPolicy::new();
            policy.configure(&self.props);
            Box::new(policy)
        } else {
            let mut policy = DefaultReplicationPolicy::new();
            policy.configure(&self.props);
            Box::new(policy)
        }
    }

    /// 获取Admin客户端配置
    ///
    /// # 返回
    /// Admin客户端配置
    pub fn admin_config(&self) -> HashMap<String, String> {
        self.extract_config_with_prefix(config_keys::ADMIN_CLIENT_PREFIX)
    }

    /// 获取Consumer客户端配置
    ///
    /// # 返回
    /// Consumer客户端配置
    pub fn consumer_config(&self) -> HashMap<String, String> {
        self.extract_config_with_prefix(config_keys::CONSUMER_CLIENT_PREFIX)
    }

    /// 获取Producer客户端配置
    ///
    /// # 返回
    /// Producer客户端配置
    pub fn producer_config(&self) -> HashMap<String, String> {
        self.extract_config_with_prefix(config_keys::PRODUCER_CLIENT_CLIENT_PREFIX)
    }

    /// 提取指定前缀的配置
    ///
    /// # 参数
    /// - `prefix`: 配置前缀
    ///
    /// # 返回
    /// 提取的配置
    fn extract_config_with_prefix(&self, prefix: &str) -> HashMap<String, String> {
        let mut config = HashMap::new();
        for (key, value) in &self.props {
            if key.starts_with(prefix) {
                // 移除前缀
                let config_key = key[prefix.len()..].to_string();
                config.insert(config_key, value.clone());
            }
        }
        config
    }

    /// 获取原始配置属性
    ///
    /// # 返回
    /// 原始配置属性
    pub fn props(&self) -> &HashMap<String, String> {
        &self.props
    }

    /// 获取配置值
    ///
    /// # 参数
    /// - `key`: 配置键
    ///
    /// # 返回
    /// 配置值（如果存在）
    pub fn get(&self, key: &str) -> Option<&String> {
        self.props.get(key)
    }
}

/// MirrorClient - MirrorMaker2客户端
///
/// 提供用于MirrorMaker2的客户端功能，包括：
/// - 复制跳数查询
/// - 心跳主题查询
///   - 检查点主题查询
/// - 上游集群查询
/// - 远程主题查询
/// - 远程消费者偏移量查询
/// - 复制策略获取
pub trait MirrorClient {
    /// 获取复制跳数
    ///
    /// # 参数
    /// - `upstream_cluster_alias`: 上游集群别名
    ///
    /// # 返回
    /// 复制跳数
    fn replication_hops(&self, upstream_cluster_alias: String) -> Result<i32, Box<dyn Error>>;

    /// 获取心跳主题列表
    ///
    /// # 返回
    /// 心跳主题列表
    fn heartbeat_topics(&self) -> Result<Vec<String>, Box<dyn Error>>;

    /// 获取检查点主题（checkpoint topics）列表
    ///
    /// # 返回
    /// 检查点主题列表
    fn checkpoint_topics(&self) -> Result<Vec<String>, Box<dyn Error>>;

    /// 获取上游集群列表
    ///
    /// # 返回
    /// 上游集群别名列表
    fn upstream_clusters(&self) -> Result<Vec<String>, Box<dyn Error>>;

    /// 获取远程主题列表
    ///
    /// # 返回
    /// 远程主题列表
    fn remote_topics(&self) -> Result<Vec<String>, Box<dyn Error>>;

    /// 获取指定源的远程主题列表
    ///
    /// # 参数
    /// - `source`: 源集群别名
    ///
    /// # 返回
    /// 远程主题列表
    fn remote_topics_for_source(&self, source: String) -> Result<Vec<String>, Box<dyn Error>>;

    /// 获取远程消费者偏移量
    ///
    /// # 参数
    /// - `consumer_group_id`: 消费者组ID
    /// - `remote_cluster_alias`: 远程集群别名
    /// - `timeout`: 超时时间（毫秒）
    ///
    /// # 返回
    /// 消费者组ID到主题偏移量的映射
    fn remote_consumer_offsets(
        &self,
        consumer_group_id: String,
        remote_cluster_alias: String,
        timeout: i64,
    ) -> Result<HashMap<String, i64>, Box<dyn Error>>;

    /// 获取复制策略
    ///
    /// # 返回
    /// 复制策略实例
    fn replication_policy(&self) -> Box<dyn ReplicationPolicy>;

    /// 关闭客户端
    fn close(&mut self) -> Result<(), Box<dyn Error>>;
}

/// BasicMirrorClient - MirrorClient的基本实现
///
/// 提供MirrorClient trait的基本实现
pub struct BasicMirrorClient {
    /// 复制策略
    replication_policy: Box<dyn ReplicationPolicy>,
    /// 消费者配置
    consumer_config: HashMap<String, String>,
    /// 是否已关闭
    closed: bool,
}

impl BasicMirrorClient {
    /// 创建新的BasicMirrorClient
    ///
    /// # 参数
    /// - `config`: MirrorClient配置
    ///
    /// # 返回
    /// 新的BasicMirrorClient实例
    pub fn new(config: MirrorClientConfig) -> Self {
        let replication_policy = config.replication_policy();
        let consumer_config = config.consumer_config();

        BasicMirrorClient {
            replication_policy,
            consumer_config,
            closed: false,
        }
    }

    /// 创建带有自定义参数的BasicMirrorClient
    ///
    /// # 参数
    /// - `replication_policy`: 复制策略
    /// - `consumer_config`: 消费者配置
    ///
    /// # 返回
    /// 新的BasicMirrorClient实例
    pub fn with_params(
        replication_policy: Box<dyn ReplicationPolicy>,
        consumer_config: HashMap<String, String>,
    ) -> Self {
        BasicMirrorClient {
            replication_policy,
            consumer_config,
            closed: false,
        }
    }

    /// 列出所有主题
    ///
    /// # 返回
    /// 所有主题名称的集合
    fn list_topics(&self) -> Result<std::collections::HashSet<String>, Box<dyn Error>> {
        // 简化实现，实际应该使用adminClient.listTopics()
        // 这里返回一个空集合作为占位符
        Ok(std::collections::HashSet::new())
    }

    /// 统计主题的复制跳数
    ///
    /// # 参数
    /// - `topic`: 主题名称
    /// - `source_cluster_alias`: 源集群别名
    ///
    /// # 返回
    /// 复制跳数
    fn count_hops_for_topic(
        &self,
        topic: &str,
        source_cluster_alias: &str,
    ) -> Result<i32, Box<dyn Error>> {
        let mut hops = 0;
        let mut current_topic = topic.to_string();

        loop {
            if self
                .replication_policy()
                .topic_source(current_topic.clone())
                .is_empty()
            {
                break;
            }

            let source = self
                .replication_policy()
                .topic_source(current_topic.clone());
            if source == source_cluster_alias {
                return Ok(hops + 1);
            }

            current_topic = self
                .replication_policy()
                .upstream_topic(current_topic.clone());
            hops += 1;

            if hops > 100 {
                // 防止无限循环
                return Err("Too many hops, possible cycle".into());
            }
        }

        Err(format!("Source cluster {} not found", source_cluster_alias).into())
    }

    /// 获取主题的所有源
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 所有源集群别名的集合
    fn all_sources(
        &self,
        topic: &str,
    ) -> Result<std::collections::HashSet<String>, Box<dyn Error>> {
        let mut sources = std::collections::HashSet::new();
        let mut current_topic = topic.to_string();

        loop {
            let source = self
                .replication_policy()
                .topic_source(current_topic.clone());
            if source.is_empty() {
                break;
            }

            sources.insert(source.clone());
            current_topic = self
                .replication_policy()
                .upstream_topic(current_topic.clone());

            if sources.len() > 100 {
                // 防止无限循环
                return Err("Too many sources, possible cycle".into());
            }
        }

        Ok(sources)
    }

    /// 判断是否为心跳主题
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 如果是心跳主题返回true，否则返回false
    fn is_heartbeat_topic(&self, topic: &str) -> bool {
        topic.ends_with(&self.replication_policy().heartbeats_topic())
    }

    /// 判断是否为检查点主题
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 如果是检查点主题返回true，否则返回false
    fn is_checkpoint_topic(&self, topic: &str) -> bool {
        topic.ends_with(&self.replication_policy().checkpoints_topic())
    }

    /// 判断是否为远程主题
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 如果是远程主题返回true，否则返回false
    fn is_remote_topic(&self, topic: &str) -> bool {
        !self
            .replication_policy()
            .is_internal_topic(topic.to_string())
            && !self
                .replication_policy()
                .topic_source(topic.to_string())
                .is_empty()
    }
}

impl MirrorClient for BasicMirrorClient {
    fn replication_hops(&self, upstream_cluster_alias: String) -> Result<i32, Box<dyn Error>> {
        let heartbeat_topics = self.heartbeat_topics()?;
        let mut min_hops = i32::MAX;

        for topic in &heartbeat_topics {
            let hops = self.count_hops_for_topic(topic, &upstream_cluster_alias);
            if let Ok(h) = hops {
                if h < min_hops {
                    min_hops = h;
                }
            }
        }

        if min_hops == i32::MAX {
            Err(format!("No path to upstream cluster {}", upstream_cluster_alias).into())
        } else {
            Ok(min_hops)
        }
    }

    fn heartbeat_topics(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let all_topics = self.list_topics()?;
        Ok(all_topics
            .into_iter()
            .filter(|topic| self.is_heartbeat_topic(topic))
            .collect())
    }

    fn checkpoint_topics(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let all_topics = self.list_topics()?;
        Ok(all_topics
            .into_iter()
            .filter(|topic| self.is_checkpoint_topic(topic))
            .collect())
    }

    fn upstream_clusters(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let heartbeat_topics = self.heartbeat_topics()?;
        let mut upstream_clusters = std::collections::HashSet::new();

        for topic in &heartbeat_topics {
            let sources = self.all_sources(topic)?;
            upstream_clusters.extend(sources);
        }

        Ok(upstream_clusters.into_iter().collect())
    }

    fn remote_topics(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let all_topics = self.list_topics()?;
        Ok(all_topics
            .into_iter()
            .filter(|topic| self.is_remote_topic(topic))
            .collect())
    }

    fn remote_topics_for_source(&self, source: String) -> Result<Vec<String>, Box<dyn Error>> {
        let all_remote_topics = self.remote_topics()?;
        Ok(all_remote_topics
            .into_iter()
            .filter(|topic| self.replication_policy().topic_source(topic.clone()) == source)
            .collect())
    }

    fn remote_consumer_offsets(
        &self,
        _consumer_group_id: String,
        _remote_cluster_alias: String,
        _timeout: i64,
    ) -> Result<HashMap<String, i64>, Box<dyn Error>> {
        // 简化实现，实际应该从检查点主题读取偏移量
        // 这里返回一个空映射作为占位符
        Ok(HashMap::new())
    }

    fn replication_policy(&self) -> Box<dyn ReplicationPolicy> {
        // 注意：这里需要克隆replication_policy，但由于trait object不能直接克隆
        // 实际实现可能需要使用Arc或其他共享机制
        // 这里返回一个默认实现作为占位符
        Box::new(DefaultReplicationPolicy::new())
    }

    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.closed = true;
        Ok(())
    }
}
