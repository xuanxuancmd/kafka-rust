//! 复制策略模块
//!
//! 定义主题名称复制策略的trait和实现

/// ReplicationPolicy - 主题名称复制策略
///
/// 定义如何复制和转换主题名称的策略接口
pub trait ReplicationPolicy {
    /// 格式化远程主题名称
    ///
    /// # 参数
    /// - `topic`: �原始主题名称
    ///
    /// # 返回
    /// 格式化后的远程主题名称
    fn format_remote_topic(&self, topic: String) -> String;

    /// 获取主题源集群别名
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 源集群别名
    fn topic_source(&self, topic: String) -> String;

    /// 获取上游主题名称
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 上游主题名称
    fn upstream_topic(&self, topic: String) -> String;

    /// 获取原始主题名称
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 原始主题名称
    fn original_topic(&self, topic: String) -> String;

    /// 获取心跳主题名称
    ///
    /// # 返回
    /// 心跳主题名称
    fn heartbeats_topic(&self) -> String;

    /// 获取偏移同步主题名称
    ///
    /// # 返回
    /// 偏移同步主题名称
    fn offset_syncs_topic(&self) -> String;

    /// 获取检查点主题名称
    ///
    /// # 返回
    /// 检查查点主题名称
    fn checkpoints_topic(&self) -> String;

    /// 判断是否为内部主题
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 如果是内部主题返回true，否则返回false
    fn is_internal_topic(&self, topic: String) -> bool;
}

/// DefaultReplicationPolicy - 默认复制策略
///
/// 在远程主题名称前添加源集群别名（如：us-west.topic）
pub struct DefaultReplicationPolicy {
    /// 分隔符，默认为'.'
    separator: String,
    /// 是否启用内部主题分隔符
    is_internal_topic_separator_enabled: bool,
}

impl DefaultReplicationPolicy {
    /// 创建新的默认复制策略
    ///
    /// # 返回
    /// 新的DefaultReplicationPolicy实例
    pub fn new() -> Self {
        DefaultReplicationPolicy {
            separator: ".".to_string(),
            is_internal_topic_separator_enabled: true,
        }
    }

    /// 创建带有自定义分隔符的复制策略
    ///
    /// # 参数
    /// - `separator`: 分隔符
    ///
    /// # 返回
    /// 新的DefaultReplicationPolicy实例
    pub fn with_separator(separator: String) -> Self {
        DefaultReplicationPolicy {
            separator,
            is_internal_topic_separator_enabled: true,
        }
    }

    /// 配置复制策略
    ///
    /// # 参数
    /// - `props`: 配置属性
    pub fn configure(&mut self, props: &std::collections::HashMap<String, String>) {
        if let Some(separator) = props.get("replication.policy.separator") {
            self.separator = separator.clone();
        }
        if let Some(enabled) = props.get("replication.policy.is.internal.topic.separator.enabled") {
            self.is_internal_topic_separator_enabled = enabled.parse().unwrap_or(true);
        }
    }

    /// 获取内部主题分隔符
    fn internal_separator(&self) -> &str {
        if self.is_internal_topic_separator_enabled {
            &self.separator
        } else {
            "."
        }
    }

    /// 获取内部主题后缀
    fn internal_suffix(&self) -> String {
        format!("{}internal", self.internal_separator())
    }

    /// 获取检查点主题后缀
    fn checkpoints_topic_suffix(&self) -> String {
        format!(
            "{}checkpoints{}",
            self.internal_separator(),
            self.internal_separator()
        )
    }

    /// 获取指定集群的偏移同步主题名称
    ///
    /// # 参数
    /// - `cluster_alias`: 集群别名
    ///
    /// # 返回
    /// 偏移同步主题名称
    pub fn offset_syncs_topic_for_cluster(&self, cluster_alias: &str) -> String {
        format!(
            "{}{}offset-syncs{}",
            cluster_alias,
            self.internal_separator(),
            self.internal_separator()
        )
    }

    /// 获取指定集群的检查点主题名称
    ///
    /// # 参数
    /// - `cluster_alias`: 集群别名
    ///
    /// # 返回
    /// 检查点主题名称
    pub fn checkpoints_topic_for_cluster(&self, cluster_alias: &str) -> String {
        format!(
            "{}{}checkpoints{}",
            cluster_alias,
            self.internal_separator(),
            self.internal_separator()
        )
    }

    /// 判断是否为检查点主题
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 如果是检查点主题返回true，否则返回false
    pub fn is_checkpoints_topic(&self, topic: &str) -> bool {
        topic.ends_with(&self.checkpoints_topic_suffix())
    }

    /// 判断是否为MirrorMaker2内部主题
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 如果是内部主题返回true，否则返回false
    pub fn is_mm2_internal_topic(&self, topic: &str) -> bool {
        self.is_heartbeat_topic(topic)
            || self.is_checkpoints_topic(topic)
            || topic.ends_with(&format!(
                "{}offset-syncs{}",
                self.internal_separator(),
                self.internal_separator()
            ))
    }

    /// 判断是否为心跳主题
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 如果是心跳主题返回true，否则返回false
    pub fn is_heartbeat_topic(&self, topic: &str) -> bool {
        topic.ends_with(&format!(
            "{}heartbeats{}",
            self.internal_separator(),
            self.internal_separator()
        ))
    }
}

impl Default for DefaultReplicationPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationPolicy for DefaultReplicationPolicy {
    fn format_remote_topic(&self, topic: String) -> String {
        // 在远程主题名称前添加源集群别名
        // 注意：这里使用默认的"source"，实际使用时应该传入正确的源集群别名
        format!("{}{}{}", "source", self.separator, topic)
    }

    fn topic_source(&self, topic: String) -> String {
        // 从主题名称中提取源集群别名
        if let Some(pos) = topic.find(&self.separator) {
            topic[..pos].to_string()
        } else {
            String::new()
        }
    }

    fn upstream_topic(&self, topic: String) -> String {
        // 从主题名称中提取上游主题名称
        if let Some(pos) = topic.find(&self.separator) {
            topic[pos + self.separator.len()..].to_string()
        } else {
            topic
        }
    }

    fn original_topic(&self, topic: String) -> String {
        // 获取原始主题名称
        self.upstream_topic(topic)
    }

    fn heartbeats_topic(&self) -> String {
        format!("heartbeats{}", self.internal_separator())
    }

    fn offset_syncs_topic(&self) -> String {
        format!("offset-syncs{}", self.internal_separator())
    }

    fn checkpoints_topic(&self) -> String {
        format!("checkpoints{}", self.internal_separator())
    }

    fn is_internal_topic(&self, topic: String) -> bool {
        self.is_mm2_internal_topic(&topic)
    }
}

/// IdentityReplicationPolicy - 身份复制策略
///
/// 不重命名远程主题，保持原始主题名称
pub struct IdentityReplicationPolicy {
    /// 源集群别名（可选）
    source_cluster_alias: Option<String>,
    /// 内部使用的DefaultReplicationPolicy
    default_policy: DefaultReplicationPolicy,
}

impl IdentityReplicationPolicy {
    /// 创建新的身份复制策略
    ///
    /// # 返回
    /// 新的IdentityReplicationPolicy实例
    pub fn new() -> Self {
        IdentityReplicationPolicy {
            source_cluster_alias: None,
            default_policy: DefaultReplicationPolicy::new(),
        }
    }

    /// 配置复制策略
    ///
    /// # 参数
    /// - `props`: 配置属性
    pub fn configure(&mut self, props: &std::collections::HashMap<String, String>) {
        self.default_policy.configure(props);
        if let Some(alias) = props.get("source.cluster.alias") {
            self.source_cluster_alias = Some(alias.clone());
        }
    }

    /// 判断主题是否看起来像心跳主题
    ///
    /// # 参数
    /// - `topic`: 主题名称
    ///
    /// # 返回
    /// 如果看起来像心跳主题返回true，否则返回false
    fn looks_like_heartbeat(&self, topic: &str) -> bool {
        topic.ends_with(&self.default_policy.heartbeats_topic())
    }
}

impl Default for IdentityReplicationPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationPolicy for IdentityReplicationPolicy {
    fn format_remote_topic(&self, topic: String) -> String {
        // 不重命名远程主题，除非看起来像心跳主题
        if self.looks_like_heartbeat(&topic) {
            self.default_policy.format_remote_topic(topic)
        } else {
            topic
        }
    }

    fn topic_source(&self, topic: String) -> String {
        // 如果不是心跳主题，返回配置的源集群别名
        if !self.looks_like_heartbeat(&topic) {
            if let Some(ref alias) = self.source_cluster_alias {
                return alias.clone();
            }
        }
        // 对于心跳主题，使用默认策略
        self.default_policy.topic_source(topic)
    }

    fn upstream_topic(&self, topic: String) -> String {
        // 如果不是心跳主题，返回原始主题名称
        if !self.looks_like_heartbeat(&topic) {
            return topic;
        }
        // 对于心跳主题，使用默认策略
        self.default_policy.upstream_topic(topic)
    }

    fn original_topic(&self, topic: String) -> String {
        // 返回原始主题名称
        if !self.looks_like_heartbeat(&topic) {
            return topic;
        }
        self.default_policy.original_topic(topic)
    }

    fn heartbeats_topic(&self) -> String {
        self.default_policy.heartbeats_topic()
    }

    fn offset_syncs_topic(&self) -> String {
        self.default_policy.offset_syncs_topic()
    }

    fn checkpoints_topic(&self) -> String {
        self.default_policy.checkpoints_topic()
    }

    fn is_internal_topic(&self, topic: String) -> bool {
        self.default_policy.is_internal_topic(topic)
    }
}
