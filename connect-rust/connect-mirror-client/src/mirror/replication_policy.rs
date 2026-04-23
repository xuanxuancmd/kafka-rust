/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;

/// ReplicationPolicy trait - used by MirrorMaker connectors to manage topic names.
/// Corresponds to Java: org.apache.kafka.connect.mirror.ReplicationPolicy
pub trait ReplicationPolicy: Send + Sync {
    /// Returns the remote topic name for the given topic and source cluster alias.
    fn format_remote_topic(&self, source_cluster_alias: &str, topic: &str) -> String;

    /// Returns the source cluster alias of given topic.
    /// Returns None if the given topic is not a remote topic.
    fn topic_source(&self, topic: &str) -> Option<String>;

    /// Returns the name of the given topic on the source cluster.
    /// Returns None if the given topic is not a remote topic.
    fn upstream_topic(&self, topic: &str) -> Option<String>;

    /// Returns the name of the original topic, which may have been replicated multiple hops.
    /// Returns the topic if it is not a remote topic.
    fn original_topic(&self, topic: &str) -> String {
        match self.upstream_topic(topic) {
            Some(upstream) if upstream != topic => self.original_topic(&upstream),
            _ => topic.to_string(),
        }
    }

    /// Returns the name of heartbeats topic.
    fn heartbeats_topic(&self) -> String {
        "heartbeats".to_string()
    }

    /// Returns the name of the offset-syncs topic for given cluster alias.
    fn offset_syncs_topic(&self, cluster_alias: &str) -> String {
        format!("mm2-offset-syncs.{}.internal", cluster_alias)
    }

    /// Returns the name of the checkpoints topic for given cluster alias.
    fn checkpoints_topic(&self, cluster_alias: &str) -> String {
        format!("{}.checkpoints.internal", cluster_alias)
    }

    /// Returns true if the topic is a heartbeats topic.
    fn is_heartbeats_topic(&self, topic: &str) -> bool {
        self.heartbeats_topic() == self.original_topic(topic)
    }

    /// Returns true if the topic is a checkpoints topic.
    fn is_checkpoints_topic(&self, topic: &str) -> bool {
        topic.ends_with(".checkpoints.internal")
    }

    /// Returns true if the topic is one of MirrorMaker internal topics.
    fn is_mm2_internal_topic(&self, topic: &str) -> bool {
        (topic.starts_with("mm2") && topic.ends_with(".internal"))
            || self.is_checkpoints_topic(topic)
    }

    /// Returns true if the topic is considered an internal topic.
    fn is_internal_topic(&self, topic: &str) -> bool {
        let is_kafka_internal_topic = topic.starts_with("__") || topic.starts_with(".");
        self.is_mm2_internal_topic(topic) || is_kafka_internal_topic
    }

    /// Configure this policy with the given properties.
    fn configure(&mut self, props: &HashMap<String, String>);
}

/// Default implementation of ReplicationPolicy which prepends the source cluster alias to remote topic names.
/// Corresponds to Java: org.apache.kafka.connect.mirror.DefaultReplicationPolicy
pub struct DefaultReplicationPolicy {
    separator: String,
    separator_pattern: regex::Regex,
    is_internal_topic_separator_enabled: bool,
}

pub const SEPARATOR_CONFIG: &str = "replication.policy.separator";
pub const SEPARATOR_DEFAULT: &str = ".";
pub const INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG: &str = "internal.topic.separator.enabled";
pub const INTERNAL_TOPIC_SEPARATOR_ENABLED_DEFAULT: bool = true;

impl DefaultReplicationPolicy {
    pub fn new() -> Self {
        Self {
            separator: SEPARATOR_DEFAULT.to_string(),
            separator_pattern: regex::Regex::new(regex::escape(SEPARATOR_DEFAULT).as_str())
                .unwrap(),
            is_internal_topic_separator_enabled: INTERNAL_TOPIC_SEPARATOR_ENABLED_DEFAULT,
        }
    }

    fn internal_separator(&self) -> &str {
        if self.is_internal_topic_separator_enabled {
            &self.separator
        } else {
            "."
        }
    }

    fn internal_suffix(&self) -> String {
        format!("{}internal", self.internal_separator())
    }

    fn checkpoints_topic_suffix(&self) -> String {
        format!(
            "{}checkpoints{}",
            self.internal_separator(),
            self.internal_suffix()
        )
    }
}

impl Default for DefaultReplicationPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationPolicy for DefaultReplicationPolicy {
    fn format_remote_topic(&self, source_cluster_alias: &str, topic: &str) -> String {
        format!("{}{}{}", source_cluster_alias, self.separator, topic)
    }

    fn topic_source(&self, topic: &str) -> Option<String> {
        let parts: Vec<&str> = self.separator_pattern.split(topic).collect();
        if parts.len() < 2 {
            None
        } else {
            Some(parts[0].to_string())
        }
    }

    fn upstream_topic(&self, topic: &str) -> Option<String> {
        match self.topic_source(topic) {
            Some(source) => Some(topic[source.len() + self.separator.len()..].to_string()),
            None => None,
        }
    }

    fn offset_syncs_topic(&self, cluster_alias: &str) -> String {
        format!(
            "mm2-offset-syncs{}{}{}",
            self.internal_separator(),
            cluster_alias,
            self.internal_suffix()
        )
    }

    fn checkpoints_topic(&self, cluster_alias: &str) -> String {
        format!("{}{}", cluster_alias, self.checkpoints_topic_suffix())
    }

    fn is_checkpoints_topic(&self, topic: &str) -> bool {
        topic.ends_with(&self.checkpoints_topic_suffix())
    }

    fn is_mm2_internal_topic(&self, topic: &str) -> bool {
        (topic.starts_with("mm2") && topic.ends_with(&self.internal_suffix()))
            || self.is_checkpoints_topic(topic)
    }

    fn configure(&mut self, props: &HashMap<String, String>) {
        if let Some(sep) = props.get(SEPARATOR_CONFIG) {
            println!("INFO: Using custom remote topic separator: '{}'", sep);
            self.separator = sep.clone();
            self.separator_pattern = regex::Regex::new(regex::escape(sep).as_str()).unwrap();
        }

        // Always read INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG if present
        if let Some(enabled) = props.get(INTERNAL_TOPIC_SEPARATOR_ENABLED_CONFIG) {
            self.is_internal_topic_separator_enabled = enabled
                .parse()
                .unwrap_or(INTERNAL_TOPIC_SEPARATOR_ENABLED_DEFAULT);
            if !self.is_internal_topic_separator_enabled {
                println!("WARN: Disabling custom topic separator for internal topics; will use '.' instead of '{}'", self.separator);
            }
        }
    }
}

/// Alternative implementation of ReplicationPolicy that does not rename remote topics.
/// Corresponds to Java: org.apache.kafka.connect.mirror.IdentityReplicationPolicy
pub struct IdentityReplicationPolicy {
    default_policy: DefaultReplicationPolicy,
    source_cluster_alias: Option<String>,
}

pub const SOURCE_CLUSTER_ALIAS_CONFIG: &str = "source.cluster.alias";

impl IdentityReplicationPolicy {
    pub fn new() -> Self {
        Self {
            default_policy: DefaultReplicationPolicy::new(),
            source_cluster_alias: None,
        }
    }

    fn looks_like_heartbeat(&self, topic: &str) -> bool {
        topic.ends_with(&self.heartbeats_topic())
    }
}

impl Default for IdentityReplicationPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationPolicy for IdentityReplicationPolicy {
    fn format_remote_topic(&self, source_cluster_alias: &str, topic: &str) -> String {
        if self.looks_like_heartbeat(topic) {
            self.default_policy
                .format_remote_topic(source_cluster_alias, topic)
        } else {
            topic.to_string()
        }
    }

    fn topic_source(&self, topic: &str) -> Option<String> {
        if self.looks_like_heartbeat(topic) {
            self.default_policy.topic_source(topic)
        } else {
            self.source_cluster_alias.clone()
        }
    }

    fn upstream_topic(&self, topic: &str) -> Option<String> {
        if self.looks_like_heartbeat(topic) {
            self.default_policy.upstream_topic(topic)
        } else {
            Some(topic.to_string())
        }
    }

    fn configure(&mut self, props: &HashMap<String, String>) {
        self.default_policy.configure(props);
        if let Some(alias) = props.get(SOURCE_CLUSTER_ALIAS_CONFIG) {
            println!("INFO: Using source cluster alias `{}`.", alias);
            self.source_cluster_alias = Some(alias.clone());
        }
    }
}
