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

//! Configuration required for MirrorClient to talk to a given target cluster.
//!
//! This needs to contain at least the connection details for the target cluster
//! (bootstrap.servers and any required TLS/SASL configuration), as well as
//! REPLICATION_POLICY_CLASS when not using the default replication policy.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorClientConfig

use std::collections::HashMap;

use super::DefaultReplicationPolicy;
use super::ReplicationPolicy;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java MirrorClientConfig
// ============================================================================

/// Configuration key for replication policy class.
/// Corresponds to Java: MirrorClientConfig.REPLICATION_POLICY_CLASS
pub const REPLICATION_POLICY_CLASS: &str = "replication.policy.class";

/// Default value for replication policy class.
/// Corresponds to Java: MirrorClientConfig.REPLICATION_POLICY_CLASS_DEFAULT
/// Default is DefaultReplicationPolicy.
pub const REPLICATION_POLICY_CLASS_DEFAULT: &str =
    "org.apache.kafka.connect.mirror.DefaultReplicationPolicy";

/// Configuration key for replication policy separator.
/// Corresponds to Java: MirrorClientConfig.REPLICATION_POLICY_SEPARATOR
pub const REPLICATION_POLICY_SEPARATOR: &str = "replication.policy.separator";

/// Default value for replication policy separator.
/// Corresponds to Java: MirrorClientConfig.REPLICATION_POLICY_SEPARATOR_DEFAULT
/// References DefaultReplicationPolicy.SEPARATOR_DEFAULT which is "."
pub const REPLICATION_POLICY_SEPARATOR_DEFAULT: &str = ".";

/// Configuration key for internal topic separator enabled.
/// Corresponds to Java: MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED
pub const INTERNAL_TOPIC_SEPARATOR_ENABLED: &str =
    "replication.policy.internal.topic.separator.enabled";

/// Default value for internal topic separator enabled.
/// Corresponds to Java: MirrorClientConfig.INTERNAL_TOPIC_SEPARATOR_ENABLED_DEFAULT
/// References DefaultReplicationPolicy.INTERNAL_TOPIC_SEPARATOR_ENABLED_DEFAULT which is true
pub const INTERNAL_TOPIC_SEPARATOR_ENABLED_DEFAULT: bool = true;

/// Configuration key for forwarding admin class.
/// Corresponds to Java: MirrorClientConfig.FORWARDING_ADMIN_CLASS
pub const FORWARDING_ADMIN_CLASS: &str = "forwarding.admin.class";

/// Default value for forwarding admin class.
/// Corresponds to Java: MirrorClientConfig.FORWARDING_ADMIN_CLASS_DEFAULT
/// Default is ForwardingAdmin.
pub const FORWARDING_ADMIN_CLASS_DEFAULT: &str = "org.apache.kafka.clients.admin.ForwardingAdmin";

/// Prefix for admin client configuration properties.
/// Corresponds to Java: MirrorClientConfig.ADMIN_CLIENT_PREFIX
pub const ADMIN_CLIENT_PREFIX: &str = "admin.";

/// Prefix for consumer client configuration properties.
/// Corresponds to Java: MirrorClientConfig.CONSUMER_CLIENT_PREFIX
pub const CONSUMER_CLIENT_PREFIX: &str = "consumer.";

/// Prefix for producer client configuration properties.
/// Corresponds to Java: MirrorClientConfig.PRODUCER_CLIENT_PREFIX
pub const PRODUCER_CLIENT_PREFIX: &str = "producer.";

// ============================================================================
// ForwardingAdmin - Basic implementation for MirrorMaker
// ============================================================================

/// ForwardingAdmin is the default value of forwarding.admin.class in MirrorMaker.
/// Users who wish to customize the MirrorMaker behaviour for the creation of topics
/// and access control lists can extend this class.
///
/// Corresponds to Java: org.apache.kafka.clients.admin.ForwardingAdmin
/// The class must have a constructor with signature (Map<String, Object> config)
/// for configuring a KafkaAdminClient and any other clients needed for external
/// resource management.
pub struct ForwardingAdmin {
    config: HashMap<String, String>,
}

impl ForwardingAdmin {
    /// Creates a new ForwardingAdmin with the given configuration.
    ///
    /// Corresponds to Java: ForwardingAdmin(Map<String, Object> configs)
    pub fn new(config: HashMap<String, String>) -> Self {
        ForwardingAdmin { config }
    }

    /// Returns the configuration used by this ForwardingAdmin.
    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Returns the class name for this ForwardingAdmin implementation.
    pub fn class_name() -> &'static str {
        FORWARDING_ADMIN_CLASS_DEFAULT
    }
}

impl Default for ForwardingAdmin {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

// ============================================================================
// MirrorClientConfig
// ============================================================================

/// Configuration required for MirrorClient to talk to a given target cluster.
///
/// This corresponds to Java: org.apache.kafka.connect.mirror.MirrorClientConfig
/// which extends AbstractConfig.
///
/// In Rust, we use a struct containing a HashMap of configuration properties
/// since Rust doesn't support class inheritance.
#[derive(Debug, Clone)]
pub struct MirrorClientConfig {
    originals: HashMap<String, String>,
}

impl MirrorClientConfig {
    /// Creates a new MirrorClientConfig with the given properties.
    ///
    /// Corresponds to Java: MirrorClientConfig(Map<?, ?> props)
    pub fn new(props: HashMap<String, String>) -> Self {
        MirrorClientConfig { originals: props }
    }

    /// Returns a ReplicationPolicy instance based on the configuration.
    ///
    /// The replication policy class is determined by the REPLICATION_POLICY_CLASS
    /// configuration property. If not specified, DefaultReplicationPolicy is used.
    ///
    /// Corresponds to Java: MirrorClientConfig.replicationPolicy()
    pub fn replication_policy(&self) -> Box<dyn ReplicationPolicy> {
        let class_name = self
            .originals
            .get(REPLICATION_POLICY_CLASS)
            .map(|s| s.as_str())
            .unwrap_or(REPLICATION_POLICY_CLASS_DEFAULT);

        // Create the appropriate replication policy based on class name
        if class_name.contains("DefaultReplicationPolicy")
            || class_name == REPLICATION_POLICY_CLASS_DEFAULT
        {
            let mut policy = DefaultReplicationPolicy::new();
            // Configure the policy with the relevant properties
            policy.configure(&self.originals);
            Box::new(policy)
        } else if class_name.contains("IdentityReplicationPolicy") {
            use super::IdentityReplicationPolicy;
            let mut policy = IdentityReplicationPolicy::new();
            policy.configure(&self.originals);
            Box::new(policy)
        } else {
            // Default to DefaultReplicationPolicy for unknown class names
            let mut policy = DefaultReplicationPolicy::new();
            policy.configure(&self.originals);
            Box::new(policy)
        }
    }

    /// Returns a ForwardingAdmin instance based on the configuration.
    ///
    /// The forwarding admin class is determined by the FORWARDING_ADMIN_CLASS
    /// configuration property. If not specified, ForwardingAdmin is used.
    ///
    /// Corresponds to Java: MirrorClientConfig.forwardingAdmin(Map<String, Object> config)
    pub fn forwarding_admin(&self) -> ForwardingAdmin {
        // Use admin configuration for ForwardingAdmin
        let admin_config = self.admin_config();
        ForwardingAdmin::new(admin_config)
    }

    /// Returns sub-configuration for Admin clients.
    ///
    /// This extracts all properties with the "admin." prefix and removes
    /// the prefix from the keys.
    ///
    /// Corresponds to Java: MirrorClientConfig.adminConfig()
    pub fn admin_config(&self) -> HashMap<String, String> {
        self.client_config(ADMIN_CLIENT_PREFIX)
    }

    /// Returns sub-configuration for Consumer clients.
    ///
    /// This extracts all properties with the "consumer." prefix and removes
    /// the prefix from the keys.
    ///
    /// Corresponds to Java: MirrorClientConfig.consumerConfig()
    pub fn consumer_config(&self) -> HashMap<String, String> {
        self.client_config(CONSUMER_CLIENT_PREFIX)
    }

    /// Returns sub-configuration for Producer clients.
    ///
    /// This extracts all properties with the "producer." prefix and removes
    /// the prefix from the keys.
    ///
    /// Corresponds to Java: MirrorClientConfig.producerConfig()
    pub fn producer_config(&self) -> HashMap<String, String> {
        self.client_config(PRODUCER_CLIENT_PREFIX)
    }

    /// Extracts configuration properties with the given prefix.
    ///
    /// Properties matching the prefix are extracted and the prefix is removed
    /// from the key names. Properties with null values are removed.
    ///
    /// Corresponds to Java: MirrorClientConfig.clientConfig(String prefix)
    fn client_config(&self, prefix: &str) -> HashMap<String, String> {
        let mut props = HashMap::new();

        for (key, value) in &self.originals {
            if key.starts_with(prefix) {
                // Remove the prefix from the key
                let stripped_key = key[prefix.len()..].to_string();
                // Skip empty keys and null-like values
                if !stripped_key.is_empty() && !value.is_empty() {
                    props.insert(stripped_key, value.clone());
                }
            }
        }

        // Also include properties without any prefix (common client properties)
        // These are properties like bootstrap.servers, security.protocol, etc.
        // But prefix-specific properties take precedence over common properties
        let common_client_props = [
            "bootstrap.servers",
            "security.protocol",
            "sasl.mechanism",
            "sasl.jaas.config",
            "ssl.truststore.location",
            "ssl.truststore.password",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.key.password",
            "client.id",
        ];

        for prop in common_client_props {
            // Only add common property if prefix-specific one doesn't exist
            if !props.contains_key(prop) {
                if let Some(value) = self.originals.get(prop) {
                    if !value.is_empty() {
                        props.insert(prop.to_string(), value.clone());
                    }
                }
            }
        }

        props
    }

    /// Returns the raw configuration value for the given key.
    ///
    /// Corresponds to Java: AbstractConfig.get(String key)
    pub fn get(&self, key: &str) -> Option<&String> {
        self.originals.get(key)
    }

    /// Returns all original configuration values.
    ///
    /// Corresponds to Java: AbstractConfig.originals()
    pub fn originals(&self) -> &HashMap<String, String> {
        &self.originals
    }

    /// Returns the string value for the given key, or default if not present.
    pub fn get_string(&self, key: &str, default: &str) -> String {
        self.originals
            .get(key)
            .map(|s| s.clone())
            .unwrap_or(default.to_string())
    }

    /// Returns the boolean value for the given key, or default if not present.
    pub fn get_bool(&self, key: &str, default: bool) -> bool {
        self.originals
            .get(key)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(default)
    }
}

impl Default for MirrorClientConfig {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}
