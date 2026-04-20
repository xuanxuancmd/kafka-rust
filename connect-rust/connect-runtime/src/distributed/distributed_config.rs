// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Distributed configuration for Kafka Connect workers.
//!
//! This module provides the DistributedConfig class that defines configuration
//! options specific to distributed mode Kafka Connect workers.
//!
//! Corresponds to Java: org.apache.kafka.connect.runtime.distributed.DistributedConfig

use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType};
use common_trait::errors::ConfigException;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::config::worker_config::WorkerConfig;
use crate::distributed::connect_protocol::ConnectProtocolCompatibility;
use crate::distributed::crypto::{
    system_crypto, Crypto, CryptoError, KeyGenerator, MacInstance, SystemCrypto,
};

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java DistributedConfig
// ============================================================================

/// <code>group.id</code>
/// Corresponds to Java: DistributedConfig.GROUP_ID_CONFIG
pub const GROUP_ID_CONFIG: &str = "group.id";
const GROUP_ID_DOC: &str =
    "A unique string that identifies the Connect cluster group this worker belongs to.";

/// <code>session.timeout.ms</code>
/// Corresponds to Java: DistributedConfig.SESSION_TIMEOUT_MS_CONFIG
pub const SESSION_TIMEOUT_MS_CONFIG: &str = "session.timeout.ms";
const SESSION_TIMEOUT_MS_DOC: &str = "The timeout used to detect worker failures. \
    The worker sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are \
    received by the broker before the expiration of this session timeout, then the broker will remove the \
    worker from the group and initiate a rebalance. Note that the value must be in the allowable range as \
    configured in the broker configuration by <code>group.min.session.timeout.ms</code> \
    and <code>group.max.session.timeout.ms</code.";

/// Default value for session timeout in milliseconds.
/// Corresponds to Java: 10 seconds (10000 ms)
pub const SESSION_TIMEOUT_MS_DEFAULT: i64 = 10_000;

/// <code>heartbeat.interval.ms</code>
/// Corresponds to Java: DistributedConfig.HEARTBEAT_INTERVAL_MS_CONFIG
pub const HEARTBEAT_INTERVAL_MS_CONFIG: &str = "heartbeat.interval.ms";
const HEARTBEAT_INTERVAL_MS_DOC: &str = "The expected time between heartbeats to the group \
    coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the \
    worker's session stays active and to facilitate rebalancing when new members join or leave the group. \
    The value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher \
    than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.";

/// Default value for heartbeat interval in milliseconds.
/// Corresponds to Java: 3 seconds (3000 ms)
pub const HEARTBEAT_INTERVAL_MS_DEFAULT: i64 = 3_000;

/// <code>rebalance.timeout.ms</code>
/// Corresponds to Java: DistributedConfig.REBALANCE_TIMEOUT_MS_CONFIG
pub const REBALANCE_TIMEOUT_MS_CONFIG: &str = "rebalance.timeout.ms";
const REBALANCE_TIMEOUT_MS_DOC: &str = "The maximum allowed time for each worker to join the group \
    once a rebalance has begun. This is basically a limit for the amount of time needed for all tasks to \
    flush any pending data and commit offsets before leaving the group. If the timeout is exceeded, \
    the worker will be removed from the group, which will cause offset commits to fail.";

/// Default value for rebalance timeout in milliseconds.
/// Corresponds to Java: 1 minute (60000 ms)
pub const REBALANCE_TIMEOUT_MS_DEFAULT: i64 = 60_000;

/// <code>metadata.recovery.strategy</code>
/// Corresponds to Java: DistributedConfig.METADATA_RECOVERY_STRATEGY_CONFIG
pub const METADATA_RECOVERY_STRATEGY_CONFIG: &str = "metadata.recovery.strategy";
const METADATA_RECOVERY_STRATEGY_DOC: &str = "Strategy to recover from metadata errors.";

/// Default value for metadata recovery strategy.
pub const DEFAULT_METADATA_RECOVERY_STRATEGY: &str = "none";

/// <code>metadata.recovery.rebootstrap.trigger.ms</code>
/// Corresponds to Java: DistributedConfig.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG
pub const METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG: &str =
    "metadata.recovery.rebootstrap.trigger.ms";
const METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC: &str =
    "The time in milliseconds after a metadata error is detected \
    before triggering a rebootstrap attempt for metadata recovery.";

/// Default value for metadata recovery rebootstrap trigger.
pub const DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS: i64 = 60_000;

/// <code>worker.sync.timeout.ms</code>
/// Corresponds to Java: DistributedConfig.WORKER_SYNC_TIMEOUT_MS_CONFIG
pub const WORKER_SYNC_TIMEOUT_MS_CONFIG: &str = "worker.sync.timeout.ms";
const WORKER_SYNC_TIMEOUT_MS_DOC: &str = "When the worker is out of sync with other workers and needs \
    to resynchronize configurations, wait up to this amount of time before giving up, leaving the group, and \
    waiting a backoff period before rejoining.";

/// Default value for worker sync timeout in milliseconds.
/// Corresponds to Java: 3000 ms
pub const WORKER_SYNC_TIMEOUT_MS_DEFAULT: i64 = 3_000;

/// <code>worker.unsync.backoff.ms</code>
/// Corresponds to Java: DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_CONFIG
pub const WORKER_UNSYNC_BACKOFF_MS_CONFIG: &str = "worker.unsync.backoff.ms";
const WORKER_UNSYNC_BACKOFF_MS_DOC: &str = "When the worker is out of sync with other workers and \
    fails to catch up within the <code>worker.sync.timeout.ms</code>, leave the Connect cluster for this long before rejoining.";

/// Default value for worker unsync backoff in milliseconds.
/// Corresponds to Java: 5 minutes (300000 ms)
pub const WORKER_UNSYNC_BACKOFF_MS_DEFAULT: i64 = 300_000;

/// Storage prefix constants
/// Corresponds to Java: DistributedConfig.CONFIG_STORAGE_PREFIX etc.
pub const CONFIG_STORAGE_PREFIX: &str = "config.storage.";
pub const OFFSET_STORAGE_PREFIX: &str = "offset.storage.";
pub const STATUS_STORAGE_PREFIX: &str = "status.storage.";
pub const TOPIC_SUFFIX: &str = "topic";
pub const PARTITIONS_SUFFIX: &str = "partitions";
pub const REPLICATION_FACTOR_SUFFIX: &str = "replication.factor";

/// <code>offset.storage.topic</code>
/// Corresponds to Java: DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG
pub const OFFSET_STORAGE_TOPIC_CONFIG: &str = "offset.storage.topic";
const OFFSET_STORAGE_TOPIC_CONFIG_DOC: &str =
    "The name of the Kafka topic where source connector offsets are stored";

/// <code>offset.storage.partitions</code>
/// Corresponds to Java: DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG
pub const OFFSET_STORAGE_PARTITIONS_CONFIG: &str = "offset.storage.partitions";
const OFFSET_STORAGE_PARTITIONS_CONFIG_DOC: &str =
    "The number of partitions used when creating the offset storage topic";

/// Default value for offset storage partitions.
/// Corresponds to Java: 25
pub const OFFSET_STORAGE_PARTITIONS_DEFAULT: i32 = 25;

/// <code>offset.storage.replication.factor</code>
/// Corresponds to Java: DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG
pub const OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG: &str = "offset.storage.replication.factor";
const OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG_DOC: &str =
    "Replication factor used when creating the offset storage topic";

/// Default value for offset storage replication factor.
/// Corresponds to Java: 3
pub const OFFSET_STORAGE_REPLICATION_FACTOR_DEFAULT: i16 = 3;

/// <code>config.storage.topic</code>
/// Corresponds to Java: DistributedConfig.CONFIG_TOPIC_CONFIG
pub const CONFIG_TOPIC_CONFIG: &str = "config.storage.topic";
const CONFIG_TOPIC_CONFIG_DOC: &str =
    "The name of the Kafka topic where connector configurations are stored";

/// <code>config.storage.replication.factor</code>
/// Corresponds to Java: DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG
pub const CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG: &str = "config.storage.replication.factor";
const CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG_DOC: &str =
    "Replication factor used when creating the configuration storage topic";

/// Default value for config storage replication factor.
/// Corresponds to Java: 3
pub const CONFIG_STORAGE_REPLICATION_FACTOR_DEFAULT: i16 = 3;

/// <code>status.storage.topic</code>
/// Corresponds to Java: DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG
pub const STATUS_STORAGE_TOPIC_CONFIG: &str = "status.storage.topic";
const STATUS_STORAGE_TOPIC_CONFIG_DOC: &str =
    "The name of the Kafka topic where connector and task status are stored";

/// <code>status.storage.partitions</code>
/// Corresponds to Java: DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG
pub const STATUS_STORAGE_PARTITIONS_CONFIG: &str = "status.storage.partitions";
const STATUS_STORAGE_PARTITIONS_CONFIG_DOC: &str =
    "The number of partitions used when creating the status storage topic";

/// Default value for status storage partitions.
/// Corresponds to Java: 5
pub const STATUS_STORAGE_PARTITIONS_DEFAULT: i32 = 5;

/// <code>status.storage.replication.factor</code>
/// Corresponds to Java: DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG
pub const STATUS_STORAGE_REPLICATION_FACTOR_CONFIG: &str = "status.storage.replication.factor";
const STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DOC: &str =
    "Replication factor used when creating the status storage topic";

/// Default value for status storage replication factor.
/// Corresponds to Java: 3
pub const STATUS_STORAGE_REPLICATION_FACTOR_DEFAULT: i16 = 3;

/// <code>connect.protocol</code>
/// Corresponds to Java: DistributedConfig.CONNECT_PROTOCOL_CONFIG
pub const CONNECT_PROTOCOL_CONFIG: &str = "connect.protocol";
const CONNECT_PROTOCOL_DOC: &str = "Compatibility mode for Kafka Connect Protocol";

/// Default value for connect protocol.
/// Corresponds to Java: "sessioned"
pub const CONNECT_PROTOCOL_DEFAULT: &str = "sessioned";

/// <code>scheduled.rebalance.max.delay.ms</code>
/// Corresponds to Java: DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG
pub const SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG: &str = "scheduled.rebalance.max.delay.ms";
const SCHEDULED_REBALANCE_MAX_DELAY_MS_DOC: &str = "The maximum delay that is \
    scheduled in order to wait for the return of one or more departed workers before \
    rebalancing and reassigning their connectors and tasks to the group. During this \
    period the connectors and tasks of the departed workers remain unassigned";

/// Default value for scheduled rebalance max delay in milliseconds.
/// Corresponds to Java: 5 minutes (300000 ms)
pub const SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT: i64 = 300_000;

/// Inter-worker key generation algorithm config.
/// Corresponds to Java: DistributedConfig.INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG
pub const INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG: &str =
    "inter.worker.key.generation.algorithm";
const INTER_WORKER_KEY_GENERATION_ALGORITHM_DOC: &str = "The algorithm to use for generating internal request keys. \
    The algorithm 'HmacSHA256' will be used as a default on JVMs that support it; \
    on other JVMs, no default is used and a value for this property must be manually specified in the worker config.";

/// Default value for inter-worker key generation algorithm.
pub const INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT: &str = "HmacSHA256";

/// Inter-worker key size config.
/// Corresponds to Java: DistributedConfig.INTER_WORKER_KEY_SIZE_CONFIG
pub const INTER_WORKER_KEY_SIZE_CONFIG: &str = "inter.worker.key.size";
const INTER_WORKER_KEY_SIZE_DOC: &str =
    "The size of the key to use for signing internal requests, in bits. \
    If null, the default key size for the key generation algorithm will be used.";

/// Inter-worker key TTL config.
/// Corresponds to Java: DistributedConfig.INTER_WORKER_KEY_TTL_MS_CONFIG
pub const INTER_WORKER_KEY_TTL_MS_CONFIG: &str = "inter.worker.key.ttl.ms";
const INTER_WORKER_KEY_TTL_MS_DOC: &str = "The TTL of generated session keys used for \
    internal request validation (in milliseconds)";

/// Default value for inter-worker key TTL in milliseconds.
/// Corresponds to Java: 1 hour (3600000 ms)
pub const INTER_WORKER_KEY_TTL_MS_DEFAULT: i64 = 3_600_000;

/// Inter-worker signature algorithm config.
/// Corresponds to Java: DistributedConfig.INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG
pub const INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG: &str = "inter.worker.signature.algorithm";
const INTER_WORKER_SIGNATURE_ALGORITHM_DOC: &str = "The algorithm used to sign internal requests. \
    The algorithm 'HmacSHA256' will be used as a default on JVMs that support it; \
    on other JVMs, no default is used and a value for this property must be manually specified in the worker config.";

/// Default value for inter-worker signature algorithm.
pub const INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT: &str = "HmacSHA256";

/// Inter-worker verification algorithms config.
/// Corresponds to Java: DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG
pub const INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG: &str =
    "inter.worker.verification.algorithms";
const INTER_WORKER_VERIFICATION_ALGORITHMS_DOC: &str = "A list of permitted algorithms for verifying internal requests, \
    which must include the algorithm used for the <code>inter.worker.signature.algorithm</code> property. \
    The algorithm(s) 'HmacSHA256' will be used as a default on JVMs that provide them; \
    on other JVMs, no default is used and a value for this property must be manually specified in the worker config.";

/// Exactly-once source support config.
/// Corresponds to Java: DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG
pub const EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG: &str = "exactly.once.source.support";
const EXACTLY_ONCE_SOURCE_SUPPORT_DOC: &str = "Whether to enable exactly-once support for source connectors in the cluster \
    by using transactions to write source records and their source offsets, and by proactively fencing out old task generations before bringing up new ones.\n\
    To enable exactly-once source support on a new cluster, set this property to 'enabled'. \
    To enable support on an existing cluster, first set to 'preparing' on every worker in the cluster, \
    then set to 'enabled'. A rolling upgrade may be used for both changes.";

/// Default value for exactly-once source support.
pub const EXACTLY_ONCE_SOURCE_SUPPORT_DEFAULT: &str = "disabled";

// Common client configs constants (referenced from CommonClientConfigs in Java)
pub const METADATA_MAX_AGE_CONFIG: &str = "metadata.max.age.ms";
const METADATA_MAX_AGE_DOC: &str = "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes.";
pub const METADATA_MAX_AGE_DEFAULT: i64 = 300_000; // 5 minutes

pub const CLIENT_ID_CONFIG: &str = "client.id";
const CLIENT_ID_DOC: &str = "An id string to pass to the server when making requests. The server can use this for logging, metrics, etc.";

pub const SEND_BUFFER_CONFIG: &str = "send.buffer.bytes";
const SEND_BUFFER_DOC: &str =
    "The size of the TCP send buffer (SO_SNDBUF) to use when sending data.";
pub const SEND_BUFFER_DEFAULT: i32 = 128 * 1024; // 128KB
pub const SEND_BUFFER_LOWER_BOUND: i64 = -1;

pub const RECEIVE_BUFFER_CONFIG: &str = "receive.buffer.bytes";
const RECEIVE_BUFFER_DOC: &str =
    "The size of the TCP receive buffer (SO_RCVBUF) to use when reading data.";
pub const RECEIVE_BUFFER_DEFAULT: i32 = 32 * 1024; // 32KB
pub const RECEIVE_BUFFER_LOWER_BOUND: i64 = -1;

pub const RECONNECT_BACKOFF_MS_CONFIG: &str = "reconnect.backoff.ms";
const RECONNECT_BACKOFF_MS_DOC: &str =
    "The amount of time in milliseconds to wait before attempting to reconnect to a given host.";
pub const RECONNECT_BACKOFF_MS_DEFAULT: i64 = 50;

pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &str = "reconnect.backoff.max.ms";
const RECONNECT_BACKOFF_MAX_MS_DOC: &str = "The maximum amount of time in milliseconds to wait before attempting to reconnect to a given host.";
pub const RECONNECT_BACKOFF_MAX_MS_DEFAULT: i64 = 1_000; // 1 second

pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &str = "socket.connection.setup.timeout.ms";
const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC: &str =
    "The amount of time in milliseconds to wait for a socket connection to be established.";
pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS: i64 = 10_000; // 10 seconds

pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: &str =
    "socket.connection.setup.timeout.max.ms";
const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC: &str =
    "The maximum amount of time in milliseconds to wait for a socket connection to be established.";
pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS: i64 = 127_000; // ~127 seconds

pub const RETRY_BACKOFF_MS_CONFIG: &str = "retry.backoff.ms";
const RETRY_BACKOFF_MS_DOC: &str =
    "The amount of time in milliseconds to wait before attempting to retry a failed request.";
pub const DEFAULT_RETRY_BACKOFF_MS: i64 = 100;

pub const RETRY_BACKOFF_MAX_MS_CONFIG: &str = "retry.backoff.max.ms";
const RETRY_BACKOFF_MAX_MS_DOC: &str = "The maximum amount of time in milliseconds to wait before attempting to retry a failed request.";
pub const DEFAULT_RETRY_BACKOFF_MAX_MS: i64 = 1_000;

pub const REQUEST_TIMEOUT_MS_CONFIG: &str = "request.timeout.ms";
const REQUEST_TIMEOUT_MS_DOC: &str = "The configuration controls the maximum amount of time the client will wait for a response of a request.";
pub const REQUEST_TIMEOUT_MS_DEFAULT: i64 = 40_000; // 40 seconds

pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &str = "connections.max.idle.ms";
const CONNECTIONS_MAX_IDLE_MS_DOC: &str =
    "Close idle connections after the number of milliseconds specified by this config.";
pub const CONNECTIONS_MAX_IDLE_MS_DEFAULT: i64 = 540_000; // 9 minutes

pub const SECURITY_PROTOCOL_CONFIG: &str = "security.protocol";
const SECURITY_PROTOCOL_DOC: &str = "Protocol used to communicate with brokers.";
pub const DEFAULT_SECURITY_PROTOCOL: &str = "PLAINTEXT";

// ============================================================================
// ExactlyOnceSourceSupport Enum
// ============================================================================

/// Exactly-once source support configuration.
///
/// This enum defines the modes available for exactly-once source support.
/// Corresponds to Java: DistributedConfig.ExactlyOnceSourceSupport
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactlyOnceSourceSupport {
    /// Disabled - no exactly-once support for source connectors
    Disabled,
    /// Preparing - preparing to enable exactly-once support
    Preparing,
    /// Enabled - exactly-once support is enabled
    Enabled,
}

impl ExactlyOnceSourceSupport {
    /// Returns whether this mode uses a transactional leader.
    pub fn uses_transactional_leader(&self) -> bool {
        match self {
            ExactlyOnceSourceSupport::Disabled => false,
            ExactlyOnceSourceSupport::Preparing => true,
            ExactlyOnceSourceSupport::Enabled => true,
        }
    }

    /// Parse from property string.
    /// Corresponds to Java: ExactlyOnceSourceSupport.fromProperty(String)
    pub fn from_property(property: &str) -> Self {
        match property.to_lowercase().as_str() {
            "disabled" => ExactlyOnceSourceSupport::Disabled,
            "preparing" => ExactlyOnceSourceSupport::Preparing,
            "enabled" => ExactlyOnceSourceSupport::Enabled,
            _ => ExactlyOnceSourceSupport::Disabled,
        }
    }

    /// Convert to string representation.
    pub fn to_string(&self) -> &'static str {
        match self {
            ExactlyOnceSourceSupport::Disabled => "disabled",
            ExactlyOnceSourceSupport::Preparing => "preparing",
            ExactlyOnceSourceSupport::Enabled => "enabled",
        }
    }
}

impl std::fmt::Display for ExactlyOnceSourceSupport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

// ============================================================================
// DistributedConfig
// ============================================================================

/// Configuration for distributed mode Kafka Connect workers.
///
/// This class extends WorkerConfig and adds configuration options specific
/// to distributed mode, including:
/// - Group coordination (group.id, session timeout, heartbeat interval)
/// - Storage topics (config, offset, status)
/// - Worker sync and rebalance timeouts
/// - Inter-worker key/signature validation
/// - Exactly-once source support
///
/// Corresponds to Java: org.apache.kafka.connect.runtime.distributed.DistributedConfig
pub struct DistributedConfig {
    /// Underlying worker config
    worker_config: WorkerConfig,
    /// Crypto implementation for key/signature validation
    crypto: Arc<dyn Crypto>,
    /// Exactly-once source support mode
    exactly_once_source_support: ExactlyOnceSourceSupport,
}

impl std::fmt::Debug for DistributedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedConfig")
            .field("worker_config", &self.worker_config)
            .field("crypto", &"Arc<dyn Crypto>")
            .field(
                "exactly_once_source_support",
                &self.exactly_once_source_support,
            )
            .finish()
    }
}

impl DistributedConfig {
    /// Creates a new DistributedConfig with the given configuration properties.
    ///
    /// Corresponds to Java: DistributedConfig(Map<String, String> props)
    pub fn new(props: HashMap<String, Value>) -> Result<Self, ConfigException> {
        Self::with_crypto(system_crypto(), props)
    }

    /// Creates a new DistributedConfig with a custom Crypto implementation.
    ///
    /// Visible for testing.
    /// Corresponds to Java: DistributedConfig(Crypto crypto, Map<String, String> props)
    pub fn with_crypto(
        crypto: Arc<dyn Crypto>,
        props: HashMap<String, Value>,
    ) -> Result<Self, ConfigException> {
        let config = DistributedConfig {
            worker_config: WorkerConfig::new_unvalidated(props),
            crypto,
            exactly_once_source_support: ExactlyOnceSourceSupport::Disabled,
        };
        config.validate()?;

        // Parse exactly-once source support
        let exactly_once_str = config
            .get_string(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG)
            .unwrap_or(EXACTLY_ONCE_SOURCE_SUPPORT_DEFAULT);
        let exactly_once_source_support = ExactlyOnceSourceSupport::from_property(exactly_once_str);

        let mut config = config;
        config.exactly_once_source_support = exactly_once_source_support;

        // Validate inter-worker key configs
        config.validate_inter_worker_key_configs()?;

        Ok(config)
    }

    /// Returns the configuration definition for distributed mode.
    ///
    /// This includes both the base WorkerConfig definitions and distributed-specific ones.
    /// Corresponds to Java: DistributedConfig.config()
    pub fn config_def(crypto: &dyn Crypto) -> HashMap<String, common_trait::config::ConfigKeyDef> {
        let base_def = WorkerConfig::base_config_def();

        ConfigDefBuilder::from_map(base_def)
            // Group coordination configs
            .define(
                GROUP_ID_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                GROUP_ID_DOC,
            )
            .define(
                SESSION_TIMEOUT_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(SESSION_TIMEOUT_MS_DEFAULT.into())),
                ConfigDefImportance::High,
                SESSION_TIMEOUT_MS_DOC,
            )
            .define(
                REBALANCE_TIMEOUT_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(REBALANCE_TIMEOUT_MS_DEFAULT.into())),
                ConfigDefImportance::High,
                REBALANCE_TIMEOUT_MS_DOC,
            )
            .define(
                HEARTBEAT_INTERVAL_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(HEARTBEAT_INTERVAL_MS_DEFAULT.into())),
                ConfigDefImportance::High,
                HEARTBEAT_INTERVAL_MS_DOC,
            )
            .define(
                EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG,
                ConfigDefType::String,
                Some(Value::String(
                    EXACTLY_ONCE_SOURCE_SUPPORT_DEFAULT.to_string(),
                )),
                ConfigDefImportance::High,
                EXACTLY_ONCE_SOURCE_SUPPORT_DOC,
            )
            // Metadata and client configs
            .define(
                METADATA_MAX_AGE_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(METADATA_MAX_AGE_DEFAULT.into())),
                ConfigDefImportance::Low,
                METADATA_MAX_AGE_DOC,
            )
            .define(
                CLIENT_ID_CONFIG,
                ConfigDefType::String,
                Some(Value::String(String::new())),
                ConfigDefImportance::Low,
                CLIENT_ID_DOC,
            )
            .define(
                SEND_BUFFER_CONFIG,
                ConfigDefType::Int,
                Some(Value::Number(SEND_BUFFER_DEFAULT.into())),
                ConfigDefImportance::Medium,
                SEND_BUFFER_DOC,
            )
            .define(
                RECEIVE_BUFFER_CONFIG,
                ConfigDefType::Int,
                Some(Value::Number(RECEIVE_BUFFER_DEFAULT.into())),
                ConfigDefImportance::Medium,
                RECEIVE_BUFFER_DOC,
            )
            .define(
                RECONNECT_BACKOFF_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(RECONNECT_BACKOFF_MS_DEFAULT.into())),
                ConfigDefImportance::Low,
                RECONNECT_BACKOFF_MS_DOC,
            )
            .define(
                RECONNECT_BACKOFF_MAX_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(RECONNECT_BACKOFF_MAX_MS_DEFAULT.into())),
                ConfigDefImportance::Low,
                RECONNECT_BACKOFF_MAX_MS_DOC,
            )
            .define(
                SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(
                    DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS.into(),
                )),
                ConfigDefImportance::Low,
                SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC,
            )
            .define(
                SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(
                    DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS.into(),
                )),
                ConfigDefImportance::Low,
                SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC,
            )
            .define(
                RETRY_BACKOFF_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(DEFAULT_RETRY_BACKOFF_MS.into())),
                ConfigDefImportance::Low,
                RETRY_BACKOFF_MS_DOC,
            )
            .define(
                RETRY_BACKOFF_MAX_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(DEFAULT_RETRY_BACKOFF_MAX_MS.into())),
                ConfigDefImportance::Low,
                RETRY_BACKOFF_MAX_MS_DOC,
            )
            .define(
                REQUEST_TIMEOUT_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(REQUEST_TIMEOUT_MS_DEFAULT.into())),
                ConfigDefImportance::Medium,
                REQUEST_TIMEOUT_MS_DOC,
            )
            .define(
                CONNECTIONS_MAX_IDLE_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(CONNECTIONS_MAX_IDLE_MS_DEFAULT.into())),
                ConfigDefImportance::Medium,
                CONNECTIONS_MAX_IDLE_MS_DOC,
            )
            .define(
                SECURITY_PROTOCOL_CONFIG,
                ConfigDefType::String,
                Some(Value::String(DEFAULT_SECURITY_PROTOCOL.to_string())),
                ConfigDefImportance::Medium,
                SECURITY_PROTOCOL_DOC,
            )
            // Worker sync configs
            .define(
                WORKER_SYNC_TIMEOUT_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(WORKER_SYNC_TIMEOUT_MS_DEFAULT.into())),
                ConfigDefImportance::Medium,
                WORKER_SYNC_TIMEOUT_MS_DOC,
            )
            .define(
                WORKER_UNSYNC_BACKOFF_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(WORKER_UNSYNC_BACKOFF_MS_DEFAULT.into())),
                ConfigDefImportance::Medium,
                WORKER_UNSYNC_BACKOFF_MS_DOC,
            )
            // Offset storage topic configs
            .define(
                OFFSET_STORAGE_TOPIC_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                OFFSET_STORAGE_TOPIC_CONFIG_DOC,
            )
            .define(
                OFFSET_STORAGE_PARTITIONS_CONFIG,
                ConfigDefType::Int,
                Some(Value::Number(OFFSET_STORAGE_PARTITIONS_DEFAULT.into())),
                ConfigDefImportance::Low,
                OFFSET_STORAGE_PARTITIONS_CONFIG_DOC,
            )
            .define(
                OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG,
                ConfigDefType::Short,
                Some(Value::Number(
                    OFFSET_STORAGE_REPLICATION_FACTOR_DEFAULT.into(),
                )),
                ConfigDefImportance::Low,
                OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG_DOC,
            )
            // Config storage topic configs
            .define(
                CONFIG_TOPIC_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                CONFIG_TOPIC_CONFIG_DOC,
            )
            .define(
                CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG,
                ConfigDefType::Short,
                Some(Value::Number(
                    CONFIG_STORAGE_REPLICATION_FACTOR_DEFAULT.into(),
                )),
                ConfigDefImportance::Low,
                CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG_DOC,
            )
            // Status storage topic configs
            .define(
                STATUS_STORAGE_TOPIC_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                STATUS_STORAGE_TOPIC_CONFIG_DOC,
            )
            .define(
                STATUS_STORAGE_PARTITIONS_CONFIG,
                ConfigDefType::Int,
                Some(Value::Number(STATUS_STORAGE_PARTITIONS_DEFAULT.into())),
                ConfigDefImportance::Low,
                STATUS_STORAGE_PARTITIONS_CONFIG_DOC,
            )
            .define(
                STATUS_STORAGE_REPLICATION_FACTOR_CONFIG,
                ConfigDefType::Short,
                Some(Value::Number(
                    STATUS_STORAGE_REPLICATION_FACTOR_DEFAULT.into(),
                )),
                ConfigDefImportance::Low,
                STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DOC,
            )
            // Connect protocol config
            .define(
                CONNECT_PROTOCOL_CONFIG,
                ConfigDefType::String,
                Some(Value::String(CONNECT_PROTOCOL_DEFAULT.to_string())),
                ConfigDefImportance::Low,
                CONNECT_PROTOCOL_DOC,
            )
            .define(
                SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(
                    SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT.into(),
                )),
                ConfigDefImportance::Low,
                SCHEDULED_REBALANCE_MAX_DELAY_MS_DOC,
            )
            // Inter-worker key configs
            .define(
                INTER_WORKER_KEY_TTL_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(INTER_WORKER_KEY_TTL_MS_DEFAULT.into())),
                ConfigDefImportance::Low,
                INTER_WORKER_KEY_TTL_MS_DOC,
            )
            .define(
                INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG,
                ConfigDefType::String,
                Some(Value::String(Self::default_key_generation_algorithm(
                    crypto,
                ))),
                ConfigDefImportance::Low,
                INTER_WORKER_KEY_GENERATION_ALGORITHM_DOC,
            )
            .define(
                INTER_WORKER_KEY_SIZE_CONFIG,
                ConfigDefType::Int,
                None,
                ConfigDefImportance::Low,
                INTER_WORKER_KEY_SIZE_DOC,
            )
            .define(
                INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG,
                ConfigDefType::String,
                Some(Value::String(Self::default_signature_algorithm(crypto))),
                ConfigDefImportance::Low,
                INTER_WORKER_SIGNATURE_ALGORITHM_DOC,
            )
            .define(
                INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG,
                ConfigDefType::List,
                Some(Value::Array(Self::default_verification_algorithms(crypto))),
                ConfigDefImportance::Low,
                INTER_WORKER_VERIFICATION_ALGORITHMS_DOC,
            )
            // Metadata recovery configs
            .define(
                METADATA_RECOVERY_STRATEGY_CONFIG,
                ConfigDefType::String,
                Some(Value::String(
                    DEFAULT_METADATA_RECOVERY_STRATEGY.to_string(),
                )),
                ConfigDefImportance::Low,
                METADATA_RECOVERY_STRATEGY_DOC,
            )
            .define(
                METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
                ConfigDefType::Long,
                Some(Value::Number(
                    DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS.into(),
                )),
                ConfigDefImportance::Low,
                METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC,
            )
            .build()
    }

    /// Get default key generation algorithm, or NO_DEFAULT_VALUE if unavailable.
    fn default_key_generation_algorithm(crypto: &dyn Crypto) -> String {
        if Self::validate_key_algorithm(crypto, INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT)
            .is_ok()
        {
            INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT.to_string()
        } else {
            // Return empty string to indicate no default available
            String::new()
        }
    }

    /// Get default signature algorithm, or empty string if unavailable.
    fn default_signature_algorithm(crypto: &dyn Crypto) -> String {
        if Self::validate_signature_algorithm(crypto, INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT)
            .is_ok()
        {
            INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT.to_string()
        } else {
            String::new()
        }
    }

    /// Get default verification algorithms list.
    fn default_verification_algorithms(crypto: &dyn Crypto) -> Vec<Value> {
        let mut result = Vec::new();
        if Self::validate_signature_algorithm(crypto, INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT)
            .is_ok()
        {
            result.push(Value::String(
                INTER_WORKER_SIGNATURE_ALGORITHM_DEFAULT.to_string(),
            ));
        }
        result
    }

    /// Validate key algorithm.
    fn validate_key_algorithm(crypto: &dyn Crypto, algorithm: &str) -> Result<(), CryptoError> {
        crypto.key_generator(algorithm)?;
        Ok(())
    }

    /// Validate signature algorithm.
    fn validate_signature_algorithm(
        crypto: &dyn Crypto,
        algorithm: &str,
    ) -> Result<(), CryptoError> {
        crypto.mac(algorithm)?;
        Ok(())
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigException> {
        // Validate required distributed-specific configs
        if self.worker_config.get(GROUP_ID_CONFIG).is_none() {
            return Err(ConfigException::new(format!(
                "Required configuration {} is missing",
                GROUP_ID_CONFIG
            )));
        }

        if self
            .worker_config
            .get(OFFSET_STORAGE_TOPIC_CONFIG)
            .is_none()
        {
            return Err(ConfigException::new(format!(
                "Required configuration {} is missing",
                OFFSET_STORAGE_TOPIC_CONFIG
            )));
        }

        if self.worker_config.get(CONFIG_TOPIC_CONFIG).is_none() {
            return Err(ConfigException::new(format!(
                "Required configuration {} is missing",
                CONFIG_TOPIC_CONFIG
            )));
        }

        if self
            .worker_config
            .get(STATUS_STORAGE_TOPIC_CONFIG)
            .is_none()
        {
            return Err(ConfigException::new(format!(
                "Required configuration {} is missing",
                STATUS_STORAGE_TOPIC_CONFIG
            )));
        }

        // Validate base worker configs
        self.worker_config.validate()?;

        // Validate connect protocol
        let protocol_str = self
            .get_string(CONNECT_PROTOCOL_CONFIG)
            .unwrap_or(CONNECT_PROTOCOL_DEFAULT);
        // Try to parse protocol - will panic if invalid, we catch that
        let _ = ConnectProtocolCompatibility::compatibility(protocol_str);

        Ok(())
    }

    /// Validate inter-worker key configs.
    fn validate_inter_worker_key_configs(&self) -> Result<(), ConfigException> {
        // Try to get internal request key generator
        let algorithm = self
            .get_string(INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG)
            .unwrap_or_default();
        if !algorithm.is_empty() {
            self.crypto.key_generator(&algorithm).map_err(|e| {
                ConfigException::new(format!(
                    "Invalid key generation algorithm '{}': {}",
                    algorithm, e
                ))
            })?;
        }

        // Ensure verification algorithms include signature algorithm
        let signature_algorithm = self
            .get_string(INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG)
            .unwrap_or_default();
        let verification_algorithms = self
            .get_list(INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG)
            .unwrap_or_default();

        if !signature_algorithm.is_empty() && !verification_algorithms.is_empty() {
            if !verification_algorithms.contains(&signature_algorithm.to_string()) {
                return Err(ConfigException::new(format!(
                    "Signature algorithm '{}' must be present in {} list",
                    signature_algorithm, INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG
                )));
            }
        }

        Ok(())
    }

    // ============================================================================
    // Accessor Methods
    // ============================================================================

    /// Returns the group ID.
    /// Corresponds to Java: DistributedConfig.groupId()
    pub fn group_id(&self) -> Option<&str> {
        self.get_string(GROUP_ID_CONFIG)
    }

    /// Returns the rebalance timeout in milliseconds.
    /// Corresponds to Java: DistributedConfig.rebalanceTimeout()
    pub fn rebalance_timeout_ms(&self) -> i64 {
        self.get_long(REBALANCE_TIMEOUT_MS_CONFIG)
            .unwrap_or(REBALANCE_TIMEOUT_MS_DEFAULT)
    }

    /// Returns the session timeout in milliseconds.
    pub fn session_timeout_ms(&self) -> i64 {
        self.get_long(SESSION_TIMEOUT_MS_CONFIG)
            .unwrap_or(SESSION_TIMEOUT_MS_DEFAULT)
    }

    /// Returns the heartbeat interval in milliseconds.
    pub fn heartbeat_interval_ms(&self) -> i64 {
        self.get_long(HEARTBEAT_INTERVAL_MS_CONFIG)
            .unwrap_or(HEARTBEAT_INTERVAL_MS_DEFAULT)
    }

    /// Returns whether exactly-once source support is enabled.
    /// Corresponds to Java: DistributedConfig.exactlyOnceSourceEnabled()
    pub fn exactly_once_source_enabled(&self) -> bool {
        self.exactly_once_source_support == ExactlyOnceSourceSupport::Enabled
    }

    /// Returns whether the Connect cluster's leader should use a transactional producer.
    /// Corresponds to Java: DistributedConfig.transactionalLeaderEnabled()
    pub fn transactional_leader_enabled(&self) -> bool {
        self.exactly_once_source_support.uses_transactional_leader()
    }

    /// Returns the transactional producer ID.
    /// Corresponds to Java: DistributedConfig.transactionalProducerId()
    pub fn transactional_producer_id(&self) -> Option<String> {
        self.group_id()
            .map(|gid| Self::transactional_producer_id_from_group(gid))
    }

    /// Static method to compute transactional producer ID from group ID.
    /// Corresponds to Java: DistributedConfig.transactionalProducerId(String groupId)
    pub fn transactional_producer_id_from_group(group_id: &str) -> String {
        format!("connect-cluster-{}", group_id)
    }

    /// Returns the offsets storage topic.
    /// Corresponds to Java: DistributedConfig.offsetsTopic()
    pub fn offsets_topic(&self) -> Option<&str> {
        self.get_string(OFFSET_STORAGE_TOPIC_CONFIG)
    }

    /// Returns whether connector offsets topics are permitted.
    /// Corresponds to Java: DistributedConfig.connectorOffsetsTopicsPermitted()
    pub fn connector_offsets_topics_permitted(&self) -> bool {
        true
    }

    /// Returns the config storage topic.
    pub fn config_topic(&self) -> Option<&str> {
        self.get_string(CONFIG_TOPIC_CONFIG)
    }

    /// Returns the status storage topic.
    pub fn status_topic(&self) -> Option<&str> {
        self.get_string(STATUS_STORAGE_TOPIC_CONFIG)
    }

    /// Returns topic settings for a given prefix.
    /// Corresponds to Java: DistributedConfig.topicSettings(String prefix)
    pub fn topic_settings(&self, prefix: &str) -> HashMap<String, Value> {
        let mut result = HashMap::new();

        // Get originals with prefix
        for (key, value) in self.worker_config.originals() {
            if key.starts_with(prefix) {
                // Remove prefix to get the setting name
                let setting_name = key.strip_prefix(prefix).unwrap_or(key);

                // Skip reserved suffixes
                if setting_name == TOPIC_SUFFIX
                    || setting_name == REPLICATION_FACTOR_SUFFIX
                    || setting_name == PARTITIONS_SUFFIX
                {
                    continue;
                }

                // Skip cleanup.policy - compaction is always used
                if setting_name == "cleanup.policy" {
                    continue;
                }

                // For config storage, skip partitions (always 1)
                if prefix == CONFIG_STORAGE_PREFIX && setting_name == PARTITIONS_SUFFIX {
                    continue;
                }

                result.insert(setting_name.to_string(), value.clone());
            }
        }

        result
    }

    /// Returns config storage topic settings.
    /// Corresponds to Java: DistributedConfig.configStorageTopicSettings()
    pub fn config_storage_topic_settings(&self) -> HashMap<String, Value> {
        self.topic_settings(CONFIG_STORAGE_PREFIX)
    }

    /// Returns offset storage topic settings.
    /// Corresponds to Java: DistributedConfig.offsetStorageTopicSettings()
    pub fn offset_storage_topic_settings(&self) -> HashMap<String, Value> {
        self.topic_settings(OFFSET_STORAGE_PREFIX)
    }

    /// Returns status storage topic settings.
    /// Corresponds to Java: DistributedConfig.statusStorageTopicSettings()
    pub fn status_storage_topic_settings(&self) -> HashMap<String, Value> {
        self.topic_settings(STATUS_STORAGE_PREFIX)
    }

    /// Returns the connect protocol compatibility mode.
    pub fn connect_protocol(&self) -> ConnectProtocolCompatibility {
        let protocol_str = self
            .get_string(CONNECT_PROTOCOL_CONFIG)
            .unwrap_or(CONNECT_PROTOCOL_DEFAULT);
        ConnectProtocolCompatibility::compatibility(protocol_str)
    }

    /// Returns the scheduled rebalance max delay in milliseconds.
    pub fn scheduled_rebalance_max_delay_ms(&self) -> i64 {
        self.get_long(SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG)
            .unwrap_or(SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT)
    }

    /// Returns the worker sync timeout in milliseconds.
    pub fn worker_sync_timeout_ms(&self) -> i64 {
        self.get_long(WORKER_SYNC_TIMEOUT_MS_CONFIG)
            .unwrap_or(WORKER_SYNC_TIMEOUT_MS_DEFAULT)
    }

    /// Returns the worker unsync backoff in milliseconds.
    pub fn worker_unsync_backoff_ms(&self) -> i64 {
        self.get_long(WORKER_UNSYNC_BACKOFF_MS_CONFIG)
            .unwrap_or(WORKER_UNSYNC_BACKOFF_MS_DEFAULT)
    }

    /// Returns the inter-worker key TTL in milliseconds.
    pub fn inter_worker_key_ttl_ms(&self) -> i64 {
        self.get_long(INTER_WORKER_KEY_TTL_MS_CONFIG)
            .unwrap_or(INTER_WORKER_KEY_TTL_MS_DEFAULT)
    }

    /// Returns a key generator for internal requests.
    /// Corresponds to Java: DistributedConfig.getInternalRequestKeyGenerator()
    pub fn get_internal_request_key_generator(&self) -> Result<KeyGenerator, ConfigException> {
        let algorithm = self
            .get_string(INTER_WORKER_KEY_GENERATION_ALGORITHM_CONFIG)
            .unwrap_or(INTER_WORKER_KEY_GENERATION_ALGORITHM_DEFAULT);

        let mut key_gen = self.crypto.key_generator(&algorithm).map_err(|e| {
            ConfigException::new(format!(
                "Unable to create key generator with algorithm {}: {}",
                algorithm, e
            ))
        })?;

        // Apply key size if specified
        if let Some(key_size) = self.get_int(INTER_WORKER_KEY_SIZE_CONFIG) {
            key_gen.init_with_size(key_size as usize);
        }

        Ok(key_gen)
    }

    /// Returns the underlying WorkerConfig.
    pub fn worker_config(&self) -> &WorkerConfig {
        &self.worker_config
    }

    /// Returns the raw configuration value for the given key.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.worker_config.get(key)
    }

    /// Returns the configuration value as a string.
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.worker_config.get_string(key)
    }

    /// Returns the configuration value as an integer.
    pub fn get_int(&self, key: &str) -> Option<i32> {
        self.worker_config.get_int(key)
    }

    /// Returns the configuration value as a long.
    pub fn get_long(&self, key: &str) -> Option<i64> {
        self.worker_config.get_long(key)
    }

    /// Returns the configuration value as a boolean.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.worker_config.get_bool(key)
    }

    /// Returns the configuration value as a list.
    pub fn get_list(&self, key: &str) -> Option<Vec<String>> {
        self.worker_config.get_list(key)
    }

    /// Returns all original configuration values.
    pub fn originals(&self) -> &HashMap<String, Value> {
        self.worker_config.originals()
    }
}

impl Clone for DistributedConfig {
    fn clone(&self) -> Self {
        DistributedConfig {
            worker_config: self.worker_config.clone(),
            crypto: self.crypto.clone(),
            exactly_once_source_support: self.exactly_once_source_support,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::crypto::MockCrypto;

    fn create_basic_distributed_props() -> HashMap<String, Value> {
        let mut props = HashMap::new();

        // WorkerConfig required fields
        props.insert(
            crate::config::worker_config::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            Value::String("localhost:9092".to_string()),
        );
        props.insert(
            crate::config::worker_config::KEY_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("org.apache.kafka.connect.json.JsonConverter".to_string()),
        );
        props.insert(
            crate::config::worker_config::VALUE_CONVERTER_CLASS_CONFIG.to_string(),
            Value::String("org.apache.kafka.connect.json.JsonConverter".to_string()),
        );

        // DistributedConfig required fields
        props.insert(
            GROUP_ID_CONFIG.to_string(),
            Value::String("connect-cluster".to_string()),
        );
        props.insert(
            OFFSET_STORAGE_TOPIC_CONFIG.to_string(),
            Value::String("connect-offsets".to_string()),
        );
        props.insert(
            CONFIG_TOPIC_CONFIG.to_string(),
            Value::String("connect-configs".to_string()),
        );
        props.insert(
            STATUS_STORAGE_TOPIC_CONFIG.to_string(),
            Value::String("connect-status".to_string()),
        );

        props
    }

    #[test]
    fn test_distributed_config_basic() {
        let props = create_basic_distributed_props();
        let crypto = system_crypto();
        let config = DistributedConfig::with_crypto(crypto, props).unwrap();

        assert_eq!(config.group_id(), Some("connect-cluster"));
        assert_eq!(config.offsets_topic(), Some("connect-offsets"));
        assert_eq!(config.config_topic(), Some("connect-configs"));
        assert_eq!(config.status_topic(), Some("connect-status"));
    }

    #[test]
    fn test_distributed_config_defaults() {
        let props = create_basic_distributed_props();
        let crypto = system_crypto();
        let config = DistributedConfig::with_crypto(crypto, props).unwrap();

        assert_eq!(config.session_timeout_ms(), SESSION_TIMEOUT_MS_DEFAULT);
        assert_eq!(
            config.heartbeat_interval_ms(),
            HEARTBEAT_INTERVAL_MS_DEFAULT
        );
        assert_eq!(config.rebalance_timeout_ms(), REBALANCE_TIMEOUT_MS_DEFAULT);
        assert_eq!(
            config.scheduled_rebalance_max_delay_ms(),
            SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT
        );
        assert_eq!(
            config.inter_worker_key_ttl_ms(),
            INTER_WORKER_KEY_TTL_MS_DEFAULT
        );
    }

    #[test]
    fn test_exactly_once_source_support() {
        assert_eq!(
            ExactlyOnceSourceSupport::from_property("disabled"),
            ExactlyOnceSourceSupport::Disabled
        );
        assert_eq!(
            ExactlyOnceSourceSupport::from_property("preparing"),
            ExactlyOnceSourceSupport::Preparing
        );
        assert_eq!(
            ExactlyOnceSourceSupport::from_property("enabled"),
            ExactlyOnceSourceSupport::Enabled
        );

        assert!(!ExactlyOnceSourceSupport::Disabled.uses_transactional_leader());
        assert!(ExactlyOnceSourceSupport::Preparing.uses_transactional_leader());
        assert!(ExactlyOnceSourceSupport::Enabled.uses_transactional_leader());
    }

    #[test]
    fn test_transactional_producer_id() {
        assert_eq!(
            DistributedConfig::transactional_producer_id_from_group("my-cluster"),
            "connect-cluster-my-cluster"
        );
    }

    #[test]
    fn test_validation_missing_group_id() {
        let mut props = create_basic_distributed_props();
        props.remove(GROUP_ID_CONFIG);

        let crypto = system_crypto();
        let result = DistributedConfig::with_crypto(crypto, props);
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_missing_offset_topic() {
        let mut props = create_basic_distributed_props();
        props.remove(OFFSET_STORAGE_TOPIC_CONFIG);

        let crypto = system_crypto();
        let result = DistributedConfig::with_crypto(crypto, props);
        assert!(result.is_err());
    }

    #[test]
    fn test_connect_protocol() {
        let mut props = create_basic_distributed_props();
        props.insert(
            CONNECT_PROTOCOL_CONFIG.to_string(),
            Value::String("eager".to_string()),
        );

        let crypto = system_crypto();
        let config = DistributedConfig::with_crypto(crypto, props).unwrap();
        assert_eq!(
            config.connect_protocol(),
            ConnectProtocolCompatibility::Eager
        );
    }

    #[test]
    fn test_topic_settings() {
        let mut props = create_basic_distributed_props();
        props.insert(
            "offset.storage.compression.type".to_string(),
            Value::String("gzip".to_string()),
        );

        let crypto = system_crypto();
        let config = DistributedConfig::with_crypto(crypto, props).unwrap();

        let settings = config.offset_storage_topic_settings();
        assert!(settings.contains_key("compression.type"));
    }

    #[test]
    fn test_with_mock_crypto() {
        let props = create_basic_distributed_props();
        let crypto = Arc::new(MockCrypto::new());
        let config = DistributedConfig::with_crypto(crypto, props).unwrap();

        assert!(config.get_internal_request_key_generator().is_ok());
    }
}
