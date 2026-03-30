//! Configuration management
//!
//! This module provides configuration traits for mirror connectors.

use connect_api::ConfigDef;
use std::collections::HashMap;

/// Mirror source connector configuration
pub trait MirrorSourceConfig {
    /// Get upstream cluster alias
    fn upstream_cluster_alias(&self) -> String;

    /// Get downstream cluster alias
    fn downstream_cluster_alias(&self) -> String;

    /// Get topic configuration
    fn topic_config(&self) -> HashMap<String, String>;

    /// Get replication policy
    fn replication_policy(&self) -> String;

    /// Get offset sync topic name
    fn offset_sync_topic(&self) -> String;

    /// Get heartbeat topic name
    fn heartbeat_topic(&self) -> String;

    /// Check if replication should be enabled
    fn replication_enabled(&self) -> bool;

    /// Get config definition
    fn config_def(&self) -> ConfigDef;
}

/// Mirror checkpoint connector configuration
pub trait MirrorCheckpointConfig {
    /// Get upstream cluster alias
    fn upstream_cluster_alias(&self) -> String;

    /// Get downstream cluster alias
    fn downstream_cluster_alias(&self) -> String;

    /// Get checkpoint topic name
    fn checkpoint_topic(&self) -> String;

    /// Get sync group offset timeout
    fn sync_group_offset_timeout_ms(&self) -> i64;

    /// Check if checkpoint sync should be enabled
    fn checkpoint_sync_enabled(&self) -> bool;

    /// Get config definition
    fn config_def(&self) -> ConfigDef;
}

/// Mirror heartbeat connector configuration
pub trait MirrorHeartbeatConfig {
    /// Get upstream cluster alias
    fn upstream_cluster_alias(&self) -> String;

    /// Get downstream cluster alias
    fn downstream_cluster_alias(&self) -> String;

    /// Get heartbeat topic name
    fn heartbeat_topic(&self) -> String;

    /// Get heartbeat interval in milliseconds
    fn heartbeat_interval_ms(&self) -> i64;

    /// Check if heartbeat should be enabled
    fn heartbeat_enabled(&self) -> bool;

    /// Get config definition
    fn config_def(&self) -> ConfigDef;
}
