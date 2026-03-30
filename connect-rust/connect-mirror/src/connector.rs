//! Mirror connectors
//!
//! This module provides mirror source, checkpoint, and heartbeat connectors.

use anyhow::Result;
use connect_api::SourceConnector;

/// Mirror Source Connector
///
/// Replicates topics from upstream cluster to downstream cluster.
pub trait MirrorSourceConnector: SourceConnector {
    /// Start connector
    fn start(&mut self) -> Result<()>;

    /// Stop connector
    fn stop(&mut self) -> Result<()>;

    /// Sync topic ACLs from upstream to downstream
    fn sync_topic_acls(&self) -> Result<()>;

    /// Sync topic configurations from upstream to downstream
    fn sync_topic_configs(&self) -> Result<()>;

    /// Refresh topic partition information
    fn refresh_topic_partitions(&self) -> Result<()>;

    /// Compute and create topic partitions
    fn compute_and_create_topic_partitions(&self) -> Result<()>;

    /// Create new topics
    fn create_new_topics(&self) -> Result<()>;

    /// Create new partitions
    fn create_new_partitions(&self) -> Result<()>;
}

/// Mirror Checkpoint Connector
///
/// Replicates consumer group checkpoints from upstream to downstream.
pub trait MirrorCheckpointConnector: SourceConnector {
    /// Start connector
    fn start(&mut self) -> Result<()>;

    /// Stop connector
    fn stop(&mut self) -> Result<()>;

    /// Refresh consumer groups
    fn refresh_consumer_groups(&self) -> Result<()>;

    /// Find consumer groups to replicate
    fn find_consumer_groups(&self) -> Result<Vec<String>>;
}

/// Mirror Heartbeat Connector
///
/// Publishes heartbeat records to monitor replication health.
pub trait MirrorHeartbeatConnector: SourceConnector {
    /// Start connector
    fn start(&mut self) -> Result<()>;

    /// Stop connector
    fn stop(&mut self) -> Result<()>;
}
