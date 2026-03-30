//! Mirror tasks
//!
//! This module provides the mirror source, checkpoint, and heartbeat tasks.

use anyhow::Result;
use connect_api::{SourceRecord, SourceTask};
use connect_mirror_client::Checkpoint;

/// Record metadata
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// Mirror Source Task
///
/// Replicates records from upstream cluster to downstream cluster.
pub trait MirrorSourceTask: SourceTask {
    /// Convert a consumer record to a source record
    fn convert_record(&self, consumer_record: ConsumerRecord) -> Result<SourceRecord>;
}

/// Mirror Checkpoint Task
///
/// Replicates consumer group checkpoints from upstream to downstream.
pub trait MirrorCheckpointTask: SourceTask {
    /// Get source records for a consumer group
    fn source_records_for_group(&self, group: String) -> Result<Vec<SourceRecord>>;

    /// Get checkpoints for a consumer group
    fn checkpoints_for_group(&self, group: String) -> Result<Vec<Checkpoint>>;

    /// Sync consumer group offset
    fn sync_group_offset(&self, group: String) -> Result<()>;
}

/// Mirror Heartbeat Task
///
/// Publishes heartbeat records to monitor replication health.
pub trait MirrorHeartbeatTask: SourceTask {
    // Heartbeat task primarily uses the poll() method from SourceTask
}

/// Consumer record representation
///
/// This represents a record consumed from the upstream cluster.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp: Option<i64>,
}
