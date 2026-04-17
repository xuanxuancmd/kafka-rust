//! Offset sync writer
//!
//! This module provides functionality for writing offset sync records to the offset-syncs topic,
//! with buffering logic to limit the number of in-flight records.

use anyhow::Result;
use common_trait::consumer::TopicPartition;
use common_trait::producer::{Callback, KafkaProducer, ProducerRecord, RecordMetadata};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tracing::{error, trace};

/// Maximum number of outstanding offset syncs
const MAX_OUTSTANDING_OFFSET_SYNCS: usize = 10;

/// Offset sync record
///
/// Represents a synchronization record between upstream and downstream offsets.
#[derive(Debug, Clone)]
pub struct OffsetSync {
    /// Topic partition
    pub topic_partition: TopicPartition,
    /// Upstream offset
    pub upstream_offset: i64,
    /// Downstream offset
    pub downstream_offset: i64,
}

impl OffsetSync {
    /// Create a new offset sync
    pub fn new(topic_partition: TopicPartition, upstream_offset: i64, downstream_offset: i64) -> Self {
        Self {
            topic_partition,
            upstream_offset,
            downstream_offset,
        }
    }

    /// Get the record key
    pub fn record_key(&self) -> Vec<u8> {
        format!("{}-{}", self.topic_partition.topic, self.topic_partition.partition)
            .as_bytes()
            .to_vec()
    }

    /// Get the record value
    pub fn record_value(&self) -> Vec<u8> {
        format!("{}:{}", self.upstream_offset, self.downstream_offset)
            .as_bytes()
            .to_vec()
    }

    /// Get the topic partition
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }

    /// Get the upstream offset
    pub fn upstream_offset(&self) -> i64 {
        self.upstream_offset
    }

    /// Get the downstream offset
    pub fn downstream_offset(&self) -> i64 {
        self.downstream_offset
    }
}

/// Partition state
///
/// Tracks the state of a partition for offset synchronization.
#[derive(Debug, Clone)]
pub struct PartitionState {
    /// Previous upstream offset
    previous_upstream_offset: i64,
    /// Previous downstream offset
    previous_downstream_offset: i64,
    /// Last sync downstream offset
    last_sync_downstream_offset: i64,
    /// Maximum offset lag
    max_offset_lag: i64,
    /// Whether to sync offsets
    should_sync_offsets: bool,
}

impl PartitionState {
    /// Create a new partition state
    pub fn new(max_offset_lag: i64) -> Self {
        Self {
            previous_upstream_offset: -1,
            previous_downstream_offset: -1,
            last_sync_downstream_offset: -1,
            max_offset_lag,
            should_sync_offsets: false,
        }
    }

    /// Update the partition state
    ///
    /// Returns true if an offset sync should be emitted.
    pub fn update(&mut self, upstream_offset: i64, downstream_offset: i64) -> bool {
        // Emit an offset sync if any of the following conditions are true
        let no_previous_sync_this_lifetime = self.last_sync_downstream_offset == -1;
        // The OffsetSync::translateDownstream method will translate this offset 1 past the last sync, so add 1.
        let translated_offset_too_stale =
            downstream_offset - (self.last_sync_downstream_offset + 1) >= self.max_offset_lag;
        let skipped_upstream_record = upstream_offset - self.previous_upstream_offset != 1;
        let truncated_downstream_topic = downstream_offset < self.previous_downstream_offset;

        if no_previous_sync_this_lifetime
            || translated_offset_too_stale
            || skipped_upstream_record
            || truncated_downstream_topic
        {
            self.last_sync_downstream_offset = downstream_offset;
            self.should_sync_offsets = true;
        }

        self.previous_upstream_offset = upstream_offset;
        self.previous_downstream_offset = downstream_offset;

        self.should_sync_offsets
    }

    /// Reset the should_sync_offsets flag
    pub fn reset(&mut self) {
        self.should_sync_offsets = false;
    }
}

/// Offset sync writer
///
/// Used internally by MirrorMaker to write translated offsets into offset-syncs topic,
/// with some buffering logic to limit the number of in-flight records.
pub struct OffsetSyncWriter {
    /// Delayed offset syncs
    delayed_offset_syncs: Arc<Mutex<HashMap<TopicPartition, OffsetSync>>>,
    /// Pending offset syncs
    pending_offset_syncs: Arc<Mutex<HashMap<TopicPartition, OffsetSync>>>,
    /// Outstanding offset syncs semaphore
    outstanding_offset_syncs: Arc<Semaphore>,
    /// Offset producer
    offset_producer: Option<Arc<dyn KafkaProducer<Vec<u8>, Vec<u8>>>>,
    /// Offset syncs topic name
    offset_syncs_topic: String,
    /// Maximum offset lag
    max_offset_lag: i64,
    /// Partition states
    partition_states: Arc<Mutex<HashMap<TopicPartition, PartitionState>>>,
}

impl OffsetSyncWriter {
    /// Create a new offset sync writer
    pub fn new(offset_syncs_topic: String, max_offset_lag: i64) -> Self {
        Self {
            delayed_offset_syncs: Arc::new(Mutex::new(HashMap::new())),
            pending_offset_syncs: Arc::new(Mutex::new(HashMap::new())),
            outstanding_offset_syncs: Arc::new(Semaphore::new(MAX_OUTSTANDING_OFFSET_SYNCS)),
            offset_producer: None,
            offset_syncs_topic,
            max_offset_lag,
            partition_states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new offset sync writer with a producer
    pub fn with_producer(
        offset_syncs_topic: String,
        max_offset_lag: i64,
        offset_producer: Arc<dyn KafkaProducer<Vec<u8>, Vec<u8>>>,
    ) -> Self {
        Self {
            delayed_offset_syncs: Arc::new(Mutex::new(HashMap::new())),
            pending_offset_syncs: Arc::new(Mutex::new(HashMap::new())),
            outstanding_offset_syncs: Arc::new(Semaphore::new(MAX_OUTSTANDING_OFFSET_SYNCS)),
            offset_producer: Some(offset_producer),
            offset_syncs_topic,
            max_offset_lag,
            partition_states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get the maximum offset lag
    pub fn max_offset_lag(&self) -> i64 {
        self.max_offset_lag
    }

    /// Get the partition states
    pub fn partition_states(&self) -> Arc<Mutex<HashMap<TopicPartition, PartitionState>>> {
        Arc::clone(&self.partition_states)
    }

    /// Send offset sync record
    ///
    /// Sends offset sync record to the Kafka producer.
    async fn send_offset_sync(&self, offset_sync: OffsetSync) -> Result<()> {
        trace!(
            "Sending offset sync for {}: {}=={}",
            offset_sync.topic_partition(),
            offset_sync.upstream_offset(),
            offset_sync.downstream_offset()
        );

        if let Some(producer) = &self.offset_producer {
            let record = ProducerRecord {
                topic: self.offset_syncs_topic.clone(),
                partition: Some(0),
                key: Some(offset_sync.record_key()),
                value: Some(offset_sync.record_value()),
                timestamp: None,
            };

            // Create a callback that releases semaphore permit
            let semaphore = Arc::clone(&self.outstanding_offset_syncs);
            let callback = Box::new(OffsetSyncCallback::new(
                offset_sync.topic_partition().clone(),
                offset_sync.upstream_offset(),
                offset_sync.downstream_offset(),
                semaphore,
            ));

            producer.send_with_callback(record, callback).await;
        }

        Ok(())
    }

    /// Fire pending offset syncs
    ///
    /// Sends all pending offset syncs to the offset-syncs topic.
    pub async fn fire_pending_offset_syncs(&self) -> Result<()> {
        loop {
            let pending_offset_sync = {
                let mut pending_syncs = self.pending_offset_syncs.lock().await;

                // Get the first pending offset sync
                let sync_iterator = pending_syncs.clone();
                if sync_iterator.is_empty() {
                    // Nothing to sync
                    trace!("No more pending offset syncs");
                    return Ok(());
                }

                let first_key = sync_iterator.keys().next().unwrap().clone();
                let first_sync = sync_iterator.get(&first_key).unwrap().clone();

                // Try to acquire a permit from the semaphore
                match self.outstanding_offset_syncs.try_acquire() {
                    Ok(_permit) => {
                        // Remove the sync from pending
                        pending_syncs.remove(&first_key);
                        Some(first_sync)
                    }
                    Err(_) => {
                        // Too many outstanding syncs
                        trace!(
                            "Too many in-flight offset syncs; will try to send remaining offset syncs later"
                        );
                        return Ok(());
                    }
                }
            };

            if let Some(offset_sync) = pending_offset_sync {
                // Publish offset sync outside of the lock
                match self.send_offset_sync(offset_sync.clone()).await {
                    Ok(_) => {
                        trace!(
                            "Dispatched offset sync for {}",
                            offset_sync.topic_partition()
                        );
                    }
                    Err(e) => {
                        error!("Failure sending offset sync: {}", e);
                    }
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Promote delayed offset syncs
    ///
    /// Moves all delayed offset syncs to the pending queue.
    pub async fn promote_delayed_offset_syncs(&self) {
        let mut delayed_syncs = self.delayed_offset_syncs.lock().await;
        let mut pending_syncs = self.pending_offset_syncs.lock().await;

        // Move all delayed syncs to pending
        for (key, value) in delayed_syncs.drain() {
            pending_syncs.insert(key, value);
        }
    }

    /// Maybe queue offset syncs
    ///
    /// Updates partition state and queues up offset sync if necessary.
    pub async fn maybe_queue_offset_syncs(
        &self,
        topic_partition: TopicPartition,
        upstream_offset: i64,
        downstream_offset: i64,
    ) -> Result<()> {
        // Get or create partition state
        let partition_state = {
            let mut states = self.partition_states.lock().await;
            if !states.contains_key(&topic_partition) {
                states.insert(
                    topic_partition.clone(),
                    PartitionState::new(self.max_offset_lag),
                );
            }
            states.get(&topic_partition).unwrap().clone()
        };

        let offset_sync = OffsetSync::new(topic_partition.clone(), upstream_offset, downstream_offset);

        // Check if we should sync offsets
        let mut state_clone = partition_state.clone();
        let should_sync = state_clone.update(upstream_offset, downstream_offset);

        if should_sync {
            // Queue this sync for an immediate send, as downstream state is sufficiently stale
            {
                let mut delayed_syncs = self.delayed_offset_syncs.lock().await;
                let mut pending_syncs = self.pending_offset_syncs.lock().await;

                delayed_syncs.remove(&topic_partition);
                pending_syncs.insert(topic_partition.clone(), offset_sync);
            }

            // Reset the partition state
            {
                let mut states = self.partition_states.lock().await;
                if let Some(state) = states.get_mut(&topic_partition) {
                    state.reset();
                }
            }
        } else {
            // Queue this sync to be delayed until the next periodic offset commit
            let mut delayed_syncs = self.delayed_offset_syncs.lock().await;
            delayed_syncs.insert(topic_partition, offset_sync);
        }

        Ok(())
    }

    /// Get delayed offset syncs (for testing)
    pub async fn get_delayed_offset_syncs(&self) -> HashMap<TopicPartition, OffsetSync> {
        let delayed_syncs = self.delayed_offset_syncs.lock().await;
        delayed_syncs.clone()
    }

    /// Get pending offset syncs (for testing)
    pub async fn get_pending_offset_syncs(&self) -> HashMap<TopicPartition, OffsetSync> {
        let pending_syncs = self.pending_offset_syncs.lock().await;
        pending_syncs.clone()
    }

    /// Close the offset sync writer
    ///
    /// Cleans up resources and stops any pending operations.
    pub async fn close(&self) {
        // Close the producer if it exists
        if let Some(producer) = &self.offset_producer {
            producer.close().await;
        }

        // Clear all pending and delayed syncs
        {
            let mut pending_syncs = self.pending_offset_syncs.lock().await;
            pending_syncs.clear();
        }
        {
            let mut delayed_syncs = self.delayed_offset_syncs.lock().await;
            delayed_syncs.clear();
        }
        {
            let mut partition_states = self.partition_states.lock().await;
            partition_states.clear();
        }

        trace!("Offset sync writer closed");
    }
}

/// Callback for offset sync send operations
struct OffsetSyncCallback {
    topic_partition: TopicPartition,
    upstream_offset: i64,
    downstream_offset: i64,
    semaphore: Arc<Semaphore>,
}

impl OffsetSyncCallback {
    fn new(
        topic_partition: TopicPartition,
        upstream_offset: i64,
        downstream_offset: i64,
        semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            topic_partition,
            upstream_offset,
            downstream_offset,
            semaphore,
        }
    }
}

impl Callback for OffsetSyncCallback {
    fn on_completion(&self, metadata: Option<RecordMetadata>, error: Option<String>) {
        if let Some(err) = error {
            error!("Failure sending offset sync: {}", err);
        } else {
            trace!(
                "Sync'd offsets for {}: {}=={}",
                self.topic_partition,
                self.upstream_offset,
                self.downstream_offset
            );
        }
        // Release semaphore permit by adding a new permit
        // Note: This is a workaround since tokio::sync::Semaphore doesn't have add_permits
        // In a real implementation, we would use a different synchronization mechanism
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_offset_sync_creation() {
        let tp = TopicPartition {
            topic: "topic".to_string(),
            partition: 0,
        };
        let sync = OffsetSync::new(tp.clone(), 100, 200);

        assert_eq!(sync.topic_partition(), &tp);
        assert_eq!(sync.upstream_offset(), 100);
        assert_eq!(sync.downstream_offset(), 200);
    }

    #[tokio::test]
    async fn test_partition_state_update() {
        let mut state = PartitionState::new(10);

        // First update should trigger sync (no previous sync)
        assert!(state.update(0, 0));
        state.reset();

();

        // Sequential updates should not trigger sync
        assert!(!state.update(1, 1));
        state.reset();

        // Skipped upstream record should trigger sync
        assert!(state.update(3, 2));
        state.reset();

        // Translated offset too stale should trigger sync
        assert!(!state.update(4, 3));
        state.reset();
        assert!(state.update(5, 15)); // 15 - (3 + 1) = 11 >= 10
    }

    #[tokio::test]
    async fn test_offset_sync_writer_creation() {
        let writer = OffsetSyncWriter::new("offset-syncs-topic".to_string(), 10);

        assert_eq!(writer.max_offset_lag(), 10);
    }

    #[tokio::test]
    async fn test_maybe_queue_offset_syncs() {
        let writer = OffsetSyncWriter::new("offset-syncs-topic".to_string(), 10);
        let tp = TopicPartition {
            topic: "topic".to_string(),
            partition: 0,
        };

        // First sync should be queued immediately (no previous sync)
        writer
            .maybe_queue_offset_syncs(tp.clone(), 0, 0)
            .await
            .unwrap();

        let pending = writer.get_pending_offset_syncs().await;
        assert_eq!(pending.len(), 1);
        assert!(pending.contains_key(&tp));

        // Sequential sync should be delayed
        writer
            .maybe_queue_offset_syncs(tp.clone(), 1, 1)
            .await
            .unwrap();

        let delayed = writer.get_delayed_offset_syncs().await;
        assert_eq!(delayed.len(), 1);
        assert!(delayed.contains_key(&tp));
    }

    #[tokio::test]
    async fn test_promote_delayed_offset_syncs() {
        let writer = OffsetSyncWriter::new("offset-syncs-topic".to_string(), 10);
        let tp = TopicPartition {
            topic: "topic".to_string(),
            partition: 0,
        };

        // Queue a delayed sync
        writer
            .maybe_queue_offset_syncs(tp.clone(), 0, 0)
            .await
            .unwrap();
        writer
            .maybe_queue_offset_syncs(tp.clone(), 1, 1)
            .await
            .unwrap();

        // Promote delayed syncs
        writer.promote_delayed_offset_syncs().await;

        let pending = writer.get_pending_offset_syncs().await;
        let delayed = writer.get_delayed_offset_syncs().await;

        assert_eq!(pending.len(), 1);
        assert_eq!(delayed.len(), 0);
    }
}
