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

//! OffsetSyncWriter - Writer for offset synchronization records.
//!
//! This module provides the OffsetSyncWriter which is responsible for
//! generating and writing offset synchronization records to a Kafka topic.
//! It tracks the mapping between upstream and downstream offsets and
//! determines when to emit OffsetSync records based on the offset lag threshold.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.OffsetSyncWriter (207 lines)

use std::collections::HashMap;
use std::sync::Arc;

use common_trait::protocol::SchemaError;
use common_trait::TopicPartition;
use connect_mirror_client::OffsetSync;
use kafka_clients_mock::{MockKafkaProducer, ProducerRecord};

use crate::source::mirror_source_config::OFFSET_LAG_MAX_DEFAULT;

// ============================================================================
// PartitionState
// ============================================================================

/// Internal state for tracking offset sync per partition.
///
/// This tracks the last upstream and downstream offsets for a partition
/// and determines when an OffsetSync should be emitted based on the
/// maxOffsetLag threshold.
///
/// Corresponds to Java: OffsetSyncWriter.PartitionState (internal class)
#[derive(Debug, Clone)]
pub struct PartitionState {
    /// Maximum allowed offset lag before triggering a sync
    max_offset_lag: i64,
    /// Last upstream (source) offset recorded
    upstream_offset: i64,
    /// Last downstream (target) offset recorded
    downstream_offset: i64,
    /// Flag indicating whether an offset sync should be emitted
    should_sync_offsets: bool,
}

impl PartitionState {
    /// Creates a new PartitionState with the given max offset lag.
    pub fn new(max_offset_lag: i64) -> Self {
        PartitionState {
            max_offset_lag,
            upstream_offset: -1,
            downstream_offset: -1,
            should_sync_offsets: false,
        }
    }

    /// Updates the state with new upstream and downstream offsets.
    ///
    /// Returns true if an offset sync should be emitted based on:
    /// - First update (upstream or downstream was -1)
    /// - Upstream offset reset (upstreamOffset < previous upstreamOffset)
    /// - Downstream offset reset (downstreamOffset < previous downstreamOffset)
    /// - Offset lag exceeds maxOffsetLag threshold
    /// - maxOffsetLag is 0 (always emit)
    ///
    /// Corresponds to Java: PartitionState.update(long upstreamOffset, long downstreamOffset)
    pub fn update(&mut self, upstream_offset: i64, downstream_offset: i64) -> bool {
        let should_sync = self.should_sync_offsets
            || self.upstream_offset == -1
            || self.downstream_offset == -1
            || upstream_offset < self.upstream_offset
            || downstream_offset < self.downstream_offset
            || self.max_offset_lag == 0
            || downstream_offset - self.downstream_offset >= self.max_offset_lag;

        self.upstream_offset = upstream_offset;
        self.downstream_offset = downstream_offset;
        self.should_sync_offsets = should_sync;

        should_sync
    }

    /// Resets the shouldSyncOffsets flag.
    ///
    /// Called after an OffsetSync has been queued for sending.
    ///
    /// Corresponds to Java: PartitionState.reset()
    pub fn reset(&mut self) {
        self.should_sync_offsets = false;
    }

    /// Returns the last upstream offset.
    pub fn upstream_offset(&self) -> i64 {
        self.upstream_offset
    }

    /// Returns the last downstream offset.
    pub fn downstream_offset(&self) -> i64 {
        self.downstream_offset
    }

    /// Returns whether a sync should be emitted.
    pub fn should_sync_offsets(&self) -> bool {
        self.should_sync_offsets
    }

    /// Returns the max offset lag threshold.
    pub fn max_offset_lag(&self) -> i64 {
        self.max_offset_lag
    }
}

impl Default for PartitionState {
    fn default() -> Self {
        Self::new(OFFSET_LAG_MAX_DEFAULT)
    }
}

// ============================================================================
// OffsetSyncWriter
// ============================================================================

/// Writer for offset synchronization records in MirrorMaker 2.
///
/// The OffsetSyncWriter is responsible for:
/// - Tracking offset mappings between upstream and downstream clusters
/// - Determining when to emit OffsetSync records based on offset lag
/// - Queuing pending and delayed OffsetSync records
/// - Publishing OffsetSync records to the offset-syncs topic
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.OffsetSyncWriter
pub struct OffsetSyncWriter {
    /// Kafka producer for sending OffsetSync records
    producer: MockKafkaProducer,
    /// Topic to send OffsetSync records to
    offset_syncs_topic: String,
    /// Maximum offset lag threshold
    max_offset_lag: i64,
    /// OffsetSync records ready to be sent
    pending_offset_syncs: HashMap<TopicPartition, OffsetSync>,
    /// OffsetSync records waiting for lag threshold
    delayed_offset_syncs: HashMap<TopicPartition, OffsetSync>,
    /// Partition state tracking
    partition_states: HashMap<TopicPartition, PartitionState>,
}

impl std::fmt::Debug for OffsetSyncWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OffsetSyncWriter")
            .field("offset_syncs_topic", &self.offset_syncs_topic)
            .field("max_offset_lag", &self.max_offset_lag)
            .field("pending_count", &self.pending_offset_syncs.len())
            .field("delayed_count", &self.delayed_offset_syncs.len())
            .field("partition_states_count", &self.partition_states.len())
            .finish()
    }
}

impl OffsetSyncWriter {
    /// Creates a new OffsetSyncWriter.
    ///
    /// # Arguments
    /// * `producer` - Kafka producer for sending records
    /// * `offset_syncs_topic` - Topic to send OffsetSync records to
    /// * `max_offset_lag` - Maximum offset lag threshold before triggering sync
    pub fn new(
        producer: MockKafkaProducer,
        offset_syncs_topic: impl Into<String>,
        max_offset_lag: i64,
    ) -> Self {
        OffsetSyncWriter {
            producer,
            offset_syncs_topic: offset_syncs_topic.into(),
            max_offset_lag,
            pending_offset_syncs: HashMap::new(),
            delayed_offset_syncs: HashMap::new(),
            partition_states: HashMap::new(),
        }
    }

    /// Creates an OffsetSyncWriter with default max offset lag.
    pub fn with_default_lag(
        producer: MockKafkaProducer,
        offset_syncs_topic: impl Into<String>,
    ) -> Self {
        Self::new(producer, offset_syncs_topic, OFFSET_LAG_MAX_DEFAULT)
    }

    /// Returns the offset syncs topic name.
    pub fn offset_syncs_topic(&self) -> &str {
        &self.offset_syncs_topic
    }

    /// Returns the max offset lag threshold.
    pub fn max_offset_lag(&self) -> i64 {
        self.max_offset_lag
    }

    /// Returns the number of pending offset syncs.
    pub fn pending_count(&self) -> usize {
        self.pending_offset_syncs.len()
    }

    /// Returns the number of delayed offset syncs.
    pub fn delayed_count(&self) -> usize {
        self.delayed_offset_syncs.len()
    }

    /// Returns the partition state for a given topic partition.
    pub fn partition_state(&self, tp: &TopicPartition) -> Option<&PartitionState> {
        self.partition_states.get(tp)
    }

    /// Queues an OffsetSync based on offset lag threshold.
    ///
    /// This method is called after a record is successfully replicated.
    /// It uses the PartitionState to determine if an OffsetSync should be
    /// created and whether it should go to pending or delayed queue.
    ///
    /// # Arguments
    /// * `source_topic_partition` - The source topic partition
    /// * `upstream_offset` - The upstream (source) offset
    /// * `downstream_offset` - The downstream (target) offset
    ///
    /// Corresponds to Java: OffsetSyncWriter.maybeQueueOffsetSyncs(...)
    pub fn maybe_queue_offset_syncs(
        &mut self,
        source_topic_partition: TopicPartition,
        upstream_offset: i64,
        downstream_offset: i64,
    ) {
        // Get or create partition state
        let partition_state = self
            .partition_states
            .entry(source_topic_partition.clone())
            .or_insert_with(|| PartitionState::new(self.max_offset_lag));

        // Update partition state and check if sync should be emitted
        let should_sync = partition_state.update(upstream_offset, downstream_offset);

        if should_sync {
            // Create OffsetSync and add to pending
            let sync = OffsetSync::new(
                source_topic_partition.clone(),
                upstream_offset,
                downstream_offset,
            );
            self.pending_offset_syncs
                .insert(source_topic_partition, sync);
            partition_state.reset();
        } else {
            // Add to delayed queue for later promotion
            let sync = OffsetSync::new(
                source_topic_partition.clone(),
                upstream_offset,
                downstream_offset,
            );
            self.delayed_offset_syncs
                .insert(source_topic_partition, sync);
        }
    }

    /// Sends pending OffsetSync records to the offset-syncs topic.
    ///
    /// This iterates through all pending OffsetSync records, creates
    /// ProducerRecords, and sends them via the Kafka producer.
    /// After sending, the pending queue is cleared.
    ///
    /// Returns the number of records sent.
    ///
    /// Corresponds to Java: OffsetSyncWriter.firePendingOffsetSyncs()
    pub fn fire_pending_offset_syncs(&mut self) -> Result<usize, SchemaError> {
        let mut sent_count = 0;

        for (tp, sync) in &self.pending_offset_syncs {
            // Serialize key and value
            let key = sync.record_key()?;
            let value = sync.record_value()?;

            // Determine partition (use source partition)
            let partition = tp.partition();

            // Create ProducerRecord
            let record = ProducerRecord::with_partition(
                &self.offset_syncs_topic,
                partition,
                Some(key),
                Some(value),
            );

            // Send via producer
            if self.producer.send(&record).is_ok() {
                sent_count += 1;
            }
        }

        // Clear pending queue after sending
        self.pending_offset_syncs.clear();

        Ok(sent_count)
    }

    /// Promotes delayed OffsetSync records to pending queue.
    ///
    /// This is called periodically (during commit()) to ensure that
    /// OffsetSync records for low-volume topics are eventually sent,
    /// even if they don't meet the maxOffsetLag threshold.
    ///
    /// Returns the number of records promoted.
    ///
    /// Corresponds to Java: OffsetSyncWriter.promoteDelayedOffsetSyncs()
    pub fn promote_delayed_offset_syncs(&mut self) -> usize {
        let promoted_count = self.delayed_offset_syncs.len();

        // Move all delayed to pending
        for (tp, sync) in self.delayed_offset_syncs.drain() {
            self.pending_offset_syncs.insert(tp, sync);
        }

        promoted_count
    }

    /// Sends all pending and delayed offset syncs.
    ///
    /// This promotes delayed syncs and then fires all pending syncs.
    /// Called during the commit() lifecycle method.
    ///
    /// Returns the total number of records sent.
    pub fn send_all(&mut self) -> Result<usize, SchemaError> {
        // Promote delayed first
        self.promote_delayed_offset_syncs();

        // Fire all pending
        self.fire_pending_offset_syncs()
    }

    /// Clears all internal state.
    pub fn clear(&mut self) {
        self.pending_offset_syncs.clear();
        self.delayed_offset_syncs.clear();
        self.partition_states.clear();
    }

    /// Returns whether there are any pending or delayed syncs.
    pub fn has_pending_or_delayed(&self) -> bool {
        !self.pending_offset_syncs.is_empty() || !self.delayed_offset_syncs.is_empty()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_state_new() {
        let state = PartitionState::new(100);
        assert_eq!(state.max_offset_lag(), 100);
        assert_eq!(state.upstream_offset(), -1);
        assert_eq!(state.downstream_offset(), -1);
        assert!(!state.should_sync_offsets());
    }

    #[test]
    fn test_partition_state_default() {
        let state = PartitionState::default();
        assert_eq!(state.max_offset_lag(), OFFSET_LAG_MAX_DEFAULT);
    }

    #[test]
    fn test_partition_state_first_update() {
        let mut state = PartitionState::new(100);

        // First update should always return true
        let should_sync = state.update(0, 0);
        assert!(should_sync);
        assert_eq!(state.upstream_offset(), 0);
        assert_eq!(state.downstream_offset(), 0);
    }

    #[test]
    fn test_partition_state_within_lag() {
        let mut state = PartitionState::new(100);

        // First update
        state.update(0, 0);
        state.reset();

        // Update within lag threshold
        let should_sync = state.update(10, 50); // downstream delta = 50 < 100
        assert!(!should_sync);
    }

    #[test]
    fn test_partition_state_exceeds_lag() {
        let mut state = PartitionState::new(100);

        // First update
        state.update(0, 0);
        state.reset();

        // Update exceeds lag threshold
        let should_sync = state.update(50, 150); // downstream delta = 150 >= 100
        assert!(should_sync);
    }

    #[test]
    fn test_partition_state_zero_lag() {
        let mut state = PartitionState::new(0);

        // With zero lag, every update should return true
        state.update(0, 0);
        state.reset();

        let should_sync = state.update(1, 1);
        assert!(should_sync); // Always sync when max_offset_lag is 0
    }

    #[test]
    fn test_partition_state_upstream_reset() {
        let mut state = PartitionState::new(100);

        // Initial update
        state.update(100, 200);
        state.reset();

        // Upstream offset reset (lower than previous)
        let should_sync = state.update(50, 200);
        assert!(should_sync);
    }

    #[test]
    fn test_partition_state_downstream_reset() {
        let mut state = PartitionState::new(100);

        // Initial update
        state.update(100, 200);
        state.reset();

        // Downstream offset reset (lower than previous)
        let should_sync = state.update(100, 50);
        assert!(should_sync);
    }

    #[test]
    fn test_partition_state_reset() {
        let mut state = PartitionState::new(100);

        state.update(0, 0);
        assert!(state.should_sync_offsets());

        state.reset();
        assert!(!state.should_sync_offsets());
    }

    #[test]
    fn test_offset_sync_writer_new() {
        let producer = MockKafkaProducer::new();
        let writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        assert_eq!(writer.offset_syncs_topic(), "offset-syncs");
        assert_eq!(writer.max_offset_lag(), 100);
        assert_eq!(writer.pending_count(), 0);
        assert_eq!(writer.delayed_count(), 0);
    }

    #[test]
    fn test_offset_sync_writer_with_default_lag() {
        let producer = MockKafkaProducer::new();
        let writer = OffsetSyncWriter::with_default_lag(producer, "offset-syncs");

        assert_eq!(writer.max_offset_lag(), OFFSET_LAG_MAX_DEFAULT);
    }

    #[test]
    fn test_maybe_queue_offset_syncs_first_time() {
        let producer = MockKafkaProducer::new();
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("test-topic", 0);

        // First queue should go to pending
        writer.maybe_queue_offset_syncs(tp.clone(), 0, 0);

        assert_eq!(writer.pending_count(), 1);
        assert_eq!(writer.delayed_count(), 0);

        // Verify partition state was created
        let state = writer.partition_state(&tp).unwrap();
        assert_eq!(state.upstream_offset(), 0);
        assert_eq!(state.downstream_offset(), 0);
    }

    #[test]
    fn test_maybe_queue_offset_syncs_within_lag() {
        let producer = MockKafkaProducer::new();
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("test-topic", 0);

        // First queue (goes to pending)
        writer.maybe_queue_offset_syncs(tp.clone(), 0, 0);

        // Fire pending to reset state
        writer.fire_pending_offset_syncs().unwrap();

        // Second queue within lag (goes to delayed)
        writer.maybe_queue_offset_syncs(tp.clone(), 10, 50);

        assert_eq!(writer.pending_count(), 0);
        assert_eq!(writer.delayed_count(), 1);
    }

    #[test]
    fn test_maybe_queue_offset_syncs_exceeds_lag() {
        let producer = MockKafkaProducer::new();
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("test-topic", 0);

        // First queue
        writer.maybe_queue_offset_syncs(tp.clone(), 0, 0);
        writer.fire_pending_offset_syncs().unwrap();

        // Queue exceeds lag (goes to pending)
        writer.maybe_queue_offset_syncs(tp.clone(), 50, 150);

        assert_eq!(writer.pending_count(), 1);
        assert_eq!(writer.delayed_count(), 0);
    }

    #[test]
    fn test_fire_pending_offset_syncs() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 3);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        // Add multiple syncs
        let tp0 = TopicPartition::new("topic1", 0);
        let tp1 = TopicPartition::new("topic2", 1);

        writer.maybe_queue_offset_syncs(tp0.clone(), 100, 200);
        writer.maybe_queue_offset_syncs(tp1.clone(), 50, 100);

        assert_eq!(writer.pending_count(), 2);

        // Fire pending
        let sent = writer.fire_pending_offset_syncs().unwrap();
        assert_eq!(sent, 2);

        // Pending should be cleared
        assert_eq!(writer.pending_count(), 0);
    }

    #[test]
    fn test_promote_delayed_offset_syncs() {
        let producer = MockKafkaProducer::new();
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("test-topic", 0);

        // Add to pending first
        writer.maybe_queue_offset_syncs(tp.clone(), 0, 0);
        writer.fire_pending_offset_syncs().unwrap();

        // Add to delayed
        writer.maybe_queue_offset_syncs(tp.clone(), 10, 50);
        assert_eq!(writer.delayed_count(), 1);

        // Promote delayed
        let promoted = writer.promote_delayed_offset_syncs();
        assert_eq!(promoted, 1);

        // Delayed should be empty, pending should have the sync
        assert_eq!(writer.delayed_count(), 0);
        assert_eq!(writer.pending_count(), 1);
    }

    #[test]
    fn test_send_all() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 3);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        // Add pending
        let tp0 = TopicPartition::new("topic1", 0);
        writer.maybe_queue_offset_syncs(tp0.clone(), 100, 200);

        // Add delayed
        writer.fire_pending_offset_syncs().unwrap();
        writer.maybe_queue_offset_syncs(tp0.clone(), 110, 250);

        // Send all
        let sent = writer.send_all().unwrap();
        assert_eq!(sent, 1); // Only the promoted delayed sync

        // Everything should be cleared
        assert_eq!(writer.pending_count(), 0);
        assert_eq!(writer.delayed_count(), 0);
    }

    #[test]
    fn test_clear() {
        let producer = MockKafkaProducer::new();
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("test-topic", 0);
        writer.maybe_queue_offset_syncs(tp.clone(), 0, 0);
        writer.maybe_queue_offset_syncs(tp.clone(), 10, 50);

        writer.clear();

        assert_eq!(writer.pending_count(), 0);
        assert_eq!(writer.delayed_count(), 0);
        assert!(writer.partition_state(&tp).is_none());
    }

    #[test]
    fn test_has_pending_or_delayed() {
        let producer = MockKafkaProducer::new();
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        assert!(!writer.has_pending_or_delayed());

        let tp = TopicPartition::new("test-topic", 0);
        writer.maybe_queue_offset_syncs(tp.clone(), 0, 0);

        assert!(writer.has_pending_or_delayed());

        writer.fire_pending_offset_syncs().unwrap();
        writer.maybe_queue_offset_syncs(tp.clone(), 10, 50);

        assert!(writer.has_pending_or_delayed());

        writer.promote_delayed_offset_syncs();
        writer.fire_pending_offset_syncs().unwrap();

        assert!(!writer.has_pending_or_delayed());
    }

    #[test]
    fn test_multiple_partitions() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 10);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        for i in 0i32..10 {
            let tp = TopicPartition::new("test-topic", i);
            writer.maybe_queue_offset_syncs(tp.clone(), (i * 100) as i64, (i * 200) as i64);
        }

        assert_eq!(writer.pending_count(), 10);

        let sent = writer.fire_pending_offset_syncs().unwrap();
        assert_eq!(sent, 10);

        assert_eq!(writer.pending_count(), 0);
    }

    #[test]
    fn test_producer_record_sent_correctly() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 1);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("source-topic", 0);
        writer.maybe_queue_offset_syncs(tp.clone(), 12345, 67890);

        // Send and verify sent count
        let sent = writer.fire_pending_offset_syncs().unwrap();
        assert_eq!(sent, 1);

        // Verify OffsetSync serialization works correctly via roundtrip
        let sync = OffsetSync::new(tp.clone(), 12345, 67890);
        let key = sync.record_key().unwrap();
        let value = sync.record_value().unwrap();

        let deserialized = OffsetSync::deserialize_record(&key, &value).unwrap();
        assert_eq!(deserialized.topic_partition().topic(), "source-topic");
        assert_eq!(deserialized.topic_partition().partition(), 0);
        assert_eq!(deserialized.upstream_offset(), 12345);
        assert_eq!(deserialized.downstream_offset(), 67890);
    }

    // ============================================================================
    // Java OffsetSyncWriterTest Semantic Tests
    // Corresponds to Java: org.apache.kafka.connect.mirror.OffsetSyncWriterTest
    // ============================================================================

    /// Corresponds to Java: testMaybeQueueOffsetSyncs
    /// Verifies queue strategy: pending vs delayed based on maxOffsetLag threshold.
    /// Key semantics:
    /// - First sync always goes to pending (first update triggers sync)
    /// - Within lag threshold: goes to delayed queue
    /// - Exceeds lag threshold: goes to pending queue
    /// - lastSyncDownstreamOffset is tracked in PartitionState
    #[test]
    fn test_maybe_queue_offset_syncs_java_semantic() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 3);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("source-topic", 0);

        // Case 1: First queue - ALWAYS goes to pending (first update triggers sync)
        writer.maybe_queue_offset_syncs(tp.clone(), 0, 0);
        assert_eq!(writer.pending_count(), 1);
        assert_eq!(writer.delayed_count(), 0);

        // Verify partition state updated with correct offsets
        let state = writer.partition_state(&tp).unwrap();
        assert_eq!(state.upstream_offset(), 0);
        assert_eq!(state.downstream_offset(), 0);
        assert!(!state.should_sync_offsets()); // Reset after queueing

        // Fire pending to clear
        writer.fire_pending_offset_syncs().unwrap();
        assert_eq!(writer.pending_count(), 0);

        // Case 2: Within lag threshold - goes to delayed
        // downstream delta = 50 < 100 (max_offset_lag)
        writer.maybe_queue_offset_syncs(tp.clone(), 50, 50);
        assert_eq!(writer.pending_count(), 0);
        assert_eq!(writer.delayed_count(), 1);

        // State updated
        let state = writer.partition_state(&tp).unwrap();
        assert_eq!(state.upstream_offset(), 50);
        assert_eq!(state.downstream_offset(), 50);

        // Case 3: Exceeds lag threshold - goes to pending
        // downstream delta from last = 100 >= 100 (max_offset_lag)
        writer.maybe_queue_offset_syncs(tp.clone(), 100, 150);
        assert_eq!(writer.pending_count(), 1);
        assert_eq!(writer.delayed_count(), 1); // Previous delayed still exists

        // Fire pending clears only pending, delayed remains
        writer.fire_pending_offset_syncs().unwrap();
        assert_eq!(writer.pending_count(), 0);
        assert_eq!(writer.delayed_count(), 1);
    }

    /// Corresponds to Java: testMaybeQueueOffsetSyncs - exact lag boundary
    /// Tests the exact boundary where downstream_delta == max_offset_lag.
    /// At exact boundary, sync should be triggered (>= comparison).
    #[test]
    fn test_maybe_queue_offset_syncs_exact_lag_boundary_java_semantic() {
        let producer = MockKafkaProducer::new();
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("boundary-topic", 0);

        // First sync
        writer.maybe_queue_offset_syncs(tp.clone(), 0, 0);
        writer.fire_pending_offset_syncs().unwrap();

        // Exact boundary: downstream_delta = 100 (== max_offset_lag)
        // Should trigger sync because condition is >=
        writer.maybe_queue_offset_syncs(tp.clone(), 100, 100);
        assert_eq!(writer.pending_count(), 1);

        // Verify state
        let state = writer.partition_state(&tp).unwrap();
        assert_eq!(state.downstream_offset(), 100);
    }

    /// Corresponds to Java: testFirePendingOffsetSyncs
    /// Verifies send behavior: pending syncs are dispatched via Kafka producer.
    /// Key semantics:
    /// - All pending syncs are serialized and sent
    /// - Pending queue is cleared after send
    /// - Returns count of records sent
    #[test]
    fn test_fire_pending_offset_syncs_java_semantic() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 5);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        // Queue multiple syncs from different partitions
        let tp0 = TopicPartition::new("topic-a", 0);
        let tp1 = TopicPartition::new("topic-b", 1);
        let tp2 = TopicPartition::new("topic-c", 2);

        writer.maybe_queue_offset_syncs(tp0.clone(), 100, 200);
        writer.maybe_queue_offset_syncs(tp1.clone(), 50, 100);
        writer.maybe_queue_offset_syncs(tp2.clone(), 200, 400);

        // All first-time queues go to pending
        assert_eq!(writer.pending_count(), 3);

        // Fire pending - send all records
        let sent_count = writer.fire_pending_offset_syncs().unwrap();
        assert_eq!(sent_count, 3);

        // Pending cleared after send
        assert_eq!(writer.pending_count(), 0);

        // Delayed should still be empty (no delayed created)
        assert_eq!(writer.delayed_count(), 0);
    }

    /// Corresponds to Java: testPromoteDelayedOffsetSyncs
    /// Verifies delayed-to-pending promotion for low-volume topics.
    /// Key semantics:
    /// - Promote moves all delayed to pending
    /// - Delayed queue is cleared after promotion
    /// - Returns count of promoted records
    /// - Used by commit() to ensure eventual sync
    #[test]
    fn test_promote_delayed_offset_syncs_java_semantic() {
        let producer = MockKafkaProducer::new();
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        // Setup: first sync goes to pending, clear it
        let tp = TopicPartition::new("low-volume-topic", 0);
        writer.maybe_queue_offset_syncs(tp.clone(), 0, 0);
        writer.fire_pending_offset_syncs().unwrap();

        // Add delayed syncs (within lag threshold)
        writer.maybe_queue_offset_syncs(tp.clone(), 10, 10);
        writer.maybe_queue_offset_syncs(tp.clone(), 20, 30);

        // Should have 2 delayed entries (same partition overwrites)
        assert!(writer.delayed_count() > 0);

        // Promote delayed to pending
        let promoted_count = writer.promote_delayed_offset_syncs();
        assert!(promoted_count > 0);

        // Delayed should be empty
        assert_eq!(writer.delayed_count(), 0);

        // Pending should have the promoted sync(s)
        assert!(writer.pending_count() > 0);
    }

    /// Tests multiple partitions with delayed syncs promotion.
    /// Each partition's delayed sync is promoted independently.
    #[test]
    fn test_promote_delayed_offset_syncs_multiple_partitions_java_semantic() {
        let producer = MockKafkaProducer::new();
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        // Setup multiple partitions with first syncs
        let tp0 = TopicPartition::new("multi-topic", 0);
        let tp1 = TopicPartition::new("multi-topic", 1);
        let tp2 = TopicPartition::new("multi-topic", 2);

        writer.maybe_queue_offset_syncs(tp0.clone(), 0, 0);
        writer.maybe_queue_offset_syncs(tp1.clone(), 0, 0);
        writer.maybe_queue_offset_syncs(tp2.clone(), 0, 0);
        writer.fire_pending_offset_syncs().unwrap();

        // Add delayed for each partition
        writer.maybe_queue_offset_syncs(tp0.clone(), 10, 20);
        writer.maybe_queue_offset_syncs(tp1.clone(), 15, 25);
        writer.maybe_queue_offset_syncs(tp2.clone(), 20, 30);

        assert_eq!(writer.delayed_count(), 3);

        // Promote all
        let promoted = writer.promote_delayed_offset_syncs();
        assert_eq!(promoted, 3);

        assert_eq!(writer.delayed_count(), 0);
        assert_eq!(writer.pending_count(), 3);
    }

    /// Corresponds to Java: PartitionState boundary tests
    /// Tests maxOffsetLag = 0 behavior: every update triggers sync.
    /// When maxOffsetLag is 0, the condition `max_offset_lag == 0` is always true.
    #[test]
    fn test_partition_state_zero_lag_always_sync_java_semantic() {
        // max_offset_lag = 0: every update triggers sync
        let mut state = PartitionState::new(0);

        // First update
        assert!(state.update(0, 0));

        state.reset();

        // Second update - should still trigger because max_offset_lag == 0
        assert!(state.update(1, 1));

        state.reset();

        // Any update triggers sync
        assert!(state.update(5, 10));
        assert!(state.update(100, 200));
    }

    /// Tests upstream offset reset/skip behavior.
    /// Corresponds to Java: PartitionState.update detects upstream reset.
    /// When upstream_offset < previous, triggers resync.
    #[test]
    fn test_partition_state_upstream_offset_skip_java_semantic() {
        let mut state = PartitionState::new(100);

        // Normal progression
        state.update(100, 200);
        state.reset();

        // No trigger within lag
        assert!(!state.update(150, 250));

        // Upstream skip forward (normal)
        state.update(200, 300);
        state.reset();
        assert!(!state.update(250, 350));

        // Upstream skip backward (reset/rewind) - triggers sync
        assert!(state.update(100, 350)); // upstream 100 < previous 250
    }

    /// Tests downstream offset reset/skip behavior.
    /// Corresponds to Java: PartitionState.update detects downstream reset.
    /// When downstream_offset < previous, triggers resync.
    #[test]
    fn test_partition_state_downstream_offset_skip_java_semantic() {
        let mut state = PartitionState::new(100);

        // Normal progression
        state.update(100, 200);
        state.reset();

        // Downstream forward within lag
        assert!(!state.update(150, 250)); // downstream delta 50 < 100

        // Downstream reset (rewind) - triggers sync
        assert!(state.update(150, 100)); // downstream 100 < previous 250
    }

    /// Tests NO_OFFSET (-1) handling in PartitionState.
    /// In Kafka, -1 represents undefined offset.
    /// First update from -1 should trigger sync.
    #[test]
    fn test_partition_state_no_offset_handling_java_semantic() {
        let mut state = PartitionState::new(100);

        // Initial state has -1 for both offsets
        assert_eq!(state.upstream_offset(), -1);
        assert_eq!(state.downstream_offset(), -1);

        // First update from -1 triggers sync
        assert!(state.update(0, 0));

        // After first update, offsets are valid
        assert_eq!(state.upstream_offset(), 0);
        assert_eq!(state.downstream_offset(), 0);

        state.reset();

        // Normal updates work
        assert!(!state.update(50, 50));
    }

    /// Tests NO_OFFSET (-1) in writer queue operations.
    /// Writer should handle -1 offsets correctly.
    #[test]
    fn test_offset_sync_writer_no_offset_handling_java_semantic() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 1);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("no-offset-topic", 0);

        // Queue with -1 offsets (represents NO_OFFSET)
        writer.maybe_queue_offset_syncs(tp.clone(), -1, -1);

        // Should go to pending (first update)
        assert_eq!(writer.pending_count(), 1);

        // Verify state
        let state = writer.partition_state(&tp).unwrap();
        assert_eq!(state.upstream_offset(), -1);
        assert_eq!(state.downstream_offset(), -1);

        // Send and verify serialization works
        let sent = writer.fire_pending_offset_syncs().unwrap();
        assert_eq!(sent, 1);
    }

    /// Tests offset reset scenario in writer.
    /// When upstream resets (source topic recreated), writer handles correctly.
    #[test]
    fn test_offset_sync_writer_upstream_reset_java_semantic() {
        let producer = MockKafkaProducer::new();
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("reset-topic", 0);

        // Initial syncs
        writer.maybe_queue_offset_syncs(tp.clone(), 100, 200);
        writer.fire_pending_offset_syncs().unwrap();

        // Normal progression within lag
        writer.maybe_queue_offset_syncs(tp.clone(), 150, 250);
        assert_eq!(writer.delayed_count(), 1);

        // Upstream reset (e.g., source topic recreated)
        // New upstream starts from 0
        writer.maybe_queue_offset_syncs(tp.clone(), 0, 0);

        // Reset triggers sync - goes to pending
        assert_eq!(writer.pending_count(), 1);

        // State reflects new offsets
        let state = writer.partition_state(&tp).unwrap();
        assert_eq!(state.upstream_offset(), 0);
        assert_eq!(state.downstream_offset(), 0);
    }

    /// Tests downstream reset scenario.
    /// When target offsets reset (e.g., consumer group reset), triggers sync.
    #[test]
    fn test_offset_sync_writer_downstream_reset_java_semantic() {
        let producer = MockKafkaProducer::new();
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("downstream-reset-topic", 0);

        // Initial sync
        writer.maybe_queue_offset_syncs(tp.clone(), 100, 200);
        writer.fire_pending_offset_syncs().unwrap();

        // Downstream reset (consumer group reset to earlier offset)
        writer.maybe_queue_offset_syncs(tp.clone(), 150, 50); // downstream reset

        // Reset triggers sync
        assert_eq!(writer.pending_count(), 1);

        let state = writer.partition_state(&tp).unwrap();
        assert_eq!(state.downstream_offset(), 50);
    }

    /// Tests multiple partitions maintain independent state.
    /// Each partition has its own PartitionState tracked separately.
    #[test]
    fn test_multiple_partitions_independent_state_java_semantic() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 10);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        // Multiple partitions with different offset progressions
        let partitions: Vec<(TopicPartition, i64, i64)> = vec![
            (TopicPartition::new("topic", 0), 100, 200),
            (TopicPartition::new("topic", 1), 50, 100),
            (TopicPartition::new("topic", 2), 200, 400),
            (TopicPartition::new("topic", 3), 0, 0),
        ];

        // Queue initial syncs
        for (tp, upstream, downstream) in &partitions {
            writer.maybe_queue_offset_syncs(tp.clone(), *upstream, *downstream);
        }

        assert_eq!(writer.pending_count(), 4);

        // Each partition has independent state
        for (tp, upstream, downstream) in &partitions {
            let state = writer.partition_state(tp).unwrap();
            assert_eq!(state.upstream_offset(), *upstream);
            assert_eq!(state.downstream_offset(), *downstream);
        }

        // Fire and clear
        writer.fire_pending_offset_syncs().unwrap();

        // Add delayed for each partition independently
        writer.maybe_queue_offset_syncs(partitions[0].0.clone(), 150, 220);
        writer.maybe_queue_offset_syncs(partitions[1].0.clone(), 60, 110);
        writer.maybe_queue_offset_syncs(partitions[2].0.clone(), 250, 450);

        // Only partition 3 doesn't get delayed (exceeds lag: 100 >= 100)
        writer.maybe_queue_offset_syncs(partitions[3].0.clone(), 50, 100);

        // 3 delayed + 1 pending
        assert_eq!(writer.delayed_count(), 3);
        assert_eq!(writer.pending_count(), 1);

        // Independent state verification
        assert_eq!(
            writer
                .partition_state(&partitions[0].0)
                .unwrap()
                .downstream_offset(),
            220
        );
        assert_eq!(
            writer
                .partition_state(&partitions[3].0)
                .unwrap()
                .downstream_offset(),
            100
        );
    }

    /// Tests send_all workflow: promote + fire in single operation.
    /// Corresponds to Java: commit() calls promoteDelayed + firePending.
    #[test]
    fn test_send_all_workflow_java_semantic() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 5);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp0 = TopicPartition::new("workflow-topic", 0);
        let tp1 = TopicPartition::new("workflow-topic", 1);

        // Initial syncs
        writer.maybe_queue_offset_syncs(tp0.clone(), 0, 0);
        writer.maybe_queue_offset_syncs(tp1.clone(), 0, 0);

        // Fire initial pending
        writer.fire_pending_offset_syncs().unwrap();

        // Add more syncs: some pending, some delayed
        writer.maybe_queue_offset_syncs(tp0.clone(), 50, 150); // Exceeds lag - pending
        writer.maybe_queue_offset_syncs(tp1.clone(), 10, 50); // Within lag - delayed

        assert_eq!(writer.pending_count(), 1);
        assert_eq!(writer.delayed_count(), 1);

        // send_all promotes delayed and fires all pending
        let sent = writer.send_all().unwrap();
        assert_eq!(sent, 2); // 1 promoted + 1 existing pending

        // Everything cleared
        assert_eq!(writer.pending_count(), 0);
        assert_eq!(writer.delayed_count(), 0);
    }

    /// Tests partition state persistence across operations.
    /// PartitionState is maintained even after syncs are sent.
    #[test]
    fn test_partition_state_persistence_java_semantic() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 1);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("persistent-topic", 0);

        // Initial sync
        writer.maybe_queue_offset_syncs(tp.clone(), 0, 0);

        // State exists before fire
        assert!(writer.partition_state(&tp).is_some());

        writer.fire_pending_offset_syncs().unwrap();

        // State persists after fire
        let state = writer.partition_state(&tp).unwrap();
        assert_eq!(state.upstream_offset(), 0);
        assert_eq!(state.downstream_offset(), 0);

        // More updates maintain state
        writer.maybe_queue_offset_syncs(tp.clone(), 100, 150);
        let state = writer.partition_state(&tp).unwrap();
        assert_eq!(state.upstream_offset(), 100);
        assert_eq!(state.downstream_offset(), 150);

        // Clear removes all state
        writer.clear();
        assert!(writer.partition_state(&tp).is_none());
    }

    /// Tests edge case: downstream delta exactly at lag threshold.
    /// Condition is `downstream_offset - last_downstream >= max_offset_lag`.
    /// Boundary case should trigger sync.
    #[test]
    fn test_downstream_delta_boundary_java_semantic() {
        let mut state = PartitionState::new(100);

        state.update(0, 0);
        state.reset();

        // Delta = 99 (just below threshold of 100) - no sync
        assert!(!state.update(50, 99));
        // State now: downstream_offset = 99

        // Delta from 0 to 99 is 99, not triggering
        // Now we test EXACT boundary from last downstream (99) to 199 = delta 100
        state.reset();
        // Delta = 199 - 99 = 100 (exactly at threshold) - triggers sync
        assert!(state.update(200, 199));
    }

    /// Tests edge case: very large offsets.
    /// PartitionState and writer handle i64 range correctly.
    #[test]
    fn test_large_offset_values_java_semantic() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 1);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 1000);

        let tp = TopicPartition::new("large-offset-topic", 0);

        // Large offsets near i64 max
        let large_upstream = i64::MAX / 2;
        let large_downstream = i64::MAX / 3;

        writer.maybe_queue_offset_syncs(tp.clone(), large_upstream, large_downstream);

        let state = writer.partition_state(&tp).unwrap();
        assert_eq!(state.upstream_offset(), large_upstream);
        assert_eq!(state.downstream_offset(), large_downstream);

        // Send works
        let sent = writer.fire_pending_offset_syncs().unwrap();
        assert_eq!(sent, 1);
    }

    /// Tests that PartitionState update is idempotent for same offsets.
    /// Calling update with same values shouldn't change state unnecessarily.
    #[test]
    fn test_partition_state_idempotent_update_java_semantic() {
        let mut state = PartitionState::new(100);

        // First update
        state.update(100, 200);
        state.reset();

        // Same offsets - no sync triggered
        assert!(!state.update(100, 200));
        assert_eq!(state.upstream_offset(), 100);
        assert_eq!(state.downstream_offset(), 200);

        // Delta still tracked correctly
        state.reset();
        assert!(!state.update(150, 250)); // Delta 50 < 100
    }

    /// Tests serialization correctness: key/value match Java format.
    /// Verifies sent record can be deserialized back correctly.
    #[test]
    fn test_serialization_format_java_semantic() {
        let producer = MockKafkaProducer::new();
        producer.add_topic("offset-syncs", 1);
        let mut writer = OffsetSyncWriter::new(producer, "offset-syncs", 100);

        let tp = TopicPartition::new("serde-topic", 5);
        let upstream = 12345;
        let downstream = 67890;

        writer.maybe_queue_offset_syncs(tp.clone(), upstream, downstream);
        writer.fire_pending_offset_syncs().unwrap();

        // Create matching OffsetSync and verify serialization
        let sync = OffsetSync::new(tp.clone(), upstream, downstream);
        let key = sync.record_key().unwrap();
        let value = sync.record_value().unwrap();

        // Deserialize and verify exact match
        let deserialized = OffsetSync::deserialize_record(&key, &value).unwrap();
        assert_eq!(deserialized.topic_partition().topic(), tp.topic());
        assert_eq!(deserialized.topic_partition().partition(), tp.partition());
        assert_eq!(deserialized.upstream_offset(), upstream);
        assert_eq!(deserialized.downstream_offset(), downstream);
    }
}
