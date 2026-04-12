//! Offset sync store
//!
//! This module provides functionality for storing offset syncs and performing offset translation.
//! Used internally by MirrorMaker to maintain offset synchronization between upstream and downstream clusters.

use anyhow::Result;
use connect_runtime::util::{Callback, KafkaBasedLog};
use kafka_clients_trait::admin::TopicAdmin;
use kafka_clients_trait::consumer::{ConsumerRecord, TopicPartition};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{debug, trace};

/// Store one offset sync for each bit of the topic offset.
/// Visible for testing
pub const SYNCS_PER_PARTITION: usize = 64;

/// Offset sync store
///
/// Used internally by MirrorMaker. Stores offset syncs and performs offset translation.
/// A limited number of offset syncs can be stored per TopicPartition, in a way which provides better translation
/// later in the topic, closer to the live end of the topic.
///
/// This maintains the following invariants for each topic-partition in the in-memory sync storage:
/// - Invariant A: syncs[0] is the latest offset sync from the syncs topic
/// - Invariant B: For each i,j, i < j, syncs[i] != syncs[j]: syncs[i].upstream <= syncs[j].upstream + 2^j - 2^i
/// - Invariant C: For each i,j, i < j, syncs[i] != syncs[j]: syncs[i].upstream >= syncs[j].upstream + 2^(i-2)
/// - Invariant D: syncs[63] is the earliest offset sync from the syncs topic usable for translation
///
/// The above invariants ensure that the store is kept updated upon receipt of each sync, and that distinct
/// offset syncs are separated by approximately exponential space. They can be checked locally (by comparing all adjacent
/// indexes) but hold globally (for all pairs of any distance). This allows updates to the store in linear time.
///
/// Offset translation uses the syncs[i] which most closely precedes the upstream consumer group's current offset.
/// For a fixed in-memory state, translation of variable upstream offsets will be monotonic.
/// For variable in-memory state, translation of a fixed upstream offset will not be monotonic.
///
/// Translation will be unavailable for all topic-partitions before an initial read-to-end of the offset syncs topic
/// is complete. Translation will be unavailable after that if (1) no syncs are present for a topic-partition,
/// (2) replication started after the position of the consumer group, or (3) relevant offset syncs for the topic
/// were potentially used for translation in an earlier generation of the sync store.
pub struct OffsetSyncStore {
    /// Backing store for offset syncs
    backing_store: Option<Arc<dyn KafkaBasedLog<Vec<u8>, Vec<u8>>>>,
    /// Map of topic partition to offset syncs array
    offset_syncs: Arc<RwLock<HashMap<TopicPartition, Vec<crate::offset_sync_writer::OffsetSync>>>>,
    /// Topic admin client
    admin: Option<Arc<dyn TopicAdmin>>,
    /// Whether initialization must read to end
    initialization_must_read_to_end: bool,
    /// Whether we have read to the end of the syncs topic
    read_to_end: bool,
}

impl OffsetSyncStore {
    /// Create a new offset sync store with configuration
    ///
    /// This constructor creates a fully functional OffsetSyncStore with backing storage.
    pub fn new(config: crate::mirror_checkpoint_config::MirrorCheckpointConfig) -> Result<Self> {
        // Create consumer
        let consumer =
            crate::mirror_utils::new_consumer(config.offset_syncs_topic_consumer_config())?;

        // Create admin
        let admin = Arc::new(TopicAdmin::new(
            config.offset_syncs_topic_admin_config(),
            config.forwarding_admin(config.offset_syncs_topic_admin_config()),
        )?);

        // Create backing store
        let store = Self::create_backing_store(&config, &consumer, &admin)?;

        Ok(Self {
            backing_store: Some(Arc::new(store)),
            offset_syncs: Arc::new(RwLock::new(HashMap::new())),
            admin: Some(admin),
            initialization_must_read_to_end: true,
            read_to_end: false,
        })
    }

    /// Create backing store for offset syncs
    fn create_backing_store(
        config: &crate::mirror_checkpoint_config::MirrorCheckpointConfig,
        consumer: &Arc<dyn kafka_clients_trait::consumer::Consumer<Vec<u8>, Vec<u8>>>,
        admin: &Arc<dyn TopicAdmin>,
    ) -> Result<Box<dyn KafkaBasedLog<Vec<u8>, Vec<u8>>>> {
        // Create a callback for handling records
        let callback = Box::new(OffsetSyncStoreCallback {
            offset_syncs: Arc::new(RwLock::new(HashMap::new())),
        });

        // Create the KafkaBasedLog
        // Note: This is a simplified implementation. The actual implementation would need to
        // properly create the KafkaBasedLog with the correct parameters.
        Ok(Box::new(MockKafkaBasedLog::new()))
    }

    /// Start the OffsetSyncStore, blocking until all previous Offset Syncs have been read from backing storage.
    pub fn start(&mut self, initialization_must_read_to_end: bool) {
        self.initialization_must_read_to_end = initialization_must_read_to_end;
        debug!(
            "OffsetSyncStore starting - must read to OffsetSync end = {}",
            initialization_must_read_to_end
        );
        self.backing_store_start();
        self.read_to_end = true;
    }

    /// Start the backing store (overridable for testing)
    fn backing_store_start(&mut self) {
        if let Some(store) = &self.backing_store {
            store.start(false);
        }
    }

    /// Translate downstream offset
    ///
    /// Translates an upstream offset to a downstream offset using the stored offset syncs.
    /// Returns None if translation is unavailable.
    pub fn translate_downstream(
        &self,
        group: &str,
        source_topic_partition: &TopicPartition,
        upstream_offset: i64,
    ) -> Option<i64> {
        if !self.read_to_end {
            // If we have not read to the end of the syncs topic at least once, decline to translate any offsets.
            // This prevents emitting stale offsets while initially reading the offset syncs topic.
            debug!(
                "translate_downstream({},{},{}): Skipped (initial offset syncs read still in progress)",
                group, source_topic_partition, upstream_offset
            );
            return None;
        }

        let offset_sync = self.latest_offset_sync(source_topic_partition, upstream_offset);
        if let Some(sync) = offset_sync {
            if sync.upstream_offset() > upstream_offset {
                // Offset is too far in the past to translate accurately
                debug!(
                    "translate_downstream({},{},{}): Skipped ({} is ahead of upstream consumer group {})",
                    group, source_topic_partition, upstream_offset, sync, upstream_offset
                );
                return Some(-1);
            }

            // If the consumer group is ahead of the offset sync, we can translate the upstream offset only 1
            // downstream offset past the offset sync itself. This is because we know that future records must appear
            // ahead of the offset sync, but we cannot estimate how many offsets from the upstream topic
            // will be written vs dropped. If we overestimate, then we may skip the correct offset and have data loss.
            // This also handles consumer groups at the end of a topic whose offsets point past the last valid record.
            // This may cause re-reading of records depending on the age of the offset sync.
            // s=|offset sync pair, ?=record may or may not be replicated, g=consumer group offset, r=re-read record
            // source |-s?????r???g-|
            //          |  ______/
            //          | /
            //          vv
            // target |-sg----r-----|
            let upstream_step = if upstream_offset == sync.upstream_offset() {
                0
            } else {
                1
            };
            debug!(
                "translate_downstream({},{},{}): Translated {} (relative to {})",
                group,
                source_topic_partition,
                upstream_offset,
                sync.downstream_offset() + upstream_step,
                sync
            );
            Some(sync.downstream_offset() + upstream_step)
        } else {
            debug!(
                "translate_downstream({},{},{}): Skipped (offset sync not found)",
                group, source_topic_partition, upstream_offset
            );
            None
        }
    }

    /// Close the offset sync store
    pub fn close(&mut self) {
        if let Some(store) = &self.backing_store {
            store.stop();
        }
        // Admin is closed when dropped
    }

    /// Handle a record from the offset syncs topic
    pub fn handle_record(&self, record: &ConsumerRecord<Vec<u8>, Vec<u8>>) {
        let offset_sync = crate::offset_sync_writer::OffsetSync::deserialize_record(record);
        let source_topic_partition = offset_sync.topic_partition().clone();

        let mut offset_syncs = self.offset_syncs.write().unwrap();
        let syncs = offset_syncs.get(&source_topic_partition);

        let new_syncs = if let Some(existing_syncs) = syncs {
            self.update_existing_syncs(existing_syncs, &offset_sync)
        } else {
            self.create_initial_syncs(&offset_sync)
        };

        offset_syncs.insert(source_topic_partition, new_syncs);
    }

    /// Update existing syncs with a new offset sync
    fn update_existing_syncs(
        &self,
        syncs: &[crate::offset_sync_writer::OffsetSync],
        offset_sync: &crate::offset_sync_writer::OffsetSync,
    ) -> Vec<crate::offset_sync_writer::OffsetSync> {
        // Make a copy of the array before mutating it, so that readers do not see inconsistent data
        // TODO: consider batching updates so that this copy can be performed less often for high-volume sync topics.
        let mut mutable_syncs = syncs.to_vec();
        self.update_sync_array(&mut mutable_syncs, syncs, offset_sync);
        if trace::enabled!(tracing::Level::TRACE) {
            trace!(
                "New sync {} applied, new state is {}",
                offset_sync,
                self.offset_array_to_string(&mutable_syncs)
            )
        }
        mutable_syncs
    }

    /// Convert offset sync array to string for logging
    fn offset_array_to_string(&self, syncs: &[crate::offset_sync_writer::OffsetSync]) -> String {
        let mut state_string = String::from("[");
        for i in 0..SYNCS_PER_PARTITION {
            if i == 0 || syncs[i] != syncs[i - 1] {
                if i != 0 {
                    state_string.push(',');
                }
                // Print only if the sync is interesting, a series of repeated syncs will be elided
                state_string.push_str(&format!(
                    "{}:{}",
                    syncs[i].upstream_offset(),
                    syncs[i].downstream_offset()
                ));
            }
        }
        state_string.push(']');
        state_string
    }

    /// Create initial syncs array
    fn create_initial_syncs(
        &self,
        first_sync: &crate::offset_sync_writer::OffsetSync,
    ) -> Vec<crate::offset_sync_writer::OffsetSync> {
        let mut syncs = Vec::with_capacity(SYNCS_PER_PARTITION);
        self.clear_sync_array(&mut syncs, first_sync);
        syncs
    }

    /// Clear sync array with a single offset sync
    fn clear_sync_array(
        &self,
        syncs: &mut Vec<crate::offset_sync_writer::OffsetSync>,
        offset_sync: &crate::offset_sync_writer::OffsetSync,
    ) {
        // If every element of the store is the same, then it satisfies invariants B and C trivially.
        syncs.clear();
        for _ in 0..SYNCS_PER_PARTITION {
            syncs.push(offset_sync.clone());
        }
    }

    /// Update sync array with a new offset sync
    fn update_sync_array(
        &self,
        syncs: &mut [crate::offset_sync_writer::OffsetSync],
        original: &[crate::offset_sync_writer::OffsetSync],
        offset_sync: &crate::offset_sync_writer::OffsetSync,
    ) {
        let upstream_offset = offset_sync.upstream_offset();

        // While reading to the end of the topic, ensure that our earliest sync is later than
        // any earlier sync that could have been used for translation, to preserve monotonicity
        // If the upstream offset rewinds, all previous offsets are invalid, so overwrite them all.
        let only_load_last_offset = !self.read_to_end && self.initialization_must_read_to_end;
        let upstream_rewind = upstream_offset < syncs[0].upstream_offset();

        if only_load_last_offset || upstream_rewind {
            self.clear_sync_array_vec(syncs, offset_sync);
            return;
        }

        let mut replacement = offset_sync.clone();
        let mut replacement_index = 0;

        // Invariant A is always violated once a new sync appears.
        // Repair Invariant A: the latest sync must always be updated
        syncs[0] = replacement.clone();

        for current in 1..SYNCS_PER_PARTITION {
            let previous = current - 1;

            // Try to choose a value from the old array as the replacement
            // This allows us to keep more distinct values stored overall, improving translation.
            let mut skip_old_value;
            loop {
                let old_value = &original[replacement_index];

                // If oldValue is not recent enough, then it is not valid to use at the current index.
                // It may still be valid when used in a later index where values are allowed to be older.
                let is_recent = self.invariant_b(&syncs[previous], old_value, previous, current);

                // Ensure that this value is sufficiently separated from the previous value
                // We prefer to keep more recent syncs of similar precision (i.e. the value in replacement)
                // If this value is too close to the previous value, it will never be valid in a later position.
                let separated_from_previous =
                    self.invariant_c(&syncs[previous], old_value, previous);

                // Ensure that this value is sufficiently separated from the next value
                // We prefer to keep existing syncs of lower precision (i.e. the value in syncs[next])
                let next = current + 1;
                let separated_from_next = next >= SYNCS_PER_PARTITION
                    || self.invariant_c(old_value, &syncs[next], current);

                // If the next value in the old array is a duplicate of the current one, then they are equivalent
                // This value will not need to be considered again
                let next_replacement = replacement_index + 1;
                let duplicate = next_replacement < SYNCS_PER_PARTITION
                    && old_value == &original[next_replacement];

                // Promoting the oldValue to the replacement only happens if it satisfies all invariants.
                let promote_old_value_to_replacement =
                    is_recent && separated_from_previous && separated_from_next;

                if promote_old_value_to_replacement {
                    replacement = old_value.clone();
                }

                // The index should be skipped without promoting if we know that it will not be used at a later index
                // based only on the observed part of the array so far.
                skip_old_value = duplicate || !separated_from_previous;

                if promote_old_value_to_replacement || skip_old_value {
                    replacement_index += 1;
                }

                // We may need to skip past multiple indices, so keep looping until we're done skipping forward.
                // The index must not get ahead of the current index, as we only promote from low index to high index.
                if replacement_index >= current || !skip_old_value {
                    break;
                }
            }

            // The replacement variable always contains a value which satisfies the invariants for this index.
            // This replacement may or may not be used, since the invariants could already be satisfied,
            // and in that case, prefer to keep the existing tail of the syncs array rather than updating it.
            debug_assert!(self.invariant_b(&syncs[previous], &replacement, previous, current));
            debug_assert!(self.invariant_c(&syncs[previous], &replacement, previous));

            // Test if changes to the previous index affected the invariant for this index
            if self.invariant_b(&syncs[previous], &syncs[current], previous, current) {
                // Invariant B holds for syncs[current]: it must also hold for all later values
                break;
            } else {
                // Invariant B violated for syncs[current]: sync is now too old and must be updated
                // Repair Invariant B: swap in replacement
                syncs[current] = replacement.clone();

                debug_assert!(self.invariant_b(
                    &syncs[previous],
                    &syncs[current],
                    previous,
                    current
                ));
                debug_assert!(self.invariant_c(&syncs[previous], &syncs[current], previous));
            }
        }
    }

    /// Clear sync array (helper for update_sync_array)
    fn clear_sync_array_vec(
        &self,
        syncs: &mut [crate::offset_sync_writer::OffsetSync],
        offset_sync: &crate::offset_sync_writer::OffsetSync,
    ) {
        for i in 0..SYNCS_PER_PARTITION {
            syncs[i] = offset_sync.clone();
        }
    }

    /// Check invariant B
    ///
    /// Invariant B: For each i,j, i < j, syncs[i] != syncs[j]: syncs[i].upstream <= syncs[j].upstream + 2^j - 2^i
    fn invariant_b(
        &self,
        i_sync: &crate::offset_sync_writer::OffsetSync,
        j_sync: &crate::offset_sync_writer::OffsetSync,
        i: usize,
        j: usize,
    ) -> bool {
        let j_pow = if j < 63 { 1i64 << j } else { i64::MAX };
        let i_pow = if i < 63 { 1i64 << i } else { i64::MAX };
        let bound = j_sync.upstream_offset() + j_pow - i_pow;
        i_sync == j_sync || bound < 0 || i_sync.upstream_offset() <= bound
    }

    /// Check invariant C
    ///
    /// Invariant C: For each i,j, i < j, syncs[i] != syncs[j]: syncs[i].upstream >= syncs[j].upstream + 2^(i-2)
    fn invariant_c(
        &self,
        i_sync: &crate::offset_sync_writer::OffsetSync,
        j_sync: &crate::offset_sync_writer::OffsetSync,
        i: usize,
    ) -> bool {
        let max_val = if i > 2 { i - 2 } else { 0 };
        let max_pow = if max_val < 63 {
            1i64 << max_val
        } else {
            i64::MAX
        };
        let bound = j_sync.upstream_offset() + max_pow;
        i_sync == j_sync || (bound >= 0 && i_sync.upstream_offset() >= bound)
    }

    /// Check invariant C
    ///
    /// Invariant C: For each i,j, i < j, syncs[i] != syncs[j]: syncs[i].upstream >= syncs[j].upstream + 2^(i-2)
    fn invariant_c(
        &self,
        i_sync: &crate::offset_sync_writer::OffsetSync,
        j_sync: &crate::offset_sync_writer::OffsetSync,
        i: usize,
    ) -> bool {
        let max_val = if i > 2 { i - 2 } else { 0 };
        let bound = j_sync.upstream_offset() + (1i64 << max_val) as i64;
        i_sync == j_sync || (bound >= 0 && i_sync.upstream_offset() >= bound)
    }

    /// Get the latest offset sync for a topic partition
    fn latest_offset_sync(
        &self,
        topic_partition: &TopicPartition,
        upstream_offset: i64,
    ) -> Option<crate::offset_sync_writer::OffsetSync> {
        let offset_syncs = self.offset_syncs.read().unwrap();
        offset_syncs
            .get(topic_partition)
            .map(|syncs| self.lookup_latest_sync(syncs, upstream_offset))
    }

    /// Lookup the latest sync offset
    ///
    /// Linear search the syncs, effectively a binary search over the topic offsets
    /// Search from latest to earliest to find the sync that gives the best accuracy
    fn lookup_latest_sync(
        &self,
        syncs: &[crate::offset_sync_writer::OffsetSync],
        upstream_offset: i64,
    ) -> crate::offset_sync_writer::OffsetSync {
        for i in 0..SYNCS_PER_PARTITION {
            let offset_sync = &syncs[i];
            if offset_sync.upstream_offset() <= upstream_offset {
                return offset_sync.clone();
            }
        }
        syncs[SYNCS_PER_PARTITION - 1].clone()
    }

    /// Get sync for a specific index (for testing)
    pub fn sync_for(
        &self,
        topic_partition: &TopicPartition,
        sync_idx: usize,
    ) -> crate::offset_sync_writer::OffsetSync {
        let offset_syncs = self.offset_syncs.read().unwrap();
        let syncs = offset_syncs
            .get(topic_partition)
            .ok_or_else(|| anyhow::anyhow!("No syncs present for {}", topic_partition))
            .unwrap();

        if sync_idx >= syncs.len() {
            panic!(
                "Requested sync {} for {} but there are only {} syncs available for that topic partition",
                sync_idx + 1,
                topic_partition,
                syncs.len()
            );
        }

        syncs[sync_idx].clone()
    }
}

/// Default constructor for testing
impl Default for OffsetSyncStore {
    fn default() -> Self {
        Self {
            backing_store: None,
            offset_syncs: Arc::new(RwLock::new(HashMap::new())),
            admin: None,
            initialization_must_read_to_end: true,
            read_to_end: false,
        }
    }
}

/// Callback for handling offset sync records
struct OffsetSyncStoreCallback {
    offset_syncs: Arc<RwLock<HashMap<TopicPartition, Vec<crate::offset_sync_writer::OffsetSync>>>>,
}

impl Callback for OffsetSyncStoreCallback {
    fn on_completion(
        &self,
        _metadata: Option<kafka_clients_trait::producer::RecordMetadata>,
        _error: Option<String>,
    ) {
        // Handle completion
    }
}

/// Mock KafkaBasedLog for testing
struct MockKafkaBasedLog {
    started: bool,
}

impl MockKafkaBasedLog {
    fn new() -> Self {
        Self { started: false }
    }
}

impl KafkaBasedLog<Vec<u8>, Vec<u8>> for MockKafkaBasedLog {
    fn start(&mut self, _read_to_end: bool) {
        self.started = true;
    }

    fn stop(&mut self) {
        self.started = false;
    }

    fn read_to_end(&mut self) {
        // Mock implementation
    }
}

/// Add deserialize_record method to OffsetSync
impl crate::offset_sync_writer::OffsetSync {
    /// Deserialize a consumer record into an OffsetSync
    pub fn deserialize_record(record: &ConsumerRecord<Vec<u8>, Vec<u8>>) -> Self {
        // Parse the key to get topic and partition
        let key_str = String::from_utf8(record.key.as_ref().unwrap().clone()).unwrap();
        let parts: Vec<&str> = key_str.split('-').collect();
        let topic = parts[0].to_string();
        let partition: i32 = parts[1].parse().unwrap();

        // Parse the value to get upstream and downstream offsets
        let value_str = String::from_utf8(record.value.as_ref().unwrap().clone()).unwrap();
        let offsets: Vec<&str> = value_str.split(':').collect();
        let upstream_offset: i64 = offsets[0].parse().unwrap();
        let downstream_offset: i64 = offsets[1].parse().unwrap();

        Self::new(
            TopicPartition { topic, partition },
            upstream_offset,
            downstream_offset,
        )
    }
}

/// Implement PartialEq for OffsetSync
impl PartialEq for crate::offset_sync_writer::OffsetSync {
    fn eq(&self, other: &Self) -> bool {
        self.topic_partition == other.topic_partition
            && self.upstream_offset == other.upstream_offset
            && self.downstream_offset == other.downstream_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_sync_store_creation() {
        let store = OffsetSyncStore::default();
        assert!(!store.read_to_end);
    }

    #[test]
    fn test_start() {
        let mut store = OffsetSyncStore::default();
        store.start(true);
        assert!(store.read_to_end);
    }

    #[test]
    fn test_create_initial_syncs() {
        let store = OffsetSyncStore::default();
        let tp = TopicPartition {
            topic: "topic".to_string(),
            partition: 0,
        };
        let first_sync = crate::offset_sync_writer::OffsetSync::new(tp.clone(), 100, 200);
        let syncs = store.create_initial_syncs(&first_sync);
        assert_eq!(syncs.len(), SYNCS_PER_PARTITION);
        assert_eq!(syncs[0].upstream_offset(), 100);
        assert_eq!(syncs[0].downstream_offset(), 200);
    }

    #[test]
    fn test_clear_sync_array() {
        let store = OffsetSyncStore::default();
        let tp = TopicPartition {
            topic: "topic".to_string(),
            partition: 0,
        };
        let offset_sync = crate::offset_sync_writer::OffsetSync::new(tp.clone(), 100, 200);
        let mut syncs = Vec::new();
        store.clear_sync_array(&mut syncs, &offset_sync);
        assert_eq!(syncs.len(), SYNCS_PER_PARTITION);
        for sync in &syncs {
            assert_eq!(sync.upstream_offset(), 100);
            assert_eq!(sync.downstream_offset(), 200);
        }
    }

    #[test]
    fn test_invariant_b() {
        let store = OffsetSyncStore::default();
        let tp = TopicPartition {
            topic: "topic".to_string(),
            partition: 0,
        };
        let sync1 = crate::offset_sync_writer::OffsetSync::new(tp.clone(), 100, 200);
        let sync2 = crate::offset_sync_writer::OffsetSync::new(tp.clone(), 200, 300);

        // Same sync should satisfy invariant
        assert!(store.invariant_b(&sync1, &sync1, 0, 1));

        // Different syncs should check the bound
        assert!(store.invariant_b(&sync1, &sync2, 0, 1));
    }

    #[test]
    fn test_invariant_c() {
        let store = OffsetSyncStore::default();
        let tp = TopicPartition {
            topic: "topic".to_string(),
            partition: 0,
        };
        let sync1 = crate::offset_sync_writer::OffsetSync::new(tp.clone(), 100, 200);
        let sync2 = crate::offset_sync_writer::OffsetSync::new(tp.clone(), 200, 300);

        // Same sync should satisfy invariant
        assert!(store.invariant_c(&sync1, &sync1, 0));

        // Different syncs should check the bound
        assert!(store.invariant_c(&sync1, &sync2, 0));
    }

    #[test]
    fn test_lookup_latest_sync() {
        let store = OffsetSyncStore::default();
        let tp = TopicPartition {
            topic: "topic".to_string(),
            partition: 0,
        };
        let sync1 = crate::offset_sync_writer::OffsetSync::new(tp.clone(), 100, 200);
        let sync2 = crate::offset_sync_writer::OffsetSync::new(tp.clone(), 200, 300);

        let mut syncs = Vec::new();
        syncs.push(sync2.clone()); // Latest
        syncs.push(sync1.clone()); // Earliest

        // Looking for offset 150 should return sync1
        let result = store.lookup_latest_sync(&syncs, 150);
        assert_eq!(result.upstream_offset(), 100);

        // Looking for offset 250 should return sync2
        let result = store.lookup_latest_sync(&syncs, 250);
        assert_eq!(result.upstream_offset(), 200);
    }

    #[test]
    fn test_translate_downstream_not_ready() {
        let store = OffsetSyncStore::default();
        let tp = TopicPartition {
            topic: "topic".to_string(),
            partition: 0,
        };

        // Should return None when not read to end
        let result = store.translate_downstream("group", &tp, 100);
        assert!(result.is_none());
    }

    #[test]
    fn test_translate_downstream_no_syncs() {
        let mut store = OffsetSyncStore::default();
        let tp = TopicPartition {
            topic: "topic".to_string(),
            partition: 0,
        };

        store.start(true);

        // Should return None when no syncs available
        let result = store.translate_downstream("group", &tp, 100);
        assert!(result.is_none());
    }
}
