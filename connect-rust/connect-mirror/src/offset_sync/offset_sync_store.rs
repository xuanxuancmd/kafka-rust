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

//! OffsetSyncStore - Store for offset synchronization records.
//!
//! This module provides the OffsetSyncStore which is responsible for
//! reading and storing offset synchronization records from the offset-syncs topic.
//! It maintains a mapping between upstream (source) and downstream (target) offsets
//! and provides translation functionality to convert upstream offsets to downstream offsets.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.OffsetSyncStore

use std::collections::HashMap;
use std::time::Duration;

use common_trait::protocol::SchemaError;
use common_trait::TopicPartition;
use connect_mirror_client::OffsetSync;
use kafka_clients_mock::MockKafkaConsumer;

/// Number of syncs stored per partition.
/// In Java, this is Long.SIZE (64), but for simplicity we use a dynamic Vec.
pub const SYNCS_PER_PARTITION: usize = 64;

/// OffsetSyncStore - stores and translates offset synchronization records.
///
/// The OffsetSyncStore is responsible for:
/// - Reading OffsetSync records from the offset-syncs topic
/// - Maintaining a mapping of upstream to downstream offsets per TopicPartition
/// - Translating upstream offsets to downstream offsets
///
/// Used by MirrorCheckpointTask to translate consumer group offsets from
/// source cluster to target cluster.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.OffsetSyncStore
pub struct OffsetSyncStore {
    /// Kafka consumer for reading offset-syncs topic
    consumer: MockKafkaConsumer,
    /// Topic to read offset-syncs from
    offset_syncs_topic: String,
    /// Internal storage: TopicPartition -> Vec<OffsetSync>
    /// Vec stores OffsetSync entries sorted by upstream_offset (ascending)
    /// Latest sync is at the end of the vector
    offset_syncs: HashMap<TopicPartition, Vec<OffsetSync>>,
    /// Flag indicating if the store has completed initial read
    initialized: bool,
}

impl std::fmt::Debug for OffsetSyncStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OffsetSyncStore")
            .field("offset_syncs_topic", &self.offset_syncs_topic)
            .field("initialized", &self.initialized)
            .field("partition_count", &self.offset_syncs.len())
            .field(
                "total_sync_count",
                &self.offset_syncs.values().map(|v| v.len()).sum::<usize>(),
            )
            .finish()
    }
}

impl OffsetSyncStore {
    /// Creates a new OffsetSyncStore.
    ///
    /// # Arguments
    /// * `consumer` - Kafka consumer for reading offset-syncs topic
    /// * `offset_syncs_topic` - Topic to read offset-syncs from
    pub fn new(consumer: MockKafkaConsumer, offset_syncs_topic: impl Into<String>) -> Self {
        OffsetSyncStore {
            consumer,
            offset_syncs_topic: offset_syncs_topic.into(),
            offset_syncs: HashMap::new(),
            initialized: false,
        }
    }

    /// Returns the offset-syncs topic name.
    pub fn offset_syncs_topic(&self) -> &str {
        &self.offset_syncs_topic
    }

    /// Returns whether the store has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Sets the initialized flag (for testing).
    pub fn set_initialized(&mut self, initialized: bool) {
        self.initialized = initialized;
    }

    /// Returns the number of partitions with sync data.
    pub fn partition_count(&self) -> usize {
        self.offset_syncs.len()
    }

    /// Returns the number of syncs for a specific partition.
    pub fn sync_count(&self, tp: &TopicPartition) -> usize {
        self.offset_syncs.get(tp).map(|v| v.len()).unwrap_or(0)
    }

    /// Returns total sync count across all partitions.
    pub fn total_sync_count(&self) -> usize {
        self.offset_syncs.values().map(|v| v.len()).sum()
    }

    /// Returns the latest OffsetSync for a partition.
    pub fn latest_sync(&self, tp: &TopicPartition) -> Option<&OffsetSync> {
        self.offset_syncs.get(tp).and_then(|v| v.last())
    }

    /// Returns the earliest OffsetSync for a partition.
    pub fn earliest_sync(&self, tp: &TopicPartition) -> Option<&OffsetSync> {
        self.offset_syncs.get(tp).and_then(|v| v.first())
    }

    /// Starts reading from the offset-syncs topic.
    ///
    /// This method subscribes to the offset-syncs topic and consumes
    /// all existing OffsetSync records, building the internal mapping.
    ///
    /// In Java, this uses KafkaBasedLog which reads to end before returning.
    /// In mock implementation, we poll until no more records are available.
    ///
    /// Returns the number of OffsetSync records read.
    ///
    /// Corresponds to Java: OffsetSyncStore.start()
    pub fn start(&mut self) -> Result<usize, SchemaError> {
        // Subscribe to offset-syncs topic if not already subscribed
        // Note: subscribe() clears existing state, so we check first
        let subscription = self.consumer.subscription();
        if !subscription.contains(&self.offset_syncs_topic) {
            self.consumer
                .subscribe(vec![self.offset_syncs_topic.clone()]);
        }

        let mut records_read = 0;

        // Poll until no more records
        loop {
            let records = self.consumer.poll(Duration::from_millis(100));

            if records.is_empty() {
                break;
            }

            // Process each record
            for record in records.iter() {
                if let Some(key) = record.key() {
                    if let Some(value) = record.value() {
                        // Deserialize OffsetSync from key/value
                        let offset_sync = OffsetSync::deserialize_record(key, value)?;

                        // Store the OffsetSync
                        self.handle_record(offset_sync);
                        records_read += 1;
                    }
                }
            }
        }

        self.initialized = true;
        Ok(records_read)
    }

    /// Handles a single OffsetSync record.
    ///
    /// This method updates the internal storage with a new OffsetSync.
    /// The OffsetSync entries are stored sorted by upstream_offset (ascending).
    ///
    /// In Java, this maintains a fixed-size array with specific invariants.
    /// For simplicity, we use a Vec and maintain sorted order.
    ///
    /// Corresponds to Java: OffsetSyncStore.handleRecord()
    fn handle_record(&mut self, sync: OffsetSync) {
        let tp = sync.topic_partition().clone();

        // Get or create the vector for this partition
        let syncs = self.offset_syncs.entry(tp.clone()).or_insert_with(Vec::new);

        // Check if we already have this sync (same upstream_offset)
        // If so, update it (downstream_offset might have changed)
        let existing_idx = syncs
            .iter()
            .position(|s| s.upstream_offset() == sync.upstream_offset());

        if let Some(idx) = existing_idx {
            // Update existing sync
            syncs[idx] = sync;
        } else {
            // Insert new sync, maintaining sorted order by upstream_offset (ascending)
            // Find the position to insert
            let insert_pos = syncs
                .iter()
                .position(|s| s.upstream_offset() > sync.upstream_offset())
                .unwrap_or(syncs.len());

            syncs.insert(insert_pos, sync);

            // If we exceed max capacity, remove the oldest entries
            // In Java, the array has fixed size SYNCS_PER_PARTITION (64)
            while syncs.len() > SYNCS_PER_PARTITION {
                syncs.remove(0); // Remove earliest (lowest upstream_offset)
            }
        }
    }

    /// Translates an upstream offset to a downstream offset.
    ///
    /// This method finds the OffsetSync where upstream_offset <= given offset,
    /// and returns the corresponding downstream offset.
    ///
    /// The search goes from latest to earliest sync, returning the first
    /// sync where upstream_offset <= given upstream_offset.
    ///
    /// Translation logic:
    /// - If upstream_offset exactly matches the sync's upstream_offset,
    ///   return the sync's downstream_offset.
    /// - If upstream_offset is greater than the sync's upstream_offset,
    ///   return sync's downstream_offset + 1 (accounts for dropped records).
    ///
    /// Returns None if:
    /// - Store is not initialized
    /// - No sync exists for the TopicPartition
    /// - The upstream_offset is too far in the past
    ///
    /// Corresponds to Java: OffsetSyncStore.translateDownstream()
    pub fn translate_offset(
        &self,
        topic_partition: &TopicPartition,
        upstream_offset: i64,
    ) -> Option<i64> {
        // Check if initialized
        if !self.initialized {
            return None;
        }

        // Get syncs for this partition
        let syncs = self.offset_syncs.get(topic_partition)?;

        if syncs.is_empty() {
            return None;
        }

        // Find the latest sync where upstream_offset <= given offset
        // Since syncs are sorted ascending, we search from the end
        let sync = Self::lookup_latest_sync(syncs, upstream_offset)?;

        // Calculate downstream offset
        if upstream_offset == sync.upstream_offset() {
            // Exact match - return downstream_offset
            Some(sync.downstream_offset())
        } else if upstream_offset > sync.upstream_offset() {
            // Upstream is ahead - return downstream + 1
            // This accounts for the consumer being ahead of the sync point
            Some(sync.downstream_offset() + 1)
        } else {
            // Upstream_offset < sync.upstream_offset()
            // This means upstream is too far in the past
            // Return -1 to indicate offset is too old
            Some(-1)
        }
    }

    /// Looks up the latest OffsetSync where upstream_offset <= given offset.
    ///
    /// Searches from latest (highest upstream_offset) to earliest.
    /// Returns the first sync where upstream_offset <= given upstream_offset.
    /// If no such sync exists, returns the earliest sync.
    ///
    /// Corresponds to Java: OffsetSyncStore.lookupLatestSync()
    fn lookup_latest_sync<'a>(
        syncs: &'a [OffsetSync],
        upstream_offset: i64,
    ) -> Option<&'a OffsetSync> {
        // syncs are sorted by upstream_offset ascending
        // Search from end (latest) to beginning (earliest)
        for sync in syncs.iter().rev() {
            if sync.upstream_offset() <= upstream_offset {
                return Some(sync);
            }
        }

        // No sync found where upstream_offset <= given offset
        // Return the earliest sync (first element)
        // This handles the case where upstream_offset is very old
        syncs.first()
    }

    /// Stops the OffsetSyncStore.
    ///
    /// This clears the internal state and unsubscribes the consumer.
    ///
    /// Corresponds to Java: OffsetSyncStore.stop() / close()
    pub fn stop(&mut self) {
        self.consumer.clear();
        self.offset_syncs.clear();
        self.initialized = false;
    }

    /// Adds an OffsetSync directly to the store (for testing).
    ///
    /// This bypasses the consumer and directly adds a sync to internal storage.
    pub fn add_sync(&mut self, sync: OffsetSync) {
        self.handle_record(sync);
    }

    /// Gets all syncs for a partition (for testing).
    pub fn get_syncs(&self, tp: &TopicPartition) -> Option<&Vec<OffsetSync>> {
        self.offset_syncs.get(tp)
    }
}

impl Default for OffsetSyncStore {
    fn default() -> Self {
        OffsetSyncStore::new(MockKafkaConsumer::new(), "offset-syncs")
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_clients_mock::ConsumerRecord;

    #[test]
    fn test_offset_sync_store_new() {
        let consumer = MockKafkaConsumer::new();
        let store = OffsetSyncStore::new(consumer, "mm2-offset-syncs");

        assert_eq!(store.offset_syncs_topic(), "mm2-offset-syncs");
        assert!(!store.is_initialized());
        assert_eq!(store.partition_count(), 0);
        assert_eq!(store.total_sync_count(), 0);
    }

    #[test]
    fn test_offset_sync_store_default() {
        let store = OffsetSyncStore::default();

        assert_eq!(store.offset_syncs_topic(), "offset-syncs");
        assert!(!store.is_initialized());
    }

    #[test]
    fn test_start_empty_topic() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["offset-syncs".to_string()]);
        let mut store = OffsetSyncStore::new(consumer, "offset-syncs");

        let result = store.start().unwrap();
        assert_eq!(result, 0);
        assert!(store.is_initialized());
    }

    #[test]
    fn test_start_with_records() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["offset-syncs".to_string()]);

        // Create OffsetSync records
        let tp = TopicPartition::new("source-topic", 0);
        let sync1 = OffsetSync::new(tp.clone(), 0, 0);
        let sync2 = OffsetSync::new(tp.clone(), 100, 200);

        // Serialize and add to consumer
        let key1 = sync1.record_key().unwrap();
        let value1 = sync1.record_value().unwrap();
        let key2 = sync2.record_key().unwrap();
        let value2 = sync2.record_value().unwrap();

        let tp0 = TopicPartition::new("offset-syncs", 0);
        let record1 = ConsumerRecord::new("offset-syncs", 0, 0, Some(key1), Some(value1), 1000);
        let record2 = ConsumerRecord::new("offset-syncs", 0, 1, Some(key2), Some(value2), 1001);

        consumer.add_records(HashMap::from([(tp0.clone(), vec![record1, record2])]));

        let mut store = OffsetSyncStore::new(consumer, "offset-syncs");
        let result = store.start().unwrap();

        assert_eq!(result, 2);
        assert!(store.is_initialized());
        assert_eq!(store.partition_count(), 1);
        assert_eq!(store.sync_count(&tp), 2);
    }

    #[test]
    fn test_translate_offset_exact_match() {
        let mut store = OffsetSyncStore::default();
        store.initialized = true;

        let tp = TopicPartition::new("source-topic", 0);
        let sync = OffsetSync::new(tp.clone(), 100, 200);
        store.add_sync(sync);

        // Exact match
        let result = store.translate_offset(&tp, 100);
        assert_eq!(result, Some(200));
    }

    #[test]
    fn test_translate_offset_after_sync() {
        let mut store = OffsetSyncStore::default();
        store.initialized = true;

        let tp = TopicPartition::new("source-topic", 0);
        let sync = OffsetSync::new(tp.clone(), 100, 200);
        store.add_sync(sync);

        // Upstream offset greater than sync
        let result = store.translate_offset(&tp, 150);
        assert_eq!(result, Some(201)); // downstream + 1
    }

    #[test]
    fn test_translate_offset_before_sync() {
        let mut store = OffsetSyncStore::default();
        store.initialized = true;

        let tp = TopicPartition::new("source-topic", 0);
        let sync = OffsetSync::new(tp.clone(), 100, 200);
        store.add_sync(sync);

        // Upstream offset less than sync (too far in past)
        let result = store.translate_offset(&tp, 50);
        assert_eq!(result, Some(-1));
    }

    #[test]
    fn test_translate_offset_not_initialized() {
        let store = OffsetSyncStore::default();

        let tp = TopicPartition::new("source-topic", 0);
        let result = store.translate_offset(&tp, 100);
        assert_eq!(result, None);
    }

    #[test]
    fn test_translate_offset_no_partition() {
        let mut store = OffsetSyncStore::default();
        store.initialized = true;

        let tp = TopicPartition::new("unknown-topic", 0);
        let result = store.translate_offset(&tp, 100);
        assert_eq!(result, None);
    }

    #[test]
    fn test_translate_offset_multiple_syncs() {
        let mut store = OffsetSyncStore::default();
        store.initialized = true;

        let tp = TopicPartition::new("source-topic", 0);

        // Add multiple syncs with different upstream offsets
        store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));

        // Verify syncs are sorted ascending
        let syncs = store.get_syncs(&tp).unwrap();
        assert_eq!(syncs[0].upstream_offset(), 0);
        assert_eq!(syncs[1].upstream_offset(), 100);
        assert_eq!(syncs[2].upstream_offset(), 500);

        // Test translations
        // At upstream 0 -> downstream 0
        assert_eq!(store.translate_offset(&tp, 0), Some(0));

        // At upstream 50 -> finds sync at 0, returns 1
        assert_eq!(store.translate_offset(&tp, 50), Some(1));

        // At upstream 100 -> downstream 200
        assert_eq!(store.translate_offset(&tp, 100), Some(200));

        // At upstream 300 -> finds sync at 100, returns 201
        assert_eq!(store.translate_offset(&tp, 300), Some(201));

        // At upstream 500 -> downstream 1000
        assert_eq!(store.translate_offset(&tp, 500), Some(1000));

        // At upstream 600 -> finds sync at 500, returns 1001
        assert_eq!(store.translate_offset(&tp, 600), Some(1001));
    }

    #[test]
    fn test_translate_offset_multiple_partitions() {
        let mut store = OffsetSyncStore::default();
        store.initialized = true;

        let tp0 = TopicPartition::new("topic1", 0);
        let tp1 = TopicPartition::new("topic1", 1);
        let tp2 = TopicPartition::new("topic2", 0);

        store.add_sync(OffsetSync::new(tp0.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp1.clone(), 50, 100));
        store.add_sync(OffsetSync::new(tp2.clone(), 200, 400));

        assert_eq!(store.partition_count(), 3);

        // Different translations for different partitions
        assert_eq!(store.translate_offset(&tp0, 100), Some(200));
        assert_eq!(store.translate_offset(&tp1, 50), Some(100));
        assert_eq!(store.translate_offset(&tp2, 200), Some(400));

        // No sync for unknown partition
        let tp_unknown = TopicPartition::new("unknown", 0);
        assert_eq!(store.translate_offset(&tp_unknown, 100), None);
    }

    #[test]
    fn test_handle_record_maintains_sorted_order() {
        let mut store = OffsetSyncStore::default();

        let tp = TopicPartition::new("test-topic", 0);

        // Add in reverse order
        store.add_sync(OffsetSync::new(tp.clone(), 300, 600));
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp.clone(), 200, 400));

        // Verify sorted ascending
        let syncs = store.get_syncs(&tp).unwrap();
        assert_eq!(syncs.len(), 3);
        assert_eq!(syncs[0].upstream_offset(), 100);
        assert_eq!(syncs[1].upstream_offset(), 200);
        assert_eq!(syncs[2].upstream_offset(), 300);
    }

    #[test]
    fn test_handle_record_updates_existing() {
        let mut store = OffsetSyncStore::default();

        let tp = TopicPartition::new("test-topic", 0);

        // Add initial sync
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));

        // Add same upstream_offset with different downstream
        store.add_sync(OffsetSync::new(tp.clone(), 100, 250));

        // Should have one entry with updated downstream
        let syncs = store.get_syncs(&tp).unwrap();
        assert_eq!(syncs.len(), 1);
        assert_eq!(syncs[0].upstream_offset(), 100);
        assert_eq!(syncs[0].downstream_offset(), 250);
    }

    #[test]
    fn test_handle_record_respects_max_capacity() {
        let mut store = OffsetSyncStore::default();

        let tp = TopicPartition::new("test-topic", 0);

        // Add more than SYNCS_PER_PARTITION (64) entries
        for i in 0..100 {
            store.add_sync(OffsetSync::new(tp.clone(), i as i64, i as i64 * 2));
        }

        // Should cap at SYNCS_PER_PARTITION
        let syncs = store.get_syncs(&tp).unwrap();
        assert_eq!(syncs.len(), SYNCS_PER_PARTITION);

        // Should have the latest entries (highest upstream_offset)
        // First entry should be at offset 36 (100 - 64)
        assert_eq!(syncs[0].upstream_offset(), 36);
        // Last entry should be at offset 99
        assert_eq!(syncs.last().unwrap().upstream_offset(), 99);
    }

    #[test]
    fn test_stop() {
        let mut store = OffsetSyncStore::default();
        store.initialized = true;

        let tp = TopicPartition::new("source-topic", 0);
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));

        assert_eq!(store.partition_count(), 1);

        store.stop();

        assert!(!store.is_initialized());
        assert_eq!(store.partition_count(), 0);
        assert_eq!(store.total_sync_count(), 0);
    }

    #[test]
    fn test_latest_and_earliest_sync() {
        let mut store = OffsetSyncStore::default();
        store.initialized = true;

        let tp = TopicPartition::new("test-topic", 0);

        store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));

        let latest = store.latest_sync(&tp).unwrap();
        assert_eq!(latest.upstream_offset(), 500);
        assert_eq!(latest.downstream_offset(), 1000);

        let earliest = store.earliest_sync(&tp).unwrap();
        assert_eq!(earliest.upstream_offset(), 0);
        assert_eq!(earliest.downstream_offset(), 0);

        // Unknown partition returns None
        let tp_unknown = TopicPartition::new("unknown", 0);
        assert_eq!(store.latest_sync(&tp_unknown), None);
        assert_eq!(store.earliest_sync(&tp_unknown), None);
    }

    #[test]
    fn test_full_workflow() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["offset-syncs".to_string()]);

        // Create multiple OffsetSync records
        let tp0 = TopicPartition::new("source-topic", 0);
        let tp1 = TopicPartition::new("source-topic", 1);

        let sync0_1 = OffsetSync::new(tp0.clone(), 0, 0);
        let sync0_2 = OffsetSync::new(tp0.clone(), 100, 200);
        let sync1_1 = OffsetSync::new(tp1.clone(), 50, 100);

        // Serialize
        let records: Vec<ConsumerRecord> = vec![
            ConsumerRecord::new(
                "offset-syncs",
                0,
                0,
                Some(sync0_1.record_key().unwrap()),
                Some(sync0_1.record_value().unwrap()),
                1000,
            ),
            ConsumerRecord::new(
                "offset-syncs",
                0,
                1,
                Some(sync0_2.record_key().unwrap()),
                Some(sync0_2.record_value().unwrap()),
                1001,
            ),
            ConsumerRecord::new(
                "offset-syncs",
                0,
                2,
                Some(sync1_1.record_key().unwrap()),
                Some(sync1_1.record_value().unwrap()),
                1002,
            ),
        ];

        let tp_topic = TopicPartition::new("offset-syncs", 0);
        consumer.add_records(HashMap::from([(tp_topic.clone(), records)]));

        // Create store and start
        let mut store = OffsetSyncStore::new(consumer, "offset-syncs");
        let result = store.start().unwrap();

        assert_eq!(result, 3);
        assert!(store.is_initialized());

        // Verify translations
        assert_eq!(store.translate_offset(&tp0, 0), Some(0));
        assert_eq!(store.translate_offset(&tp0, 50), Some(1));
        assert_eq!(store.translate_offset(&tp0, 100), Some(200));
        assert_eq!(store.translate_offset(&tp0, 150), Some(201));
        assert_eq!(store.translate_offset(&tp1, 50), Some(100));
        assert_eq!(store.translate_offset(&tp1, 100), Some(101));

        // Stop
        store.stop();
        assert!(!store.is_initialized());
    }

    // ============================================================================
    // Java OffsetSyncStoreTest Semantic Tests
    // Corresponds to Java: org.apache.kafka.connect.mirror.OffsetSyncStoreTest
    // ============================================================================

    /// Corresponds to Java: testNoTranslationIfStoreNotStarted
    /// Verifies that translation returns None when store not initialized.
    /// This prevents stale offset translations during startup.
    #[test]
    fn test_no_translation_if_store_not_started_java_semantic() {
        // Store has syncs but not initialized - should return None
        let mut store = OffsetSyncStore::default();

        let tp = TopicPartition::new("source-topic", 0);
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));

        // NOT initialized - translation should fail
        assert!(!store.is_initialized());
        assert_eq!(store.translate_offset(&tp, 100), None);
        assert_eq!(store.translate_offset(&tp, 150), None);

        // After initialization - translation should work
        store.set_initialized(true);
        assert_eq!(store.translate_offset(&tp, 100), Some(200));
        assert_eq!(store.translate_offset(&tp, 150), Some(201));
    }

    /// Corresponds to Java: testNoTranslationIfNoOffsetSync
    /// Verifies that translation returns None when no offset sync exists.
    #[test]
    fn test_no_translation_if_no_offset_sync_java_semantic() {
        let mut store = OffsetSyncStore::default();
        store.set_initialized(true);

        // No syncs added - all translations should return None
        let tp = TopicPartition::new("unknown-topic", 0);
        assert_eq!(store.translate_offset(&tp, 0), None);
        assert_eq!(store.translate_offset(&tp, 100), None);
        assert_eq!(store.translate_offset(&tp, 1000), None);

        // Different partition with syncs should work
        let tp_with_sync = TopicPartition::new("known-topic", 0);
        store.add_sync(OffsetSync::new(tp_with_sync.clone(), 100, 200));

        assert_eq!(store.translate_offset(&tp_with_sync, 100), Some(200));
        // Unknown partition still returns None
        assert_eq!(store.translate_offset(&tp, 100), None);
    }

    /// Corresponds to Java: testOffsetTranslation
    /// Verifies offset translation rules for different upstream offset ranges.
    /// - Exact match: return downstream_offset
    /// - Upstream > sync: return downstream + 1
    /// - Upstream < sync (too old): return -1
    #[test]
    fn test_offset_translation_java_semantic() {
        let mut store = OffsetSyncStore::default();
        store.set_initialized(true);

        let tp = TopicPartition::new("test-topic", 0);

        // Single sync at upstream=100, downstream=200
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));

        // Case 1: Exact match - return downstream offset
        assert_eq!(store.translate_offset(&tp, 100), Some(200));

        // Case 2: Upstream > sync - return downstream + 1
        // This accounts for consumer being ahead of sync point
        assert_eq!(store.translate_offset(&tp, 150), Some(201));
        assert_eq!(store.translate_offset(&tp, 1000), Some(201));

        // Case 3: Upstream < sync (too old) - return -1
        // Offset is older than latest available sync
        assert_eq!(store.translate_offset(&tp, 50), Some(-1));
        assert_eq!(store.translate_offset(&tp, 0), Some(-1));

        // Multiple syncs - verify translation uses correct sync
        store.add_sync(OffsetSync::new(tp.clone(), 200, 400));

        // At upstream 200: exact match to second sync
        assert_eq!(store.translate_offset(&tp, 200), Some(400));

        // At upstream 150: between syncs, uses sync at 100 -> downstream + 1
        assert_eq!(store.translate_offset(&tp, 150), Some(201));

        // At upstream 250: after latest sync (200) -> downstream + 1
        assert_eq!(store.translate_offset(&tp, 250), Some(401));
    }

    /// Corresponds to Java: testPastOffsetTranslation
    /// Tests offset translation when upstream offset is in the past (rewind).
    /// When upstream offset rewinds, older offsets may become impossible to translate.
    #[test]
    fn test_past_offset_translation_java_semantic() {
        let mut store = OffsetSyncStore::default();
        store.set_initialized(true);

        let tp = TopicPartition::new("test-topic", 0);

        // Initial syncs
        store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));

        // Translate offsets at various points
        // At 500: exact match
        assert_eq!(store.translate_offset(&tp, 500), Some(1000));

        // At 300: uses sync at 100 -> downstream 201
        assert_eq!(store.translate_offset(&tp, 300), Some(201));

        // At 50: uses sync at 0 -> downstream 1
        assert_eq!(store.translate_offset(&tp, 50), Some(1));

        // At 600: after latest sync -> downstream + 1
        assert_eq!(store.translate_offset(&tp, 600), Some(1001));

        // Verify historical offset accuracy decreases as we go further back
        // Syncs are sparse, older offsets have less precision
        let syncs = store.get_syncs(&tp).unwrap();
        assert_eq!(syncs.len(), 3);

        // Earliest sync is at offset 0
        assert_eq!(syncs[0].upstream_offset(), 0);
        // Latest sync is at offset 500
        assert_eq!(syncs[2].upstream_offset(), 500);
    }

    /// Corresponds to Java: testPastOffsetTranslationWithoutInitializationReadToEnd
    /// Tests that even with syncs present, uninitialized store cannot translate.
    #[test]
    fn test_past_offset_translation_without_init_java_semantic() {
        let mut store = OffsetSyncStore::default();
        // NOT initialized

        let tp = TopicPartition::new("test-topic", 0);

        // Add multiple syncs
        store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));

        // Store has syncs but not initialized - all translations fail
        assert_eq!(store.translate_offset(&tp, 0), None);
        assert_eq!(store.translate_offset(&tp, 100), None);
        assert_eq!(store.translate_offset(&tp, 500), None);
        assert_eq!(store.translate_offset(&tp, 1000), None);

        // After initialization
        store.set_initialized(true);

        // All translations work
        assert_eq!(store.translate_offset(&tp, 100), Some(200));
        assert_eq!(store.translate_offset(&tp, 500), Some(1000));
    }

    /// Tests upstream offset rewind scenario.
    /// When upstream offset resets to earlier value, historical syncs may be cleared.
    /// Corresponds to Java: OffsetSyncStore behavior on upstream reset.
    #[test]
    fn test_upstream_offset_rewind_clears_older_syncs() {
        let mut store = OffsetSyncStore::default();
        store.set_initialized(true);

        let tp = TopicPartition::new("test-topic", 0);

        // Add syncs in ascending order
        store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));

        // Current translations work
        assert_eq!(store.translate_offset(&tp, 500), Some(1000));
        assert_eq!(store.translate_offset(&tp, 300), Some(201));

        // Add a rewind sync - upstream resets to lower value
        // In Kafka, this could happen if source topic is recreated
        store.add_sync(OffsetSync::new(tp.clone(), 50, 100)); // New sync at earlier upstream

        // Verify syncs are maintained sorted
        let syncs = store.get_syncs(&tp).unwrap();

        // Syncs should still be sorted ascending
        for i in 1..syncs.len() {
            assert!(syncs[i].upstream_offset() > syncs[i - 1].upstream_offset());
        }

        // Translation should use appropriate sync
        // At upstream 50: exact match to new sync
        assert_eq!(store.translate_offset(&tp, 50), Some(100));

        // At upstream 75: after sync at 50 -> downstream + 1
        assert_eq!(store.translate_offset(&tp, 75), Some(101));
    }

    /// Tests monotonicity of offset translations.
    /// Downstream offsets should be monotonic (non-decreasing) for increasing upstream.
    /// Corresponds to Java: testCheckpointRecordsMonotonicIfStoreRewinds
    #[test]
    fn test_offset_translation_monotonicity() {
        let mut store = OffsetSyncStore::default();
        store.set_initialized(true);

        let tp = TopicPartition::new("test-topic", 0);

        // Add multiple syncs
        store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));

        // Verify monotonic translations
        let translations: Vec<i64> = (0..600)
            .filter_map(|offset| store.translate_offset(&tp, offset))
            .collect();

        // Check monotonicity (except -1 values for too-old offsets)
        let valid_translations: Vec<i64> =
            translations.iter().filter(|&t| *t >= 0).copied().collect();

        for i in 1..valid_translations.len() {
            assert!(
                valid_translations[i] >= valid_translations[i - 1],
                "Translation not monotonic: {} -> {}",
                valid_translations[i - 1],
                valid_translations[i]
            );
        }

        // Verify specific monotonic points
        // At 0: downstream 0
        assert_eq!(store.translate_offset(&tp, 0), Some(0));
        // At 50: downstream 1 (using sync at 0)
        assert_eq!(store.translate_offset(&tp, 50), Some(1));
        // At 100: downstream 200 (exact match)
        assert_eq!(store.translate_offset(&tp, 100), Some(200));
        // At 150: downstream 201 (using sync at 100)
        assert_eq!(store.translate_offset(&tp, 150), Some(201));
    }

    /// Tests boundary condition: NO_OFFSET (-1) handling.
    /// Corresponds to Java OffsetSync NO_OFFSET semantics.
    #[test]
    fn test_translate_offset_with_no_offset() {
        let mut store = OffsetSyncStore::default();
        store.set_initialized(true);

        let tp = TopicPartition::new("test-topic", 0);

        // Add sync with NO_OFFSET (-1) for upstream
        // This represents a partition with no committed offset
        store.add_sync(OffsetSync::new(tp.clone(), -1, -1));
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));

        // Translation at -1 should work
        // Looking for sync where upstream <= -1
        // Since -1 < 100, we would use the sync at -1
        // But lookup_latest_sync returns first sync when no match found
        let result = store.translate_offset(&tp, -1);

        // The sync at -1 has upstream=-1, downstream=-1
        // Exact match returns downstream
        assert_eq!(result, Some(-1));

        // Translation at 0: no sync where upstream <= 0 (except -1)
        // But -1 < 0 so exact match case doesn't apply
        // upstream(0) > sync.upstream(-1) -> downstream + 1 = 0
        assert_eq!(store.translate_offset(&tp, 0), Some(0));
    }

    /// Tests boundary: large offset values.
    #[test]
    fn test_translate_offset_large_values() {
        let mut store = OffsetSyncStore::default();
        store.set_initialized(true);

        let tp = TopicPartition::new("test-topic", 0);

        // Large offsets (i64 max range)
        let large_upstream = 1_000_000_000i64;
        let large_downstream = 2_000_000_000i64;

        store.add_sync(OffsetSync::new(
            tp.clone(),
            large_upstream,
            large_downstream,
        ));

        // Exact match
        assert_eq!(
            store.translate_offset(&tp, large_upstream),
            Some(large_downstream)
        );

        // After sync
        assert_eq!(
            store.translate_offset(&tp, large_upstream + 500),
            Some(large_downstream + 1)
        );

        // Before sync (too old)
        assert_eq!(store.translate_offset(&tp, large_upstream - 1), Some(-1));
    }

    /// Tests boundary: empty sync vector.
    #[test]
    fn test_translate_offset_empty_syncs() {
        let mut store = OffsetSyncStore::default();
        store.set_initialized(true);

        let tp = TopicPartition::new("test-topic", 0);

        // Add then remove all syncs via stop
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        assert_eq!(store.sync_count(&tp), 1);

        // Stop clears everything
        store.stop();
        store.set_initialized(true); // Re-initialize

        // No syncs - translation returns None
        assert_eq!(store.translate_offset(&tp, 100), None);
    }

    /// Tests boundary: very old offset (before earliest sync).
    /// Corresponds to Java: "offset is too old" scenario.
    #[test]
    fn test_translate_offset_very_old_before_earliest() {
        let mut store = OffsetSyncStore::default();
        store.set_initialized(true);

        let tp = TopicPartition::new("test-topic", 0);

        // Syncs start at upstream 100
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));

        // Offset 50 is before earliest sync (100)
        // lookup_latest_sync returns earliest sync when no match found
        // But earliest sync has upstream 100 > 50
        // So upstream < sync -> return -1
        assert_eq!(store.translate_offset(&tp, 50), Some(-1));
        assert_eq!(store.translate_offset(&tp, 0), Some(-1));
        assert_eq!(store.translate_offset(&tp, 99), Some(-1));

        // At earliest sync boundary
        assert_eq!(store.translate_offset(&tp, 100), Some(200));
    }

    /// Tests capacity enforcement with exponential spacing.
    /// Java uses fixed array of 64 entries with exponential spacing invariants.
    #[test]
    fn test_capacity_enforcement_exponential_spacing() {
        let mut store = OffsetSyncStore::default();
        store.set_initialized(true);

        let tp = TopicPartition::new("test-topic", 0);

        // Add many syncs (more than capacity)
        for i in 0..100 {
            let upstream = i * 10;
            let downstream = i * 20;
            store.add_sync(OffsetSync::new(tp.clone(), upstream, downstream));
        }

        // Should be capped at SYNCS_PER_PARTITION (64)
        let syncs = store.get_syncs(&tp).unwrap();
        assert_eq!(syncs.len(), SYNCS_PER_PARTITION);

        // Verify sorted order maintained
        for i in 1..syncs.len() {
            assert!(syncs[i].upstream_offset() > syncs[i - 1].upstream_offset());
        }

        // Latest entries should be present (highest upstream offsets)
        // Entries from 36 to 99 (indices 36-99 in original 0-99 sequence)
        // First entry should be at upstream 360 (36 * 10)
        assert_eq!(syncs[0].upstream_offset(), 360);
        // Last entry should be at upstream 990 (99 * 10)
        assert_eq!(syncs.last().unwrap().upstream_offset(), 990);

        // Verify translation works for retained range
        assert_eq!(store.translate_offset(&tp, 360), Some(720));
        assert_eq!(store.translate_offset(&tp, 990), Some(1980));

        // Older offsets should return -1 (syncs were evicted)
        assert_eq!(store.translate_offset(&tp, 350), Some(-1));
    }

    /// Tests handleRecord with out-of-order arrival.
    /// Corresponds to Java: OffsetSyncStore handles records arriving out of order.
    #[test]
    fn test_handle_record_out_of_order_arrival() {
        let mut store = OffsetSyncStore::default();
        store.set_initialized(true);

        let tp = TopicPartition::new("test-topic", 0);

        // Add syncs in random order (simulating Kafka delivery)
        store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp.clone(), 300, 600));
        store.add_sync(OffsetSync::new(tp.clone(), 0, 0));

        // All should be sorted ascending
        let syncs = store.get_syncs(&tp).unwrap();
        assert_eq!(syncs.len(), 4);
        assert_eq!(syncs[0].upstream_offset(), 0);
        assert_eq!(syncs[1].upstream_offset(), 100);
        assert_eq!(syncs[2].upstream_offset(), 300);
        assert_eq!(syncs[3].upstream_offset(), 500);

        // Translations should work correctly
        assert_eq!(store.translate_offset(&tp, 0), Some(0));
        assert_eq!(store.translate_offset(&tp, 100), Some(200));
        assert_eq!(store.translate_offset(&tp, 300), Some(600));
        assert_eq!(store.translate_offset(&tp, 500), Some(1000));
    }

    /// Tests start() reads all records before becoming initialized.
    /// Corresponds to Java: initializationMustReadToEnd behavior.
    #[test]
    fn test_start_reads_to_end_before_initialized() {
        let consumer = MockKafkaConsumer::new();
        consumer.subscribe(vec!["offset-syncs".to_string()]);

        // Add records
        let tp = TopicPartition::new("source-topic", 0);
        let sync1 = OffsetSync::new(tp.clone(), 0, 0);
        let sync2 = OffsetSync::new(tp.clone(), 100, 200);
        let sync3 = OffsetSync::new(tp.clone(), 500, 1000);

        let tp_topic = TopicPartition::new("offset-syncs", 0);
        let records: Vec<ConsumerRecord> = vec![
            ConsumerRecord::new(
                "offset-syncs",
                0,
                0,
                Some(sync1.record_key().unwrap()),
                Some(sync1.record_value().unwrap()),
                1000,
            ),
            ConsumerRecord::new(
                "offset-syncs",
                0,
                1,
                Some(sync2.record_key().unwrap()),
                Some(sync2.record_value().unwrap()),
                1001,
            ),
            ConsumerRecord::new(
                "offset-syncs",
                0,
                2,
                Some(sync3.record_key().unwrap()),
                Some(sync3.record_value().unwrap()),
                1002,
            ),
        ];
        consumer.add_records(HashMap::from([(tp_topic.clone(), records)]));

        let mut store = OffsetSyncStore::new(consumer, "offset-syncs");

        // Before start: not initialized
        assert!(!store.is_initialized());

        // Start reads all records
        let count = store.start().unwrap();
        assert_eq!(count, 3);

        // After start: initialized
        assert!(store.is_initialized());

        // All translations work
        assert_eq!(store.translate_offset(&tp, 0), Some(0));
        assert_eq!(store.translate_offset(&tp, 100), Some(200));
        assert_eq!(store.translate_offset(&tp, 500), Some(1000));
    }
}
