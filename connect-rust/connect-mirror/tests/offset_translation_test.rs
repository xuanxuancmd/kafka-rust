// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! End-to-end tests for offset translation validation.
//!
//! This module tests the complete offset translation flow:
//! - OffsetSyncStore.translateOffset() - converting upstream to downstream offsets
//! - Checkpoint advance scenarios - when upstream consumer group offset increases
//! - Checkpoint rewind scenarios - when upstream consumer group offset decreases
//! - waitForCheckpointOnAllPartitions semantics
//!
//! Corresponds to Java: MirrorConnectorsIntegrationBaseTest.testOffsetTranslationBehindReplicationFlow() (lines 747-806)

use std::collections::HashMap;

use common_trait::TopicPartition;
use connect_mirror::offset_sync::OffsetSyncStore;
use connect_mirror_client::OffsetSync;

// ============================================================================
// Helper Functions
// ============================================================================

/// Creates an OffsetSyncStore pre-loaded with sync records.
fn create_store_with_syncs(
    topic: &str,
    partition: i32,
    syncs: Vec<(i64, i64)>, // (upstream_offset, downstream_offset)
) -> OffsetSyncStore {
    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let tp = TopicPartition::new(topic, partition);
    for (upstream, downstream) in syncs {
        store.add_sync(OffsetSync::new(tp.clone(), upstream, downstream));
    }

    store
}

/// Creates a multi-partition OffsetSyncStore.
fn create_multi_partition_store(
    syncs_by_partition: HashMap<i32, Vec<(i64, i64)>>,
) -> OffsetSyncStore {
    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let topic = "test-topic";
    for (partition, syncs) in syncs_by_partition {
        let tp = TopicPartition::new(topic, partition);
        for (upstream, downstream) in syncs {
            store.add_sync(OffsetSync::new(tp.clone(), upstream, downstream));
        }
    }

    store
}

/// Simulates waitForCheckpointOnAllPartitions logic.
/// Returns true when all partitions have a valid checkpoint (translated offset >= 0).
fn wait_for_checkpoint_on_all_partitions(
    store: &OffsetSyncStore,
    topic: &str,
    partitions: &[i32],
    upstream_offsets: &HashMap<i32, i64>,
) -> bool {
    for partition in partitions {
        let tp = TopicPartition::new(topic, *partition);
        let upstream_offset = upstream_offsets.get(partition).unwrap_or(&0);

        let translated = store.translate_offset(&tp, *upstream_offset);
        match translated {
            Some(offset) if offset >= 0 => continue,
            Some(_) => return false, // Offset too old or other negative value
            None => return false,    // No sync or not initialized
        }
    }
    true
}

/// Simulates waitForNewCheckpointOnAllPartitions logic.
/// Returns true when new checkpoints differ from previous checkpoints.
fn wait_for_new_checkpoint_on_all_partitions(
    store: &OffsetSyncStore,
    topic: &str,
    partitions: &[i32],
    upstream_offsets: &HashMap<i32, i64>,
    last_checkpoints: &HashMap<i32, i64>,
) -> Option<HashMap<i32, i64>> {
    let mut new_checkpoints = HashMap::new();

    for partition in partitions {
        let tp = TopicPartition::new(topic, *partition);
        let upstream_offset = upstream_offsets.get(partition).unwrap_or(&0);

        let translated = store.translate_offset(&tp, *upstream_offset);
        match translated {
            Some(offset) if offset >= 0 => {
                // Check if checkpoint has changed
                let last = last_checkpoints.get(partition);
                if last.is_none() || *last.unwrap() != offset {
                    new_checkpoints.insert(*partition, offset);
                } else {
                    // Same checkpoint - not new
                    return None;
                }
            }
            Some(_) => return None, // Offset too old or other negative value
            None => return None,    // No sync or not initialized
        }
    }

    Some(new_checkpoints)
}

// ============================================================================
// Tests - Basic Offset Translation
// ============================================================================

/// Tests offset translation with exact match.
/// Corresponds to Java: OffsetSyncStore.translateDownstream() exact match case.
#[test]
fn test_offset_translation_exact_match() {
    let store = create_store_with_syncs("test-topic", 0, vec![(100, 200), (500, 1000)]);

    let tp = TopicPartition::new("test-topic", 0);

    // Exact match at upstream 100 -> downstream 200
    assert_eq!(store.translate_offset(&tp, 100), Some(200));

    // Exact match at upstream 500 -> downstream 1000
    assert_eq!(store.translate_offset(&tp, 500), Some(1000));
}

/// Tests offset translation when upstream is after sync point.
/// Corresponds to Java: OffsetSyncStore.translateDownstream() upstream > sync case.
#[test]
fn test_offset_translation_after_sync() {
    let store = create_store_with_syncs("test-topic", 0, vec![(100, 200), (500, 1000)]);

    let tp = TopicPartition::new("test-topic", 0);

    // Upstream 150 is after sync at 100 -> downstream + 1 = 201
    assert_eq!(store.translate_offset(&tp, 150), Some(201));

    // Upstream 300 is after sync at 100 -> downstream + 1 = 201
    assert_eq!(store.translate_offset(&tp, 300), Some(201));

    // Upstream 600 is after sync at 500 -> downstream + 1 = 1001
    assert_eq!(store.translate_offset(&tp, 600), Some(1001));
}

/// Tests offset translation when upstream is before sync point.
/// Corresponds to Java: OffsetSyncStore.translateDownstream() upstream < sync case.
#[test]
fn test_offset_translation_before_sync() {
    let store = create_store_with_syncs("test-topic", 0, vec![(100, 200), (500, 1000)]);

    let tp = TopicPartition::new("test-topic", 0);

    // Upstream 50 is before earliest sync at 100 -> returns -1 (too old)
    assert_eq!(store.translate_offset(&tp, 50), Some(-1));

    // Upstream 0 is before earliest sync at 100 -> returns -1
    assert_eq!(store.translate_offset(&tp, 0), Some(-1));
}

/// Tests offset translation with no syncs.
#[test]
fn test_offset_translation_no_syncs() {
    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let tp = TopicPartition::new("no-sync-topic", 0);

    // No syncs -> returns None
    assert_eq!(store.translate_offset(&tp, 100), None);
}

/// Tests offset translation when store not initialized.
#[test]
fn test_offset_translation_not_initialized() {
    let mut store = OffsetSyncStore::default();
    let tp = TopicPartition::new("test-topic", 0);
    store.add_sync(OffsetSync::new(tp.clone(), 100, 200));

    // NOT initialized -> returns None even with syncs
    assert_eq!(store.translate_offset(&tp, 100), None);

    // After initialization -> works
    store.set_initialized(true);
    assert_eq!(store.translate_offset(&tp, 100), Some(200));
}

/// Tests offset translation with multiple partitions.
#[test]
fn test_offset_translation_multiple_partitions() {
    let syncs_by_partition: HashMap<i32, Vec<(i64, i64)>> = HashMap::from([
        (0, vec![(0, 0), (100, 200), (500, 1000)]),
        (1, vec![(0, 0), (50, 100), (200, 400)]),
        (2, vec![(0, 0), (75, 150)]),
    ]);

    let store = create_multi_partition_store(syncs_by_partition);

    // Partition 0
    let tp0 = TopicPartition::new("test-topic", 0);
    assert_eq!(store.translate_offset(&tp0, 100), Some(200));
    assert_eq!(store.translate_offset(&tp0, 150), Some(201));
    assert_eq!(store.translate_offset(&tp0, 500), Some(1000));

    // Partition 1
    let tp1 = TopicPartition::new("test-topic", 1);
    assert_eq!(store.translate_offset(&tp1, 50), Some(100));
    assert_eq!(store.translate_offset(&tp1, 100), Some(101));
    assert_eq!(store.translate_offset(&tp1, 200), Some(400));

    // Partition 2
    let tp2 = TopicPartition::new("test-topic", 2);
    assert_eq!(store.translate_offset(&tp2, 75), Some(150));
    assert_eq!(store.translate_offset(&tp2, 100), Some(151));
}

// ============================================================================
// Tests - Checkpoint Advancement Scenario
// ============================================================================

/// Tests checkpoint advancement when upstream consumer group offset increases.
/// Corresponds to Java: testOffsetTranslationBehindReplicationFlow checkpoint advancement.
#[test]
fn test_checkpoint_advancement() {
    // Initial syncs: upstream to downstream mapping
    // upstream 0 -> downstream 0
    // upstream 100 -> downstream 200
    // upstream 500 -> downstream 1000
    let store = create_store_with_syncs("test-topic", 0, vec![(0, 0), (100, 200), (500, 1000)]);

    let tp = TopicPartition::new("test-topic", 0);

    // Initial checkpoint at upstream 100 -> downstream 200
    let initial_checkpoint = store.translate_offset(&tp, 100);
    assert_eq!(initial_checkpoint, Some(200));

    // Consumer group advances to upstream 200
    // Since 200 is after sync at 100, translated offset is 201
    let partial_checkpoint = store.translate_offset(&tp, 200);
    assert_eq!(partial_checkpoint, Some(201));

    // Verify advancement: partial > initial
    assert!(partial_checkpoint.unwrap() > initial_checkpoint.unwrap());

    // Consumer group advances further to upstream 500 -> downstream 1000
    let advanced_checkpoint = store.translate_offset(&tp, 500);
    assert_eq!(advanced_checkpoint, Some(1000));

    // Verify further advancement
    assert!(advanced_checkpoint.unwrap() > partial_checkpoint.unwrap());
}

/// Tests checkpoint advancement with new syncs being added.
/// Simulates the scenario where replication continues and new syncs arrive.
#[test]
fn test_checkpoint_advancement_with_new_syncs() {
    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let tp = TopicPartition::new("test-topic", 0);

    // Initial syncs
    store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
    store.add_sync(OffsetSync::new(tp.clone(), 100, 200));

    // Initial checkpoint at upstream 100
    let initial = store.translate_offset(&tp, 100);
    assert_eq!(initial, Some(200));

    // Simulate replication: new sync arrives at upstream 500
    store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));

    // Checkpoint at upstream 500 now returns 1000 (exact match)
    let new_checkpoint = store.translate_offset(&tp, 500);
    assert_eq!(new_checkpoint, Some(1000));

    // Advancement verified
    assert!(new_checkpoint.unwrap() > initial.unwrap());

    // Consumer group at upstream 450 (before new sync at 500)
    // Uses sync at 100 -> downstream + 1 = 201
    let intermediate_checkpoint = store.translate_offset(&tp, 450);
    assert_eq!(intermediate_checkpoint, Some(201));
}

/// Tests checkpoint advancement across multiple partitions.
/// Corresponds to Java: waitForNewCheckpointOnAllPartitions verification.
#[test]
fn test_checkpoint_advancement_all_partitions() {
    let syncs_by_partition: HashMap<i32, Vec<(i64, i64)>> = HashMap::from([
        (0, vec![(0, 0), (100, 200)]),
        (1, vec![(0, 0), (100, 200)]),
        (2, vec![(0, 0), (100, 200)]),
    ]);

    let store = create_multi_partition_store(syncs_by_partition);

    let partitions = vec![0, 1, 2];

    // Initial offsets at upstream 100
    let initial_offsets: HashMap<i32, i64> = HashMap::from([(0, 100), (1, 100), (2, 100)]);
    let initial_checkpoints = wait_for_new_checkpoint_on_all_partitions(
        &store,
        "test-topic",
        &partitions,
        &initial_offsets,
        &HashMap::new(),
    );

    assert!(initial_checkpoints.is_some());
    let checkpoints = initial_checkpoints.unwrap();
    assert_eq!(checkpoints.get(&0), Some(&200));
    assert_eq!(checkpoints.get(&1), Some(&200));
    assert_eq!(checkpoints.get(&2), Some(&200));

    // Consumer groups advance to upstream 200
    let advanced_offsets: HashMap<i32, i64> = HashMap::from([(0, 200), (1, 200), (2, 200)]);
    let new_checkpoints = wait_for_new_checkpoint_on_all_partitions(
        &store,
        "test-topic",
        &partitions,
        &advanced_offsets,
        &checkpoints,
    );

    assert!(new_checkpoints.is_some());
    let new = new_checkpoints.unwrap();
    // At upstream 200, uses sync at 100 -> downstream + 1 = 201
    assert_eq!(new.get(&0), Some(&201));
    assert_eq!(new.get(&1), Some(&201));
    assert_eq!(new.get(&2), Some(&201));
}

// ============================================================================
// Tests - Checkpoint Rewind Scenario
// ============================================================================

/// Tests checkpoint rewind when upstream consumer group offset decreases.
/// Corresponds to Java: testOffsetTranslationBehindReplicationFlow checkpoint rewind.
#[test]
fn test_checkpoint_rewind() {
    let store = create_store_with_syncs("test-topic", 0, vec![(0, 0), (100, 200), (500, 1000)]);

    let tp = TopicPartition::new("test-topic", 0);

    // Consumer group at upstream 500 -> downstream 1000
    let high_checkpoint = store.translate_offset(&tp, 500);
    assert_eq!(high_checkpoint, Some(1000));

    // Consumer group rewinds to upstream 200
    // Uses sync at 100 -> downstream + 1 = 201
    let rewound_checkpoint = store.translate_offset(&tp, 200);
    assert_eq!(rewound_checkpoint, Some(201));

    // Verify rewind: rewound < high
    assert!(rewound_checkpoint.unwrap() < high_checkpoint.unwrap());

    // Consumer group rewinds further to upstream 100 -> downstream 200
    let further_rewound = store.translate_offset(&tp, 100);
    assert_eq!(further_rewound, Some(200));

    // Verify further rewind
    assert!(further_rewound.unwrap() < rewound_checkpoint.unwrap());
}

/// Tests checkpoint rewind to offset too old (returns -1).
#[test]
fn test_checkpoint_rewind_too_old() {
    let store = create_store_with_syncs("test-topic", 0, vec![(100, 200), (500, 1000)]);

    let tp = TopicPartition::new("test-topic", 0);

    // Consumer group at upstream 500
    let checkpoint = store.translate_offset(&tp, 500);
    assert_eq!(checkpoint, Some(1000));

    // Consumer group rewinds to upstream 50 (before earliest sync at 100)
    // Returns -1 (offset too old)
    let rewound = store.translate_offset(&tp, 50);
    assert_eq!(rewound, Some(-1));

    // -1 indicates offset is too old to translate
    assert!(rewound.unwrap() < 0);
}

/// Tests checkpoint monotonicity constraint.
/// Corresponds to Java: testCheckpointRecordsMonotonicIfStoreRewinds.
#[test]
fn test_checkpoint_monotonicity() {
    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let tp = TopicPartition::new("test-topic", 0);

    // Add syncs in order
    store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
    store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
    store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));

    // Verify monotonic translations for increasing upstream offsets
    let translations: Vec<i64> = (0..600)
        .filter_map(|offset| store.translate_offset(&tp, offset))
        .filter(|&t| t >= 0) // Exclude -1 (too old)
        .collect();

    // Check monotonicity
    for i in 1..translations.len() {
        assert!(
            translations[i] >= translations[i - 1],
            "Translation not monotonic at index {}: {} < {}",
            i,
            translations[i],
            translations[i - 1]
        );
    }
}

/// Tests checkpoint rewind with sync reset scenario.
/// Simulates upstream topic being recreated (offset reset).
#[test]
fn test_checkpoint_rewind_with_sync_reset() {
    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let tp = TopicPartition::new("test-topic", 0);

    // Initial syncs
    store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
    store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
    store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));

    // Initial checkpoint
    let initial = store.translate_offset(&tp, 500);
    assert_eq!(initial, Some(1000));

    // Simulate upstream topic reset: new sync at lower upstream
    // This represents topic being recreated with offset starting at 0
    store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
    store.add_sync(OffsetSync::new(tp.clone(), 10, 20));

    // Checkpoint at upstream 10 -> downstream 20
    let after_reset = store.translate_offset(&tp, 10);
    assert_eq!(after_reset, Some(20));

    // This is much lower than previous checkpoint (1000)
    assert!(after_reset.unwrap() < initial.unwrap());
}

// ============================================================================
// Tests - waitForCheckpointOnAllPartitions Semantics
// ============================================================================

/// Tests waitForCheckpointOnAllPartitions with all checkpoints available.
#[test]
fn test_wait_for_checkpoint_all_partitions_available() {
    let syncs_by_partition: HashMap<i32, Vec<(i64, i64)>> = HashMap::from([
        (0, vec![(0, 0), (100, 200)]),
        (1, vec![(0, 0), (100, 200)]),
        (2, vec![(0, 0), (100, 200)]),
    ]);

    let store = create_multi_partition_store(syncs_by_partition);

    let partitions = vec![0, 1, 2];
    let offsets: HashMap<i32, i64> = HashMap::from([(0, 100), (1, 100), (2, 100)]);

    let result = wait_for_checkpoint_on_all_partitions(&store, "test-topic", &partitions, &offsets);

    assert!(result);
}

/// Tests waitForCheckpointOnAllPartitions with missing partition.
#[test]
fn test_wait_for_checkpoint_missing_partition() {
    let syncs_by_partition: HashMap<i32, Vec<(i64, i64)>> = HashMap::from([
        (0, vec![(0, 0), (100, 200)]),
        (1, vec![(0, 0), (100, 200)]),
        // Partition 2 has no syncs
    ]);

    let store = create_multi_partition_store(syncs_by_partition);

    let partitions = vec![0, 1, 2];
    let offsets: HashMap<i32, i64> = HashMap::from([(0, 100), (1, 100), (2, 100)]);

    let result = wait_for_checkpoint_on_all_partitions(&store, "test-topic", &partitions, &offsets);

    // Partition 2 has no syncs -> false
    assert!(!result);
}

/// Tests waitForCheckpointOnAllPartitions with too old offset.
#[test]
fn test_wait_for_checkpoint_too_old_offset() {
    let syncs_by_partition: HashMap<i32, Vec<(i64, i64)>> = HashMap::from([
        (0, vec![(100, 200)]), // Syncs start at upstream 100
        (1, vec![(100, 200)]),
        (2, vec![(100, 200)]),
    ]);

    let store = create_multi_partition_store(syncs_by_partition);

    let partitions = vec![0, 1, 2];
    // Offsets before earliest sync (50 < 100)
    let offsets: HashMap<i32, i64> = HashMap::from([(0, 50), (1, 50), (2, 50)]);

    let result = wait_for_checkpoint_on_all_partitions(&store, "test-topic", &partitions, &offsets);

    // All offsets are too old (-1) -> false
    assert!(!result);
}

/// Tests waitForNewCheckpointOnAllPartitions detection.
#[test]
fn test_wait_for_new_checkpoint_detection() {
    let syncs_by_partition: HashMap<i32, Vec<(i64, i64)>> = HashMap::from([
        (0, vec![(0, 0), (100, 200), (500, 1000)]),
        (1, vec![(0, 0), (100, 200), (500, 1000)]),
        (2, vec![(0, 0), (100, 200), (500, 1000)]),
    ]);

    let store = create_multi_partition_store(syncs_by_partition);

    let partitions = vec![0, 1, 2];

    // Initial checkpoints
    let initial_offsets: HashMap<i32, i64> = HashMap::from([(0, 100), (1, 100), (2, 100)]);
    let initial_checkpoints = wait_for_new_checkpoint_on_all_partitions(
        &store,
        "test-topic",
        &partitions,
        &initial_offsets,
        &HashMap::new(),
    );

    assert!(initial_checkpoints.is_some());
    let initial = initial_checkpoints.unwrap();

    // Same offsets should return None (no new checkpoint)
    let same_checkpoints = wait_for_new_checkpoint_on_all_partitions(
        &store,
        "test-topic",
        &partitions,
        &initial_offsets,
        &initial,
    );
    assert!(same_checkpoints.is_none());

    // Advanced offsets should return new checkpoints
    let advanced_offsets: HashMap<i32, i64> = HashMap::from([(0, 500), (1, 500), (2, 500)]);
    let new_checkpoints = wait_for_new_checkpoint_on_all_partitions(
        &store,
        "test-topic",
        &partitions,
        &advanced_offsets,
        &initial,
    );

    assert!(new_checkpoints.is_some());
    let new = new_checkpoints.unwrap();
    assert_eq!(new.get(&0), Some(&1000));
    assert!(new.get(&0).unwrap() > initial.get(&0).unwrap());
}

// ============================================================================
// Tests - Full Offset Translation Workflow
// ============================================================================

/// Tests complete offset translation workflow simulating:
/// 1. Initial checkpoint establishment
/// 2. Advancement via replication
/// 3. Rewind via consumer group offset reset
#[test]
fn test_full_offset_translation_workflow() {
    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let tp = TopicPartition::new("workflow-topic", 0);

    // Phase 1: Initial syncs
    store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
    store.add_sync(OffsetSync::new(tp.clone(), 100, 200));

    // Initial checkpoint at upstream 100
    let phase1_checkpoint = store.translate_offset(&tp, 100);
    assert_eq!(phase1_checkpoint, Some(200));

    // Phase 2: Replication continues, new syncs arrive
    store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));
    store.add_sync(OffsetSync::new(tp.clone(), 1000, 2000));

    // Checkpoint at upstream 500 (exact match)
    let phase2_checkpoint = store.translate_offset(&tp, 500);
    assert_eq!(phase2_checkpoint, Some(1000));

    // Advancement verified
    assert!(phase2_checkpoint.unwrap() > phase1_checkpoint.unwrap());

    // Consumer group advances further
    let phase2_advanced = store.translate_offset(&tp, 1000);
    assert_eq!(phase2_advanced, Some(2000));

    // Phase 3: Consumer group rewind
    // Rewind to upstream 200
    let phase3_checkpoint = store.translate_offset(&tp, 200);
    assert_eq!(phase3_checkpoint, Some(201)); // Uses sync at 100

    // Rewind verified
    assert!(phase3_checkpoint.unwrap() < phase2_advanced.unwrap());

    // Further rewind to upstream 100
    let phase3_further = store.translate_offset(&tp, 100);
    assert_eq!(phase3_further, Some(200));

    assert!(phase3_further.unwrap() < phase3_checkpoint.unwrap());
}

/// Tests offset translation with capacity limits.
/// Verifies that older syncs are evicted when capacity exceeded.
#[test]
fn test_offset_translation_capacity_limits() {
    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let tp = TopicPartition::new("capacity-topic", 0);

    // Add many syncs (more than capacity of 64)
    for i in 0..100 {
        let upstream = i * 10;
        let downstream = i * 20;
        store.add_sync(OffsetSync::new(tp.clone(), upstream, downstream));
    }

    // Should have capped at 64 syncs
    assert_eq!(store.sync_count(&tp), 64);

    // Older syncs were evicted - translation for old offset returns -1
    let old_offset = store.translate_offset(&tp, 350); // Before first retained sync
    assert_eq!(old_offset, Some(-1));

    // Recent syncs still work
    let recent_offset = store.translate_offset(&tp, 360); // First retained sync
    assert_eq!(recent_offset, Some(720));

    // Latest syncs work
    let latest = store.translate_offset(&tp, 990);
    assert_eq!(latest, Some(1980));
}

/// Tests offset translation with NO_OFFSET (-1) boundary.
#[test]
fn test_offset_translation_no_offset_boundary() {
    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let tp = TopicPartition::new("boundary-topic", 0);

    // Add sync with -1 (NO_OFFSET) semantics
    store.add_sync(OffsetSync::new(tp.clone(), -1, -1));
    store.add_sync(OffsetSync::new(tp.clone(), 100, 200));

    // Translate at -1
    let result = store.translate_offset(&tp, -1);
    assert_eq!(result, Some(-1));

    // Translate at 0 (after sync at -1)
    // upstream 0 > sync.upstream -1 -> downstream + 1 = 0
    let result2 = store.translate_offset(&tp, 0);
    assert_eq!(result2, Some(0));
}

/// Tests offset translation preserves order with out-of-order sync arrival.
#[test]
fn test_offset_translation_out_of_order_arrival() {
    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let tp = TopicPartition::new("order-topic", 0);

    // Add syncs in random order (simulating Kafka delivery)
    store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));
    store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
    store.add_sync(OffsetSync::new(tp.clone(), 300, 600));
    store.add_sync(OffsetSync::new(tp.clone(), 0, 0));

    // Verify sorted order maintained
    let syncs = store.get_syncs(&tp).unwrap();
    assert_eq!(syncs.len(), 4);
    assert_eq!(syncs[0].upstream_offset(), 0);
    assert_eq!(syncs[1].upstream_offset(), 100);
    assert_eq!(syncs[2].upstream_offset(), 300);
    assert_eq!(syncs[3].upstream_offset(), 500);

    // Translations work correctly
    assert_eq!(store.translate_offset(&tp, 0), Some(0));
    assert_eq!(store.translate_offset(&tp, 100), Some(200));
    assert_eq!(store.translate_offset(&tp, 300), Some(600));
    assert_eq!(store.translate_offset(&tp, 500), Some(1000));
}

/// Tests checkpoint advancement vs rewind differentiation.
#[test]
fn test_advancement_vs_rewind_differentiation() {
    let store = create_store_with_syncs(
        "diff-topic",
        0,
        vec![(0, 0), (100, 200), (200, 400), (500, 1000)],
    );

    let tp = TopicPartition::new("diff-topic", 0);

    // Track checkpoint history
    let checkpoints: Vec<i64> = vec![
        store.translate_offset(&tp, 100).unwrap(), // 200
        store.translate_offset(&tp, 200).unwrap(), // 400 (advancement)
        store.translate_offset(&tp, 500).unwrap(), // 1000 (advancement)
        store.translate_offset(&tp, 150).unwrap(), // 201 (rewind)
        store.translate_offset(&tp, 100).unwrap(), // 200 (further rewind)
    ];

    // Verify advancement pattern: 200 -> 400 -> 1000
    assert!(checkpoints[1] > checkpoints[0]); // 400 > 200
    assert!(checkpoints[2] > checkpoints[1]); // 1000 > 400

    // Verify rewind pattern: 1000 -> 201 -> 200
    assert!(checkpoints[3] < checkpoints[2]); // 201 < 1000
    assert!(checkpoints[4] < checkpoints[3]); // 200 < 201
}

/// Tests offset translation with large offset values.
#[test]
fn test_offset_translation_large_values() {
    let large_upstream = 1_000_000_000i64;
    let large_downstream = 2_000_000_000i64;

    let store = create_store_with_syncs("large-topic", 0, vec![(large_upstream, large_downstream)]);

    let tp = TopicPartition::new("large-topic", 0);

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
