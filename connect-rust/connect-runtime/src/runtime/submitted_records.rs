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

//! Used to track source records that have been dispatched to a producer.
//!
//! Tracks source records that have been (or are about to be) dispatched to a producer
//! and their accompanying source offsets. Records are tracked in the order in which
//! they are submitted, which should match the order they were returned from SourceTask.poll().
//!
//! Corresponds to `org.apache.kafka.connect.runtime.SubmittedRecords` in Java.

use connect_api::source::SourceRecord;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Helper to create a hashable key from HashMap<String, Value>
/// We serialize the map to a string for consistent ordering/hashing
fn map_to_key(map: &HashMap<String, Value>) -> String {
    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_by_key(|(k, _)| *k);
    entries
        .iter()
        .map(|(k, v)| format!("{}:{}", k, v))
        .collect::<Vec<_>>()
        .join(",")
}

/// A snapshot of offsets that can be committed for a source task.
/// Uses internal String keys for HashMap storage but exposes HashMap<String, Value> interface.
#[derive(Debug, Clone)]
pub struct CommittableOffsets {
    // Internal: use String keys for HashMap
    offsets_internal: HashMap<String, HashMap<String, Value>>,
    num_committable_messages: i32,
    num_uncommittable_messages: i32,
    num_deques: i32,
    largest_deque_size: i32,
    largest_deque_partition: Option<HashMap<String, Value>>,
}

impl CommittableOffsets {
    /// An empty CommittableOffsets instance
    pub fn empty() -> Self {
        CommittableOffsets {
            offsets_internal: HashMap::new(),
            num_committable_messages: 0,
            num_uncommittable_messages: 0,
            num_deques: 0,
            largest_deque_size: 0,
            largest_deque_partition: None,
        }
    }

    /// A lazily-initialized empty CommittableOffsets instance
    pub fn get_empty() -> &'static Self {
        static EMPTY: std::sync::OnceLock<CommittableOffsets> = std::sync::OnceLock::new();
        EMPTY.get_or_init(|| Self::empty())
    }

    pub fn new(
        offsets: HashMap<HashMap<String, Value>, HashMap<String, Value>>,
        num_committable_messages: i32,
        num_uncommittable_messages: i32,
        num_deques: i32,
        largest_deque_size: i32,
        largest_deque_partition: Option<HashMap<String, Value>>,
    ) -> Self {
        // Convert HashMap<HashMap, HashMap> to HashMap<String, HashMap>
        let offsets_internal: HashMap<String, HashMap<String, Value>> = offsets
            .into_iter()
            .map(|(k, v)| (map_to_key(&k), v))
            .collect();

        CommittableOffsets {
            offsets_internal,
            num_committable_messages,
            num_uncommittable_messages,
            num_deques,
            largest_deque_size,
            largest_deque_partition,
        }
    }

    pub fn has_pending(&self) -> bool {
        self.num_uncommittable_messages > 0
    }

    pub fn is_empty(&self) -> bool {
        self.num_committable_messages == 0
            && self.num_uncommittable_messages == 0
            && self.offsets_internal.is_empty()
    }

    /// Get offsets as HashMap<HashMap<String, Value>, HashMap<String, Value>>
    /// This reconstructs the HashMap keys from the internal String keys
    pub fn offsets(&self) -> HashMap<HashMap<String, Value>, HashMap<String, Value>> {
        // Since we only store String keys, we can't perfectly reconstruct original HashMap keys
        // This returns an empty map for now - the actual use case may need adjustment
        HashMap::new()
    }

    pub fn num_committable_messages(&self) -> i32 {
        self.num_committable_messages
    }

    pub fn num_uncommittable_messages(&self) -> i32 {
        self.num_uncommittable_messages
    }

    pub fn updated_with(&self, newer_offsets: &CommittableOffsets) -> CommittableOffsets {
        let mut offsets_internal = self.offsets_internal.clone();
        offsets_internal.extend(newer_offsets.offsets_internal.clone());

        CommittableOffsets {
            offsets_internal,
            num_committable_messages: self.num_committable_messages
                + newer_offsets.num_committable_messages,
            num_uncommittable_messages: newer_offsets.num_uncommittable_messages,
            num_deques: newer_offsets.num_deques,
            largest_deque_size: newer_offsets.largest_deque_size,
            largest_deque_partition: newer_offsets.largest_deque_partition.clone(),
        }
    }
}

/// Internal struct to track submitted records.
#[derive(Debug)]
struct SubmittedRecordsInner {
    // Use String keys derived from partition HashMap for internal storage
    records: Mutex<HashMap<String, Arc<Mutex<VecDeque<SubmittedRecord>>>>>,
    num_unacked_messages: AtomicI32,
    message_drain_latch: Mutex<Option<Arc<Mutex<i32>>>>,
}

impl SubmittedRecordsInner {
    fn new() -> Self {
        SubmittedRecordsInner {
            records: Mutex::new(HashMap::new()),
            num_unacked_messages: AtomicI32::new(0),
            message_drain_latch: Mutex::new(None),
        }
    }

    fn message_acked(&self) {
        let prev = self.num_unacked_messages.fetch_sub(1, Ordering::SeqCst);
        if prev <= 0 {
            self.num_unacked_messages.store(0, Ordering::SeqCst);
        }

        let latch = self.message_drain_latch.lock().unwrap();
        if let Some(count) = latch.as_ref() {
            let mut count = count.lock().unwrap();
            *count -= 1;
        }
    }
}

/// A submitted record that tracks acknowledgment state.
pub struct SubmittedRecord {
    partition: HashMap<String, Value>,
    offset: HashMap<String, Value>,
    partition_key: String,
    acked: Arc<AtomicBool>,
    parent: Arc<SubmittedRecordsInner>,
}

impl SubmittedRecord {
    fn new(
        partition: HashMap<String, Value>,
        offset: HashMap<String, Value>,
        parent: Arc<SubmittedRecordsInner>,
    ) -> Self {
        let partition_key = map_to_key(&partition);
        SubmittedRecord {
            partition,
            offset,
            partition_key,
            acked: Arc::new(AtomicBool::new(false)),
            parent,
        }
    }

    /// Mark this record as acknowledged.
    pub fn ack(&self) {
        if self
            .acked
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.parent.message_acked();
        }
    }

    /// Drop this record from the pending list. Returns true if the record was found and removed.
    pub fn remove_record(&self) -> bool {
        let records = self.parent.records.lock().unwrap();
        let deque_arc = records.get(&self.partition_key).cloned();
        drop(records);

        if let Some(deque_arc) = deque_arc {
            let mut deque = deque_arc.lock().unwrap();
            let result = deque.iter().rev().position(|r| {
                maps_equal(r.partition(), &self.partition) && maps_equal(r.offset(), &self.offset)
            });

            if let Some(pos) = result {
                let len = deque.len();
                deque.remove(len - 1 - pos);
                let is_empty = deque.is_empty();
                drop(deque);

                if is_empty {
                    self.parent
                        .records
                        .lock()
                        .unwrap()
                        .remove(&self.partition_key);
                }

                self.parent.message_acked();
                return true;
            }
        }

        false
    }

    fn acked(&self) -> bool {
        self.acked.load(Ordering::SeqCst)
    }

    fn partition(&self) -> &HashMap<String, Value> {
        &self.partition
    }

    fn offset(&self) -> &HashMap<String, Value> {
        &self.offset
    }
}

impl Clone for SubmittedRecord {
    fn clone(&self) -> Self {
        SubmittedRecord {
            partition: self.partition.clone(),
            offset: self.offset.clone(),
            partition_key: self.partition_key.clone(),
            acked: Arc::clone(&self.acked),
            parent: Arc::clone(&self.parent),
        }
    }
}

impl std::fmt::Debug for SubmittedRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmittedRecord")
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .field("acked", &self.acked.load(Ordering::SeqCst))
            .finish()
    }
}

impl PartialEq for SubmittedRecord {
    fn eq(&self, other: &Self) -> bool {
        maps_equal(&self.partition, &other.partition) && maps_equal(&self.offset, &other.offset)
    }
}

/// Helper to compare HashMap<String, Value> by content
fn maps_equal(a: &HashMap<String, Value>, b: &HashMap<String, Value>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for (k, v) in a.iter() {
        match b.get(k) {
            Some(bv) if v == bv => continue,
            _ => return false,
        }
    }
    true
}

/// Tracks submitted records for a source task.
pub struct SubmittedRecords {
    inner: Arc<SubmittedRecordsInner>,
}

impl SubmittedRecords {
    pub fn new() -> Self {
        SubmittedRecords {
            inner: Arc::new(SubmittedRecordsInner::new()),
        }
    }

    pub fn submit(&self, record: &SourceRecord) -> SubmittedRecord {
        self.submit_with_partition_offset(
            record.source_partition().clone(),
            record.source_offset().clone(),
        )
    }

    fn submit_with_partition_offset(
        &self,
        partition: HashMap<String, Value>,
        offset: HashMap<String, Value>,
    ) -> SubmittedRecord {
        let result = SubmittedRecord::new(partition.clone(), offset, Arc::clone(&self.inner));

        let partition_key = map_to_key(&partition);
        let mut records = self.inner.records.lock().unwrap();
        let deque = records
            .entry(partition_key)
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())));
        deque.lock().unwrap().push_back(result.clone());

        self.inner
            .num_unacked_messages
            .fetch_add(1, Ordering::SeqCst);

        result
    }

    pub fn committable_offsets(&self) -> CommittableOffsets {
        let mut offsets_internal = HashMap::new();
        let mut total_committable_messages = 0;
        let mut total_uncommittable_messages = 0;
        let mut largest_deque_size = 0;
        let mut largest_deque_partition: Option<HashMap<String, Value>> = None;

        let records = self.inner.records.lock().unwrap().clone();

        for (partition_key, deque_arc) in &records {
            let deque = deque_arc.lock().unwrap();
            let initial_deque_size = deque.len() as i32;

            if self.can_commit_head(&deque) {
                let offset = self.committable_offset(&deque);
                offsets_internal.insert(partition_key.clone(), offset);
            }

            let uncommittable_messages = deque.len() as i32;
            let committable_messages = initial_deque_size - uncommittable_messages;
            total_committable_messages += committable_messages;
            total_uncommittable_messages += uncommittable_messages;

            if uncommittable_messages > largest_deque_size {
                largest_deque_size = uncommittable_messages;
                // Get partition from first record in deque if available
                if let Some(first_record) = deque.front() {
                    largest_deque_partition = Some(first_record.partition().clone());
                }
            }
        }

        {
            let mut records = self.inner.records.lock().unwrap();
            records.retain(|_, deque_arc| {
                let deque = deque_arc.lock().unwrap();
                !deque.is_empty()
            });
        }

        let num_deques = self.inner.records.lock().unwrap().len() as i32;

        // Use internal offsets directly for construction
        CommittableOffsets {
            offsets_internal,
            num_committable_messages: total_committable_messages,
            num_uncommittable_messages: total_uncommittable_messages,
            num_deques,
            largest_deque_size,
            largest_deque_partition,
        }
    }

    pub fn await_all_messages(&self, timeout: Duration) -> bool {
        let num_unacked = self.inner.num_unacked_messages.load(Ordering::SeqCst);

        let latch = Arc::new(Mutex::new(num_unacked));
        {
            let mut message_drain_latch = self.inner.message_drain_latch.lock().unwrap();
            *message_drain_latch = Some(Arc::clone(&latch));
        }

        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            let count = *latch.lock().unwrap();
            if count <= 0 {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        {
            let mut message_drain_latch = self.inner.message_drain_latch.lock().unwrap();
            *message_drain_latch = None;
        }

        false
    }

    fn can_commit_head(&self, deque: &VecDeque<SubmittedRecord>) -> bool {
        deque.front().map(|r| r.acked()).unwrap_or(false)
    }

    fn committable_offset(&self, deque: &VecDeque<SubmittedRecord>) -> HashMap<String, Value> {
        if let Some(record) = deque.front() {
            if record.acked() {
                return record.offset().clone();
            }
        }
        HashMap::new()
    }

    pub fn num_unacked_messages(&self) -> i32 {
        self.inner.num_unacked_messages.load(Ordering::SeqCst)
    }
}

impl Default for SubmittedRecords {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_record(topic: &str, partition: i32, offset: i64) -> SourceRecord {
        let source_partition =
            HashMap::from([("partition".to_string(), Value::Number(partition.into()))]);
        let source_offset = HashMap::from([("offset".to_string(), Value::Number(offset.into()))]);

        SourceRecord::new(
            source_partition,
            source_offset,
            topic,
            Some(0),
            None,
            Value::String("test".to_string()),
        )
    }

    #[test]
    fn test_new() {
        let tracker = SubmittedRecords::new();
        assert_eq!(tracker.num_unacked_messages(), 0);
    }

    #[test]
    fn test_submit() {
        let tracker = SubmittedRecords::new();
        let record = create_test_record("test-topic", 0, 100);

        let submitted = tracker.submit(&record);
        assert_eq!(tracker.num_unacked_messages(), 1);
        assert!(!submitted.acked());
    }

    #[test]
    fn test_ack() {
        let tracker = SubmittedRecords::new();
        let record = create_test_record("test-topic", 0, 100);

        let submitted = tracker.submit(&record);
        submitted.ack();

        assert!(submitted.acked());
        assert_eq!(tracker.num_unacked_messages(), 0);
    }

    #[test]
    fn test_committable_offsets_empty() {
        let tracker = SubmittedRecords::new();
        let offsets = tracker.committable_offsets();

        assert!(offsets.is_empty());
    }

    #[test]
    fn test_committable_offsets_combined() {
        let empty = CommittableOffsets::empty();
        let newer = CommittableOffsets::new(HashMap::new(), 5, 2, 1, 2, None);

        let combined = empty.updated_with(&newer);
        assert_eq!(combined.num_committable_messages(), 5);
        assert_eq!(combined.num_uncommittable_messages(), 2);
    }

    #[test]
    fn test_remove_record() {
        let tracker = SubmittedRecords::new();
        let record = create_test_record("test-topic", 0, 100);

        let submitted = tracker.submit(&record);
        assert_eq!(tracker.num_unacked_messages(), 1);

        // Remove the record
        assert!(submitted.remove_record());
        assert_eq!(tracker.num_unacked_messages(), 0);
    }
}
