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

//! OffsetStorageWriter is a buffered writer that wraps the simple OffsetBackingStore interface.
//!
//! It maintains a copy of the key-value data in memory and buffers writes. It allows
//! you to take a snapshot, which can then be asynchronously flushed to the backing store
//! while new writes continue to be processed. This allows Kafka Connect to process
//! offset commits in the background while continuing to process messages.
//!
//! Corresponds to `org.apache.kafka.connect.storage.OffsetStorageWriter` in Java.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_trait::storage::OffsetBackingStore;
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};

use super::offset_utils::validate_format_map;

/// Callback for flush completion.
pub type FlushCallback = Box<dyn FnOnce(Result<(), Box<dyn std::error::Error + Send + Sync>>) + Send>;

/// A hashable wrapper for partition data.
/// Uses serialized JSON string as the internal representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionKey {
    /// Serialized JSON string representation
    serialized: String,
    /// Original partition map (for reference)
    original: HashMap<String, Value>,
}

impl std::hash::Hash for PartitionKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Only hash the serialized string, not the original HashMap
        self.serialized.hash(state);
    }
}

impl PartitionKey {
    /// Creates a new PartitionKey from a partition map.
    pub fn new(namespace: &str, partition: HashMap<String, Value>) -> Self {
        let key_value = Value::Array(vec![
            Value::String(namespace.to_string()),
            Value::Object(partition.clone().into_iter().collect()),
        ]);
        let serialized = serde_json::to_string(&key_value).unwrap_or_default();
        Self {
            serialized,
            original: partition,
        }
    }

    /// Returns the serialized bytes for storage.
    pub fn as_bytes(&self) -> Vec<u8> {
        self.serialized.as_bytes().to_vec()
    }

    /// Returns the original partition map.
    pub fn original(&self) -> &HashMap<String, Value> {
        &self.original
    }
}

/// OffsetStorageWriter is a buffered writer for offset data.
///
/// Connect uses an OffsetStorage implementation to save state about the current progress
/// of source connectors, which may have many input partitions and "offsets" may not be
/// as simple as they are for Kafka partitions or files.
///
/// Note that this only provides write functionality. This is intentional to ensure stale
/// data is never read. Offset data should only be read during startup or reconfiguration
/// of a task.
pub struct OffsetStorageWriter {
    /// The backing store for persisting offsets
    backing_store: Arc<dyn OffsetBackingStore>,
    /// Namespace for offset keys
    namespace: String,
    /// Offset data in Connect format (partition key -> offset)
    data: Arc<Mutex<HashMap<PartitionKey, HashMap<String, Value>>>>,
    /// Data pending to be flushed
    to_flush: Arc<Mutex<Option<HashMap<PartitionKey, HashMap<String, Value>>>>>,
    /// Semaphore to ensure only one flush at a time
    flush_in_progress: Arc<Semaphore>,
    /// Unique ID for each flush request to handle callbacks after timeouts
    current_flush_id: Arc<Mutex<u64>>,
}

/// Serializes an offset map to bytes.
fn serialize_offset(offset: &HashMap<String, Value>) -> Vec<u8> {
    let value_value = Value::Object(offset.clone().into_iter().collect());
    serde_json::to_vec(&value_value).unwrap_or_default()
}

impl OffsetStorageWriter {
    /// Creates a new OffsetStorageWriter.
    ///
    /// # Arguments
    /// * `backing_store` - The backing store for persisting offsets
    /// * `namespace` - The namespace for offset keys (usually the connector name)
    pub fn new(backing_store: Arc<dyn OffsetBackingStore>, namespace: String) -> Self {
        Self {
            backing_store,
            namespace,
            data: Arc::new(Mutex::new(HashMap::new())),
            to_flush: Arc::new(Mutex::new(None)),
            flush_in_progress: Arc::new(Semaphore::new(1)),
            current_flush_id: Arc::new(Mutex::new(0)),
        }
    }

    /// Set an offset for a partition using Connect data values.
    ///
    /// # Arguments
    /// * `partition` - The partition to store an offset for
    /// * `offset` - The offset value
    pub async fn offset(
        &self,
        partition: HashMap<String, Value>,
        offset: HashMap<String, Value>,
    ) {
        // Validate format
        if let Err(e) = validate_format_map(&partition) {
            eprintln!("Invalid partition format: {}", e);
            return;
        }
        if let Err(e) = validate_format_map(&offset) {
            eprintln!("Invalid offset format: {}", e);
            return;
        }

        let key = PartitionKey::new(&self.namespace, partition);
        let mut data = self.data.lock().await;
        data.insert(key, offset);
    }

    /// Performs the first step of a flush operation, snapshotting the current state.
    /// This does not actually initiate the flush with the underlying storage.
    /// Ensures that any previous flush operations have finished before beginning a new flush.
    ///
    /// # Returns
    /// true if a flush was initiated, false if no data was available
    pub async fn begin_flush(&self) -> bool {
        self.begin_flush_timeout(Duration::ZERO).await
    }

    /// Performs the first step of a flush operation with a timeout.
    ///
    /// # Arguments
    /// * `timeout` - Maximum duration to wait for previous flushes to finish
    ///
    /// # Returns
    /// true if a flush was initiated, false if no data was available
    pub async fn begin_flush_timeout(&self, timeout: Duration) -> bool {
        // Try to acquire the semaphore (wait for previous flush to complete)
        let acquired = if timeout.is_zero() {
            self.flush_in_progress.try_acquire().is_ok()
        } else {
            tokio::time::timeout(timeout, self.flush_in_progress.acquire())
                .await
                .map(|r| r.is_ok())
                .unwrap_or(false)
        };

        if !acquired {
            return false;
        }

        let mut data = self.data.lock().await;
        if data.is_empty() {
            self.flush_in_progress.add_permits(1);
            return false;
        }

        let mut to_flush = self.to_flush.lock().await;
        *to_flush = Some(data.clone());
        data.clear();
        true
    }

    /// Flush the current offsets and clear them from this writer.
    /// This is non-blocking: it moves the current set of offsets out of the way,
    /// serializes the data, and asynchronously writes the data to the backing store.
    ///
    /// # Arguments
    /// * `callback` - Callback to invoke when flush completes
    ///
    /// # Returns
    /// Result indicating success or failure
    pub async fn do_flush(&self, callback: Option<FlushCallback>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let flush_id = *self.current_flush_id.lock().await;

        // Serialize the data
        let offsets_serialized: HashMap<Vec<u8>, Vec<u8>> = {
            let to_flush = self.to_flush.lock().await;
            let flush_data = to_flush.as_ref().unwrap();

            let mut serialized = HashMap::with_capacity(flush_data.len());
            for (partition_key, offset) in flush_data.iter() {
                let key_bytes = partition_key.as_bytes();
                let value_bytes = serialize_offset(offset);
                serialized.insert(key_bytes, value_bytes);
            }
            serialized
        };

        // Submit to backing store
        let result = self.backing_store.set(offsets_serialized).await;

        // Handle completion - check if result is error for canceling
        let is_error = result.is_err();
        {
            let mut current_flush_id = self.current_flush_id.lock().await;
            if flush_id == *current_flush_id {
                if is_error {
                    self.cancel_flush_internal().await;
                } else {
                    *current_flush_id += 1;
                    self.flush_in_progress.add_permits(1);
                    *self.to_flush.lock().await = None;
                }
            }
        }

        // Invoke callback if present (before returning)
        if let Some(cb) = callback {
            // We need to create a new result for the callback since we're about to return the original
            let callback_result = if is_error {
                Err("Flush failed".into())
            } else {
                Ok(())
            };
            cb(callback_result);
        }

        result
    }

    /// Cancel a flush that has been initiated by begin_flush.
    /// This should not be called if do_flush has already been invoked.
    pub async fn cancel_flush(&self) {
        let to_flush = self.to_flush.lock().await;
        if to_flush.is_some() {
            self.cancel_flush_internal().await;
        }
    }

    /// Internal cancel flush implementation.
    async fn cancel_flush_internal(&self) {
        let mut to_flush = self.to_flush.lock().await;
        if let Some(flush_data) = to_flush.take() {
            let mut data = self.data.lock().await;
            // Combine the cancelled flush data back into the main data
            for (key, offset) in flush_data {
                data.insert(key, offset);
            }
        }
        
        let mut current_flush_id = self.current_flush_id.lock().await;
        *current_flush_id += 1;
        self.flush_in_progress.add_permits(1);
    }

    /// Returns true if a flush is currently in progress.
    pub async fn is_flushing(&self) -> bool {
        self.to_flush.lock().await.is_some()
    }

    /// Returns the number of pending offsets.
    pub async fn pending_offset_count(&self) -> usize {
        self.data.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::memory_offset_backing_store::MemoryOffsetBackingStore;
    use serde_json::json;

    fn make_partition(id: i32) -> HashMap<String, Value> {
        HashMap::from([
            ("partition".to_string(), json!(id)),
        ])
    }

    fn make_offset(val: i64) -> HashMap<String, Value> {
        HashMap::from([
            ("offset".to_string(), json!(val)),
        ])
    }

    #[tokio::test]
    async fn test_new_writer_is_empty() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let writer = OffsetStorageWriter::new(backing_store, "test-connector".to_string());
        assert_eq!(writer.pending_offset_count().await, 0);
    }

    #[tokio::test]
    async fn test_offset() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let writer = OffsetStorageWriter::new(backing_store, "test-connector".to_string());

        writer.offset(make_partition(1), make_offset(100)).await;
        assert_eq!(writer.pending_offset_count().await, 1);
    }

    #[tokio::test]
    async fn test_begin_flush_empty() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let writer = OffsetStorageWriter::new(backing_store, "test-connector".to_string());

        let initiated = writer.begin_flush().await;
        assert!(!initiated);
    }

    #[tokio::test]
    async fn test_begin_flush_with_data() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let writer = OffsetStorageWriter::new(backing_store, "test-connector".to_string());

        writer.offset(make_partition(1), make_offset(100)).await;
        assert_eq!(writer.pending_offset_count().await, 1);

        let initiated = writer.begin_flush().await;
        assert!(initiated);
        assert!(writer.is_flushing().await);
        assert_eq!(writer.pending_offset_count().await, 0);
    }

    #[tokio::test]
    async fn test_do_flush() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let writer = OffsetStorageWriter::new(backing_store.clone(), "test-connector".to_string());

        writer.offset(make_partition(1), make_offset(100)).await;
        writer.begin_flush().await;
        
        let result = writer.do_flush(None).await;
        assert!(result.is_ok());
        assert!(!writer.is_flushing().await);
    }

    #[tokio::test]
    async fn test_cancel_flush() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let writer = OffsetStorageWriter::new(backing_store, "test-connector".to_string());

        writer.offset(make_partition(1), make_offset(100)).await;
        writer.begin_flush().await;
        
        writer.cancel_flush().await;
        assert!(!writer.is_flushing().await);
        assert_eq!(writer.pending_offset_count().await, 1);
    }

    #[tokio::test]
    async fn test_multiple_offsets() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let writer = OffsetStorageWriter::new(backing_store, "test-connector".to_string());

        writer.offset(make_partition(1), make_offset(100)).await;
        writer.offset(make_partition(2), make_offset(200)).await;
        assert_eq!(writer.pending_offset_count().await, 2);
    }

    #[tokio::test]
    async fn test_flush_with_callback() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let writer = OffsetStorageWriter::new(backing_store, "test-connector".to_string());

        writer.offset(make_partition(1), make_offset(100)).await;
        writer.begin_flush().await;

        let callback_called = Arc::new(Mutex::new(false));
        let callback_called_clone = callback_called.clone();
        
        let callback = Box::new(move |result: Result<(), Box<dyn std::error::Error + Send + Sync>>| {
            assert!(result.is_ok());
            *callback_called_clone.blocking_lock() = true;
        });

        writer.do_flush(Some(callback)).await.unwrap();
        assert!(*callback_called.lock().await);
    }

    #[tokio::test]
    async fn test_consecutive_flushes() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let writer = OffsetStorageWriter::new(backing_store.clone(), "test-connector".to_string());

        // First flush
        writer.offset(make_partition(1), make_offset(100)).await;
        writer.begin_flush().await;
        writer.do_flush(None).await.unwrap();

        // Second flush
        writer.offset(make_partition(1), make_offset(200)).await;
        writer.begin_flush().await;
        let result = writer.do_flush(None).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_partition_key_hashable() {
        let key1 = PartitionKey::new("test", make_partition(1));
        let key2 = PartitionKey::new("test", make_partition(1));
        let key3 = PartitionKey::new("test", make_partition(2));

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);

        // Test that we can use it as HashMap key
        let mut map = HashMap::new();
        map.insert(key1, 1);
        map.insert(key3, 2);
        assert_eq!(map.len(), 2);
    }
}