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

//! A CloseableOffsetStorageReader that extends OffsetStorageReaderImpl with close functionality.
//!
//! This interface extends OffsetStorageReader with close functionality. The close method
//! cancels all outstanding offset read requests and throws an exception in all current
//! and future calls to read offsets. This is useful for unblocking task threads which
//! need to shut down but are blocked on offset reads.
//!
//! Corresponds to `org.apache.kafka.connect.storage.CloseableOffsetStorageReader` in Java.

use std::collections::HashMap;
use std::sync::Arc;

use common_trait::storage::OffsetBackingStore;
use serde_json::Value;
use tokio::sync::Mutex;

use super::offset_storage_reader_impl::OffsetStorageReaderImpl;
use super::offset_storage_writer::PartitionKey;

/// A CloseableOffsetStorageReader that wraps OffsetStorageReaderImpl with close functionality.
///
/// This provides the ability to cancel all outstanding offset read requests and throw
/// an exception in all current and future calls to read offsets. This is useful for
/// unblocking task threads which need to shut down but are blocked on offset reads.
pub struct CloseableOffsetStorageReader {
    /// The underlying offset storage reader
    reader: OffsetStorageReaderImpl,
    /// Flag indicating if the reader is closed
    closed: Arc<Mutex<bool>>,
}

impl CloseableOffsetStorageReader {
    /// Creates a new CloseableOffsetStorageReader.
    ///
    /// # Arguments
    /// * `backing_store` - The backing store for reading offsets
    /// * `namespace` - The namespace for offset keys (usually the connector name)
    pub fn new(backing_store: Arc<dyn OffsetBackingStore>, namespace: String) -> Self {
        Self {
            reader: OffsetStorageReaderImpl::new(backing_store, namespace),
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Returns the offset for a single partition.
    ///
    /// # Arguments
    /// * `partition` - The partition to get the offset for
    ///
    /// # Returns
    /// The offset for the partition, or None if no offset exists
    ///
    /// # Errors
    /// Panics if the reader is closed
    pub async fn offset(&self, partition: HashMap<String, Value>) -> Option<HashMap<String, Value>> {
        self.check_closed();
        self.reader.offset(partition).await
    }

    /// Returns the offsets for a collection of partitions.
    ///
    /// # Arguments
    /// * `partitions` - The partitions to get offsets for
    ///
    /// # Returns
    /// A map from partition to offset (offset may be None if no offset exists)
    ///
    /// # Errors
    /// Panics if the reader is closed
    pub async fn offsets(
        &self,
        partitions: Vec<HashMap<String, Value>>,
    ) -> HashMap<PartitionKey, Option<HashMap<String, Value>>> {
        self.check_closed();
        self.reader.offsets(partitions).await
    }

    /// Cancel all outstanding offset read requests, and throw an exception in all
    /// current and future calls to `offsets` and `offset`. This is useful for
    /// unblocking task threads which need to shut down but are blocked on offset reads.
    pub async fn close(&self) {
        // Close the underlying reader
        self.reader.close().await;
        
        // Mark this reader as closed
        let mut closed = self.closed.lock().await;
        *closed = true;
    }

    /// Returns true if the reader is closed.
    pub async fn is_closed(&self) -> bool {
        *self.closed.lock().await
    }

    /// Checks if the reader is closed and panics if so.
    fn check_closed(&self) {
        // Use blocking_lock since this is a quick check
        let closed = self.closed.blocking_lock();
        if *closed {
            panic!("CloseableOffsetStorageReader is closed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::memory_offset_backing_store::MemoryOffsetBackingStore;
    use serde_json::json;

    fn make_partition(id: i32) -> HashMap<String, Value> {
        HashMap::from([("partition".to_string(), json!(id))])
    }

    fn make_offset(val: i64) -> HashMap<String, Value> {
        HashMap::from([("offset".to_string(), json!(val))])
    }

    #[tokio::test]
    async fn test_new_reader_not_closed() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let reader = CloseableOffsetStorageReader::new(backing_store, "test-connector".to_string());
        assert!(!reader.is_closed().await);
    }

    #[tokio::test]
    async fn test_close_reader() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let reader = CloseableOffsetStorageReader::new(backing_store, "test-connector".to_string());

        reader.close().await;
        assert!(reader.is_closed().await);
    }

    #[tokio::test]
    async fn test_offset_missing() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let reader = CloseableOffsetStorageReader::new(backing_store, "test-connector".to_string());

        let result = reader.offset(make_partition(1)).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_offset_found() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());

        // Set up offset
        let partition = make_partition(1);
        let offset = make_offset(100);
        let key = PartitionKey::new("test-connector", partition.clone());
        let offset_bytes = serde_json::to_vec(&json!(offset)).unwrap();
        let values = HashMap::from([(key.as_bytes(), offset_bytes)]);
        backing_store.set(values).await.unwrap();

        let reader = CloseableOffsetStorageReader::new(backing_store, "test-connector".to_string());

        let result = reader.offset(partition).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    #[should_panic(expected = "CloseableOffsetStorageReader is closed")]
    async fn test_offset_after_close_panics() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let reader = CloseableOffsetStorageReader::new(backing_store, "test-connector".to_string());

        reader.close().await;

        // This should panic
        reader.offset(make_partition(1)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "CloseableOffsetStorageReader is closed")]
    async fn test_offsets_after_close_panics() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let reader = CloseableOffsetStorageReader::new(backing_store, "test-connector".to_string());

        reader.close().await;

        // This should panic
        reader.offsets(vec![make_partition(1)]).await;
    }

    #[tokio::test]
    async fn test_offsets_multiple_partitions() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());

        // Set up two offsets
        let key1 = PartitionKey::new("test-connector", make_partition(1));
        let value1 = serde_json::to_vec(&json!({"offset": 100})).unwrap();
        let key2 = PartitionKey::new("test-connector", make_partition(2));
        let value2 = serde_json::to_vec(&json!({"offset": 200})).unwrap();

        let values = HashMap::from([(key1.as_bytes(), value1), (key2.as_bytes(), value2)]);
        backing_store.set(values).await.unwrap();

        let reader = CloseableOffsetStorageReader::new(backing_store, "test-connector".to_string());

        let results = reader.offsets(vec![make_partition(1), make_partition(2)]).await;
        assert_eq!(results.len(), 2);
    }
}