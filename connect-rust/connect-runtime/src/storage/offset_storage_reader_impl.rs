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

//! Implementation of OffsetStorageReader.
//!
//! Unlike OffsetStorageWriter which is implemented directly, the interface is only
//! separate from this implementation because it needs to be included in the public
//! API package.
//!
//! Corresponds to `org.apache.kafka.connect.storage.OffsetStorageReaderImpl` in Java.

use std::collections::HashMap;
use std::sync::Arc;

use common_trait::storage::OffsetBackingStore;
use serde_json::Value;
use tokio::sync::Mutex;

use super::offset_storage_writer::PartitionKey;
use super::offset_utils::validate_format_map;

/// Deserializes bytes to an offset map.
fn deserialize_offset(bytes: &[u8]) -> Option<HashMap<String, Value>> {
    if bytes.is_empty() {
        return None;
    }
    let value: Value = serde_json::from_slice(bytes).ok()?;
    if let Value::Object(obj) = value {
        Some(obj.into_iter().collect())
    } else {
        None
    }
}

/// Implementation of OffsetStorageReader for reading offset data.
///
/// Offset data should only be read during startup or reconfiguration of a task.
/// By always serving those requests by reading the values from the backing store,
/// we ensure we never accidentally use stale data.
pub struct OffsetStorageReaderImpl {
    /// The backing store for reading offsets
    backing_store: Arc<dyn OffsetBackingStore>,
    /// Namespace for offset keys
    namespace: String,
    /// Flag indicating if the reader is closed
    closed: Arc<Mutex<bool>>,
}

impl OffsetStorageReaderImpl {
    /// Creates a new OffsetStorageReaderImpl.
    ///
    /// # Arguments
    /// * `backing_store` - The backing store for reading offsets
    /// * `namespace` - The namespace for offset keys (usually the connector name)
    pub fn new(backing_store: Arc<dyn OffsetBackingStore>, namespace: String) -> Self {
        Self {
            backing_store,
            namespace,
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
    pub async fn offset(&self, partition: HashMap<String, Value>) -> Option<HashMap<String, Value>> {
        let key = PartitionKey::new(&self.namespace, partition);
        let keys = vec![key.as_bytes()];
        let raw = self.backing_store.get(keys).await;
        
        let (_, value) = raw.into_iter().next()?;
        value.and_then(|v| deserialize_offset(&v))
    }

    /// Returns the offsets for a collection of partitions.
    ///
    /// # Arguments
    /// * `partitions` - The partitions to get offsets for
    ///
    /// # Returns
    /// A map from partition to offset (offset may be None if no offset exists)
    pub async fn offsets(
        &self,
        partitions: Vec<HashMap<String, Value>>,
    ) -> HashMap<PartitionKey, Option<HashMap<String, Value>>> {
        // Check if closed
        {
            let closed = self.closed.lock().await;
            if *closed {
                panic!("Offset reader is closed");
            }
        }

        // Build partition keys and key-to-original mapping
        let partition_keys: Vec<PartitionKey> = partitions
            .iter()
            .filter(|p| validate_format_map(p).is_ok())
            .map(|p| PartitionKey::new(&self.namespace, p.clone()))
            .collect();

        // Serialize keys for backing store
        let keys: Vec<Vec<u8>> = partition_keys.iter().map(|k| k.as_bytes()).collect();
        
        // Get from backing store
        let raw = self.backing_store.get(keys).await;

        // Build result map
        let mut result: HashMap<PartitionKey, Option<HashMap<String, Value>>> = HashMap::new();
        
        for partition_key in partition_keys {
            let key_bytes = partition_key.as_bytes();
            if let Some(raw_value) = raw.get(&key_bytes) {
                let offset = raw_value.as_ref().and_then(|v| {
                    deserialize_offset(v).and_then(|o| {
                        validate_format_map(&o).ok()?;
                        Some(o)
                    })
                });
                result.insert(partition_key, offset);
            } else {
                result.insert(partition_key, None);
            }
        }

        result
    }

    /// Closes the reader.
    pub async fn close(&self) {
        let mut closed = self.closed.lock().await;
        *closed = true;
    }

    /// Returns true if the reader is closed.
    pub async fn is_closed(&self) -> bool {
        *self.closed.lock().await
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

    async fn setup_store_with_offset(
        namespace: &str,
        partition: HashMap<String, Value>,
        offset: HashMap<String, Value>,
    ) -> Arc<MemoryOffsetBackingStore> {
        let store = Arc::new(MemoryOffsetBackingStore::new());

        let key = PartitionKey::new(namespace, partition);
        let offset_bytes = serde_json::to_vec(&Value::Object(offset.into_iter().collect())).unwrap();

        let values = HashMap::from([(key.as_bytes(), offset_bytes)]);
        store.set(values).await.unwrap();

        store
    }

    #[tokio::test]
    async fn test_new_reader_not_closed() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let reader = OffsetStorageReaderImpl::new(backing_store, "test-connector".to_string());
        assert!(!reader.is_closed().await);
    }

    #[tokio::test]
    async fn test_close_reader() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let reader = OffsetStorageReaderImpl::new(backing_store, "test-connector".to_string());
        
        reader.close().await;
        assert!(reader.is_closed().await);
    }

    #[tokio::test]
    async fn test_offset_missing() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let reader = OffsetStorageReaderImpl::new(backing_store, "test-connector".to_string());

        let result = reader.offset(make_partition(1)).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_offset_found() {
        let partition = make_partition(1);
        let offset = make_offset(100);

        let backing_store = setup_store_with_offset("test-connector", partition.clone(), offset.clone()).await;
        let reader = OffsetStorageReaderImpl::new(backing_store, "test-connector".to_string());

        let result = reader.offset(partition).await;
        assert!(result.is_some());
        let result_offset = result.unwrap();
        assert_eq!(result_offset.get("offset"), Some(&json!(100)));
    }

    #[tokio::test]
    async fn test_offsets_multiple_partitions() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());

        // Set up two offsets
        let partition1 = make_partition(1);
        let partition2 = make_partition(2);

        let key1 = PartitionKey::new("test-connector", partition1.clone());
        let value1 = serde_json::to_vec(&json!({"offset": 100})).unwrap();

        let key2 = PartitionKey::new("test-connector", partition2.clone());
        let value2 = serde_json::to_vec(&json!({"offset": 200})).unwrap();

        let values = HashMap::from([(key1.as_bytes(), value1), (key2.as_bytes(), value2)]);
        backing_store.set(values).await.unwrap();

        let reader = OffsetStorageReaderImpl::new(backing_store, "test-connector".to_string());

        let results = reader.offsets(vec![partition1.clone(), partition2.clone()]).await;
        assert_eq!(results.len(), 2);

        // Check that both partitions have offsets
        for (_, offset_opt) in results.iter() {
            assert!(offset_opt.is_some());
        }
    }

    #[tokio::test]
    async fn test_offsets_partial_found() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());

        let partition1 = make_partition(1);

        // Only set one offset
        let key1 = PartitionKey::new("test-connector", partition1.clone());
        let value1 = serde_json::to_vec(&json!({"offset": 100})).unwrap();

        let values = HashMap::from([(key1.as_bytes(), value1)]);
        backing_store.set(values).await.unwrap();

        let reader = OffsetStorageReaderImpl::new(backing_store, "test-connector".to_string());

        let partition2 = make_partition(2);
        let results = reader.offsets(vec![partition1.clone(), partition2.clone()]).await;
        assert_eq!(results.len(), 2);

        // One has offset, one doesn't
        let count_with_offset = results.values().filter(|v| v.is_some()).count();
        let count_without_offset = results.values().filter(|v| v.is_none()).count();
        assert_eq!(count_with_offset, 1);
        assert_eq!(count_without_offset, 1);
    }

    #[tokio::test]
    async fn test_different_namespace() {
        let partition = make_partition(1);
        let offset = make_offset(100);

        // Set up with namespace "connector-A"
        let backing_store = setup_store_with_offset("connector-A", partition.clone(), offset.clone()).await;

        // Read with different namespace "connector-B"
        let reader = OffsetStorageReaderImpl::new(backing_store, "connector-B".to_string());

        let result = reader.offset(partition).await;
        // Should not find offset because namespace is different
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_offset_with_complex_partition() {
        let partition = HashMap::from([
            ("partition".to_string(), json!(1)),
            ("topic".to_string(), json!("test-topic")),
            ("region".to_string(), json!("us-east")),
        ]);
        let offset = HashMap::from([
            ("offset".to_string(), json!(12345)),
            ("timestamp".to_string(), json!(1699999999)),
        ]);

        let backing_store = setup_store_with_offset("test-connector", partition.clone(), offset.clone()).await;
        let reader = OffsetStorageReaderImpl::new(backing_store, "test-connector".to_string());

        let result = reader.offset(partition).await;
        assert!(result.is_some());
        let result_offset = result.unwrap();
        assert_eq!(result_offset.get("offset"), Some(&json!(12345)));
        assert_eq!(result_offset.get("timestamp"), Some(&json!(1699999999)));
    }

    #[tokio::test]
    #[should_panic(expected = "Offset reader is closed")]
    async fn test_read_after_close_panics() {
        let backing_store = Arc::new(MemoryOffsetBackingStore::new());
        let reader = OffsetStorageReaderImpl::new(backing_store, "test-connector".to_string());

        reader.close().await;

        // This should panic
        reader.offset(make_partition(1)).await;
    }
}