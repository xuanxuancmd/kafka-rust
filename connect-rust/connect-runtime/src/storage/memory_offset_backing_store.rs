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

//! Implementation of OffsetBackingStore that doesn't actually persist any data.
//!
//! To ensure this behaves similarly to a real backing store, operations are
//! executed asynchronously on a background thread.
//!
//! Corresponds to `org.apache.kafka.connect.storage.MemoryOffsetBackingStore` in Java.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common_trait::storage::{GetFuture, OffsetBackingStore, SetFuture, WorkerConfig};
use dashmap::DashMap;
use tokio::sync::RwLock;

/// Implementation of OffsetBackingStore that stores data in memory.
///
/// This implementation doesn't actually persist any data. To ensure it behaves
/// similarly to a real backing store, operations are executed asynchronously.
pub struct MemoryOffsetBackingStore {
    /// In-memory storage for offset data (key -> value)
    data: Arc<DashMap<Vec<u8>, Vec<u8>>>,
    /// Flag indicating if the store is started
    started: Arc<RwLock<bool>>,
}

impl MemoryOffsetBackingStore {
    /// Creates a new MemoryOffsetBackingStore.
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            started: Arc::new(RwLock::new(false)),
        }
    }

    /// Hook to allow subclasses to persist data.
    /// This is a no-op in the memory implementation.
    pub async fn save(&self) {
        // No-op for memory store
    }

    /// Returns the internal data map for testing purposes.
    pub fn data_map(&self) -> &DashMap<Vec<u8>, Vec<u8>> {
        &self.data
    }

    /// Clears all stored data.
    pub fn clear(&self) {
        self.data.clear();
    }

    /// Returns the number of entries stored.
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

impl Default for MemoryOffsetBackingStore {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetBackingStore for MemoryOffsetBackingStore {
    fn configure(&mut self, _config: &WorkerConfig) {
        // No configuration needed for memory store
    }

    fn start(&mut self) {
        let mut started = self.started.blocking_write();
        *started = true;
    }

    fn stop(&mut self) {
        let mut started = self.started.blocking_write();
        *started = false;
    }

    fn get(&self, keys: Vec<Vec<u8>>) -> GetFuture {
        let data = self.data.clone();
        Box::pin(async move {
            let result = HashMap::with_capacity(keys.len());
            keys.into_iter().fold(result, |mut acc, key| {
                let value = data.get(&key).map(|v| v.clone());
                acc.insert(key, value);
                acc
            })
        })
    }

    fn set(&self, values: HashMap<Vec<u8>, Vec<u8>>) -> SetFuture {
        let data = self.data.clone();
        Box::pin(async move {
            for (key, value) in values {
                data.insert(key, value);
            }
            Ok(())
        })
    }

    fn connector_partitions(
        &self,
        _connector_name: &str,
    ) -> HashSet<HashMap<String, serde_json::Value>> {
        // Memory store doesn't track partitions in the same way as Kafka-based stores
        // Return empty set for now
        HashSet::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_store_is_empty() {
        let store = MemoryOffsetBackingStore::new();
        assert_eq!(store.size(), 0);
    }

    #[tokio::test]
    async fn test_start_and_stop() {
        let mut store = MemoryOffsetBackingStore::new();
        store.start();
        {
            let started = store.started.blocking_read();
            assert!(*started);
        }
        
        store.stop();
        {
            let started = store.started.blocking_read();
            assert!(!*started);
        }
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let store = MemoryOffsetBackingStore::new();
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        
        let values = HashMap::from([(key.clone(), value.clone())]);
        store.set(values).await.unwrap();
        
        let result = store.get(vec![key.clone()]).await;
        assert_eq!(result.get(&key), Some(&Some(value)));
        assert_eq!(store.size(), 1);
    }

    #[tokio::test]
    async fn test_get_missing_key() {
        let store = MemoryOffsetBackingStore::new();
        let key = vec![1, 2, 3];
        
        let result = store.get(vec![key.clone()]).await;
        assert_eq!(result.get(&key), Some(&None));
    }

    #[tokio::test]
    async fn test_multiple_keys() {
        let store = MemoryOffsetBackingStore::new();
        let key1 = vec![1, 2, 3];
        let value1 = vec![4, 5, 6];
        let key2 = vec![7, 8, 9];
        let value2 = vec![10, 11, 12];
        
        let values = HashMap::from([
            (key1.clone(), value1.clone()),
            (key2.clone(), value2.clone()),
        ]);
        store.set(values).await.unwrap();
        
        let result = store.get(vec![key1.clone(), key2.clone()]).await;
        assert_eq!(result.get(&key1), Some(&Some(value1)));
        assert_eq!(result.get(&key2), Some(&Some(value2)));
        assert_eq!(store.size(), 2);
    }

    #[tokio::test]
    async fn test_clear() {
        let store = MemoryOffsetBackingStore::new();
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        
        let values = HashMap::from([(key.clone(), value.clone())]);
        store.set(values).await.unwrap();
        assert_eq!(store.size(), 1);
        
        store.clear();
        assert_eq!(store.size(), 0);
    }

    #[tokio::test]
    async fn test_connector_partitions() {
        let store = MemoryOffsetBackingStore::new();
        let partitions = store.connector_partitions("test-connector");
        assert!(partitions.is_empty());
    }

    #[tokio::test]
    async fn test_overwrite_value() {
        let store = MemoryOffsetBackingStore::new();
        let key = vec![1, 2, 3];
        let value1 = vec![4, 5, 6];
        let value2 = vec![7, 8, 9];
        
        store.set(HashMap::from([(key.clone(), value1.clone())])).await.unwrap();
        store.set(HashMap::from([(key.clone(), value2.clone())])).await.unwrap();
        
        let result = store.get(vec![key.clone()]).await;
        assert_eq!(result.get(&key), Some(&Some(value2)));
        assert_eq!(store.size(), 1);
    }
}