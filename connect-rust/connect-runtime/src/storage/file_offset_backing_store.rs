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

//! Implementation of OffsetBackingStore that saves data locally to a file.
//!
//! To ensure this behaves similarly to a real backing store, operations are
//! executed asynchronously. This extends MemoryOffsetBackingStore with
//! file persistence capability.
//!
//! Corresponds to `org.apache.kafka.connect.storage.FileOffsetBackingStore` in Java.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use common_trait::storage::{GetFuture, OffsetBackingStore, SetFuture, WorkerConfig};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use log::{debug, error, info};

use super::memory_offset_backing_store::MemoryOffsetBackingStore;

/// Configuration key for the offset storage file path.
pub const OFFSET_STORAGE_FILE_FILENAME_CONFIG: &str = "offset.storage.file.filename";

/// Implementation of OffsetBackingStore that saves data locally to a file.
///
/// This implementation persists data to a local file. To ensure it behaves
/// similarly to a real backing store, operations are executed asynchronously.
/// This extends MemoryOffsetBackingStore with file persistence capability.
pub struct FileOffsetBackingStore {
    /// The underlying memory store
    memory_store: MemoryOffsetBackingStore,
    /// The file path for storing offsets
    file_path: PathBuf,
    /// Flag indicating if the store is started
    started: Arc<RwLock<bool>>,
    /// Connector partitions tracking
    connector_partitions: DashMap<String, HashSet<HashMap<String, serde_json::Value>>>,
}

/// Data structure for serialization to file.
/// Maps byte arrays to byte arrays.
#[derive(Serialize, Deserialize)]
struct OffsetData {
    /// The offset entries (key -> value)
    data: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl FileOffsetBackingStore {
    /// Creates a new FileOffsetBackingStore.
    ///
    /// # Arguments
    /// * `file_path` - The path to the file for storing offsets
    pub fn new(file_path: PathBuf) -> Self {
        Self {
            memory_store: MemoryOffsetBackingStore::new(),
            file_path,
            started: Arc::new(RwLock::new(false)),
            connector_partitions: DashMap::new(),
        }
    }

    /// Creates a FileOffsetBackingStore with default configuration.
    pub fn with_default_path() -> Self {
        Self::new(PathBuf::from("connect.offsets"))
    }

    /// Loads offset data from the file.
    async fn load(&self) {
        debug!("Loading offsets from file {}", self.file_path.display());

        // Check if file exists
        if !self.file_path.exists() {
            debug!("Offset file {} does not exist, starting with empty state", self.file_path.display());
            return;
        }

        // Read and parse the file
        let content = tokio::fs::read(&self.file_path).await;
        match content {
            Ok(bytes) => {
                // Try to deserialize
                match serde_json::from_slice::<OffsetData>(&bytes) {
                    Ok(offset_data) => {
                        // Load into memory store
                        let data = self.memory_store.data_map();
                        for (key, value) in offset_data.data {
                            if let Some(v) = value {
                                data.insert(key, v);
                            }
                        }
                        info!("Loaded {} offset entries from file", data.len());
                    }
                    Err(e) => {
                        // Could be corrupted or empty file
                        if bytes.is_empty() {
                            debug!("Offset file is empty, starting with empty state");
                        } else {
                            error!("Failed to parse offset file {}: {}", self.file_path.display(), e);
                        }
                    }
                }
            }
            Err(e) => {
                // File might be newly created or corrupted
                debug!("Could not read offset file {}: {}", self.file_path.display(), e);
            }
        }
    }

    /// Saves offset data to the file.
    async fn save(&self) {
        debug!("Saving offsets to file {}", self.file_path.display());

        // Collect data from memory store
        let data = self.memory_store.data_map();
        let mut offset_data = OffsetData {
            data: HashMap::new(),
        };

        for entry in data.iter() {
            offset_data.data.insert(entry.key().clone(), Some(entry.value().clone()));
        }

        // Serialize to JSON
        let bytes = serde_json::to_vec(&offset_data).unwrap_or_default();

        // Write to file
        // Create parent directories if needed
        if let Some(parent) = self.file_path.parent() {
            if !parent.exists() {
                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                    error!("Failed to create directory {}: {}", parent.display(), e);
                    return;
                }
            }
        }

        match tokio::fs::write(&self.file_path, &bytes).await {
            Ok(_) => {
                info!("Saved {} offset entries to file", offset_data.data.len());
            }
            Err(e) => {
                error!("Failed to write offset file {}: {}", self.file_path.display(), e);
            }
        }
    }

    /// Returns the file path for this store.
    pub fn file_path(&self) -> &PathBuf {
        &self.file_path
    }

    /// Returns the number of entries stored.
    pub fn size(&self) -> usize {
        self.memory_store.size()
    }

    /// Clears all stored data and deletes the file.
    pub async fn clear_all(&self) {
        self.memory_store.clear();
        self.connector_partitions.clear();

        // Remove the file
        if self.file_path.exists() {
            if let Err(e) = tokio::fs::remove_file(&self.file_path).await {
                debug!("Could not remove offset file {}: {}", self.file_path.display(), e);
            }
        }
    }
}

impl OffsetBackingStore for FileOffsetBackingStore {
    fn configure(&mut self, config: &WorkerConfig) {
        // Get file path from config
        if let Some(path) = config.get(OFFSET_STORAGE_FILE_FILENAME_CONFIG) {
            self.file_path = PathBuf::from(path);
        }
        debug!("Configured FileOffsetBackingStore with file {}", self.file_path.display());
    }

    fn start(&mut self) {
        let mut started = self.started.blocking_write();
        *started = true;

        info!("Starting FileOffsetBackingStore with file {}", self.file_path.display());

        // Load existing data from file
        // Use blocking approach since start() is synchronous
        let file_path = self.file_path.clone();
        let data = self.memory_store.data_map().clone();

        // Try to load synchronously using blocking approach
        if file_path.exists() {
            if let Ok(bytes) = std::fs::read(&file_path) {
                if !bytes.is_empty() {
                    if let Ok(offset_data) = serde_json::from_slice::<OffsetData>(&bytes) {
                        for (key, value) in offset_data.data {
                            if let Some(v) = value {
                                data.insert(key, v);
                            }
                        }
                    }
                }
            }
        }
    }

    fn stop(&mut self) {
        let mut started = self.started.blocking_write();
        *started = false;

        // Save data to file before stopping
        let data = self.memory_store.data_map().clone();
        let file_path = self.file_path.clone();

        let mut offset_data = OffsetData {
            data: HashMap::new(),
        };

        for entry in data.iter() {
            offset_data.data.insert(entry.key().clone(), Some(entry.value().clone()));
        }

        let bytes = serde_json::to_vec(&offset_data).unwrap_or_default();

        // Create parent directories if needed
        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).unwrap_or_else(|e| {
                    error!("Failed to create directory {}: {}", parent.display(), e);
                });
            }
        }

        std::fs::write(&file_path, &bytes).unwrap_or_else(|e| {
            error!("Failed to write offset file {}: {}", file_path.display(), e);
        });

        info!("Stopped FileOffsetBackingStore");
    }

    fn get(&self, keys: Vec<Vec<u8>>) -> GetFuture {
        self.memory_store.get(keys)
    }

    fn set(&self, values: HashMap<Vec<u8>, Vec<u8>>) -> SetFuture {
        let memory_store_data = self.memory_store.data_map().clone();
        let file_path = self.file_path.clone();
        let started = self.started.clone();

        Box::pin(async move {
            // First, update memory store
            for (key, value) in values.iter() {
                memory_store_data.insert(key.clone(), value.clone());
            }

            // Then, save to file if started
            let is_started = *started.read().await;
            if is_started {
                // Serialize current state
                let mut offset_data = OffsetData {
                    data: HashMap::new(),
                };

                for entry in memory_store_data.iter() {
                    offset_data.data.insert(entry.key().clone(), Some(entry.value().clone()));
                }

                let bytes = serde_json::to_vec(&offset_data).unwrap_or_default();

                // Write to file
                if let Some(parent) = file_path.parent() {
                    if !parent.exists() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                }

                tokio::fs::write(&file_path, &bytes).await?;
            }

            Ok(())
        })
    }

    fn connector_partitions(
        &self,
        connector_name: &str,
    ) -> HashSet<HashMap<String, serde_json::Value>> {
        self.connector_partitions
            .get(connector_name)
            .map(|v| v.clone())
            .unwrap_or_default()
    }
}

impl Default for FileOffsetBackingStore {
    fn default() -> Self {
        Self::with_default_path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_new_store_with_path() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("offsets.json");
        let store = FileOffsetBackingStore::new(file_path.clone());
        assert_eq!(store.file_path(), &file_path);
        assert_eq!(store.size(), 0);
    }

    #[tokio::test]
    async fn test_start_stop_cycle() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("offsets.json");
        let mut store = FileOffsetBackingStore::new(file_path.clone());

        store.start();
        assert_eq!(store.size(), 0);

        // Set some data
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        store.set(HashMap::from([(key.clone(), value.clone())])).await.unwrap();

        store.stop();

        // File should exist
        assert!(file_path.exists());
    }

    #[tokio::test]
    async fn test_persist_and_reload() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("offsets.json");

        // First store: write data
        {
            let mut store1 = FileOffsetBackingStore::new(file_path.clone());
            store1.start();

            let key = vec![1, 2, 3];
            let value = vec![4, 5, 6];
            store1.set(HashMap::from([(key.clone(), value.clone())])).await.unwrap();

            store1.stop();
        }

        // Second store: read data
        {
            let mut store2 = FileOffsetBackingStore::new(file_path.clone());
            store2.start();

            // Data should be loaded
            assert_eq!(store2.size(), 1);

            let key = vec![1, 2, 3];
            let result = store2.get(vec![key.clone()]).await;
            assert_eq!(result.get(&key), Some(&Some(vec![4, 5, 6])));
        }
    }

    #[tokio::test]
    async fn test_multiple_entries() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("offsets.json");
        let mut store = FileOffsetBackingStore::new(file_path.clone());

        store.start();

        let entries = HashMap::from([
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ]);
        store.set(entries).await.unwrap();

        assert_eq!(store.size(), 3);

        store.stop();

        // Verify file content
        let content = std::fs::read(&file_path).unwrap();
        let offset_data: OffsetData = serde_json::from_slice(&content).unwrap();
        assert_eq!(offset_data.data.len(), 3);
    }

    #[tokio::test]
    async fn test_empty_file_handling() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("offsets.json");

        // Create empty file
        std::fs::write(&file_path, "").unwrap();

        let mut store = FileOffsetBackingStore::new(file_path.clone());
        store.start();
        assert_eq!(store.size(), 0);
    }

    #[tokio::test]
    async fn test_missing_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("nonexistent.json");

        let mut store = FileOffsetBackingStore::new(file_path.clone());
        store.start();
        assert_eq!(store.size(), 0);

        // Can still set values
        let key = vec![1];
        let value = vec![2];
        store.set(HashMap::from([(key.clone(), value.clone())])).await.unwrap();
        assert_eq!(store.size(), 1);
    }

    #[tokio::test]
    async fn test_connector_partitions() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("offsets.json");
        let store = FileOffsetBackingStore::new(file_path.clone());

        let partitions = store.connector_partitions("test-connector");
        assert!(partitions.is_empty());
    }

    #[tokio::test]
    async fn test_clear_all() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("offsets.json");
        let mut store = FileOffsetBackingStore::new(file_path.clone());

        store.start();

        let key = vec![1];
        let value = vec![2];
        store.set(HashMap::from([(key.clone(), value.clone())])).await.unwrap();

        assert!(file_path.exists());

        store.clear_all().await;

        assert_eq!(store.size(), 0);
        assert!(!file_path.exists());
    }

    #[test]
    fn test_default_path() {
        let store = FileOffsetBackingStore::with_default_path();
        assert_eq!(store.file_path(), &PathBuf::from("connect.offsets"));
    }

    #[test]
    fn test_offset_data_serde() {
        let data = OffsetData {
            data: HashMap::from([
                (vec![1, 2], Some(vec![3, 4])),
                (vec![5, 6], Some(vec![7, 8])),
            ]),
        };

        let bytes = serde_json::to_vec(&data).unwrap();
        let decoded: OffsetData = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(decoded.data.len(), 2);
        assert_eq!(decoded.data.get(&vec![1, 2]), Some(&Some(vec![3, 4])));
    }
}