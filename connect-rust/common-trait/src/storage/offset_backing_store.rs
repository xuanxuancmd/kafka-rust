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

//! OffsetBackingStore trait for storing source connector offsets.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use super::types;

/// Type alias for boxed future returned by get().
pub type GetFuture = Pin<Box<dyn Future<Output = HashMap<Vec<u8>, Option<Vec<u8>>>> + Send>>;

/// Type alias for boxed future returned by set().
pub type SetFuture =
    Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>;

/// OffsetBackingStore is an interface for storage backends that store key-value data.
///
/// The backing store doesn't need to handle serialization or deserialization.
/// It only needs to support reading/writing bytes. Since it is expected these
/// operations will require network operations, only bulk operations are supported.
///
/// Since OffsetBackingStore is a shared resource that may be used by many OffsetStorage
/// instances that are associated with individual tasks, the caller must be sure keys
/// include information about the connector so that the shared namespace does not
/// result in conflicting keys.
pub trait OffsetBackingStore: Send + Sync {
    /// Configure class with the given key-value pairs.
    ///
    /// # Arguments
    /// * `config` - Worker configuration (can be DistributedConfig or StandaloneConfig)
    fn configure(&mut self, config: &types::WorkerConfig);

    /// Start this offset store.
    fn start(&mut self);

    /// Stop the backing store.
    /// Implementations should attempt to shutdown gracefully, but not block indefinitely.
    fn stop(&mut self);

    /// Get the values for the specified keys.
    ///
    /// # Arguments
    /// * `keys` - List of keys to look up
    ///
    /// # Returns
    /// A future for the resulting map from key to value. Values may be null if the key doesn't exist.
    fn get(&self, keys: Vec<Vec<u8>>) -> GetFuture;

    /// Set the specified keys and values.
    ///
    /// # Arguments
    /// * `values` - Map from key to value
    ///
    /// # Returns
    /// A future that completes when the operation is done
    fn set(&self, values: HashMap<Vec<u8>, Vec<u8>>) -> SetFuture;

    /// Get all the partitions for the specified connector.
    ///
    /// # Arguments
    /// * `connector_name` - The name of the connector whose partitions are to be retrieved
    ///
    /// # Returns
    /// Set of connector partitions. Each partition is represented as a map of partition attributes.
    fn connector_partitions(
        &self,
        connector_name: &str,
    ) -> HashSet<HashMap<String, serde_json::Value>>;
}
