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

/// Interface for caches, semi-persistent maps which store key-value mappings
/// until either an eviction criteria is met or the entries are manually
/// invalidated.
///
/// Caches are not required to be thread-safe, but some implementations may be.
///
/// This is the Rust equivalent of the Java `org.apache.kafka.common.cache.Cache` interface.
///
/// # Thread Safety
///
/// All methods use `&self` (immutable reference) to support both single-threaded
/// and multi-threaded implementations:
/// - Single-threaded implementations (like `LruCache`) use interior mutability via `RefCell`
/// - Thread-safe implementations (like `SynchronizedCache`) use `Mutex` for synchronization
///
/// The `get` method returns `Option<V>` (a cloned value) rather than `Option<&V>`
/// to support thread-safe implementations which use `Mutex` internally.
/// Returning a reference would require holding a lock for the lifetime of the reference,
/// which is impractical for a trait interface.
pub trait Cache<K, V: Clone> {
    /// Look up a value in the cache.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// A cloned copy of the cached value, or `None` if it is not present.
    fn get(&self, key: &K) -> Option<V>;

    /// Insert an entry into the cache.
    ///
    /// # Arguments
    /// * `key` - The key to insert
    /// * `value` - The value to insert
    fn put(&self, key: K, value: V);

    /// Manually invalidate a key, clearing its entry from the cache.
    ///
    /// # Arguments
    /// * `key` - The key to remove
    ///
    /// # Returns
    /// The removed value if the key existed in the cache, or `None` if it was not present.
    fn remove(&self, key: &K) -> Option<V>;

    /// Get the number of entries in this cache.
    ///
    /// If this cache is used by multiple threads concurrently, the returned
    /// value will only be approximate.
    ///
    /// # Returns
    /// The number of entries in the cache.
    fn size(&self) -> usize;
}
