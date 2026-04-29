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

use std::cell::RefCell;
use std::num::NonZeroUsize;

use lru::LruCache as LruCacheInner;

use crate::cache::Cache;

/// A simple Least Recently Used (LRU) cache implementation.
///
/// This cache evicts the least recently used entry when the number of entries
/// exceeds the maximum size configured during construction.
///
/// This is the Rust equivalent of the Java `org.apache.kafka.common.cache.LRUCache` class.
///
/// Uses `RefCell` for interior mutability to satisfy the `Cache` trait's
/// `&self` method signatures while allowing mutation of the underlying cache.
///
/// # Example
///
/// ```
/// use common_trait::cache::{Cache, LruCache};
///
/// let cache = LruCache::<String, String>::new(16);
/// cache.put("key1".to_string(), "value1".to_string());
/// assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
/// ```
pub struct LruCache<K, V> {
    cache: RefCell<LruCacheInner<K, V>>,
}

/// Default capacity for LruCache when not explicitly specified.
pub const DEFAULT_CAPACITY: usize = 16;

impl<K: std::hash::Hash + Eq, V> LruCache<K, V> {
    /// Creates a new LruCache with the specified maximum capacity.
    ///
    /// # Arguments
    /// * `capacity` - The maximum number of entries the cache can hold.
    ///                When the cache exceeds this size, the least recently
    ///                used entry will be evicted.
    ///
    /// # Panics
    /// Panics if `capacity` is 0.
    ///
    /// # Example
    ///
    /// ```
    /// use common_trait::cache::LruCache;
    ///
    /// let cache = LruCache::<String, i32>::new(100);
    /// ```
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: RefCell::new(LruCacheInner::new(
                NonZeroUsize::new(capacity).expect("capacity must be non-zero"),
            )),
        }
    }

    /// Creates a new LruCache with the default capacity (16 entries).
    ///
    /// # Example
    ///
    /// ```
    /// use common_trait::cache::LruCache;
    ///
    /// let cache = LruCache::<String, i32>::with_default_capacity();
    /// ```
    pub fn with_default_capacity() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl<K: std::hash::Hash + Eq, V: Clone> Cache<K, V> for LruCache<K, V> {
    /// Look up a value in the cache.
    ///
    /// If the entry exists, this returns a cloned copy of the value.
    /// Note: Unlike Java's LRUCache.get(), this does not update the LRU order
    /// (uses peek internally) to match Kafka's behavior where order updates
    /// only happen on put operations.
    fn get(&self, key: &K) -> Option<V> {
        self.cache.borrow().peek(key).cloned()
    }

    /// Insert an entry into the cache.
    ///
    /// If the cache already contains an entry with this key, the existing entry
    /// is replaced with the new value and promoted to the most recently used position.
    ///
    /// If inserting a new entry would exceed the maximum capacity, the least
    /// recently used entry is evicted before the insertion.
    fn put(&self, key: K, value: V) {
        self.cache.borrow_mut().put(key, value);
    }

    /// Manually invalidate a key, clearing its entry from the cache.
    ///
    /// Returns the removed value if the key existed, or `None` otherwise.
    fn remove(&self, key: &K) -> Option<V> {
        self.cache.borrow_mut().pop(key)
    }

    /// Get the number of entries currently in the cache.
    fn size(&self) -> usize {
        self.cache.borrow().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let cache = LruCache::<String, String>::new(3);

        // Insert entries
        cache.put("a".to_string(), "value_a".to_string());
        cache.put("b".to_string(), "value_b".to_string());
        cache.put("c".to_string(), "value_c".to_string());

        // Verify size
        assert_eq!(cache.size(), 3);

        // Verify get
        assert_eq!(cache.get(&"a".to_string()), Some("value_a".to_string()));
        assert_eq!(cache.get(&"b".to_string()), Some("value_b".to_string()));
        assert_eq!(cache.get(&"c".to_string()), Some("value_c".to_string()));

        // Add fourth entry - should evict least recently used
        cache.put("d".to_string(), "value_d".to_string());
        assert_eq!(cache.size(), 3);

        // "a" should be evicted (least recently used)
        assert_eq!(cache.get(&"a".to_string()), None);
        assert_eq!(cache.get(&"b".to_string()), Some("value_b".to_string()));
        assert_eq!(cache.get(&"c".to_string()), Some("value_c".to_string()));
        assert_eq!(cache.get(&"d".to_string()), Some("value_d".to_string()));
    }

    #[test]
    fn test_remove() {
        let cache = LruCache::<i32, String>::new(5);
        cache.put(1, "one".to_string());
        cache.put(2, "two".to_string());

        assert_eq!(cache.remove(&1), Some("one".to_string()));
        assert_eq!(cache.remove(&1), None); // Already removed
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn test_default_capacity() {
        let cache = LruCache::<String, i32>::with_default_capacity();
        assert_eq!(DEFAULT_CAPACITY, 16);
    }

    #[test]
    fn test_update_existing_key() {
        let cache = LruCache::<String, String>::new(2);
        cache.put("key".to_string(), "old".to_string());
        cache.put("key".to_string(), "new".to_string());

        assert_eq!(cache.size(), 1);
        assert_eq!(cache.get(&"key".to_string()), Some("new".to_string()));
    }

    #[test]
    #[should_panic(expected = "capacity must be non-zero")]
    fn test_zero_capacity_panics() {
        let _cache = LruCache::<String, String>::new(0);
    }
}
