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

use std::hash::Hash;
use std::sync::Mutex;

use crate::cache::{Cache, LruCache};

/// A thread-safe wrapper around a Cache implementation.
///
/// This cache provides synchronized access to an underlying cache,
/// ensuring thread-safety by using a Mutex to protect all operations.
///
/// This is the Rust equivalent of the Java `org.apache.kafka.common.cache.SynchronizedCache` class.
/// In Java, synchronization is achieved using the `synchronized` keyword on methods.
/// In Rust, we use `Mutex` for mutual exclusion.
///
/// # Example
///
/// ```
/// use common_trait::cache::{Cache, SynchronizedCache};
///
/// let cache = SynchronizedCache::<String, String>::new(16);
/// cache.put("key1".to_string(), "value1".to_string());
/// assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
/// ```
pub struct SynchronizedCache<K, V> {
    cache: Mutex<LruCache<K, V>>,
}

impl<K: Hash + Eq, V: Clone> SynchronizedCache<K, V> {
    /// Creates a new SynchronizedCache with the specified maximum capacity.
    ///
    /// The underlying LruCache will be created with the given capacity,
    /// evicting the least recently used entry when the number of entries
    /// exceeds this limit.
    ///
    /// # Arguments
    /// * `capacity` - The maximum number of entries the cache can hold.
    ///
    /// # Panics
    /// Panics if `capacity` is 0.
    ///
    /// # Example
    ///
    /// ```
    /// use common_trait::cache::SynchronizedCache;
    ///
    /// let cache = SynchronizedCache::<String, i32>::new(100);
    /// ```
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(capacity)),
        }
    }

    /// Creates a new SynchronizedCache with the default capacity (16 entries).
    ///
    /// # Example
    ///
    /// ```
    /// use common_trait::cache::SynchronizedCache;
    ///
    /// let cache = SynchronizedCache::<String, i32>::with_default_capacity();
    /// ```
    pub fn with_default_capacity() -> Self {
        Self {
            cache: Mutex::new(LruCache::with_default_capacity()),
        }
    }
}

impl<K: Hash + Eq, V: Clone> Cache<K, V> for SynchronizedCache<K, V> {
    /// Look up a value in the cache.
    ///
    /// This method acquires a lock on the underlying cache and returns
    /// a cloned copy of the value if present. The lock is released before
    /// returning, allowing other threads to access the cache.
    ///
    /// # Thread Safety
    /// Uses `Mutex::lock()` to ensure exclusive access during the lookup.
    fn get(&self, key: &K) -> Option<V> {
        self.cache.lock().unwrap().get(key)
    }

    /// Insert an entry into the cache.
    ///
    /// This method acquires a lock on the underlying cache and inserts
    /// the key-value pair. The lock is released after the insertion.
    ///
    /// # Thread Safety
    /// Uses `Mutex::lock()` to ensure exclusive access during the insertion.
    fn put(&self, key: K, value: V) {
        self.cache.lock().unwrap().put(key, value);
    }

    /// Manually invalidate a key, clearing its entry from the cache.
    ///
    /// This method acquires a lock on the underlying cache and removes
    /// the entry if it exists. The lock is released after the removal.
    ///
    /// # Thread Safety
    /// Uses `Mutex::lock()` to ensure exclusive access during the removal.
    fn remove(&self, key: &K) -> Option<V> {
        self.cache.lock().unwrap().remove(key)
    }

    /// Get the number of entries in the cache.
    ///
    /// This method acquires a lock on the underlying cache and returns
    /// the current number of entries. Note that in a multi-threaded
    /// environment, this value may be stale immediately after returning.
    ///
    /// # Thread Safety
    /// Uses `Mutex::lock()` to ensure exclusive access during the count.
    fn size(&self) -> usize {
        self.cache.lock().unwrap().size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_operations() {
        let cache = SynchronizedCache::<String, String>::new(3);

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
        let cache = SynchronizedCache::<i32, String>::new(5);
        cache.put(1, "one".to_string());
        cache.put(2, "two".to_string());

        assert_eq!(cache.remove(&1), Some("one".to_string()));
        assert_eq!(cache.remove(&1), None); // Already removed
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn test_default_capacity() {
        let cache = SynchronizedCache::<String, i32>::with_default_capacity();
        assert_eq!(cache.size(), 0); // Empty cache
    }

    #[test]
    fn test_update_existing_key() {
        let cache = SynchronizedCache::<String, String>::new(2);
        cache.put("key".to_string(), "old".to_string());
        cache.put("key".to_string(), "new".to_string());

        assert_eq!(cache.size(), 1);
        assert_eq!(cache.get(&"key".to_string()), Some("new".to_string()));
    }

    #[test]
    #[should_panic(expected = "capacity must be non-zero")]
    fn test_zero_capacity_panics() {
        let _cache = SynchronizedCache::<String, String>::new(0);
    }

    #[test]
    fn test_thread_safety() {
        let cache = Arc::new(SynchronizedCache::<i32, i32>::new(100));
        let mut handles = vec![];

        // Spawn multiple threads writing to the cache
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                for j in 0..10 {
                    cache_clone.put(i * 10 + j, i * 10 + j);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all entries are present (or some were evicted due to capacity)
        // With capacity 100 and 100 entries, some might be evicted
        let size = cache.size();
        assert!(size > 0 && size <= 100);
    }

    #[test]
    fn test_concurrent_read_write() {
        let cache = Arc::new(SynchronizedCache::<String, String>::new(50));
        let mut handles = vec![];

        // Writer thread
        let writer_cache = Arc::clone(&cache);
        let writer = thread::spawn(move || {
            for i in 0..20 {
                writer_cache.put(format!("key_{}", i), format!("value_{}", i));
            }
        });
        handles.push(writer);

        // Reader threads
        for _ in 0..5 {
            let reader_cache = Arc::clone(&cache);
            let reader = thread::spawn(move || {
                for i in 0..20 {
                    // May return None if writer hasn't written yet, or Some if written
                    let _ = reader_cache.get(&format!("key_{}", i));
                }
            });
            handles.push(reader);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
