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

//! CachedConnectors - Connector instance cache for AbstractHerder.
//!
//! Manages cached connector metadata to avoid repeated plugin loading.
//! Corresponds to `org.apache.kafka.connect.runtime.AbstractHerder.CachedConnectors` in Java.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use common_trait::herder::Plugins;

/// CachedConnector - Represents cached connector metadata.
///
/// Stores connector class information without a live instance.
#[derive(Debug, Clone)]
pub struct CachedConnector {
    /// Connector class name.
    class_name: String,
    /// Connector version.
    version: String,
    /// Whether this is a source connector.
    is_source: bool,
}

impl CachedConnector {
    /// Create a new CachedConnector.
    pub fn new(class_name: String, version: String, is_source: bool) -> Self {
        CachedConnector {
            class_name,
            version,
            is_source,
        }
    }

    /// Get the class name.
    pub fn class_name(&self) -> &str {
        &self.class_name
    }

    /// Get the version.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Check if this is a source connector.
    pub fn is_source(&self) -> bool {
        self.is_source
    }
}

/// CachedConnectors - Cache for connector metadata.
///
/// Provides simple caching of connector class metadata.
/// Used by AbstractHerder for connector type detection.
///
/// Thread safety: Uses RwLock for concurrent read access.
///
/// Corresponds to `org.apache.kafka.connect.runtime.AbstractHerder.CachedConnectors` in Java.
pub struct CachedConnectors {
    /// Cached connectors indexed by class_name.
    cache: RwLock<HashMap<String, CachedConnector>>,
    /// Plugins instance (optional, for future use).
    _plugins: Option<Arc<dyn Plugins>>,
}

impl CachedConnectors {
    /// Create a new CachedConnectors with plugins.
    ///
    /// # Arguments
    /// * `_plugins` - Plugins instance (stored for future use).
    pub fn new(_plugins: Arc<dyn Plugins>) -> Self {
        CachedConnectors {
            cache: RwLock::new(HashMap::new()),
            _plugins: Some(_plugins),
        }
    }

    /// Create a new empty CachedConnectors without plugins.
    pub fn new_empty() -> Self {
        CachedConnectors {
            cache: RwLock::new(HashMap::new()),
            _plugins: None,
        }
    }

    /// Get a cached connector by class name.
    ///
    /// Returns the cached connector if present, otherwise creates
    /// a default entry based on class name pattern.
    ///
    /// # Arguments
    /// * `class_name` - The connector class name or alias.
    ///
    /// # Returns
    /// The cached connector metadata.
    pub fn get_connector(&self, class_name: &str) -> CachedConnector {
        // Check cache first
        {
            let cache = self.cache.read().unwrap();
            if let Some(connector) = cache.get(class_name) {
                return connector.clone();
            }
        }

        // Create default cached connector based on class name pattern
        let is_source = class_name.contains("Source");
        let cached = CachedConnector::new(class_name.to_string(), "1.0.0".to_string(), is_source);

        // Cache it
        {
            let mut cache = self.cache.write().unwrap();
            cache.insert(class_name.to_string(), cached.clone());
        }

        cached
    }

    /// Add a connector to the cache explicitly.
    pub fn put_connector(&self, class_name: &str, cached: CachedConnector) {
        let mut cache = self.cache.write().unwrap();
        cache.insert(class_name.to_string(), cached);
    }

    /// Clear the connector cache.
    pub fn clear(&self) {
        let mut cache = self.cache.write().unwrap();
        cache.clear();
    }

    /// Get the number of cached connectors.
    pub fn size(&self) -> usize {
        self.cache.read().unwrap().len()
    }

    /// Check if a connector is cached.
    pub fn contains(&self, class_name: &str) -> bool {
        self.cache.read().unwrap().contains_key(class_name)
    }
}

impl Default for CachedConnectors {
    fn default() -> Self {
        Self::new_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_connector_creation() {
        let connector =
            CachedConnector::new("FileStreamSource".to_string(), "1.0.0".to_string(), true);
        assert_eq!(connector.class_name(), "FileStreamSource");
        assert_eq!(connector.version(), "1.0.0");
        assert!(connector.is_source());
    }

    #[test]
    fn test_cached_connectors_new_empty() {
        let cached = CachedConnectors::new_empty();
        assert_eq!(cached.size(), 0);
    }

    #[test]
    fn test_cached_connectors_get_connector() {
        let cached = CachedConnectors::new_empty();

        let connector = cached.get_connector("FileStreamSource");
        assert!(connector.is_source());
        assert_eq!(cached.size(), 1);
    }

    #[test]
    fn test_cached_connectors_get_sink_connector() {
        let cached = CachedConnectors::new_empty();

        let connector = cached.get_connector("FileStreamSink");
        assert!(!connector.is_source());
    }

    #[test]
    fn test_cached_connectors_clear() {
        let cached = CachedConnectors::new_empty();
        cached.get_connector("test");
        assert_eq!(cached.size(), 1);

        cached.clear();
        assert_eq!(cached.size(), 0);
    }

    #[test]
    fn test_cached_connectors_put_connector() {
        let cached = CachedConnectors::new_empty();

        let connector =
            CachedConnector::new("CustomConnector".to_string(), "2.0.0".to_string(), true);
        cached.put_connector("CustomConnector", connector);

        assert!(cached.contains("CustomConnector"));
        let retrieved = cached.get_connector("CustomConnector");
        assert_eq!(retrieved.version(), "2.0.0");
    }
}
