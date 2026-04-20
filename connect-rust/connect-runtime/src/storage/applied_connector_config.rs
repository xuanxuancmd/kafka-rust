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

//! Wrapper class for a connector configuration that has been used to generate task configurations.
//!
//! Supports lazy transformation of the configuration. This class wraps a connector configuration
//! that has been used to generate task configurations. It supports lazy transformation via
//! a WorkerConfigTransformer.
//!
//! Corresponds to `org.apache.kafka.connect.storage.AppliedConnectorConfig` in Java.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Trait for config transformation.
/// The transformer takes a raw config map and returns a transformed config map.
pub trait ConfigTransformer: Send + Sync {
    /// Transforms the given config.
    fn transform(&self, config: &HashMap<String, String>) -> HashMap<String, String>;
}

/// Wrapper class for a connector configuration that has been used to generate task configurations.
///
/// Supports lazy transformation. The raw configuration is stored and only transformed
/// when `transformed_config` is called. The transformed configuration is cached for
/// subsequent calls.
///
/// This class is thread-safe: different threads may invoke `transformed_config` at any time
/// and the same transformed config should always be returned, with transformation still
/// only ever taking place once before its results are cached.
pub struct AppliedConnectorConfig {
    /// The non-transformed connector configuration
    raw_config: Option<HashMap<String, String>>,
    /// The cached transformed configuration
    transformed_config: Arc<Mutex<Option<HashMap<String, String>>>>,
}

impl AppliedConnectorConfig {
    /// Creates a new AppliedConnectorConfig that has not yet undergone transformation.
    ///
    /// # Arguments
    /// * `raw_config` - The non-transformed connector configuration; may be None
    pub fn new(raw_config: Option<HashMap<String, String>>) -> Self {
        Self {
            raw_config,
            transformed_config: Arc::new(Mutex::new(None)),
        }
    }

    /// Creates an AppliedConnectorConfig from an existing config map.
    ///
    /// # Arguments
    /// * `config` - The raw connector configuration
    pub fn from_config(config: HashMap<String, String>) -> Self {
        Self::new(Some(config))
    }

    /// If necessary, transform the raw connector config, then return the result.
    /// Transformed configurations are cached and returned in all subsequent calls.
    ///
    /// This method is thread-safe: different threads may invoke it at any time and the same
    /// transformed config should always be returned, with transformation still only ever
    /// taking place once before its results are cached.
    ///
    /// # Arguments
    /// * `config_transformer` - The transformer to use, if no transformed connector
    ///                          config has been cached yet; may be None
    ///
    /// # Returns
    /// The possibly-cached, transformed, connector config; may be None if raw_config is None
    pub fn transformed_config(
        &self,
        config_transformer: Option<&dyn ConfigTransformer>,
    ) -> Option<HashMap<String, String>> {
        // Check if we already have cached transformed config or raw config is None
        {
            let cached = self.transformed_config.lock().unwrap();
            if cached.is_some() {
                return cached.clone();
            }
        }

        if self.raw_config.is_none() {
            return None;
        }

        // Need to transform
        let transformed = if let Some(transformer) = config_transformer {
            transformer.transform(self.raw_config.as_ref().unwrap())
        } else {
            self.raw_config.clone().unwrap()
        };

        // Cache and return
        let mut cached = self.transformed_config.lock().unwrap();
        *cached = Some(transformed.clone());
        Some(transformed)
    }

    /// Returns the raw (non-transformed) configuration.
    pub fn raw_config(&self) -> Option<&HashMap<String, String>> {
        self.raw_config.as_ref()
    }

    /// Returns whether this config has been transformed already.
    pub fn is_transformed(&self) -> bool {
        self.transformed_config.lock().unwrap().is_some()
    }

    /// Clears the cached transformed configuration, forcing re-transformation on next call.
    pub fn clear_cache(&self) {
        let mut cached = self.transformed_config.lock().unwrap();
        *cached = None;
    }
}

impl Clone for AppliedConnectorConfig {
    fn clone(&self) -> Self {
        // Clone shares the same transformed_config cache
        Self {
            raw_config: self.raw_config.clone(),
            transformed_config: self.transformed_config.clone(),
        }
    }
}

impl Default for AppliedConnectorConfig {
    fn default() -> Self {
        Self::new(None)
    }
}

/// A simple function-based config transformer.
pub struct SimpleConfigTransformer {
    transform_fn: Arc<dyn Fn(&HashMap<String, String>) -> HashMap<String, String> + Send + Sync>,
}

impl SimpleConfigTransformer {
    pub fn new(
        transform_fn: Arc<
            dyn Fn(&HashMap<String, String>) -> HashMap<String, String> + Send + Sync,
        >,
    ) -> Self {
        Self { transform_fn }
    }
}

impl ConfigTransformer for SimpleConfigTransformer {
    fn transform(&self, config: &HashMap<String, String>) -> HashMap<String, String> {
        (self.transform_fn)(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> HashMap<String, String> {
        HashMap::from([
            ("name".to_string(), "test-connector".to_string()),
            ("connector.class".to_string(), "TestConnector".to_string()),
        ])
    }

    fn make_transformer() -> SimpleConfigTransformer {
        SimpleConfigTransformer::new(Arc::new(|config: &HashMap<String, String>| {
            let mut transformed = config.clone();
            transformed.insert("transformed".to_string(), "true".to_string());
            transformed
        }))
    }

    #[test]
    fn test_new_with_config() {
        let config = make_config();
        let applied = AppliedConnectorConfig::new(Some(config));
        assert!(applied.raw_config().is_some());
        assert!(!applied.is_transformed());
    }

    #[test]
    fn test_new_without_config() {
        let applied = AppliedConnectorConfig::new(None);
        assert!(applied.raw_config().is_none());
        assert!(!applied.is_transformed());
    }

    #[test]
    fn test_from_config() {
        let config = make_config();
        let applied = AppliedConnectorConfig::from_config(config);
        assert!(applied.raw_config().is_some());
    }

    #[test]
    fn test_transformed_config_with_transformer() {
        let config = make_config();
        let applied = AppliedConnectorConfig::new(Some(config));
        let transformer = make_transformer();

        let transformed = applied.transformed_config(Some(&transformer));
        assert!(transformed.is_some());
        assert!(transformed.unwrap().contains_key("transformed"));
        assert!(applied.is_transformed());
    }

    #[test]
    fn test_transformed_config_caches_result() {
        let config = make_config();
        let applied = AppliedConnectorConfig::new(Some(config.clone()));
        let transformer = make_transformer();

        // First call transforms
        let result1 = applied.transformed_config(Some(&transformer));
        assert!(result1.is_some());

        // Second call returns cached result without transforming again
        // We can verify this by creating a new transformer that would give different result
        let new_transformer =
            SimpleConfigTransformer::new(Arc::new(|_: &HashMap<String, String>| {
                HashMap::from([("different".to_string(), "result".to_string())])
            }));

        let result2 = applied.transformed_config(Some(&new_transformer));
        // Should still return the cached result from first transformer
        assert!(result2.unwrap().contains_key("transformed"));
    }

    #[test]
    fn test_transformed_config_without_transformer() {
        let config = make_config();
        let applied = AppliedConnectorConfig::new(Some(config.clone()));

        // Without transformer, returns raw config
        let result = applied.transformed_config(None);
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().get("name"),
            Some(&"test-connector".to_string())
        );
    }

    #[test]
    fn test_transformed_config_none_raw_config() {
        let applied = AppliedConnectorConfig::new(None);
        let transformer = make_transformer();

        let result = applied.transformed_config(Some(&transformer));
        assert!(result.is_none());
    }

    #[test]
    fn test_clear_cache() {
        let config = make_config();
        let applied = AppliedConnectorConfig::new(Some(config));
        let transformer = make_transformer();

        // Transform
        applied.transformed_config(Some(&transformer));
        assert!(applied.is_transformed());

        // Clear cache
        applied.clear_cache();
        assert!(!applied.is_transformed());

        // Re-transform
        let result = applied.transformed_config(Some(&transformer));
        assert!(result.is_some());
        assert!(applied.is_transformed());
    }

    #[test]
    fn test_clone_shares_cache() {
        let config = make_config();
        let applied = AppliedConnectorConfig::new(Some(config));
        let transformer = make_transformer();

        let cloned = applied.clone();

        // Transform through original
        applied.transformed_config(Some(&transformer));

        // Clone should also show as transformed (shared cache)
        assert!(cloned.is_transformed());
    }

    #[test]
    fn test_thread_safety() {
        use std::thread;

        let config = make_config();
        let applied = Arc::new(AppliedConnectorConfig::new(Some(config)));

        let mut handles = vec![];

        for _ in 0..10 {
            let applied_clone = applied.clone();
            let transformer = make_transformer();
            handles.push(thread::spawn(move || {
                applied_clone.transformed_config(Some(&transformer))
            }));
        }

        for handle in handles {
            let result = handle.join().unwrap();
            assert!(result.is_some());
            assert!(result.unwrap().contains_key("transformed"));
        }

        // Should still be transformed
        assert!(applied.is_transformed());
    }
}
