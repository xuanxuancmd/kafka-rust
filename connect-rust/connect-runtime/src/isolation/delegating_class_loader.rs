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

//! Delegating class loader for Kafka Connect plugin isolation.
//!
//! This module provides a delegating class loader that manages plugin class loaders
//! and provides a registry for plugin class resolution. In Rust, we use a simplified,
//! configuration-driven approach instead of Java's ClassLoader hierarchy.
//!
//! Corresponds to Java: org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader

use crate::isolation::plugin_desc::PluginDesc;
use crate::isolation::plugin_scan_result::PluginScanResult;
use crate::isolation::plugin_type::PluginType;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// Delegating class loader for managing plugin class loaders.
///
/// In Java, DelegatingClassLoader extends URLClassLoader and provides child-first
/// loading for plugin classes. It maintains a registry of plugin loaders and
/// delegates class loading to the appropriate PluginClassLoader.
///
/// In Rust, we use a simplified, configuration-driven approach:
/// - No actual class loading (Rust uses its own module system)
/// - `plugin_loaders` maps class names to versioned loader entries
/// - `aliases` maps short names/aliases to full class names
/// - Plugin resolution is done through registry lookups
///
/// This approach allows us to track and resolve plugin classes without
/// implementing the full Java classloader hierarchy.
///
/// Corresponds to Java: org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader
#[derive(Debug)]
pub struct DelegatingClassLoader {
    /// Identifier for the parent loader.
    parent_id: String,
    /// Registry of plugin loaders, keyed by class name.
    /// Each class name maps to a sorted map of (PluginDesc -> loader_id).
    plugin_loaders: Arc<RwLock<HashMap<String, BTreeMap<PluginDesc, String>>>>,
    /// Registry of aliases, mapping short names to full class names.
    aliases: Arc<RwLock<HashMap<String, String>>>,
}

impl DelegatingClassLoader {
    /// Creates a new DelegatingClassLoader.
    ///
    /// # Arguments
    /// * `parent_id` - Optional identifier for the parent loader (defaults to "system")
    pub fn new(parent_id: Option<&str>) -> Self {
        Self {
            parent_id: parent_id.unwrap_or("system").to_string(),
            plugin_loaders: Arc::new(RwLock::new(HashMap::new())),
            aliases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the parent loader identifier.
    pub fn parent_id(&self) -> &str {
        &self.parent_id
    }

    /// Installs discovered plugins from a scan result.
    ///
    /// This method registers all discovered plugins with their corresponding
    /// loader identifiers.
    ///
    /// Corresponds to Java: DelegatingClassLoader.installDiscoveredPlugins(PluginScanResult)
    pub fn install_discovered_plugins(&self, scan_result: &PluginScanResult) {
        let mut loaders = self.plugin_loaders.write().unwrap();

        for plugin_desc in scan_result.iter_all() {
            let class_name = plugin_desc.plugin_class().to_string();
            let loader_id = if plugin_desc.is_isolated() {
                plugin_desc.location().to_string()
            } else {
                "classpath".to_string()
            };

            loaders
                .entry(class_name.clone())
                .or_insert_with(BTreeMap::new)
                .insert(plugin_desc.clone(), loader_id);
        }

        // Update aliases
        self.update_aliases_from_plugins(loaders.values());
    }

    /// Updates the alias registry from plugin loaders.
    fn update_aliases_from_plugins(
        &self,
        loaders_values: std::collections::hash_map::Values<
            '_,
            String,
            BTreeMap<PluginDesc, String>,
        >,
    ) {
        let mut aliases = self.aliases.write().unwrap();

        // Compute aliases based on simple class names
        for inner_map in loaders_values {
            for plugin_desc in inner_map.keys() {
                let full_name = plugin_desc.plugin_class();
                let simple_name = plugin_desc.simple_name();

                // Map simple name to full name if not already present
                if !aliases.contains_key(simple_name) {
                    aliases.insert(simple_name.to_string(), full_name.to_string());
                }
            }
        }
    }

    /// Resolves a class or alias to its full class name.
    ///
    /// Corresponds to Java: DelegatingClassLoader.resolveFullClassName(String)
    pub fn resolve_full_class_name(&self, class_or_alias: &str) -> String {
        let aliases = self.aliases.read().unwrap();
        aliases
            .get(class_or_alias)
            .cloned()
            .unwrap_or_else(|| class_or_alias.to_string())
    }

    /// Gets the plugin loader ID for a class.
    ///
    /// Returns the loader identifier for the plugin that owns this class.
    pub fn get_plugin_loader_id(&self, class_name: &str) -> Option<String> {
        let loaders = self.plugin_loaders.read().unwrap();
        loaders.get(class_name).and_then(|inner| {
            // Return the latest version's loader
            inner.values().next().cloned()
        })
    }

    /// Gets the plugin descriptor for a class or alias.
    ///
    /// # Arguments
    /// * `class_or_alias` - The class name or alias
    /// * `preferred_location` - Preferred location to match (optional)
    /// * `allowed_types` - Set of allowed plugin types
    ///
    /// Corresponds to Java: DelegatingClassLoader.pluginDesc(String, String, Set<PluginType>)
    pub fn plugin_desc(
        &self,
        class_or_alias: &str,
        preferred_location: Option<&str>,
        allowed_types: &HashSet<PluginType>,
    ) -> Option<PluginDesc> {
        if class_or_alias.is_empty() {
            return None;
        }

        let full_name = self.resolve_full_class_name(class_or_alias);
        let loaders = self.plugin_loaders.read().unwrap();

        let inner = loaders.get(&full_name)?;
        let mut result: Option<PluginDesc> = None;

        for plugin_desc in inner.keys() {
            if !allowed_types.contains(&plugin_desc.type_()) {
                continue;
            }
            result = Some(plugin_desc.clone());
            if let Some(pref_loc) = preferred_location {
                if plugin_desc.location() == pref_loc {
                    return result;
                }
            }
        }

        result
    }

    /// Gets the connector loader ID for a connector class.
    ///
    /// Corresponds to Java: DelegatingClassLoader.connectorLoader(String)
    pub fn connector_loader_id(&self, connector_class_or_alias: &str) -> String {
        let full_name = self.resolve_full_class_name(connector_class_or_alias);
        self.get_plugin_loader_id(&full_name)
            .unwrap_or_else(|| self.parent_id.clone())
    }

    /// Gets all registered class names.
    pub fn registered_classes(&self) -> Vec<String> {
        let loaders = self.plugin_loaders.read().unwrap();
        loaders.keys().cloned().collect()
    }

    /// Gets all aliases.
    pub fn aliases(&self) -> HashMap<String, String> {
        let aliases = self.aliases.read().unwrap();
        aliases.clone()
    }

    /// Checks if a class is registered.
    pub fn has_class(&self, class_name: &str) -> bool {
        let loaders = self.plugin_loaders.read().unwrap();
        loaders.contains_key(class_name)
    }

    /// Gets the number of registered plugins.
    pub fn plugin_count(&self) -> usize {
        let loaders = self.plugin_loaders.read().unwrap();
        loaders.len()
    }

    /// Clears all registered plugins and aliases.
    pub fn clear(&self) {
        let mut loaders = self.plugin_loaders.write().unwrap();
        let mut aliases = self.aliases.write().unwrap();
        loaders.clear();
        aliases.clear();
    }
}

impl Clone for DelegatingClassLoader {
    fn clone(&self) -> Self {
        Self {
            parent_id: self.parent_id.clone(),
            plugin_loaders: self.plugin_loaders.clone(),
            aliases: self.aliases.clone(),
        }
    }
}

impl Default for DelegatingClassLoader {
    fn default() -> Self {
        Self::new(None)
    }
}

impl std::fmt::Display for DelegatingClassLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DelegatingClassLoader{{parent={}, plugins={}}}",
            self.parent_id,
            self.plugin_count()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_delegating_class_loader() {
        let loader = DelegatingClassLoader::new(None);
        assert_eq!(loader.parent_id(), "system");

        let loader_with_parent = DelegatingClassLoader::new(Some("custom"));
        assert_eq!(loader_with_parent.parent_id(), "custom");
    }

    #[test]
    fn test_default() {
        let loader = DelegatingClassLoader::default();
        assert_eq!(loader.parent_id(), "system");
    }

    #[test]
    fn test_resolve_full_class_name() {
        let loader = DelegatingClassLoader::new(None);

        // Without aliases, returns input unchanged
        let resolved = loader.resolve_full_class_name("TestClass");
        assert_eq!(resolved, "TestClass");
    }

    #[test]
    fn test_has_class() {
        let loader = DelegatingClassLoader::new(None);
        assert!(!loader.has_class("TestClass"));
    }

    #[test]
    fn test_plugin_count() {
        let loader = DelegatingClassLoader::new(None);
        assert_eq!(loader.plugin_count(), 0);
    }

    #[test]
    fn test_clear() {
        let loader = DelegatingClassLoader::new(None);
        loader.clear();
        assert_eq!(loader.plugin_count(), 0);
    }

    #[test]
    fn test_display() {
        let loader = DelegatingClassLoader::new(Some("parent"));
        let display = format!("{}", loader);
        assert!(display.contains("parent"));
        assert!(display.contains("plugins=0"));
    }

    #[test]
    fn test_clone() {
        let loader = DelegatingClassLoader::new(Some("parent"));
        let cloned = loader.clone();
        assert_eq!(cloned.parent_id(), loader.parent_id());
    }

    #[test]
    fn test_connector_loader_id() {
        let loader = DelegatingClassLoader::new(None);
        // Without registered plugins, returns parent
        let id = loader.connector_loader_id("TestConnector");
        assert_eq!(id, "system");
    }

    #[test]
    fn test_registered_classes() {
        let loader = DelegatingClassLoader::new(None);
        let classes = loader.registered_classes();
        assert_eq!(classes.len(), 0);
    }

    #[test]
    fn test_aliases() {
        let loader = DelegatingClassLoader::new(None);
        let aliases = loader.aliases();
        assert_eq!(aliases.len(), 0);
    }
}
