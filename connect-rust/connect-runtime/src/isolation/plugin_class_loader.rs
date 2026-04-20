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

//! Plugin class loader for Kafka Connect plugin isolation.
//!
//! This module provides a plugin-specific class loader that represents
//! the loading context for a plugin. In Rust, we use a simplified,
//! configuration-driven approach instead of Java's ClassLoader hierarchy.
//!
//! Corresponds to Java: org.apache.kafka.connect.runtime.isolation.PluginClassLoader

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Plugin class loader representing a plugin's loading context.
///
/// In Java, PluginClassLoader extends URLClassLoader and provides child-first
/// class loading for plugin isolation. It loads classes from its URLs first,
/// then delegates to the parent for classes not found or that shouldn't be
/// loaded in isolation.
///
/// In Rust, we use a simplified, configuration-driven approach:
/// - No actual class loading (Rust uses its own module system)
/// - `plugin_location` represents the plugin's location (JAR path or directory)
/// - `urls` represent the resources associated with this plugin
/// - `parent_id` is a string identifier for the parent loader
/// - An internal registry maps class names to their source locations
///
/// This approach allows us to track which plugin "owns" which classes without
/// implementing the full Java classloader hierarchy.
///
/// Corresponds to Java: org.apache.kafka.connect.runtime.isolation.PluginClassLoader
#[derive(Debug)]
pub struct PluginClassLoader {
    /// The top-level location of the plugin (JAR path or directory).
    plugin_location: String,
    /// URLs/resources associated with this plugin.
    urls: Vec<String>,
    /// Identifier for the parent loader.
    parent_id: String,
    /// Registry of classes loaded by this loader.
    /// Maps class names to their source locations.
    class_registry: Arc<RwLock<HashMap<String, String>>>,
}

impl PluginClassLoader {
    /// Creates a new PluginClassLoader.
    ///
    /// # Arguments
    /// * `plugin_location` - The top-level location of the plugin
    /// * `urls` - URLs/resources associated with this plugin
    /// * `parent_id` - Identifier for the parent loader
    pub fn new(plugin_location: &str, urls: Vec<String>, parent_id: &str) -> Self {
        Self {
            plugin_location: plugin_location.to_string(),
            urls,
            parent_id: parent_id.to_string(),
            class_registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the plugin location.
    ///
    /// Corresponds to Java: PluginClassLoader.location()
    pub fn location(&self) -> &str {
        &self.plugin_location
    }

    /// Returns the URLs associated with this plugin.
    pub fn urls(&self) -> &[String] {
        &self.urls
    }

    /// Returns the parent loader identifier.
    pub fn parent_id(&self) -> &str {
        &self.parent_id
    }

    /// Registers a class as being loaded by this loader.
    ///
    /// In Java, this happens during actual class loading.
    /// In Rust, we use this to track class ownership.
    pub fn register_class(&self, class_name: &str, source: &str) {
        let mut registry = self.class_registry.write().unwrap();
        registry.insert(class_name.to_string(), source.to_string());
    }

    /// Checks if a class is registered with this loader.
    pub fn has_class(&self, class_name: &str) -> bool {
        let registry = self.class_registry.read().unwrap();
        registry.contains_key(class_name)
    }

    /// Gets the source location for a registered class.
    pub fn get_class_source(&self, class_name: &str) -> Option<String> {
        let registry = self.class_registry.read().unwrap();
        registry.get(class_name).cloned()
    }

    /// Returns all classes registered with this loader.
    pub fn registered_classes(&self) -> Vec<String> {
        let registry = self.class_registry.read().unwrap();
        registry.keys().cloned().collect()
    }

    /// Creates a description string for this loader.
    pub fn description(&self) -> String {
        format!(
            "PluginClassLoader{{pluginLocation={}, urls={}, parent={}}}",
            self.plugin_location,
            self.urls.len(),
            self.parent_id
        )
    }
}

impl Clone for PluginClassLoader {
    fn clone(&self) -> Self {
        Self {
            plugin_location: self.plugin_location.clone(),
            urls: self.urls.clone(),
            parent_id: self.parent_id.clone(),
            class_registry: self.class_registry.clone(),
        }
    }
}

impl std::fmt::Display for PluginClassLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PluginClassLoader{{pluginLocation={}}}",
            self.plugin_location
        )
    }
}

impl Default for PluginClassLoader {
    fn default() -> Self {
        Self::new("", Vec::new(), "system")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_plugin_class_loader() {
        let loader = PluginClassLoader::new(
            "/path/to/plugin.jar",
            vec!["file:///path/to/plugin.jar".to_string()],
            "parent",
        );
        assert_eq!(loader.location(), "/path/to/plugin.jar");
        assert_eq!(loader.urls().len(), 1);
        assert_eq!(loader.parent_id(), "parent");
    }

    #[test]
    fn test_register_class() {
        let loader = PluginClassLoader::new("/plugin.jar", vec![], "parent");
        assert!(!loader.has_class("TestClass"));

        loader.register_class("TestClass", "/plugin.jar/TestClass.class");
        assert!(loader.has_class("TestClass"));
        assert_eq!(
            loader.get_class_source("TestClass"),
            Some("/plugin.jar/TestClass.class".to_string())
        );
    }

    #[test]
    fn test_registered_classes() {
        let loader = PluginClassLoader::new("/plugin.jar", vec![], "parent");
        loader.register_class("ClassA", "/source/a");
        loader.register_class("ClassB", "/source/b");

        let classes = loader.registered_classes();
        assert_eq!(classes.len(), 2);
        assert!(classes.contains(&"ClassA".to_string()));
        assert!(classes.contains(&"ClassB".to_string()));
    }

    #[test]
    fn test_display() {
        let loader = PluginClassLoader::new("/plugin.jar", vec![], "parent");
        let display = format!("{}", loader);
        assert!(display.contains("/plugin.jar"));
    }

    #[test]
    fn test_description() {
        let loader = PluginClassLoader::new(
            "/plugin.jar",
            vec!["url1".to_string(), "url2".to_string()],
            "parent",
        );
        let desc = loader.description();
        assert!(desc.contains("/plugin.jar"));
        assert!(desc.contains("urls=2"));
        assert!(desc.contains("parent"));
    }

    #[test]
    fn test_clone() {
        let loader = PluginClassLoader::new("/plugin.jar", vec!["url".to_string()], "parent");
        loader.register_class("TestClass", "/source");

        let cloned = loader.clone();
        assert_eq!(cloned.location(), loader.location());
        // Cloned should share the same registry
        assert!(cloned.has_class("TestClass"));
    }

    #[test]
    fn test_default() {
        let loader = PluginClassLoader::default();
        assert_eq!(loader.location(), "");
        assert_eq!(loader.urls().len(), 0);
        assert_eq!(loader.parent_id(), "system");
    }
}
