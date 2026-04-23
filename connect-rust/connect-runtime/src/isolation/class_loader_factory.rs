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

//! Class loader factory for Kafka Connect plugin isolation.
//!
//! This module provides a factory for creating plugin class loaders.
//! In Rust, we use a configuration-driven approach instead of Java's ClassLoader hierarchy.
//!
//! Corresponds to Java: org.apache.kafka.connect.runtime.isolation.ClassLoaderFactory

use crate::isolation::delegating_class_loader::DelegatingClassLoader;
use crate::isolation::plugin_class_loader::PluginClassLoader;
use crate::isolation::plugin_class_loader_factory::PluginClassLoaderFactory;

/// Factory for creating plugin class loaders.
///
/// In Java, this factory creates `DelegatingClassLoader` and `PluginClassLoader` instances
/// using `SecurityManagerCompatibility.doPrivileged()`.
///
/// In Rust, we use a simplified, configuration-driven approach where:
/// - DelegatingClassLoader maintains a registry of plugin loaders
/// - PluginClassLoader represents a plugin's loading context
/// - No actual class loading occurs (Rust uses its own module system)
///
/// Corresponds to Java: org.apache.kafka.connect.runtime.isolation.ClassLoaderFactory
pub struct ClassLoaderFactory;

impl ClassLoaderFactory {
    /// Creates a new ClassLoaderFactory.
    pub fn new() -> Self {
        Self
    }

    /// Creates a new DelegatingClassLoader.
    ///
    /// In Java, this creates a classloader with a parent classloader.
    /// In Rust, we create a DelegatingClassLoader with an optional parent identifier.
    ///
    /// # Arguments
    /// * `parent_id` - Optional identifier for the parent loader (defaults to "system")
    pub fn new_delegating_class_loader(&self, parent_id: Option<&str>) -> DelegatingClassLoader {
        DelegatingClassLoader::new(parent_id)
    }

    /// Creates a new PluginClassLoader for a plugin at the given location.
    ///
    /// In Java, this creates a URLClassLoader with the plugin's URLs.
    /// In Rust, we create a PluginClassLoader with the plugin's location and URL list.
    ///
    /// # Arguments
    /// * `plugin_location` - The location of the plugin (e.g., path to JAR)
    /// * `urls` - List of URLs/resources associated with this plugin
    /// * `parent_id` - Identifier for the parent loader
    pub fn new_plugin_class_loader(
        &self,
        plugin_location: &str,
        urls: Vec<String>,
        parent_id: &str,
    ) -> PluginClassLoader {
        PluginClassLoader::new(plugin_location, urls, parent_id)
    }
}

impl Default for ClassLoaderFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginClassLoaderFactory for ClassLoaderFactory {
    fn new_plugin_class_loader(
        &self,
        plugin_location: &str,
        urls: Vec<String>,
        parent_id: &str,
    ) -> PluginClassLoader {
        self.new_plugin_class_loader(plugin_location, urls, parent_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_factory() {
        let factory = ClassLoaderFactory::new();
        let delegating = factory.new_delegating_class_loader(None);
        assert_eq!(delegating.parent_id(), "system");
    }

    #[test]
    fn test_new_delegating_with_parent() {
        let factory = ClassLoaderFactory::new();
        let delegating = factory.new_delegating_class_loader(Some("custom-parent"));
        assert_eq!(delegating.parent_id(), "custom-parent");
    }

    #[test]
    fn test_new_plugin_class_loader() {
        let factory = ClassLoaderFactory::new();
        let plugin_loader = factory.new_plugin_class_loader(
            "/path/to/plugin.jar",
            vec!["file:///path/to/plugin.jar".to_string()],
            "parent",
        );
        assert_eq!(plugin_loader.location(), "/path/to/plugin.jar");
        assert_eq!(plugin_loader.parent_id(), "parent");
    }

    #[test]
    fn test_default() {
        let factory = ClassLoaderFactory::default();
        let _delegating = factory.new_delegating_class_loader(None);
    }

    #[test]
    fn test_plugin_class_loader_factory_impl() {
        let factory = ClassLoaderFactory::new();
        let plugin_loader = PluginClassLoaderFactory::new_plugin_class_loader(
            &factory,
            "/plugin.jar",
            vec!["url".to_string()],
            "parent",
        );
        assert_eq!(plugin_loader.location(), "/plugin.jar");
    }
}
