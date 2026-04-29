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

//! Plugin class loader factory interface for Kafka Connect.
//!
//! This module defines the factory interface for creating plugin class loaders.
//! Corresponds to Java: org.apache.kafka.connect.runtime.isolation.PluginClassLoaderFactory

use crate::isolation::plugin_class_loader::PluginClassLoader;

/// Factory interface for creating plugin class loaders.
///
/// In Java, this interface is used for mocking classloader initialization in tests.
/// In Rust, we use this trait to define the factory contract for creating
/// PluginClassLoader instances.
///
/// Corresponds to Java: org.apache.kafka.connect.runtime.isolation.PluginClassLoaderFactory
pub trait PluginClassLoaderFactory {
    /// Creates a new PluginClassLoader.
    ///
    /// # Arguments
    /// * `plugin_location` - The location of the plugin (e.g., path to JAR or directory)
    /// * `urls` - List of URLs/resources associated with this plugin
    /// * `parent_id` - Identifier for the parent loader
    fn new_plugin_class_loader(
        &self,
        plugin_location: &str,
        urls: Vec<String>,
        parent_id: &str,
    ) -> PluginClassLoader;
}

/// Default implementation of PluginClassLoaderFactory.
///
/// This provides a simple factory that creates PluginClassLoader instances.
pub struct DefaultPluginClassLoaderFactory;

impl DefaultPluginClassLoaderFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultPluginClassLoaderFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginClassLoaderFactory for DefaultPluginClassLoaderFactory {
    fn new_plugin_class_loader(
        &self,
        plugin_location: &str,
        urls: Vec<String>,
        parent_id: &str,
    ) -> PluginClassLoader {
        PluginClassLoader::new(plugin_location, urls, parent_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_factory() {
        let factory = DefaultPluginClassLoaderFactory::new();
        let loader = factory.new_plugin_class_loader(
            "/path/to/plugin.jar",
            vec!["url".to_string()],
            "parent",
        );
        assert_eq!(loader.location(), "/path/to/plugin.jar");
    }

    #[test]
    fn test_trait_impl() {
        let factory: &dyn PluginClassLoaderFactory = &DefaultPluginClassLoaderFactory::new();
        let loader = factory.new_plugin_class_loader("test", vec![], "parent");
        assert_eq!(loader.location(), "test");
    }
}
