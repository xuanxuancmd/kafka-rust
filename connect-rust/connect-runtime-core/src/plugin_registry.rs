//! Plugin Registry
//!
//! Provides compile-time plugin registration mechanism using global registry.

use connect_api::{SinkConnector, SourceConnector};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Plugin type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PluginType {
    /// Source connector
    Source,
    /// Sink connector
    Sink,
    /// Transformation
    Transformation,
}

impl PluginType {
    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            PluginType::Source => "source",
            PluginType::Sink => "sink",
            PluginType::Transformation => "transformation",
        }
    }
}

/// Plugin descriptor
#[derive(Clone)]
pub struct PluginDescriptor {
    /// Plugin name
    pub name: String,
    /// Plugin type
    pub plugin_type: PluginType,
    /// Plugin version
    pub version: String,
    /// Class name (for compatibility with Java)
    pub class_name: String,
}

/// Plugin factory trait
pub trait PluginFactory: Send + Sync {
    /// Create a source connector instance
    fn create_source_connector(&self) -> Result<Box<dyn SourceConnector>, String>;

    /// Create a sink connector instance
    fn create_sink_connector(&self) -> Result<Box<dyn SinkConnector>, String>;

    /// Get plugin descriptor
    fn descriptor(&self) -> PluginDescriptor;
}

/// Global plugin registry
pub struct PluginRegistry {
    plugins: RwLock<HashMap<String, Arc<dyn PluginFactory>>>,
    by_type: RwLock<HashMap<PluginType, Vec<String>>>,
}

impl PluginRegistry {
    /// Create a new plugin registry
    pub fn new() -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
            by_type: RwLock::new(HashMap::new()),
        }
    }

    /// Register a plugin
    pub fn register(&self, factory: Arc<dyn PluginFactory>) {
        let descriptor = factory.descriptor();
        let name = descriptor.name.clone();
        let plugin_type = descriptor.plugin_type;

        // Store plugin by name
        {
            let mut plugins = self.plugins.write().unwrap();
            plugins.insert(name.clone(), factory);
        }

        // Store name by type
        {
            let mut by_type = self.by_type.write().unwrap();
            by_type
                .entry(plugin_type)
                .or_insert_with(Vec::new)
                .push(name);
        }
    }

    /// Get plugin by name
    pub fn get_plugin(&self, name: &str) -> Option<Arc<dyn PluginFactory>> {
        let plugins = self.plugins.read().unwrap();
        plugins.get(name).cloned()
    }

    /// Get all plugins
    pub fn all_plugins(&self) -> Vec<PluginDescriptor> {
        let plugins = self.plugins.read().unwrap();
        plugins
            .values()
            .map(|factory| factory.descriptor())
            .collect()
    }

    /// Get plugins by type
    pub fn plugins_by_type(&self, plugin_type: PluginType) -> Vec<PluginDescriptor> {
        let by_type = self.by_type.read().unwrap();
        if let Some(names) = by_type.get(&plugin_type) {
            let plugins = self.plugins.read().unwrap();
            names
                .iter()
                .filter_map(|name| plugins.get(name).map(|f| f.descriptor()))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Check if plugin exists
    pub fn contains(&self, name: &str) -> bool {
        let plugins = self.plugins.read().unwrap();
        plugins.contains_key(name)
    }

    /// Get plugin count
    pub fn count(&self) -> usize {
        let plugins = self.plugins.read().unwrap();
        plugins.len()
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global plugin registry instance
lazy_static::lazy_static! {
    static ref GLOBAL_REGISTRY: PluginRegistry = PluginRegistry::new();
}

/// Get the global plugin registry
pub fn global_registry() -> &'static PluginRegistry {
    &GLOBAL_REGISTRY
}

/// Register a plugin to the global registry
pub fn register_plugin(factory: Arc<dyn PluginFactory>) {
    GLOBAL_REGISTRY.register(factory);
}

/// Get a plugin from the global registry
pub fn get_plugin(name: &str) -> Option<Arc<dyn PluginFactory>> {
    GLOBAL_REGISTRY.get_plugin(name)
}

/// Get all plugins from the global registry
pub fn all_plugins() -> Vec<PluginDescriptor> {
    GLOBAL_REGISTRY.all_plugins()
}

/// Get plugins by type from the global registry
pub fn plugins_by_type(plugin_type: PluginType) -> Vec<PluginDescriptor> {
    GLOBAL_REGISTRY.plugins_by_type(plugin_type)
}

/// Check if a plugin exists in the global registry
pub fn contains_plugin(name: &str) -> bool {
    GLOBAL_REGISTRY.contains(name)
}

/// Get the count of plugins in the global registry
pub fn plugin_count() -> usize {
    GLOBAL_REGISTRY.count()
}
