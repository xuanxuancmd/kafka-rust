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

//! Plugins module for Kafka Connect runtime.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.isolation.Plugins` in Java.
//! Manages plugin discovery, loading, and instantiation.
//!
//! ## Key Differences from Java (Degraded Approach)
//!
//! In Java, Plugins uses ClassLoader isolation for plugin loading. In Rust, we use:
//! - Configuration-driven plugin discovery (no ClassLoader)
//! - Trait objects and factory patterns for plugin instantiation
//! - Compile-time plugin registration (future macro support)
//!
//! This approach provides similar functionality while respecting Rust's type system.

use crate::config::{PLUGIN_DISCOVERY_CONFIG, PLUGIN_PATH_CONFIG};
use crate::isolation::plugin_desc::PluginDesc;
use crate::isolation::plugin_discovery_mode::PluginDiscoveryMode;
use crate::isolation::plugin_scan_result::PluginScanResult;
use crate::isolation::plugin_type::PluginType;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use thiserror::Error;

/// Error type for plugin operations.
#[derive(Debug, Error)]
pub enum PluginError {
    /// Plugin class not found.
    #[error("Plugin class not found: {0}")]
    ClassNotFoundException(String),

    /// Failed to instantiate plugin.
    #[error("Failed to instantiate plugin: {0}")]
    InstantiationError(String),

    /// Invalid plugin configuration.
    #[error("Invalid plugin configuration: {0}")]
    ConfigurationError(String),

    /// Multiple plugins match the alias.
    #[error("Multiple plugins match alias '{alias}'. Classes found: {matches}")]
    AmbiguousAliasError { alias: String, matches: String },

    /// Version not found for plugin.
    #[error("Version '{version}' not found for plugin '{class}'")]
    VersionNotFoundError { class: String, version: String },
}

/// ClassLoader usage mode.
///
/// In Java, this determines whether to use the current thread's ClassLoader
/// or the plugin ClassLoader. In Rust (degraded), this affects how we
/// resolve plugin classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassLoaderUsage {
    /// Use the current classloader (class already loaded).
    CurrentClassLoader,
    /// Use the plugins classloader (need to discover/load).
    Plugins,
}

/// Plugin factory trait for creating plugin instances.
///
/// In Rust, we use this trait to replace Java's `Utils.newInstance()`.
/// Each plugin type needs a factory implementation.
pub trait PluginFactory<T>: Send + Sync {
    /// Creates a new instance of the plugin.
    fn create(&self) -> Result<T, PluginError>;

    /// Returns the class name this factory creates.
    fn class_name(&self) -> &str;
}

/// Plugin manager for a specific plugin type.
///
/// This corresponds to the per-plugin-type management in Java's Plugins class.
/// Provides access to plugin descriptors and factory for creating instances.
pub struct PluginManager<T> {
    /// The plugin type this manager handles.
    plugin_type: PluginType,
    /// Available plugin descriptors.
    plugins: BTreeSet<PluginDesc>,
    /// Factory registry for creating instances (keyed by class name).
    factories: HashMap<String, Box<dyn PluginFactory<T>>>,
}

impl<T> PluginManager<T> {
    /// Creates a new PluginManager.
    pub fn new(plugin_type: PluginType) -> Self {
        PluginManager {
            plugin_type,
            plugins: BTreeSet::new(),
            factories: HashMap::new(),
        }
    }

    /// Creates a PluginManager with existing plugins.
    pub fn with_plugins(plugin_type: PluginType, plugins: BTreeSet<PluginDesc>) -> Self {
        PluginManager {
            plugin_type,
            plugins,
            factories: HashMap::new(),
        }
    }

    /// Returns the plugin type.
    pub fn plugin_type(&self) -> PluginType {
        self.plugin_type
    }

    /// Returns all available plugins.
    pub fn plugins(&self) -> &BTreeSet<PluginDesc> {
        &self.plugins
    }

    /// Registers a factory for a plugin class.
    pub fn register_factory(&mut self, class_name: String, factory: Box<dyn PluginFactory<T>>) {
        self.factories.insert(class_name, factory);
    }

    /// Finds a plugin by class name or alias.
    pub fn find_plugin(&self, class_or_alias: &str) -> Option<&PluginDesc> {
        // First, try exact match by class name
        for plugin in &self.plugins {
            if plugin.plugin_class() == class_or_alias {
                return Some(plugin);
            }
        }

        // Second, try simple name match (alias resolution)
        for plugin in &self.plugins {
            let simple_name = plugin.simple_name();
            if simple_name == class_or_alias {
                return Some(plugin);
            }
            // Also check with "Connector" suffix for connectors
            if self.plugin_type == PluginType::Source || self.plugin_type == PluginType::Sink {
                if simple_name == format!("{}Connector", class_or_alias) {
                    return Some(plugin);
                }
            }
        }

        None
    }

    /// Finds plugins matching a class name or alias.
    /// Returns all matching plugins for version selection.
    pub fn find_plugins_by_alias(&self, class_or_alias: &str) -> Vec<&PluginDesc> {
        let mut matches = Vec::new();

        for plugin in &self.plugins {
            // Exact class name match
            if plugin.plugin_class() == class_or_alias {
                matches.push(plugin);
                continue;
            }

            // Simple name match
            let simple_name = plugin.simple_name();
            if simple_name == class_or_alias {
                matches.push(plugin);
                continue;
            }

            // With "Connector" suffix for connectors
            if self.plugin_type == PluginType::Source || self.plugin_type == PluginType::Sink {
                if simple_name == format!("{}Connector", class_or_alias) {
                    matches.push(plugin);
                }
            }
        }

        matches
    }

    /// Creates an instance of the plugin.
    pub fn create_instance(&self, class_name: &str) -> Result<T, PluginError> {
        self.factories
            .get(class_name)
            .ok_or_else(|| PluginError::ClassNotFoundException(class_name.to_string()))
            .and_then(|factory| factory.create())
    }
}

/// Plugins struct - main plugin management class.
///
/// This corresponds to `org.apache.kafka.connect.runtime.isolation.Plugins` in Java.
/// Manages discovery, loading, and instantiation of all plugin types.
///
/// ## Degraded Approach (No ClassLoader)
///
/// Unlike Java which uses `DelegatingClassLoader` and `LoaderSwap`, this Rust implementation:
/// - Uses configuration-driven discovery
/// - Stores plugin descriptors directly
/// - Uses trait objects for plugin instances
/// - Provides factory-based instantiation
pub struct Plugins {
    /// The scan result containing all discovered plugins.
    scan_result: PluginScanResult,
    /// Plugin path for discovery.
    plugin_path: Vec<PathBuf>,
    /// Discovery mode used.
    discovery_mode: PluginDiscoveryMode,
    /// Original configuration.
    config: HashMap<String, String>,
}

impl Plugins {
    /// Creates a new Plugins instance from configuration.
    ///
    /// This corresponds to Java's `Plugins(Map<String, String> props)` constructor.
    ///
    /// # Arguments
    /// * `props` - Worker configuration properties
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use connect_runtime::isolation::Plugins;
    ///
    /// let mut props = HashMap::new();
    /// props.insert("plugin.path", "/path/to/plugins");
    /// let plugins = Plugins::new(props);
    /// ```
    pub fn new(props: HashMap<String, String>) -> Self {
        let plugin_path = Self::parse_plugin_path(&props);
        let discovery_mode = Self::parse_discovery_mode(&props);

        // Perform plugin scanning
        let scan_result = Self::scan_plugins(&plugin_path, discovery_mode);

        Plugins {
            scan_result,
            plugin_path,
            discovery_mode,
            config: props,
        }
    }

    /// Creates a Plugins with a pre-existing scan result.
    /// Used for testing or when plugins are pre-discovered.
    pub fn with_scan_result(props: HashMap<String, String>, scan_result: PluginScanResult) -> Self {
        let plugin_path = Self::parse_plugin_path(&props);
        let discovery_mode = Self::parse_discovery_mode(&props);

        Plugins {
            scan_result,
            plugin_path,
            discovery_mode,
            config: props,
        }
    }

    /// Parse plugin path from configuration.
    fn parse_plugin_path(props: &HashMap<String, String>) -> Vec<PathBuf> {
        props
            .get(PLUGIN_PATH_CONFIG)
            .map(|s| {
                s.split(',')
                    .map(|p| PathBuf::from(p.trim()))
                    .filter(|p| !p.as_os_str().is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Parse discovery mode from configuration.
    fn parse_discovery_mode(props: &HashMap<String, String>) -> PluginDiscoveryMode {
        props
            .get(PLUGIN_DISCOVERY_CONFIG)
            .and_then(|s| PluginDiscoveryMode::from_config_str(s))
            .unwrap_or_default()
    }

    /// Scan for plugins in the given paths.
    ///
    /// This corresponds to Java's `initLoaders()` method.
    /// In the degraded approach, we use configuration-based scanning.
    fn scan_plugins(
        plugin_paths: &[PathBuf],
        discovery_mode: PluginDiscoveryMode,
    ) -> PluginScanResult {
        // For now, return an empty result.
        // Full implementation would integrate with PluginScanner.
        // This placeholder allows compilation while PluginScanner is being implemented.
        PluginScanResult::new()
    }

    /// Returns the scan result containing all discovered plugins.
    pub fn scan_result(&self) -> &PluginScanResult {
        &self.scan_result
    }

    /// Returns the plugin discovery mode.
    pub fn discovery_mode(&self) -> PluginDiscoveryMode {
        self.discovery_mode
    }

    /// Returns the plugin path.
    pub fn plugin_path(&self) -> &[PathBuf] {
        &self.plugin_path
    }

    /// Returns the DelegatingClassLoader.
    ///
    /// **Degraded**: In Java, this returns the DelegatingClassLoader used for plugin isolation.
    /// In Rust, this returns `None` as we don't use ClassLoader-based isolation.
    ///
    /// Corresponds to Java's `delegatingLoader()` method.
    pub fn delegating_class_loader(&self) -> Option<()> {
        None
    }

    /// Returns all connectors (sink + source).
    ///
    /// Corresponds to Java's `connectors()` method.
    pub fn connectors(&self) -> BTreeSet<PluginDesc> {
        let mut connectors = BTreeSet::new();
        connectors.extend(self.scan_result.sink_connectors().iter().cloned());
        connectors.extend(self.scan_result.source_connectors().iter().cloned());
        connectors
    }

    /// Returns sink connector plugins.
    ///
    /// Corresponds to Java's `sinkConnectors()` method.
    pub fn sink_connectors(&self) -> &BTreeSet<PluginDesc> {
        self.scan_result.sink_connectors()
    }

    /// Returns source connector plugins.
    ///
    /// Corresponds to Java's `sourceConnectors()` method.
    pub fn source_connectors(&self) -> &BTreeSet<PluginDesc> {
        self.scan_result.source_connectors()
    }

    /// Returns converter plugins.
    ///
    /// Corresponds to Java's `converters()` method.
    pub fn converters(&self) -> &BTreeSet<PluginDesc> {
        self.scan_result.converters()
    }

    /// Returns header converter plugins.
    ///
    /// Corresponds to Java's `headerConverters()` method.
    pub fn header_converters(&self) -> &BTreeSet<PluginDesc> {
        self.scan_result.header_converters()
    }

    /// Returns transformation plugins.
    ///
    /// Corresponds to Java's `transformations()` method.
    pub fn transformations(&self) -> &BTreeSet<PluginDesc> {
        self.scan_result.transformations()
    }

    /// Returns predicate plugins.
    ///
    /// Corresponds to Java's `predicates()` method.
    pub fn predicates(&self) -> &BTreeSet<PluginDesc> {
        self.scan_result.predicates()
    }

    /// Returns config provider plugins.
    ///
    /// Corresponds to Java's `configProviders()` method.
    pub fn config_providers(&self) -> &BTreeSet<PluginDesc> {
        self.scan_result.config_providers()
    }

    /// Returns connector client config override policy plugins.
    ///
    /// Corresponds to Java's `connectorClientConfigPolicies()` method.
    pub fn connector_client_config_policies(&self) -> &BTreeSet<PluginDesc> {
        self.scan_result.connector_client_config_policies()
    }

    /// Creates a new Connector instance.
    ///
    /// **Degraded**: In Java, this uses ClassLoader isolation and reflection.
    /// In Rust, this uses configuration-based instantiation.
    ///
    /// Corresponds to Java's `newConnector(String connectorClassOrAlias)` method.
    ///
    /// # Arguments
    /// * `connector_class_or_alias` - The class name or alias of the connector
    ///
    /// # Errors
    /// Returns `PluginError` if the connector cannot be found or instantiated.
    ///
    /// # Examples
    /// ```
    /// use connect_runtime::isolation::Plugins;
    /// use std::collections::HashMap;
    ///
    /// let plugins = Plugins::new(HashMap::new());
    /// // Note: This will fail if no connectors are registered
    /// // let connector = plugins.new_connector("FileStreamSource");
    /// ```
    pub fn new_connector(&self, connector_class_or_alias: &str) -> Result<(), PluginError> {
        self.new_connector_with_range(connector_class_or_alias, None)
    }

    /// Creates a new Connector instance with version range.
    ///
    /// Corresponds to Java's `newConnector(String connectorClassOrAlias, VersionRange range)` method.
    pub fn new_connector_with_range(
        &self,
        connector_class_or_alias: &str,
        range: Option<&str>,
    ) -> Result<(), PluginError> {
        // Find the connector class
        let klass = self.connector_class(connector_class_or_alias, range)?;

        // In degraded mode, we cannot instantiate without a factory
        // Return an error indicating the need for factory registration
        Err(PluginError::InstantiationError(format!(
            "Connector '{}' found but no factory registered. \
             In degraded mode, plugins must be registered via PluginFactory.",
            klass
        )))
    }

    /// Finds the connector class from alias or class name.
    ///
    /// Corresponds to Java's `connectorClass(String connectorClassOrAlias, VersionRange range)` method.
    pub fn connector_class(
        &self,
        connector_class_or_alias: &str,
        range: Option<&str>,
    ) -> Result<String, PluginError> {
        let connectors = self.connectors();
        let matches = self.find_plugins_by_alias(connector_class_or_alias, &connectors);

        if matches.is_empty() {
            return Err(PluginError::ClassNotFoundException(format!(
                "Failed to find any class that implements Connector and which name matches '{}', \
                 available connectors are: {}",
                connector_class_or_alias,
                self.plugin_names_from_set(&connectors)
            )));
        }

        // Apply version range filtering if specified
        if let Some(version_range) = range {
            return self.select_versioned_plugin(matches, version_range);
        }

        // If only one match, return it
        if matches.len() == 1 {
            return Ok(matches[0].plugin_class().to_string());
        }

        // Multiple matches without version - need disambiguation
        Err(PluginError::AmbiguousAliasError {
            alias: connector_class_or_alias.to_string(),
            matches: self.plugin_names(&matches),
        })
    }

    /// Creates a new Task instance.
    ///
    /// **Degraded**: In Java, this creates a Task instance using the plugin's classloader.
    /// In Rust, this uses configuration-based instantiation.
    ///
    /// Corresponds to Java's `newTask(Class<? extends Task> taskClass)` method.
    ///
    /// # Arguments
    /// * `task_class` - The class name of the task
    ///
    /// # Errors
    /// Returns `PluginError` if the task cannot be instantiated.
    pub fn new_task(&self, task_class: &str) -> Result<(), PluginError> {
        // In degraded mode, we cannot instantiate without a factory
        Err(PluginError::InstantiationError(format!(
            "Task '{}' cannot be instantiated. \
             In degraded mode, plugins must be registered via PluginFactory.",
            task_class
        )))
    }

    /// Creates a new Converter instance.
    ///
    /// **Degraded**: In Java, this uses ClassLoader isolation and reflection.
    /// In Rust, this uses configuration-based instantiation.
    ///
    /// Corresponds to Java's `newConverter(AbstractConfig config, String classPropertyName, ClassLoaderUsage classLoaderUsage)` method.
    ///
    /// # Arguments
    /// * `class_or_alias` - The class name or alias of the converter
    /// * `is_key` - Whether this is for key conversion
    /// * `converter_config` - Configuration for the converter
    /// * `class_loader_usage` - Which classloader to use (degraded: affects resolution)
    ///
    /// # Errors
    /// Returns `PluginError` if the converter cannot be found or instantiated.
    pub fn new_converter(
        &self,
        class_or_alias: &str,
        is_key: bool,
        converter_config: HashMap<String, String>,
        class_loader_usage: ClassLoaderUsage,
    ) -> Result<(), PluginError> {
        let converters = self.converters();

        // Resolve the converter class
        let klass = match class_loader_usage {
            ClassLoaderUsage::CurrentClassLoader => {
                // Assume class_or_alias is the exact class name
                class_or_alias.to_string()
            }
            ClassLoaderUsage::Plugins => {
                // Find the converter by alias or class name
                self.plugin_class(class_or_alias, None, PluginType::Converter)?
            }
        };

        // Verify the class is in our known converters
        if !converters.iter().any(|c| c.plugin_class() == klass) {
            return Err(PluginError::ClassNotFoundException(format!(
                "Converter '{}' not found in available converters: {}",
                klass,
                self.plugin_names_from_set(converters)
            )));
        }

        // In degraded mode, we cannot instantiate without a factory
        Err(PluginError::InstantiationError(format!(
            "Converter '{}' found but no factory registered. \
             In degraded mode, plugins must be registered via PluginFactory.",
            klass
        )))
    }

    /// Creates a new Converter with version range.
    ///
    /// Corresponds to Java's `newConverter(AbstractConfig config, String classPropertyName, String versionPropertyName)` method.
    pub fn new_converter_with_version(
        &self,
        class_or_alias: &str,
        version: Option<&str>,
        is_key: bool,
        converter_config: HashMap<String, String>,
    ) -> Result<(), PluginError> {
        let class_loader_usage = if version.is_some() {
            ClassLoaderUsage::Plugins
        } else {
            ClassLoaderUsage::CurrentClassLoader
        };

        self.new_converter(class_or_alias, is_key, converter_config, class_loader_usage)
    }

    /// Creates a new HeaderConverter instance.
    ///
    /// Corresponds to Java's `newHeaderConverter(AbstractConfig config, String classPropertyName, ClassLoaderUsage classLoaderUsage)` method.
    pub fn new_header_converter(
        &self,
        class_or_alias: &str,
        class_loader_usage: ClassLoaderUsage,
    ) -> Result<(), PluginError> {
        let header_converters = self.header_converters();

        let klass = match class_loader_usage {
            ClassLoaderUsage::CurrentClassLoader => class_or_alias.to_string(),
            ClassLoaderUsage::Plugins => {
                self.plugin_class(class_or_alias, None, PluginType::HeaderConverter)?
            }
        };

        if !header_converters.iter().any(|c| c.plugin_class() == klass) {
            return Err(PluginError::ClassNotFoundException(format!(
                "HeaderConverter '{}' not found",
                klass
            )));
        }

        Err(PluginError::InstantiationError(format!(
            "HeaderConverter '{}' found but no factory registered",
            klass
        )))
    }

    /// Creates a new ConfigProvider instance.
    ///
    /// Corresponds to Java's `newConfigProvider(AbstractConfig config, String providerPrefix, ClassLoaderUsage classLoaderUsage)` method.
    pub fn new_config_provider(
        &self,
        class_or_alias: &str,
        class_loader_usage: ClassLoaderUsage,
    ) -> Result<(), PluginError> {
        let config_providers = self.config_providers();

        let klass = match class_loader_usage {
            ClassLoaderUsage::CurrentClassLoader => class_or_alias.to_string(),
            ClassLoaderUsage::Plugins => {
                self.plugin_class(class_or_alias, None, PluginType::ConfigProvider)?
            }
        };

        if !config_providers.iter().any(|c| c.plugin_class() == klass) {
            return Err(PluginError::ClassNotFoundException(format!(
                "ConfigProvider '{}' not found",
                klass
            )));
        }

        Err(PluginError::InstantiationError(format!(
            "ConfigProvider '{}' found but no factory registered",
            klass
        )))
    }

    /// Returns the plugin class for a given alias or class name.
    ///
    /// Corresponds to Java's `pluginClass(DelegatingClassLoader loader, String classOrAlias, Class<U> pluginClass, VersionRange range)` method.
    pub fn plugin_class(
        &self,
        class_or_alias: &str,
        range: Option<&str>,
        plugin_type: PluginType,
    ) -> Result<String, PluginError> {
        let plugins = self.scan_result.plugins_by_type(plugin_type);
        let matches = self.find_plugins_by_alias(class_or_alias, plugins);

        if matches.is_empty() {
            return Err(PluginError::ClassNotFoundException(format!(
                "Failed to find any class that implements {} and which name matches '{}', \
                 available plugins are: {}",
                plugin_type.simple_name(),
                class_or_alias,
                self.plugin_names_from_set(plugins)
            )));
        }

        if let Some(version_range) = range {
            return self.select_versioned_plugin(matches, version_range);
        }

        if matches.len() == 1 {
            return Ok(matches[0].plugin_class().to_string());
        }

        Err(PluginError::AmbiguousAliasError {
            alias: class_or_alias.to_string(),
            matches: self.plugin_names(&matches),
        })
    }

    /// Returns the latest version for a plugin.
    ///
    /// Corresponds to Java's `latestVersion(String classOrAlias, PluginType... allowedTypes)` method.
    pub fn latest_version(
        &self,
        class_or_alias: &str,
        allowed_types: &[PluginType],
    ) -> Option<String> {
        self.plugin_version(class_or_alias, None, allowed_types)
    }

    /// Returns the version for a plugin.
    ///
    /// Corresponds to Java's `pluginVersion(String classOrAlias, ClassLoader sourceLoader, PluginType... allowedTypes)` method.
    pub fn plugin_version(
        &self,
        class_or_alias: &str,
        _source_loader: Option<&str>,
        allowed_types: &[PluginType],
    ) -> Option<String> {
        // Search across allowed types
        for plugin_type in allowed_types {
            let plugins = self.scan_result.plugins_by_type(*plugin_type);
            for plugin in plugins {
                if plugin.plugin_class() == class_or_alias {
                    return Some(plugin.version().to_string());
                }
            }
        }

        // Also search by alias
        for plugin_type in allowed_types {
            let plugins = self.scan_result.plugins_by_type(*plugin_type);
            for plugin in plugins {
                if plugin.simple_name() == class_or_alias {
                    return Some(plugin.version().to_string());
                }
            }
        }

        None
    }

    /// Returns connector plugin managers.
    ///
    /// **Degraded**: In Java, this returns a collection of PluginManager instances.
    /// In Rust, we return an empty collection as PluginManager needs factory registration.
    ///
    /// Corresponds to Java's `connectorPluginManagers()` usage pattern.
    pub fn connector_plugin_managers(&self) -> Vec<PluginManager<()>> {
        // Return managers for sink and source connectors
        let sink_manager = PluginManager::with_plugins(
            PluginType::Sink,
            self.scan_result.sink_connectors().clone(),
        );
        let source_manager = PluginManager::with_plugins(
            PluginType::Source,
            self.scan_result.source_connectors().clone(),
        );

        vec![sink_manager, source_manager]
    }

    /// Creates a plugin manager for a specific type.
    pub fn plugin_manager(&self, plugin_type: PluginType) -> PluginManager<()> {
        PluginManager::with_plugins(
            plugin_type,
            self.scan_result.plugins_by_type(plugin_type).clone(),
        )
    }

    // === Helper methods ===

    /// Finds plugins matching a class name or alias.
    fn find_plugins_by_alias<'a>(
        &self,
        class_or_alias: &str,
        plugins: &'a BTreeSet<PluginDesc>,
    ) -> Vec<&'a PluginDesc> {
        let mut matches = Vec::new();

        for plugin in plugins {
            // Exact class name match
            if plugin.plugin_class() == class_or_alias {
                matches.push(plugin);
                continue;
            }

            // Simple name match
            let simple_name = plugin.simple_name();
            if simple_name == class_or_alias {
                matches.push(plugin);
                continue;
            }

            // With type-specific suffix
            let suffix = match plugin.type_() {
                PluginType::Source | PluginType::Sink => "Connector",
                PluginType::Converter => "Converter",
                PluginType::HeaderConverter => "HeaderConverter",
                PluginType::Transformation => "Transformation",
                PluginType::Predicate => "Predicate",
                PluginType::ConfigProvider => "ConfigProvider",
                PluginType::RestExtension => "ConnectRestExtension",
                PluginType::ConnectorClientConfigOverridePolicy => "Policy",
            };

            if simple_name == format!("{}{}", class_or_alias, suffix) {
                matches.push(plugin);
            }
        }

        matches
    }

    /// Selects a versioned plugin from matches.
    fn select_versioned_plugin(
        &self,
        matches: Vec<&PluginDesc>,
        version_range: &str,
    ) -> Result<String, PluginError> {
        // Parse the version range (simplified version matching)
        // Full implementation would use Maven VersionRange parsing
        let target_version = version_range.trim();

        for plugin in &matches {
            if plugin.version() == target_version {
                return Ok(plugin.plugin_class().to_string());
            }
        }

        // Try range-based matching (e.g., "[1.0,2.0)")
        if target_version.starts_with('[') || target_version.contains(',') {
            // Simplified: find the latest version that matches
            // For now, just return the highest version
            if let Some(plugin) = matches.iter().max_by_key(|p| p.version()) {
                return Ok(plugin.plugin_class().to_string());
            }
        }

        Err(PluginError::VersionNotFoundError {
            class: matches
                .first()
                .map(|p| p.simple_name())
                .unwrap_or("unknown")
                .to_string(),
            version: version_range.to_string(),
        })
    }

    /// Returns formatted plugin names for error messages.
    /// Accepts a slice of references to PluginDesc.
    fn plugin_names(&self, plugins: &[&PluginDesc]) -> String {
        plugins
            .iter()
            .map(|p| format!("{}", p))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Returns formatted plugin names from a BTreeSet for error messages.
    fn plugin_names_from_set(&self, plugins: &BTreeSet<PluginDesc>) -> String {
        plugins
            .iter()
            .map(|p| format!("{}", p))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Convert BTreeSet to a Vec of references for use with plugin_names.
    fn set_to_refs<'a>(&self, plugins: &'a BTreeSet<PluginDesc>) -> Vec<&'a PluginDesc> {
        plugins.iter().collect()
    }

    /// Resolves a full class name from an alias.
    ///
    /// Corresponds to Java's `DelegatingClassLoader.resolveFullClassName()` method.
    pub fn resolve_full_class_name(&self, class_or_alias: &str) -> String {
        // If it looks like a full class name (contains dots), return it
        if class_or_alias.contains('.') {
            return class_or_alias.to_string();
        }

        // Try to resolve as an alias across all plugin types
        let all_types = [
            PluginType::Source,
            PluginType::Sink,
            PluginType::Converter,
            PluginType::HeaderConverter,
            PluginType::Transformation,
            PluginType::Predicate,
            PluginType::ConfigProvider,
            PluginType::RestExtension,
            PluginType::ConnectorClientConfigOverridePolicy,
        ];
        for plugin_type in all_types {
            let plugins = self.scan_result.plugins_by_type(plugin_type);
            for plugin in plugins {
                if plugin.simple_name() == class_or_alias {
                    return plugin.plugin_class().to_string();
                }
            }
        }

        // Cannot resolve, return original
        class_or_alias.to_string()
    }
}

impl Default for Plugins {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_plugin_desc(class_name: &str, plugin_type: PluginType) -> PluginDesc {
        PluginDesc::new(
            class_name.to_string(),
            "1.0.0".to_string(),
            plugin_type,
            "classpath".to_string(),
        )
    }

    #[test]
    fn test_new_plugins_empty() {
        let plugins = Plugins::new(HashMap::new());
        assert!(plugins.scan_result().is_empty());
        assert_eq!(plugins.plugin_path().len(), 0);
    }

    #[test]
    fn test_parse_plugin_path() {
        let mut props = HashMap::new();
        props.insert(
            PLUGIN_PATH_CONFIG.to_string(),
            "/path1,/path2,/path3".to_string(),
        );

        let plugins = Plugins::new(props);
        assert_eq!(plugins.plugin_path().len(), 3);
    }

    #[test]
    fn test_parse_discovery_mode() {
        let mut props = HashMap::new();
        props.insert(
            PLUGIN_DISCOVERY_CONFIG.to_string(),
            "hybrid_warn".to_string(),
        );

        let plugins = Plugins::new(props);
        assert_eq!(plugins.discovery_mode(), PluginDiscoveryMode::HybridWarn);
    }

    #[test]
    fn test_delegating_class_loader_returns_none() {
        let plugins = Plugins::new(HashMap::new());
        assert!(plugins.delegating_class_loader().is_none());
    }

    #[test]
    fn test_connectors_empty() {
        let plugins = Plugins::new(HashMap::new());
        assert!(plugins.connectors().is_empty());
    }

    #[test]
    fn test_connectors_with_plugins() {
        let mut scan_result = PluginScanResult::new();
        scan_result.add_plugin(create_test_plugin_desc(
            "org.example.SourceConnector",
            PluginType::Source,
        ));
        scan_result.add_plugin(create_test_plugin_desc(
            "org.example.SinkConnector",
            PluginType::Sink,
        ));

        let plugins = Plugins::with_scan_result(HashMap::new(), scan_result);
        assert_eq!(plugins.connectors().len(), 2);
        assert_eq!(plugins.sink_connectors().len(), 1);
        assert_eq!(plugins.source_connectors().len(), 1);
    }

    #[test]
    fn test_find_plugins_by_alias_exact_match() {
        let mut scan_result = PluginScanResult::new();
        scan_result.add_plugin(create_test_plugin_desc(
            "org.apache.kafka.connect.file.FileSourceConnector",
            PluginType::Source,
        ));

        let plugins = Plugins::with_scan_result(HashMap::new(), scan_result);

        let found = plugins.find_plugins_by_alias(
            "org.apache.kafka.connect.file.FileSourceConnector",
            plugins.source_connectors(),
        );
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].plugin_class(),
            "org.apache.kafka.connect.file.FileSourceConnector"
        );
    }

    #[test]
    fn test_find_plugins_by_alias_simple_name() {
        let mut scan_result = PluginScanResult::new();
        scan_result.add_plugin(create_test_plugin_desc(
            "org.apache.kafka.connect.file.FileSourceConnector",
            PluginType::Source,
        ));

        let plugins = Plugins::with_scan_result(HashMap::new(), scan_result);

        let found =
            plugins.find_plugins_by_alias("FileSourceConnector", plugins.source_connectors());
        assert_eq!(found.len(), 1);
    }

    #[test]
    fn test_find_plugins_by_alias_with_connector_suffix() {
        let mut scan_result = PluginScanResult::new();
        scan_result.add_plugin(create_test_plugin_desc(
            "org.apache.kafka.connect.file.FileSourceConnector",
            PluginType::Source,
        ));

        let plugins = Plugins::with_scan_result(HashMap::new(), scan_result);

        let found = plugins.find_plugins_by_alias("FileSource", plugins.source_connectors());
        assert_eq!(found.len(), 1);
    }

    #[test]
    fn test_connector_class_not_found() {
        let plugins = Plugins::new(HashMap::new());

        let result = plugins.connector_class("NonExistentConnector", None);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PluginError::ClassNotFoundException(_)
        ));
    }

    #[test]
    fn test_connector_class_found() {
        let mut scan_result = PluginScanResult::new();
        scan_result.add_plugin(create_test_plugin_desc(
            "org.example.TestConnector",
            PluginType::Source,
        ));

        let plugins = Plugins::with_scan_result(HashMap::new(), scan_result);

        let result = plugins.connector_class("org.example.TestConnector", None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "org.example.TestConnector");
    }

    #[test]
    fn test_connector_class_ambiguous() {
        let mut scan_result = PluginScanResult::new();
        scan_result.add_plugin(create_test_plugin_desc(
            "org.example.TestConnector",
            PluginType::Source,
        ));
        scan_result.add_plugin(create_test_plugin_desc(
            "com.other.TestConnector",
            PluginType::Source,
        ));

        let plugins = Plugins::with_scan_result(HashMap::new(), scan_result);

        let result = plugins.connector_class("TestConnector", None);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PluginError::AmbiguousAliasError { .. }
        ));
    }

    #[test]
    fn test_plugin_class_converter() {
        let mut scan_result = PluginScanResult::new();
        scan_result.add_plugin(create_test_plugin_desc(
            "org.apache.kafka.connect.storage.StringConverter",
            PluginType::Converter,
        ));

        let plugins = Plugins::with_scan_result(HashMap::new(), scan_result);

        let result = plugins.plugin_class("StringConverter", None, PluginType::Converter);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "org.apache.kafka.connect.storage.StringConverter"
        );
    }

    #[test]
    fn test_latest_version() {
        let mut scan_result = PluginScanResult::new();
        scan_result.add_plugin(PluginDesc::new(
            "org.example.Connector".to_string(),
            "2.0.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        ));

        let plugins = Plugins::with_scan_result(HashMap::new(), scan_result);

        let version = plugins.latest_version("org.example.Connector", &[PluginType::Source]);
        assert_eq!(version, Some("2.0.0".to_string()));
    }

    #[test]
    fn test_plugin_names() {
        let mut plugins = BTreeSet::new();
        plugins.insert(create_test_plugin_desc(
            "org.example.Connector",
            PluginType::Source,
        ));

        let p = Plugins::new(HashMap::new());
        let names = p.plugin_names_from_set(&plugins);
        assert!(names.contains("org.example.Connector"));
    }

    #[test]
    fn test_resolve_full_class_name() {
        let mut scan_result = PluginScanResult::new();
        scan_result.add_plugin(create_test_plugin_desc(
            "org.apache.kafka.connect.file.FileSourceConnector",
            PluginType::Source,
        ));

        let plugins = Plugins::with_scan_result(HashMap::new(), scan_result);

        // Full class name - returns same
        assert_eq!(
            plugins.resolve_full_class_name("org.example.SomeClass"),
            "org.example.SomeClass"
        );

        // Alias - resolves to full name
        assert_eq!(
            plugins.resolve_full_class_name("FileSourceConnector"),
            "org.apache.kafka.connect.file.FileSourceConnector"
        );
    }

    #[test]
    fn test_connector_plugin_managers() {
        let mut scan_result = PluginScanResult::new();
        scan_result.add_plugin(create_test_plugin_desc(
            "org.example.Sink",
            PluginType::Sink,
        ));
        scan_result.add_plugin(create_test_plugin_desc(
            "org.example.Source",
            PluginType::Source,
        ));

        let plugins = Plugins::with_scan_result(HashMap::new(), scan_result);
        let managers = plugins.connector_plugin_managers();

        assert_eq!(managers.len(), 2);
        assert_eq!(managers[0].plugin_type(), PluginType::Sink);
        assert_eq!(managers[1].plugin_type(), PluginType::Source);
    }

    #[test]
    fn test_plugin_manager() {
        let mut scan_result = PluginScanResult::new();
        scan_result.add_plugin(create_test_plugin_desc(
            "org.example.Converter",
            PluginType::Converter,
        ));

        let plugins = Plugins::with_scan_result(HashMap::new(), scan_result);
        let manager = plugins.plugin_manager(PluginType::Converter);

        assert_eq!(manager.plugin_type(), PluginType::Converter);
        assert_eq!(manager.plugins().len(), 1);
    }

    #[test]
    fn test_class_loader_usage() {
        assert_eq!(
            ClassLoaderUsage::CurrentClassLoader,
            ClassLoaderUsage::CurrentClassLoader
        );
        assert_ne!(
            ClassLoaderUsage::CurrentClassLoader,
            ClassLoaderUsage::Plugins
        );
    }

    #[test]
    fn test_plugin_manager_find_plugin() {
        let mut manager: PluginManager<PluginDesc> = PluginManager::new(PluginType::Source);
        let desc = create_test_plugin_desc("org.example.SourceConnector", PluginType::Source);
        manager.plugins.insert(desc.clone());

        // Exact match
        let found = manager.find_plugin("org.example.SourceConnector");
        assert!(found.is_some());

        // Simple name match
        let found = manager.find_plugin("SourceConnector");
        assert!(found.is_some());

        // Alias match with suffix
        let found = manager.find_plugin("Source");
        assert!(found.is_some());
    }

    #[test]
    fn test_plugin_error_display() {
        let err = PluginError::ClassNotFoundException("TestClass".to_string());
        assert!(err.to_string().contains("TestClass"));

        let err = PluginError::AmbiguousAliasError {
            alias: "Test".to_string(),
            matches: "class1, class2".to_string(),
        };
        assert!(err.to_string().contains("Test"));
        assert!(err.to_string().contains("class1"));
    }

    #[test]
    fn test_default() {
        let plugins = Plugins::default();
        assert!(plugins.scan_result().is_empty());
    }
}
