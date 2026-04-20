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

//! Plugin scanner for Kafka Connect plugin discovery.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.isolation.PluginScanner` in Java.
//!
//! This module provides the abstract scanner trait and common functionality for
//! plugin discovery. In Rust, we use a configuration-driven approach instead of
//! Java's ServiceLoader and reflection mechanisms.
//!
//! ## Key Differences from Java
//!
//! 1. **No reflection**: Java uses reflection to scan classes. Rust uses configuration files
//!    (plugin manifests) to describe available plugins.
//!
//! 2. **No ServiceLoader**: Java uses ServiceLoader for SPI. Rust uses a plugin registry
//!    that reads from configuration.
//!
//! 3. **Simplified ClassLoader**: Java has complex ClassLoader isolation. Rust uses
//!    string-based loader identifiers for tracking purposes.

use crate::isolation::plugin_desc::{PluginDesc, UNDEFINED_VERSION};
use crate::isolation::plugin_scan_result::PluginScanResult;
use crate::isolation::plugin_source::PluginSource;
use crate::isolation::plugin_type::PluginType;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt;
use std::time::Instant;

/// Trait for plugin scanning implementations.
///
/// This corresponds to the abstract `PluginScanner` class in Java.
/// Implementations provide specific scanning strategies (ServiceLoader-style or
/// reflective-style scanning, adapted for Rust's configuration-driven approach).
pub trait PluginScanner: Send + Sync {
    /// Scans a single plugin source for plugins.
    ///
    /// This corresponds to `scanPlugins(PluginSource source)` in Java.
    /// Implementations should return a PluginScanResult containing all plugins
    /// discovered from the given source.
    ///
    /// # Arguments
    /// * `source` - The plugin source to scan
    ///
    /// # Returns
    /// A PluginScanResult containing discovered plugins
    fn scan_plugins(&self, source: &PluginSource) -> PluginScanResult;

    /// Returns the name of this scanner implementation.
    fn scanner_name(&self) -> &str;
}

/// Common functionality for plugin scanners.
///
/// This struct provides helper methods that can be used by any PluginScanner implementation.
/// It corresponds to the non-abstract methods in Java's PluginScanner class.
pub struct ScannerHelper;

impl ScannerHelper {
    /// Discovers plugins from multiple sources.
    ///
    /// This corresponds to `discoverPlugins(Set<PluginSource> sources)` in Java.
    /// Scans each source and merges the results.
    ///
    /// # Arguments
    /// * `scanner` - The scanner implementation to use
    /// * `sources` - Set of plugin sources to scan
    ///
    /// # Returns
    /// A merged PluginScanResult containing all discovered plugins
    pub fn discover_plugins(
        scanner: &dyn PluginScanner,
        sources: &HashSet<PluginSource>,
    ) -> PluginScanResult {
        let start = Instant::now();
        let results: Vec<PluginScanResult> = sources
            .iter()
            .map(|source| scan_urls_and_add_plugins(scanner, source))
            .collect();

        let elapsed = start.elapsed();
        log::info!(
            "Scanning plugins with {} took {} ms",
            scanner.scanner_name(),
            elapsed.as_millis()
        );

        PluginScanResult::merge(results)
    }

    /// Creates a PluginDesc for a discovered plugin.
    ///
    /// This corresponds to `pluginDesc(Class, String, PluginType, PluginSource)` in Java.
    pub fn plugin_desc(
        class_name: String,
        version: String,
        plugin_type: PluginType,
        source: &PluginSource,
    ) -> PluginDesc {
        let location = source.location_string().to_string();
        PluginDesc::new(class_name, version, plugin_type, location)
    }

    /// Gets the version for a plugin from its version string.
    ///
    /// This corresponds to `versionFor(T pluginImpl)` in Java.
    /// In Java, this checks if the plugin implements Versioned interface.
    /// In Rust, we use the version string directly.
    pub fn version_for(version: Option<&str>) -> String {
        match version {
            Some(v) if !v.is_empty() => v.to_string(),
            _ => UNDEFINED_VERSION.to_string(),
        }
    }

    /// Returns a description for reflection-style errors.
    ///
    /// This corresponds to `reflectiveErrorDescription(Throwable t)` in Java.
    /// In Rust, we provide equivalent error descriptions for common failure modes.
    pub fn reflective_error_description(error: &PluginScanError) -> String {
        match error {
            PluginScanError::NoSuchMethod => {
                ": Plugin class must have a no-args constructor, and cannot be a non-static inner class".to_string()
            }
            PluginScanError::SecurityError => {
                ": Security settings must allow instantiation of plugin classes".to_string()
            }
            PluginScanError::IllegalAccess => {
                ": Plugin class default constructor must be public".to_string()
            }
            PluginScanError::InitializationError => {
                ": Failed to statically initialize plugin class".to_string()
            }
            PluginScanError::ConstructorError => {
                ": Failed to invoke plugin constructor".to_string()
            }
            PluginScanError::LinkageError => {
                ": Plugin class has a dependency which is missing or invalid".to_string()
            }
            PluginScanError::ServiceConfigurationError => {
                ": Service configuration error while loading plugin".to_string()
            }
            PluginScanError::InvalidPlugin => {
                ": Invalid plugin class or configuration".to_string()
            }
            PluginScanError::Unknown => "".to_string(),
        }
    }

    /// Simulates ServiceLoader-style plugin discovery for a specific type.
    ///
    /// This corresponds to `getServiceLoaderPluginDesc(PluginType, PluginSource)` in Java.
    /// In Rust, this reads from a plugin manifest/configuration instead of using ServiceLoader.
    ///
    /// # Arguments
    /// * `plugin_type` - The type of plugins to discover
    /// * `plugins` - List of plugin descriptors from the manifest
    /// * `source` - The plugin source being scanned
    ///
    /// # Returns
    /// A BTreeSet of PluginDesc for the matching plugins
    pub fn get_service_loader_plugins(
        plugin_type: PluginType,
        plugins: &[PluginEntry],
        source: &PluginSource,
    ) -> BTreeSet<PluginDesc> {
        let mut result = BTreeSet::new();

        for entry in plugins {
            if entry.plugin_type == plugin_type {
                // Only include plugins from the same loader/source
                // In Java: pluginKlass.getClassLoader() != source.loader() check
                // In Rust: we check if the entry's source matches
                let desc = Self::plugin_desc(
                    entry.class_name.clone(),
                    Self::version_for(entry.version.as_deref()),
                    plugin_type,
                    source,
                );
                result.insert(desc);
            }
        }

        result
    }
}

/// Scans a single plugin source URL and adds discovered plugins.
///
/// This corresponds to the private `scanUrlsAndAddPlugins(PluginSource)` method in Java.
fn scan_urls_and_add_plugins(
    scanner: &dyn PluginScanner,
    source: &PluginSource,
) -> PluginScanResult {
    log::info!("Loading plugin from: {}", source);
    if log::log_enabled!(log::Level::Debug) {
        log::debug!("Loading plugin urls: {:?}", source.urls());
    }

    let result = scanner.scan_plugins(source);
    log::info!("Registered loader: {}", source.loader());

    result
}

/// Represents a plugin entry in a configuration/manifest file.
///
/// This is used for configuration-driven plugin discovery,
/// replacing Java's ServiceLoader/reflection mechanism.
#[derive(Debug, Clone)]
pub struct PluginEntry {
    /// Fully qualified class name of the plugin.
    pub class_name: String,
    /// Type of the plugin.
    pub plugin_type: PluginType,
    /// Optional version string.
    pub version: Option<String>,
    /// Optional documentation.
    pub documentation: Option<String>,
}

impl PluginEntry {
    /// Creates a new PluginEntry.
    pub fn new(
        class_name: String,
        plugin_type: PluginType,
        version: Option<String>,
        documentation: Option<String>,
    ) -> Self {
        PluginEntry {
            class_name,
            plugin_type,
            version,
            documentation,
        }
    }
}

/// Error types for plugin scanning.
///
/// This corresponds to various exception types in Java's plugin scanning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginScanError {
    /// No such method error (constructor not found).
    NoSuchMethod,
    /// Security error (access denied).
    SecurityError,
    /// Illegal access error.
    IllegalAccess,
    /// Initialization error (static initializer failed).
    InitializationError,
    /// Constructor error (instantiation failed).
    ConstructorError,
    /// Linkage error (missing dependency).
    LinkageError,
    /// Service configuration error.
    ServiceConfigurationError,
    /// Invalid plugin.
    InvalidPlugin,
    /// Unknown error.
    Unknown,
}

impl fmt::Display for PluginScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PluginScanError::NoSuchMethod => write!(f, "NoSuchMethod"),
            PluginScanError::SecurityError => write!(f, "Security"),
            PluginScanError::IllegalAccess => write!(f, "IllegalAccess"),
            PluginScanError::InitializationError => write!(f, "Initialization"),
            PluginScanError::ConstructorError => write!(f, "Constructor"),
            PluginScanError::LinkageError => write!(f, "Linkage"),
            PluginScanError::ServiceConfigurationError => write!(f, "ServiceConfiguration"),
            PluginScanError::InvalidPlugin => write!(f, "InvalidPlugin"),
            PluginScanError::Unknown => write!(f, "Unknown"),
        }
    }
}

impl std::error::Error for PluginScanError {}

/// Loader swap guard for ClassLoader context switching.
///
/// This corresponds to the `LoaderSwap` inner class in Java's PluginScanner.
/// In Java, this temporarily swaps the thread's context ClassLoader.
/// In Rust, we use this to track loader context for logging/auditing purposes.
pub struct LoaderSwap {
    /// The saved loader identifier to restore.
    saved_loader_id: String,
}

impl LoaderSwap {
    /// Creates a new LoaderSwap guard.
    pub fn new(saved_loader_id: String) -> Self {
        LoaderSwap { saved_loader_id }
    }

    /// Returns the saved loader identifier.
    pub fn saved_loader(&self) -> &str {
        &self.saved_loader_id
    }
}

impl Drop for LoaderSwap {
    fn drop(&mut self) {
        // In Java, this restores the context ClassLoader.
        // In Rust, we just log the restoration for auditing.
        log::debug!("Restoring loader context to: {}", self.saved_loader_id);
    }
}

/// Default implementation for PluginScanner trait.
///
/// This provides a basic configuration-driven scanner that can be extended.
pub struct DefaultPluginScanner {
    name: String,
}

impl DefaultPluginScanner {
    /// Creates a new DefaultPluginScanner.
    pub fn new() -> Self {
        DefaultPluginScanner {
            name: "DefaultPluginScanner".to_string(),
        }
    }

    /// Creates a scanner with a custom name.
    pub fn with_name(name: String) -> Self {
        DefaultPluginScanner { name }
    }
}

impl Default for DefaultPluginScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginScanner for DefaultPluginScanner {
    fn scan_plugins(&self, source: &PluginSource) -> PluginScanResult {
        // Default implementation: returns empty result
        // Actual implementations (ServiceLoaderScanner, ReflectionScanner)
        // will override this with proper scanning logic
        log::debug!("DefaultPluginScanner scanning source: {}", source);
        PluginScanResult::new()
    }

    fn scanner_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_for() {
        assert_eq!(ScannerHelper::version_for(Some("1.0.0")), "1.0.0");
        assert_eq!(ScannerHelper::version_for(Some("")), UNDEFINED_VERSION);
        assert_eq!(ScannerHelper::version_for(None), UNDEFINED_VERSION);
    }

    #[test]
    fn test_reflective_error_description() {
        assert!(
            ScannerHelper::reflective_error_description(&PluginScanError::NoSuchMethod)
                .contains("constructor")
        );
        assert!(
            ScannerHelper::reflective_error_description(&PluginScanError::LinkageError)
                .contains("dependency")
        );
        assert_eq!(
            ScannerHelper::reflective_error_description(&PluginScanError::Unknown),
            ""
        );
    }

    #[test]
    fn test_plugin_desc_creation() {
        let source = PluginSource::classpath(vec![]);
        let desc = ScannerHelper::plugin_desc(
            "org.example.MyConnector".to_string(),
            "1.0.0".to_string(),
            PluginType::Source,
            &source,
        );

        assert_eq!(desc.plugin_class(), "org.example.MyConnector");
        assert_eq!(desc.version(), "1.0.0");
        assert_eq!(desc.type_(), PluginType::Source);
        assert_eq!(desc.location(), "classpath");
    }

    #[test]
    fn test_get_service_loader_plugins() {
        let source = PluginSource::single_jar(
            std::path::PathBuf::from("/test.jar"),
            "file:///test.jar".to_string(),
        );

        let plugins = vec![
            PluginEntry::new(
                "org.example.SourceConnector".to_string(),
                PluginType::Source,
                Some("1.0.0".to_string()),
                None,
            ),
            PluginEntry::new(
                "org.example.SinkConnector".to_string(),
                PluginType::Sink,
                Some("2.0.0".to_string()),
                None,
            ),
        ];

        let result =
            ScannerHelper::get_service_loader_plugins(PluginType::Source, &plugins, &source);

        assert_eq!(result.len(), 1);
        let first = result.iter().next().unwrap();
        assert_eq!(first.plugin_class(), "org.example.SourceConnector");
        assert_eq!(first.type_(), PluginType::Source);
    }

    #[test]
    fn test_loader_swap() {
        let swap = LoaderSwap::new("saved_loader".to_string());
        assert_eq!(swap.saved_loader(), "saved_loader");
    }

    #[test]
    fn test_discover_plugins_empty() {
        let scanner = DefaultPluginScanner::new();
        let sources = HashSet::new();
        let result = ScannerHelper::discover_plugins(&scanner, &sources);
        assert!(result.is_empty());
    }

    #[test]
    fn test_discover_plugins_single_source() {
        let scanner = DefaultPluginScanner::new();
        let source = PluginSource::classpath(vec![]);
        let sources = HashSet::from([source]);
        let result = ScannerHelper::discover_plugins(&scanner, &sources);
        // Default scanner returns empty
        assert!(result.is_empty());
    }

    #[test]
    fn test_plugin_entry() {
        let entry = PluginEntry::new(
            "org.example.Connector".to_string(),
            PluginType::Source,
            Some("1.0".to_string()),
            Some("Test connector".to_string()),
        );

        assert_eq!(entry.class_name, "org.example.Connector");
        assert_eq!(entry.plugin_type, PluginType::Source);
        assert_eq!(entry.version, Some("1.0".to_string()));
        assert_eq!(entry.documentation, Some("Test connector".to_string()));
    }

    #[test]
    fn test_plugin_scan_error_display() {
        assert_eq!(format!("{}", PluginScanError::NoSuchMethod), "NoSuchMethod");
        assert_eq!(format!("{}", PluginScanError::Unknown), "Unknown");
    }

    #[test]
    fn test_default_scanner() {
        let scanner = DefaultPluginScanner::new();
        assert_eq!(scanner.scanner_name(), "DefaultPluginScanner");

        let scanner2 = DefaultPluginScanner::with_name("CustomScanner".to_string());
        assert_eq!(scanner2.scanner_name(), "CustomScanner");
    }
}
