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

//! Plugin utilities for Kafka Connect.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.isolation.PluginUtils` in Java.
//!
//! This module provides utility methods for:
//! - Class name filtering (isolation checking)
//! - Plugin path handling
//! - Alias computation
//! - Version requirement parsing

use crate::isolation::plugin_desc::PluginDesc;
use crate::isolation::plugin_scan_result::PluginScanResult;
use crate::isolation::plugin_source::{PluginSource, PluginSourceType};
use crate::isolation::plugin_type::PluginType;
use common_trait::util::version_range::VersionRange;
use regex::Regex;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// A set that preserves insertion order (similar to Java's LinkedHashSet).
/// In Rust, we use Vec to preserve order and HashSet for uniqueness checking.
pub type OrderedSet<T> = Vec<T>;

/// Error type for plugin utils operations.
#[derive(Debug, Error)]
pub enum PluginUtilsError {
    /// Invalid path error.
    #[error("Invalid path: {0}")]
    InvalidPath(String),

    /// File not found error.
    #[error("File not found: {0}")]
    FileNotFoundException(String),

    /// IO error.
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    /// Invalid version specification.
    #[error("Invalid version specification: {0}")]
    InvalidVersionSpecification(String),
}

/// Plugin utility functions.
///
/// This struct provides static-like utility methods for plugin handling.
/// All methods are implemented as associated functions (no instance state).
pub struct PluginUtils;

// ============================================================================
// EXCLUDE and INCLUDE patterns (matching Java's regex patterns)
// ============================================================================

lazy_static::lazy_static! {
    /// EXCLUDE pattern - Classes that should NOT be loaded in isolation.
    ///
    /// This matches Java's EXCLUDE pattern which filters out:
    /// - Java standard library (java.*)
    /// - Java EE/Jakarta packages (javax.*)
    /// - Apache Kafka core classes (org.apache.kafka.*)
    /// - SLF4J logging (org.slf4j.*)
    ///
    /// The pattern is constructed to match these package prefixes and all their subpackages.
    static ref EXCLUDE: Regex = Regex::new(
        r"^(?:\
java\
|javax\\.accessibility\
|javax\\.activation\
|javax\\.activity\
|javax\\.annotation\
|javax\\.batch\\.api\
|javax\\.batch\\.operations\
|javax\\.batch\\.runtime\
|javax\\.crypto\
|javax\\.decorator\
|javax\\.ejb\
|javax\\.el\
|javax\\.enterprise\\.concurrent\
|javax\\.enterprise\\.context\
|javax\\.enterprise\\.context\\.spi\
|javax\\.enterprise\\.deploy\\.model\
|javax\\.enterprise\\.deploy\\.shared\
|javax\\.enterprise\\.deploy\\.spi\
|javax\\.enterprise\\.event\
|javax\\.enterprise\\.inject\
|javax\\.enterprise\\.inject\\.spi\
|javax\\.enterprise\\.util\
|javax\\.faces\
|javax\\.imageio\
|javax\\.inject\
|javax\\.interceptor\
|javax\\.jms\
|javax\\.json\
|javax\\.jws\
|javax\\.lang\\.model\
|javax\\.mail\
|javax\\.management\
|javax\\.management\\.j2ee\
|javax\\.naming\
|javax\\.net\
|javax\\.persistence\
|javax\\.print\
|javax\\.resource\
|javax\\.rmi\
|javax\\.script\
|javax\\.security\\.auth\
|javax\\.security\\.auth\\.message\
|javax\\.security\\.cert\
|javax\\.security\\.jacc\
|javax\\.security\\.sasl\
|javax\\.servlet\
|javax\\.sound\\.midi\
|javax\\.sound\\.sampled\
|javax\\.sql\
|javax\\.swing\
|javax\\.tools\
|javax\\.transaction\
|javax\\.validation\
|javax\\.websocket\
|javax\\.ws\\.rs\
|javax\\.xml\
|javax\\.xml\\.bind\
|javax\\.xml\\.registry\
|javax\\.xml\\.rpc\
|javax\\.xml\\.soap\
|javax\\.xml\\.ws\
|org\\.ietf\\.jgss\
|org\\.omg\\.CORBA\
|org\\.omg\\.CosNaming\
|org\\.omg\\.Dynamic\
|org\\.omg\\.DynamicAny\
|org\\.omg\\.IOP\
|org\\.omg\\.Messaging\
|org\\.omg\\.PortableInterceptor\
|org\\.omg\\.PortableServer\
|org\\.omg\\.SendingContext\
|org\\.omg\\.stub\\.java\\.rmi\
|org\\.w3c\\.dom\
|org\\.xml\\.sax\
|org\\.apache\\.kafka\
|org\\.slf4j\
)\\..*$"
    ).unwrap();

    /// INCLUDE pattern - Classes that SHOULD be loaded in isolation even if excluded.
    ///
    /// This matches Java's INCLUDE pattern which allows specific Kafka Connect
    /// subpackages to be loaded in isolation:
    /// - Connect transforms (except Transformation interface itself)
    /// - Connect JSON components
    /// - Connect file components
    /// - Mirror components
    /// - Converters
    /// - Config providers (except ConfigProvider interface)
    /// - REST extensions
    static ref INCLUDE: Regex = Regex::new(
        r"^org\\.apache\\.kafka\\.(?:connect\\.(?:\
transforms\\.(?!Transformation|predicates\\.Predicate$).*\
|json\\..*\
|file\\..*\
|mirror\\..*\
|mirror-client\\..*\
|converters\\..*\
|storage\\.StringConverter\
|storage\\.SimpleHeaderConverter\
|rest\\.basic\\.auth\\.extension\\.BasicAuthSecurityRestExtension\
|connector\\.policy\\.(?!ConnectorClientConfig(?:OverridePolicy|Request(?:\\$ClientType)?)$).*\
)\
|common\\.config\\.provider\\.(?!ConfigProvider$).*\
)$"
    ).unwrap();

    /// Pattern for splitting comma-separated paths with whitespace.
    static ref COMMA_WITH_WHITESPACE: Regex = Regex::new(r"\s*,\s*").unwrap();
}

impl PluginUtils {
    /// Returns whether the class with the given name should be loaded in isolation.
    ///
    /// This corresponds to `shouldLoadInIsolation(String name)` in Java.
    ///
    /// A class should be loaded in isolation if:
    /// - It is NOT in the EXCLUDE pattern, OR
    /// - It IS in the INCLUDE pattern (override for specific Kafka Connect classes)
    ///
    /// # Arguments
    /// * `name` - The fully qualified class name
    ///
    /// # Returns
    /// `true` if this class should be loaded in isolation, `false` otherwise.
    ///
    /// # Examples
    /// ```
    /// use connect_runtime::isolation::PluginUtils;
    ///
    /// // Standard Java classes should not be loaded in isolation
    /// assert!(!PluginUtils::should_load_in_isolation("java.lang.String"));
    ///
    /// // Kafka Connect transforms should be loaded in isolation
    /// assert!(PluginUtils::should_load_in_isolation("org.apache.kafka.connect.transforms.TimestampRouter"));
    /// ```
    pub fn should_load_in_isolation(name: &str) -> bool {
        // Logic: !(EXCLUDE matches AND NOT INCLUDE matches)
        // = NOT excluded OR included
        !(EXCLUDE.is_match(name) && !INCLUDE.is_match(name))
    }

    /// Returns whether a path corresponds to a JAR or ZIP archive.
    ///
    /// This corresponds to `isArchive(Path path)` in Java.
    ///
    /// # Arguments
    /// * `path` - The path to validate
    ///
    /// # Returns
    /// `true` if the path is a JAR or ZIP archive file, otherwise `false`.
    pub fn is_archive(path: &Path) -> bool {
        let path_str = path.to_string_lossy().to_lowercase();
        path_str.ends_with(".jar") || path_str.ends_with(".zip")
    }

    /// Returns whether a path corresponds to a Java class file.
    ///
    /// This corresponds to `isClassFile(Path path)` in Java.
    ///
    /// # Arguments
    /// * `path` - The path to validate
    ///
    /// # Returns
    /// `true` if the path is a Java class file, otherwise `false`.
    pub fn is_class_file(path: &Path) -> bool {
        path.to_string_lossy().to_lowercase().ends_with(".class")
    }

    /// Returns the simple class name of a plugin.
    ///
    /// This corresponds to `simpleName(PluginDesc<?> plugin)` in Java.
    ///
    /// # Arguments
    /// * `plugin` - The plugin descriptor
    ///
    /// # Returns
    /// The plugin's simple class name (without package).
    pub fn simple_name(plugin: &PluginDesc) -> &str {
        plugin.simple_name()
    }

    /// Remove the plugin type name at the end of a plugin class name.
    ///
    /// This corresponds to `prunedName(PluginDesc<?> plugin)` in Java.
    /// This method is used to extract plugin aliases.
    ///
    /// # Arguments
    /// * `plugin` - The plugin descriptor
    ///
    /// # Returns
    /// The pruned simple class name of the plugin.
    ///
    /// # Examples
    /// ```
    /// use connect_runtime::isolation::{PluginDesc, PluginType, PluginUtils};
    ///
    /// let desc = PluginDesc::new(
    ///     "org.example.FileSourceConnector".to_string(),
    ///     "1.0".to_string(),
    ///     PluginType::Source,
    ///     "classpath".to_string()
    /// );
    /// // "FileSourceConnector" -> "FileSource" (removes "Connector" suffix)
    /// ```
    pub fn pruned_name(plugin: &PluginDesc) -> String {
        match plugin.type_() {
            PluginType::Source | PluginType::Sink => {
                Self::prune_plugin_name(plugin.simple_name(), "Connector")
            }
            _ => Self::prune_plugin_name(plugin.simple_name(), plugin.type_().simple_name()),
        }
    }

    /// Prunes a suffix from a plugin name.
    ///
    /// This corresponds to the private `prunePluginName` method in Java.
    fn prune_plugin_name(simple_name: &str, suffix: &str) -> String {
        if let Some(pos) = simple_name.rfind(suffix) {
            if pos > 0 {
                return simple_name[..pos].to_string();
            }
        }
        simple_name.to_string()
    }

    /// Computes alias mappings from a plugin scan result.
    ///
    /// This corresponds to `computeAliases(PluginScanResult scanResult)` in Java.
    ///
    /// Each plugin can be referenced by:
    /// - Its simple class name (e.g., "FileSourceConnector")
    /// - Its pruned name (e.g., "FileSource" for connectors)
    ///
    /// If an alias maps to multiple distinct class names, it is considered
    /// ambiguous and excluded from the result.
    ///
    /// # Arguments
    /// * `scan_result` - The plugin scan result
    ///
    /// # Returns
    /// A map from alias to class name.
    pub fn compute_aliases(scan_result: &PluginScanResult) -> HashMap<String, String> {
        // Collect all potential aliases and their associated class names
        let mut alias_collisions: HashMap<String, HashSet<String>> = HashMap::new();

        scan_result.for_each(|plugin_desc| {
            // Add simple name as potential alias
            let simple_name = plugin_desc.simple_name();
            alias_collisions
                .entry(simple_name.to_string())
                .or_insert_with(HashSet::new)
                .insert(plugin_desc.plugin_class().to_string());

            // Add pruned name as potential alias
            let pruned_name = Self::pruned_name(plugin_desc);
            alias_collisions
                .entry(pruned_name)
                .or_insert_with(HashSet::new)
                .insert(plugin_desc.plugin_class().to_string());
        });

        // Build final alias map, excluding ambiguous aliases
        let mut aliases: HashMap<String, String> = HashMap::new();
        for (alias, class_names) in alias_collisions {
            if class_names.len() == 1 {
                // Only one class name: alias is valid
                let class_name = class_names.iter().next().unwrap().clone();
                aliases.insert(alias, class_name);
            } else {
                // Multiple class names: alias is ambiguous, skip
                log::debug!(
                    "Ignoring ambiguous alias '{}' since it refers to multiple distinct plugins {}",
                    alias,
                    class_names.iter().cloned().collect::<Vec<_>>().join(", ")
                );
            }
        }

        aliases
    }

    /// Parses plugin locations from a plugin path string.
    ///
    /// This corresponds to `pluginLocations(String pluginPath, boolean failFast)` in Java.
    ///
    /// The plugin path is a comma-separated list of paths. Each path can be:
    /// - A directory containing plugins (JAR files or class directories)
    /// - A single JAR file
    ///
    /// # Arguments
    /// * `plugin_path` - The comma-separated plugin path string
    /// * `fail_fast` - Whether to fail immediately on errors
    ///
    /// # Returns
    /// A set of paths to plugin locations (preserving insertion order).
    pub fn plugin_locations(
        plugin_path: Option<&str>,
        fail_fast: bool,
    ) -> Result<OrderedSet<PathBuf>, PluginUtilsError> {
        if plugin_path.is_none() {
            return Ok(OrderedSet::new());
        }

        let plugin_path = plugin_path.unwrap();
        let path_elements = COMMA_WITH_WHITESPACE.split(plugin_path.trim());
        let mut plugin_locations: OrderedSet<PathBuf> = OrderedSet::new();
        let mut seen: HashSet<PathBuf> = HashSet::new();

        for path in path_elements {
            let path = path.trim();
            if path.is_empty() {
                continue;
            }

            let plugin_path_element = PathBuf::from(path);

            if !plugin_path_element.exists() {
                if fail_fast {
                    return Err(PluginUtilsError::FileNotFoundException(
                        plugin_path_element.to_string_lossy().to_string(),
                    ));
                }
                log::error!("Could not get listing for plugin path: {}. Ignoring.", path);
                continue;
            }

            if plugin_path_element.is_dir() {
                // Directory: scan for subdirectories/archives
                let sub_locations = Self::scan_plugin_directory(&plugin_path_element)?;
                for loc in sub_locations {
                    if !seen.contains(&loc) {
                        seen.insert(loc.clone());
                        plugin_locations.push(loc);
                    }
                }
            } else if Self::is_archive(&plugin_path_element) {
                // Single archive file
                if !seen.contains(&plugin_path_element) {
                    seen.insert(plugin_path_element.clone());
                    plugin_locations.push(plugin_path_element);
                }
            }
        }

        Ok(plugin_locations)
    }

    /// Scans a directory for plugin locations.
    ///
    /// This corresponds to the private `pluginLocations(Path pluginPathElement)` in Java.
    fn scan_plugin_directory(dir: &Path) -> Result<Vec<PathBuf>, io::Error> {
        let mut locations = Vec::new();

        if dir.is_dir() {
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();

                // Filter: directory, archive, or class file
                if path.is_dir() || Self::is_archive(&path) || Self::is_class_file(&path) {
                    locations.push(path);
                }
            }
        }

        Ok(locations)
    }

    /// Gets plugin URLs from a top path.
    ///
    /// This corresponds to `pluginUrls(Path topPath)` in Java.
    /// Returns a list of paths to archives (JAR or ZIP files) or the top path
    /// itself if it contains only class files.
    ///
    /// # Arguments
    /// * `top_path` - The root path for plugin search
    ///
    /// # Returns
    /// A list of potential plugin paths.
    pub fn plugin_urls(top_path: &Path) -> Result<Vec<PathBuf>, io::Error> {
        // If the top path itself is an archive, return it
        if Self::is_archive(top_path) {
            return Ok(vec![top_path.to_path_buf()]);
        }

        let mut contains_class_files = false;
        let mut archives: BTreeSet<PathBuf> = BTreeSet::new();
        let mut visited: HashSet<PathBuf> = HashSet::new();

        // DFS traversal
        Self::dfs_scan(
            top_path,
            &mut contains_class_files,
            &mut archives,
            &mut visited,
        )?;

        if contains_class_files {
            if archives.is_empty() {
                // Only class files, return the top path
                return Ok(vec![top_path.to_path_buf()]);
            }
            log::warn!(
                "Plugin path contains both java archives and class files. Returning only the archives"
            );
        }

        Ok(archives.into_iter().collect())
    }

    /// DFS scan for plugin archives and class files.
    fn dfs_scan(
        current: &Path,
        contains_class_files: &mut bool,
        archives: &mut BTreeSet<PathBuf>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<(), io::Error> {
        visited.insert(current.to_path_buf());

        if !current.is_dir() {
            if Self::is_archive(current) {
                archives.insert(current.to_path_buf());
            } else if Self::is_class_file(current) {
                *contains_class_files = true;
            }
            return Ok(());
        }

        for entry in std::fs::read_dir(current)? {
            let entry = entry?;
            let path = entry.path();

            // Handle symbolic links
            let path = if path.is_symlink() {
                let target = std::fs::read_link(&path)?;
                let resolved = if target.is_absolute() {
                    target
                } else {
                    // Get parent directory safely
                    let parent = path
                        .parent()
                        .map(|p| p.to_path_buf())
                        .unwrap_or_else(PathBuf::new);
                    parent.join(target)
                };
                if resolved.exists() {
                    resolved
                } else {
                    log::warn!(
                        "Resolving symbolic link '{}' failed. Ignoring this path.",
                        path.display()
                    );
                    continue;
                }
            } else {
                path
            };

            if !visited.contains(&path) {
                Self::dfs_scan(&path, contains_class_files, archives, visited)?;
            }
        }

        Ok(())
    }

    /// Creates plugin sources from plugin locations.
    ///
    /// This corresponds to `pluginSources(Set<Path> pluginLocations, ClassLoader classLoader, PluginClassLoaderFactory factory)` in Java.
    ///
    /// In Rust (degraded approach), we use simplified plugin sources without actual ClassLoader.
    ///
    /// # Arguments
    /// * `plugin_locations` - Set of plugin location paths
    /// * `parent_loader_id` - Parent loader identifier (degraded: string instead of ClassLoader)
    ///
    /// # Returns
    /// A set of PluginSource instances (preserving insertion order).
    pub fn plugin_sources(
        plugin_locations: &OrderedSet<PathBuf>,
        parent_loader_id: &str,
    ) -> OrderedSet<PluginSource> {
        let mut plugin_sources: OrderedSet<PluginSource> = OrderedSet::new();
        let mut seen: HashSet<String> = HashSet::new();

        for plugin_location in plugin_locations {
            match Self::isolated_plugin_source(plugin_location, parent_loader_id) {
                Ok(source) => {
                    let key = source.location_string();
                    if !seen.contains(&key) {
                        seen.insert(key);
                        plugin_sources.push(source);
                    }
                }
                Err(e) => {
                    log::error!(
                        "Invalid path in plugin path: {}. Ignoring. Error: {}",
                        plugin_location.display(),
                        e
                    );
                }
            }
        }

        // Add classpath source
        let classpath_source = Self::classpath_plugin_source(parent_loader_id);
        let key = classpath_source.location_string();
        if !seen.contains(&key) {
            seen.insert(key);
            plugin_sources.push(classpath_source);
        }

        plugin_sources
    }

    /// Creates an isolated plugin source.
    ///
    /// This corresponds to `isolatedPluginSource(Path pluginLocation, ClassLoader parent, PluginClassLoaderFactory factory)` in Java.
    ///
    /// In Rust (degraded approach), we create a PluginSource without actual ClassLoader.
    pub fn isolated_plugin_source(
        plugin_location: &Path,
        _parent_loader_id: &str,
    ) -> Result<PluginSource, PluginUtilsError> {
        let paths = Self::plugin_urls(plugin_location)?;

        // Infer the type of the source
        let source_type = if paths.len() == 1 && paths[0] == *plugin_location {
            if Self::is_archive(plugin_location) {
                PluginSourceType::SingleJar
            } else {
                PluginSourceType::ClassHierarchy
            }
        } else {
            PluginSourceType::MultiJar
        };

        // Convert paths to URL strings
        let urls: Vec<String> = paths
            .iter()
            .map(|p| format!("file://{}", p.display()))
            .collect();

        Ok(PluginSource::new(
            Some(plugin_location.to_path_buf()),
            source_type,
            plugin_location.to_string_lossy().to_string(),
            urls,
        ))
    }

    /// Creates a classpath plugin source.
    ///
    /// This corresponds to `classpathPluginSource(ClassLoader classLoader)` in Java.
    pub fn classpath_plugin_source(loader_id: &str) -> PluginSource {
        PluginSource::classpath(vec![])
    }

    /// Parses a connector version requirement.
    ///
    /// This corresponds to `connectorVersionRequirement(String version)` in Java.
    ///
    /// The version specification can be:
    /// - `null` or `"latest"` - returns None (no restriction)
    /// - A version range specification like `[1.0,2.0)` or `[1.0]`
    /// - A simple version number (will be enclosed in brackets)
    ///
    /// # Arguments
    /// * `version` - The version specification string
    ///
    /// # Returns
    /// A VersionRange if specified, or None for "latest" or null.
    ///
    /// # Errors
    /// Returns an error if the version specification is invalid.
    pub fn connector_version_requirement(
        version: Option<&str>,
    ) -> Result<Option<VersionRange>, PluginUtilsError> {
        if version.is_none() {
            return Ok(None);
        }

        let version = version.unwrap().trim();
        if version == "latest" {
            return Ok(None);
        }

        // Try to parse as VersionRange
        let range = VersionRange::parse(version).map_err(|e| {
            PluginUtilsError::InvalidVersionSpecification(format!("{}: {}", version, e))
        })?;

        // If the range already has restrictions, return it
        if range.has_restrictions() {
            return Ok(Some(range));
        }

        // If the version is not enclosed, treat it as a hard requirement
        // Enclose in brackets: [version]
        let enclosed = format!("[{}]", version);
        let range = VersionRange::parse(&enclosed).map_err(|e| {
            PluginUtilsError::InvalidVersionSpecification(format!("{}: {}", enclosed, e))
        })?;

        Ok(Some(range))
    }

    /// Checks if a version matches a version range.
    ///
    /// # Arguments
    /// * `version` - The version string to check
    /// * `range` - The optional version range
    ///
    /// # Returns
    /// `true` if the version matches the range, or if no range is specified.
    pub fn version_matches(version: &str, range: Option<&VersionRange>) -> bool {
        match range {
            None => true, // No restriction
            Some(r) => r.matches(version),
        }
    }

    /// Finds the best matching plugin from a set based on version requirements.
    ///
    /// # Arguments
    /// * `plugins` - The set of plugin descriptors
    /// * `range` - The optional version range
    ///
    /// # Returns
    /// The best matching plugin, or None if no match found.
    pub fn find_best_match(
        plugins: &BTreeSet<PluginDesc>,
        range: Option<&VersionRange>,
    ) -> Option<PluginDesc> {
        if range.is_none() {
            // No restriction: return the latest version
            return plugins
                .iter()
                .max_by(|a, b| {
                    // Compare by version (using string comparison)
                    a.version().cmp(b.version())
                })
                .cloned();
        }

        let range = range.unwrap();

        // Find all matching versions and return the highest
        plugins
            .iter()
            .filter(|p| range.matches(p.version()))
            .max_by(|a, b| a.version().cmp(b.version()))
            .cloned()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_load_in_isolation_java_classes() {
        // Java standard library should NOT be loaded in isolation
        assert!(!PluginUtils::should_load_in_isolation("java.lang.String"));
        assert!(!PluginUtils::should_load_in_isolation(
            "java.util.ArrayList"
        ));
        assert!(!PluginUtils::should_load_in_isolation("java.io.File"));
    }

    #[test]
    fn test_should_load_in_isolation_javax_classes() {
        // Java EE classes should NOT be loaded in isolation
        assert!(!PluginUtils::should_load_in_isolation(
            "javax.servlet.http.HttpServlet"
        ));
        assert!(!PluginUtils::should_load_in_isolation(
            "javax.jms.Connection"
        ));
    }

    #[test]
    fn test_should_load_in_isolation_kafka_core() {
        // Kafka core classes should NOT be loaded in isolation
        assert!(!PluginUtils::should_load_in_isolation(
            "org.apache.kafka.clients.producer.KafkaProducer"
        ));
        assert!(!PluginUtils::should_load_in_isolation(
            "org.apache.kafka.common.TopicPartition"
        ));
    }

    #[test]
    fn test_should_load_in_isolation_connect_plugins() {
        // Kafka Connect plugins SHOULD be loaded in isolation
        assert!(PluginUtils::should_load_in_isolation(
            "org.apache.kafka.connect.transforms.TimestampRouter"
        ));
        assert!(PluginUtils::should_load_in_isolation(
            "org.apache.kafka.connect.json.JsonConverter"
        ));
        assert!(PluginUtils::should_load_in_isolation(
            "org.apache.kafka.connect.file.FileStreamSourceConnector"
        ));
    }

    #[test]
    fn test_should_load_in_isolation_custom_plugins() {
        // Custom plugins (not in exclude list) should be loaded in isolation
        assert!(PluginUtils::should_load_in_isolation(
            "com.example.CustomConnector"
        ));
        assert!(PluginUtils::should_load_in_isolation(
            "org.custom.MyConverter"
        ));
    }

    #[test]
    fn test_should_load_in_isolation_slf4j() {
        // SLF4J should NOT be loaded in isolation
        assert!(!PluginUtils::should_load_in_isolation("org.slf4j.Logger"));
    }

    #[test]
    fn test_is_archive() {
        assert!(PluginUtils::is_archive(Path::new("/path/to/plugin.jar")));
        assert!(PluginUtils::is_archive(Path::new("/path/to/plugin.zip")));
        assert!(PluginUtils::is_archive(Path::new("/path/to/PLUGIN.JAR")));
        assert!(!PluginUtils::is_archive(Path::new("/path/to/plugin.txt")));
        assert!(!PluginUtils::is_archive(Path::new("/path/to/classes")));
    }

    #[test]
    fn test_is_class_file() {
        assert!(PluginUtils::is_class_file(Path::new(
            "/path/to/MyClass.class"
        )));
        assert!(PluginUtils::is_class_file(Path::new(
            "/path/to/MYCLASS.CLASS"
        )));
        assert!(!PluginUtils::is_class_file(Path::new(
            "/path/to/MyClass.java"
        )));
        assert!(!PluginUtils::is_class_file(Path::new(
            "/path/to/MyClass.txt"
        )));
    }

    #[test]
    fn test_simple_name() {
        let desc = PluginDesc::new(
            "org.apache.kafka.connect.file.FileSourceConnector".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        );

        assert_eq!(PluginUtils::simple_name(&desc), "FileSourceConnector");
    }

    #[test]
    fn test_pruned_name_connector() {
        let desc = PluginDesc::new(
            "org.example.FileSourceConnector".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        );

        // Source/Sink: remove "Connector" suffix
        assert_eq!(PluginUtils::pruned_name(&desc), "FileSource");
    }

    #[test]
    fn test_pruned_name_converter() {
        let desc = PluginDesc::new(
            "org.example.JsonConverter".to_string(),
            "1.0".to_string(),
            PluginType::Converter,
            "classpath".to_string(),
        );

        // Converter: remove "Converter" suffix
        assert_eq!(PluginUtils::pruned_name(&desc), "Json");
    }

    #[test]
    fn test_pruned_name_no_suffix() {
        let desc = PluginDesc::new(
            "org.example.MyPlugin".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        );

        // No suffix to remove
        assert_eq!(PluginUtils::pruned_name(&desc), "MyPlugin");
    }

    #[test]
    fn test_compute_aliases_empty() {
        let result = PluginScanResult::new();
        let aliases = PluginUtils::compute_aliases(&result);
        assert!(aliases.is_empty());
    }

    #[test]
    fn test_compute_aliases_single_plugin() {
        let mut result = PluginScanResult::new();
        result.add_plugin(PluginDesc::new(
            "org.example.FileSourceConnector".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        ));

        let aliases = PluginUtils::compute_aliases(&result);

        // Should have two aliases: simple name and pruned name
        assert_eq!(
            aliases.get("FileSourceConnector"),
            Some(&"org.example.FileSourceConnector".to_string())
        );
        assert_eq!(
            aliases.get("FileSource"),
            Some(&"org.example.FileSourceConnector".to_string())
        );
    }

    #[test]
    fn test_compute_aliases_ambiguous() {
        let mut result = PluginScanResult::new();

        // Two different classes with same simple name
        result.add_plugin(PluginDesc::new(
            "org.example.FileSourceConnector".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        ));
        result.add_plugin(PluginDesc::new(
            "com.other.FileSourceConnector".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        ));

        let aliases = PluginUtils::compute_aliases(&result);

        // Alias should NOT be present because it's ambiguous
        assert!(!aliases.contains_key("FileSourceConnector"));
        assert!(!aliases.contains_key("FileSource"));
    }

    #[test]
    fn test_connector_version_requirement_none() {
        let result = PluginUtils::connector_version_requirement(None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_connector_version_requirement_latest() {
        let result = PluginUtils::connector_version_requirement(Some("latest"));
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_connector_version_requirement_exact() {
        let result = PluginUtils::connector_version_requirement(Some("[1.0]"));
        assert!(result.is_ok());
        let range = result.unwrap().unwrap();
        assert!(range.is_exact());
        assert!(range.matches("1.0"));
    }

    #[test]
    fn test_connector_version_requirement_range() {
        let result = PluginUtils::connector_version_requirement(Some("[1.0,2.0)"));
        assert!(result.is_ok());
        let range = result.unwrap().unwrap();
        assert!(range.matches("1.0"));
        assert!(range.matches("1.5"));
        assert!(!range.matches("2.0"));
    }

    #[test]
    fn test_connector_version_requirement_simple_version() {
        // Simple version (not enclosed) should be treated as hard requirement
        let result = PluginUtils::connector_version_requirement(Some("1.0"));
        assert!(result.is_ok());
        let range = result.unwrap().unwrap();
        assert!(range.is_exact());
        assert!(range.matches("1.0"));
    }

    #[test]
    fn test_version_matches_no_range() {
        assert!(PluginUtils::version_matches("1.0", None));
        assert!(PluginUtils::version_matches("99.0", None));
    }

    #[test]
    fn test_version_matches_with_range() {
        let range = VersionRange::parse("[1.0,2.0)").unwrap();
        assert!(PluginUtils::version_matches("1.0", Some(&range)));
        assert!(PluginUtils::version_matches("1.5", Some(&range)));
        assert!(!PluginUtils::version_matches("2.0", Some(&range)));
    }

    #[test]
    fn test_find_best_match_no_range() {
        let mut plugins = BTreeSet::new();
        plugins.insert(PluginDesc::new(
            "org.example.Connector".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        ));
        plugins.insert(PluginDesc::new(
            "org.example.Connector".to_string(),
            "2.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        ));

        // Without range, should return highest version
        let best = PluginUtils::find_best_match(&plugins, None);
        assert!(best.is_some());
        assert_eq!(best.unwrap().version(), "2.0");
    }

    #[test]
    fn test_find_best_match_with_range() {
        let mut plugins = BTreeSet::new();
        plugins.insert(PluginDesc::new(
            "org.example.Connector".to_string(),
            "1.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        ));
        plugins.insert(PluginDesc::new(
            "org.example.Connector".to_string(),
            "2.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        ));
        plugins.insert(PluginDesc::new(
            "org.example.Connector".to_string(),
            "3.0".to_string(),
            PluginType::Source,
            "classpath".to_string(),
        ));

        // Range [1.0,2.0] should match 1.0 and 2.0, return 2.0 (highest)
        let range = VersionRange::parse("[1.0,2.0]").unwrap();
        let best = PluginUtils::find_best_match(&plugins, Some(&range));
        assert!(best.is_some());
        assert_eq!(best.unwrap().version(), "2.0");
    }

    #[test]
    fn test_prune_plugin_name() {
        assert_eq!(
            PluginUtils::prune_plugin_name("FileSourceConnector", "Connector"),
            "FileSource"
        );
        assert_eq!(
            PluginUtils::prune_plugin_name("JsonConverter", "Converter"),
            "Json"
        );
        assert_eq!(
            PluginUtils::prune_plugin_name("MyPlugin", "Connector"),
            "MyPlugin"
        );
        assert_eq!(
            PluginUtils::prune_plugin_name("Connector", "Connector"),
            "Connector"
        );
    }
}
