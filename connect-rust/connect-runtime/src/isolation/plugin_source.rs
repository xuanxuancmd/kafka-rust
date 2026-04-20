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

//! Plugin source for Kafka Connect plugin discovery.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.isolation.PluginSource` in Java.
//! Represents a source of plugins (classpath, JAR files, or class hierarchy).

use std::fmt;
use std::path::PathBuf;

/// Type of plugin source.
///
/// This corresponds to `org.apache.kafka.connect.runtime.isolation.PluginSource.Type` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PluginSourceType {
    /// Plugins loaded from the classpath.
    /// Corresponds to CLASSPATH in Java.
    Classpath,
    /// Plugins loaded from a single JAR file.
    /// Corresponds to SINGLE_JAR in Java.
    SingleJar,
    /// Plugins loaded from multiple JAR files in a directory.
    /// Corresponds to MULTI_JAR in Java.
    MultiJar,
    /// Plugins loaded from a class hierarchy (class files in a directory).
    /// Corresponds to CLASS_HIERARCHY in Java.
    ClassHierarchy,
}

impl PluginSourceType {
    /// Returns true if this source represents isolated plugins (not classpath).
    pub fn is_isolated(&self) -> bool {
        matches!(
            self,
            PluginSourceType::SingleJar
                | PluginSourceType::MultiJar
                | PluginSourceType::ClassHierarchy
        )
    }

    /// Returns true if this source is from archive files.
    pub fn is_archive(&self) -> bool {
        matches!(
            self,
            PluginSourceType::SingleJar | PluginSourceType::MultiJar
        )
    }
}

impl fmt::Display for PluginSourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PluginSourceType::Classpath => write!(f, "classpath"),
            PluginSourceType::SingleJar => write!(f, "single_jar"),
            PluginSourceType::MultiJar => write!(f, "multi_jar"),
            PluginSourceType::ClassHierarchy => write!(f, "class_hierarchy"),
        }
    }
}

/// Plugin source representing a location where plugins can be discovered.
///
/// This corresponds to `org.apache.kafka.connect.runtime.isolation.PluginSource` in Java.
/// In Rust, we use a simplified representation that doesn't rely on actual ClassLoader
/// instances. Instead, we use:
/// - `location`: The path where plugins are located (None for classpath)
/// - `type`: The type of plugin source
/// - `loader_id`: A string identifier for the loader (used for tracking)
/// - `urls`: URLs representing the plugin paths
///
/// This is a configuration-driven approach that replaces Java's ClassLoader-based isolation.
#[derive(Debug, Clone)]
pub struct PluginSource {
    /// The location of the plugin source (file path).
    /// None for classpath sources.
    location: Option<PathBuf>,
    /// The type of this plugin source.
    source_type: PluginSourceType,
    /// Identifier for the "loader" (in Rust, this is a string identifier).
    /// For classpath, this is "classpath".
    /// For isolated sources, this is typically the location path.
    loader_id: String,
    /// URLs representing the plugin paths.
    /// In Java, these are used for URLClassLoader.
    /// In Rust, these represent the paths for plugin discovery.
    urls: Vec<String>,
}

impl PluginSource {
    /// Creates a new PluginSource.
    ///
    /// # Arguments
    /// * `location` - Optional path to the plugin location
    /// * `source_type` - Type of the plugin source
    /// * `loader_id` - Identifier for the loader
    /// * `urls` - URLs representing plugin paths
    pub fn new(
        location: Option<PathBuf>,
        source_type: PluginSourceType,
        loader_id: String,
        urls: Vec<String>,
    ) -> Self {
        PluginSource {
            location,
            source_type,
            loader_id,
            urls,
        }
    }

    /// Creates a classpath plugin source.
    ///
    /// This corresponds to `PluginSource.classpathPluginSource()` in Java.
    pub fn classpath(urls: Vec<String>) -> Self {
        PluginSource {
            location: None,
            source_type: PluginSourceType::Classpath,
            loader_id: "classpath".to_string(),
            urls,
        }
    }

    /// Creates a single JAR plugin source.
    pub fn single_jar(location: PathBuf, url: String) -> Self {
        PluginSource {
            location: Some(location.clone()),
            source_type: PluginSourceType::SingleJar,
            loader_id: location.to_string_lossy().to_string(),
            urls: vec![url],
        }
    }

    /// Creates a multi JAR plugin source.
    pub fn multi_jar(location: PathBuf, urls: Vec<String>) -> Self {
        PluginSource {
            location: Some(location.clone()),
            source_type: PluginSourceType::MultiJar,
            loader_id: location.to_string_lossy().to_string(),
            urls,
        }
    }

    /// Creates a class hierarchy plugin source.
    pub fn class_hierarchy(location: PathBuf) -> Self {
        let url = location.to_string_lossy().to_string();
        PluginSource {
            location: Some(location.clone()),
            source_type: PluginSourceType::ClassHierarchy,
            loader_id: url.clone(),
            urls: vec![url],
        }
    }

    /// Returns the location of this plugin source.
    pub fn location(&self) -> Option<&PathBuf> {
        self.location.as_ref()
    }

    /// Returns the type of this plugin source.
    pub fn source_type(&self) -> PluginSourceType {
        self.source_type
    }

    /// Returns the loader identifier.
    /// In Java, this returns the actual ClassLoader.
    /// In Rust, we return a string identifier.
    pub fn loader(&self) -> &str {
        &self.loader_id
    }

    /// Returns the URLs for this plugin source.
    pub fn urls(&self) -> &[String] {
        &self.urls
    }

    /// Returns true if this source is isolated (not classpath).
    pub fn is_isolated(&self) -> bool {
        self.source_type.is_isolated()
    }

    /// Returns the location as a string for display.
    pub fn location_string(&self) -> String {
        self.location
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "classpath".to_string())
    }
}

impl fmt::Display for PluginSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.location {
            Some(loc) => write!(f, "{} ({})", loc.display(), self.source_type),
            None => write!(f, "classpath"),
        }
    }
}

impl PartialEq for PluginSource {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location && self.source_type == other.source_type
    }
}

impl Eq for PluginSource {}

impl std::hash::Hash for PluginSource {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.location.hash(state);
        self.source_type.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classpath_source() {
        let source = PluginSource::classpath(vec![
            "file:///path/to/lib.jar".to_string(),
            "file:///path/to/classes".to_string(),
        ]);

        assert!(source.location().is_none());
        assert_eq!(source.source_type(), PluginSourceType::Classpath);
        assert_eq!(source.loader(), "classpath");
        assert!(!source.is_isolated());
        assert_eq!(source.urls().len(), 2);
    }

    #[test]
    fn test_single_jar_source() {
        let location = PathBuf::from("/path/to/plugin.jar");
        let source =
            PluginSource::single_jar(location.clone(), "file:///path/to/plugin.jar".to_string());

        assert_eq!(source.location(), Some(&location));
        assert_eq!(source.source_type(), PluginSourceType::SingleJar);
        assert!(source.is_isolated());
        assert_eq!(source.urls().len(), 1);
    }

    #[test]
    fn test_multi_jar_source() {
        let location = PathBuf::from("/path/to/plugins");
        let urls = vec![
            "file:///path/to/plugins/plugin1.jar".to_string(),
            "file:///path/to/plugins/plugin2.jar".to_string(),
        ];
        let source = PluginSource::multi_jar(location.clone(), urls.clone());

        assert_eq!(source.location(), Some(&location));
        assert_eq!(source.source_type(), PluginSourceType::MultiJar);
        assert!(source.is_isolated());
        assert_eq!(source.urls().len(), 2);
    }

    #[test]
    fn test_class_hierarchy_source() {
        let location = PathBuf::from("/path/to/classes");
        let source = PluginSource::class_hierarchy(location.clone());

        assert_eq!(source.location(), Some(&location));
        assert_eq!(source.source_type(), PluginSourceType::ClassHierarchy);
        assert!(source.is_isolated());
        assert_eq!(source.urls().len(), 1);
    }

    #[test]
    fn test_source_type_is_isolated() {
        assert!(!PluginSourceType::Classpath.is_isolated());
        assert!(PluginSourceType::SingleJar.is_isolated());
        assert!(PluginSourceType::MultiJar.is_isolated());
        assert!(PluginSourceType::ClassHierarchy.is_isolated());
    }

    #[test]
    fn test_source_type_is_archive() {
        assert!(!PluginSourceType::Classpath.is_archive());
        assert!(PluginSourceType::SingleJar.is_archive());
        assert!(PluginSourceType::MultiJar.is_archive());
        assert!(!PluginSourceType::ClassHierarchy.is_archive());
    }

    #[test]
    fn test_display() {
        let classpath = PluginSource::classpath(vec![]);
        assert_eq!(format!("{}", classpath), "classpath");

        let jar =
            PluginSource::single_jar(PathBuf::from("/test.jar"), "file:///test.jar".to_string());
        assert!(format!("{}", jar).contains("/test.jar"));
        assert!(format!("{}", jar).contains("single_jar"));
    }

    #[test]
    fn test_equality() {
        let source1 = PluginSource::single_jar(PathBuf::from("/test.jar"), "url1".to_string());
        let source2 = PluginSource::single_jar(PathBuf::from("/test.jar"), "url2".to_string());
        let source3 = PluginSource::single_jar(PathBuf::from("/other.jar"), "url1".to_string());

        assert_eq!(source1, source2); // Same location and type
        assert_ne!(source1, source3); // Different location
    }
}
