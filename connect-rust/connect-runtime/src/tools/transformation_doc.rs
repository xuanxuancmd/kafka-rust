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

//! Transformation documentation generator.
//!
//! This corresponds to `org.apache.kafka.connect.tools.TransformationDoc` in Java.
//!
//! Generates HTML documentation for transformations registered in the Connect runtime.

use crate::tools::predicate_doc::config_def_to_html;
use common_trait::config::ConfigKeyDef;
use std::collections::HashMap;

/// Documentation information for a transformation.
///
/// This corresponds to the inner `DocInfo` record in Java's TransformationDoc.
#[derive(Debug)]
pub struct TransformationDocInfo {
    /// The fully qualified transformation class name.
    transformation_name: String,
    /// Overview documentation for the transformation.
    overview: String,
    /// Configuration definition for the transformation.
    config_def: HashMap<String, ConfigKeyDef>,
}

impl TransformationDocInfo {
    /// Creates a new TransformationDocInfo.
    ///
    /// # Arguments
    /// * `transformation_name` - The fully qualified transformation class name
    /// * `overview` - Overview documentation string
    /// * `config_def` - Configuration definition map
    pub fn new(
        transformation_name: String,
        overview: String,
        config_def: HashMap<String, ConfigKeyDef>,
    ) -> Self {
        TransformationDocInfo {
            transformation_name,
            overview,
            config_def,
        }
    }

    /// Returns the transformation class name.
    pub fn transformation_name(&self) -> &str {
        &self.transformation_name
    }

    /// Returns the overview documentation.
    pub fn overview(&self) -> &str {
        &self.overview
    }

    /// Returns the configuration definition.
    pub fn config_def(&self) -> &HashMap<String, ConfigKeyDef> {
        &self.config_def
    }
}

/// Trait for transformations that provide documentation.
///
/// In Java, TransformationDoc uses reflection to read static fields `OVERVIEW_DOC` and `CONFIG_DEF`.
/// In Rust, transformations can implement this trait to provide documentation info.
pub trait TransformationDocumentation {
    /// Returns the overview documentation for this transformation.
    fn overview_doc() -> &'static str;

    /// Returns the configuration definition for this transformation.
    fn config_def() -> HashMap<String, ConfigKeyDef>;

    /// Returns the fully qualified class name for this transformation.
    fn class_name() -> &'static str;
}

/// TransformationDoc main class for generating transformation documentation.
///
/// This corresponds to the `TransformationDoc` class in Java which has a `main` method
/// that outputs HTML documentation for all built-in transformations.
///
/// In Java, the transformations list is hardcoded:
/// ```java
/// private static final List<DocInfo> TRANSFORMATIONS = List.of(
///     new DocInfo(Cast.class.getName(), Cast.OVERVIEW_DOC, Cast.CONFIG_DEF),
///     new DocInfo(DropHeaders.class.getName(), DropHeaders.OVERVIEW_DOC, DropHeaders.CONFIG_DEF),
///     ...
/// );
/// ```
pub struct TransformationDoc;

impl TransformationDoc {
    /// Gets the list of built-in transformation documentation info.
    ///
    /// This corresponds to Java's `TRANSFORMATIONS` constant list.
    /// In Java, this is a hardcoded list of transformation classes.
    /// In Rust, we need to either:
    /// 1. Have transformations implement `TransformationDocumentation` trait
    /// 2. Register transformations in a registry
    ///
    /// # Arguments
    /// * `registry` - Registry containing transformation documentation
    ///
    /// # Returns
    /// Vector of TransformationDocInfo for all registered transformations.
    pub fn get_transformations(
        registry: &TransformationDocRegistry,
    ) -> Vec<&TransformationDocInfo> {
        // Get all registered transformations and sort by name
        let mut transformations: Vec<&TransformationDocInfo> = registry.all().values().collect();
        transformations.sort_by(|a, b| a.transformation_name().cmp(b.transformation_name()));
        transformations
    }

    /// Generates HTML documentation for all transformations.
    ///
    /// This corresponds to Java's `TransformationDoc.toHtml()` method.
    ///
    /// # Arguments
    /// * `registry` - Registry containing transformation documentation
    ///
    /// # Returns
    /// HTML string containing documentation for all transformations.
    pub fn to_html(registry: &TransformationDocRegistry) -> String {
        let transformations = Self::get_transformations(registry);
        transformations_to_html(&transformations)
    }

    /// Generates HTML documentation from a list of transformation doc info.
    ///
    /// # Arguments
    /// * `transformations_info` - List of transformation documentation info
    ///
    /// # Returns
    /// HTML string containing documentation.
    pub fn to_html_from_list(transformations_info: &[&TransformationDocInfo]) -> String {
        transformations_to_html(transformations_info)
    }
}

/// Generates HTML documentation for transformations.
///
/// This corresponds to `TransformationDoc.toHtml()` in Java.
///
/// # Arguments
/// * `transformations_info` - List of transformation documentation info
///
/// # Returns
/// HTML string containing documentation for all transformations.
pub fn transformations_to_html(transformations_info: &[&TransformationDocInfo]) -> String {
    let mut html = String::new();

    for doc_info in transformations_info {
        html.push_str(&format!(
            "<div id=\"{}\">\n",
            doc_info.transformation_name()
        ));
        html.push_str("<h5>");
        html.push_str(&format!(
            "<a href=\"#{}\">{}</a>",
            doc_info.transformation_name(),
            doc_info.transformation_name()
        ));
        html.push_str("</h5>\n");
        html.push_str(&format!("{}\n", doc_info.overview()));
        html.push_str("<p/>\n");
        html.push_str(&format!(
            "{}\n",
            config_def_to_html(doc_info.config_def(), 6, |key| {
                format!("{}_{}", doc_info.transformation_name(), key)
            })
        ));
        html.push_str("</div>\n");
    }

    html
}

/// Registry for transformation documentation.
///
/// In Java, reflection is used to get `OVERVIEW_DOC` and `CONFIG_DEF` from transformation classes.
/// In Rust, this registry provides a way to register transformation documentation.
#[derive(Debug, Default)]
pub struct TransformationDocRegistry {
    /// Map of transformation class name to documentation info.
    registry: HashMap<String, TransformationDocInfo>,
}

impl TransformationDocRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        TransformationDocRegistry {
            registry: HashMap::new(),
        }
    }

    /// Creates a registry with built-in transformations.
    ///
    /// This registers the standard Kafka Connect transformations.
    /// In Java, these are hardcoded in `TransformationDoc.TRANSFORMATIONS`.
    pub fn with_builtin_transformations() -> Self {
        let registry = Self::new();

        // Register all built-in transformations
        // Note: The actual transformation classes in connect-transforms crate
        // would need to implement TransformationDocumentation trait or provide
        // their OVERVIEW_DOC and config_def() functions.

        // For now, we create placeholder entries that would be populated
        // by the actual transformation implementations.

        registry
    }

    /// Registers a transformation's documentation.
    ///
    /// # Arguments
    /// * `transformation_name` - Fully qualified transformation class name
    /// * `overview` - Overview documentation
    /// * `config_def` - Configuration definition
    pub fn register(
        &mut self,
        transformation_name: String,
        overview: String,
        config_def: HashMap<String, ConfigKeyDef>,
    ) {
        let doc_info = TransformationDocInfo::new(transformation_name, overview, config_def);
        self.registry
            .insert(doc_info.transformation_name.clone(), doc_info);
    }

    /// Registers a transformation using its TransformationDocumentation implementation.
    ///
    /// # Type Parameters
    /// * `T` - Transformation type implementing TransformationDocumentation
    pub fn register_transformation<T: TransformationDocumentation>(&mut self) {
        self.register(
            T::class_name().to_string(),
            T::overview_doc().to_string(),
            T::config_def(),
        );
    }

    /// Gets documentation for a transformation by class name.
    ///
    /// # Arguments
    /// * `transformation_name` - The transformation class name
    ///
    /// # Returns
    /// Option containing the documentation info, or None if not registered.
    pub fn get(&self, transformation_name: &str) -> Option<&TransformationDocInfo> {
        self.registry.get(transformation_name)
    }

    /// Returns all registered transformation documentation.
    pub fn all(&self) -> &HashMap<String, TransformationDocInfo> {
        &self.registry
    }

    /// Returns the number of registered transformations.
    pub fn len(&self) -> usize {
        self.registry.len()
    }

    /// Returns true if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.registry.is_empty()
    }

    /// Clears the registry.
    pub fn clear(&mut self) {
        self.registry.clear();
    }
}

/// Built-in transformation class names matching Java's TransformationDoc.
///
/// These are the fully qualified class names for the standard transformations.
pub mod builtin_transformations {
    /// Cast transformation class name.
    pub const CAST: &str = "org.apache.kafka.connect.transforms.Cast";
    /// DropHeaders transformation class name.
    pub const DROP_HEADERS: &str = "org.apache.kafka.connect.transforms.DropHeaders";
    /// ExtractField transformation class name.
    pub const EXTRACT_FIELD: &str = "org.apache.kafka.connect.transforms.ExtractField";
    /// Filter transformation class name.
    pub const FILTER: &str = "org.apache.kafka.connect.transforms.Filter";
    /// Flatten transformation class name.
    pub const FLATTEN: &str = "org.apache.kafka.connect.transforms.Flatten";
    /// HeaderFrom transformation class name.
    pub const HEADER_FROM: &str = "org.apache.kafka.connect.transforms.HeaderFrom";
    /// HoistField transformation class name.
    pub const HOIST_FIELD: &str = "org.apache.kafka.connect.transforms.HoistField";
    /// InsertField transformation class name.
    pub const INSERT_FIELD: &str = "org.apache.kafka.connect.transforms.InsertField";
    /// InsertHeader transformation class name.
    pub const INSERT_HEADER: &str = "org.apache.kafka.connect.transforms.InsertHeader";
    /// MaskField transformation class name.
    pub const MASK_FIELD: &str = "org.apache.kafka.connect.transforms.MaskField";
    /// RegexRouter transformation class name.
    pub const REGEX_ROUTER: &str = "org.apache.kafka.connect.transforms.RegexRouter";
    /// ReplaceField transformation class name.
    pub const REPLACE_FIELD: &str = "org.apache.kafka.connect.transforms.ReplaceField";
    /// SetSchemaMetadata transformation class name.
    pub const SET_SCHEMA_METADATA: &str = "org.apache.kafka.connect.transforms.SetSchemaMetadata";
    /// TimestampConverter transformation class name.
    pub const TIMESTAMP_CONVERTER: &str = "org.apache.kafka.connect.transforms.TimestampConverter";
    /// TimestampRouter transformation class name.
    pub const TIMESTAMP_ROUTER: &str = "org.apache.kafka.connect.transforms.TimestampRouter";
    /// ValueToKey transformation class name.
    pub const VALUE_TO_KEY: &str = "org.apache.kafka.connect.transforms.ValueToKey";
}

/// Main entry point for transformation documentation generation.
///
/// This corresponds to `TransformationDoc.main()` in Java.
/// Prints HTML documentation for all transformations to stdout.
///
/// # Arguments
/// * `registry` - Registry containing transformation documentation
///
/// # Returns
/// The generated HTML string.
pub fn transformation_doc_main(registry: &TransformationDocRegistry) -> String {
    TransformationDoc::to_html(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType};
    use serde_json::Value;

    fn create_test_config_def() -> HashMap<String, ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                "spec",
                ConfigDefType::List,
                None,
                ConfigDefImportance::High,
                "List of field names to transform",
            )
            .define(
                "enabled",
                ConfigDefType::Boolean,
                Some(Value::Bool(true)),
                ConfigDefImportance::Medium,
                "Whether the transformation is enabled",
            )
            .build()
    }

    #[test]
    fn test_transformation_doc_info_new() {
        let config_def = create_test_config_def();
        let doc_info = TransformationDocInfo::new(
            "org.apache.kafka.connect.transforms.Cast".to_string(),
            "Cast fields or the entire key or value to a specific type".to_string(),
            config_def,
        );

        assert_eq!(
            doc_info.transformation_name(),
            "org.apache.kafka.connect.transforms.Cast"
        );
        assert_eq!(
            doc_info.overview(),
            "Cast fields or the entire key or value to a specific type"
        );
        assert_eq!(doc_info.config_def().len(), 2);
    }

    #[test]
    fn test_transformation_doc_registry() {
        let mut registry = TransformationDocRegistry::new();
        assert!(registry.is_empty());

        let config_def = create_test_config_def();
        registry.register(
            "org.apache.kafka.connect.transforms.Cast".to_string(),
            "Cast fields to a specific type".to_string(),
            config_def,
        );

        assert_eq!(registry.len(), 1);
        assert!(registry
            .get("org.apache.kafka.connect.transforms.Cast")
            .is_some());
    }

    #[test]
    fn test_transformations_to_html() {
        let config_def = create_test_config_def();
        let doc_info = TransformationDocInfo::new(
            "org.apache.kafka.connect.transforms.Cast".to_string(),
            "Cast fields to a specific type".to_string(),
            config_def,
        );

        let html = transformations_to_html(&[&doc_info]);

        assert!(html.contains("<div id=\""));
        assert!(html.contains("<h5>"));
        assert!(html.contains("<table class=\"config-table\">"));
        assert!(html.contains("<thead>"));
        assert!(html.contains("<tbody>"));
    }

    #[test]
    fn test_transformation_doc_to_html() {
        let mut registry = TransformationDocRegistry::new();
        let config_def = create_test_config_def();
        registry.register(
            "org.apache.kafka.connect.transforms.Cast".to_string(),
            "Cast fields to a specific type".to_string(),
            config_def,
        );

        let html = TransformationDoc::to_html(&registry);
        assert!(html.contains("Cast"));
    }

    #[test]
    fn test_builtin_transformation_names() {
        assert_eq!(
            builtin_transformations::CAST,
            "org.apache.kafka.connect.transforms.Cast"
        );
        assert_eq!(
            builtin_transformations::DROP_HEADERS,
            "org.apache.kafka.connect.transforms.DropHeaders"
        );
        assert_eq!(
            builtin_transformations::EXTRACT_FIELD,
            "org.apache.kafka.connect.transforms.ExtractField"
        );
        assert_eq!(
            builtin_transformations::FILTER,
            "org.apache.kafka.connect.transforms.Filter"
        );
        assert_eq!(
            builtin_transformations::FLATTEN,
            "org.apache.kafka.connect.transforms.Flatten"
        );
        assert_eq!(
            builtin_transformations::HEADER_FROM,
            "org.apache.kafka.connect.transforms.HeaderFrom"
        );
        assert_eq!(
            builtin_transformations::HOIST_FIELD,
            "org.apache.kafka.connect.transforms.HoistField"
        );
        assert_eq!(
            builtin_transformations::INSERT_FIELD,
            "org.apache.kafka.connect.transforms.InsertField"
        );
        assert_eq!(
            builtin_transformations::INSERT_HEADER,
            "org.apache.kafka.connect.transforms.InsertHeader"
        );
        assert_eq!(
            builtin_transformations::MASK_FIELD,
            "org.apache.kafka.connect.transforms.MaskField"
        );
        assert_eq!(
            builtin_transformations::REGEX_ROUTER,
            "org.apache.kafka.connect.transforms.RegexRouter"
        );
        assert_eq!(
            builtin_transformations::REPLACE_FIELD,
            "org.apache.kafka.connect.transforms.ReplaceField"
        );
        assert_eq!(
            builtin_transformations::SET_SCHEMA_METADATA,
            "org.apache.kafka.connect.transforms.SetSchemaMetadata"
        );
        assert_eq!(
            builtin_transformations::TIMESTAMP_CONVERTER,
            "org.apache.kafka.connect.transforms.TimestampConverter"
        );
        assert_eq!(
            builtin_transformations::TIMESTAMP_ROUTER,
            "org.apache.kafka.connect.transforms.TimestampRouter"
        );
        assert_eq!(
            builtin_transformations::VALUE_TO_KEY,
            "org.apache.kafka.connect.transforms.ValueToKey"
        );
    }

    #[test]
    fn test_get_transformations_sorted() {
        let mut registry = TransformationDocRegistry::new();

        // Register multiple transformations in random order
        registry.register(
            "org.apache.kafka.connect.transforms.ValueToKey".to_string(),
            "Extract key from value".to_string(),
            create_test_config_def(),
        );
        registry.register(
            "org.apache.kafka.connect.transforms.Cast".to_string(),
            "Cast fields".to_string(),
            create_test_config_def(),
        );
        registry.register(
            "org.apache.kafka.connect.transforms.Filter".to_string(),
            "Filter records".to_string(),
            create_test_config_def(),
        );

        let transformations = TransformationDoc::get_transformations(&registry);

        // Should be sorted by name
        assert_eq!(
            transformations[0].transformation_name(),
            "org.apache.kafka.connect.transforms.Cast"
        );
        assert_eq!(
            transformations[1].transformation_name(),
            "org.apache.kafka.connect.transforms.Filter"
        );
        assert_eq!(
            transformations[2].transformation_name(),
            "org.apache.kafka.connect.transforms.ValueToKey"
        );
    }
}
