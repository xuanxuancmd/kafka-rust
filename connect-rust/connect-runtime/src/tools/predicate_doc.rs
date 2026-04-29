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

//! Predicate documentation generator.
//!
//! This corresponds to `org.apache.kafka.connect.tools.PredicateDoc` in Java.
//!
//! Generates HTML documentation for predicates registered in the Connect runtime.

use crate::isolation::plugins::Plugins;
use common_trait::config::ConfigKeyDef;
use std::collections::HashMap;

/// Documentation information for a predicate.
///
/// This corresponds to the inner `DocInfo` class in Java's PredicateDoc.
#[derive(Debug)]
pub struct PredicateDocInfo {
    /// The fully qualified predicate class name.
    predicate_name: String,
    /// Overview documentation for the predicate.
    overview: String,
    /// Configuration definition for the predicate.
    config_def: HashMap<String, ConfigKeyDef>,
}

impl PredicateDocInfo {
    /// Creates a new PredicateDocInfo.
    ///
    /// # Arguments
    /// * `predicate_name` - The fully qualified predicate class name
    /// * `overview` - Overview documentation string
    /// * `config_def` - Configuration definition map
    pub fn new(
        predicate_name: String,
        overview: String,
        config_def: HashMap<String, ConfigKeyDef>,
    ) -> Self {
        PredicateDocInfo {
            predicate_name,
            overview,
            config_def,
        }
    }

    /// Returns the predicate class name.
    pub fn predicate_name(&self) -> &str {
        &self.predicate_name
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

/// Trait for predicates that provide documentation.
///
/// In Java, PredicateDoc uses reflection to read static fields `OVERVIEW_DOC` and `CONFIG_DEF`.
/// In Rust, predicates can implement this trait to provide documentation info.
pub trait PredicateDocumentation {
    /// Returns the overview documentation for this predicate.
    fn overview_doc() -> &'static str;

    /// Returns the configuration definition for this predicate.
    fn config_def() -> HashMap<String, ConfigKeyDef>;

    /// Returns the fully qualified class name for this predicate.
    fn class_name() -> &'static str;
}

/// Generates HTML documentation for predicates.
///
/// This corresponds to `PredicateDoc.toHtml()` in Java.
///
/// # Arguments
/// * `predicates_info` - List of predicate documentation info
///
/// # Returns
/// HTML string containing documentation for all predicates.
pub fn predicates_to_html(predicates_info: &[&PredicateDocInfo]) -> String {
    let mut html = String::new();

    for doc_info in predicates_info {
        html.push_str(&format!("<div id=\"{}\">\n", doc_info.predicate_name()));
        html.push_str("<h5>");
        html.push_str(&format!(
            "<a href=\"#{}\">{}</a>",
            doc_info.predicate_name(),
            doc_info.predicate_name()
        ));
        html.push_str("</h5>\n");
        html.push_str(&format!("{}\n", doc_info.overview()));
        html.push_str("<p/>\n");
        html.push_str(&format!(
            "{}\n",
            config_def_to_html(doc_info.config_def(), 6, |key| {
                format!("{}_{}", doc_info.predicate_name(), key)
            })
        ));
        html.push_str("</div>\n");
    }

    html
}

/// Converts a ConfigDef to HTML format.
///
/// This corresponds to `ConfigDef.toHtml()` in Java.
///
/// # Arguments
/// * `config_def` - The configuration definition map
/// * `indent_level` - HTML indentation level
/// * `key_formatter` - Function to format configuration key names
///
/// # Returns
/// HTML string representing the configuration definition.
pub fn config_def_to_html<F>(
    config_def: &HashMap<String, ConfigKeyDef>,
    indent_level: usize,
    key_formatter: F,
) -> String
where
    F: Fn(&str) -> String,
{
    let indent = " ".repeat(indent_level);
    let mut html = String::new();

    html.push_str(&format!("{}<table class=\"config-table\">\n", indent));
    html.push_str(&format!("{}  <thead>\n", indent));
    html.push_str(&format!("{}    <tr>\n", indent));
    html.push_str(&format!("{}      <th>Name</th>\n", indent));
    html.push_str(&format!("{}      <th>Description</th>\n", indent));
    html.push_str(&format!("{}      <th>Type</th>\n", indent));
    html.push_str(&format!("{}      <th>Default</th>\n", indent));
    html.push_str(&format!("{}      <th>Importance</th>\n", indent));
    html.push_str(&format!("{}    </tr>\n", indent));
    html.push_str(&format!("{}  </thead>\n", indent));
    html.push_str(&format!("{}  <tbody>\n", indent));

    // Sort config keys by name for consistent output
    let mut sorted_keys: Vec<&String> = config_def.keys().collect();
    sorted_keys.sort();

    for key_name in sorted_keys {
        if let Some(key_def) = config_def.get(key_name) {
            // Skip internal configs
            if key_def.internal_config() {
                continue;
            }

            let formatted_key = key_formatter(key_name);
            html.push_str(&format!("{}    <tr>\n", indent));
            html.push_str(&format!(
                "{}      <td id=\"{}\">{}</td>\n",
                indent, formatted_key, key_name
            ));
            html.push_str(&format!("{}      <td>{}</td>\n", indent, key_def.doc()));
            html.push_str(&format!(
                "{}      <td>{}</td>\n",
                indent,
                key_def.config_type().name()
            ));
            html.push_str(&format!(
                "{}      <td>{}</td>\n",
                indent,
                key_def
                    .default_value()
                    .map(|v| format_default_value(v))
                    .unwrap_or_else(|| "null".to_string())
            ));
            html.push_str(&format!(
                "{}      <td>{}</td>\n",
                indent,
                key_def.importance().name()
            ));
            html.push_str(&format!("{}    </tr>\n", indent));
        }
    }

    html.push_str(&format!("{}  </tbody>\n", indent));
    html.push_str(&format!("{}</table>\n", indent));

    html
}

/// Formats a default value for HTML display.
///
/// # Arguments
/// * `value` - The JSON value to format
///
/// # Returns
/// String representation of the value.
pub fn format_default_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_default_value).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(obj) => {
            let items: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_default_value(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

/// PredicateDoc main class for generating predicate documentation.
///
/// This corresponds to the `PredicateDoc` class in Java which has a `main` method
/// that outputs HTML documentation for all predicates.
pub struct PredicateDoc;

impl PredicateDoc {
    /// Generates HTML documentation for all predicates registered in Plugins.
    ///
    /// This corresponds to Java's `PredicateDoc.PREDICATES` initialization and `toHtml()` method.
    ///
    /// In Java, this uses reflection to read `OVERVIEW_DOC` and `CONFIG_DEF` static fields.
    /// In Rust, this requires predicates to implement `PredicateDocumentation` trait
    /// or be registered via a documentation registry.
    ///
    /// # Arguments
    /// * `plugins` - Plugins instance to get predicate descriptors
    /// * `doc_registry` - Registry of predicate documentation providers
    ///
    /// # Returns
    /// HTML string containing documentation for all predicates.
    pub fn generate_html(plugins: &Plugins, doc_registry: &PredicateDocRegistry) -> String {
        let predicates = plugins.predicates();
        let mut predicates_info: Vec<&PredicateDocInfo> = Vec::new();

        for plugin_desc in predicates {
            // Look up documentation from registry
            if let Some(doc) = doc_registry.get(plugin_desc.plugin_class()) {
                predicates_info.push(doc);
            }
        }

        // Sort by predicate name
        predicates_info.sort_by(|a, b| a.predicate_name().cmp(b.predicate_name()));

        predicates_to_html(&predicates_info)
    }

    /// Generates HTML documentation from a list of predicate doc info.
    ///
    /// This is a simpler method that doesn't require Plugins instance.
    ///
    /// # Arguments
    /// * `predicates_info` - List of predicate documentation info
    ///
    /// # Returns
    /// HTML string containing documentation.
    pub fn to_html(predicates_info: &[&PredicateDocInfo]) -> String {
        predicates_to_html(predicates_info)
    }
}

/// Registry for predicate documentation.
///
/// In Java, reflection is used to get `OVERVIEW_DOC` and `CONFIG_DEF` from predicate classes.
/// In Rust, this registry provides a way to register predicate documentation.
#[derive(Debug, Default)]
pub struct PredicateDocRegistry {
    /// Map of predicate class name to documentation info.
    registry: HashMap<String, PredicateDocInfo>,
}

impl PredicateDocRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        PredicateDocRegistry {
            registry: HashMap::new(),
        }
    }

    /// Registers a predicate's documentation.
    ///
    /// # Arguments
    /// * `predicate_name` - Fully qualified predicate class name
    /// * `overview` - Overview documentation
    /// * `config_def` - Configuration definition
    pub fn register(
        &mut self,
        predicate_name: String,
        overview: String,
        config_def: HashMap<String, ConfigKeyDef>,
    ) {
        let doc_info = PredicateDocInfo::new(predicate_name, overview, config_def);
        self.registry
            .insert(doc_info.predicate_name.clone(), doc_info);
    }

    /// Registers a predicate using its PredicateDocumentation implementation.
    ///
    /// # Type Parameters
    /// * `P` - Predicate type implementing PredicateDocumentation
    pub fn register_predicate<P: PredicateDocumentation>(&mut self) {
        self.register(
            P::class_name().to_string(),
            P::overview_doc().to_string(),
            P::config_def(),
        );
    }

    /// Gets documentation for a predicate by class name.
    ///
    /// # Arguments
    /// * `predicate_name` - The predicate class name
    ///
    /// # Returns
    /// Option containing the documentation info, or None if not registered.
    pub fn get(&self, predicate_name: &str) -> Option<&PredicateDocInfo> {
        self.registry.get(predicate_name)
    }

    /// Returns all registered predicate documentation.
    pub fn all(&self) -> &HashMap<String, PredicateDocInfo> {
        &self.registry
    }

    /// Returns the number of registered predicates.
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

/// Main entry point for predicate documentation generation.
///
/// This corresponds to `PredicateDoc.main()` in Java.
/// Prints HTML documentation for all predicates to stdout.
///
/// # Arguments
/// * `predicates_info` - List of predicate documentation info
///
/// # Returns
/// The generated HTML string.
pub fn predicate_doc_main(predicates_info: &[&PredicateDocInfo]) -> String {
    PredicateDoc::to_html(predicates_info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType};
    use serde_json::Value;

    fn create_test_config_def() -> HashMap<String, ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                "name",
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The predicate name",
            )
            .define(
                "enabled",
                ConfigDefType::Boolean,
                Some(Value::Bool(true)),
                ConfigDefImportance::Medium,
                "Whether the predicate is enabled",
            )
            .build()
    }

    #[test]
    fn test_predicate_doc_info_new() {
        let config_def = create_test_config_def();
        let doc_info = PredicateDocInfo::new(
            "org.apache.kafka.connect.transforms.predicates.HasHeaderKey".to_string(),
            "A predicate that checks for a header key".to_string(),
            config_def,
        );

        assert_eq!(
            doc_info.predicate_name(),
            "org.apache.kafka.connect.transforms.predicates.HasHeaderKey"
        );
        assert_eq!(
            doc_info.overview(),
            "A predicate that checks for a header key"
        );
        assert_eq!(doc_info.config_def().len(), 2);
    }

    #[test]
    fn test_predicate_doc_registry() {
        let mut registry = PredicateDocRegistry::new();
        assert!(registry.is_empty());

        let config_def = create_test_config_def();
        registry.register(
            "org.apache.kafka.connect.transforms.predicates.HasHeaderKey".to_string(),
            "A predicate that checks for a header key".to_string(),
            config_def,
        );

        assert_eq!(registry.len(), 1);
        assert!(registry
            .get("org.apache.kafka.connect.transforms.predicates.HasHeaderKey")
            .is_some());
    }

    #[test]
    fn test_predicates_to_html() {
        let config_def = create_test_config_def();
        let doc_info = PredicateDocInfo::new(
            "org.apache.kafka.connect.transforms.predicates.HasHeaderKey".to_string(),
            "A predicate that checks for a header key".to_string(),
            config_def,
        );

        let html = predicates_to_html(&[&doc_info]);

        assert!(html.contains("<div id=\""));
        assert!(html.contains("<h5>"));
        assert!(html.contains("<table class=\"config-table\">"));
        assert!(html.contains("<thead>"));
        assert!(html.contains("<tbody>"));
    }

    #[test]
    fn test_config_def_to_html() {
        let config_def = create_test_config_def();
        let html = config_def_to_html(&config_def, 6, |key| format!("pred_{}", key));

        assert!(html.contains("<table class=\"config-table\">"));
        assert!(html.contains("name"));
        assert!(html.contains("The predicate name"));
        assert!(html.contains("STRING"));
        assert!(html.contains("enabled"));
        assert!(html.contains("true"));
        assert!(html.contains("HIGH"));
    }

    #[test]
    fn test_predicate_doc_generate_html_empty() {
        let plugins = Plugins::new(HashMap::new());
        let registry = PredicateDocRegistry::new();

        let html = PredicateDoc::generate_html(&plugins, &registry);
        // Empty predicates should produce empty HTML
        assert!(html.is_empty() || !html.contains("<div id="));
    }

    #[test]
    fn test_format_default_value() {
        assert_eq!(format_default_value(&Value::Null), "null");
        assert_eq!(format_default_value(&Value::Bool(true)), "true");
        assert_eq!(format_default_value(&Value::Bool(false)), "false");
        assert_eq!(
            format_default_value(&Value::Number(serde_json::Number::from(42))),
            "42"
        );
        assert_eq!(
            format_default_value(&Value::String("test".to_string())),
            "test"
        );
    }
}
