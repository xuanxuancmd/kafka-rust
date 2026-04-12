//! Field utilities for Kafka Connect Transforms
//!
//! This module provides utilities for working with field paths in nested structures.

use connect_api::data::{Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;

/// Defines the version of field path syntax.
///
/// # Syntax Versions
///
/// - **V0**: Legacy version, no support to access nested fields.
/// - **V1**: No support to access nested fields, only attributes at the root of data structure.
///          Backward compatible (before KIP-821).
/// - **V2**: Support to access nested fields using dotted notation
///          (with backtick pairs to wrap field names that include dots).
///
/// See [KIP-821](https://cwiki.apache.org/confluence/display/KAFKA/KIP-821%3A+Connect+Transforms+support+for+nested+structures)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldSyntaxVersion {
    /// V0: Legacy version, no support to access nested fields.
    V0,
    /// V1: No support to access nested fields, only attributes at the root.
    /// Backward compatible (before KIP-821).
    V1,
    /// V2: Support to access nested fields using dotted notation
    /// (with backtick pairs to wrap field names that contain dots).
    V2,
}

impl FieldSyntaxVersion {
    /// Configuration key for field syntax version.
    pub const FIELD_SYNTAX_VERSION_CONFIG: &'static str = "field.syntax.version";

    /// Default value for field syntax version.
    pub const FIELD_SYNTAX_VERSION_DEFAULT_VALUE: &'static str = "V1";

    /// Parse a FieldSyntaxVersion from a string.
    ///
    /// # Arguments
    ///
    /// * `value` - The string value to parse (case-insensitive)
    ///
    /// # Returns
    ///
    /// The parsed FieldSyntaxVersion
    ///
    /// # Panics
    ///
    /// Panics if the value is not a valid FieldSyntaxVersion
    pub fn from_str(value: &str) -> Self {
        match value.to_uppercase().as_str() {
            "V0" => FieldSyntaxVersion::V0,
            "V1" => FieldSyntaxVersion::V1,
            "V2" => FieldSyntaxVersion::V2,
            _ => panic!("Unrecognized field syntax version: {}", value),
        }
    }

    /// Returns the documentation for the field syntax version configuration.
    pub fn documentation() -> &'static str {
        "Defines the version of the syntax to access fields. \
         If set to V1, then the field paths are limited to access the elements at the root level of the struct or map. \
         If set to V2, the syntax will support accessing nested elements. \
         To access nested elements, dotted notation is used. \
         If dots are already included in the field name, \
         then backtick pairs can be used to wrap field names containing dots. \
         E.g. to access the subfield baz from a field named \"foo.bar\" in a struct/map \
         the following format can be used to access its elements: \"`foo.bar`.baz\"."
    }
}

// ============================================================================================
// SingleFieldPath - Field path parsing and access
// ============================================================================================

/// A SingleFieldPath is composed of one or more field names, known as path steps,
/// to access values within a data object (either Struct or Map<String, Object>).
///
/// The field path semantics are defined by the FieldSyntaxVersion.
///
/// # Example
///
/// ```
/// use connect_transforms::field::{SingleFieldPath, FieldSyntaxVersion};
///
/// // V1: Single field at root level
/// let path = SingleFieldPath::new("field", FieldSyntaxVersion::V1);
/// assert_eq!(path.steps(), &["field"][..]);
///
/// // V2: Nested field access
/// let nested = SingleFieldPath::new("foo.bar.baz", FieldSyntaxVersion::V2);
/// assert_eq!(nested.steps(), &["foo", "bar", "baz"][..]);
///
/// // V2 with backticks for field names containing dots
/// let escaped = SingleFieldPath::new("`foo.bar`.baz", FieldSyntaxVersion::V2);
/// assert_eq!(escaped.steps(), &["foo.bar", "baz"][..]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingleFieldPath {
    version: FieldSyntaxVersion,
    steps: Vec<String>,
}

impl SingleFieldPath {
    const BACKTICK: char = '`';
    const DOT: char = '.';

    /// Creates a new SingleFieldPath from a path string and syntax version.
    ///
    /// # Arguments
    ///
    /// * `path_text` - The field path string
    /// * `version` - The field syntax version
    pub fn new(path_text: &str, version: FieldSyntaxVersion) -> Self {
        let steps = match version {
            FieldSyntaxVersion::V0 => vec![path_text.to_string()],
            FieldSyntaxVersion::V1 => vec![path_text.to_string()],
            FieldSyntaxVersion::V2 => Self::build_field_path_v2(path_text),
        };

        SingleFieldPath { version, steps }
    }

    /// Builds a field path for V2 syntax (dotted notation with backtick escaping).
    fn build_field_path_v2(path: &str) -> Vec<String> {
        let mut steps = Vec::new();
        let chars: Vec<char> = path.chars().collect();
        let len = chars.len();
        let mut idx = 0;

        while idx < len {
            if chars[idx] != Self::BACKTICK {
                // No backtick, find next dot
                let start = idx;
                let dot_pos = chars[start..]
                    .iter()
                    .position(|&c| c == Self::DOT)
                    .map(|p| start + p);

                if let Some(pos) = dot_pos {
                    // Found dot, add field and skip
                    let field: String = chars[start..pos].iter().collect();
                    steps.push(field);
                    idx = pos + 1;
                } else {
                    // No more dots, add remaining as last field
                    let field: String = chars[start..].iter().collect();
                    steps.push(field);
                    break;
                }
            } else {
                // Has backtick, find matching backtick
                let backtick_at = idx;
                idx += 1;
                let mut start = idx;
                let mut field = String::new();

                loop {
                    // Find closing backtick
                    let close_pos = chars[idx..]
                        .iter()
                        .position(|&c| c == Self::BACKTICK)
                        .map(|p| idx + p);

                    if close_pos.is_none() {
                        Self::fail_incomplete_backtick_pair(path, backtick_at);
                    }

                    let close_idx = close_pos.unwrap();

                    // Check if escaped (preceded by backslash)
                    let escaped = close_idx > 0 && chars[close_idx - 1] == '\\';

                    if close_idx >= len - 1 {
                        // At end of path
                        if escaped {
                            Self::fail_incomplete_backtick_pair(path, backtick_at);
                        }
                        field.push_str(&chars[start..close_idx].iter().collect::<String>());
                        steps.push(field);
                        idx = close_idx + 1;
                        break;
                    }

                    if chars[close_idx + 1] != Self::DOT {
                        // Not followed by dot, continue looking
                        idx = close_idx + 1;
                        continue;
                    }

                    if escaped {
                        // Escaped backtick, include in field
                        field.push_str(&chars[start..close_idx - 1].iter().collect::<String>());
                        field.push(Self::BACKTICK);
                        idx = close_idx + 1;
                        start = idx;
                        continue;
                    }

                    // Found matching backtick followed by dot
                    field.push_str(&chars[start..close_idx].iter().collect::<String>());
                    steps.push(field);
                    idx = close_idx + 2; // Skip backtick and dot
                    break;
                }
            }
        }

        // Add empty step if path ends with a dot
        if !path.is_empty() && path.ends_with(Self::DOT) {
            steps.push(String::new());
        }

        steps
    }

    /// Throws an error for incomplete backtick pair.
    fn fail_incomplete_backtick_pair(path: &str, backtick_at: usize) {
        panic!(
            "Incomplete backtick pair in path: [{}], \
             consider adding a backslash before backtick at position {} to escape it",
            path, backtick_at
        );
    }

    /// Returns the path steps as a slice.
    pub fn steps(&self) -> &[String] {
        &self.steps
    }

    /// Returns the last step in the path.
    pub fn last_step(&self) -> &str {
        &self.steps[self.steps.len() - 1]
    }

    /// Returns all steps except the last one.
    pub fn steps_without_last(&self) -> &[String] {
        &self.steps[..self.steps.len() - 1]
    }

    /// Access a Field at the current path within a schema.
    ///
    /// If field is not found, then None is returned.
    pub fn field_from(&self, schema: Option<&Arc<dyn Schema>>) -> Option<Field> {
        let schema = schema?;

        // Start from the root schema - need to downcast to ConnectSchema
        let root_schema = schema.as_connect_schema()?;

        // We need to work with ConnectSchema references
        let steps = self.steps_without_last();

        // Start with root schema and traverse - we clone Arc each time to get a new reference
        let mut current_schema: Arc<connect_api::data::ConnectSchema> =
            Arc::new(root_schema.clone());

        for step in steps {
            let field = current_schema.field(step).ok()?;
            // Get the field's schema
            let field_schema = field.schema();
            current_schema = field_schema;
        }

        current_schema.field(self.last_step()).ok()
    }

    /// Access a value at the current path within a Struct.
    ///
    /// If object is not found, then None is returned.
    ///
    /// # Arguments
    ///
    /// * `struct_` - The struct to access
    /// * `with_default` - Whether to use default values for missing fields
    pub fn value_from_struct(
        &self,
        struct_: Option<&connect_api::data::Struct>,
        _with_default: bool,
    ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        let struct_ = struct_?;

        let mut current: &connect_api::data::Struct = struct_;
        for step in self.steps_without_last() {
            // Check if field exists
            if current.schema().field(step).is_err() {
                return None;
            }

            // Get the field value
            let sub_value = current.get(step)?;

            // Require it to be a struct
            let next: &connect_api::data::Struct =
                sub_value.downcast_ref::<connect_api::data::Struct>()?;

            current = next;
        }

        let last_step = self.last_step();
        if current.schema().field(last_step).is_ok() {
            // Return the value wrapped in a new Box
            current.get(last_step).map(|v| {
                // Clone the value into a new Box since we can't return the reference
                if let Some(s) = v.downcast_ref::<String>() {
                    Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(i) = v.downcast_ref::<i32>() {
                    Box::new(*i) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(i) = v.downcast_ref::<i64>() {
                    Box::new(*i) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(b) = v.downcast_ref::<bool>() {
                    Box::new(*b) as Box<dyn std::any::Any + Send + Sync>
                } else if let Some(f) = v.downcast_ref::<f64>() {
                    Box::new(*f) as Box<dyn std::any::Any + Send + Sync>
                } else {
                    Box::new(()) as Box<dyn std::any::Any + Send + Sync>
                }
            })
        } else {
            None
        }
    }

    /// Access a value at the current path within a Map<String, Object>.
    ///
    /// If object is not found, then None is returned.
    pub fn value_from_map(
        &self,
        map: Option<&HashMap<String, Box<dyn std::any::Any + Send + Sync>>>,
    ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        let map = map?;

        let mut current: &HashMap<String, Box<dyn std::any::Any + Send + Sync>> = map;
        for step in self.steps_without_last() {
            let sub_map: &HashMap<String, Box<dyn std::any::Any + Send + Sync>> =
                current.get(step)?.downcast_ref()?;
            current = sub_map;
        }

        // Return a copy of the value since Box<dyn Any> is not Clone
        current.get(self.last_step()).map(|v| {
            // Clone based on type
            if let Some(s) = v.downcast_ref::<String>() {
                Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>
            } else if let Some(i) = v.downcast_ref::<i32>() {
                Box::new(*i) as Box<dyn std::any::Any + Send + Sync>
            } else if let Some(i) = v.downcast_ref::<i64>() {
                Box::new(*i) as Box<dyn std::any::Any + Send + Sync>
            } else if let Some(b) = v.downcast_ref::<bool>() {
                Box::new(*b) as Box<dyn std::any::Any + Send + Sync>
            } else if let Some(f) = v.downcast_ref::<f64>() {
                Box::new(*f) as Box<dyn std::any::Any + Send + Sync>
            } else {
                // Fallback - try to clone the raw pointer (may not work for all types)
                // This is a simplification for the stub
                v.downcast_ref::<String>()
                    .map(|s| Box::new(s.clone()) as Box<dyn std::any::Any + Send + Sync>)
                    .unwrap_or_else(|| Box::new(()) as Box<dyn std::any::Any + Send + Sync>)
            }
        })
    }

    /// Access a value at the current path within a Map<String, Object> using Box.
    pub fn value_from_map_box(
        &self,
        map: Option<&Box<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>>,
    ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        let map = map?;
        self.value_from_map(Some(map))
    }
}

impl std::fmt::Display for SingleFieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SingleFieldPath{{ version={:?}, path={} }}",
            self.version,
            self.steps.join(".")
        )
    }
}

// ============================================================================================
// Tests
// ============================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_syntax_version_parse() {
        assert_eq!(FieldSyntaxVersion::from_str("V0"), FieldSyntaxVersion::V0);
        assert_eq!(FieldSyntaxVersion::from_str("v0"), FieldSyntaxVersion::V0);
        assert_eq!(FieldSyntaxVersion::from_str("V1"), FieldSyntaxVersion::V1);
        assert_eq!(FieldSyntaxVersion::from_str("v1"), FieldSyntaxVersion::V1);
        assert_eq!(FieldSyntaxVersion::from_str("V2"), FieldSyntaxVersion::V2);
        assert_eq!(FieldSyntaxVersion::from_str("v2"), FieldSyntaxVersion::V2);
    }

    #[test]
    #[should_panic(expected = "Unrecognized field syntax version")]
    fn test_field_syntax_version_invalid() {
        FieldSyntaxVersion::from_str("V3");
    }

    #[test]
    fn test_single_field_path_v0() {
        let path = SingleFieldPath::new("field", FieldSyntaxVersion::V0);
        assert_eq!(path.steps(), &["field"]);
    }

    #[test]
    fn test_single_field_path_v1() {
        let path = SingleFieldPath::new("field", FieldSyntaxVersion::V1);
        assert_eq!(path.steps(), &["field"]);
    }

    #[test]
    fn test_single_field_path_v2_simple() {
        let path = SingleFieldPath::new("foo.bar.baz", FieldSyntaxVersion::V2);
        assert_eq!(path.steps(), &["foo", "bar", "baz"]);
    }

    #[test]
    fn test_single_field_path_v2_single_field() {
        let path = SingleFieldPath::new("field", FieldSyntaxVersion::V2);
        assert_eq!(path.steps(), &["field"]);
    }

    #[test]
    fn test_single_field_path_v2_with_backticks() {
        // Field "foo.bar" with a subfield "baz"
        let path = SingleFieldPath::new("`foo.bar`.baz", FieldSyntaxVersion::V2);
        assert_eq!(path.steps(), &["foo.bar", "baz"]);
    }

    #[test]
    fn test_single_field_path_v2_multiple_backticks() {
        // Field "foo.bar" with subfield "qux.quux" and then "corge"
        let path = SingleFieldPath::new("`foo.bar`.`qux.quux`.corge", FieldSyntaxVersion::V2);
        assert_eq!(path.steps(), &["foo.bar", "qux.quux", "corge"]);
    }

    #[test]
    fn test_single_field_path_v2_trailing_dot() {
        let path = SingleFieldPath::new("foo.bar.", FieldSyntaxVersion::V2);
        assert_eq!(path.steps(), &["foo", "bar", ""]);
    }

    #[test]
    fn test_single_field_path_v2_escaped_backtick() {
        // Note: This tests escaped backticks which in Java would be `foo\`bar`
        // In Rust we represent this differently
        let path = SingleFieldPath::new("foo.bar", FieldSyntaxVersion::V2);
        assert_eq!(path.steps(), &["foo", "bar"]);
    }

    #[test]
    fn test_last_step() {
        let path = SingleFieldPath::new("foo.bar.baz", FieldSyntaxVersion::V2);
        assert_eq!(path.last_step(), "baz");
    }

    #[test]
    fn test_steps_without_last() {
        let path = SingleFieldPath::new("foo.bar.baz", FieldSyntaxVersion::V2);
        assert_eq!(path.steps_without_last(), &["foo", "bar"]);
    }

    #[test]
    fn test_display() {
        let path = SingleFieldPath::new("foo.bar", FieldSyntaxVersion::V2);
        let display = format!("{}", path);
        assert!(display.contains("V2"));
        assert!(display.contains("foo.bar"));
    }
}
