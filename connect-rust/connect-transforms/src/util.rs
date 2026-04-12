//! Utility modules for Kafka Connect Transforms
//!
//! This module provides utility classes and functions used by transform implementations.

use connect_api::config::ConfigDef;
use connect_api::data::{ConnectSchema, Field, Schema, SchemaAndValue, SchemaBuilder};
use regex::Regex;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================================
// SimpleConfig - A simple configuration wrapper
// ============================================================================================

/// SimpleConfig is a barebones concrete implementation of Config.
///
/// It provides a simple way to create configuration objects from a ConfigDef
/// and a map of original values.
#[derive(Debug, Clone)]
pub struct SimpleConfig {
    values: HashMap<String, SchemaAndValue>,
}

impl SimpleConfig {
    /// Creates a new SimpleConfig from a ConfigDef and original values.
    ///
    /// # Arguments
    ///
    /// * `config_def` - The configuration definition
    /// * `originals` - A map of original configuration values (typically from connector config)
    pub fn new(_config_def: &ConfigDef, originals: &HashMap<String, SchemaAndValue>) -> Self {
        let mut values = HashMap::new();
        for (key, value) in originals {
            values.insert(key.clone(), value.clone());
        }
        SimpleConfig { values }
    }

    /// Creates a new SimpleConfig from a map of values.
    pub fn with_values(values: HashMap<String, SchemaAndValue>) -> Self {
        SimpleConfig { values }
    }

    /// Gets a string configuration value.
    pub fn get_string(&self, key: &str) -> Option<String> {
        self.values.get(key).and_then(|v| {
            v.value()
                .and_then(|val| val.downcast_ref::<String>().map(|s| s.clone()))
        })
    }

    /// Gets a string configuration value with a default.
    pub fn get_string_or(&self, key: &str, default: &str) -> String {
        self.get_string(key).unwrap_or_else(|| default.to_string())
    }

    /// Gets an i32 configuration value.
    pub fn get_int(&self, key: &str) -> Option<i32> {
        self.values
            .get(key)
            .and_then(|v| v.value().and_then(|val| val.downcast_ref::<i32>().copied()))
    }

    /// Gets an i32 configuration value with a default.
    pub fn get_int_or(&self, key: &str, default: i32) -> i32 {
        self.get_int(key).unwrap_or(default)
    }

    /// Gets an i64 configuration value.
    pub fn get_long(&self, key: &str) -> Option<i64> {
        self.values
            .get(key)
            .and_then(|v| v.value().and_then(|val| val.downcast_ref::<i64>().copied()))
    }

    /// Gets an i64 configuration value with a default.
    pub fn get_long_or(&self, key: &str, default: i64) -> i64 {
        self.get_long(key).unwrap_or(default)
    }

    /// Gets a boolean configuration value.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.values.get(key).and_then(|v| {
            v.value()
                .and_then(|val| val.downcast_ref::<bool>().copied())
        })
    }

    /// Gets a boolean configuration value with a default.
    pub fn get_bool_or(&self, key: &str, default: bool) -> bool {
        self.get_bool(key).unwrap_or(default)
    }

    /// Gets a list configuration value.
    pub fn get_list(&self, key: &str) -> Option<Vec<String>> {
        self.values.get(key).and_then(|v| {
            v.value()
                .and_then(|val| val.downcast_ref::<Vec<String>>().map(|v| v.clone()))
        })
    }

    /// Gets a list configuration value with a default.
    pub fn get_list_or(&self, key: &str, default: Vec<String>) -> Vec<String> {
        self.get_list(key).unwrap_or(default)
    }

    /// Checks if a configuration key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    /// Gets the underlying values map.
    pub fn values(&self) -> &HashMap<String, SchemaAndValue> {
        &self.values
    }
}

// ============================================================================================
// SchemaUtil - Schema utility functions
// ============================================================================================

/// SchemaUtil provides utility functions for working with Connect schemas.
pub struct SchemaUtil;

impl SchemaUtil {
    /// Copies the basic properties from a source schema to a SchemaBuilder.
    ///
    /// This includes:
    /// - Schema name
    /// - Schema version
    /// - Documentation
    /// - Parameters
    ///
    /// # Arguments
    ///
    /// * `source` - The source schema to copy from
    /// * `builder` - The builder to copy properties to
    ///
    /// # Returns
    ///
    /// The modified SchemaBuilder
    pub fn copy_schema_basics(source: &dyn Schema, mut builder: SchemaBuilder) -> SchemaBuilder {
        if let Some(name) = source.name() {
            builder = builder.name(name.to_string());
        }

        if let Some(version) = source.version() {
            builder = builder.version(version);
        }

        if let Some(doc) = source.doc() {
            builder = builder.doc(doc.to_string());
        }

        if let Some(params) = source.parameters() {
            builder = builder.parameters(params.clone());
        }

        builder
    }

    /// Copies the schema type and basic properties from a source schema to a new SchemaBuilder.
    ///
    /// This handles special cases for ARRAY, MAP, and STRUCT types.
    /// Note: For ARRAY and MAP types, this creates a builder but the nested schemas
    /// need to be handled separately as they require type coercion.
    ///
    /// # Arguments
    ///
    /// * `source` - The source schema to copy from
    ///
    /// # Returns
    ///
    /// A new SchemaBuilder with the copied properties
    pub fn copy_schema_basics_new(source: &dyn Schema) -> SchemaBuilder {
        let type_ = source.type_();

        let builder = match type_ {
            // For ARRAY and MAP, we need to handle nested schemas specially
            // The caller should use copy_schema_basics then add nested schemas manually
            connect_api::data::Type::Array | connect_api::data::Type::Map => {
                // Create a placeholder that will be replaced by the caller
                // with the correct nested schemas
                SchemaBuilder::struct_()
            }
            connect_api::data::Type::Struct => SchemaBuilder::struct_(),
            connect_api::data::Type::Int8 => SchemaBuilder::int8(),
            connect_api::data::Type::Int16 => SchemaBuilder::int16(),
            connect_api::data::Type::Int32 => SchemaBuilder::int32(),
            connect_api::data::Type::Int64 => SchemaBuilder::int64(),
            connect_api::data::Type::Float32 => SchemaBuilder::float32(),
            connect_api::data::Type::Float64 => SchemaBuilder::float64(),
            connect_api::data::Type::Boolean => SchemaBuilder::boolean(),
            connect_api::data::Type::String => SchemaBuilder::string(),
            connect_api::data::Type::Bytes => SchemaBuilder::bytes(),
        };

        Self::copy_schema_basics(source, builder)
    }

    /// Copies a schema with its nested schemas (for ARRAY and MAP types).
    ///
    /// # Arguments
    ///
    /// * `source` - The source schema to copy from
    /// * `value_schema` - The value schema for ARRAY type
    /// * `key_schema` - The key schema for MAP type (optional)
    ///
    /// # Returns
    ///
    /// A new SchemaBuilder with the copied properties including nested schemas
    pub fn copy_schema_with_nested(
        source: &dyn Schema,
        value_schema: Arc<connect_api::data::ConnectSchema>,
        key_schema: Option<Arc<connect_api::data::ConnectSchema>>,
    ) -> SchemaBuilder {
        let type_ = source.type_();

        let builder = match type_ {
            connect_api::data::Type::Array => SchemaBuilder::array(value_schema),
            connect_api::data::Type::Map => {
                let key = key_schema.unwrap_or_else(|| {
                    Arc::new(connect_api::data::ConnectSchema::new(
                        connect_api::data::Type::String,
                    ))
                });
                SchemaBuilder::map(key, value_schema)
            }
            _ => Self::copy_schema_basics_new(source),
        };

        Self::copy_schema_basics(source, builder)
    }

    /// Adds a field to a SchemaBuilder for a struct schema.
    pub fn add_field(builder: SchemaBuilder, field: &Field) -> SchemaBuilder {
        builder.field(field.name().to_string(), field.schema())
    }
}

// ============================================================================================
// Requirements - Validation utilities
// ============================================================================================

/// Requirements provides validation functions for data types.
///
/// Similar to Java's Requirements class, it throws DataException
/// when validation fails.
pub struct Requirements;

impl Requirements {
    /// Requires that a schema is present.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema to validate
    /// * `purpose` - Description of the purpose for error messages
    ///
    /// # Panics
    ///
    /// Panics with DataException if schema is None
    pub fn require_schema(schema: Option<&Arc<dyn Schema>>, purpose: &str) {
        if schema.is_none() {
            panic!("Schema required for [{}]", purpose);
        }
    }

    /// Requires that a value is a Map.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to check
    /// * `purpose` - Description of the purpose for error messages
    ///
    /// # Returns
    ///
    /// The value as a reference to a HashMap (borrowed from the Arc)
    ///
    /// # Panics
    ///
    /// Panics with DataException if value is not a Map
    pub fn require_map<'a>(
        value: Option<&'a Arc<dyn Any + Send + Sync>>,
        purpose: &str,
    ) -> &'a HashMap<String, Box<dyn Any + Send + Sync>> {
        let val = match value {
            Some(v) => v.as_ref(),
            None => {
                panic!(
                    "Only Map objects supported in absence of schema for [{}], found: null",
                    purpose
                );
            }
        };

        val.downcast_ref::<HashMap<String, Box<dyn Any + Send + Sync>>>()
            .unwrap_or_else(|| {
                panic!(
                    "Only Map objects supported in absence of schema for [{}], found: {}",
                    purpose,
                    Self::null_safe_class_name(Some(val))
                )
            })
    }

    /// Requires that a value is a Map or null.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to check
    /// * `purpose` - Description of the purpose for error messages
    ///
    /// # Returns
    ///
    /// The value as a reference to a HashMap, or None if value is null
    pub fn require_map_or_null<'a>(
        value: Option<&'a Arc<dyn Any + Send + Sync>>,
        purpose: &str,
    ) -> Option<&'a HashMap<String, Box<dyn Any + Send + Sync>>> {
        if value.is_none() {
            return None;
        }
        Some(Self::require_map(value, purpose))
    }

    /// Requires that a value is a Struct.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to check
    /// * `purpose` - Description of the purpose for error messages
    ///
    /// # Returns
    ///
    /// The value as a Struct
    ///
    /// # Panics
    ///
    /// Panics with DataException if value is not a Struct
    pub fn require_struct(
        value: Option<&Arc<dyn Any + Send + Sync>>,
        purpose: &str,
    ) -> connect_api::data::Struct {
        let val = match value {
            Some(v) => v.as_ref(),
            None => {
                panic!(
                    "Only Struct objects supported for [{}], found: null",
                    purpose
                );
            }
        };

        val.downcast_ref::<connect_api::data::Struct>()
            .map(|s| s.clone())
            .unwrap_or_else(|| {
                panic!(
                    "Only Struct objects supported for [{}], found: {}",
                    purpose,
                    Self::null_safe_class_name(Some(val))
                )
            })
    }

    /// Requires that a value is a Struct or null.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to check
    /// * `purpose` - Description of the purpose for error messages
    ///
    /// # Returns
    ///
    /// The value as a Struct, or None if value is null
    pub fn require_struct_or_null(
        value: Option<&Arc<dyn Any + Send + Sync>>,
        purpose: &str,
    ) -> Option<connect_api::data::Struct> {
        if value.is_none() {
            return None;
        }
        Some(Self::require_struct(value, purpose))
    }

    /// Returns a class name that is null-safe.
    fn null_safe_class_name(value: Option<&dyn Any>) -> String {
        match value {
            None => "null".to_string(),
            Some(v) => {
                let type_name = std::any::type_name_of_val(v);
                type_name.to_string()
            }
        }
    }
}

// ============================================================================================
// Validators - Configuration validators
// ============================================================================================

/// RegexValidator validates that a configuration value is a valid regular expression.
pub struct RegexValidator;

impl RegexValidator {
    /// Creates a new RegexValidator.
    pub fn new() -> Self {
        RegexValidator
    }

    /// Validates that the given value is a valid regex pattern.
    ///
    /// # Arguments
    ///
    /// * `name` - The configuration key name (for error messages)
    /// * `value` - The value to validate
    ///
    /// # Panics
    ///
    /// Panics with ConfigException if the pattern is invalid
    pub fn ensure_valid(&self, name: &str, value: &str) {
        match Regex::new(value) {
            Ok(_) => {}
            Err(e) => {
                panic!("Invalid regex for {}: {}", name, e);
            }
        }
    }

    /// Returns a description of the validator.
    pub fn to_string(&self) -> String {
        "valid regex".to_string()
    }
}

impl Default for RegexValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// NonEmptyListValidator validates that a configuration value is a non-empty list.
pub struct NonEmptyListValidator;

impl NonEmptyListValidator {
    /// Creates a new NonEmptyListValidator.
    pub fn new() -> Self {
        NonEmptyListValidator
    }

    /// Validates that the given value is a non-empty list.
    ///
    /// # Arguments
    ///
    /// * `name` - The configuration key name (for error messages)
    /// * `value` - The value to validate
    ///
    /// # Panics
    ///
    /// Panics with ConfigException if the list is empty or null
    pub fn ensure_valid(&self, name: &str, value: Option<&Vec<String>>) {
        match value {
            Some(list) if !list.is_empty() => {}
            _ => {
                panic!("Value for {} must be a non-empty list", name);
            }
        }
    }

    /// Returns a description of the validator.
    pub fn to_string(&self) -> String {
        "non-empty list".to_string()
    }
}

impl Default for NonEmptyListValidator {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================================
// Tests
// ============================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regex_validator_valid() {
        let validator = RegexValidator::new();
        validator.ensure_valid("pattern", r"\d{3}-\d{4}");
    }

    #[test]
    #[should_panic(expected = "Invalid regex")]
    fn test_regex_validator_invalid() {
        let validator = RegexValidator::new();
        validator.ensure_valid("pattern", r"[invalid");
    }

    #[test]
    fn test_non_empty_list_validator_valid() {
        let validator = NonEmptyListValidator::new();
        let list = Some(vec!["a".to_string(), "b".to_string()]);
        validator.ensure_valid("list", list.as_ref());
    }

    #[test]
    #[should_panic(expected = "non-empty list")]
    fn test_non_empty_list_validator_empty() {
        let validator = NonEmptyListValidator::new();
        let list = Some(vec![]);
        validator.ensure_valid("list", list.as_ref());
    }

    #[test]
    #[should_panic(expected = "non-empty list")]
    fn test_non_empty_list_validator_null() {
        let validator = NonEmptyListValidator::new();
        validator.ensure_valid("list", None);
    }

    #[test]
    fn test_simple_config_get_string() {
        let mut values = HashMap::new();
        values.insert(
            "test.key".to_string(),
            SchemaAndValue::new(None, Some(Box::new("test_value".to_string()))),
        );

        let config = SimpleConfig::with_values(values);

        assert_eq!(
            config.get_string("test.key"),
            Some("test_value".to_string())
        );
        assert_eq!(
            config.get_string_or("test.key", "default"),
            "test_value".to_string()
        );
        assert_eq!(
            config.get_string_or("missing.key", "default"),
            "default".to_string()
        );
    }

    #[test]
    fn test_simple_config_get_bool() {
        let mut values = HashMap::new();
        values.insert(
            "test.bool".to_string(),
            SchemaAndValue::new(None, Some(Box::new(true))),
        );

        let config = SimpleConfig::with_values(values);

        assert_eq!(config.get_bool("test.bool"), Some(true));
        assert_eq!(config.get_bool_or("test.bool", false), true);
        assert_eq!(config.get_bool_or("missing.bool", false), false);
    }

    #[test]
    fn test_schema_util_copy_schema_basics() {
        use connect_api::data::{ConnectSchema, Type};

        let schema = Arc::new(
            ConnectSchema::new(Type::String)
                .with_name("test.name".to_string())
                .with_version(5)
                .with_doc("test doc".to_string()),
        );

        let builder = SchemaBuilder::string();
        let result = SchemaUtil::copy_schema_basics(schema.as_ref(), builder);

        // Build the schema and verify properties
        let built = result.build().unwrap();
        assert_eq!(built.name(), Some("test.name"));
        assert_eq!(built.version(), Some(5));
        assert_eq!(built.doc(), Some("test doc"));
    }

    #[test]
    fn test_requirements_require_schema() {
        let schema: Option<Arc<dyn Schema>> = None;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Requirements::require_schema(schema.as_ref(), "test purpose");
        }));
        assert!(result.is_err());
    }

    #[test]
    fn test_requirements_require_schema_present() {
        use connect_api::data::Type;
        let schema: Option<Arc<dyn Schema>> = Some(Arc::new(ConnectSchema::new(Type::String)));
        Requirements::require_schema(schema.as_ref(), "test purpose"); // Should not panic
    }
}
