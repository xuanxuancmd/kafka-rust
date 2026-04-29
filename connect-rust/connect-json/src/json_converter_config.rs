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

//! Configuration options for JsonConverter instances.
//!
//! Corresponds to: `org.apache.kafka.connect.json.JsonConverterConfig` in Java Kafka Connect.
//!
//! Source: connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverterConfig.java

use crate::decimal_format::DecimalFormat;
use common_trait::config::{
    CaseInsensitiveValidString, ConfigDefImportance, ConfigDefType, ConfigDefWidth, ConfigKeyDef,
};
use connect_api::storage::ConverterConfig;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

// ============================================================================
// Configuration Constants (matching Java 1:1)
// ============================================================================

/// Configuration name for enabling schemas.
/// Corresponds to: `JsonConverterConfig.SCHEMAS_ENABLE_CONFIG`
pub const SCHEMAS_ENABLE_CONFIG: &str = "schemas.enable";

/// Default value for schemas.enable configuration.
/// Corresponds to: `JsonConverterConfig.SCHEMAS_ENABLE_DEFAULT`
pub const SCHEMAS_ENABLE_DEFAULT: bool = true;

/// Documentation for schemas.enable configuration.
/// Corresponds to: `JsonConverterConfig.SCHEMAS_ENABLE_DOC`
const SCHEMAS_ENABLE_DOC: &str = "Include schemas within each of the serialized values and keys.";

/// Display name for schemas.enable configuration.
/// Corresponds to: `JsonConverterConfig.SCHEMAS_ENABLE_DISPLAY`
const SCHEMAS_ENABLE_DISPLAY: &str = "Enable Schemas";

/// Configuration name for schema content.
/// Corresponds to: `JsonConverterConfig.SCHEMA_CONTENT_CONFIG`
pub const SCHEMA_CONTENT_CONFIG: &str = "schema.content";

/// Default value for schema.content configuration (null).
/// Corresponds to: `JsonConverterConfig.SCHEMA_CONTENT_DEFAULT`
pub const SCHEMA_CONTENT_DEFAULT: Option<&str> = None;

/// Documentation for schema.content configuration.
/// Corresponds to: `JsonConverterConfig.SCHEMA_CONTENT_DOC`
const SCHEMA_CONTENT_DOC: &str = "When set, this is used as the schema for all messages, and the schemas within each of the message will be ignored. Otherwise, the schema will be included in the content of each message. This configuration applies only 'schemas.enable' is true, and it exclusively affects the sink connector.";

/// Display name for schema.content configuration.
/// Corresponds to: `JsonConverterConfig.SCHEMA_CONTENT_DISPLAY`
const SCHEMA_CONTENT_DISPLAY: &str = "Schema Content";

/// Configuration name for schema cache size.
/// Corresponds to: `JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG`
pub const SCHEMAS_CACHE_SIZE_CONFIG: &str = "schemas.cache.size";

/// Default value for schemas.cache.size configuration.
/// Corresponds to: `JsonConverterConfig.SCHEMAS_CACHE_SIZE_DEFAULT`
pub const SCHEMAS_CACHE_SIZE_DEFAULT: i32 = 1000;

/// Documentation for schemas.cache.size configuration.
/// Corresponds to: `JsonConverterConfig.SCHEMAS_CACHE_SIZE_DOC`
const SCHEMAS_CACHE_SIZE_DOC: &str =
    "The maximum number of schemas that can be cached in this converter instance.";

/// Display name for schemas.cache.size configuration.
/// Corresponds to: `JsonConverterConfig.SCHEMAS_CACHE_SIZE_DISPLAY`
const SCHEMAS_CACHE_SIZE_DISPLAY: &str = "Schema Cache Size";

/// Configuration name for decimal format.
/// Corresponds to: `JsonConverterConfig.DECIMAL_FORMAT_CONFIG`
pub const DECIMAL_FORMAT_CONFIG: &str = "decimal.format";

/// Default value for decimal.format configuration.
/// Corresponds to: `JsonConverterConfig.DECIMAL_FORMAT_DEFAULT`
pub const DECIMAL_FORMAT_DEFAULT: &str = "BASE64";

/// Documentation for decimal.format configuration.
/// Corresponds to: `JsonConverterConfig.DECIMAL_FORMAT_DOC`
const DECIMAL_FORMAT_DOC: &str = "Controls which format this converter will serialize decimals in. This value is case insensitive and can be either 'BASE64' (default) or 'NUMERIC'";

/// Display name for decimal.format configuration.
/// Corresponds to: `JsonConverterConfig.DECIMAL_FORMAT_DISPLAY`
const DECIMAL_FORMAT_DISPLAY: &str = "Decimal Format";

/// Configuration name for replace null with default.
/// Corresponds to: `JsonConverterConfig.REPLACE_NULL_WITH_DEFAULT_CONFIG`
pub const REPLACE_NULL_WITH_DEFAULT_CONFIG: &str = "replace.null.with.default";

/// Default value for replace.null.with.default configuration.
/// Corresponds to: `JsonConverterConfig.REPLACE_NULL_WITH_DEFAULT_DEFAULT`
pub const REPLACE_NULL_WITH_DEFAULT_DEFAULT: bool = true;

/// Documentation for replace.null.with.default configuration.
/// Corresponds to: `JsonConverterConfig.REPLACE_NULL_WITH_DEFAULT_DOC`
const REPLACE_NULL_WITH_DEFAULT_DOC: &str = "Whether to replace fields that have a default value and that are null to the default value. When set to true, the default value is used, otherwise null is used.";

/// Display name for replace.null.with.default configuration.
/// Corresponds to: `JsonConverterConfig.REPLACE_NULL_WITH_DEFAULT_DISPLAY`
const REPLACE_NULL_WITH_DEFAULT_DISPLAY: &str = "Replace null with default";

// ============================================================================
// JsonConverterConfig Struct
// ============================================================================

/// Configuration options for JsonConverter instances.
///
/// This struct holds the parsed configuration values and provides getter methods
/// to access them.
///
/// Corresponds to: `org.apache.kafka.connect.json.JsonConverterConfig`
pub struct JsonConverterConfig {
    /// The base ConverterConfig.
    converter_config: ConverterConfig,
    /// Cached value: whether schemas are enabled.
    schemas_enabled: bool,
    /// Cached value: schema cache size.
    schema_cache_size: i32,
    /// Cached value: decimal format for serialization.
    decimal_format: DecimalFormat,
    /// Cached value: whether to replace null with default.
    replace_null_with_default: bool,
    /// Cached value: schema content as bytes (null if not provided).
    schema_content: Option<Vec<u8>>,
}

impl JsonConverterConfig {
    /// Creates a new JsonConverterConfig from the given configuration map.
    ///
    /// This constructor parses the configuration values and caches them
    /// for efficient access.
    ///
    /// # Arguments
    ///
    /// * `props` - A HashMap containing configuration key-value pairs
    ///
    /// # Returns
    ///
    /// A new JsonConverterConfig instance with parsed values.
    ///
    /// Corresponds to: `JsonConverterConfig(Map<String, ?> props)` in Java
    pub fn new(props: HashMap<String, Value>) -> Self {
        // Parse schemas.enable
        let schemas_enabled = props
            .get(SCHEMAS_ENABLE_CONFIG)
            .and_then(|v| v.as_bool())
            .unwrap_or(SCHEMAS_ENABLE_DEFAULT);

        // Parse schemas.cache.size
        let schema_cache_size = props
            .get(SCHEMAS_CACHE_SIZE_CONFIG)
            .and_then(|v| v.as_i64())
            .unwrap_or(SCHEMAS_CACHE_SIZE_DEFAULT as i64) as i32;

        // Parse decimal.format
        let decimal_format = props
            .get(DECIMAL_FORMAT_CONFIG)
            .and_then(|v| v.as_str())
            .map(|s| DecimalFormat::from_str(&s.to_uppercase()).unwrap_or(DecimalFormat::Base64))
            .unwrap_or_else(|| {
                DecimalFormat::from_str(DECIMAL_FORMAT_DEFAULT).unwrap_or(DecimalFormat::Base64)
            });

        // Parse replace.null.with.default
        let replace_null_with_default = props
            .get(REPLACE_NULL_WITH_DEFAULT_CONFIG)
            .and_then(|v| v.as_bool())
            .unwrap_or(REPLACE_NULL_WITH_DEFAULT_DEFAULT);

        // Parse schema.content
        let schema_content = props
            .get(SCHEMA_CONTENT_CONFIG)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.as_bytes().to_vec());

        // Create ConverterConfig from props
        let converter_config =
            ConverterConfig::new(common_trait::config::AbstractConfig::new(props));

        JsonConverterConfig {
            converter_config,
            schemas_enabled,
            schema_cache_size,
            decimal_format,
            replace_null_with_default,
            schema_content,
        }
    }

    /// Returns the configuration definition for JsonConverterConfig.
    ///
    /// This defines all configuration keys, their types, default values,
    /// documentation, and other metadata.
    ///
    /// # Returns
    ///
    /// A HashMap of configuration key definitions.
    ///
    /// Corresponds to: `JsonConverterConfig.configDef()` in Java
    pub fn config_def() -> HashMap<String, ConfigKeyDef> {
        // Start with ConverterConfig's base definition
        let base_config = ConverterConfig::new_config_def();

        // Define JsonConverterConfig specific configurations
        let schemas_group = "Schemas";

        let order_in_group = 0;

        base_config
            // Schemas group configurations
            .define_with_group(
                SCHEMAS_ENABLE_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(SCHEMAS_ENABLE_DEFAULT)),
                ConfigDefImportance::High,
                SCHEMAS_ENABLE_DOC,
                schemas_group,
                order_in_group,
            )
            .define_with_group(
                SCHEMAS_CACHE_SIZE_CONFIG,
                ConfigDefType::Int,
                Some(Value::Number(SCHEMAS_CACHE_SIZE_DEFAULT.into())),
                ConfigDefImportance::High,
                SCHEMAS_CACHE_SIZE_DOC,
                schemas_group,
                order_in_group + 1,
            )
            .define_with_group(
                SCHEMA_CONTENT_CONFIG,
                ConfigDefType::String,
                None, // Default is null
                ConfigDefImportance::High,
                SCHEMA_CONTENT_DOC,
                schemas_group,
                order_in_group + 2,
            )
            // Serialization group configurations
            .define_full(
                DECIMAL_FORMAT_CONFIG,
                ConfigDefType::String,
                Some(Value::String(DECIMAL_FORMAT_DEFAULT.to_string())),
                Some(Box::new(CaseInsensitiveValidString::in_values(&[
                    "BASE64", "NUMERIC",
                ]))),
                ConfigDefImportance::Low,
                DECIMAL_FORMAT_DOC,
                ConfigDefWidth::Medium,
            )
            .define_full(
                REPLACE_NULL_WITH_DEFAULT_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(REPLACE_NULL_WITH_DEFAULT_DEFAULT)),
                None,
                ConfigDefImportance::Low,
                REPLACE_NULL_WITH_DEFAULT_DOC,
                ConfigDefWidth::Medium,
            )
            .build()
    }

    /// Returns whether schemas are enabled.
    ///
    /// # Returns
    ///
    /// `true` if schemas are enabled, `false` otherwise.
    ///
    /// Corresponds to: `JsonConverterConfig.schemasEnabled()` in Java
    pub fn schemas_enabled(&self) -> bool {
        self.schemas_enabled
    }

    /// Returns the schema cache size.
    ///
    /// # Returns
    ///
    /// The maximum number of schemas that can be cached.
    ///
    /// Corresponds to: `JsonConverterConfig.schemaCacheSize()` in Java
    pub fn schema_cache_size(&self) -> i32 {
        self.schema_cache_size
    }

    /// Returns the decimal serialization format.
    ///
    /// # Returns
    ///
    /// The DecimalFormat enum value (Base64 or Numeric).
    ///
    /// Corresponds to: `JsonConverterConfig.decimalFormat()` in Java
    pub fn decimal_format(&self) -> DecimalFormat {
        self.decimal_format
    }

    /// Returns whether to replace null values with default values.
    ///
    /// When serializing and deserializing fields, if this is true and
    /// a field has a default value but is null, the default value will
    /// be used instead.
    ///
    /// # Returns
    ///
    /// `true` if null should be replaced with default, `false` otherwise.
    ///
    /// Corresponds to: `JsonConverterConfig.replaceNullWithDefault()` in Java
    pub fn replace_null_with_default(&self) -> bool {
        self.replace_null_with_default
    }

    /// Returns the schema content bytes, if provided.
    ///
    /// If a default schema is provided in the converter config, this will be
    /// used for all messages. This is only relevant if schemas are enabled.
    ///
    /// # Returns
    ///
    /// The schema content as bytes, or `None` if not provided.
    ///
    /// Corresponds to: `JsonConverterConfig.schemaContent()` in Java
    pub fn schema_content(&self) -> Option<&[u8]> {
        self.schema_content.as_deref()
    }

    /// Returns the underlying ConverterConfig.
    ///
    /// # Returns
    ///
    /// Reference to the base ConverterConfig.
    pub fn converter_config(&self) -> &ConverterConfig {
        &self.converter_config
    }
}
