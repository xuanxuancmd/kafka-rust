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

//! HoistField Transformation - wraps a field into a Struct/Map.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.HoistField` in Java.
//!
//! HoistField wraps the record's key or value into a new Struct (if a schema
//! is present) or a Map (if schemaless), with the original data placed under
//! a specified field name.
//!
//! # Configuration
//!
//! * `field` - The name of the field that will be created in the resulting
//!   Struct or Map to hold the original record's key or value. Required configuration.
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::hoist_field::{HoistField, HoistFieldTarget};
//! use connect_api::transforms::transformation::Transformation;
//! use connect_api::source::SourceRecord;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Create a HoistField for Value with field name "line"
//! let mut transform = HoistField::new(HoistFieldTarget::Value);
//! let mut configs = HashMap::new();
//! configs.insert("field".to_string(), json!("line"));
//! transform.configure(configs);
//!
//! // Create a record with a simple string value
//! let record = SourceRecord::new(
//!     HashMap::new(),
//!     HashMap::new(),
//!     "test-topic".to_string(),
//!     None,
//!     None,
//!     json!("simple string"),
//! );
//!
//! // Transform will wrap the value into {"line": "simple string"}
//! let result = transform.transform(record).unwrap().unwrap();
//! assert_eq!(result.value(), &json!({"line": "simple string"}));
//! ```

use crate::transforms::util::SimpleConfig;
use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType, ConfigKeyDef};
use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Configuration key for the field name.
const FIELD_CONFIG: &str = "field";

/// Overview documentation for HoistField.
pub const OVERVIEW_DOC: &str = "Wrap the key or value in a Struct or Map with a single field.";

/// HoistField transformation that wraps a field into a Struct/Map.
///
/// This corresponds to `org.apache.kafka.connect.transforms.HoistField<R>` in Java.
///
/// The HoistField transformation wraps the record's key or value into a new
/// Struct (schema-aware mode) or Map (schemaless mode), placing the original
/// value under a configured field name.
///
/// # Type Parameters
///
/// This transformation works on `SourceRecord` types that implement `ConnectRecord`.
///
/// # Configuration
///
/// - `field`: The name of the field to create in the wrapping Struct/Map (required).
///
/// # Schema Modes
///
/// The transformation handles both schemaless and schema-aware modes:
///
/// - **Schemaless mode**: Creates a new Map (JSON Object) with a single field
///   containing the original value.
///
/// - **Schema-aware mode**: Creates a new Struct with a single field containing
///   the original value. The schema is derived from the original schema.
///
/// # Thread Safety
///
/// In Rust, this transformation is inherently thread-safe as it only uses immutable
/// configuration and thread-safe data structures.
#[derive(Debug)]
pub struct HoistField {
    /// The target (Key or Value) - determines which part of the record to hoist.
    target: HoistFieldTarget,
    /// The name of the field to create in the wrapping Struct/Map.
    field: String,
}

/// HoistFieldTarget enum representing Key or Value operation.
///
/// This replaces Java's inner classes `HoistField.Key` and `HoistField.Value`.
/// In Kafka Connect configuration, the target is determined by the alias:
/// - "HoistField$Key" → HoistFieldTarget::Key
/// - "HoistField$Value" → HoistFieldTarget::Value
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoistFieldTarget {
    /// Operate on record key (HoistField.Key in Java).
    Key,
    /// Operate on record value (HoistField.Value in Java).
    Value,
}

impl HoistField {
    /// Creates a new HoistField transformation for the specified target.
    ///
    /// This corresponds to the constructor in Java.
    ///
    /// # Arguments
    ///
    /// * `target` - The target (Key or Value)
    pub fn new(target: HoistFieldTarget) -> Self {
        HoistField {
            target,
            field: String::new(),
        }
    }

    /// Returns the version of this transformation.
    ///
    /// This corresponds to `Transformation.version()` in Java, which returns
    /// `AppInfoParser.getVersion()` from connect-transforms module.
    ///
    /// Version "3.9.0" corresponds to Kafka 3.9.0.
    pub fn version() -> &'static str {
        "3.9.0"
    }

    /// Returns the configuration definition for this transformation.
    ///
    /// This corresponds to `HoistField.CONFIG_DEF` in Java.
    ///
    /// The configuration includes:
    /// - `field`: Field name for the wrapped value (required)
    pub fn config_def() -> HashMap<String, ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                FIELD_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The name of the field that will be created in the resulting Struct or Map to hold the original record's key or value.",
            )
            .build()
    }

    /// Returns the target of this transformation.
    pub fn target(&self) -> HoistFieldTarget {
        self.target
    }

    /// Returns the field name for the wrapped value.
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Applies the transformation in schemaless mode.
    ///
    /// In schemaless mode, the original value is wrapped into a new Map (JSON Object)
    /// with a single field named according to the configuration.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the key or value wrapped in a Map.
    fn apply_schemaless(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            HoistFieldTarget::Key => record.key(),
            HoistFieldTarget::Value => Some(record.value()),
        };

        // Create a new Map with the original value under the configured field name
        let mut wrapper_map = serde_json::Map::new();

        // Insert the original value (or null if operating value is None/null)
        match operating_value {
            Some(value) => wrapper_map.insert(self.field.clone(), value.clone()),
            None => wrapper_map.insert(self.field.clone(), Value::Null),
        };

        let wrapped_value = Value::Object(wrapper_map);

        // Create and return the new record based on target
        match self.target {
            HoistFieldTarget::Key => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                Some(wrapped_value),
                record.value().clone(),
            ))),
            HoistFieldTarget::Value => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                record.key().cloned(),
                wrapped_value,
            ))),
        }
    }

    /// Applies the transformation in schema-aware mode.
    ///
    /// In schema-aware mode, the original value is wrapped into a new Struct
    /// with a single field. The schema is derived from the original schema.
    ///
    /// Note: In the current Rust implementation, schemas are represented as strings
    /// (schema names), not full schema objects. This implementation treats schema-aware
    /// mode similarly to schemaless mode for the value wrapping.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the key or value wrapped in a Struct.
    fn apply_with_schema(
        &self,
        record: SourceRecord,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            HoistFieldTarget::Key => record.key(),
            HoistFieldTarget::Value => Some(record.value()),
        };

        // Check if operating value is a Struct (schema-aware mode)
        // In Java: requireStruct(operatingValue, PURPOSE)
        // In our implementation, we accept any value type for schema-aware mode
        // as we don't have full schema validation yet

        // Create a new Struct/Map wrapper
        let mut wrapper_map = serde_json::Map::new();

        // Insert the original value (or null if operating value is None/null)
        match operating_value {
            Some(value) => wrapper_map.insert(self.field.clone(), value.clone()),
            None => wrapper_map.insert(self.field.clone(), Value::Null),
        };

        let wrapped_value = Value::Object(wrapper_map);

        // Create and return the new record based on target
        // In Java, the schema would be set, but in our implementation we set null schema
        match self.target {
            HoistFieldTarget::Key => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                Some(wrapped_value),
                record.value().clone(),
            ))),
            HoistFieldTarget::Value => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                record.key().cloned(),
                wrapped_value,
            ))),
        }
    }
}

impl Default for HoistField {
    /// Creates a HoistField with default target (Value).
    fn default() -> Self {
        HoistField::new(HoistFieldTarget::Value)
    }
}

impl Transformation<SourceRecord> for HoistField {
    /// Configures this transformation with the given configuration map.
    ///
    /// This corresponds to `HoistField.configure(Map<String, ?> configs)` in Java.
    ///
    /// # Arguments
    ///
    /// * `configs` - A map of configuration key-value pairs
    ///
    /// # Configuration Keys
    ///
    /// * `field` - The name of the field to create in the wrapper (required, JSON string)
    ///
    /// # Errors
    ///
    /// The configuration will fail if `field` is not provided.
    fn configure(&mut self, configs: HashMap<String, Value>) {
        let config_def = Self::config_def();
        let simple_config = SimpleConfig::new(config_def, configs)
            .expect("Failed to parse configuration for HoistField");

        // Parse field name - expecting a JSON string
        self.field = simple_config
            .get_string(FIELD_CONFIG)
            .unwrap_or("")
            .to_string();
    }

    /// Transforms the given record by wrapping the key or value.
    ///
    /// This corresponds to `HoistField.apply(R record)` in Java.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the key or value wrapped in a Struct/Map.
    /// Returns `Ok(Some(record))` with the transformed record.
    /// Returns `Err(ConnectError)` on transformation errors.
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Determine whether to use schemaless or schema-aware mode
        // In Java: if (operatingSchema(record) != null)
        // In our implementation: check if operating schema is present
        let has_schema = match self.target {
            HoistFieldTarget::Key => record.key_schema().is_some(),
            HoistFieldTarget::Value => record.value_schema().is_some(),
        };

        if has_schema {
            // Schema-aware mode
            self.apply_with_schema(record)
        } else {
            // Schemaless mode
            self.apply_schemaless(record)
        }
    }

    /// Closes this transformation and releases any resources.
    ///
    /// This corresponds to `HoistField.close()` in Java.
    /// In the Java implementation, this clears the schema cache.
    /// In Rust, we reset the field to default.
    fn close(&mut self) {
        self.field.clear();
    }
}

impl fmt::Display for HoistField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HoistField{{target={}, field={}}}",
            match self.target {
                HoistFieldTarget::Key => "Key",
                HoistFieldTarget::Value => "Value",
            },
            self.field
        )
    }
}

/// HoistField$Key - convenience constructor for Key target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.HoistField$Key` in Java.
pub fn hoist_field_key() -> HoistField {
    HoistField::new(HoistFieldTarget::Key)
}

/// HoistField$Value - convenience constructor for Value target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.HoistField$Value` in Java.
pub fn hoist_field_value() -> HoistField {
    HoistField::new(HoistFieldTarget::Value)
}

