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

//! InsertField Transformation - inserts new fields into Struct/Map.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.InsertField` in Java.
//!
//! InsertField inserts new fields into a record's key or value. The fields can
//! be metadata from the record (timestamp, topic, partition) or static values.
//!
//! # Configuration
//!
//! * `timestamp.field` - Field name for inserting record's timestamp
//! * `topic.field` - Field name for inserting record's topic
//! * `partition.field` - Field name for inserting record's partition
//! * `offset.field` - Field name for inserting record's offset (only for SinkRecord)
//! * `static.field` - Field name for inserting a static value
//! * `static.value` - The static value to insert
//! * `replace.null.with.default` - Whether to replace null with default values (default: true)
//!
//! # Example
//!
//! ```
//! use connect_transforms::transforms::insert_field::{InsertField, InsertFieldTarget};
//! use connect_api::transforms::Transformation;
//! use connect_api::source::SourceRecord;
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! // Create an InsertField for Value with topic field insertion
//! let mut transform = InsertField::new(InsertFieldTarget::Value);
//! let mut configs = HashMap::new();
//! configs.insert("topic.field".to_string(), json!("topic_name"));
//! transform.configure(configs);
//!
//! // Create a record with an existing value
//! let record = SourceRecord::new(
//!     HashMap::new(),
//!     HashMap::new(),
//!     "my-topic".to_string(),
//!     None,
//!     None,
//!     json!({"data": "value"}),
//! );
//!
//! // Transform will add "topic_name": "my-topic" to the value
//! let result = transform.transform(record).unwrap().unwrap();
//! assert_eq!(result.value(), &json!({"data": "value", "topic_name": "my-topic"}));
//! ```

use crate::transforms::util::{Requirements, SimpleConfig};
use common_trait::config::{ConfigDefBuilder, ConfigDefImportance, ConfigDefType, ConfigKeyDef};
use connect_api::connector::ConnectRecord;
use connect_api::errors::ConnectError;
use connect_api::source::SourceRecord;
use connect_api::transforms::Transformation;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Configuration key for timestamp field.
const TIMESTAMP_FIELD_CONFIG: &str = "timestamp.field";

/// Configuration key for topic field.
const TOPIC_FIELD_CONFIG: &str = "topic.field";

/// Configuration key for partition field.
const PARTITION_FIELD_CONFIG: &str = "partition.field";

/// Configuration key for offset field.
const OFFSET_FIELD_CONFIG: &str = "offset.field";

/// Configuration key for static field.
const STATIC_FIELD_CONFIG: &str = "static.field";

/// Configuration key for static value.
const STATIC_VALUE_CONFIG: &str = "static.value";

/// Configuration key for replace null with default.
const REPLACE_NULL_CONFIG: &str = "replace.null.with.default";

/// Default value for replace null with default.
const DEFAULT_REPLACE_NULL: bool = true;

/// Overview documentation for InsertField.
pub const OVERVIEW_DOC: &str =
    "Insert fields into a record's key or value using metadata from the record or a static value.";

/// Purpose string for error messages.
const PURPOSE: &str = "inserting field";

/// InsertFieldTarget enum representing Key or Value operation.
///
/// This replaces Java's inner classes `InsertField.Key` and `InsertField.Value`.
/// In Kafka Connect configuration, the target is determined by the alias:
/// - "InsertField$Key" → InsertFieldTarget::Key
/// - "InsertField$Value" → InsertFieldTarget::Value
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertFieldTarget {
    /// Operate on record key (InsertField.Key in Java).
    Key,
    /// Operate on record value (InsertField.Value in Java).
    Value,
}

/// InsertField transformation that inserts new fields into Struct/Map.
///
/// This corresponds to `org.apache.kafka.connect.transforms.InsertField<R>` in Java.
///
/// The InsertField transformation inserts new fields into a record's key or value.
/// The fields can be derived from record metadata (timestamp, topic, partition, offset)
/// or from a configured static value.
///
/// # Type Parameters
///
/// This transformation works on `SourceRecord` types that implement `ConnectRecord`.
///
/// # Configuration
///
/// - `timestamp.field`: Field name for inserting record's timestamp
/// - `topic.field`: Field name for inserting record's topic
/// - `partition.field`: Field name for inserting record's partition
/// - `offset.field`: Field name for inserting record's offset (SinkRecord only)
/// - `static.field`: Field name for inserting a static value
/// - `static.value`: The static value to insert
/// - `replace.null.with.default`: Whether to replace null with defaults (default: true)
///
/// # Schema Modes
///
/// The transformation handles both schemaless and schema-aware modes:
///
/// - **Schemaless mode**: The value is a Map (JSON Object), and fields are added
///   directly as key-value pairs.
///
/// - **Schema-aware mode**: The value is a Struct (JSON Object with schema), and
///   fields are added with appropriate schema types.
///
/// # Field Types
///
/// When inserting metadata fields:
/// - **Timestamp field**: Inserted as an optional Timestamp type
/// - **Topic field**: Inserted as a String type
/// - **Partition field**: Inserted as an optional Int32 type
/// - **Offset field**: Inserted as an optional Int64 type (SinkRecord only)
/// - **Static field**: Inserted as an optional String type
///
/// # Thread Safety
///
/// In Rust, this transformation is inherently thread-safe as it only uses immutable
/// configuration and thread-safe data structures.
#[derive(Debug)]
pub struct InsertField {
    /// The target (Key or Value) - determines which part of the record to modify.
    target: InsertFieldTarget,
    /// Field name for inserting timestamp.
    timestamp_field: Option<String>,
    /// Field name for inserting topic.
    topic_field: Option<String>,
    /// Field name for inserting partition.
    partition_field: Option<String>,
    /// Field name for inserting offset.
    offset_field: Option<String>,
    /// Field name for inserting static value.
    static_field: Option<String>,
    /// The static value to insert.
    static_value: Option<Value>,
    /// Whether to replace null values with default values.
    replace_null_with_default: bool,
}

impl InsertField {
    /// Creates a new InsertField transformation for the specified target.
    ///
    /// This corresponds to the constructor in Java.
    ///
    /// # Arguments
    ///
    /// * `target` - The target (Key or Value)
    pub fn new(target: InsertFieldTarget) -> Self {
        InsertField {
            target,
            timestamp_field: None,
            topic_field: None,
            partition_field: None,
            offset_field: None,
            static_field: None,
            static_value: None,
            replace_null_with_default: DEFAULT_REPLACE_NULL,
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
    /// This corresponds to `InsertField.CONFIG_DEF` in Java.
    ///
    /// The configuration includes:
    /// - `timestamp.field`: Field name for timestamp (optional)
    /// - `topic.field`: Field name for topic (optional)
    /// - `partition.field`: Field name for partition (optional)
    /// - `offset.field`: Field name for offset (optional)
    /// - `static.field`: Field name for static value (optional)
    /// - `static.value`: The static value to insert (optional)
    /// - `replace.null.with.default`: Boolean flag (default: true)
    pub fn config_def() -> HashMap<String, ConfigKeyDef> {
        ConfigDefBuilder::new()
            .define(
                TIMESTAMP_FIELD_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The name of the field to insert the record's timestamp.",
            )
            .define(
                TOPIC_FIELD_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The name of the field to insert the record's topic.",
            )
            .define(
                PARTITION_FIELD_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The name of the field to insert the record's partition.",
            )
            .define(
                OFFSET_FIELD_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The name of the field to insert the record's offset. Only applicable for SinkRecord.",
            )
            .define(
                STATIC_FIELD_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The name of the field to insert a static value.",
            )
            .define(
                STATIC_VALUE_CONFIG,
                ConfigDefType::String,
                None,
                ConfigDefImportance::High,
                "The static value to insert for the static.field.",
            )
            .define(
                REPLACE_NULL_CONFIG,
                ConfigDefType::Boolean,
                Some(Value::Bool(DEFAULT_REPLACE_NULL)),
                ConfigDefImportance::Low,
                "Whether to replace null field values with their default values from the schema.",
            )
            .build()
    }

    /// Returns the target of this transformation.
    pub fn target(&self) -> InsertFieldTarget {
        self.target
    }

    /// Returns the timestamp field name if configured.
    pub fn timestamp_field(&self) -> Option<&str> {
        self.timestamp_field.as_deref()
    }

    /// Returns the topic field name if configured.
    pub fn topic_field(&self) -> Option<&str> {
        self.topic_field.as_deref()
    }

    /// Returns the partition field name if configured.
    pub fn partition_field(&self) -> Option<&str> {
        self.partition_field.as_deref()
    }

    /// Returns the offset field name if configured.
    pub fn offset_field(&self) -> Option<&str> {
        self.offset_field.as_deref()
    }

    /// Returns the static field name if configured.
    pub fn static_field(&self) -> Option<&str> {
        self.static_field.as_deref()
    }

    /// Returns the static value if configured.
    pub fn static_value(&self) -> Option<&Value> {
        self.static_value.as_ref()
    }

    /// Returns whether null values should be replaced with defaults.
    pub fn replace_null_with_default(&self) -> bool {
        self.replace_null_with_default
    }

    /// Checks if any field insertion is configured.
    fn has_field_insertions(&self) -> bool {
        self.timestamp_field.is_some()
            || self.topic_field.is_some()
            || self.partition_field.is_some()
            || self.offset_field.is_some()
            || self.static_field.is_some()
    }

    /// Applies the transformation in schemaless mode.
    ///
    /// In schemaless mode, the value is expected to be a Map (JSON Object).
    /// Fields are added directly as key-value pairs.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the configured fields added.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if the value is not a Map.
    fn apply_schemaless(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            InsertFieldTarget::Key => record.key(),
            InsertFieldTarget::Value => Some(record.value()),
        };

        // Handle null/tombstone records
        // In Java: if (operatingValue == null) return null
        let operating_value = match operating_value {
            Some(v) if !v.is_null() => v.clone(),
            _ => {
                // If value is null and we're operating on value, return unchanged (tombstone)
                // In Java: tombstone records remain unchanged
                if self.target == InsertFieldTarget::Value && record.value().is_null() {
                    return Ok(Some(record));
                }
                // For null key, return unchanged as well
                return Ok(Some(record));
            }
        };

        // Validate that operating value is a Map
        Requirements::require_map(&operating_value, PURPOSE)?;

        // Get the JSON Object map directly
        let value_obj = operating_value.as_object().unwrap();
        let mut new_map = value_obj.clone();

        // Insert timestamp field
        if let Some(field_name) = &self.timestamp_field {
            let timestamp = record.timestamp();
            match timestamp {
                Some(ts) => {
                    new_map.insert(field_name.clone(), Value::Number(ts.into()));
                }
                None => {
                    new_map.insert(field_name.clone(), Value::Null);
                }
            };
        }

        // Insert topic field
        if let Some(field_name) = &self.topic_field {
            new_map.insert(
                field_name.clone(),
                Value::String(record.topic().to_string()),
            );
        }

        // Insert partition field
        if let Some(field_name) = &self.partition_field {
            match record.kafka_partition() {
                Some(p) => {
                    new_map.insert(field_name.clone(), Value::Number(p.into()));
                }
                None => {
                    new_map.insert(field_name.clone(), Value::Null);
                }
            };
        }

        // Insert offset field
        // Note: offset is only available for SinkRecord, not SourceRecord
        // For SourceRecord, we don't have offset, so we insert null or skip
        if let Some(field_name) = &self.offset_field {
            // SourceRecord doesn't have offset, insert null
            new_map.insert(field_name.clone(), Value::Null);
        }

        // Insert static field
        if let Some(field_name) = &self.static_field {
            match &self.static_value {
                Some(v) => {
                    new_map.insert(field_name.clone(), v.clone());
                }
                None => {
                    new_map.insert(field_name.clone(), Value::Null);
                }
            };
        }

        let new_value = Value::Object(new_map);

        // Create and return the new record based on target
        match self.target {
            InsertFieldTarget::Key => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                Some(new_value),
                record.value().clone(),
            ))),
            InsertFieldTarget::Value => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                record.key().cloned(),
                new_value,
            ))),
        }
    }

    /// Applies the transformation in schema-aware mode.
    ///
    /// In schema-aware mode, the value is expected to be a Struct (JSON Object with schema).
    /// Fields are added with appropriate schema types.
    ///
    /// Note: In the current Rust implementation, schemas are represented as strings
    /// (schema names), not full schema objects. This implementation treats schema-aware
    /// mode similarly to schemaless mode for the value modification.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the configured fields added.
    ///
    /// # Errors
    ///
    /// Returns `ConnectError::Data` if the value is not a Struct/Object.
    fn apply_with_schema(
        &self,
        record: SourceRecord,
    ) -> Result<Option<SourceRecord>, ConnectError> {
        // Get the operating value based on target
        let operating_value = match self.target {
            InsertFieldTarget::Key => record.key(),
            InsertFieldTarget::Value => Some(record.value()),
        };

        // Handle null/tombstone records
        // In Java: if (operatingValue == null) return null
        let operating_value = match operating_value {
            Some(v) if !v.is_null() => v.clone(),
            _ => {
                // If value is null and we're operating on value, return unchanged (tombstone)
                if self.target == InsertFieldTarget::Value && record.value().is_null() {
                    return Ok(Some(record));
                }
                // For null key, return unchanged as well
                return Ok(Some(record));
            }
        };

        // Validate that operating value is a Struct (Object)
        if !operating_value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Struct objects supported for [{}], found: {}",
                PURPOSE,
                Self::null_safe_type_name(&operating_value)
            )));
        }

        let value_obj = operating_value.as_object().unwrap();

        // Create new object with added fields
        let mut new_obj = value_obj.clone();

        // Insert timestamp field (as timestamp number)
        if let Some(field_name) = &self.timestamp_field {
            let timestamp = record.timestamp();
            match timestamp {
                Some(ts) => {
                    new_obj.insert(field_name.clone(), Value::Number(ts.into()));
                }
                None => {
                    new_obj.insert(field_name.clone(), Value::Null);
                }
            };
        }

        // Insert topic field (as string)
        if let Some(field_name) = &self.topic_field {
            new_obj.insert(
                field_name.clone(),
                Value::String(record.topic().to_string()),
            );
        }

        // Insert partition field (as optional int32)
        if let Some(field_name) = &self.partition_field {
            match record.kafka_partition() {
                Some(p) => {
                    new_obj.insert(field_name.clone(), Value::Number(p.into()));
                }
                None => {
                    new_obj.insert(field_name.clone(), Value::Null);
                }
            };
        }

        // Insert offset field (as optional int64)
        // Note: offset is only available for SinkRecord, not SourceRecord
        if let Some(field_name) = &self.offset_field {
            // SourceRecord doesn't have offset, insert null
            new_obj.insert(field_name.clone(), Value::Null);
        }

        // Insert static field (as optional string)
        if let Some(field_name) = &self.static_field {
            match &self.static_value {
                Some(v) => {
                    new_obj.insert(field_name.clone(), v.clone());
                }
                None => {
                    new_obj.insert(field_name.clone(), Value::Null);
                }
            };
        }

        let new_value = Value::Object(new_obj);

        // Create and return the new record based on target
        // In Java, the schema would be updated, but in our implementation we keep null schema
        match self.target {
            InsertFieldTarget::Key => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                Some(new_value),
                record.value().clone(),
            ))),
            InsertFieldTarget::Value => Ok(Some(SourceRecord::new(
                record.source_partition().clone(),
                record.source_offset().clone(),
                record.topic().to_string(),
                record.kafka_partition(),
                record.key().cloned(),
                new_value,
            ))),
        }
    }

    /// Returns the type name of a value, or "null" if the value is null.
    ///
    /// # Arguments
    /// * `x` - The value to get the type name for
    fn null_safe_type_name(x: &Value) -> String {
        match x {
            Value::Null => "null",
            Value::Bool(_) => "Boolean",
            Value::Number(_) => "Number",
            Value::String(_) => "String",
            Value::Array(_) => "Array",
            Value::Object(_) => "Map",
        }
        .to_string()
    }
}

impl Default for InsertField {
    /// Creates an InsertField with default target (Value).
    fn default() -> Self {
        InsertField::new(InsertFieldTarget::Value)
    }
}

impl Transformation<SourceRecord> for InsertField {
    /// Configures this transformation with the given configuration map.
    ///
    /// This corresponds to `InsertField.configure(Map<String, ?> configs)` in Java.
    ///
    /// # Arguments
    ///
    /// * `configs` - A map of configuration key-value pairs
    ///
    /// # Configuration Keys
    ///
    /// * `timestamp.field` - Field name for timestamp (optional, JSON string)
    /// * `topic.field` - Field name for topic (optional, JSON string)
    /// * `partition.field` - Field name for partition (optional, JSON string)
    /// * `offset.field` - Field name for offset (optional, JSON string)
    /// * `static.field` - Field name for static value (optional, JSON string)
    /// * `static.value` - The static value to insert (optional, JSON string)
    /// * `replace.null.with.default` - Boolean flag (optional, default: true)
    fn configure(&mut self, configs: HashMap<String, Value>) {
        let config_def = Self::config_def();
        let simple_config = SimpleConfig::new(config_def, configs)
            .expect("Failed to parse configuration for InsertField");

        // Parse timestamp field
        self.timestamp_field = simple_config
            .get_string(TIMESTAMP_FIELD_CONFIG)
            .map(|s| s.to_string());

        // Parse topic field
        self.topic_field = simple_config
            .get_string(TOPIC_FIELD_CONFIG)
            .map(|s| s.to_string());

        // Parse partition field
        self.partition_field = simple_config
            .get_string(PARTITION_FIELD_CONFIG)
            .map(|s| s.to_string());

        // Parse offset field
        self.offset_field = simple_config
            .get_string(OFFSET_FIELD_CONFIG)
            .map(|s| s.to_string());

        // Parse static field and static value
        self.static_field = simple_config
            .get_string(STATIC_FIELD_CONFIG)
            .map(|s| s.to_string());
        if let Some(static_value_str) = simple_config.get_string(STATIC_VALUE_CONFIG) {
            // Parse the static value string - treat as string value
            self.static_value = Some(Value::String(static_value_str.to_string()));
        } else {
            self.static_value = None;
        }

        // Parse replace.null.with.default boolean
        self.replace_null_with_default = simple_config
            .get_bool(REPLACE_NULL_CONFIG)
            .unwrap_or(DEFAULT_REPLACE_NULL);
    }

    /// Transforms the given record by inserting configured fields.
    ///
    /// This corresponds to `InsertField.apply(R record)` in Java.
    ///
    /// # Arguments
    ///
    /// * `record` - The source record to transform
    ///
    /// # Returns
    ///
    /// Returns a new record with the configured fields inserted.
    /// Returns `Ok(Some(record))` with the transformed record.
    /// Returns `Err(ConnectError)` on transformation errors.
    ///
    /// # Behavior
    ///
    /// - If no field insertions are configured, returns the record unchanged
    /// - If the operating value is null (tombstone), returns the record unchanged
    /// - In schemaless mode, fields are added to the Map
    /// - In schema-aware mode, fields are added to the Struct
    fn transform(&self, record: SourceRecord) -> Result<Option<SourceRecord>, ConnectError> {
        // If no field insertions are configured, return unchanged
        if !self.has_field_insertions() {
            return Ok(Some(record));
        }

        // Determine whether to use schemaless or schema-aware mode
        let has_schema = match self.target {
            InsertFieldTarget::Key => record.key_schema().is_some(),
            InsertFieldTarget::Value => record.value_schema().is_some(),
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
    /// This corresponds to `InsertField.close()` in Java.
    /// In the Java implementation, this clears the schema cache.
    /// In Rust, we reset the configuration to defaults.
    fn close(&mut self) {
        self.timestamp_field = None;
        self.topic_field = None;
        self.partition_field = None;
        self.offset_field = None;
        self.static_field = None;
        self.static_value = None;
        self.replace_null_with_default = DEFAULT_REPLACE_NULL;
    }
}

impl fmt::Display for InsertField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "InsertField{{target={}",
            match self.target {
                InsertFieldTarget::Key => "Key",
                InsertFieldTarget::Value => "Value",
            }
        )?;

        if let Some(ts) = &self.timestamp_field {
            write!(f, ", timestamp_field={}", ts)?;
        }
        if let Some(t) = &self.topic_field {
            write!(f, ", topic_field={}", t)?;
        }
        if let Some(p) = &self.partition_field {
            write!(f, ", partition_field={}", p)?;
        }
        if let Some(o) = &self.offset_field {
            write!(f, ", offset_field={}", o)?;
        }
        if let Some(s) = &self.static_field {
            write!(f, ", static_field={}", s)?;
        }
        if let Some(v) = &self.static_value {
            write!(f, ", static_value={}", v)?;
        }

        write!(f, "}}")
    }
}

/// InsertField$Key - convenience constructor for Key target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.InsertField$Key` in Java.
pub fn insert_field_key() -> InsertField {
    InsertField::new(InsertFieldTarget::Key)
}

/// InsertField$Value - convenience constructor for Value target.
///
/// This corresponds to `org.apache.kafka.connect.transforms.InsertField$Value` in Java.
pub fn insert_field_value() -> InsertField {
    InsertField::new(InsertFieldTarget::Value)
}

