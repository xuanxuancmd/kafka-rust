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

//! Requirements utility class for Kafka Connect transforms.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.util.Requirements` in Java.

use connect_api::data::Schema;
use connect_api::data::Struct;
use connect_api::errors::ConnectError;
use connect_api::sink::SinkRecord;
use serde_json::Value;
use std::collections::HashMap;

/// Requirements provides static helper methods for validating schemas and values.
///
/// This corresponds to `org.apache.kafka.connect.transforms.util.Requirements` in Java.
pub struct Requirements;

impl Requirements {
    /// Requires that a schema is not null.
    ///
    /// Throws a DataException if the schema is null.
    ///
    /// # Arguments
    /// * `schema` - The schema to check (can be None)
    /// * `purpose` - The purpose description for the error message
    ///
    /// # Errors
    /// Returns a DataException if schema is None.
    pub fn require_schema(schema: Option<&dyn Schema>, purpose: &str) -> Result<(), ConnectError> {
        if schema.is_none() {
            return Err(ConnectError::data(format!(
                "Schema required for [{}]",
                purpose
            )));
        }
        Ok(())
    }

    /// Requires that a value is a Map (HashMap<String, Value>).
    ///
    /// Throws a DataException if the value is not a Map.
    ///
    /// # Arguments
    /// * `value` - The value to check
    /// * `purpose` - The purpose description for the error message
    ///
    /// # Errors
    /// Returns a DataException if value is not a Map.
    pub fn require_map(
        value: &Value,
        purpose: &str,
    ) -> Result<HashMap<String, Value>, ConnectError> {
        if !value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Map objects supported in absence of schema for [{}], found: {}",
                purpose,
                Self::null_safe_type_name(value)
            )));
        }
        // Convert JSON object to HashMap
        let obj = value.as_object().unwrap();
        let mut map = HashMap::new();
        for (k, v) in obj.iter() {
            map.insert(k.clone(), v.clone());
        }
        Ok(map)
    }

    /// Requires that a value is a Map or null.
    ///
    /// Returns null if the value is null, otherwise requires it to be a Map.
    ///
    /// # Arguments
    /// * `value` - The value to check (can be null)
    /// * `purpose` - The purpose description for the error message
    ///
    /// # Errors
    /// Returns a DataException if value is not null and not a Map.
    pub fn require_map_or_null(
        value: Option<&Value>,
        purpose: &str,
    ) -> Result<Option<HashMap<String, Value>>, ConnectError> {
        if value.is_none() {
            return Ok(None);
        }
        Self::require_map(value.unwrap(), purpose).map(Some)
    }

    /// Requires that a value is a Struct.
    ///
    /// Throws a DataException if the value is not a Struct.
    ///
    /// # Arguments
    /// * `value` - The value to check
    /// * `purpose` - The purpose description for the error message
    ///
    /// # Errors
    /// Returns a DataException if value is not a Struct.
    pub fn require_struct<'a>(
        value: &'a Struct,
        _purpose: &str,
    ) -> Result<&'a Struct, ConnectError> {
        // In Rust, we receive a Struct reference directly, so this is just validation
        // The Java version checks instanceof, but in Rust we have type safety
        Ok(value)
    }

    /// Requires that a JSON Value is an Object (Struct-like).
    ///
    /// This is for schemaless mode where Struct is represented as JSON Object.
    /// Throws a DataException if the value is not an Object.
    ///
    /// # Arguments
    /// * `value` - The JSON Value to check
    /// * `purpose` - The purpose description for the error message
    ///
    /// # Errors
    /// Returns a DataException if value is not an Object.
    pub fn require_struct_value(value: &Value, purpose: &str) -> Result<(), ConnectError> {
        if !value.is_object() {
            return Err(ConnectError::data(format!(
                "Only Struct objects supported for [{}], found: {}",
                purpose,
                Self::null_safe_type_name(value)
            )));
        }
        Ok(())
    }

    /// Requires that a value is a Struct or null.
    ///
    /// Returns null if the value is null, otherwise requires it to be a Struct.
    ///
    /// # Arguments
    /// * `value` - The value to check (can be null)
    /// * `purpose` - The purpose description for the error message
    ///
    /// # Errors
    /// Returns a DataException if value is not null and not a Struct.
    pub fn require_struct_or_null<'a>(
        value: Option<&'a Struct>,
        _purpose: &str,
    ) -> Result<Option<&'a Struct>, ConnectError> {
        if value.is_none() {
            return Ok(None);
        }
        Self::require_struct(value.unwrap(), "").map(Some)
    }

    /// Returns the type name of a value, or "null" if the value is null.
    ///
    /// # Arguments
    /// * `x` - The value to get the type name for
    pub fn null_safe_type_name(x: &Value) -> String {
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

/// Alternative implementation for require_sink_record using SinkRecord directly.
///
/// In Rust, we cannot do runtime type checking like Java's instanceof,
/// so we require SinkRecord directly instead of a generic ConnectRecord.
///
/// # Arguments
/// * `record` - The SinkRecord to check
/// * `purpose` - The purpose description (not used in this implementation)
///
/// # Returns
/// Returns a reference to the SinkRecord.
pub fn require_sink_record_direct<'a>(
    record: &'a SinkRecord,
    _purpose: &str,
) -> Result<&'a SinkRecord, ConnectError> {
    Ok(record)
}

