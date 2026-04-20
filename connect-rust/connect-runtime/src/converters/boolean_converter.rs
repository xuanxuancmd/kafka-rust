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

//! BooleanConverter for boolean conversion.
//!
//! This corresponds to `org.apache.kafka.connect.converters.BooleanConverter` in Java.

use common_trait::config::ConfigDef;
use common_trait::header::Headers;
use connect_api::data::{ConnectSchema, Schema, SchemaAndValue, SchemaBuilder, SchemaType};
use connect_api::errors::ConnectError;
use connect_api::storage::{Converter, ConverterConfig, HeaderConverter};
use serde_json::Value;
use std::collections::HashMap;

/// BooleanConverter for boolean values.
///
/// This corresponds to `org.apache.kafka.connect.converters.BooleanConverter` in Java.
/// Supports serializing to and deserializing from boolean values.
/// When converting from bytes to Kafka Connect format, the converter will always return
/// an optional BOOLEAN schema.
pub struct BooleanConverter {
    is_key: bool,
}

impl BooleanConverter {
    /// Creates a new BooleanConverter.
    pub fn new() -> Self {
        BooleanConverter { is_key: false }
    }

    /// Returns the optional boolean schema.
    fn optional_boolean_schema() -> ConnectSchema {
        SchemaBuilder::bool().optional().build()
    }
}

impl Default for BooleanConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl Converter for BooleanConverter {
    fn configure(&mut self, _configs: HashMap<String, Value>, is_key: bool) {
        self.is_key = is_key;
    }

    fn from_connect_data(
        &self,
        _topic: &str,
        schema: Option<&dyn Schema>,
        value: Option<&Value>,
    ) -> Result<Option<Vec<u8>>, ConnectError> {
        // Check schema type if provided
        if let Some(s) = schema {
            if s.r#type() != SchemaType::Boolean {
                return Err(ConnectError::data(format!(
                    "Invalid schema type for BooleanConverter: {}",
                    s.r#type()
                )));
            }
        }

        // Handle null value
        if value.is_none() || value.map(|v| v.is_null()).unwrap_or(false) {
            return Ok(None);
        }

        let val = value.unwrap();
        let bool_val = match val {
            Value::Bool(b) => *b,
            _ => {
                return Err(ConnectError::data(format!(
                    "BooleanConverter is not compatible with objects of type {}",
                    match val {
                        Value::Null => "null",
                        Value::Bool(_) => "boolean",
                        Value::Number(_) => "number",
                        Value::String(_) => "string",
                        Value::Array(_) => "array",
                        Value::Object(_) => "object",
                    }
                )));
            }
        };

        // Serialize boolean to bytes (1 byte: 1 for true, 0 for false)
        Ok(Some(vec![if bool_val { 1 } else { 0 }]))
    }

    fn from_connect_data_with_headers<H>(
        &self,
        _topic: &str,
        _headers: &mut H,
        schema: Option<&dyn Schema>,
        value: Option<&Value>,
    ) -> Result<Option<Vec<u8>>, ConnectError>
    where
        H: Headers,
    {
        self.from_connect_data(_topic, schema, value)
    }

    fn to_connect_data(
        &self,
        _topic: &str,
        value: Option<&[u8]>,
    ) -> Result<SchemaAndValue, ConnectError> {
        // Handle null value
        if value.is_none() {
            return Ok(SchemaAndValue::new(
                Some(Self::optional_boolean_schema()),
                None,
            ));
        }

        let bytes = value.unwrap();
        if bytes.is_empty() {
            return Err(ConnectError::data(
                "Failed to deserialize boolean: empty bytes",
            ));
        }

        // Deserialize boolean from bytes
        let bool_val = match bytes[0] {
            0 => false,
            1 => true,
            _ => {
                return Err(ConnectError::data(format!(
                    "Failed to deserialize boolean: invalid byte value {}",
                    bytes[0]
                )))
            }
        };

        Ok(SchemaAndValue::new(
            Some(Self::optional_boolean_schema()),
            Some(Value::Bool(bool_val)),
        ))
    }

    fn to_connect_data_with_headers<H>(
        &self,
        _topic: &str,
        _headers: &H,
        value: Option<&[u8]>,
    ) -> Result<SchemaAndValue, ConnectError>
    where
        H: Headers,
    {
        self.to_connect_data(_topic, value)
    }

    fn config(&self) -> &'static dyn ConfigDef {
        &BOOLEAN_CONVERTER_CONFIG_DEF
    }

    fn close(&mut self) {}
}

impl HeaderConverter for BooleanConverter {
    fn configure(&mut self, _configs: HashMap<String, String>, is_key: bool) {
        self.is_key = is_key;
    }

    fn from_connect_header(
        &self,
        _topic: &str,
        _header_key: &str,
        _schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<Vec<u8>>, ConnectError> {
        if value.is_null() {
            return Ok(None);
        }

        let bool_val = match value {
            Value::Bool(b) => *b,
            _ => {
                return Err(ConnectError::data(format!(
                    "BooleanConverter is not compatible with objects of type {}",
                    match value {
                        Value::Null => "null",
                        Value::Bool(_) => "boolean",
                        Value::Number(_) => "number",
                        Value::String(_) => "string",
                        Value::Array(_) => "array",
                        Value::Object(_) => "object",
                    }
                )));
            }
        };

        Ok(Some(vec![if bool_val { 1 } else { 0 }]))
    }

    fn to_connect_header(
        &self,
        _topic: &str,
        _header_key: &str,
        value: Option<&[u8]>,
    ) -> Result<SchemaAndValue, ConnectError> {
        self.to_connect_data(_topic, value)
    }

    fn config(&self) -> &'static dyn ConfigDef {
        &BOOLEAN_CONVERTER_CONFIG_DEF
    }

    fn close(&mut self) {}
}

/// Static ConfigDef for BooleanConverter.
pub struct BooleanConverterConfigDef;

impl ConfigDef for BooleanConverterConfigDef {
    fn config_def(&self) -> HashMap<String, common_trait::config::ConfigValueEntry> {
        ConverterConfig::new_config_def()
            .build()
            .into_iter()
            .map(|(k, v)| (k, v.to_config_value_entry()))
            .collect()
    }
}

pub static BOOLEAN_CONVERTER_CONFIG_DEF: BooleanConverterConfigDef = BooleanConverterConfigDef;
