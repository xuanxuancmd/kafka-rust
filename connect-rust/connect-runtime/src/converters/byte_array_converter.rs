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

//! ByteArrayConverter for raw byte data.
//!
//! This corresponds to `org.apache.kafka.connect.converters.ByteArrayConverter` in Java.

use common_trait::config::ConfigDef;
use common_trait::header::Headers;
use connect_api::data::{ConnectSchema, Schema, SchemaAndValue, SchemaBuilder, SchemaType};
use connect_api::errors::ConnectError;
use connect_api::storage::{Converter, ConverterConfig, HeaderConverter};
use serde_json::Value;
use std::collections::HashMap;

/// ByteArrayConverter - pass-through converter for raw byte data.
///
/// This corresponds to `org.apache.kafka.connect.converters.ByteArrayConverter` in Java.
/// This implementation currently does nothing with the topic names or header keys.
pub struct ByteArrayConverter;

impl ByteArrayConverter {
    /// Creates a new ByteArrayConverter.
    pub fn new() -> Self {
        ByteArrayConverter
    }

    /// Returns the optional bytes schema.
    fn optional_bytes_schema() -> ConnectSchema {
        SchemaBuilder::bytes().optional().build()
    }

    /// Extracts bytes from a ByteBuffer-like value.
    fn get_bytes_from_value(value: &Value) -> Result<Option<Vec<u8>>, ConnectError> {
        match value {
            Value::Null => Ok(None),
            Value::String(s) => Ok(Some(s.as_bytes().to_vec())),
            Value::Array(arr) => {
                // Treat as byte array
                let bytes: Result<Vec<u8>, _> = arr
                    .iter()
                    .map(|v| {
                        v.as_u64()
                            .and_then(|n| if n <= 255 { Some(n as u8) } else { None })
                            .ok_or_else(|| ConnectError::data("Invalid byte value in array"))
                    })
                    .collect();
                bytes.map(Some)
            }
            _ => Err(ConnectError::data(format!(
                "ByteArrayConverter is not compatible with objects of type {}",
                match value {
                    Value::Null => "null",
                    Value::Bool(_) => "boolean",
                    Value::Number(_) => "number",
                    Value::String(_) => "string",
                    Value::Array(_) => "array",
                    Value::Object(_) => "object",
                }
            ))),
        }
    }
}

impl Default for ByteArrayConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl Converter for ByteArrayConverter {
    fn configure(&mut self, _configs: HashMap<String, Value>, _is_key: bool) {
        // Do nothing - ByteArrayConverter has no configuration
    }

    fn from_connect_data(
        &self,
        _topic: &str,
        schema: Option<&dyn Schema>,
        value: Option<&Value>,
    ) -> Result<Option<Vec<u8>>, ConnectError> {
        // Check schema type if provided
        if let Some(s) = schema {
            if s.r#type() != SchemaType::Bytes {
                return Err(ConnectError::data(format!(
                    "Invalid schema type for ByteArrayConverter: {}",
                    s.r#type()
                )));
            }
        }

        // Handle null value
        if value.is_none() || value.map(|v| v.is_null()).unwrap_or(false) {
            return Ok(None);
        }

        let val = value.unwrap();
        Self::get_bytes_from_value(val)
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
                Some(Self::optional_bytes_schema()),
                None,
            ));
        }

        let bytes = value.unwrap();
        Ok(SchemaAndValue::new(
            Some(Self::optional_bytes_schema()),
            Some(Value::Array(
                bytes
                    .iter()
                    .map(|b| Value::Number((*b as u64).into()))
                    .collect(),
            )),
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
        &BYTE_ARRAY_CONVERTER_CONFIG_DEF
    }

    fn close(&mut self) {}
}

impl HeaderConverter for ByteArrayConverter {
    fn configure(&mut self, _configs: HashMap<String, String>, _is_key: bool) {
        // Do nothing - ByteArrayConverter has no configuration
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
        Self::get_bytes_from_value(value)
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
        &BYTE_ARRAY_CONVERTER_CONFIG_DEF
    }

    fn close(&mut self) {}
}

/// Static ConfigDef for ByteArrayConverter.
pub struct ByteArrayConverterConfigDef;

impl ConfigDef for ByteArrayConverterConfigDef {
    fn config_def(&self) -> HashMap<String, common_trait::config::ConfigValueEntry> {
        ConverterConfig::new_config_def()
            .build()
            .into_iter()
            .map(|(k, v)| (k, v.to_config_value_entry()))
            .collect()
    }
}

pub static BYTE_ARRAY_CONVERTER_CONFIG_DEF: ByteArrayConverterConfigDef =
    ByteArrayConverterConfigDef;
