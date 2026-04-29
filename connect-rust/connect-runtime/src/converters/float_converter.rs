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

//! FloatConverter for float (Float32) conversion.
//!
//! This corresponds to `org.apache.kafka.connect.converters.FloatConverter` in Java.

use common_trait::config::ConfigDef;
use common_trait::header::Headers;
use connect_api::data::{ConnectSchema, Schema, SchemaAndValue, SchemaType};
use connect_api::errors::ConnectError;
use connect_api::storage::{Converter, HeaderConverter};
use serde_json::Value;
use std::collections::HashMap;

use crate::converters::number_converter::{
    extract_f32, optional_schema, NUMBER_CONVERTER_CONFIG_DEF,
};

/// FloatConverter for float (Float32) values.
///
/// This corresponds to `org.apache.kafka.connect.converters.FloatConverter` in Java.
/// Supports serializing to and deserializing from float values.
/// When converting from bytes to Kafka Connect format, the converter will always return
/// an optional FLOAT32 schema.
pub struct FloatConverter {
    is_key: bool,
}

impl FloatConverter {
    /// Creates a new FloatConverter.
    pub fn new() -> Self {
        FloatConverter { is_key: false }
    }

    /// Returns the optional Float32 schema.
    fn optional_float32_schema() -> ConnectSchema {
        optional_schema(SchemaType::Float32)
    }
}

impl Default for FloatConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl Converter for FloatConverter {
    fn configure(&mut self, _configs: HashMap<String, Value>, is_key: bool) {
        self.is_key = is_key;
    }

    fn from_connect_data(
        &self,
        _topic: &str,
        _schema: Option<&dyn Schema>,
        value: Option<&Value>,
    ) -> Result<Option<Vec<u8>>, ConnectError> {
        // Handle null value
        if value.is_none() || value.map(|v| v.is_null()).unwrap_or(false) {
            return Ok(None);
        }

        let val = value.unwrap();
        let f32_val = extract_f32(val)?;

        // Serialize float to bytes (4 bytes, big-endian)
        Ok(Some(f32_val.to_be_bytes().to_vec()))
    }

    fn from_connect_data_with_headers<H>(
        &self,
        _topic: &str,
        _headers: &mut H,
        _schema: Option<&dyn Schema>,
        value: Option<&Value>,
    ) -> Result<Option<Vec<u8>>, ConnectError>
    where
        H: Headers,
    {
        self.from_connect_data(_topic, _schema, value)
    }

    fn to_connect_data(
        &self,
        _topic: &str,
        value: Option<&[u8]>,
    ) -> Result<SchemaAndValue, ConnectError> {
        // Handle null value
        if value.is_none() {
            return Ok(SchemaAndValue::new(
                Some(Self::optional_float32_schema()),
                None,
            ));
        }

        let bytes = value.unwrap();
        if bytes.len() != 4 {
            return Err(ConnectError::data(format!(
                "Failed to deserialize float: expected 4 bytes, got {}",
                bytes.len()
            )));
        }

        // Deserialize float from bytes (big-endian)
        let f32_val = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        Ok(SchemaAndValue::new(
            Some(Self::optional_float32_schema()),
            Some(Value::Number(
                serde_json::Number::from_f64(f32_val as f64)
                    .unwrap_or_else(|| serde_json::Number::from(0)),
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
        &NUMBER_CONVERTER_CONFIG_DEF
    }

    fn close(&mut self) {}
}

impl HeaderConverter for FloatConverter {
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

        let f32_val = extract_f32(value)?;
        Ok(Some(f32_val.to_be_bytes().to_vec()))
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
        &NUMBER_CONVERTER_CONFIG_DEF
    }

    fn close(&mut self) {}
}
