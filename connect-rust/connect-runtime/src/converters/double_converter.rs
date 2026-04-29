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

//! DoubleConverter for double (Float64) conversion.
//!
//! This corresponds to `org.apache.kafka.connect.converters.DoubleConverter` in Java.

use common_trait::config::ConfigDef;
use common_trait::header::Headers;
use connect_api::data::{ConnectSchema, Schema, SchemaAndValue, SchemaType};
use connect_api::errors::ConnectError;
use connect_api::storage::{Converter, HeaderConverter};
use serde_json::Value;
use std::collections::HashMap;

use crate::converters::number_converter::{
    extract_f64, optional_schema, NUMBER_CONVERTER_CONFIG_DEF,
};

/// DoubleConverter for double (Float64) values.
///
/// This corresponds to `org.apache.kafka.connect.converters.DoubleConverter` in Java.
/// Supports serializing to and deserializing from double values.
/// When converting from bytes to Kafka Connect format, the converter will always return
/// an optional FLOAT64 schema.
pub struct DoubleConverter {
    is_key: bool,
}

impl DoubleConverter {
    /// Creates a new DoubleConverter.
    pub fn new() -> Self {
        DoubleConverter { is_key: false }
    }

    /// Returns the optional Float64 schema.
    fn optional_float64_schema() -> ConnectSchema {
        optional_schema(SchemaType::Float64)
    }
}

impl Default for DoubleConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl Converter for DoubleConverter {
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
        let f64_val = extract_f64(val)?;

        // Serialize double to bytes (8 bytes, big-endian)
        Ok(Some(f64_val.to_be_bytes().to_vec()))
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
                Some(Self::optional_float64_schema()),
                None,
            ));
        }

        let bytes = value.unwrap();
        if bytes.len() != 8 {
            return Err(ConnectError::data(format!(
                "Failed to deserialize double: expected 8 bytes, got {}",
                bytes.len()
            )));
        }

        // Deserialize double from bytes (big-endian)
        let f64_val = f64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

        Ok(SchemaAndValue::new(
            Some(Self::optional_float64_schema()),
            Some(Value::Number(
                serde_json::Number::from_f64(f64_val)
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

impl HeaderConverter for DoubleConverter {
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

        let f64_val = extract_f64(value)?;
        Ok(Some(f64_val.to_be_bytes().to_vec()))
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
