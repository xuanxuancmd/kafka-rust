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

//! Boolean Converter
//!
//! Converter and HeaderConverter implementation that supports serializing to and
//! deserializing from Boolean values.
//!
//! When converting from bytes to Kafka Connect format, the converter will always return an optional
//! BOOLEAN schema.

use crate::converters::boolean_converter_config::BooleanConverterConfig;
use connect_api::data::{ConnectSchema, Schema, SchemaAndValue, Type};
use connect_api::error::{ConnectException, DataException};
use connect_api::storage::{Converter, ConverterType, HeaderConverter};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Converter and HeaderConverter implementation that supports serializing to and
/// deserializing from Boolean values.
pub struct BooleanConverter {
    /// Configuration
    config: BooleanConverterConfig,
}

impl BooleanConverter {
    /// Create a new BooleanConverter.
    pub fn new() -> Self {
        Self {
            config: BooleanConverterConfig::default(),
        }
    }

    /// Create a new BooleanConverter with configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - converter configuration
    pub fn with_config(config: BooleanConverterConfig) -> Self {
        Self { config }
    }

    /// Convert bytes to boolean value.
    ///
    /// # Arguments
    ///
    /// * `value` - bytes to deserialize
    ///
    /// # Returns
    ///
    /// the deserialized boolean value
    fn deserialize(&self, value: &[u8]) -> Result<Option<bool>, ConnectException> {
        if value.is_empty() {
            return Ok(None);
        }

        if value.len() != 1 {
            return Err(ConnectException::new(
                "Invalid boolean value: expected 1 byte".to_string(),
            ));
        }

        match value[0] {
            0 => Ok(Some(false)),
            1 => Ok(Some(true)),
            _ => Err(ConnectException::new(format!(
                "Invalid boolean value: expected 0 or 1, got {}",
                value[0]
            ))),
        }
    }

    /// Serialize boolean value to bytes.
    ///
    /// # Arguments
    ///
    /// * `value` - boolean value to serialize
    ///
    /// # Returns
    ///
    /// the serialized bytes
    fn serialize(&self, value: Option<bool>) -> Vec<u8> {
        match value {
            Some(b) => vec![if b { 1u8 } else { 0u8 }],
            None => vec![],
        }
    }
}

impl Default for BooleanConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl Converter for BooleanConverter {
    fn configure(&mut self, configs: HashMap<String, String>, is_key: bool) {
        self.config = BooleanConverterConfig::new_with_type(configs, is_key);
    }

    fn from_connect_data(
        &self,
        _topic: &str,
        schema: Option<&dyn Schema>,
        value: &dyn Any,
    ) -> Result<Vec<u8>, ConnectException> {
        // Validate schema type if provided
        if let Some(s) = schema {
            if s.type_() != Type::Boolean {
                return Err(DataException::new(format!(
                    "Invalid schema type for BooleanConverter: {}",
                    s.type_()
                ))
                .into());
            }
        }

        // Cast value to boolean
        let bool_value = if value.is::<()>() {
            None
        } else if let Some(b) = value.downcast_ref::<bool>() {
            Some(*b)
        } else {
            return Err(DataException::new(format!(
                "BooleanConverter is not compatible with objects of type",
            ))
            .into());
        };

        Ok(self.serialize(bool_value))
    }

    fn to_connect_data(
        &self,
        _topic: &str,
        value: &[u8],
    ) -> Result<SchemaAndValue, ConnectException> {
        let bool_value = self.deserialize(value)?;
        let schema = Arc::new(ConnectSchema::new(Type::Boolean).with_optional(true));

        Ok(SchemaAndValue::new(
            Some(schema),
            bool_value.map(|b| Box::new(b) as Box<dyn Any + Send + Sync>),
        ))
    }
}

impl HeaderConverter for BooleanConverter {
    fn to_connect_header(
        &self,
        topic: &str,
        _header_key: &str,
        value: &[u8],
    ) -> Result<SchemaAndValue, ConnectException> {
        self.to_connect_data(topic, value)
    }

    fn from_connect_header(
        &self,
        topic: &str,
        _header_key: &str,
        schema: Option<&dyn Schema>,
        value: &dyn Any,
    ) -> Result<Option<Vec<u8>>, ConnectException> {
        let bytes = self.from_connect_data(topic, schema, value)?;
        Ok(Some(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_converter_new() {
        let converter = BooleanConverter::new();
        assert!(!converter.config.is_key());
    }

    #[test]
    fn test_serialize_true() {
        let converter = BooleanConverter::new();
        let result = converter.serialize(Some(true));
        assert_eq!(result, vec![1u8]);
    }

    #[test]
    fn test_serialize_false() {
        let converter = BooleanConverter::new();
        let result = converter.serialize(Some(false));
        assert_eq!(result, vec![0u8]);
    }

    #[test]
    fn test_serialize_none() {
        let converter = BooleanConverter::new();
        let result = converter.serialize(None);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_deserialize_true() {
        let converter = BooleanConverter::new();
        let result = converter.deserialize(&[1u8]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(true));
    }

    #[test]
    fn test_deserialize_false() {
        let converter = BooleanConverter::new();
        let result = converter.deserialize(&[0u8]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(false));
    }

    #[test]
    fn test_deserialize_empty() {
        let converter = BooleanConverter::new();
        let result = converter.deserialize(&[]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_from_connect_data() {
        let converter = BooleanConverter::new();
        let schema = Arc::new(ConnectSchema::new(Type::Boolean));
        let result = converter.from_connect_data("test", Some(schema.as_ref()), &true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![1u8]);
    }

    #[test]
    fn test_to_connect_data() {
        let converter = BooleanConverter::new();
        let result = converter.to_connect_data("test", &[1u8]);
        assert!(result.is_ok());
        let sav = result.unwrap();
        assert!(sav.schema().is_some());
    }
}
