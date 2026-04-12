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

//! Byte Array Converter
//!
//! Pass-through converter for raw byte data.
//!
//! This implementation currently does nothing with the topic names or header keys.

use connect_api::data::{ConnectSchema, Schema, SchemaAndValue, Type};
use connect_api::error::{ConnectException, DataException};
use connect_api::storage::{Converter, HeaderConverter};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Pass-through converter for raw byte data.
pub struct ByteArrayConverter;

impl ByteArrayConverter {
    /// Create a new ByteArrayConverter.
    pub fn new() -> Self {
        Self
    }

    /// Extract bytes from Vec<u8>.
    ///
    /// # Arguments
    ///
    /// * `value` - value to extract bytes from
    ///
    /// # Returns
    ///
    ///  bytes if present, None otherwise
    fn extract_bytes(&self, value: &dyn Any) -> Result<Option<Vec<u8>>, DataException> {
        if value.is::<()>() {
            return Ok(None);
        }

        if let Some(bytes) = value.downcast_ref::<Vec<u8>>() {
            return Ok(Some(bytes.clone()));
        }

        Err(DataException::new(format!(
            "ByteArrayConverter is not compatible with objects of type",
        )))
    }
}

impl Default for ByteArrayConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl Converter for ByteArrayConverter {
    fn configure(&mut self, _configs: HashMap<String, String>, _is_key: bool) {
        // No configuration needed
    }

    fn from_connect_data(
        &self,
        _topic: &str,
        schema: Option<&dyn Schema>,
        value: &dyn Any,
    ) -> Result<Vec<u8>, ConnectException> {
        // Validate schema type if provided
        if let Some(s) = schema {
            if s.type_() != Type::Bytes {
                return Err(DataException::new(format!(
                    "Invalid schema type for ByteArrayConverter: {}",
                    s.type_()
                ))
                .into());
            }
        }

        self.extract_bytes(value).map_err(|e| e.into())
    }

    fn to_connect_data(
        &self,
        _topic: &str,
        value: &[u8],
    ) -> Result<SchemaAndValue, ConnectException> {
        let schema = Arc::new(ConnectSchema::new(Type::Bytes).with_optional(true));
        Ok(SchemaAndValue::new(
            Some(schema),
            Some(Box::new(value.to_vec()) as Box<dyn Any + Send + Sync>),
        ))
    }
}

impl HeaderConverter for ByteArrayConverter {
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
    fn test_byte_array_converter_new() {
        let _converter = ByteArrayConverter::new();
    }

    #[test]
    fn test_from_connect_data() {
        let converter = ByteArrayConverter::new();
        let schema = Arc::new(ConnectSchema::new(Type::Bytes));
        let bytes = vec![1u8, 2u8, 3u8];
        let result = converter.from_connect_data("test", Some(schema.as_ref()), &bytes);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), bytes);
    }

    #[test]
    fn test_to_connect_data() {
        let converter = ByteArrayConverter::new();
        let bytes = vec![1u8, 2u8, 3u8];
        let result = converter.to_connect_data("test", &bytes);
        assert!(result.is_ok());
        let sav = result.unwrap();
        assert!(sav.schema().is_some());
    }

    #[test]
    fn test_extract_bytes() {
        let converter = ByteArrayConverter::new();
        let bytes = vec![1u8, 2u8, 3u8];
        let result = converter.extract_bytes(&bytes);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(bytes));
    }
}
