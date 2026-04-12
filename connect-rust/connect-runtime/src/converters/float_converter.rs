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

//! Float Converter
//!
//! Converter and HeaderConverter implementation that only supports serializing to and
//! deserializing from float values.
//!
//! It does support handling nulls. When converting from bytes to Kafka Connect format,
//! converter will always return an optional FLOAT32 schema.
//!
//! This implementation currently does nothing with the topic names or header keys.

use crate::converters::number_converter::NumberConverter;
use connect_api::data::{ConnectSchema, Type};
use std::sync::Arc;

/// Converter and HeaderConverter implementation that only supports serializing to and
/// deserializing from float values.
pub type FloatConverter = NumberConverter<f32>;

impl FloatConverter {
    /// Create a new FloatConverter.
    pub fn new() -> Self {
        NumberConverter::new(
            "float".to_string(),
            Arc::new(ConnectSchema::new(Type::Float32).with_optional(true)),
        )
    }
}

impl Default for FloatConverter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connect_api::storage::Converter;

    #[test]
    fn test_float_converter_new() {
        let converter = FloatConverter::new();
        assert_eq!(converter.type_name(), "float");
    }

    #[test]
    fn test_float_converter_from_connect_data() {
        let mut converter = FloatConverter::new();
        converter.configure(std::collections::HashMap::new(), false);
        let schema = Arc::new(ConnectSchema::new(Type::Float32));
        let value: f32 = 42.5;
        let result = converter.from_connect_data("test", Some(schema.as_ref()), &value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_float_converter_to_connect_data() {
        let mut converter = FloatConverter::new();
        converter.configure(std::collections::HashMap::new(), false);
        let bytes = 42.5f32.to_le_bytes().to_vec();
        let result = converter.to_connect_data("test", &bytes);
        assert!(result.is_ok());
    }
}
