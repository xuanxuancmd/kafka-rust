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

//! Integer Converter
//!
//! Converter and HeaderConverter implementation that only supports serializing to and
//! deserializing from integer values.
//!
//! It does support handling nulls. When converting from bytes to Kafka Connect format,
//! the converter will always return an optional INT32 schema.
//!
//! This implementation currently does nothing with the topic names or header keys.

use crate::converters::number_converter::NumberConverter;
use connect_api::data::{ConnectSchema, Type};
use std::sync::Arc;

/// Converter and HeaderConverter implementation that only supports serializing to and
/// deserializing from integer values.
pub type IntegerConverter = NumberConverter<i32>;

impl IntegerConverter {
    /// Create a new IntegerConverter.
    pub fn new() -> Self {
        NumberConverter::new(
            "integer".to_string(),
            Arc::new(ConnectSchema::new(Type::Int32).with_optional(true)),
        )
    }
}

impl Default for IntegerConverter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connect_api::storage::Converter;

    #[test]
    fn test_integer_converter_new() {
        let converter = IntegerConverter::new();
        assert_eq!(converter.type_name(), "integer");
    }

    #[test]
    fn test_integer_converter_from_connect_data() {
        let mut converter = IntegerConverter::new();
        converter.configure(std::collections::HashMap::new(), false);
        let schema = Arc::new(ConnectSchema::new(Type::Int32));
        let value: i32 = 42;
        let result = converter.from_connect_data("test", Some(schema.as_ref()), &value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_integer_converter_to_connect_data() {
        let mut converter = IntegerConverter::new();
        converter.configure(std::collections::HashMap::new(), false);
        let bytes = 42i32.to_le_bytes().to_vec();
        let result = converter.to_connect_data("test", &bytes);
        assert!(result.is_ok());
    }
}
