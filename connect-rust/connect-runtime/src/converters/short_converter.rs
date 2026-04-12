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

//! Short Converter
//!
//! Converter and HeaderConverter implementation that only supports serializing to and
//! deserializing from short values.
//!
//! It does support handling nulls. When converting from bytes to Kafka Connect format,
//! converter will always return an optional INT16 schema.
//!
//! This implementation currently does nothing with the topic names or header keys.

use crate::converters::number_converter::NumberConverter;
use connect_api::data::{ConnectSchema, Type};
use std::sync::Arc;

/// Converter and HeaderConverter implementation that only supports serializing to and
/// deserializing from short values.
pub type ShortConverter = NumberConverter<i16>;

impl ShortConverter {
    /// Create a new ShortConverter.
    pub fn new() -> Self {
        NumberConverter::new(
            "short".to_string(),
            Arc::new(ConnectSchema::new(Type::Int16).with_optional(true)),
        )
    }
}

impl Default for ShortConverter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connect_api::storage::Converter;

    #[test]
    fn test_short_converter_new() {
        let converter = ShortConverter::new();
        assert_eq!(converter.type_name(), "short");
    }

    #[test]
    fn test_short_converter_from_connect_data() {
        let mut converter = ShortConverter::new();
        converter.configure(std::collections::HashMap::new(), false);
        let schema = Arc::new(ConnectSchema::new(Type::Int16));
        let value: i16 = 42;
        let result = converter.from_connect_data("test", Some(schema.as_ref()), &value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_short_converter_to_connect_data() {
        let mut converter = ShortConverter::new();
        converter.configure(std::collections::HashMap::new(), false);
        let bytes = 42i16.to_le_bytes().to_vec();
        let result = converter.to_connect_data("test", &bytes);
        assert!(result.is_ok());
    }
}
