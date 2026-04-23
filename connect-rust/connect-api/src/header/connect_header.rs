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

use crate::header::Header;
use serde_json::Value;

/// ConnectHeader is a concrete implementation of Header.
///
/// This corresponds to `org.apache.kafka.connect.header.ConnectHeader` in Java.
#[derive(Debug, Clone)]
pub struct ConnectHeader {
    key: String,
    value: Value,
    schema_name: Option<String>,
}

impl ConnectHeader {
    /// Creates a new ConnectHeader with the given key and value.
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        ConnectHeader {
            key: key.into(),
            value,
            schema_name: None,
        }
    }

    /// Creates a new ConnectHeader with the given key, value, and schema name.
    pub fn with_schema(
        key: impl Into<String>,
        value: Value,
        schema_name: impl Into<String>,
    ) -> Self {
        ConnectHeader {
            key: key.into(),
            value,
            schema_name: Some(schema_name.into()),
        }
    }
}

impl Header for ConnectHeader {
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> &Value {
        &self.value
    }

    fn schema_name(&self) -> Option<&str> {
        self.schema_name.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let header = ConnectHeader::new("key", Value::String("value".to_string()));
        assert_eq!(header.key(), "key");
        assert_eq!(header.value(), &Value::String("value".to_string()));
        assert!(header.schema_name().is_none());
    }

    #[test]
    fn test_with_schema() {
        let header =
            ConnectHeader::with_schema("key", Value::String("value".to_string()), "schema");
        assert_eq!(header.schema_name(), Some("schema"));
    }
}
