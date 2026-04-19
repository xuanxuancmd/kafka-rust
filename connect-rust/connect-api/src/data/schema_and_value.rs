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

use crate::data::{ConnectSchema, Schema, SchemaBuilder};
use serde_json::Value;

/// SchemaAndValue represents a schema and its associated value.
///
/// This corresponds to `org.apache.kafka.connect.data.SchemaAndValue` in Java.
#[derive(Debug, Clone)]
pub struct SchemaAndValue {
    schema: Option<ConnectSchema>,
    value: Option<Value>,
}

impl SchemaAndValue {
    /// Creates a new SchemaAndValue with schema and value.
    pub fn new(schema: Option<ConnectSchema>, value: Option<Value>) -> Self {
        SchemaAndValue { schema, value }
    }

    /// Creates a SchemaAndValue with a null value.
    pub fn null() -> Self {
        SchemaAndValue {
            schema: None,
            value: None,
        }
    }

    /// Returns the schema.
    pub fn schema(&self) -> Option<&dyn Schema> {
        self.schema.as_ref().map(|s| s as &dyn Schema)
    }

    /// Returns the schema as ConnectSchema reference.
    pub fn schema_ref(&self) -> Option<&ConnectSchema> {
        self.schema.as_ref()
    }

    /// Returns the value.
    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    /// Returns whether this is a null value.
    pub fn is_null(&self) -> bool {
        self.value.is_none() || self.value.as_ref().map(|v| v.is_null()).unwrap_or(true)
    }
}
