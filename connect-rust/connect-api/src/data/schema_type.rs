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

/// SchemaType represents the type of a schema.
///
/// This corresponds to `org.apache.kafka.connect.data.Schema.Type` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaType {
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    String,
    Bytes,
    Array,
    Map,
    Struct,
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl SchemaType {
    /// Returns the name of this schema type.
    pub fn name(&self) -> &'static str {
        match self {
            SchemaType::Int8 => "INT8",
            SchemaType::Int16 => "INT16",
            SchemaType::Int32 => "INT32",
            SchemaType::Int64 => "INT64",
            SchemaType::Float32 => "FLOAT32",
            SchemaType::Float64 => "FLOAT64",
            SchemaType::Boolean => "BOOLEAN",
            SchemaType::String => "STRING",
            SchemaType::Bytes => "BYTES",
            SchemaType::Array => "ARRAY",
            SchemaType::Map => "MAP",
            SchemaType::Struct => "STRUCT",
        }
    }

    /// Returns whether this schema type is primitive.
    pub fn is_primitive(&self) -> bool {
        matches!(
            self,
            SchemaType::Int8
                | SchemaType::Int16
                | SchemaType::Int32
                | SchemaType::Int64
                | SchemaType::Float32
                | SchemaType::Float64
                | SchemaType::Boolean
                | SchemaType::String
                | SchemaType::Bytes
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        assert_eq!(SchemaType::Int8.name(), "INT8");
        assert_eq!(SchemaType::String.name(), "STRING");
    }

    #[test]
    fn test_is_primitive() {
        assert!(SchemaType::Int8.is_primitive());
        assert!(SchemaType::String.is_primitive());
        assert!(!SchemaType::Array.is_primitive());
        assert!(!SchemaType::Struct.is_primitive());
    }
}
