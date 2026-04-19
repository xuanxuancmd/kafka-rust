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

use crate::data::{Field, SchemaType};
use crate::errors::ConnectError;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::LazyLock;

// Static empty parameters HashMap for use in SimpleSchema statics
static EMPTY_PARAMS: LazyLock<HashMap<String, String>> = LazyLock::new(HashMap::new);

/// Schema trait for Kafka Connect schemas.
///
/// This corresponds to `org.apache.kafka.connect.data.Schema` in Java.
pub trait Schema {
    /// Returns the type of this schema.
    fn r#type(&self) -> SchemaType;

    /// Returns whether this schema is optional.
    fn is_optional(&self) -> bool;

    /// Returns the name of this schema.
    fn name(&self) -> Option<&str>;

    /// Returns the version of this schema.
    fn version(&self) -> Option<i32>;

    /// Returns the documentation of this schema.
    fn doc(&self) -> Option<&str>;

    /// Returns the parameters of this schema.
    fn parameters(&self) -> &HashMap<String, String>;

    /// Returns the key schema for map schemas.
    fn key_schema(&self) -> Option<&dyn Schema>;

    /// Returns the value schema for map and array schemas.
    fn value_schema(&self) -> Option<&dyn Schema>;

    /// Returns the fields for struct schemas.
    fn fields(&self) -> &[Field];

    /// Returns a field by name.
    fn field(&self, name: &str) -> Option<&Field>;

    /// Returns the default value for this schema.
    fn default_value(&self) -> Option<&Value>;
}

/// Predefined schema constants.
pub struct PredefinedSchemas;

impl PredefinedSchemas {
    /// INT8_SCHEMA - optional int8 schema
    pub fn int8_schema() -> &'static SimpleSchema {
        &INT8_SCHEMA
    }

    /// INT16_SCHEMA - optional int16 schema
    pub fn int16_schema() -> &'static SimpleSchema {
        &INT16_SCHEMA
    }

    /// INT32_SCHEMA - optional int32 schema
    pub fn int32_schema() -> &'static SimpleSchema {
        &INT32_SCHEMA
    }

    /// INT64_SCHEMA - optional int64 schema
    pub fn int64_schema() -> &'static SimpleSchema {
        &INT64_SCHEMA
    }

    /// FLOAT32_SCHEMA - optional float32 schema
    pub fn float32_schema() -> &'static SimpleSchema {
        &FLOAT32_SCHEMA
    }

    /// FLOAT64_SCHEMA - optional float64 schema
    pub fn float64_schema() -> &'static SimpleSchema {
        &FLOAT64_SCHEMA
    }

    /// BOOLEAN_SCHEMA - optional boolean schema
    pub fn boolean_schema() -> &'static SimpleSchema {
        &BOOLEAN_SCHEMA
    }

    /// STRING_SCHEMA - optional string schema
    pub fn string_schema() -> &'static SimpleSchema {
        &STRING_SCHEMA
    }

    /// BYTES_SCHEMA - optional bytes schema
    pub fn bytes_schema() -> &'static SimpleSchema {
        &BYTES_SCHEMA
    }

    /// OPTIONAL_INT8_SCHEMA - optional int8 schema
    pub fn optional_int8_schema() -> &'static SimpleSchema {
        &OPTIONAL_INT8_SCHEMA
    }

    /// OPTIONAL_INT16_SCHEMA - optional int16 schema
    pub fn optional_int16_schema() -> &'static SimpleSchema {
        &OPTIONAL_INT16_SCHEMA
    }

    /// OPTIONAL_INT32_SCHEMA - optional int32 schema
    pub fn optional_int32_schema() -> &'static SimpleSchema {
        &OPTIONAL_INT32_SCHEMA
    }

    /// OPTIONAL_INT64_SCHEMA - optional int64 schema
    pub fn optional_int64_schema() -> &'static SimpleSchema {
        &OPTIONAL_INT64_SCHEMA
    }

    /// OPTIONAL_FLOAT32_SCHEMA - optional float32 schema
    pub fn optional_float32_schema() -> &'static SimpleSchema {
        &OPTIONAL_FLOAT32_SCHEMA
    }

    /// OPTIONAL_FLOAT64_SCHEMA - optional float64 schema
    pub fn optional_float64_schema() -> &'static SimpleSchema {
        &OPTIONAL_FLOAT64_SCHEMA
    }

    /// OPTIONAL_BOOLEAN_SCHEMA - optional boolean schema
    pub fn optional_boolean_schema() -> &'static SimpleSchema {
        &OPTIONAL_BOOLEAN_SCHEMA
    }

    /// OPTIONAL_STRING_SCHEMA - optional string schema
    pub fn optional_string_schema() -> &'static SimpleSchema {
        &OPTIONAL_STRING_SCHEMA
    }

    /// OPTIONAL_BYTES_SCHEMA - optional bytes schema
    pub fn optional_bytes_schema() -> &'static SimpleSchema {
        &OPTIONAL_BYTES_SCHEMA
    }
}

/// SimpleSchema - a simple implementation of Schema for primitive types.
pub struct SimpleSchema {
    schema_type: SchemaType,
    optional: bool,
    name: Option<String>,
    version: Option<i32>,
    doc: Option<String>,
    parameters: &'static LazyLock<HashMap<String, String>>,
}

impl SimpleSchema {
    pub fn new(schema_type: SchemaType, optional: bool) -> Self {
        SimpleSchema {
            schema_type,
            optional,
            name: None,
            version: None,
            doc: None,
            parameters: &EMPTY_PARAMS,
        }
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_version(mut self, version: i32) -> Self {
        self.version = Some(version);
        self
    }

    pub fn with_doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }
}

impl Schema for SimpleSchema {
    fn r#type(&self) -> SchemaType {
        self.schema_type
    }

    fn is_optional(&self) -> bool {
        self.optional
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn version(&self) -> Option<i32> {
        self.version
    }

    fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }

    fn parameters(&self) -> &HashMap<String, String> {
        &*self.parameters
    }

    fn key_schema(&self) -> Option<&dyn Schema> {
        None
    }

    fn value_schema(&self) -> Option<&dyn Schema> {
        None
    }

    fn fields(&self) -> &[Field] {
        &[]
    }

    fn field(&self, _name: &str) -> Option<&Field> {
        None
    }

    fn default_value(&self) -> Option<&Value> {
        None
    }
}

static INT8_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Int8,
    optional: false,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static INT16_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Int16,
    optional: false,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static INT32_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Int32,
    optional: false,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static INT64_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Int64,
    optional: false,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static FLOAT32_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Float32,
    optional: false,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static FLOAT64_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Float64,
    optional: false,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static BOOLEAN_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Boolean,
    optional: false,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static STRING_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::String,
    optional: false,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static BYTES_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Bytes,
    optional: false,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static OPTIONAL_INT8_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Int8,
    optional: true,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static OPTIONAL_INT16_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Int16,
    optional: true,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static OPTIONAL_INT32_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Int32,
    optional: true,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static OPTIONAL_INT64_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Int64,
    optional: true,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static OPTIONAL_FLOAT32_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Float32,
    optional: true,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static OPTIONAL_FLOAT64_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Float64,
    optional: true,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static OPTIONAL_BOOLEAN_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Boolean,
    optional: true,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static OPTIONAL_STRING_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::String,
    optional: true,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};

static OPTIONAL_BYTES_SCHEMA: SimpleSchema = SimpleSchema {
    schema_type: SchemaType::Bytes,
    optional: true,
    name: None,
    version: None,
    doc: None,
    parameters: &EMPTY_PARAMS,
};
