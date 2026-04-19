// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use connect_api::data::{Field, Schema, SchemaType};
use connect_api::errors::ConnectError;

/// FakeSchema is a test helper class that provides a minimal Schema implementation
/// for testing purposes. It returns null for most methods.
pub struct FakeSchema {
    name: Option<String>,
}

impl FakeSchema {
    pub fn new() -> Self {
        FakeSchema {
            name: Some("fake".to_string()),
        }
    }

    pub fn with_name(name: &str) -> Self {
        FakeSchema {
            name: Some(name.to_string()),
        }
    }
}

impl Schema for FakeSchema {
    fn type_(&self) -> SchemaType {
        SchemaType::Int32 // Default type for testing
    }

    fn is_optional(&self) -> bool {
        false
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn version(&self) -> Option<i32> {
        None
    }

    fn doc(&self) -> Option<&str> {
        None
    }

    fn parameters(&self) -> Option<&std::collections::HashMap<String, String>> {
        None
    }

    fn default_value(&self) -> Option<&str> {
        None
    }

    fn fields(&self) -> &[Field] {
        &[]
    }

    fn field(&self, _name: &str) -> Option<&Field> {
        None
    }

    fn value_schema(&self) -> Option<&Schema> {
        None
    }

    fn key_schema(&self) -> Option<&Schema> {
        None
    }
}

#[test]
fn test_fake_schema_basic() {
    let fake = FakeSchema::new();

    assert_eq!(SchemaType::Int32, fake.type_());
    assert!(!fake.is_optional());
    assert_eq!(Some("fake"), fake.name());
    assert_eq!(None, fake.version());
    assert_eq!(None, fake.doc());
    assert_eq!(0, fake.fields().len());
}

#[test]
fn test_fake_schema_with_custom_name() {
    let fake = FakeSchema::with_name("custom_name");

    assert_eq!(Some("custom_name"), fake.name());
}
