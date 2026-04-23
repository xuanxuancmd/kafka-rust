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

use connect_api::data::{Schema, SchemaAndValue, SchemaBuilder};
use connect_api::header::{ConnectHeader, Header};

#[test]
fn test_basic_header() {
    let key = "testKey";
    let schema = SchemaBuilder::string().build();
    let value = "testValue";

    let header = ConnectHeader::new(key, SchemaAndValue::new(schema, value));

    assert_eq!(key, header.key());
    assert_eq!(Some(schema), header.schema());
    assert_eq!(Some(value), header.value().as_string());
}

#[test]
fn test_header_with_optional_schema() {
    let key = "testKey";
    let value = "testValue";

    let header = ConnectHeader::new(key, SchemaAndValue::new_optional(value));

    assert_eq!(key, header.key());
    // Optional schema should be None
}

#[test]
fn test_header_rename() {
    let key = "testKey";
    let new_key = "renamedKey";
    let schema = SchemaBuilder::string().build();
    let value = "testValue";

    let header = ConnectHeader::new(key, SchemaAndValue::new(schema, value));
    let renamed = header.rename(new_key);

    assert_eq!(new_key, renamed.key());
    assert_eq!(header.schema(), renamed.schema());
    assert_eq!(header.value(), renamed.value());
}

#[test]
fn test_header_equality() {
    let key = "testKey";
    let schema = SchemaBuilder::string().build();
    let value = "testValue";

    let header1 = ConnectHeader::new(key, SchemaAndValue::new(schema.clone(), value));
    let header2 = ConnectHeader::new(key, SchemaAndValue::new(schema.clone(), value));

    assert_eq!(header1, header2);

    // Different key
    let header3 = ConnectHeader::new("differentKey", SchemaAndValue::new(schema.clone(), value));
    assert_ne!(header1, header3);

    // Different value
    let header4 = ConnectHeader::new(key, SchemaAndValue::new(schema.clone(), "differentValue"));
    assert_ne!(header1, header4);
}
