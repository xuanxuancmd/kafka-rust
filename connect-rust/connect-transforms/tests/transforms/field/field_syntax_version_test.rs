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

use connect_transforms::transforms::field::{
    FieldSyntaxVersion, FIELD_SYNTAX_VERSION_CONFIG, FIELD_SYNTAX_VERSION_DEFAULT_VALUE,
};

#[test]
fn test_field_syntax_version_v1_name() {
    assert_eq!(FieldSyntaxVersion::V1.name(), "V1");
}

#[test]
fn test_field_syntax_version_v2_name() {
    assert_eq!(FieldSyntaxVersion::V2.name(), "V2");
}

#[test]
fn test_field_syntax_version_from_string() {
    assert_eq!(
        FieldSyntaxVersion::from_string("V1"),
        Some(FieldSyntaxVersion::V1)
    );
    assert_eq!(
        FieldSyntaxVersion::from_string("V2"),
        Some(FieldSyntaxVersion::V2)
    );
    assert_eq!(
        FieldSyntaxVersion::from_string("v1"),
        Some(FieldSyntaxVersion::V1)
    );
    assert_eq!(
        FieldSyntaxVersion::from_string("v2"),
        Some(FieldSyntaxVersion::V2)
    );
    assert_eq!(FieldSyntaxVersion::from_string("invalid"), None);
}

#[test]
fn test_field_syntax_version_config_key() {
    assert_eq!(FIELD_SYNTAX_VERSION_CONFIG, "field.syntax.version");
}

#[test]
fn test_field_syntax_version_default_value() {
    assert_eq!(FIELD_SYNTAX_VERSION_DEFAULT_VALUE, "V1");
}

#[test]
fn test_field_syntax_version_default() {
    let default = FieldSyntaxVersion::default();
    assert_eq!(default, FieldSyntaxVersion::V1);
}
