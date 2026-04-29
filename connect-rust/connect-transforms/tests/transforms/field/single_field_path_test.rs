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

use connect_transforms::transforms::field::{FieldSyntaxVersion, SingleFieldPath};

#[test]
fn test_single_field_path_v2_simple() {
    let path = SingleFieldPath::new("foobarbaz", FieldSyntaxVersion::V2).unwrap();
    assert_eq!(path.field_names(), &["foobarbaz"]);
}

#[test]
fn test_single_field_path_v2_with_dots() {
    let path = SingleFieldPath::new("foo.bar.baz", FieldSyntaxVersion::V2).unwrap();
    assert_eq!(path.field_names(), &["foo", "bar", "baz"]);
}

#[test]
fn test_single_field_path_v2_empty() {
    let path = SingleFieldPath::new("", FieldSyntaxVersion::V2).unwrap();
    // Empty path returns empty array
    let empty: Vec<&str> = vec![];
    assert_eq!(path.field_names(), empty.as_slice());
}

#[test]
fn test_single_field_path_v2_backtick_wrapped() {
    let path = SingleFieldPath::new("`foo.bar.baz`", FieldSyntaxVersion::V2).unwrap();
    assert_eq!(path.field_names(), &["foo.bar.baz"]);
}

#[test]
fn test_single_field_path_v2_backtick_partial() {
    let path = SingleFieldPath::new("foo.`bar.baz`", FieldSyntaxVersion::V2).unwrap();
    assert_eq!(path.field_names(), &["foo", "bar.baz"]);
}

#[test]
fn test_single_field_path_v1_single_step() {
    let path = SingleFieldPath::new("foo.bar.baz", FieldSyntaxVersion::V1).unwrap();
    // V1 treats entire path as single step
    assert_eq!(path.field_names(), &["foo.bar.baz"]);
}

#[test]
fn test_single_field_path_default() {
    let path = SingleFieldPath::default();
    assert_eq!(path.field_names(), &[""]);
}
