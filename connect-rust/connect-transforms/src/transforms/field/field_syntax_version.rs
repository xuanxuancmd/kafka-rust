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

//! Field syntax version for Kafka Connect transforms.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.field.FieldSyntaxVersion` in Java.
//!
//! Defines different versions of field path semantics used in Kafka Connect Transforms
//! to support accessing nested structures, as described in KIP-821.

use std::str::FromStr;

/// Configuration key for field syntax version.
/// Corresponds to `FIELD_SYNTAX_VERSION_CONFIG` in Java.
pub const FIELD_SYNTAX_VERSION_CONFIG: &str = "field.syntax.version";

/// Documentation for field syntax version configuration.
/// Corresponds to `FIELD_SYNTAX_VERSION_DOC` in Java.
pub const FIELD_SYNTAX_VERSION_DOC: &str = "A field syntax version is used to determine the field path semantics. \
    V1 does not support accessing nested fields and only allows access to attributes at the root of the data structure. \
    This is backward compatible with the behavior before KIP-821. \
    V2 supports accessing nested fields using a dotted notation. \
    Field names containing dots can be wrapped in backticks to distinguish them.";

/// Default value for field syntax version configuration.
/// Corresponds to `FIELD_SYNTAX_VERSION_DEFAULT_VALUE` in Java.
pub const FIELD_SYNTAX_VERSION_DEFAULT_VALUE: &str = "V1";

/// Field syntax version enum.
///
/// Defines different versions of field path semantics:
/// - V1: Does not support accessing nested fields, backward compatible with pre-KIP-821 behavior
/// - V2: Supports accessing nested fields using dotted notation
///
/// Corresponds to `org.apache.kafka.connect.transforms.field.FieldSyntaxVersion` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldSyntaxVersion {
    /// Version 1: Does not support accessing nested fields.
    /// Only allows access to attributes at the root of the data structure.
    /// Backward compatible with behavior before KIP-821.
    V1,
    /// Version 2: Supports accessing nested fields using dotted notation.
    /// Field names containing dots can be wrapped in backticks to distinguish them.
    V2,
}

impl FieldSyntaxVersion {
    /// Returns the name of this field syntax version as a string.
    ///
    /// Corresponds to `name()` method in Java enum.
    pub fn name(&self) -> &'static str {
        match self {
            FieldSyntaxVersion::V1 => "V1",
            FieldSyntaxVersion::V2 => "V2",
        }
    }

    /// Parses a field syntax version from a string.
    ///
    /// The input string is converted to uppercase before matching.
    /// Returns `Some(FieldSyntaxVersion)` if the string matches a valid version,
    /// or `None` if the string is invalid.
    ///
    /// Corresponds to `fromConfig` logic in Java (case-insensitive parsing).
    pub fn from_string(s: &str) -> Option<FieldSyntaxVersion> {
        match s.to_uppercase().as_str() {
            "V1" => Some(FieldSyntaxVersion::V1),
            "V2" => Some(FieldSyntaxVersion::V2),
            _ => None,
        }
    }

    /// Returns all possible field syntax versions.
    pub fn values() -> &'static [FieldSyntaxVersion] {
        &[FieldSyntaxVersion::V1, FieldSyntaxVersion::V2]
    }
}

impl Default for FieldSyntaxVersion {
    /// Returns the default field syntax version (V1).
    /// Corresponds to Java default value.
    fn default() -> Self {
        FieldSyntaxVersion::V1
    }
}

impl FromStr for FieldSyntaxVersion {
    type Err = String;

    /// Parses a field syntax version from a string.
    ///
    /// Returns an error if the string does not match a valid version.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        FieldSyntaxVersion::from_string(s)
            .ok_or_else(|| format!("Unrecognized field syntax version: {}", s))
    }
}

impl std::fmt::Display for FieldSyntaxVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

