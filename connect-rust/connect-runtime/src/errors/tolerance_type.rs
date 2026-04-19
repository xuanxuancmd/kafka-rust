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

//! ToleranceType enum defines the behavior for tolerating errors during
//! connector operation.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.errors.ToleranceType` in Java.

use std::fmt;

/// The tolerance type for error handling.
///
/// Controls whether errors are tolerated or cause immediate task failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ToleranceType {
    /// No tolerance - any error will result in immediate connector task failure.
    /// This is the strictest error handling mode.
    NONE,
    /// All tolerance - errors will be tolerated, and problematic records will
    /// be skipped. This allows the connector to continue processing even when
    /// errors occur.
    ALL,
}

impl ToleranceType {
    /// Returns a human-readable name for this tolerance type.
    pub fn name(&self) -> &'static str {
        match self {
            ToleranceType::NONE => "none",
            ToleranceType::ALL => "all",
        }
    }

    /// Parses a string to a ToleranceType.
    /// Returns None if the string is not a valid tolerance type.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().trim() {
            "none" => Some(ToleranceType::NONE),
            "all" => Some(ToleranceType::ALL),
            _ => None,
        }
    }

    /// Returns true if this tolerance type allows errors to be tolerated.
    pub fn is_tolerant(&self) -> bool {
        match self {
            ToleranceType::NONE => false,
            ToleranceType::ALL => true,
        }
    }
}

impl Default for ToleranceType {
    fn default() -> Self {
        ToleranceType::NONE
    }
}

impl fmt::Display for ToleranceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tolerance_type_name() {
        assert_eq!(ToleranceType::NONE.name(), "none");
        assert_eq!(ToleranceType::ALL.name(), "all");
    }

    #[test]
    fn test_tolerance_type_parse() {
        assert_eq!(ToleranceType::parse("none"), Some(ToleranceType::NONE));
        assert_eq!(ToleranceType::parse("all"), Some(ToleranceType::ALL));
        assert_eq!(ToleranceType::parse("NONE"), Some(ToleranceType::NONE));
        assert_eq!(ToleranceType::parse("ALL"), Some(ToleranceType::ALL));
        assert_eq!(ToleranceType::parse("invalid"), None);
    }

    #[test]
    fn test_tolerance_type_is_tolerant() {
        assert!(!ToleranceType::NONE.is_tolerant());
        assert!(ToleranceType::ALL.is_tolerant());
    }

    #[test]
    fn test_tolerance_type_default() {
        assert_eq!(ToleranceType::default(), ToleranceType::NONE);
    }

    #[test]
    fn test_tolerance_type_display() {
        assert_eq!(format!("{}", ToleranceType::NONE), "none");
        assert_eq!(format!("{}", ToleranceType::ALL), "all");
    }
}
