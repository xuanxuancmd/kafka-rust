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

//! Regex validator for configuration values.
//!
//! This validator validates that a configuration value is a syntactically
//! valid regular expression. It does NOT validate that a string matches
//! a pattern - instead it validates that the pattern itself is valid.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.util.RegexValidator` in Java.

use common_trait::config::Validator;
use common_trait::errors::ConfigException;
use regex::Regex;
use serde_json::Value;
use std::fmt;

/// Validator that ensures a configuration value is a valid regular expression pattern.
///
/// This validator attempts to compile the regex pattern to verify its syntax.
/// If the pattern cannot be compiled, a ConfigException is thrown.
///
/// This corresponds to `org.apache.kafka.connect.transforms.util.RegexValidator` in Java.
///
/// # Example
///
/// ```
/// use connect_transforms::transforms::util::RegexValidator;
/// use common_trait::config::Validator;
/// use serde_json::Value;
///
/// let validator = RegexValidator::new();
///
/// // Valid regex pattern
/// assert!(validator.ensure_valid("pattern", &Value::String(".*".to_string())).is_ok());
///
/// // Invalid regex pattern
/// assert!(validator.ensure_valid("pattern", &Value::String("[".to_string())).is_err());
///
/// // Null is allowed
/// assert!(validator.ensure_valid("pattern", &Value::Null).is_ok());
/// ```
#[derive(Debug, Clone)]
pub struct RegexValidator;

impl RegexValidator {
    /// Creates a new RegexValidator.
    ///
    /// This corresponds to the constructor in Java.
    pub fn new() -> Self {
        RegexValidator
    }
}

impl Default for RegexValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator for RegexValidator {
    /// Validates that the given value is a valid regular expression pattern.
    ///
    /// This method attempts to compile the regex pattern. If compilation fails,
    /// a ConfigException is thrown indicating the pattern is invalid.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the configuration parameter being validated
    /// * `value` - The value to validate (should be a string containing a regex pattern)
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if the value is null or a valid regex pattern.
    /// Returns Err(ConfigException) if the value is not a string or contains an invalid regex.
    ///
    /// This corresponds to `RegexValidator.ensureValid()` in Java.
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        match value {
            Value::Null => {
                // Null is allowed (no validation needed)
                Ok(())
            }
            Value::String(pattern) => {
                // Attempt to compile the regex to validate its syntax
                match Regex::new(pattern) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(ConfigException::new(format!(
                        "Entry must be a valid regular expression for configuration {}: {}",
                        name, e
                    ))),
                }
            }
            _ => Err(ConfigException::new(format!(
                "Entry must be a string for configuration {}, but got {}",
                name, value
            ))),
        }
    }

    /// Returns a string representation of this validator.
    ///
    /// This corresponds to `RegexValidator.toString()` in Java, which returns "regular expression".
    fn to_string(&self) -> String {
        "regular expression".to_string()
    }
}

impl fmt::Display for RegexValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "regular expression")
    }
}

