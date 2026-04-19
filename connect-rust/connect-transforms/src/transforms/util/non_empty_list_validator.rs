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

//! Non-empty list validator for configuration values.
//!
//! This corresponds to `org.apache.kafka.connect.transforms.util.NonEmptyListValidator` in Java.

use serde_json::Value;
use std::fmt;

/// Validator that ensures a list value is not null and not empty.
///
/// This corresponds to `org.apache.kafka.connect.transforms.util.NonEmptyListValidator` in Java.
/// Unlike the `NonEmptyListValidator` in common-trait which also checks for duplicates,
/// this validator only checks that the list is non-null and non-empty.
#[derive(Debug, Clone, Copy)]
pub struct NonEmptyListValidator;

impl NonEmptyListValidator {
    /// Creates a new NonEmptyListValidator.
    pub fn new() -> Self {
        NonEmptyListValidator
    }
}

impl Default for NonEmptyListValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NonEmptyListValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "non-empty list")
    }
}

/// Validates that a configuration value is a non-empty list.
///
/// # Arguments
///
/// * `name` - The name of the configuration being validated
/// * `value` - The value to validate (should be a JSON Array)
///
/// # Returns
///
/// * `Ok(())` if the value is a non-empty list
/// * `Err` with a descriptive message if the value is null or an empty list
pub fn ensure_valid(name: &str, value: &Value) -> Result<(), String> {
    match value {
        Value::Null => Err(format!("Empty list for configuration {}", name)),
        Value::Array(arr) => {
            if arr.is_empty() {
                Err(format!("Empty list for configuration {}", name))
            } else {
                Ok(())
            }
        }
        _ => Err(format!(
            "Expected a list value for configuration {}, but got {}",
            name, value
        )),
    }
}

impl NonEmptyListValidator {
    /// Ensures that the given value is valid for the configuration with the given name.
    ///
    /// Throws an error if the value is null or an empty list.
    pub fn validate(&self, name: &str, value: &Value) -> Result<(), String> {
        ensure_valid(name, value)
    }
}

