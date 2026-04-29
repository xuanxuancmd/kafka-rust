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

//! Validators for configuration values.
//!
//! This module provides validators for configuration values, corresponding to
//! the inner classes in `org.apache.kafka.common.config.ConfigDef.Validators` in Java.

use crate::errors::ConfigException;
use regex::Regex;
use serde_json::Value;
use std::collections::HashSet;
use std::fmt;

/// Validator trait for validating configuration values.
///
/// This corresponds to `org.apache.kafka.common.config.ConfigDef.Validator` in Java.
pub trait Validator: fmt::Debug + Send + Sync {
    /// Ensures that the given value is valid for the configuration with the given name.
    ///
    /// Throws a `ConfigException` if the value is invalid.
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException>;

    /// Returns a string representation of this validator.
    fn to_string(&self) -> String;
}

// ============================================================================
// NonEmptyStringValidator
// ============================================================================

/// Validator that ensures a string value is not empty.
///
/// This corresponds to `ConfigDef.NonEmptyString` in Java.
#[derive(Debug, Clone)]
pub struct NonEmptyStringValidator;

impl NonEmptyStringValidator {
    /// Creates a new NonEmptyStringValidator.
    pub fn new() -> Self {
        NonEmptyStringValidator
    }
}

impl Default for NonEmptyStringValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator for NonEmptyStringValidator {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        match value {
            Value::Null => {
                // Null is allowed for string configurations
                Ok(())
            }
            Value::String(s) => {
                if s.is_empty() {
                    Err(ConfigException::new(format!(
                        "Empty string is not a valid value for configuration {}",
                        name
                    )))
                } else {
                    Ok(())
                }
            }
            _ => Err(ConfigException::new(format!(
                "Expected a string value for configuration {}, but got {}",
                name, value
            ))),
        }
    }

    fn to_string(&self) -> String {
        "NonEmptyString".to_string()
    }
}

// ============================================================================
// NonEmptyListValidator
// ============================================================================

/// Validator that ensures a list value is not empty.
///
/// This corresponds to the functionality in `ConfigDef.ValidList` with `isEmptyAllowed=false`.
#[derive(Debug, Clone)]
pub struct NonEmptyListValidator {
    /// Whether null values are allowed
    is_null_allowed: bool,
}

impl NonEmptyListValidator {
    /// Creates a new NonEmptyListValidator that does not allow null.
    pub fn new() -> Self {
        NonEmptyListValidator {
            is_null_allowed: false,
        }
    }

    /// Creates a new NonEmptyListValidator with specified null allowance.
    pub fn with_null_allowed(is_null_allowed: bool) -> Self {
        NonEmptyListValidator { is_null_allowed }
    }
}

impl Default for NonEmptyListValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator for NonEmptyListValidator {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        match value {
            Value::Null => {
                if self.is_null_allowed {
                    Ok(())
                } else {
                    Err(ConfigException::new(format!(
                        "Null value is not allowed for configuration {}",
                        name
                    )))
                }
            }
            Value::Array(arr) => {
                if arr.is_empty() {
                    Err(ConfigException::new(format!(
                        "Empty list is not a valid value for configuration {}",
                        name
                    )))
                } else {
                    // Check for duplicates
                    let mut seen = HashSet::new();
                    for item in arr {
                        let key = item.to_string();
                        if seen.contains(&key) {
                            Err(ConfigException::new(format!(
                                "Duplicate entry {} found in configuration {}",
                                key, name
                            )))?;
                        }
                        seen.insert(key);
                    }
                    Ok(())
                }
            }
            _ => Err(ConfigException::new(format!(
                "Expected a list value for configuration {}, but got {}",
                name, value
            ))),
        }
    }

    fn to_string(&self) -> String {
        if self.is_null_allowed {
            "NonEmptyList (null allowed)".to_string()
        } else {
            "NonEmptyList".to_string()
        }
    }
}

// ============================================================================
// RegexValidator
// ============================================================================

/// Validator that ensures a string value matches a regular expression.
///
/// This corresponds to the regex validation functionality in Kafka.
#[derive(Debug, Clone)]
pub struct RegexValidator {
    pattern: Regex,
    pattern_str: String,
}

impl RegexValidator {
    /// Creates a new RegexValidator with the given pattern.
    pub fn new(pattern: impl Into<String>) -> Self {
        let pattern_str = pattern.into();
        let regex = Regex::new(&pattern_str).expect("Invalid regex pattern");
        RegexValidator {
            pattern: regex,
            pattern_str,
        }
    }

    /// Returns the pattern string.
    pub fn pattern(&self) -> &str {
        &self.pattern_str
    }
}

impl Validator for RegexValidator {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        match value {
            Value::Null => {
                // Null is allowed
                Ok(())
            }
            Value::String(s) => {
                if self.pattern.is_match(s) {
                    Ok(())
                } else {
                    Err(ConfigException::new(format!(
                        "Configuration {} does not match pattern {}: value {}",
                        name, self.pattern_str, s
                    )))
                }
            }
            _ => Err(ConfigException::new(format!(
                "Expected a string value for configuration {}, but got {}",
                name, value
            ))),
        }
    }

    fn to_string(&self) -> String {
        format!("RegexValidator({})", self.pattern_str)
    }
}

// ============================================================================
// LambdaValidator
// ============================================================================

/// Validator that uses a custom function to validate values.
///
/// This corresponds to `ConfigDef.LambdaValidator` in Java.
pub struct LambdaValidator {
    ensure_valid_fn: Box<dyn Fn(&str, &Value) -> Result<(), ConfigException> + Send + Sync>,
    to_string_fn: Box<dyn Fn() -> String + Send + Sync>,
}

impl fmt::Debug for LambdaValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LambdaValidator")
            .field("ensure_valid_fn", &"<function>")
            .field("to_string_fn", &"<function>")
            .finish()
    }
}

impl LambdaValidator {
    /// Creates a new LambdaValidator with the given validation and string functions.
    pub fn new(
        ensure_valid_fn: impl Fn(&str, &Value) -> Result<(), ConfigException> + Send + Sync + 'static,
        to_string_fn: impl Fn() -> String + Send + Sync + 'static,
    ) -> Self {
        LambdaValidator {
            ensure_valid_fn: Box::new(ensure_valid_fn),
            to_string_fn: Box::new(to_string_fn),
        }
    }

    /// Creates a LambdaValidator from closures.
    pub fn with(
        ensure_valid: impl Fn(&str, &Value) -> Result<(), ConfigException> + Send + Sync + 'static,
        to_string: impl Fn() -> String + Send + Sync + 'static,
    ) -> Self {
        Self::new(ensure_valid, to_string)
    }
}

impl Validator for LambdaValidator {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        (self.ensure_valid_fn)(name, value)
    }

    fn to_string(&self) -> String {
        (self.to_string_fn)()
    }
}

// ============================================================================
// CompositeValidator
// ============================================================================

/// Validator that combines multiple validators.
///
/// This corresponds to `ConfigDef.CompositeValidator` in Java.
#[derive(Debug)]
pub struct CompositeValidator {
    validators: Vec<Box<dyn Validator>>,
}

impl CompositeValidator {
    /// Creates a new CompositeValidator with the given validators.
    pub fn new(validators: Vec<Box<dyn Validator>>) -> Self {
        CompositeValidator { validators }
    }

    /// Creates a CompositeValidator from an array of validators.
    pub fn of(validators: Vec<Box<dyn Validator>>) -> Self {
        Self::new(validators)
    }

    /// Returns the validators.
    pub fn validators(&self) -> &[Box<dyn Validator>] {
        &self.validators
    }
}

impl Validator for CompositeValidator {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        for validator in &self.validators {
            validator.ensure_valid(name, value)?;
        }
        Ok(())
    }

    fn to_string(&self) -> String {
        let validator_strs: Vec<String> = self.validators.iter().map(|v| v.to_string()).collect();
        format!("CompositeValidator([{}])", validator_strs.join(", "))
    }
}

// ============================================================================
// RangeValidator
// ============================================================================

/// Validator that ensures a numeric value is within a specified range.
///
/// This corresponds to `ConfigDef.Range` in Java.
#[derive(Debug, Clone)]
pub struct RangeValidator {
    min: Option<f64>,
    max: Option<f64>,
}

impl RangeValidator {
    /// Creates a RangeValidator with at least a minimum value.
    pub fn at_least(min: f64) -> Self {
        RangeValidator {
            min: Some(min),
            max: None,
        }
    }

    /// Creates a RangeValidator with a range between min and max (inclusive).
    pub fn between(min: f64, max: f64) -> Self {
        RangeValidator {
            min: Some(min),
            max: Some(max),
        }
    }

    /// Returns the minimum value.
    pub fn min(&self) -> Option<f64> {
        self.min
    }

    /// Returns the maximum value.
    pub fn max(&self) -> Option<f64> {
        self.max
    }
}

impl Validator for RangeValidator {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        match value {
            Value::Null => {
                // Null is allowed for numeric configurations
                Ok(())
            }
            Value::Number(n) => {
                let num = n.as_f64().unwrap_or(0.0);

                if let Some(min) = self.min {
                    if num < min {
                        Err(ConfigException::new(format!(
                            "Configuration {} must be at least {}, but got {}",
                            name, min, num
                        )))?;
                    }
                }

                if let Some(max) = self.max {
                    if num > max {
                        Err(ConfigException::new(format!(
                            "Configuration {} must be at most {}, but got {}",
                            name, max, num
                        )))?;
                    }
                }

                Ok(())
            }
            _ => Err(ConfigException::new(format!(
                "Expected a numeric value for configuration {}, but got {}",
                name, value
            ))),
        }
    }

    fn to_string(&self) -> String {
        match (self.min, self.max) {
            (Some(min), Some(max)) => format!("RangeValidator({}..{})", min, max),
            (Some(min), None) => format!("RangeValidator(atLeast {})", min),
            (None, Some(max)) => format!("RangeValidator(atMost {})", max),
            (None, None) => "RangeValidator(any)".to_string(),
        }
    }
}

// ============================================================================
// ValidString
// ============================================================================

/// Validator that ensures a string value is one of a predefined set of valid strings.
///
/// This corresponds to `ConfigDef.ValidString` in Java.
#[derive(Debug, Clone)]
pub struct ValidString {
    valid_strings: Vec<String>,
}

impl ValidString {
    /// Creates a ValidString validator with the given valid strings.
    pub fn in_strings(valid_strings: Vec<String>) -> Self {
        ValidString { valid_strings }
    }

    /// Creates a ValidString validator from an array of strings.
    pub fn in_values(valid_strings: &[&str]) -> Self {
        ValidString {
            valid_strings: valid_strings.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Returns the valid strings.
    pub fn valid_strings(&self) -> &[String] {
        &self.valid_strings
    }
}

impl Validator for ValidString {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        match value {
            Value::Null => {
                // Null is allowed
                Ok(())
            }
            Value::String(s) => {
                if self.valid_strings.contains(s) {
                    Ok(())
                } else {
                    Err(ConfigException::new(format!(
                        "Configuration {} must be one of {}, but got {}",
                        name,
                        self.valid_strings.join(", "),
                        s
                    )))
                }
            }
            _ => Err(ConfigException::new(format!(
                "Expected a string value for configuration {}, but got {}",
                name, value
            ))),
        }
    }

    fn to_string(&self) -> String {
        format!("ValidString([{}])", self.valid_strings.join(", "))
    }
}

// ============================================================================
// CaseInsensitiveValidString
// ============================================================================

/// Validator that ensures a string value is one of a predefined set of valid strings,
/// with case-insensitive comparison.
///
/// This corresponds to `ConfigDef.CaseInsensitiveValidString` in Java.
#[derive(Debug, Clone)]
pub struct CaseInsensitiveValidString {
    valid_strings: Vec<String>,
    valid_strings_uppercase: Vec<String>,
}

impl CaseInsensitiveValidString {
    /// Creates a CaseInsensitiveValidString validator with the given valid strings.
    pub fn in_strings(valid_strings: Vec<String>) -> Self {
        let valid_strings_uppercase = valid_strings.iter().map(|s| s.to_uppercase()).collect();
        CaseInsensitiveValidString {
            valid_strings,
            valid_strings_uppercase,
        }
    }

    /// Creates a CaseInsensitiveValidString validator from an array of strings.
    pub fn in_values(valid_strings: &[&str]) -> Self {
        let valid_strings: Vec<String> = valid_strings.iter().map(|s| s.to_string()).collect();
        let valid_strings_uppercase = valid_strings.iter().map(|s| s.to_uppercase()).collect();
        CaseInsensitiveValidString {
            valid_strings,
            valid_strings_uppercase,
        }
    }

    /// Returns the valid strings.
    pub fn valid_strings(&self) -> &[String] {
        &self.valid_strings
    }
}

impl Validator for CaseInsensitiveValidString {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        match value {
            Value::Null => {
                // Null is allowed
                Ok(())
            }
            Value::String(s) => {
                let s_upper = s.to_uppercase();
                if self.valid_strings_uppercase.contains(&s_upper) {
                    Ok(())
                } else {
                    Err(ConfigException::new(format!(
                        "Configuration {} must be one of {} (case-insensitive), but got {}",
                        name,
                        self.valid_strings.join(", "),
                        s
                    )))
                }
            }
            _ => Err(ConfigException::new(format!(
                "Expected a string value for configuration {}, but got {}",
                name, value
            ))),
        }
    }

    fn to_string(&self) -> String {
        format!(
            "CaseInsensitiveValidString([{}])",
            self.valid_strings.join(", ")
        )
    }
}

// ============================================================================
// ValidList
// ============================================================================

/// Validator that ensures list values are valid.
///
/// This corresponds to `ConfigDef.ValidList` in Java.
#[derive(Debug, Clone)]
pub struct ValidList {
    valid_strings: Vec<String>,
    is_empty_allowed: bool,
    is_null_allowed: bool,
}

impl ValidList {
    /// Creates a ValidList validator that allows any non-duplicate values.
    pub fn any_non_duplicate_values(is_empty_allowed: bool, is_null_allowed: bool) -> Self {
        ValidList {
            valid_strings: Vec::new(),
            is_empty_allowed,
            is_null_allowed,
        }
    }

    /// Creates a ValidList validator with the given valid strings.
    pub fn in_strings(valid_strings: Vec<String>) -> Self {
        ValidList {
            valid_strings,
            is_empty_allowed: true,
            is_null_allowed: false,
        }
    }

    /// Creates a ValidList validator from an array of strings.
    pub fn in_values(valid_strings: &[&str]) -> Self {
        ValidList {
            valid_strings: valid_strings.iter().map(|s| s.to_string()).collect(),
            is_empty_allowed: true,
            is_null_allowed: false,
        }
    }

    /// Creates a ValidList validator with the given valid strings and empty allowance.
    pub fn in_with_empty_allowed(is_empty_allowed: bool, valid_strings: &[&str]) -> Self {
        ValidList {
            valid_strings: valid_strings.iter().map(|s| s.to_string()).collect(),
            is_empty_allowed,
            is_null_allowed: false,
        }
    }

    /// Returns the valid strings.
    pub fn valid_strings(&self) -> &[String] {
        &self.valid_strings
    }

    /// Returns whether empty lists are allowed.
    pub fn is_empty_allowed(&self) -> bool {
        self.is_empty_allowed
    }

    /// Returns whether null values are allowed.
    pub fn is_null_allowed(&self) -> bool {
        self.is_null_allowed
    }
}

impl Validator for ValidList {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        match value {
            Value::Null => {
                if self.is_null_allowed {
                    Ok(())
                } else {
                    Err(ConfigException::new(format!(
                        "Null value is not allowed for configuration {}",
                        name
                    )))
                }
            }
            Value::Array(arr) => {
                if !self.is_empty_allowed && arr.is_empty() {
                    Err(ConfigException::new(format!(
                        "Empty list is not allowed for configuration {}",
                        name
                    )))?;
                }

                // Check for duplicates
                let mut seen = HashSet::new();
                for item in arr {
                    let key = match item {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    if seen.contains(&key) {
                        Err(ConfigException::new(format!(
                            "Duplicate entry {} found in configuration {}",
                            key, name
                        )))?;
                    }
                    seen.insert(key);

                    // Check if string is valid (if we have valid_strings)
                    if !self.valid_strings.is_empty() {
                        if let Value::String(s) = item {
                            if !self.valid_strings.contains(s) {
                                Err(ConfigException::new(format!(
                                    "Value {} in configuration {} is not valid. Valid values are: {}",
                                    s,
                                    name,
                                    self.valid_strings.join(", ")
                                )))?;
                            }
                        }
                    }
                }

                Ok(())
            }
            _ => Err(ConfigException::new(format!(
                "Expected a list value for configuration {}, but got {}",
                name, value
            ))),
        }
    }

    fn to_string(&self) -> String {
        if self.valid_strings.is_empty() {
            format!(
                "ValidList(anyNonDuplicateValues, emptyAllowed={}, nullAllowed={})",
                self.is_empty_allowed, self.is_null_allowed
            )
        } else {
            format!(
                "ValidList([{}], emptyAllowed={})",
                self.valid_strings.join(", "),
                self.is_empty_allowed
            )
        }
    }
}

// ============================================================================
// NonNullValidator
// ============================================================================

/// Validator that ensures a value is not null.
///
/// This corresponds to `ConfigDef.NonNullValidator` in Java.
#[derive(Debug, Clone)]
pub struct NonNullValidator;

impl NonNullValidator {
    /// Creates a new NonNullValidator.
    pub fn new() -> Self {
        NonNullValidator
    }
}

impl Default for NonNullValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator for NonNullValidator {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        if value.is_null() {
            Err(ConfigException::new(format!(
                "Configuration {} cannot be null",
                name
            )))
        } else {
            Ok(())
        }
    }

    fn to_string(&self) -> String {
        "NonNullValidator".to_string()
    }
}

// ============================================================================
// NonEmptyStringWithoutControlCharsValidator
// ============================================================================

/// Validator that ensures a string value is not empty and does not contain control characters.
///
/// This corresponds to `ConfigDef.NonEmptyStringWithoutControlChars` in Java.
#[derive(Debug, Clone)]
pub struct NonEmptyStringWithoutControlCharsValidator;

impl NonEmptyStringWithoutControlCharsValidator {
    /// Creates a new NonEmptyStringWithoutControlCharsValidator.
    pub fn new() -> Self {
        NonEmptyStringWithoutControlCharsValidator
    }
}

impl Default for NonEmptyStringWithoutControlCharsValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator for NonEmptyStringWithoutControlCharsValidator {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        match value {
            Value::Null => {
                // Null is allowed
                Ok(())
            }
            Value::String(s) => {
                if s.is_empty() {
                    Err(ConfigException::new(format!(
                        "Empty string is not a valid value for configuration {}",
                        name
                    )))?;
                }

                // Check for control characters (ISO control characters are chars with code points < 32 or between 127 and 159)
                for c in s.chars() {
                    if c.is_control() {
                        Err(ConfigException::new(format!(
                            "Configuration {} contains control character (code point {}) which is not allowed",
                            name,
                            c as u32
                        )))?;
                    }
                }

                Ok(())
            }
            _ => Err(ConfigException::new(format!(
                "Expected a string value for configuration {}, but got {}",
                name, value
            ))),
        }
    }

    fn to_string(&self) -> String {
        "NonEmptyStringWithoutControlChars".to_string()
    }
}

// ============================================================================
// ListSizeValidator
// ============================================================================

/// Validator that ensures a list size does not exceed a maximum.
///
/// This corresponds to `ConfigDef.ListSize` in Java.
#[derive(Debug, Clone)]
pub struct ListSizeValidator {
    max_size: usize,
}

impl ListSizeValidator {
    /// Creates a ListSizeValidator with the given maximum size.
    pub fn at_most_of_size(max_size: usize) -> Self {
        ListSizeValidator { max_size }
    }

    /// Returns the maximum size.
    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

impl Validator for ListSizeValidator {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        match value {
            Value::Null => {
                // Null is allowed
                Ok(())
            }
            Value::Array(arr) => {
                if arr.len() > self.max_size {
                    Err(ConfigException::new(format!(
                        "Configuration {} list size {} exceeds maximum allowed size {}",
                        name,
                        arr.len(),
                        self.max_size
                    )))
                } else {
                    Ok(())
                }
            }
            _ => Err(ConfigException::new(format!(
                "Expected a list value for configuration {}, but got {}",
                name, value
            ))),
        }
    }

    fn to_string(&self) -> String {
        format!("ListSizeValidator(atMost {})", self.max_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_non_empty_string_validator() {
        let validator = NonEmptyStringValidator::new();

        // Null should be allowed
        assert!(validator.ensure_valid("test", &Value::Null).is_ok());

        // Empty string should fail
        assert!(validator
            .ensure_valid("test", &Value::String(String::new()))
            .is_err());

        // Non-empty string should pass
        assert!(validator
            .ensure_valid("test", &Value::String("hello".to_string()))
            .is_ok());
    }

    #[test]
    fn test_non_empty_list_validator() {
        let validator = NonEmptyListValidator::new();

        // Null should fail
        assert!(validator.ensure_valid("test", &Value::Null).is_err());

        // Empty list should fail
        assert!(validator
            .ensure_valid("test", &Value::Array(vec![]))
            .is_err());

        // Non-empty list should pass
        assert!(validator
            .ensure_valid("test", &Value::Array(vec![Value::String("a".to_string())]))
            .is_ok());

        // Duplicate entries should fail
        assert!(validator
            .ensure_valid(
                "test",
                &Value::Array(vec![
                    Value::String("a".to_string()),
                    Value::String("a".to_string()),
                ])
            )
            .is_err());
    }

    #[test]
    fn test_regex_validator() {
        let validator = RegexValidator::new("^[a-z]+$");

        // Null should be allowed
        assert!(validator.ensure_valid("test", &Value::Null).is_ok());

        // Matching string should pass
        assert!(validator
            .ensure_valid("test", &Value::String("abc".to_string()))
            .is_ok());

        // Non-matching string should fail
        assert!(validator
            .ensure_valid("test", &Value::String("ABC".to_string()))
            .is_err());
    }

    #[test]
    fn test_lambda_validator() {
        let validator = LambdaValidator::new(
            |name, value| {
                if value.is_null() {
                    Ok(())
                } else if let Value::String(s) = value {
                    if s.starts_with("test") {
                        Ok(())
                    } else {
                        Err(ConfigException::new(format!(
                            "Configuration {} must start with 'test'",
                            name
                        )))
                    }
                } else {
                    Err(ConfigException::new(format!(
                        "Expected string for configuration {}",
                        name
                    )))
                }
            },
            || "Custom validator".to_string(),
        );

        assert!(validator.ensure_valid("test", &Value::Null).is_ok());
        assert!(validator
            .ensure_valid("test", &Value::String("test123".to_string()))
            .is_ok());
        assert!(validator
            .ensure_valid("test", &Value::String("abc".to_string()))
            .is_err());
    }

    #[test]
    fn test_composite_validator() {
        let validator = CompositeValidator::new(vec![
            Box::new(NonEmptyStringValidator::new()),
            Box::new(RegexValidator::new("^[a-z]+$")),
        ]);

        // Must pass both validators
        assert!(validator
            .ensure_valid("test", &Value::String("abc".to_string()))
            .is_ok());

        // Empty string fails NonEmptyStringValidator
        assert!(validator
            .ensure_valid("test", &Value::String(String::new()))
            .is_err());

        // Numbers fail regex validator
        assert!(validator
            .ensure_valid("test", &Value::String("abc123".to_string()))
            .is_err());
    }

    #[test]
    fn test_range_validator_at_least() {
        let validator = RangeValidator::at_least(0.0);

        assert!(validator.ensure_valid("test", &Value::Null).is_ok());
        assert!(validator
            .ensure_valid("test", &serde_json::json!(5))
            .is_ok());
        assert!(validator
            .ensure_valid("test", &serde_json::json!(-1))
            .is_err());
    }

    #[test]
    fn test_range_validator_between() {
        let validator = RangeValidator::between(0.0, 100.0);

        assert!(validator.ensure_valid("test", &Value::Null).is_ok());
        assert!(validator
            .ensure_valid("test", &serde_json::json!(50))
            .is_ok());
        assert!(validator
            .ensure_valid("test", &serde_json::json!(-1))
            .is_err());
        assert!(validator
            .ensure_valid("test", &serde_json::json!(101))
            .is_err());
    }

    #[test]
    fn test_valid_string() {
        let validator = ValidString::in_values(&["a", "b", "c"]);

        assert!(validator.ensure_valid("test", &Value::Null).is_ok());
        assert!(validator
            .ensure_valid("test", &Value::String("a".to_string()))
            .is_ok());
        assert!(validator
            .ensure_valid("test", &Value::String("d".to_string()))
            .is_err());
    }

    #[test]
    fn test_case_insensitive_valid_string() {
        let validator = CaseInsensitiveValidString::in_values(&["A", "B", "C"]);

        assert!(validator.ensure_valid("test", &Value::Null).is_ok());
        assert!(validator
            .ensure_valid("test", &Value::String("a".to_string()))
            .is_ok());
        assert!(validator
            .ensure_valid("test", &Value::String("A".to_string()))
            .is_ok());
        assert!(validator
            .ensure_valid("test", &Value::String("d".to_string()))
            .is_err());
    }

    #[test]
    fn test_valid_list_any_non_duplicate() {
        let validator = ValidList::any_non_duplicate_values(false, false);

        // Null should fail
        assert!(validator.ensure_valid("test", &Value::Null).is_err());

        // Empty should fail (empty not allowed)
        assert!(validator
            .ensure_valid("test", &Value::Array(vec![]))
            .is_err());

        // Valid non-empty list
        assert!(validator
            .ensure_valid(
                "test",
                &Value::Array(vec![
                    Value::String("a".to_string()),
                    Value::String("b".to_string()),
                ])
            )
            .is_ok());

        // Duplicate should fail
        assert!(validator
            .ensure_valid(
                "test",
                &Value::Array(vec![
                    Value::String("a".to_string()),
                    Value::String("a".to_string()),
                ])
            )
            .is_err());
    }

    #[test]
    fn test_valid_list_in() {
        let validator = ValidList::in_values(&["a", "b", "c"]);

        assert!(validator.ensure_valid("test", &Value::Null).is_err());
        assert!(validator
            .ensure_valid("test", &Value::Array(vec![]))
            .is_ok()); // empty allowed by default
        assert!(validator
            .ensure_valid("test", &Value::Array(vec![Value::String("a".to_string())]))
            .is_ok());
        assert!(validator
            .ensure_valid("test", &Value::Array(vec![Value::String("d".to_string())]))
            .is_err());
    }

    #[test]
    fn test_non_null_validator() {
        let validator = NonNullValidator::new();

        assert!(validator.ensure_valid("test", &Value::Null).is_err());
        assert!(validator
            .ensure_valid("test", &Value::String("any".to_string()))
            .is_ok());
    }

    #[test]
    fn test_non_empty_string_without_control_chars() {
        let validator = NonEmptyStringWithoutControlCharsValidator::new();

        assert!(validator.ensure_valid("test", &Value::Null).is_ok());
        assert!(validator
            .ensure_valid("test", &Value::String(String::new()))
            .is_err());
        assert!(validator
            .ensure_valid("test", &Value::String("normal".to_string()))
            .is_ok());
        assert!(validator
            .ensure_valid("test", &Value::String("with\u{0001}control".to_string()))
            .is_err());
    }

    #[test]
    fn test_list_size_validator() {
        let validator = ListSizeValidator::at_most_of_size(3);

        assert!(validator.ensure_valid("test", &Value::Null).is_ok());
        assert!(validator
            .ensure_valid("test", &Value::Array(vec![]))
            .is_ok());
        assert!(validator
            .ensure_valid(
                "test",
                &Value::Array(vec![
                    Value::String("a".to_string()),
                    Value::String("b".to_string()),
                    Value::String("c".to_string()),
                ])
            )
            .is_ok());
        assert!(validator
            .ensure_valid(
                "test",
                &Value::Array(vec![
                    Value::String("a".to_string()),
                    Value::String("b".to_string()),
                    Value::String("c".to_string()),
                    Value::String("d".to_string()),
                ])
            )
            .is_err());
    }
}
