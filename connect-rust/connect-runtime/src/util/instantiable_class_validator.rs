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

//! Validator that ensures a class can be instantiated with a no-argument constructor.
//!
//! This corresponds to `org.apache.kafka.connect.util.InstantiableClassValidator` in Java.

use common_trait::config::Validator;
use common_trait::errors::ConfigException;
use serde_json::Value;

/// Validator that ensures a class can be instantiated.
///
/// This corresponds to `org.apache.kafka.connect.util.InstantiableClassValidator` in Java.
///
/// In the Java implementation, this validator attempts to:
/// 1. Find a public no-argument constructor
/// 2. Instantiate the class
/// 3. Close the instance if it implements Closeable
///
/// In Rust, since we don't have runtime class instantiation like Java,
/// we implement this as checking that the type name is valid and represents
/// a type that could potentially be instantiated. The actual instantiation
/// would be handled by the connector/task factory.
#[derive(Debug, Clone)]
pub struct InstantiableClassValidator;

impl InstantiableClassValidator {
    /// Creates a new InstantiableClassValidator.
    pub fn new() -> Self {
        InstantiableClassValidator
    }

    /// Checks if the given type name appears to be a valid instantiable type.
    fn is_valid_instantiable_type(type_name: &str) -> bool {
        // Basic validation checks
        // - Must not be empty
        // - Must not contain invalid characters
        // - Must look like a valid type name

        if type_name.is_empty() {
            return false;
        }

        // Check for common invalid patterns
        // In Java, abstract classes and interfaces can't be instantiated
        // In Rust, we check for obvious patterns that indicate non-instantiable types
        let invalid_patterns = ["Abstract", "Interface", "Trait", "Base"];

        // Check if type name starts with invalid patterns (indicating abstract/base)
        // This is a simplified heuristic - a real implementation would have a type registry
        for pattern in &invalid_patterns {
            if type_name.starts_with(pattern) {
                return false;
            }
        }

        true
    }
}

impl Default for InstantiableClassValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator for InstantiableClassValidator {
    fn ensure_valid(&self, name: &str, value: &Value) -> Result<(), ConfigException> {
        // Null is allowed - the value will be null if the class couldn't be found
        if value.is_null() {
            return Ok(());
        }

        // Get the class name from the value
        let type_name = match value {
            Value::String(s) => s.clone(),
            Value::Object(obj) => {
                // If it's an object, try to get the "class" or "type" field
                obj.get("class")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        obj.get("type")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_else(|| value.to_string())
            }
            other => other.to_string(),
        };

        if !Self::is_valid_instantiable_type(&type_name) {
            return Err(ConfigException::new(format!(
                "Configuration {} must be a class with a public, no-argument constructor, but got {}",
                name, type_name
            )));
        }

        Ok(())
    }

    fn to_string(&self) -> String {
        "A class with a public, no-argument constructor".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instantiable_class_validator_creation() {
        let validator = InstantiableClassValidator::new();
        let _ = validator; // Just verify creation
    }

    #[test]
    fn test_instantiable_class_validator_null_allowed() {
        let validator = InstantiableClassValidator::new();
        assert!(validator.ensure_valid("test", &Value::Null).is_ok());
    }

    #[test]
    fn test_instantiable_class_validator_valid_type() {
        let validator = InstantiableClassValidator::new();
        assert!(validator
            .ensure_valid("test", &Value::String("MyConnector".to_string()))
            .is_ok());
    }

    #[test]
    fn test_instantiable_class_validator_invalid_abstract() {
        let validator = InstantiableClassValidator::new();
        assert!(validator
            .ensure_valid("test", &Value::String("AbstractConnector".to_string()))
            .is_err());
    }

    #[test]
    fn test_instantiable_class_validator_invalid_empty() {
        let validator = InstantiableClassValidator::new();
        assert!(validator
            .ensure_valid("test", &Value::String("".to_string()))
            .is_err());
    }

    #[test]
    fn test_instantiable_class_validator_to_string() {
        let validator = InstantiableClassValidator::new();
        assert_eq!(
            validator.to_string(),
            "A class with a public, no-argument constructor"
        );
    }
}
