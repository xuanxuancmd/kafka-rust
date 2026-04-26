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

//! Validator that ensures a value is a concrete subclass of a specific superclass.
//!
//! This corresponds to `org.apache.kafka.connect.util.ConcreteSubClassValidator` in Java.

use common_trait::config::Validator;
use common_trait::errors::ConfigException;
use serde_json::Value;

/// Validator that ensures a class is a concrete subclass of a specific superclass.
///
/// This corresponds to `org.apache.kafka.connect.util.ConcreteSubClassValidator` in Java.
///
/// In the Java implementation, this validator checks that a class is:
/// 1. Not the superclass itself (must be a subclass)
/// 2. Not abstract (must be concrete)
///
/// In Rust, since we don't have Java's class hierarchy, we implement this as
/// checking that a type name is not the expected type name and represents a valid
/// concrete type.
#[derive(Debug, Clone)]
pub struct ConcreteSubClassValidator {
    /// The expected superclass name
    expected_super_class: String,
}

impl ConcreteSubClassValidator {
    /// Creates a new ConcreteSubClassValidator for the given superclass.
    ///
    /// Corresponds to Java: ConcreteSubClassValidator.forSuperClass(Class<?> expectedSuperClass)
    pub fn for_super_class(expected_super_class: impl Into<String>) -> Self {
        ConcreteSubClassValidator {
            expected_super_class: expected_super_class.into(),
        }
    }

    /// Returns the expected superclass name.
    pub fn expected_super_class(&self) -> &str {
        &self.expected_super_class
    }

    /// Checks if the given type name is a valid concrete subclass.
    fn is_valid_concrete_subclass(&self, type_name: &str) -> bool {
        // Must not be the superclass itself
        if type_name == self.expected_super_class {
            return false;
        }

        // In Rust, we don't have Java's abstract classes concept.
        // We perform a simplified check: the type name must not be empty
        // and must be different from the expected superclass.
        // A more sophisticated implementation would check against a registry
        // of types and their hierarchies.
        !type_name.is_empty()
    }
}

impl Validator for ConcreteSubClassValidator {
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

        if !self.is_valid_concrete_subclass(&type_name) {
            return Err(ConfigException::new(format!(
                "Configuration {} must be a concrete subclass of {}, but got {}",
                name, self.expected_super_class, type_name
            )));
        }

        Ok(())
    }

    fn to_string(&self) -> String {
        format!("A concrete subclass of {}", self.expected_super_class)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concrete_sub_class_validator_creation() {
        let validator = ConcreteSubClassValidator::for_super_class("BaseConnector");
        assert_eq!(validator.expected_super_class(), "BaseConnector");
    }

    #[test]
    fn test_concrete_sub_class_validator_null_allowed() {
        let validator = ConcreteSubClassValidator::for_super_class("BaseConnector");
        assert!(validator.ensure_valid("test", &Value::Null).is_ok());
    }

    #[test]
    fn test_concrete_sub_class_validator_valid_subclass() {
        let validator = ConcreteSubClassValidator::for_super_class("BaseConnector");
        assert!(validator
            .ensure_valid("test", &Value::String("MyConnector".to_string()))
            .is_ok());
    }

    #[test]
    fn test_concrete_sub_class_validator_invalid_superclass() {
        let validator = ConcreteSubClassValidator::for_super_class("BaseConnector");
        assert!(validator
            .ensure_valid("test", &Value::String("BaseConnector".to_string()))
            .is_err());
    }

    #[test]
    fn test_concrete_sub_class_validator_to_string() {
        let validator = ConcreteSubClassValidator::for_super_class("BaseConnector");
        assert_eq!(
            validator.to_string(),
            "A concrete subclass of BaseConnector"
        );
    }
}
