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

//! Safe deserialization check to prevent security vulnerabilities.
//!
//! This corresponds to `org.apache.kafka.connect.util.SafeObjectInputStream` in Java.
//!
//! In Java, this class extends ObjectInputStream and blocks deserialization of
//! known dangerous classes to prevent deserialization attacks.
//!
//! In Rust, since we typically use serde for serialization/deserialization,
//! this module provides a utility to check if a type name is safe to deserialize.

use std::collections::HashSet;

/// Default set of class/package names that should not be deserialized.
///
/// These are known to be vulnerable to deserialization attacks in Java.
/// Corresponds to Java: SafeObjectInputStream.DEFAULT_NO_DESERIALIZE_CLASS_NAMES
pub fn default_no_deserialize_class_names() -> HashSet<String> {
    [
        // Apache Commons Collections - known vulnerable transformers
        "org.apache.commons.collections.functors.InvokerTransformer",
        "org.apache.commons.collections.functors.InstantiateTransformer",
        "org.apache.commons.collections4.functors.InvokerTransformer",
        "org.apache.commons.collections4.functors.InstantiateTransformer",
        // Groovy - vulnerable closures
        "org.codehaus.groovy.runtime.ConvertedClosure",
        "org.codehaus.groovy.runtime.MethodClosure",
        // Spring - ObjectFactory vulnerability
        "org.springframework.beans.factory.ObjectFactory",
        // Xalan - TemplatesImpl vulnerability
        "com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl",
        "org.apache.xalan.xsltc.trax.TemplatesImpl",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Safe deserialization checker.
///
/// This struct provides methods to check if a type is safe to deserialize,
/// preventing potential security vulnerabilities from malicious serialized data.
///
/// Corresponds to `org.apache.kafka.connect.util.SafeObjectInputStream` in Java.
#[derive(Debug, Clone)]
pub struct SafeObjectInputStreamChecker {
    /// Set of blocked class/package names
    blocked_names: HashSet<String>,
}

impl SafeObjectInputStreamChecker {
    /// Creates a new SafeObjectInputStreamChecker with the default blocked list.
    pub fn new() -> Self {
        SafeObjectInputStreamChecker {
            blocked_names: default_no_deserialize_class_names(),
        }
    }

    /// Creates a new SafeObjectInputStreamChecker with a custom blocked list.
    pub fn with_blocked_names(blocked_names: HashSet<String>) -> Self {
        SafeObjectInputStreamChecker { blocked_names }
    }

    /// Adds a name to the blocked list.
    pub fn add_blocked_name(&mut self, name: impl Into<String>) {
        self.blocked_names.insert(name.into());
    }

    /// Removes a name from the blocked list.
    pub fn remove_blocked_name(&mut self, name: &str) {
        self.blocked_names.remove(name);
    }

    /// Checks if a type name is blocked (should not be deserialized).
    ///
    /// Corresponds to Java: SafeObjectInputStream.isBlocked(String name)
    pub fn is_blocked(&self, name: &str) -> bool {
        // Check if the name ends with any blocked class name
        for blocked in &self.blocked_names {
            if name.ends_with(blocked) {
                return true;
            }
        }
        false
    }

    /// Validates that a type name is safe to deserialize.
    ///
    /// Returns an error if the type is blocked.
    pub fn validate_safe(&self, name: &str) -> Result<(), SafeDeserializationError> {
        if self.is_blocked(name) {
            return Err(SafeDeserializationError::BlockedType(name.to_string()));
        }
        Ok(())
    }

    /// Returns the blocked names.
    pub fn blocked_names(&self) -> &HashSet<String> {
        &self.blocked_names
    }
}

impl Default for SafeObjectInputStreamChecker {
    fn default() -> Self {
        Self::new()
    }
}

/// Error that occurs when attempting to deserialize a blocked type.
#[derive(Debug)]
pub enum SafeDeserializationError {
    /// The type is blocked for security reasons
    BlockedType(String),
}

impl std::fmt::Display for SafeDeserializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SafeDeserializationError::BlockedType(name) => {
                write!(
                    f,
                    "Illegal type to deserialize: '{}' prevented for security reasons",
                    name
                )
            }
        }
    }
}

impl std::error::Error for SafeDeserializationError {}

/// Trait for types that can be safely deserialized.
///
/// This trait can be implemented by types that want to provide
/// custom safety checks during deserialization.
pub trait SafeDeserializable: serde::de::DeserializeOwned {
    /// Returns true if this type is safe to deserialize.
    fn is_safe_to_deserialize() -> bool {
        true
    }

    /// Returns the type name for safety checking.
    fn type_name_for_check() -> &'static str;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_blocked_names() {
        let blocked = default_no_deserialize_class_names();
        assert!(!blocked.is_empty());
        assert!(blocked.contains("org.apache.commons.collections.functors.InvokerTransformer"));
    }

    #[test]
    fn test_safe_checker_creation() {
        let checker = SafeObjectInputStreamChecker::new();
        assert!(!checker.blocked_names().is_empty());
    }

    #[test]
    fn test_is_blocked() {
        let checker = SafeObjectInputStreamChecker::new();

        // Blocked types
        assert!(checker.is_blocked("org.apache.commons.collections.functors.InvokerTransformer"));
        assert!(checker
            .is_blocked("some.package.org.apache.commons.collections.functors.InvokerTransformer"));

        // Safe types
        assert!(!checker.is_blocked("org.apache.kafka.connect.runtime.Worker"));
        assert!(!checker.is_blocked("com.example.MyConnector"));
    }

    #[test]
    fn test_validate_safe() {
        let checker = SafeObjectInputStreamChecker::new();

        // Blocked should return error
        assert!(checker
            .validate_safe("org.apache.commons.collections.functors.InvokerTransformer")
            .is_err());

        // Safe should return ok
        assert!(checker.validate_safe("com.example.MyConnector").is_ok());
    }

    #[test]
    fn test_custom_blocked_names() {
        let mut blocked = HashSet::new();
        blocked.insert("com.example.DangerousType".to_string());

        let checker = SafeObjectInputStreamChecker::with_blocked_names(blocked);
        assert!(checker.is_blocked("com.example.DangerousType"));
        assert!(!checker.is_blocked("org.apache.commons.collections.functors.InvokerTransformer"));
    }

    #[test]
    fn test_add_remove_blocked_name() {
        let mut checker = SafeObjectInputStreamChecker::new();

        // Add a new blocked name
        checker.add_blocked_name("com.example.NewDangerousType");
        assert!(checker.is_blocked("com.example.NewDangerousType"));

        // Remove a blocked name
        checker.remove_blocked_name("com.example.NewDangerousType");
        assert!(!checker.is_blocked("com.example.NewDangerousType"));
    }

    #[test]
    fn test_error_display() {
        let error = SafeDeserializationError::BlockedType("test.Type".to_string());
        let display = format!("{}", error);
        assert!(display.contains("test.Type"));
        assert!(display.contains("security reasons"));
    }
}
