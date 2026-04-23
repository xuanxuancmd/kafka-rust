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

//! Session key for internal REST request validation in Kafka Connect.
//!
//! A session key can be used to validate internal REST requests between workers.
//! This provides security for inter-worker communication in distributed mode.
//!
//! Corresponds to Java: `org.apache.kafka.connect.runtime.SessionKey`

use serde::{Deserialize, Serialize};

/// A session key for validating internal REST requests between workers.
///
/// This struct contains the actual cryptographic key and the timestamp
/// when it was generated. The key can be used to sign/validate requests
/// between workers in a Kafka Connect cluster.
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.SessionKey`
/// (Java uses a record: `public record SessionKey(SecretKey key, long creationTimestamp)`)
///
/// Note: In Rust, we store the key as bytes rather than using Java's `SecretKey`
/// interface. This provides similar functionality with Rust's type system.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionKey {
    /// The actual cryptographic key bytes.
    /// Used for request validation between workers.
    /// May not be null.
    key: Vec<u8>,

    /// The time at which the key was generated.
    /// Stored as milliseconds since epoch.
    creation_timestamp: i64,
}

impl SessionKey {
    /// Creates a new session key.
    ///
    /// # Arguments
    /// * `key` - The cryptographic key bytes (must not be empty)
    /// * `creation_timestamp` - The timestamp when the key was generated (ms since epoch)
    ///
    /// # Panics
    /// Panics if `key` is empty (equivalent to Java's `Objects.requireNonNull`).
    ///
    /// Corresponds to Java: `SessionKey(SecretKey key, long creationTimestamp)`
    pub fn new(key: Vec<u8>, creation_timestamp: i64) -> Self {
        assert!(!key.is_empty(), "Key may not be null or empty");
        SessionKey {
            key,
            creation_timestamp,
        }
    }

    /// Returns the cryptographic key bytes.
    ///
    /// Corresponds to Java: `SessionKey.key()`
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the creation timestamp.
    ///
    /// Corresponds to Java: `SessionKey.creationTimestamp()`
    pub fn creation_timestamp(&self) -> i64 {
        self.creation_timestamp
    }
}

impl std::fmt::Display for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SessionKey{{creationTimestamp={}, keyLength={}}}",
            self.creation_timestamp,
            self.key.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_key_new() {
        let key_bytes = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let timestamp = 1234567890;
        let session_key = SessionKey::new(key_bytes.clone(), timestamp);

        assert_eq!(session_key.key(), &key_bytes);
        assert_eq!(session_key.creation_timestamp(), timestamp);
    }

    #[test]
    #[should_panic(expected = "Key may not be null or empty")]
    fn test_session_key_empty_key_panics() {
        SessionKey::new(vec![], 1234567890);
    }

    #[test]
    fn test_session_key_display() {
        let session_key = SessionKey::new(vec![0u8; 16], 1234567890);
        let display = format!("{}", session_key);
        assert!(display.contains("1234567890"));
        assert!(display.contains("16"));
    }

    #[test]
    fn test_session_key_clone() {
        let key = SessionKey::new(vec![1, 2, 3], 100);
        let cloned = key.clone();
        assert_eq!(key, cloned);
    }

    #[test]
    fn test_session_key_serialization() {
        let key = SessionKey::new(vec![1, 2, 3, 4], 1234567890);
        let serialized = serde_json::to_string(&key).unwrap();
        let deserialized: SessionKey = serde_json::from_str(&serialized).unwrap();
        assert_eq!(key, deserialized);
    }
}
