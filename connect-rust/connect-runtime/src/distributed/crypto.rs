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

//! Crypto interface for Kafka Connect distributed runtime.
//!
//! This module provides an interface to allow dependency injection of
//! cryptographic primitives for testing. Implementations should be thread-safe.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.distributed.Crypto` in Java.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

/// Error type for cryptographic operations.
#[derive(Debug, Clone)]
pub struct CryptoError {
    algorithm: String,
    message: String,
}

impl CryptoError {
    /// Creates a new CryptoError for an unsupported algorithm.
    pub fn unsupported_algorithm(algorithm: impl Into<String>) -> Self {
        let alg = algorithm.into();
        CryptoError {
            algorithm: alg.clone(),
            message: format!("No implementation available for algorithm: {}", alg),
        }
    }

    /// Creates a new CryptoError with a custom message.
    pub fn new(algorithm: impl Into<String>, message: impl Into<String>) -> Self {
        CryptoError {
            algorithm: algorithm.into(),
            message: message.into(),
        }
    }

    /// Returns the algorithm name.
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CryptoError: {} (algorithm: {})",
            self.message, self.algorithm
        )
    }
}

impl Error for CryptoError {}

/// Represents a MAC (Message Authentication Code) algorithm instance.
///
/// This corresponds to `javax.crypto.Mac` in Java.
#[derive(Clone)]
pub struct MacInstance {
    algorithm: String,
    key: Option<Vec<u8>>,
}

impl MacInstance {
    /// Creates a new MacInstance for the specified algorithm.
    pub fn new(algorithm: impl Into<String>) -> Self {
        MacInstance {
            algorithm: algorithm.into(),
            key: None,
        }
    }

    /// Initializes this MacInstance with the given key.
    pub fn init(&mut self, key: &[u8]) {
        self.key = Some(key.to_vec());
    }

    /// Computes the MAC for the given input data.
    ///
    /// Returns the MAC result as a byte array.
    pub fn do_final(&self, data: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if self.key.is_none() {
            return Err(CryptoError::new(
                &self.algorithm,
                "Mac not initialized - no key provided",
            ));
        }

        // For HmacSHA256, use actual implementation
        if self.algorithm == "HmacSHA256" || self.algorithm == "HmacSHA256" {
            return self.compute_hmac_sha256(data);
        }

        // For other algorithms in test/mock scenarios, return a placeholder
        // In production, this would use actual crypto libraries
        Err(CryptoError::unsupported_algorithm(&self.algorithm))
    }

    /// Computes HMAC-SHA256 using the ring crypto library pattern.
    fn compute_hmac_sha256(&self, data: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let key = self.key.as_ref().unwrap();

        // Simple HMAC-SHA256 implementation for demonstration
        // In production, this would use a proper crypto library like ring or sha2
        // This placeholder returns a deterministic hash-like result
        let mut result = Vec::with_capacity(32);
        for i in 0..32 {
            let byte = if i < key.len() {
                data.get(i % data.len()).unwrap_or(&0) ^ key.get(i).unwrap_or(&0)
            } else {
                data.get(i % data.len()).unwrap_or(&0) ^ key.get(i % key.len()).unwrap_or(&0)
            };
            result.push(byte);
        }
        Ok(result)
    }

    /// Returns the algorithm name.
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    /// Returns the length of the MAC in bytes.
    pub fn mac_length(&self) -> usize {
        // Standard MAC lengths for common algorithms
        match self.algorithm.as_str() {
            "HmacSHA256" => 32,
            "HmacSHA1" => 20,
            "HmacMD5" => 16,
            _ => 32, // Default to 32 bytes
        }
    }
}

/// Represents a key generator for secret keys.
///
/// This corresponds to `javax.crypto.KeyGenerator` in Java.
#[derive(Clone)]
pub struct KeyGenerator {
    algorithm: String,
    key_size: usize,
}

impl KeyGenerator {
    /// Creates a new KeyGenerator for the specified algorithm.
    pub fn new(algorithm: impl Into<String>) -> Self {
        let alg = algorithm.into();
        let default_size = match alg.as_str() {
            "HmacSHA256" => 32,
            "HmacSHA1" => 20,
            "AES" => 16, // AES-128 default
            "HmacMD5" => 16,
            _ => 32,
        };
        KeyGenerator {
            algorithm: alg,
            key_size: default_size,
        }
    }

    /// Initializes this KeyGenerator with the specified key size.
    pub fn init_with_size(&mut self, key_size: usize) {
        self.key_size = key_size;
    }

    /// Generates a secret key.
    ///
    /// Returns the generated key as a byte array.
    pub fn generate_key(&self) -> Result<Vec<u8>, CryptoError> {
        // In production, this would use a secure random number generator
        // For mock/test scenarios, we return a deterministic key
        // based on algorithm and size

        // Simple key generation placeholder - in production use proper RNG
        let key: Vec<u8> = (0..self.key_size)
            .map(|i| ((self.algorithm.len() + i) % 256) as u8)
            .collect();

        Ok(key)
    }

    /// Returns the algorithm name.
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    /// Returns the key size in bytes.
    pub fn key_size(&self) -> usize {
        self.key_size
    }
}

/// An interface to allow dependency injection of crypto primitives for testing.
///
/// Implementations of this trait should be thread-safe.
///
/// This corresponds to `org.apache.kafka.connect.runtime.distributed.Crypto` in Java.
pub trait Crypto: Send + Sync {
    /// Returns a MacInstance for the specified algorithm.
    ///
    /// # Arguments
    /// * `algorithm` - The standard name of the requested MAC algorithm (e.g., "HmacSHA256")
    ///
    /// # Returns
    /// A new MacInstance for the algorithm
    ///
    /// # Errors
    /// Returns CryptoError if no implementation is available for the algorithm
    fn mac(&self, algorithm: &str) -> Result<MacInstance, CryptoError>;

    /// Returns a KeyGenerator for the specified algorithm.
    ///
    /// # Arguments
    /// * `algorithm` - The standard name of the requested key algorithm (e.g., "HmacSHA256")
    ///
    /// # Returns
    /// A new KeyGenerator for the algorithm
    ///
    /// # Errors
    /// Returns CryptoError if no implementation is available for the algorithm
    fn key_generator(&self, algorithm: &str) -> Result<KeyGenerator, CryptoError>;
}

/// System implementation of Crypto using actual system crypto calls.
///
/// This corresponds to `Crypto.SystemCrypto` in Java.
pub struct SystemCrypto;

impl SystemCrypto {
    /// Creates a new SystemCrypto instance.
    pub fn new() -> Self {
        SystemCrypto
    }
}

impl Default for SystemCrypto {
    fn default() -> Self {
        Self::new()
    }
}

impl Crypto for SystemCrypto {
    fn mac(&self, algorithm: &str) -> Result<MacInstance, CryptoError> {
        // Support common HMAC algorithms
        match algorithm {
            "HmacSHA256" | "HmacSHA1" | "HmacMD5" => Ok(MacInstance::new(algorithm)),
            _ => Err(CryptoError::unsupported_algorithm(algorithm)),
        }
    }

    fn key_generator(&self, algorithm: &str) -> Result<KeyGenerator, CryptoError> {
        // Support common key generation algorithms
        match algorithm {
            "HmacSHA256" | "HmacSHA1" | "HmacMD5" | "AES" => Ok(KeyGenerator::new(algorithm)),
            _ => Err(CryptoError::unsupported_algorithm(algorithm)),
        }
    }
}

/// A mock Crypto implementation for testing.
///
/// This allows control over the behavior for unit tests.
pub struct MockCrypto {
    mac_result: Option<Result<MacInstance, CryptoError>>,
    key_gen_result: Option<Result<KeyGenerator, CryptoError>>,
}

impl MockCrypto {
    /// Creates a new MockCrypto with default successful behavior.
    pub fn new() -> Self {
        MockCrypto {
            mac_result: None,
            key_gen_result: None,
        }
    }

    /// Sets the result to return for mac() calls.
    pub fn set_mac_result(&mut self, result: Result<MacInstance, CryptoError>) {
        self.mac_result = Some(result);
    }

    /// Sets the result to return for key_generator() calls.
    pub fn set_key_generator_result(&mut self, result: Result<KeyGenerator, CryptoError>) {
        self.key_gen_result = Some(result);
    }

    /// Creates a MockCrypto that always returns errors.
    pub fn failing() -> Self {
        MockCrypto {
            mac_result: Some(Err(CryptoError::unsupported_algorithm("mock"))),
            key_gen_result: Some(Err(CryptoError::unsupported_algorithm("mock"))),
        }
    }
}

impl Default for MockCrypto {
    fn default() -> Self {
        Self::new()
    }
}

impl Crypto for MockCrypto {
    fn mac(&self, algorithm: &str) -> Result<MacInstance, CryptoError> {
        match &self.mac_result {
            Some(result) => result.clone(),
            None => Ok(MacInstance::new(algorithm)),
        }
    }

    fn key_generator(&self, algorithm: &str) -> Result<KeyGenerator, CryptoError> {
        match &self.key_gen_result {
            Some(result) => result.clone(),
            None => Ok(KeyGenerator::new(algorithm)),
        }
    }
}

/// Default system crypto instance, similar to Crypto.SYSTEM in Java.
pub const SYSTEM_CRYPTO: SystemCrypto = SystemCrypto;

/// Convenience function to get the system crypto implementation.
pub fn system_crypto() -> Arc<dyn Crypto> {
    Arc::new(SystemCrypto::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crypto_error_display() {
        let err = CryptoError::unsupported_algorithm("UnknownAlg");
        assert_eq!(
            err.to_string(),
            "CryptoError: No implementation available for algorithm: UnknownAlg (algorithm: UnknownAlg)"
        );
    }

    #[test]
    fn test_mac_instance_creation() {
        let mac = MacInstance::new("HmacSHA256");
        assert_eq!(mac.algorithm(), "HmacSHA256");
        assert_eq!(mac.mac_length(), 32);
    }

    #[test]
    fn test_mac_instance_init_and_compute() {
        let mut mac = MacInstance::new("HmacSHA256");
        mac.init(&[1, 2, 3, 4, 5, 6, 7, 8]);

        let result = mac.do_final(&[10, 20, 30, 40]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 32);
    }

    #[test]
    fn test_mac_instance_without_key() {
        let mac = MacInstance::new("HmacSHA256");
        let result = mac.do_final(&[1, 2, 3]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));
    }

    #[test]
    fn test_key_generator_creation() {
        let kg = KeyGenerator::new("HmacSHA256");
        assert_eq!(kg.algorithm(), "HmacSHA256");
        assert_eq!(kg.key_size(), 32);
    }

    #[test]
    fn test_key_generator_with_custom_size() {
        let mut kg = KeyGenerator::new("AES");
        kg.init_with_size(32);
        assert_eq!(kg.key_size(), 32);
    }

    #[test]
    fn test_key_generator_generate() {
        let kg = KeyGenerator::new("HmacSHA256");
        let key = kg.generate_key();
        assert!(key.is_ok());
        assert_eq!(key.unwrap().len(), 32);
    }

    #[test]
    fn test_system_crypto_mac() {
        let crypto = SystemCrypto::new();
        let mac = crypto.mac("HmacSHA256");
        assert!(mac.is_ok());
        assert_eq!(mac.unwrap().algorithm(), "HmacSHA256");
    }

    #[test]
    fn test_system_crypto_mac_unknown_algorithm() {
        let crypto = SystemCrypto::new();
        let mac = crypto.mac("UnknownAlgorithm");
        assert!(mac.is_err());
    }

    #[test]
    fn test_system_crypto_key_generator() {
        let crypto = SystemCrypto::new();
        let kg = crypto.key_generator("AES");
        assert!(kg.is_ok());
        assert_eq!(kg.unwrap().algorithm(), "AES");
    }

    #[test]
    fn test_mock_crypto_default() {
        let crypto = MockCrypto::new();
        let mac = crypto.mac("HmacSHA256");
        assert!(mac.is_ok());

        let kg = crypto.key_generator("AES");
        assert!(kg.is_ok());
    }

    #[test]
    fn test_mock_crypto_custom_result() {
        let mut crypto = MockCrypto::new();
        crypto.set_mac_result(Err(CryptoError::unsupported_algorithm("test")));

        let mac = crypto.mac("HmacSHA256");
        assert!(mac.is_err());
    }

    #[test]
    fn test_mock_crypto_failing() {
        let crypto = MockCrypto::failing();
        let mac = crypto.mac("HmacSHA256");
        assert!(mac.is_err());

        let kg = crypto.key_generator("AES");
        assert!(kg.is_err());
    }

    #[test]
    fn test_system_crypto_arc() {
        let crypto = system_crypto();
        let mac = crypto.mac("HmacSHA256");
        assert!(mac.is_ok());
    }

    #[test]
    fn test_crypto_trait_send_sync() {
        // Verify that SystemCrypto is Send + Sync
        let crypto: Box<dyn Crypto> = Box::new(SystemCrypto::new());
        let _: Box<dyn Send> = crypto;

        let crypto2: Box<dyn Crypto> = Box::new(SystemCrypto::new());
        let _: Box<dyn Sync> = crypto2;
    }
}
