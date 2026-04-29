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

//! InternalRequestSignature - Signature verification for internal requests.
//!
//! Provides HMAC-based signature verification for intra-cluster communication
//! between Kafka Connect workers.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.InternalRequestSignature` in Java.

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;

use super::errors::BadRequestException;

/// Header name for the signature.
pub const SIGNATURE_HEADER: &str = "X-Connect-Authorization";

/// Header name for the signature algorithm.
pub const SIGNATURE_ALGORITHM_HEADER: &str = "X-Connect-Request-Signature-Algorithm";

/// Supported signature algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureAlgorithm {
    /// HMAC-SHA256
    HmacSha256,
}

impl SignatureAlgorithm {
    /// Returns the algorithm name as used in headers.
    pub fn name(&self) -> &'static str {
        match self {
            SignatureAlgorithm::HmacSha256 => "HmacSHA256",
        }
    }

    /// Parses an algorithm name from a string.
    pub fn from_name(name: &str) -> Result<Self, String> {
        match name {
            "HmacSHA256" | "HmacSHA256" => Ok(SignatureAlgorithm::HmacSha256),
            _ => Err(format!("Unsupported signature algorithm: {}", name)),
        }
    }
}

/// Crypto provider for generating MAC instances.
///
/// This is a simplified version - in production, this would use proper crypto.
pub struct Crypto;

impl Crypto {
    /// Creates a new Crypto instance.
    pub fn new() -> Self {
        Crypto
    }

    /// Creates a MAC instance for the given algorithm.
    pub fn mac(&self, algorithm: &str) -> Result<Hmac<Sha256>, String> {
        match algorithm {
            "HmacSHA256" => {
                // In real implementation, we'd return an uninitialized MAC
                // that can be initialized with a key later
                // For now, we return a dummy MAC that will be replaced
                Hmac::<Sha256>::new_from_slice(&[])
                    .map_err(|e| format!("Failed to create MAC: {}", e))
            }
            _ => Err(format!("Unsupported algorithm: {}", algorithm)),
        }
    }

    /// System-wide crypto instance.
    pub const SYSTEM: Crypto = Crypto;
}

impl Default for Crypto {
    fn default() -> Self {
        Self::new()
    }
}

/// InternalRequestSignature - Signature for intra-cluster requests.
///
/// This struct contains the signature data extracted from request headers
/// and provides methods to verify the signature against a secret key.
///
/// Corresponds to `InternalRequestSignature` in Java.
#[derive(Debug)]
pub struct InternalRequestSignature {
    /// The request body bytes.
    request_body: Vec<u8>,
    /// The algorithm name.
    algorithm: String,
    /// The signature bytes (Base64 decoded).
    request_signature: Vec<u8>,
}

impl InternalRequestSignature {
    /// Creates a new InternalRequestSignature.
    ///
    /// # Arguments
    ///
    /// * `request_body` - The raw request body bytes
    /// * `algorithm` - The signature algorithm name
    /// * `request_signature` - The signature bytes (already decoded from Base64)
    pub fn new(request_body: Vec<u8>, algorithm: String, request_signature: Vec<u8>) -> Self {
        InternalRequestSignature {
            request_body,
            algorithm,
            request_signature,
        }
    }

    /// Adds a signature to an outgoing request.
    ///
    /// This method signs the request body and adds the signature headers.
    ///
    /// # Arguments
    ///
    /// * `crypto` - Crypto provider for MAC generation
    /// * `key` - Secret key bytes for signing
    /// * `request_body` - The request body to sign
    /// * `algorithm` - Signature algorithm to use
    /// * `headers` - Headers map to add signature to
    pub fn add_to_request(
        crypto: &Crypto,
        key: &[u8],
        request_body: &[u8],
        algorithm: &str,
        headers: &mut HashMap<String, String>,
    ) -> Result<(), String> {
        let signature = Self::sign(crypto, key, request_body, algorithm)?;

        headers.insert(
            SIGNATURE_HEADER.to_string(),
            BASE64_STANDARD.encode(&signature),
        );
        headers.insert(
            SIGNATURE_ALGORITHM_HEADER.to_string(),
            algorithm.to_string(),
        );

        Ok(())
    }

    /// Signs the request body with the given key.
    fn sign(
        _crypto: &Crypto,
        key: &[u8],
        request_body: &[u8],
        algorithm: &str,
    ) -> Result<Vec<u8>, String> {
        match algorithm {
            "HmacSHA256" => {
                let mut mac = Hmac::<Sha256>::new_from_slice(key)
                    .map_err(|e| format!("Failed to initialize MAC: {}", e))?;
                mac.update(request_body);
                Ok(mac.finalize().into_bytes().to_vec())
            }
            _ => Err(format!("Unsupported algorithm: {}", algorithm)),
        }
    }

    /// Extracts a signature from request headers.
    ///
    /// # Arguments
    ///
    /// * `crypto` - Crypto provider for MAC generation
    /// * `request_body` - The raw request body bytes
    /// * `headers` - HTTP headers map
    ///
    /// # Returns
    ///
    /// Returns None if signature headers are not present.
    /// Returns BadRequestException if signature format is invalid.
    pub fn from_headers(
        crypto: &Crypto,
        request_body: Vec<u8>,
        headers: &HashMap<String, String>,
    ) -> Result<Option<Self>, BadRequestException> {
        let algorithm = headers.get(SIGNATURE_ALGORITHM_HEADER);
        let encoded_signature = headers.get(SIGNATURE_HEADER);

        if algorithm.is_none() || encoded_signature.is_none() {
            return Ok(None);
        }

        let algorithm = algorithm.unwrap();
        let encoded_signature = encoded_signature.unwrap();

        // Validate algorithm
        crypto
            .mac(algorithm)
            .map_err(|e| BadRequestException::new(e))?;

        // Decode signature
        let decoded_signature = BASE64_STANDARD
            .decode(encoded_signature)
            .map_err(|e| BadRequestException::new(format!("Invalid Base64 signature: {}", e)))?;

        Ok(Some(InternalRequestSignature::new(
            request_body,
            algorithm.to_string(),
            decoded_signature,
        )))
    }

    /// Returns the signature algorithm name.
    pub fn key_algorithm(&self) -> &str {
        &self.algorithm
    }

    /// Validates the signature against a secret key.
    ///
    /// # Arguments
    ///
    /// * `key` - Secret key bytes to validate against
    ///
    /// # Returns
    ///
    /// Returns true if the signature is valid.
    pub fn is_valid(&self, key: &[u8]) -> bool {
        // Compute expected signature
        let expected = Self::sign(&Crypto::SYSTEM, key, &self.request_body, &self.algorithm);

        match expected {
            Ok(expected_sig) => {
                // Use constant-time comparison for security
                if expected_sig.len() != self.request_signature.len() {
                    return false;
                }

                // Simple constant-time comparison
                let mut result = 0u8;
                for (a, b) in expected_sig.iter().zip(self.request_signature.iter()) {
                    result |= a ^ b;
                }
                result == 0
            }
            Err(_) => false,
        }
    }

    /// Returns the request body bytes.
    pub fn request_body(&self) -> &[u8] {
        &self.request_body
    }

    /// Returns the signature bytes.
    pub fn request_signature(&self) -> &[u8] {
        &self.request_signature
    }

    /// Returns the algorithm name.
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }
}

impl PartialEq for InternalRequestSignature {
    fn eq(&self, other: &Self) -> bool {
        // Use constant-time comparison for signature bytes
        if self.request_signature.len() != other.request_signature.len() {
            return false;
        }

        let mut sig_result = 0u8;
        for (a, b) in self
            .request_signature
            .iter()
            .zip(other.request_signature.iter())
        {
            sig_result |= a ^ b;
        }

        if self.request_body.len() != other.request_body.len() {
            return false;
        }

        let mut body_result = 0u8;
        for (a, b) in self.request_body.iter().zip(other.request_body.iter()) {
            body_result |= a ^ b;
        }

        sig_result == 0 && body_result == 0 && self.algorithm == other.algorithm
    }
}

impl std::hash::Hash for InternalRequestSignature {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.algorithm.hash(state);
        // Note: We don't hash the signature/body bytes to avoid timing attacks
        // In production, this would be handled differently
        self.request_body.len().hash(state);
        self.request_signature.len().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature_algorithm() {
        let algo = SignatureAlgorithm::HmacSha256;
        assert_eq!(algo.name(), "HmacSHA256");

        let parsed = SignatureAlgorithm::from_name("HmacSHA256");
        assert!(parsed.is_ok());
        assert_eq!(parsed.unwrap(), SignatureAlgorithm::HmacSha256);

        let invalid = SignatureAlgorithm::from_name("invalid");
        assert!(invalid.is_err());
    }

    #[test]
    fn test_add_to_request() {
        let crypto = Crypto::new();
        let key = b"test-secret-key";
        let body = b"request body content";
        let mut headers = HashMap::new();

        let result = InternalRequestSignature::add_to_request(
            &crypto,
            key,
            body,
            "HmacSHA256",
            &mut headers,
        );
        assert!(result.is_ok());

        assert!(headers.contains_key(SIGNATURE_HEADER));
        assert!(headers.contains_key(SIGNATURE_ALGORITHM_HEADER));
        assert_eq!(
            headers.get(SIGNATURE_ALGORITHM_HEADER).unwrap(),
            "HmacSHA256"
        );
    }

    #[test]
    fn test_from_headers_missing() {
        let crypto = Crypto::new();
        let headers = HashMap::new();

        let result = InternalRequestSignature::from_headers(&crypto, vec![1, 2, 3], &headers);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_from_headers_present() {
        let crypto = Crypto::new();
        let key = b"test-secret-key";
        let body = b"test body";
        let mut headers = HashMap::new();

        // Add signature
        InternalRequestSignature::add_to_request(&crypto, key, body, "HmacSHA256", &mut headers)
            .unwrap();

        // Extract signature
        let result = InternalRequestSignature::from_headers(&crypto, body.to_vec(), &headers);
        assert!(result.is_ok());
        let sig = result.unwrap().unwrap();

        assert_eq!(sig.key_algorithm(), "HmacSHA256");
        assert!(sig.is_valid(key));
        assert!(!sig.is_valid(b"wrong-key"));
    }

    #[test]
    fn test_is_valid() {
        let crypto = Crypto::new();
        let key = b"test-secret-key";
        let body = b"request body";
        let mut headers = HashMap::new();

        InternalRequestSignature::add_to_request(&crypto, key, body, "HmacSHA256", &mut headers)
            .unwrap();

        let sig = InternalRequestSignature::from_headers(&crypto, body.to_vec(), &headers)
            .unwrap()
            .unwrap();

        assert!(sig.is_valid(key));
        assert!(!sig.is_valid(b"different-key"));
    }

    #[test]
    fn test_signature_roundtrip() {
        let crypto = Crypto::new();
        let key = b"secret-key-123";
        let original_body = vec![0u8, 1, 2, 3, 4, 5];

        let mut headers = HashMap::new();
        InternalRequestSignature::add_to_request(
            &crypto,
            key,
            &original_body,
            "HmacSHA256",
            &mut headers,
        )
        .unwrap();

        let extracted =
            InternalRequestSignature::from_headers(&crypto, original_body.clone(), &headers)
                .unwrap()
                .unwrap();

        assert_eq!(extracted.request_body(), original_body);
        assert_eq!(extracted.key_algorithm(), "HmacSHA256");
        assert!(extracted.is_valid(key));
    }

    #[test]
    fn test_partial_eq() {
        let crypto = Crypto::new();
        let key = b"secret-key";
        let body1 = b"body content";
        let body2 = b"different body";

        let mut headers1 = HashMap::new();
        let mut headers2 = HashMap::new();

        InternalRequestSignature::add_to_request(&crypto, key, body1, "HmacSHA256", &mut headers1)
            .unwrap();

        InternalRequestSignature::add_to_request(&crypto, key, body2, "HmacSHA256", &mut headers2)
            .unwrap();

        let sig1 = InternalRequestSignature::from_headers(&crypto, body1.to_vec(), &headers1)
            .unwrap()
            .unwrap();

        let sig2 = InternalRequestSignature::from_headers(&crypto, body2.to_vec(), &headers2)
            .unwrap()
            .unwrap();

        // Different bodies should produce different signatures
        assert_ne!(sig1, sig2);

        // Same body with same key should produce same signature
        let sig1_dup = InternalRequestSignature::from_headers(&crypto, body1.to_vec(), &headers1)
            .unwrap()
            .unwrap();
        assert_eq!(sig1, sig1_dup);
    }
}
