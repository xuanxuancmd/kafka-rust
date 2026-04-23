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

//! RestRequestTimeout - Interface for REST request timeout configuration.
//!
//! This trait provides timeout values for REST API requests.
//! Corresponds to `org.apache.kafka.connect.runtime.rest.RestRequestTimeout` in Java.

/// Trait for REST request timeout configuration.
///
/// Provides timeout values used for REST API operations.
///
/// Corresponds to `RestRequestTimeout` interface in Java.
pub trait RestRequestTimeout {
    /// Returns the current timeout that should be used for REST requests, in milliseconds.
    fn timeout_ms(&self) -> u64;

    /// Returns the current timeout that should be used for health check REST requests, in milliseconds.
    fn health_check_timeout_ms(&self) -> u64;
}

/// Constant timeout implementation.
///
/// A simple implementation with fixed timeout values.
/// Corresponds to `RestRequestTimeout.constant()` in Java.
#[derive(Debug, Clone, Copy)]
pub struct ConstantRequestTimeout {
    timeout_ms: u64,
    health_check_timeout_ms: u64,
}

impl ConstantRequestTimeout {
    /// Creates a new ConstantRequestTimeout with the given values.
    ///
    /// # Arguments
    ///
    /// * `timeout_ms` - Timeout for regular REST requests in milliseconds
    /// * `health_check_timeout_ms` - Timeout for health check requests in milliseconds
    pub fn new(timeout_ms: u64, health_check_timeout_ms: u64) -> Self {
        ConstantRequestTimeout {
            timeout_ms,
            health_check_timeout_ms,
        }
    }
}

impl RestRequestTimeout for ConstantRequestTimeout {
    fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    fn health_check_timeout_ms(&self) -> u64 {
        self.health_check_timeout_ms
    }
}

impl Default for ConstantRequestTimeout {
    fn default() -> Self {
        // Default values matching Java defaults
        ConstantRequestTimeout::new(90_000, 30_000) // 90s for requests, 30s for health checks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_request_timeout() {
        let timeout = ConstantRequestTimeout::new(100_000, 50_000);
        assert_eq!(timeout.timeout_ms(), 100_000);
        assert_eq!(timeout.health_check_timeout_ms(), 50_000);
    }

    #[test]
    fn test_default_timeout() {
        let timeout = ConstantRequestTimeout::default();
        assert_eq!(timeout.timeout_ms(), 90_000);
        assert_eq!(timeout.health_check_timeout_ms(), 30_000);
    }

    #[test]
    fn test_trait_impl() {
        let timeout: &dyn RestRequestTimeout = &ConstantRequestTimeout::new(60_000, 20_000);
        assert_eq!(timeout.timeout_ms(), 60_000);
        assert_eq!(timeout.health_check_timeout_ms(), 20_000);
    }
}
