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

//! Herder request interface for Kafka Connect.
//!
//! A HerderRequest represents a request to the herder that can be cancelled.
//! This is used for async operations like connector restarts with delays.
//!
//! Corresponds to Java: `org.apache.kafka.connect.runtime.HerderRequest`

/// A request handle that can be cancelled.
///
/// This interface represents an async request to the herder that can be
/// cancelled before it completes. This is used for operations like
/// delayed connector restarts.
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.HerderRequest`
pub trait HerderRequest {
    /// Cancel the request.
    ///
    /// If the request has not yet been processed, this will prevent it
    /// from being executed. If the request is already in progress or
    /// completed, this may have no effect.
    ///
    /// Corresponds to Java: `HerderRequest.cancel()`
    fn cancel(&self);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple mock implementation for testing.
    struct MockHerderRequest {
        cancelled: bool,
    }

    impl HerderRequest for MockHerderRequest {
        fn cancel(&self) {
            // In a real implementation, this would set some flag
        }
    }

    #[test]
    fn test_herder_request_trait() {
        let request = MockHerderRequest { cancelled: false };
        request.cancel();
    }
}
