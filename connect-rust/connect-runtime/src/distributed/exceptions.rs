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

//! Distributed exceptions for Kafka Connect runtime.
//!
//! This module provides exception types that correspond to
//! `org.apache.kafka.connect.runtime.distributed` exceptions in Java.

use std::error::Error;
use std::fmt;

/// Raised when a request has been received by a worker which cannot handle it,
/// but can forward it to the right target.
///
/// This corresponds to `org.apache.kafka.connect.runtime.distributed.RequestTargetException` in Java.
#[derive(Debug)]
pub struct RequestTargetException {
    message: String,
    forward_url: String,
}

impl RequestTargetException {
    /// Creates a new RequestTargetException with a message and forward URL.
    pub fn new(message: impl Into<String>, forward_url: impl Into<String>) -> Self {
        RequestTargetException {
            message: message.into(),
            forward_url: forward_url.into(),
        }
    }

    /// Returns the forward URL where the request should be sent.
    pub fn forward_url(&self) -> &str {
        &self.forward_url
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for RequestTargetException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RequestTargetException: {} (forward to: {})",
            self.message, self.forward_url
        )
    }
}

impl Error for RequestTargetException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

/// Thrown when a request intended for the owner of a task or connector is received
/// by a worker which doesn't own it (typically the leader).
///
/// This corresponds to `org.apache.kafka.connect.runtime.distributed.NotAssignedException` in Java.
#[derive(Debug)]
pub struct NotAssignedException {
    message: String,
    owner_url: String,
}

impl NotAssignedException {
    /// Creates a new NotAssignedException with a message and owner URL.
    pub fn new(message: impl Into<String>, owner_url: impl Into<String>) -> Self {
        NotAssignedException {
            message: message.into(),
            owner_url: owner_url.into(),
        }
    }

    /// Returns the URL of the worker that owns the task or connector.
    pub fn owner_url(&self) -> &str {
        &self.owner_url
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for NotAssignedException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NotAssignedException: {} (owner: {})",
            self.message, self.owner_url
        )
    }
}

impl Error for NotAssignedException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

/// Indicates an operation was not permitted because it can only be performed
/// on the leader and this worker is not currently the leader.
///
/// This corresponds to `org.apache.kafka.connect.runtime.distributed.NotLeaderException` in Java.
#[derive(Debug)]
pub struct NotLeaderException {
    message: String,
    leader_url: String,
}

impl NotLeaderException {
    /// Creates a new NotLeaderException with a message and leader URL.
    pub fn new(message: impl Into<String>, leader_url: impl Into<String>) -> Self {
        NotLeaderException {
            message: message.into(),
            leader_url: leader_url.into(),
        }
    }

    /// Returns the URL of the leader worker.
    pub fn leader_url(&self) -> &str {
        &self.leader_url
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for NotLeaderException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NotLeaderException: {} (leader: {})",
            self.message, self.leader_url
        )
    }
}

impl Error for NotLeaderException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

/// Indicates that a request cannot be completed because a rebalance is expected to begin.
///
/// This corresponds to `org.apache.kafka.connect.runtime.distributed.RebalanceNeededException` in Java.
#[derive(Debug)]
pub struct RebalanceNeededException {
    message: String,
}

impl RebalanceNeededException {
    /// Creates a new RebalanceNeededException with a message.
    pub fn new(message: impl Into<String>) -> Self {
        RebalanceNeededException {
            message: message.into(),
        }
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for RebalanceNeededException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RebalanceNeededException: {}", self.message)
    }
}

impl Error for RebalanceNeededException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_target_exception() {
        let ex = RequestTargetException::new("Cannot handle request", "http://worker1:8083");
        assert_eq!(ex.message(), "Cannot handle request");
        assert_eq!(ex.forward_url(), "http://worker1:8083");
        assert_eq!(
            ex.to_string(),
            "RequestTargetException: Cannot handle request (forward to: http://worker1:8083)"
        );
    }

    #[test]
    fn test_not_assigned_exception() {
        let ex = NotAssignedException::new("Task not assigned to this worker", "http://owner:8083");
        assert_eq!(ex.message(), "Task not assigned to this worker");
        assert_eq!(ex.owner_url(), "http://owner:8083");
        assert_eq!(
            ex.to_string(),
            "NotAssignedException: Task not assigned to this worker (owner: http://owner:8083)"
        );
    }

    #[test]
    fn test_not_leader_exception() {
        let ex = NotLeaderException::new("Not the leader", "http://leader:8083");
        assert_eq!(ex.message(), "Not the leader");
        assert_eq!(ex.leader_url(), "http://leader:8083");
        assert_eq!(
            ex.to_string(),
            "NotLeaderException: Not the leader (leader: http://leader:8083)"
        );
    }

    #[test]
    fn test_rebalance_needed_exception() {
        let ex = RebalanceNeededException::new("Rebalance expected to begin");
        assert_eq!(ex.message(), "Rebalance expected to begin");
        assert_eq!(
            ex.to_string(),
            "RebalanceNeededException: Rebalance expected to begin"
        );
    }

    #[test]
    fn test_error_trait_implementation() {
        let ex = RequestTargetException::new("test", "url");
        let _: &dyn Error = &ex;

        let ex2 = NotAssignedException::new("test", "url");
        let _: &dyn Error = &ex2;

        let ex3 = NotLeaderException::new("test", "url");
        let _: &dyn Error = &ex3;

        let ex4 = RebalanceNeededException::new("test");
        let _: &dyn Error = &ex4;
    }
}
