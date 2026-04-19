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

//! Isolation level for Kafka consumer.
//!
//! This corresponds to `org.apache.kafka.clients.consumer.IsolationLevel` in Java.

/// Isolation level for reading messages from Kafka.
///
/// This controls how consumers read messages in transactional scenarios.
/// This corresponds to `org.apache.kafka.clients.consumer.IsolationLevel` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IsolationLevel {
    /// Read all messages, including those from aborted transactions.
    /// This corresponds to `READ_UNCOMMITTED` in Java.
    ReadUncommitted,
    /// Only read messages from committed transactions.
    /// This corresponds to `READ_COMMITTED` in Java.
    ReadCommitted,
}

impl Default for IsolationLevel {
    /// Default isolation level is ReadUncommitted.
    fn default() -> Self {
        IsolationLevel::ReadUncommitted
    }
}

impl IsolationLevel {
    /// Returns whether this isolation level reads committed transactions only.
    pub fn is_read_committed(&self) -> bool {
        matches!(self, IsolationLevel::ReadCommitted)
    }

    /// Returns whether this isolation level reads all messages (including aborted).
    pub fn is_read_uncommitted(&self) -> bool {
        matches!(self, IsolationLevel::ReadUncommitted)
    }

    /// Returns the string representation of this isolation level.
    pub fn as_str(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "read_uncommitted",
            IsolationLevel::ReadCommitted => "read_committed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_isolation_level() {
        let level = IsolationLevel::default();
        assert_eq!(level, IsolationLevel::ReadUncommitted);
    }

    #[test]
    fn test_is_read_committed() {
        assert!(!IsolationLevel::ReadUncommitted.is_read_committed());
        assert!(IsolationLevel::ReadCommitted.is_read_committed());
    }

    #[test]
    fn test_is_read_uncommitted() {
        assert!(IsolationLevel::ReadUncommitted.is_read_uncommitted());
        assert!(!IsolationLevel::ReadCommitted.is_read_uncommitted());
    }

    #[test]
    fn test_as_str() {
        assert_eq!(IsolationLevel::ReadUncommitted.as_str(), "read_uncommitted");
        assert_eq!(IsolationLevel::ReadCommitted.as_str(), "read_committed");
    }

    #[test]
    fn test_equality() {
        assert_eq!(
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadUncommitted
        );
        assert_eq!(IsolationLevel::ReadCommitted, IsolationLevel::ReadCommitted);
        assert_ne!(
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted
        );
    }

    #[test]
    fn test_clone() {
        let level = IsolationLevel::ReadCommitted;
        let cloned = level.clone();
        assert_eq!(level, cloned);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(IsolationLevel::ReadUncommitted);
        set.insert(IsolationLevel::ReadCommitted);
        assert_eq!(set.len(), 2);
    }
}
