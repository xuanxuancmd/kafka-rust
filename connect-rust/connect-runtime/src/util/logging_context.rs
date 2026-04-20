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

//! Mapped Diagnostic Context (MDC) for SLF4J-like logging.
//!
//! This corresponds to `org.apache.kafka.connect.util.LoggingContext` in Java.

use super::connector_task_id::ConnectorTaskId;
use std::collections::HashMap;

/// The MDC key for connector context.
pub const CONNECTOR_CONTEXT: &str = "connector.context";

/// Scope values for logging context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Worker scope (starting connectors).
    Worker,
    /// Task scope (task implementation).
    Task,
    /// Offsets scope (committing offsets).
    Offsets,
    /// Validate scope (validating configs).
    Validate,
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scope::Worker => write!(f, "worker"),
            Scope::Task => write!(f, "task"),
            Scope::Offsets => write!(f, "offsets"),
            Scope::Validate => write!(f, "validate"),
        }
    }
}

/// Logging context manager for Connect operations.
///
/// This corresponds to `org.apache.kafka.connect.util.LoggingContext` in Java.
/// Uses MDC-style context for adding connector/task information to logs.
pub struct LoggingContext {
    previous: HashMap<String, Option<String>>,
}

impl LoggingContext {
    /// All MDC context keys managed by Connect.
    pub fn all_contexts() -> Vec<String> {
        vec![CONNECTOR_CONTEXT.to_string()]
    }

    /// Clear all MDC parameters.
    pub fn clear() {
        // In Rust, we don't have MDC, but we can use a global context
        // For now, this is a stub implementation
        log::debug!("Clearing logging context");
    }

    /// Create logging context for a connector.
    pub fn for_connector(connector_name: &str) -> Self {
        let previous = Self::save_previous_context();
        let prefix = Self::prefix_for(connector_name, Scope::Worker, None);
        Self::set_context(CONNECTOR_CONTEXT, &prefix);
        LoggingContext { previous }
    }

    /// Create logging context for a task.
    pub fn for_task(id: &ConnectorTaskId) -> Self {
        let previous = Self::save_previous_context();
        let prefix = Self::prefix_for(id.connector(), Scope::Task, Some(id.task()));
        Self::set_context(CONNECTOR_CONTEXT, &prefix);
        LoggingContext { previous }
    }

    /// Create logging context for offsets.
    pub fn for_offsets(id: &ConnectorTaskId) -> Self {
        let previous = Self::save_previous_context();
        let prefix = Self::prefix_for(id.connector(), Scope::Offsets, Some(id.task()));
        Self::set_context(CONNECTOR_CONTEXT, &prefix);
        LoggingContext { previous }
    }

    /// Generate a prefix string for the context.
    ///
    /// Format: `[connectorName|scope] ` or `[connectorName|task-N|scope] `
    fn prefix_for(connector_name: &str, scope: Scope, task_number: Option<i32>) -> String {
        let mut result = String::from("[");
        result.push_str(connector_name);

        if let Some(task) = task_number {
            result.push('|');
            result.push_str(&format!("task-{}", task));
        }

        if scope != Scope::Task {
            result.push('|');
            result.push_str(&scope.to_string());
        }

        result.push_str("] ");
        result
    }

    /// Save the previous context values.
    fn save_previous_context() -> HashMap<String, Option<String>> {
        Self::all_contexts()
            .iter()
            .map(|key| (key.clone(), Self::get_context(key)))
            .collect()
    }

    /// Set a context value (stub implementation).
    fn set_context(_key: &str, _value: &str) {
        // In real implementation, this would set MDC value
        // For now, log the context for visibility
        log::debug!("Setting logging context: {} = {}", _key, _value);
    }

    /// Get a context value (stub implementation).
    fn get_context(_key: &str) -> Option<String> {
        // In real implementation, this would get MDC value
        None
    }

    /// Remove a context value (stub implementation).
    fn remove_context(_key: &str) {
        log::debug!("Removing logging context: {}", _key);
    }
}

impl Drop for LoggingContext {
    fn drop(&mut self) {
        // Restore previous context on close
        for key in Self::all_contexts() {
            match self.previous.get(&key) {
                Some(Some(value)) => Self::set_context(&key, value),
                Some(None) => Self::remove_context(&key),
                None => Self::remove_context(&key),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_for_worker() {
        let prefix = LoggingContext::prefix_for("my-connector", Scope::Worker, None);
        assert_eq!(prefix, "[my-connector|worker] ");
    }

    #[test]
    fn test_prefix_for_task() {
        let prefix = LoggingContext::prefix_for("my-connector", Scope::Task, Some(0));
        assert_eq!(prefix, "[my-connector|task-0] ");
    }

    #[test]
    fn test_prefix_for_offsets() {
        let prefix = LoggingContext::prefix_for("my-connector", Scope::Offsets, Some(5));
        assert_eq!(prefix, "[my-connector|task-5|offsets] ");
    }

    #[test]
    fn test_prefix_for_validate() {
        let prefix = LoggingContext::prefix_for("test-conn", Scope::Validate, None);
        assert_eq!(prefix, "[test-conn|validate] ");
    }

    #[test]
    fn test_scope_display() {
        assert_eq!(Scope::Worker.to_string(), "worker");
        assert_eq!(Scope::Task.to_string(), "task");
        assert_eq!(Scope::Offsets.to_string(), "offsets");
        assert_eq!(Scope::Validate.to_string(), "validate");
    }

    #[test]
    fn test_for_connector() {
        let _ctx = LoggingContext::for_connector("test-connector");
        // Context is set, and will be restored on drop
    }

    #[test]
    fn test_for_task() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let _ctx = LoggingContext::for_task(&id);
        // Context is set, and will be restored on drop
    }

    #[test]
    fn test_for_offsets() {
        let id = ConnectorTaskId::new("test-connector".to_string(), 3);
        let _ctx = LoggingContext::for_offsets(&id);
        // Context is set, and will be restored on drop
    }
}
