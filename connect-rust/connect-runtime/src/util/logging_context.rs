/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::util::ConnectorTaskId;
use std::collections::HashMap;

/// A utility for defining Mapped Diagnostic Context (MDC) for SLF4J logs
pub struct LoggingContext {
    previous: Option<HashMap<String, String>>,
}

impl LoggingContext {
    /// The name of the Mapped Diagnostic Context (MDC) key that defines context for a connector
    pub const CONNECTOR_CONTEXT: &str = "connector.context";

    /// All context keys
    pub const ALL_CONTEXTS: Vec<&str> = vec![Self::CONNECTOR_CONTEXT];

    /// Clear all MDC parameters
    pub fn clear() {
        // TODO: Implement MDC clear
    }

    /// Modify the current MDC logging context for a connector
    pub fn for_connector(connector_name: &str) -> Self {
        let context = Self {
            previous: None, // TODO: Get copy of current MDC context
        };
        let prefix = Self::prefix_for(connector_name, Scope::Worker, None);
        // TODO: MDC.put(CONNECTOR_CONTEXT, prefix);
        context
    }

    /// Modify the current MDC logging context for a task
    pub fn for_task(id: &ConnectorTaskId) -> Self {
        let context = Self {
            previous: None, // TODO: Get copy of current MDC context
        };
        let prefix = Self::prefix_for(id.connector(), Scope::Task, Some(id.task()));
        // TODO: MDC.put(CONNECTOR_CONTEXT, prefix);
        context
    }

    /// Modify the current MDC logging context for offsets
    pub fn for_offsets(id: &ConnectorTaskId) -> Self {
        let context = Self {
            previous: None, // TODO: Get copy of current MDC context
        };
        let prefix = Self::prefix_for(id.connector(), Scope::Offsets, Some(id.task()));
        // TODO: MDC.put(CONNECTOR_CONTEXT, prefix);
        context
    }

    /// Return the prefix for a connector, task number, and scope
    pub fn prefix_for(connector_name: &str, scope: Scope, task_number: Option<i32>) -> String {
        let mut result = format!("[{}", connector_name);
        if let Some(task_num) = task_number {
            result = format!("{}|task-{}", result, task_num);
        }
        if scope != Scope::Task {
            result = format!("{}|{}", result, scope);
        }
        format!("{} ", result)
    }
}

impl Drop for LoggingContext {
    fn drop(&mut self) {
        // Restore previous MDC context
        // TODO: Implement MDC restore
    }
}

/// The Scope values used by Connect when specifying the context
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// The scope value for the worker as it starts a connector
    Worker,
    /// The scope value for Task implementations
    Task,
    /// The scope value for committing offsets
    Offsets,
    /// The scope value for validating connector configurations
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
