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

//! Exception thrown when a connector has generated too many task configs.
//!
//! This exception is thrown when a connector has generated more tasks than
//! the value for `tasks.max` that it has been configured with.
//!
//! Corresponds to Java: `org.apache.kafka.connect.runtime.TooManyTasksException`

use crate::config::connector_config::TASKS_MAX_ENFORCE_CONFIG;
use connect_api::errors::ConnectException;

/// Exception thrown when a connector has generated too many task configs.
///
/// This exception is thrown when a connector has generated more tasks than
/// the value for `tasks.max` that it has been configured with.
///
/// This behavior should be considered a bug in the connector implementation
/// and is disallowed by default. If necessary, it can be permitted by
/// reconfiguring the connector with `tasks.max.enforce` set to false;
/// however, this option will be removed in a future release of Kafka Connect.
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.TooManyTasksException`
/// (Java: extends `ConnectException`)
#[derive(Debug)]
pub struct TooManyTasksException {
    /// The underlying ConnectException
    inner: ConnectException,
}

impl TooManyTasksException {
    /// Creates a new TooManyTasksException.
    ///
    /// # Arguments
    /// * `conn_name` - The name of the connector
    /// * `num_tasks` - The number of tasks generated
    /// * `max_tasks` - The maximum number of tasks configured
    ///
    /// Corresponds to Java: `TooManyTasksException(String connName, int numTasks, int maxTasks)`
    pub fn new(conn_name: &str, num_tasks: i32, max_tasks: i32) -> Self {
        let message = format!(
            "The connector {} has generated {} tasks, which is greater than {}, \
             the maximum number of tasks it is configured to create. \
             This behaviour should be considered a bug and is disallowed. \
             If necessary, it can be permitted by reconfiguring the connector \
             with '{}' set to false; however, this option will be removed in a \
             future release of Kafka Connect.",
            conn_name, num_tasks, max_tasks, TASKS_MAX_ENFORCE_CONFIG
        );

        TooManyTasksException {
            inner: ConnectException::new(message),
        }
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        self.inner.message()
    }
}

impl std::fmt::Display for TooManyTasksException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl std::error::Error for TooManyTasksException {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_too_many_tasks_exception() {
        let ex = TooManyTasksException::new("test-connector", 10, 5);

        let message = ex.message();
        assert!(message.contains("test-connector"));
        assert!(message.contains("10"));
        assert!(message.contains("5"));
        assert!(message.contains(TASKS_MAX_ENFORCE_CONFIG));
    }

    #[test]
    fn test_too_many_tasks_exception_display() {
        let ex = TooManyTasksException::new("my-connector", 3, 1);
        let display = format!("{}", ex);

        assert!(display.contains("my-connector"));
        assert!(display.contains("ConnectException"));
    }

    #[test]
    fn test_too_many_tasks_exception_error() {
        let ex = TooManyTasksException::new("conn", 5, 2);

        // Should implement Error trait
        let _: &dyn std::error::Error = &ex;
    }
}
