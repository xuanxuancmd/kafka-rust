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

//! Closeable connector context for Kafka Connect.
//!
//! This interface extends ConnectorContext with a close method, allowing
//! connector contexts to be closed to prevent zombie connector threads
//! from making calls after their connector instance should be shut down.
//!
//! Corresponds to Java: `org.apache.kafka.connect.runtime.CloseableConnectorContext`

use connect_api::connector::ConnectorContext;

/// A connector context that can be closed.
///
/// This interface extends `ConnectorContext` with a `close` method, which
/// causes all future calls to it to throw a `ConnectException`. This is useful
/// to prevent zombie connector threads from making such calls after their
/// connector instance should be shut down.
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.CloseableConnectorContext`
/// (Java: `public interface CloseableConnectorContext extends ConnectorContext, Closeable`)
pub trait CloseableConnectorContext: ConnectorContext {
    /// Close this connector context.
    ///
    /// After calling this method, all future calls to the context will
    /// throw a `ConnectException`. This prevents zombie connector threads
    /// from making calls after their connector instance should be shut down.
    ///
    /// Corresponds to Java: `CloseableConnectorContext.close()`
    fn close(&mut self);
}

#[cfg(test)]
mod tests {
    use super::*;
    use connect_api::errors::ConnectError;

    /// A mock implementation for testing.
    struct MockCloseableConnectorContext {
        closed: bool,
    }

    impl ConnectorContext for MockCloseableConnectorContext {
        fn request_task_reconfiguration(&mut self) {
            if self.closed {
                panic!("Context is closed");
            }
        }

        fn raise_error(&mut self, _error: ConnectError) {
            if self.closed {
                panic!("Context is closed");
            }
        }
    }

    impl CloseableConnectorContext for MockCloseableConnectorContext {
        fn close(&mut self) {
            self.closed = true;
        }
    }

    #[test]
    fn test_closeable_connector_context() {
        let mut context = MockCloseableConnectorContext { closed: false };

        // Should work before close
        context.request_task_reconfiguration();

        // After close
        context.close();
    }

    #[test]
    #[should_panic(expected = "Context is closed")]
    fn test_context_closed_raises_error() {
        let mut context = MockCloseableConnectorContext { closed: false };
        context.close();
        context.request_task_reconfiguration();
    }
}
