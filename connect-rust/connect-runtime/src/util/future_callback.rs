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

//! An implementation of ConvertingFutureCallback that doesn't do any conversion.
//!
//! The callback result is returned transparently via `get`.
//!
//! This corresponds to `org.apache.kafka.connect.util.FutureCallback` in Java.

use std::sync::Arc;

use super::callback::Callback;
use super::converting_future_callback::ConvertingFutureCallback;
use common_trait::errors::ConnectError;

/// An implementation of ConvertingFutureCallback that doesn't do any conversion.
///
/// The callback result type is the same as the future result type (T, T),
/// and the `convert` method simply returns the input unchanged.
///
/// This corresponds to `org.apache.kafka.connect.util.FutureCallback` in Java.
pub struct FutureCallback<T>
where
    T: Send + Sync + Clone + 'static,
{
    inner: ConvertingFutureCallback<T, T>,
}

impl<T: Send + Sync + 'static + Clone> FutureCallback<T> {
    /// Creates a new FutureCallback without an underlying callback.
    pub fn new() -> Self {
        FutureCallback {
            inner: ConvertingFutureCallback::new(Box::new(|result: T| result)),
        }
    }

    /// Creates a new FutureCallback with an underlying callback.
    ///
    /// The underlying callback will be invoked when this callback completes.
    pub fn with_underlying(underlying: Arc<dyn Callback<T>>) -> Self {
        FutureCallback {
            inner: ConvertingFutureCallback::with_underlying(
                underlying,
                Box::new(|result: T| result),
            ),
        }
    }

    /// Attempts to cancel this callback.
    pub fn cancel(&self, may_interrupt_if_running: bool) -> Result<bool, ConnectError> {
        self.inner.cancel(may_interrupt_if_running)
    }

    /// Returns whether this callback was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// Returns whether this callback has completed.
    pub fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Waits for the callback to complete and returns the result.
    pub fn get(&self) -> Result<T, ConnectError> {
        self.inner.get()
    }

    /// Waits for the callback to complete with a timeout and returns the result.
    pub fn get_with_timeout(&self, timeout: std::time::Duration) -> Result<T, ConnectError> {
        self.inner.get_with_timeout(timeout)
    }

    /// Records a stage for tracking progress.
    pub fn record_stage(&self, stage: &super::stage::Stage) {
        self.inner.record_stage(stage);
    }
}

impl<T: Send + Sync + 'static + Clone> Default for FutureCallback<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Sync + 'static + Clone> Callback<T> for FutureCallback<T> {
    fn on_completion(
        &self,
        error: Option<Box<dyn std::error::Error + Send + Sync>>,
        result: Option<T>,
    ) {
        self.inner.on_completion(error, result);
    }

    fn record_stage(&self, stage: &super::stage::Stage) {
        self.inner.record_stage(stage);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_future_callback_creation() {
        let callback = FutureCallback::<String>::new();
        assert!(!callback.is_done());
        assert!(!callback.is_cancelled());
    }

    #[test]
    fn test_future_callback_completion() {
        let callback = FutureCallback::<String>::new();

        callback.on_completion(None, Some("test_result".to_string()));

        assert!(callback.is_done());
        assert!(!callback.is_cancelled());

        let result = callback.get().unwrap();
        assert_eq!(result, "test_result");
    }

    #[test]
    fn test_future_callback_error() {
        let callback = FutureCallback::<String>::new();

        callback.on_completion(
            Some(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "test error",
            ))),
            None,
        );

        assert!(callback.is_done());
        assert!(callback.get().is_err());
    }

    #[test]
    fn test_future_callback_cancel() {
        let callback = FutureCallback::<String>::new();

        let cancelled = callback.cancel(true).unwrap();
        assert!(cancelled);
        assert!(callback.is_cancelled());
        assert!(callback.is_done());
    }
}
