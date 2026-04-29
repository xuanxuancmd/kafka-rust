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

//! An abstract implementation of Callback that also implements Future.
//!
//! This allows for operations like waiting until the callback is completed
//! via `on_completion`. The result from the callback can be converted by
//! concrete implementations before being retrieved.
//!
//! This corresponds to `org.apache.kafka.connect.util.ConvertingFutureCallback` in Java.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use super::callback::Callback;
use super::stage::Stage;
use common_trait::errors::ConnectError;

/// Error that includes stage information for timeout exceptions.
///
/// This corresponds to `org.apache.kafka.connect.util.ConvertingFutureCallback.StagedTimeoutException` in Java.
#[derive(Debug)]
pub struct StagedTimeoutException {
    stage: Stage,
}

impl StagedTimeoutException {
    /// Creates a new StagedTimeoutException with the given stage.
    pub fn new(stage: Stage) -> Self {
        StagedTimeoutException { stage }
    }

    /// Returns the stage associated with this exception.
    pub fn stage(&self) -> &Stage {
        &self.stage
    }
}

impl std::fmt::Display for StagedTimeoutException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Timed out while waiting for operation to complete. {}",
            self.stage.summarize()
        )
    }
}

impl std::error::Error for StagedTimeoutException {}

/// State for ConvertingFutureCallback.
struct CallbackState<T> {
    result: Option<T>,
    exception: Option<Box<dyn std::error::Error + Send + Sync>>,
    cancelled: bool,
    finished: bool,
    /// Store stage description for timeout errors
    stage_description: Option<String>,
}

/// An abstract implementation of Callback that also implements Future-like behavior.
///
/// This allows for operations like waiting until the callback is completed via
/// `on_completion`. The result from the callback can be converted by concrete
/// implementations of this class before being retrieved.
///
/// This corresponds to `org.apache.kafka.connect.util.ConvertingFutureCallback` in Java.
///
/// Type parameters:
/// - U: the callback result type
/// - T: the future result type obtained after converting the callback result
pub struct ConvertingFutureCallback<U, T>
where
    U: Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
{
    /// The underlying callback to invoke when this callback completes
    underlying: Option<Arc<dyn Callback<T>>>,
    /// Convert function to transform U to T
    convert_fn: Box<dyn Fn(U) -> T + Send + Sync>,
    /// The state protected by mutex and condvar
    state: Mutex<CallbackState<T>>,
    /// Condvar for waiting on completion
    condvar: Condvar,
    /// Phantom data for the callback input type
    _input_type: std::marker::PhantomData<U>,
}

impl<U: Send + Sync + 'static, T: Send + Sync + Clone + 'static> ConvertingFutureCallback<U, T> {
    /// Creates a new ConvertingFutureCallback without an underlying callback.
    ///
    /// The `convert_fn` is used to transform the callback result type U to
    /// the future result type T.
    pub fn new(convert_fn: Box<dyn Fn(U) -> T + Send + Sync>) -> Self {
        ConvertingFutureCallback {
            underlying: None,
            convert_fn,
            state: Mutex::new(CallbackState {
                result: None,
                exception: None,
                cancelled: false,
                finished: false,
                stage_description: None,
            }),
            condvar: Condvar::new(),
            _input_type: std::marker::PhantomData,
        }
    }

    /// Creates a new ConvertingFutureCallback with an underlying callback.
    ///
    /// The underlying callback will be invoked when this callback completes,
    /// with the converted result.
    pub fn with_underlying(
        underlying: Arc<dyn Callback<T>>,
        convert_fn: Box<dyn Fn(U) -> T + Send + Sync>,
    ) -> Self {
        ConvertingFutureCallback {
            underlying: Some(underlying),
            convert_fn,
            state: Mutex::new(CallbackState {
                result: None,
                exception: None,
                cancelled: false,
                finished: false,
                stage_description: None,
            }),
            condvar: Condvar::new(),
            _input_type: std::marker::PhantomData,
        }
    }

    /// Converts the callback result to the future result type.
    pub fn convert(&self, result: U) -> T {
        (self.convert_fn)(result)
    }

    /// Attempts to cancel this callback.
    ///
    /// Returns true if the callback was successfully cancelled, false otherwise.
    /// If `may_interrupt_if_running` is false and the callback is already running,
    /// this method will wait for the callback to complete and return false.
    pub fn cancel(&self, may_interrupt_if_running: bool) -> Result<bool, ConnectError> {
        let mut state = self.state.lock().unwrap();

        if state.finished {
            return Ok(false);
        }

        if may_interrupt_if_running {
            state.cancelled = true;
            state.finished = true;
            self.condvar.notify_all();
            return Ok(true);
        }

        // Wait for completion
        loop {
            if state.finished {
                return Ok(false);
            }
            state = self.condvar.wait(state).unwrap();
        }
    }

    /// Returns whether this callback was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.state.lock().unwrap().cancelled
    }

    /// Returns whether this callback has completed.
    pub fn is_done(&self) -> bool {
        self.state.lock().unwrap().finished
    }

    /// Waits for the callback to complete and returns the result.
    ///
    /// Blocks until the callback completes or is cancelled.
    pub fn get(&self) -> Result<T, ConnectError> {
        let mut state = self.state.lock().unwrap();

        loop {
            if state.finished {
                break;
            }
            state = self.condvar.wait(state).unwrap();
        }

        self.get_result_internal(&state)
    }

    /// Waits for the callback to complete with a timeout and returns the result.
    ///
    /// Blocks until the callback completes, is cancelled, or the timeout expires.
    pub fn get_with_timeout(&self, timeout: Duration) -> Result<T, ConnectError> {
        let mut state = self.state.lock().unwrap();

        let result = self.condvar.wait_timeout(state, timeout).unwrap();
        state = result.0;

        if !state.finished {
            if let Some(ref stage_desc) = state.stage_description {
                return Err(ConnectError::general(format!(
                    "Timed out while waiting for operation to complete. Stage: {}",
                    stage_desc
                )));
            } else {
                return Err(ConnectError::general(
                    "Timeout waiting for callback to complete",
                ));
            }
        }

        self.get_result_internal(&state)
    }

    /// Records a stage for tracking progress.
    ///
    /// This stage will be included in timeout exceptions to provide context.
    pub fn record_stage(&self, stage: &Stage) {
        let mut state = self.state.lock().unwrap();
        state.stage_description = Some(stage.description().to_string());
    }

    /// Gets the result from the state.
    fn get_result_internal(&self, state: &CallbackState<T>) -> Result<T, ConnectError> {
        if state.cancelled {
            return Err(ConnectError::general("Callback was cancelled"));
        }

        if let Some(ref err) = state.exception {
            return Err(ConnectError::general(err.to_string()));
        }

        match state.result.clone() {
            Some(r) => Ok(r),
            None => Err(ConnectError::general("No result available")),
        }
    }
}

impl<U: Send + Sync + 'static, T: Send + Sync + Clone + 'static> Callback<U>
    for ConvertingFutureCallback<U, T>
{
    fn on_completion(
        &self,
        error: Option<Box<dyn std::error::Error + Send + Sync>>,
        result: Option<U>,
    ) {
        let mut state = self.state.lock().unwrap();

        if state.finished {
            return;
        }

        // Store exception for later retrieval
        if let Some(ref err) = error {
            state.exception = Some(Box::new(ConnectError::general(err.to_string())));
        }
        if let Some(res) = result {
            let converted = self.convert(res);
            state.result = Some(converted);
        }

        let converted_result = state.result.clone();

        if let Some(ref underlying) = self.underlying {
            underlying.on_completion(error, converted_result);
        }

        state.finished = true;
        self.condvar.notify_all();
    }

    fn record_stage(&self, stage: &Stage) {
        self.record_stage(stage);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_converting_future_callback_creation() {
        let callback = ConvertingFutureCallback::<String, String>::new(Box::new(|s| s));
        assert!(!callback.is_done());
        assert!(!callback.is_cancelled());
    }

    #[test]
    fn test_converting_future_callback_completion() {
        let callback = ConvertingFutureCallback::<String, String>::new(Box::new(|s| s));

        callback.on_completion(None, Some("test_result".to_string()));

        assert!(callback.is_done());
        assert!(!callback.is_cancelled());

        let result = callback.get().unwrap();
        assert_eq!(result, "test_result");
    }

    #[test]
    fn test_converting_future_callback_error() {
        let callback = ConvertingFutureCallback::<String, String>::new(Box::new(|s| s));

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
    fn test_converting_future_callback_cancel() {
        let callback = ConvertingFutureCallback::<String, String>::new(Box::new(|s| s));

        let cancelled = callback.cancel(true).unwrap();
        assert!(cancelled);
        assert!(callback.is_cancelled());
        assert!(callback.is_done());
    }

    #[test]
    fn test_converting_with_conversion() {
        let callback = ConvertingFutureCallback::<i32, String>::new(Box::new(|n| n.to_string()));

        callback.on_completion(None, Some(42));

        let result = callback.get().unwrap();
        assert_eq!(result, "42");
    }
}
