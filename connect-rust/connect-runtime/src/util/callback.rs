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

//! Generic callback interface for asynchronous operations.
//!
//! This corresponds to `org.apache.kafka.connect.util.Callback` in Java.

use std::sync::Arc;

use super::stage::Stage;

/// Generic interface for callbacks.
///
/// This corresponds to `org.apache.kafka.connect.util.Callback` in Java.
/// The type parameter V must be Send + Sync to allow the callback to be used
/// across threads.
pub trait Callback<V: Send + Sync + 'static>: Send + Sync {
    /// Invoked upon completion of the operation.
    ///
    /// # Arguments
    /// * `error` - The error that caused the operation to fail, or None if no error occurred
    /// * `result` - The return value, or None if the operation failed
    fn on_completion(
        &self,
        error: Option<Box<dyn std::error::Error + Send + Sync>>,
        result: Option<V>,
    );

    /// Record a stage for tracking progress.
    fn record_stage(&self, _stage: &Stage) {
        // Default implementation does nothing
    }

    /// Chain staging to another callback.
    /// This method requires Self: Sized because it returns a concrete type.
    fn chain_staging<V2: Send + Sync + 'static>(
        self: Arc<Self>,
        chained: Arc<dyn Callback<V2>>,
    ) -> Arc<dyn Callback<V2>>
    where
        Self: Sized + 'static,
    {
        Arc::new(ChainedCallback::new(self, chained))
    }
}

/// A simple callback implementation that can be used with closures.
pub struct SimpleCallback<V> {
    func: Box<dyn Fn(Option<Box<dyn std::error::Error + Send + Sync>>, Option<V>) + Send + Sync>,
}

impl<V> SimpleCallback<V> {
    /// Creates a new simple callback with the given function.
    pub fn new<F>(func: F) -> Self
    where
        F: Fn(Option<Box<dyn std::error::Error + Send + Sync>>, Option<V>) + Send + Sync + 'static,
    {
        SimpleCallback {
            func: Box::new(func),
        }
    }
}

impl<V: Send + Sync + 'static> Callback<V> for SimpleCallback<V> {
    fn on_completion(
        &self,
        error: Option<Box<dyn std::error::Error + Send + Sync>>,
        result: Option<V>,
    ) {
        (self.func)(error, result);
    }
}

/// A callback that does nothing.
pub struct NullCallback<V: Send + Sync + 'static> {
    _marker: std::marker::PhantomData<V>,
}

impl<V: Send + Sync + 'static> NullCallback<V> {
    /// Creates a new null callback.
    pub fn new() -> Self {
        NullCallback {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<V: Send + Sync + 'static> Default for NullCallback<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Send + Sync + 'static> Callback<V> for NullCallback<V> {
    fn on_completion(
        &self,
        _error: Option<Box<dyn std::error::Error + Send + Sync>>,
        _result: Option<V>,
    ) {
        // Do nothing
    }
}

/// A chained callback that forwards staging to the original callback.
struct ChainedCallback<V, V2> {
    original: Arc<dyn Callback<V>>,
    chained: Arc<dyn Callback<V2>>,
}

impl<V: Send + Sync + 'static, V2: Send + Sync + 'static> ChainedCallback<V, V2> {
    fn new(original: Arc<dyn Callback<V>>, chained: Arc<dyn Callback<V2>>) -> Self {
        ChainedCallback { original, chained }
    }
}

impl<V: Send + Sync + 'static, V2: Send + Sync + 'static> Callback<V2> for ChainedCallback<V, V2> {
    fn on_completion(
        &self,
        error: Option<Box<dyn std::error::Error + Send + Sync>>,
        result: Option<V2>,
    ) {
        self.chained.on_completion(error, result);
    }

    fn record_stage(&self, stage: &Stage) {
        self.original.record_stage(stage);
    }
}
