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

//! Callback interface for asynchronous operations.
//!
//! Corresponds to `org.apache.kafka.connect.util.Callback` in Java.

use crate::errors::ConnectError;
use std::marker::PhantomData;

/// Callback interface for asynchronous completion.
///
/// This is used for operations that complete asynchronously,
/// such as connector/task lifecycle operations and offset operations.
pub trait Callback<T> {
    /// Called when the operation completes.
    ///
    /// # Arguments
    /// * `error` - An error if the operation failed, or None if successful
    /// * `result` - The result if the operation succeeded, or None if failed
    fn on_completion(&self, error: Option<ConnectError>, result: Option<T>);
}

/// A simple callback that can be created from a closure.
pub struct FnCallback<T, F>
where
    F: Fn(Option<ConnectError>, Option<T>) + Send + Sync,
{
    f: F,
    _marker: PhantomData<T>,
}

impl<T, F> FnCallback<T, F>
where
    F: Fn(Option<ConnectError>, Option<T>) + Send + Sync,
{
    pub fn new(f: F) -> Self {
        FnCallback {
            f,
            _marker: PhantomData,
        }
    }
}

impl<T, F> Callback<T> for FnCallback<T, F>
where
    F: Fn(Option<ConnectError>, Option<T>) + Send + Sync,
{
    fn on_completion(&self, error: Option<ConnectError>, result: Option<T>) {
        (self.f)(error, result)
    }
}
