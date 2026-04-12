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

use crate::util::Callback;
use crate::util::ConvertingFutureCallback;

/// An implementation of ConvertingFutureCallback that doesn't do any conversion
pub struct FutureCallback<T> {
    inner: ConvertingFutureCallback<T, T>,
}

impl<T> FutureCallback<T> {
    /// Create a new future callback with underlying callback
    pub fn with_underlying(underlying: Box<dyn Callback<T>>) -> Self {
        Self {
            inner: ConvertingFutureCallback::with_underlying(Some(underlying)),
        }
    }

    /// Create a new future callback
    pub fn new() -> Self {
        Self {
            inner: ConvertingFutureCallback::new(),
        }
    }
}

impl<T> ConvertingFutureCallback<T, T> for FutureCallback<T> {
    pub fn convert(&self, result: T) -> T {
        result
    }
}
