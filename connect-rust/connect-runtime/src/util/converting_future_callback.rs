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

use crate::errors::ConnectException;
use crate::util::Callback;
use crate::util::Stage;
use crate::util::StagedTimeoutException;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

/// An abstract implementation of Callback that also implements the Future interface.
pub struct ConvertingFutureCallback<U, T> {
    underlying: Option<Box<dyn Callback<T>>>,
    finished_latch: Arc<Semaphore>,
    result: Arc<Mutex<Option<T>>>,
    exception: Arc<Mutex<Option<Box<dyn std::error::Error + Send>>>>,
    cancelled: Arc<Mutex<bool>>,
    current_stage: Arc<Mutex<Option<Stage>>>,
}

impl<U, T> ConvertingFutureCallback<U, T> {
    /// Create a new converting future callback
    pub fn new() -> Self {
        Self::with_underlying(None)
    }

    /// Create a new converting future callback with underlying callback
    pub fn with_underlying(underlying: Option<Box<dyn Callback<T>>>) -> Self {
        Self {
            underlying,
            finished_latch: Arc::new(Semaphore::new(0)),
            result: Arc::new(Mutex::new(None)),
            exception: Arc::new(Mutex::new(None)),
            cancelled: Arc::new(Mutex::new(false)),
            current_stage: Arc::new(Mutex::new(None)),
        }
    }

    /// Convert callback result to future result
    pub fn convert(&self, _result: U) -> T {
        unimplemented!("convert must be implemented by subclass")
    }

    /// Get the result
    pub async fn get(&self) -> Result<T, Box<dyn std::error::Error + Send>> {
        self.finished_latch.acquire().await;
        self.result_impl()
    }

    /// Get the result with timeout
    pub async fn get_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<T, Box<dyn std::error::Error + Send>> {
        match tokio::time::timeout(timeout, self.finished_latch.acquire()).await {
            Ok(_) => self.result_impl(),
            Err(_) => {
                let stage = self.current_stage.lock().await.clone();
                match stage {
                    Some(stage) => Err(Box::new(StagedTimeoutException::new(stage))),
                    None => Err(Box::new(tokio::time::error::Elapsed)),
                }
            }
        }
    }

    /// Check if cancelled
    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.lock().unwrap()
    }

    /// Check if done
    pub fn is_done(&self) -> bool {
        self.finished_latch.available_permits() > 0
    }

    fn result_impl(&self) -> Result<T, Box<dyn std::error::Error + Send>> {
        if *self.cancelled.lock().unwrap() {
            return Err(Box::new(tokio::sync::oneshot::error::RecvError));
        }
        if let Some(ref err) = self.exception.lock().unwrap().as_ref() {
            return Err(err.clone());
        }
        match self.result.lock().unwrap().as_ref() {
            Some(result) => Ok(result.clone()),
            None => Err(Box::new(ConnectException::new("No result available".to_string(), None))),
        }
    }
}

impl<U, T> Callback<U> for ConvertingFutureCallback<U, T> {
    fn on_completion(&self, error: Option<Box<dyn std::error::Error + Send>>, result: Option<U>) {
        let mut finished = self.finished_latch.available_permits() > 0;
        if !finished {
            if let Some(err) = error {
                *self.exception.lock().unwrap() = Some(err);
            } else if let Some(result) = result {
                *self.result.lock().unwrap() = Some(self.convert(result));
            }

            if let Some(ref underlying) = self.underlying.as_ref() {
                underlying.on_completion(error, self.result.lock().unwrap().clone());
            }
            self.finished_latch.add_permits(1);
        }
    }

    fn record_stage(&self, stage: &Stage) {
        *self.current_stage.lock().unwrap() = Some(stage.clone());
    }
}
