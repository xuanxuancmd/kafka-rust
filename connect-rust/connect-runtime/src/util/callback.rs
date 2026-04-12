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

/// Generic interface for callbacks
pub trait Callback<V> {
    /// Invoked upon completion of the operation.
    ///
    /// # Arguments
    ///
    /// * `error` - the error that caused the operation to fail, or None if no error occurred
    /// * `result` - the return value, or None if the operation failed
    fn on_completion(&self, error: Option<Box<dyn std::error::Error + Send>>, result: Option<V>);

    /// Record a stage for this callback
    fn record_stage(&self, _stage: &Stage) {
        // Default implementation does nothing
    }

    /// Chain staging with another callback
    fn chain_staging<V2>(&self, chained: Box<dyn Callback<V2>>) -> Box<dyn Callback<V2>> {
        Box::new(ChainedCallback::new(chained))
    }
}

/// Helper struct for chaining callbacks
struct ChainedCallback<V> {
    chained: Box<dyn Callback<V>>,
}

impl<V> ChainedCallback<V> {
    fn new(chained: Box<dyn Callback<V>>) -> Self {
        Self { chained }
    }
}

impl<V> Callback<V> for ChainedCallback<V> {
    fn on_completion(&self, error: Option<Box<dyn std::error::Error + Send>>, result: Option<V>) {
        self.chained.on_completion(error, result);
    }

    fn record_stage(&self, stage: &Stage) {
        self.chained.record_stage(stage);
    }
}

// Forward declaration of Stage
struct Stage;
