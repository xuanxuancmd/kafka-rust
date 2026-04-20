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

//! Temporary stage wrapper that auto-completes on close.
//!
//! This corresponds to `org.apache.kafka.connect.util.TemporaryStage` in Java.

use common_trait::util::time::Time;

use super::callback::Callback;
use super::stage::Stage;

/// A temporary stage that automatically completes when closed.
///
/// This corresponds to `org.apache.kafka.connect.util.TemporaryStage` in Java.
/// Used for tracking the duration of operations in a RAII-style manner.
pub struct TemporaryStage {
    stage: Stage,
    time: std::sync::Arc<dyn Time>,
}

impl TemporaryStage {
    /// Creates a new temporary stage that will complete when closed.
    ///
    /// # Arguments
    /// * `description` - A description of the operation being tracked
    /// * `callback` - The callback to record the stage on
    /// * `time` - The time interface for getting current time
    pub fn new<V: Send + Sync + 'static>(
        description: String,
        callback: &dyn Callback<V>,
        time: std::sync::Arc<dyn Time>,
    ) -> Self {
        let stage = Stage::new(description, time.milliseconds());
        callback.record_stage(&stage);
        TemporaryStage { stage, time }
    }

    /// Returns the underlying stage.
    pub fn stage(&self) -> &Stage {
        &self.stage
    }
}

impl Drop for TemporaryStage {
    fn drop(&mut self) {
        self.stage.complete(self.time.milliseconds());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_trait::util::time::SystemTimeImpl;
    use std::sync::Arc;

    struct TestCallback;

    impl Callback<()> for TestCallback {
        fn on_completion(
            &self,
            _error: Option<Box<dyn std::error::Error + Send + Sync>>,
            _result: Option<()>,
        ) {
        }

        fn record_stage(&self, _stage: &Stage) {
            // For testing, just track that this was called
        }
    }

    #[test]
    fn test_temporary_stage_auto_complete() {
        let time = Arc::new(SystemTimeImpl::new());
        let callback = TestCallback;

        let temp_stage = TemporaryStage::new("test operation".to_string(), &callback, time.clone());
        assert!(temp_stage.stage().completed().is_none());

        // When dropped, it should complete
        drop(temp_stage);

        // Cannot check completion after drop since stage is moved,
        // but the Drop implementation ensures it's called
    }
}
