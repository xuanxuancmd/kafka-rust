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

use crate::util::{Callback, Stage};
use common_trait::time::Time;

/// Temporary stage that automatically completes when dropped
pub struct TemporaryStage {
    stage: Stage,
    time: Arc<dyn Time>,
}

impl TemporaryStage {
    /// Create a new temporary stage
    pub fn new(description: String, callback: Box<dyn Callback<()>>, time: Arc<dyn Time>) -> Self {
        let stage = Stage::new(description, time.milliseconds());
        callback.record_stage(&stage);
        Self { stage, time }
    }
}

impl Drop for TemporaryStage {
    fn drop(&mut self) {
        self.stage.complete(self.time.milliseconds());
    }
}
