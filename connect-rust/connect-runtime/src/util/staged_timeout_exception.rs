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

use crate::util::Stage;
use std::time::Duration;

/// Timeout exception that includes stage information
#[derive(Debug)]
pub struct StagedTimeoutException {
    stage: Stage,
}

impl StagedTimeoutException {
    /// Create a new staged timeout exception
    pub fn new(stage: Stage) -> Self {
        Self { stage }
    }

    /// Get the stage
    pub fn stage(&self) -> &Stage {
        &self.stage
    }
}

impl std::error::Error for StagedTimeoutException {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    fn description(&self) -> String {
        format!("Timeout occurred during stage: {}", self.stage.summarize())
    }
}

impl From<Duration> for StagedTimeoutException {
    fn from(_duration: Duration) -> Self {
        Self {
            stage: Stage::new("unknown".to_string(), 0),
        }
    }
}
