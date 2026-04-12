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

use std::sync::Arc;
use tokio::sync::Mutex;

/// Represents a stage of operation with timing information
#[derive(Debug, Clone)]
pub struct Stage {
    description: String,
    started: i64,
    completed: Arc<Mutex<Option<i64>>>,
}

impl Stage {
    /// Create a new stage
    pub fn new(description: String, started: i64) -> Self {
        Self {
            description,
            started,
            completed: Arc::new(Mutex::new(None)),
        }
    }

    /// Get description
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Get start time
    pub fn started(&self) -> i64 {
        self.started
    }

    /// Get completion time
    pub fn completed(&self) -> Option<i64> {
        *self.completed.lock().unwrap()
    }

    /// Mark stage as completed
    pub fn complete(&self, time: i64) {
        let mut completed = self.completed.lock().unwrap();
        if time < self.started {
            log::warn!(
                "Ignoring invalid completion time {} since it is before this stage's start time of {}",
                time, self.started
            );
            return;
        }

        if completed.is_some() {
            log::warn!(
                "Ignoring completion time of {} since this stage was already completed at {}",
                time,
                completed.unwrap()
            );
            return;
        }

        *completed = Some(time);
    }

    /// Summarize this stage
    pub fn summarize(&self) -> String {
        match self.completed() {
            Some(completed) => format!(
                "The last operation the worker completed was {}, which began at {} and completed at {}.",
                self.description(),
                chrono::DateTime::<chrono::Utc>::from_timestamp_millis(self.started())
                    .unwrap()
                    .to_rfc3339(),
                chrono::DateTime::<chrono::Utc>::from_timestamp_millis(completed)
                    .unwrap()
                    .to_rfc3339()
            ),
            None => format!(
                "The worker is currently {}, which began at {}.",
                self.description(),
                chrono::DateTime::<chrono::Utc>::from_timestamp_millis(self.started())
                    .unwrap()
                    .to_rfc3339()
            ),
        }
    }
}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(started {}, completed={:?})",
            self.description,
            self.started,
            self.completed()
        )
    }
}
