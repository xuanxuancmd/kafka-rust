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

//! Stage tracking for Connect operations.
//!
//! This corresponds to `org.apache.kafka.connect.util.Stage` in Java.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Represents a stage of operation in Connect.
///
/// This corresponds to `org.apache.kafka.connect.util.Stage` in Java.
/// Used to track progress of operations for debugging and status reporting.
#[derive(Debug)]
pub struct Stage {
    description: String,
    started: i64,
    completed: Mutex<Option<i64>>,
}

impl Stage {
    /// Creates a new stage with the given description and start time.
    ///
    /// # Arguments
    /// * `description` - A description of what this stage represents
    /// * `started` - The start time in milliseconds since epoch
    pub fn new(description: String, started: i64) -> Self {
        Stage {
            description,
            started,
            completed: Mutex::new(None),
        }
    }

    /// Creates a new stage with the given description, using current time as start.
    pub fn new_now(description: String) -> Self {
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Stage::new(description, started)
    }

    /// Returns the description of this stage.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the start time in milliseconds since epoch.
    pub fn started(&self) -> i64 {
        self.started
    }

    /// Returns the completion time if this stage has completed.
    pub fn completed(&self) -> Option<i64> {
        *self.completed.lock().unwrap()
    }

    /// Marks this stage as completed at the given time.
    ///
    /// # Arguments
    /// * `time` - The completion time in milliseconds since epoch
    pub fn complete(&self, time: i64) {
        let mut completed = self.completed.lock().unwrap();

        if time < self.started {
            log::warn!(
                "Ignoring invalid completion time {} since it is before this stage's start time of {}",
                time,
                self.started
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

    /// Marks this stage as completed at the current time.
    pub fn complete_now(&self) {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.complete(time);
    }

    /// Returns a summary string of this stage.
    pub fn summarize(&self) -> String {
        let completed = self.completed.lock().unwrap();
        if let Some(completed_time) = *completed {
            format!(
                "The last operation the worker completed was {}, which began at {} and completed at {}.",
                self.description(),
                format_timestamp(self.started()),
                format_timestamp(completed_time)
            )
        } else {
            format!(
                "The worker is currently {}, which began at {}.",
                self.description(),
                format_timestamp(self.started())
            )
        }
    }
}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let completed = self.completed.lock().unwrap();
        write!(
            f,
            "{}(started {}, completed={})",
            self.description,
            self.started,
            completed
                .map(|t| t.to_string())
                .unwrap_or_else(|| "null".to_string())
        )
    }
}

/// Format a timestamp in milliseconds as a human-readable string.
fn format_timestamp(ms: i64) -> String {
    // Convert milliseconds to seconds for chrono
    let secs = ms / 1000;
    let nanos = ((ms % 1000) * 1_000_000) as u32;

    // Use a simple format since chrono might not be available
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;

    format!("{}d {}h {}m {}s", days, hours, minutes, seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stage_creation() {
        let stage = Stage::new("creating connector".to_string(), 1000);
        assert_eq!(stage.description(), "creating connector");
        assert_eq!(stage.started(), 1000);
        assert!(stage.completed().is_none());
    }

    #[test]
    fn test_stage_completion() {
        let stage = Stage::new("test".to_string(), 1000);
        stage.complete(2000);
        assert_eq!(stage.completed(), Some(2000));
    }

    #[test]
    fn test_stage_invalid_completion_before_start() {
        let stage = Stage::new("test".to_string(), 1000);
        stage.complete(500); // Before start time
        assert!(stage.completed().is_none());
    }

    #[test]
    fn test_stage_double_completion() {
        let stage = Stage::new("test".to_string(), 1000);
        stage.complete(2000);
        stage.complete(3000); // Should be ignored
        assert_eq!(stage.completed(), Some(2000));
    }

    #[test]
    fn test_stage_summarize_incomplete() {
        let stage = Stage::new("running task".to_string(), 1000);
        let summary = stage.summarize();
        assert!(summary.contains("currently"));
        assert!(summary.contains("running task"));
    }

    #[test]
    fn test_stage_summarize_complete() {
        let stage = Stage::new("running task".to_string(), 1000);
        stage.complete(2000);
        let summary = stage.summarize();
        assert!(summary.contains("completed"));
        assert!(summary.contains("running task"));
    }
}
