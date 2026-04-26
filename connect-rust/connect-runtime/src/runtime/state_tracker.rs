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

//! Utility class that tracks the current state and the duration of time spent in each state.
//!
//! This class is thread-safe and tracks state transitions and time spent in each state.
//! Corresponds to `org.apache.kafka.connect.runtime.StateTracker` in Java.

use crate::runtime::status::State;
use std::sync::Mutex;

/// Utility class that tracks the current state and the duration of time spent in each state.
///
/// This class is thread-safe and uses atomic operations for efficient concurrent access.
/// It tracks the time spent in each state and can calculate ratios of time spent in specific states.
///
/// Corresponds to `org.apache.kafka.connect.runtime.StateTracker` in Java.
pub struct StateTracker {
    /// The last state change record.
    last_state: Mutex<StateChange>,
}

impl StateTracker {
    /// Creates a new StateTracker with initial empty state.
    pub fn new() -> Self {
        StateTracker {
            last_state: Mutex::new(StateChange::new()),
        }
    }

    /// Change the current state.
    ///
    /// Records the transition from the current state to the new state,
    /// accumulating the time spent in the previous state.
    ///
    /// # Arguments
    /// * `new_state` - The new state to transition to; may not be null
    /// * `now` - The current time in milliseconds
    ///
    /// Corresponds to Java: `public void changeState(State newState, long now)`
    pub fn change_state(&self, new_state: State, now: u64) {
        let mut last_state = self.last_state.lock().unwrap();
        *last_state = last_state.new_state(new_state, now);
    }

    /// Calculate the ratio of time spent in the specified state.
    ///
    /// Returns the ratio of time spent in the specified state to the time spent in all states.
    ///
    /// # Arguments
    /// * `ratio_state` - The state for which the ratio is to be calculated; may not be null
    /// * `now` - The current time in milliseconds
    ///
    /// # Returns
    /// The ratio of time spent in the specified state to the time spent in all states (0.0 to 1.0)
    ///
    /// Corresponds to Java: `public double durationRatio(State ratioState, long now)`
    pub fn duration_ratio(&self, ratio_state: State, now: u64) -> f64 {
        let last_state = self.last_state.lock().unwrap();
        last_state.duration_ratio(ratio_state, now)
    }

    /// Get the current state.
    ///
    /// # Returns
    /// The current state; may be None if no state change has been recorded
    ///
    /// Corresponds to Java: `public State currentState()`
    pub fn current_state(&self) -> Option<State> {
        let last_state = self.last_state.lock().unwrap();
        last_state.state
    }
}

impl Default for StateTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// An immutable record of the accumulated times at the most recent state change.
///
/// This class is required to efficiently make `StateTracker` thread-safe.
/// All time accumulation is performed atomically during state transitions.
///
/// Corresponds to Java: `private static final class StateChange`
#[derive(Debug, Clone)]
struct StateChange {
    /// The current state (may be None if no state has been set).
    state: Option<State>,
    /// The start time of the current state (in milliseconds).
    start_time: u64,
    /// Total time spent in UNASSIGNED state (in milliseconds).
    unassigned_total_time_ms: u64,
    /// Total time spent in RUNNING state (in milliseconds).
    running_total_time_ms: u64,
    /// Total time spent in PAUSED state (in milliseconds).
    paused_total_time_ms: u64,
    /// Total time spent in STOPPED state (in milliseconds).
    stopped_total_time_ms: u64,
    /// Total time spent in FAILED state (in milliseconds).
    failed_total_time_ms: u64,
    /// Total time spent in DESTROYED state (in milliseconds).
    destroyed_total_time_ms: u64,
    /// Total time spent in RESTARTING state (in milliseconds).
    restarting_total_time_ms: u64,
}

impl StateChange {
    /// Creates the initial StateChange instance before any state has changed.
    fn new() -> Self {
        StateChange {
            state: None,
            start_time: 0,
            unassigned_total_time_ms: 0,
            running_total_time_ms: 0,
            paused_total_time_ms: 0,
            stopped_total_time_ms: 0,
            failed_total_time_ms: 0,
            destroyed_total_time_ms: 0,
            restarting_total_time_ms: 0,
        }
    }

    /// Creates a StateChange with all fields specified.
    fn with_values(
        state: Option<State>,
        start_time: u64,
        unassigned_total_time_ms: u64,
        running_total_time_ms: u64,
        paused_total_time_ms: u64,
        stopped_total_time_ms: u64,
        failed_total_time_ms: u64,
        destroyed_total_time_ms: u64,
        restarting_total_time_ms: u64,
    ) -> Self {
        StateChange {
            state,
            start_time,
            unassigned_total_time_ms,
            running_total_time_ms,
            paused_total_time_ms,
            stopped_total_time_ms,
            failed_total_time_ms,
            destroyed_total_time_ms,
            restarting_total_time_ms,
        }
    }

    /// Return a new StateChange that includes the accumulated times of this state plus
    /// the time spent in the current state.
    ///
    /// # Arguments
    /// * `state` - The new state to transition to
    /// * `now` - The time at which the state transition occurs
    ///
    /// # Returns
    /// The new StateChange, though may be this instance if the state did not actually change
    ///
    /// Corresponds to Java: `public StateChange newState(State state, long now)`
    fn new_state(&self, state: State, now: u64) -> Self {
        // Handle initial state transition (no previous state)
        if self.state.is_none() {
            return StateChange::with_values(Some(state), now, 0, 0, 0, 0, 0, 0, 0);
        }

        // If state unchanged, return this instance
        if Some(state) == self.state {
            return self.clone();
        }

        // Calculate duration spent in the previous state
        let duration = now.saturating_sub(self.start_time);

        // Accumulate time for each state
        let mut unassigned_time = self.unassigned_total_time_ms;
        let mut running_time = self.running_total_time_ms;
        let mut paused_time = self.paused_total_time_ms;
        let mut stopped_time = self.stopped_total_time_ms;
        let mut failed_time = self.failed_total_time_ms;
        let mut destroyed_time = self.destroyed_total_time_ms;
        let mut restarting_time = self.restarting_total_time_ms;

        // Add duration to the appropriate state total based on the previous state
        match self.state.unwrap() {
            State::Unassigned => {
                unassigned_time += duration;
            }
            State::Running => {
                running_time += duration;
            }
            State::Paused => {
                paused_time += duration;
            }
            State::Stopped => {
                stopped_time += duration;
            }
            State::Failed => {
                failed_time += duration;
            }
            State::Destroyed => {
                destroyed_time += duration;
            }
            State::Restarting => {
                restarting_time += duration;
            }
            State::Starting => {
                // Starting is not tracked in Java version, but we include it for completeness
            }
        }

        StateChange::with_values(
            Some(state),
            now,
            unassigned_time,
            running_time,
            paused_time,
            stopped_time,
            failed_time,
            destroyed_time,
            restarting_time,
        )
    }

    /// Calculate the ratio of time spent in the specified state.
    ///
    /// # Arguments
    /// * `ratio_state` - The state for which the ratio is to be calculated
    /// * `now` - The current time in milliseconds
    ///
    /// # Returns
    /// The ratio of time spent in the specified state to the time spent in all states
    ///
    /// Corresponds to Java: `public double durationRatio(State ratioState, long now)`
    fn duration_ratio(&self, ratio_state: State, now: u64) -> f64 {
        if self.state.is_none() {
            return 0.0;
        }

        // Calculate time spent in current state
        let duration_current = now.saturating_sub(self.start_time);

        // Calculate time spent in the desired state
        let mut duration_desired = if Some(ratio_state) == self.state {
            duration_current
        } else {
            0
        };

        // Add accumulated time for the desired state
        match ratio_state {
            State::Unassigned => {
                duration_desired += self.unassigned_total_time_ms;
            }
            State::Running => {
                duration_desired += self.running_total_time_ms;
            }
            State::Paused => {
                duration_desired += self.paused_total_time_ms;
            }
            State::Stopped => {
                duration_desired += self.stopped_total_time_ms;
            }
            State::Failed => {
                duration_desired += self.failed_total_time_ms;
            }
            State::Destroyed => {
                duration_desired += self.destroyed_total_time_ms;
            }
            State::Restarting => {
                duration_desired += self.restarting_total_time_ms;
            }
            State::Starting => {
                // Starting is not tracked
            }
        }

        // Calculate total time across all tracked states
        let total = duration_current
            + self.unassigned_total_time_ms
            + self.running_total_time_ms
            + self.paused_total_time_ms
            + self.failed_total_time_ms
            + self.destroyed_total_time_ms
            + self.restarting_total_time_ms;

        if total == 0 {
            return 0.0;
        }

        duration_desired as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_tracker_initial_state() {
        let tracker = StateTracker::new();
        assert_eq!(tracker.current_state(), None);
    }

    #[test]
    fn test_state_tracker_change_state() {
        let tracker = StateTracker::new();

        tracker.change_state(State::Running, 1000);
        assert_eq!(tracker.current_state(), Some(State::Running));

        tracker.change_state(State::Paused, 2000);
        assert_eq!(tracker.current_state(), Some(State::Paused));
    }

    #[test]
    fn test_state_tracker_duration_ratio() {
        let tracker = StateTracker::new();

        // Transition to Running at time 0
        tracker.change_state(State::Running, 0);

        // Transition to Paused at time 1000
        tracker.change_state(State::Paused, 1000);

        // Check duration ratio at time 2000
        // Running: 1000ms, Paused: 1000ms, Total: 2000ms
        let running_ratio = tracker.duration_ratio(State::Running, 2000);
        let paused_ratio = tracker.duration_ratio(State::Paused, 2000);

        assert!((running_ratio - 0.5).abs() < 0.001);
        assert!((paused_ratio - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_state_tracker_no_change_same_state() {
        let tracker = StateTracker::new();

        tracker.change_state(State::Running, 1000);
        tracker.change_state(State::Running, 2000); // Same state, should not change

        // Duration ratio should account for time correctly
        let ratio = tracker.duration_ratio(State::Running, 3000);
        assert!((ratio - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_state_tracker_multiple_transitions() {
        let tracker = StateTracker::new();

        // Sequence: Running(0-1000) -> Paused(1000-3000) -> Running(3000-5000)
        tracker.change_state(State::Running, 0);
        tracker.change_state(State::Paused, 1000);
        tracker.change_state(State::Running, 3000);

        // At time 5000:
        // Running: 1000 + 2000 = 3000ms
        // Paused: 2000ms
        // Total tracked: 5000ms (excluding starting)
        let running_ratio = tracker.duration_ratio(State::Running, 5000);
        let paused_ratio = tracker.duration_ratio(State::Paused, 5000);

        // Running ratio should be 3000/(3000+2000+2000) = 3000/7000 ≈ 0.43
        // Wait, let me recalculate:
        // current duration = 5000 - 3000 = 2000 (in Running state)
        // total = 2000 + 1000 (previous running) + 2000 (paused) = 5000ms
        // Actually: duration_current = 2000, unassigned=0, running=1000, paused=2000, failed=0, destroyed=0, restarting=0
        // But ratio_state is Running and current state is Running, so duration_desired starts with 2000
        // Then adds running_total_time_ms = 1000, giving 3000
        // total = 2000 + 0 + 1000 + 2000 + 0 + 0 + 0 = 5000
        // So ratio = 3000/5000 = 0.6
        assert!((running_ratio - 0.6).abs() < 0.001);
        assert!((paused_ratio - 0.4).abs() < 0.001);
    }

    #[test]
    fn test_state_tracker_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let tracker = Arc::new(StateTracker::new());
        let mut handles = vec![];

        for i in 0..10 {
            let tracker_clone = Arc::clone(&tracker);
            handles.push(thread::spawn(move || {
                tracker_clone.change_state(State::Running, 1000 + i * 100);
                tracker_clone.change_state(State::Paused, 2000 + i * 100);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Final state should be valid (one of the last states set)
        let final_state = tracker.current_state();
        assert!(final_state.is_some());
    }
}
