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

//! WorkerTask trait - Core abstraction for task execution in Kafka Connect.
//!
//! This module defines the WorkerTask trait that manages the lifecycle,
//! state transitions, and execution of individual source/sink tasks.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.WorkerTask` in Java.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common_trait::errors::ConnectError;
use common_trait::worker::{ConnectorTaskId, TargetState};

/// Task configuration containing key-value properties.
///
/// Corresponds to `org.apache.kafka.connect.runtime.TaskConfig` in Java.
#[derive(Debug, Clone)]
pub struct TaskConfig {
    config: HashMap<String, String>,
}

impl TaskConfig {
    /// Creates a new TaskConfig from a HashMap.
    pub fn new(config: HashMap<String, String>) -> Self {
        TaskConfig { config }
    }

    /// Returns the configuration map.
    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Gets a configuration value by key.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.config.get(key)
    }
}

impl Default for TaskConfig {
    fn default() -> Self {
        TaskConfig {
            config: HashMap::new(),
        }
    }
}

/// Task metrics for tracking commits and batch processing.
///
/// Corresponds to `WorkerTask.TaskMetricsGroup` in Java.
#[derive(Debug, Default)]
pub struct TaskMetrics {
    /// Total number of successful commits.
    commit_success_count: u64,
    /// Total number of failed commits.
    commit_failure_count: u64,
    /// Total commit duration in milliseconds.
    total_commit_duration_ms: u64,
    /// Total number of batches processed.
    batch_count: u64,
    /// Total batch size.
    total_batch_size: u64,
}

impl TaskMetrics {
    /// Creates new TaskMetrics.
    pub fn new() -> Self {
        TaskMetrics::default()
    }

    /// Records a commit attempt with duration and success status.
    pub fn record_commit(&mut self, duration_ms: u64, success: bool) {
        if success {
            self.commit_success_count += 1;
        } else {
            self.commit_failure_count += 1;
        }
        self.total_commit_duration_ms += duration_ms;
    }

    /// Records a batch of processed records.
    pub fn record_batch(&mut self, size: usize) {
        self.batch_count += 1;
        self.total_batch_size += size as u64;
    }

    /// Returns the number of successful commits.
    pub fn commit_success_count(&self) -> u64 {
        self.commit_success_count
    }

    /// Returns the number of failed commits.
    pub fn commit_failure_count(&self) -> u64 {
        self.commit_failure_count
    }

    /// Returns the average commit duration in milliseconds.
    pub fn average_commit_duration_ms(&self) -> u64 {
        let total = self.commit_success_count + self.commit_failure_count;
        if total == 0 {
            0
        } else {
            self.total_commit_duration_ms / total
        }
    }

    /// Returns the average batch size.
    pub fn average_batch_size(&self) -> u64 {
        if self.batch_count == 0 {
            0
        } else {
            self.total_batch_size / self.batch_count
        }
    }

    /// Returns the number of batches processed.
    pub fn batch_count(&self) -> u64 {
        self.batch_count
    }
}

/// Thread-safe task status listener trait.
///
/// This is defined in connect-runtime to add Send + Sync constraints
/// without modifying common-trait. WorkerTask requires thread-safe
/// listeners for multi-threaded execution.
///
/// Implementations must be Send + Sync to be used with WorkerTask.
pub trait ThreadSafeTaskStatusListener: Send + Sync {
    /// Called when the task starts.
    fn on_startup(&self, id: &ConnectorTaskId);

    /// Called when the task is running.
    fn on_running(&self, id: &ConnectorTaskId);

    /// Called when the task is paused.
    fn on_pause(&self, id: &ConnectorTaskId);

    /// Called when the task is stopping.
    fn on_shutdown(&self, id: &ConnectorTaskId);

    /// Called when the task has stopped.
    fn on_stopped(&self, id: &ConnectorTaskId);

    /// Called when the task fails with an error message.
    fn on_failure(&self, id: &ConnectorTaskId, error_message: &str);

    /// Called when the task is restarting.
    fn on_restart(&self, id: &ConnectorTaskId);
}

/// Internal state for WorkerTask execution.
struct WorkerTaskState {
    /// Current target state (Started/Paused/Stopped).
    target_state: TargetState,
    /// Whether the task is stopping.
    stopping: bool,
    /// Whether the task has failed.
    failed: bool,
    /// Whether the task has started.
    started: bool,
    /// Whether the task is paused.
    paused: bool,
    /// Stored error message if task failed.
    error_message: Option<Arc<String>>,
}

impl Default for WorkerTaskState {
    fn default() -> Self {
        WorkerTaskState {
            target_state: TargetState::Started,
            stopping: false,
            failed: false,
            started: false,
            paused: false,
            error_message: None,
        }
    }
}

/// WorkerTask trait - core abstraction for task execution.
///
/// This trait defines the lifecycle, state transitions, and execution
/// patterns for both source and sink tasks in Kafka Connect.
///
/// Implementations must provide:
/// - `initialize`: Set up task with configuration
/// - `do_start`: Start the task execution
/// - `execute`: Main execution loop
/// - `close`: Clean up resources
///
/// The trait provides default implementations for:
/// - `start`: Wrapper that calls `do_start`
/// - `stop`: Trigger task shutdown
/// - `transition_to`: State transitions
/// - `await_stop`: Wait for task completion
/// - `pause/resume`: Pause and resume operations
pub trait WorkerTask: Send + Sync {
    /// Returns the task ID.
    fn id(&self) -> &ConnectorTaskId;

    /// Returns the thread-safe status listener.
    fn status_listener(&self) -> &dyn ThreadSafeTaskStatusListener;

    /// Returns a mutable reference to the task metrics.
    fn metrics(&mut self) -> &mut TaskMetrics;

    /// Initialize the task with configuration.
    ///
    /// Called before `do_start` to set up internal components.
    /// Corresponds to `initialize(TaskConfig)` in Java.
    fn initialize(&mut self, config: TaskConfig) -> Result<(), ConnectError>;

    /// Start the task execution.
    ///
    /// Called after successful initialization.
    /// Corresponds to `doStart()` in Java.
    fn do_start(&mut self) -> Result<(), ConnectError>;

    /// Execute the main task loop.
    ///
    /// Called repeatedly while task is running.
    /// For source tasks: polls for new records.
    /// For sink tasks: processes consumed records.
    /// Corresponds to `execute()` in Java.
    fn execute(&mut self) -> Result<(), ConnectError>;

    /// Close and release all resources.
    ///
    /// Called during shutdown sequence.
    /// Corresponds to `close()` in Java.
    fn close(&mut self) -> Result<(), ConnectError>;

    /// Commit offsets.
    ///
    /// Called to commit processed offsets.
    /// Corresponds to `commitOffsets()` in Java.
    fn commit_offsets(&mut self) -> Result<(), ConnectError>;

    /// Called when task transitions to paused state.
    ///
    /// Override to implement pause-specific behavior.
    fn on_pause(&mut self) {
        self.status_listener().on_pause(self.id());
    }

    /// Called when task transitions to resumed state.
    ///
    /// Override to implement resume-specific behavior.
    fn on_resume(&mut self) {
        self.status_listener().on_running(self.id());
    }

    /// Called when task starts up.
    fn on_startup(&mut self) {
        self.status_listener().on_startup(self.id());
    }

    /// Called when task shuts down normally.
    fn on_shutdown(&mut self) {
        self.status_listener().on_shutdown(self.id());
    }

    /// Called when task fails with an error.
    fn on_failure(&mut self, error: &ConnectError) {
        self.status_listener()
            .on_failure(self.id(), error.message());
    }

    /// Called when task stops (transition to stopped state).
    fn on_stop(&mut self) {
        self.status_listener().on_stopped(self.id());
    }

    /// Check if the task is stopping.
    fn is_stopping(&self) -> bool;

    /// Check if the task is paused.
    fn is_paused(&self) -> bool;

    /// Check if the task has started.
    fn is_started(&self) -> bool;

    /// Check if the task has failed.
    fn is_failed(&self) -> bool;

    /// Get the current target state.
    fn target_state(&self) -> TargetState;

    /// Set the target state.
    fn set_target_state(&mut self, state: TargetState);

    /// Set the stopping flag.
    fn set_stopping(&mut self, stopping: bool);

    /// Set the started flag.
    fn set_started(&mut self, started: bool);

    /// Set the paused flag.
    fn set_paused(&mut self, paused: bool);

    /// Set the failed flag and store error message.
    fn set_failed(&mut self, failed: bool, error_message: Option<String>);

    /// Get the stored error message if task failed.
    ///
    /// Returns the error message string, not a reference to avoid
    /// lifetime issues with MutexGuard.
    fn get_error_message(&self) -> Option<String>;

    /// Transition to a new target state.
    ///
    /// This method handles state transitions:
    /// - STARTED: Resume task if paused
    /// - PAUSED: Pause task execution
    /// - STOPPED: Stop task completely
    ///
    /// Corresponds to `transitionTo(TargetState)` in Java.
    fn transition_to(&mut self, state: TargetState) {
        let prev_state = self.target_state();
        self.set_target_state(state);

        match state {
            TargetState::Started => {
                if prev_state == TargetState::Paused {
                    self.set_paused(false);
                    self.on_resume();
                }
            }
            TargetState::Paused => {
                self.set_paused(true);
                self.on_pause();
            }
            TargetState::Stopped => {
                self.set_stopping(true);
            }
        }
    }

    /// Start the task.
    ///
    /// Calls `do_start` and notifies the status listener.
    /// Corresponds to `start()` wrapper in Java.
    fn start(&mut self) -> Result<(), ConnectError> {
        self.set_started(true);
        self.on_startup();
        self.do_start()?;
        self.status_listener().on_running(self.id());
        Ok(())
    }

    /// Stop the task.
    ///
    /// Sets the stopping flag and wakes up any waiting threads.
    /// Does not wait for task to complete.
    /// Corresponds to `stop()` in Java.
    fn stop(&mut self) {
        self.set_stopping(true);
        self.set_target_state(TargetState::Stopped);
    }

    /// Wait for the task to stop.
    ///
    /// Blocks until the task completes or timeout expires.
    /// Returns true if task stopped within timeout, false otherwise.
    /// Corresponds to `awaitStop(long)` in Java.
    fn await_stop(&self, timeout: Duration) -> bool;

    /// Run the main execution loop.
    ///
    /// This method:
    /// 1. Checks for stopping/paused states
    /// 2. Calls `execute` repeatedly
    /// 3. Handles errors and notifies listener
    ///
    /// Corresponds to `doRun()` in Java.
    fn do_run(&mut self) {
        if self.is_stopping() {
            return;
        }

        // Wait if paused
        while self.is_paused() && !self.is_stopping() {
            if self.target_state() == TargetState::Stopped {
                return;
            }
        }

        if self.is_stopping() {
            return;
        }

        // Main execution loop
        while !self.is_stopping() && !self.is_paused() {
            let result = self.execute();
            match result {
                Ok(()) => continue,
                Err(e) => {
                    let error_msg = e.message().to_string();
                    self.set_failed(true, Some(error_msg.clone()));
                    self.on_failure(&e);
                    return;
                }
            }
        }
    }

    /// Record a commit success with duration.
    fn record_commit_success(&mut self, duration_ms: u64) {
        self.metrics().record_commit(duration_ms, true);
    }

    /// Record a commit failure with duration.
    fn record_commit_failure(&mut self, duration_ms: u64) {
        self.metrics().record_commit(duration_ms, false);
    }

    /// Record a batch of processed records.
    fn record_batch(&mut self, size: usize) {
        self.metrics().record_batch(size);
    }
}

/// A simple implementation of WorkerTask for testing.
///
/// This provides a concrete implementation that can be used
/// to test the WorkerTask trait behavior.
pub struct SimpleWorkerTask {
    id: ConnectorTaskId,
    status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
    metrics: TaskMetrics,
    state: Mutex<WorkerTaskState>,
    stop_condvar: Condvar,
    initialized: AtomicBool,
    started: AtomicBool,
    execute_count: Mutex<u64>,
    close_called: AtomicBool,
}

impl SimpleWorkerTask {
    /// Creates a new SimpleWorkerTask.
    pub fn new(
        id: ConnectorTaskId,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
    ) -> Self {
        SimpleWorkerTask {
            id,
            status_listener,
            metrics: TaskMetrics::new(),
            state: Mutex::new(WorkerTaskState::default()),
            stop_condvar: Condvar::new(),
            initialized: AtomicBool::new(false),
            started: AtomicBool::new(false),
            execute_count: Mutex::new(0),
            close_called: AtomicBool::new(false),
        }
    }

    /// Returns whether initialize was called.
    pub fn was_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    /// Returns whether start was called.
    pub fn was_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Returns whether close was called.
    pub fn was_closed(&self) -> bool {
        self.close_called.load(Ordering::SeqCst)
    }

    /// Returns the execute call count.
    pub fn execute_count(&self) -> u64 {
        *self.execute_count.lock().unwrap()
    }

    /// Sets the execute count for testing.
    pub fn set_execute_count(&self, count: u64) {
        *self.execute_count.lock().unwrap() = count;
    }

    /// Signals that the task has completed stopping.
    pub fn signal_stopped(&self) {
        self.stop_condvar.notify_all();
    }
}

impl WorkerTask for SimpleWorkerTask {
    fn id(&self) -> &ConnectorTaskId {
        &self.id
    }

    fn status_listener(&self) -> &dyn ThreadSafeTaskStatusListener {
        self.status_listener.as_ref()
    }

    fn metrics(&mut self) -> &mut TaskMetrics {
        &mut self.metrics
    }

    fn initialize(&mut self, _config: TaskConfig) -> Result<(), ConnectError> {
        self.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn do_start(&mut self) -> Result<(), ConnectError> {
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn execute(&mut self) -> Result<(), ConnectError> {
        let mut count = self.execute_count.lock().unwrap();
        *count += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ConnectError> {
        self.close_called.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn commit_offsets(&mut self) -> Result<(), ConnectError> {
        self.record_commit_success(10);
        Ok(())
    }

    fn is_stopping(&self) -> bool {
        self.state.lock().unwrap().stopping
    }

    fn is_paused(&self) -> bool {
        self.state.lock().unwrap().paused
    }

    fn is_started(&self) -> bool {
        self.state.lock().unwrap().started
    }

    fn is_failed(&self) -> bool {
        self.state.lock().unwrap().failed
    }

    fn target_state(&self) -> TargetState {
        self.state.lock().unwrap().target_state
    }

    fn set_target_state(&mut self, state: TargetState) {
        self.state.lock().unwrap().target_state = state;
    }

    fn set_stopping(&mut self, stopping: bool) {
        self.state.lock().unwrap().stopping = stopping;
        if stopping {
            self.stop_condvar.notify_all();
        }
    }

    fn set_started(&mut self, started: bool) {
        self.state.lock().unwrap().started = started;
    }

    fn set_paused(&mut self, paused: bool) {
        self.state.lock().unwrap().paused = paused;
    }

    fn set_failed(&mut self, failed: bool, error_message: Option<String>) {
        let mut state = self.state.lock().unwrap();
        state.failed = failed;
        state.error_message = error_message.map(|s| Arc::new(s));
    }

    fn get_error_message(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap()
            .error_message
            .as_ref()
            .map(|s| s.to_string())
    }

    fn await_stop(&self, timeout: Duration) -> bool {
        let state = self.state.lock().unwrap();
        let result = self
            .stop_condvar
            .wait_timeout_while(state, timeout, |s| !s.stopping);
        match result {
            Ok((_state, timeout_result)) => !timeout_result.timed_out(),
            Err(_) => false,
        }
    }
}

/// A mock status listener for testing.
///
/// Records all status events for verification in tests.
/// Implements ThreadSafeTaskStatusListener with Send + Sync.
pub struct MockTaskStatusListener {
    events: Mutex<Vec<(String, ConnectorTaskId, Option<String>)>>,
}

impl MockTaskStatusListener {
    /// Creates a new MockTaskStatusListener.
    pub fn new() -> Self {
        MockTaskStatusListener {
            events: Mutex::new(Vec::new()),
        }
    }

    /// Returns all recorded events.
    pub fn events(&self) -> Vec<(String, ConnectorTaskId, Option<String>)> {
        self.events.lock().unwrap().clone()
    }

    /// Clears all recorded events.
    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    /// Returns the last event type.
    pub fn last_event_type(&self) -> Option<String> {
        self.events.lock().unwrap().last().map(|e| e.0.clone())
    }

    /// Returns the count of events with a specific type.
    pub fn count_event_type(&self, event_type: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.0 == event_type)
            .count()
    }

    /// Returns the last failure error message.
    pub fn last_failure_message(&self) -> Option<String> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.0 == "failure")
            .last()
            .and_then(|e| e.2.clone())
    }
}

impl Default for MockTaskStatusListener {
    fn default() -> Self {
        MockTaskStatusListener::new()
    }
}

impl ThreadSafeTaskStatusListener for MockTaskStatusListener {
    fn on_startup(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(("startup".to_string(), id.clone(), None));
    }

    fn on_running(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(("running".to_string(), id.clone(), None));
    }

    fn on_pause(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(("pause".to_string(), id.clone(), None));
    }

    fn on_shutdown(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(("shutdown".to_string(), id.clone(), None));
    }

    fn on_stopped(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(("stopped".to_string(), id.clone(), None));
    }

    fn on_failure(&self, id: &ConnectorTaskId, error_message: &str) {
        self.events.lock().unwrap().push((
            "failure".to_string(),
            id.clone(),
            Some(error_message.to_string()),
        ));
    }

    fn on_restart(&self, id: &ConnectorTaskId) {
        self.events
            .lock()
            .unwrap()
            .push(("restart".to_string(), id.clone(), None));
    }
}

/// A failing worker task for error testing.
///
/// This implementation returns errors from execute/close to test error handling.
pub struct FailingWorkerTask {
    id: ConnectorTaskId,
    status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
    metrics: TaskMetrics,
    state: Mutex<WorkerTaskState>,
    stop_condvar: Condvar,
    fail_on_execute: AtomicBool,
    fail_on_close: AtomicBool,
    fail_on_initialize: AtomicBool,
}

impl FailingWorkerTask {
    /// Creates a new FailingWorkerTask.
    pub fn new(
        id: ConnectorTaskId,
        status_listener: Arc<dyn ThreadSafeTaskStatusListener>,
    ) -> Self {
        FailingWorkerTask {
            id,
            status_listener,
            metrics: TaskMetrics::new(),
            state: Mutex::new(WorkerTaskState::default()),
            stop_condvar: Condvar::new(),
            fail_on_execute: AtomicBool::new(false),
            fail_on_close: AtomicBool::new(false),
            fail_on_initialize: AtomicBool::new(false),
        }
    }

    /// Set whether execute should fail.
    pub fn set_fail_on_execute(&self, fail: bool) {
        self.fail_on_execute.store(fail, Ordering::SeqCst);
    }

    /// Set whether close should fail.
    pub fn set_fail_on_close(&self, fail: bool) {
        self.fail_on_close.store(fail, Ordering::SeqCst);
    }

    /// Set whether initialize should fail.
    pub fn set_fail_on_initialize(&self, fail: bool) {
        self.fail_on_initialize.store(fail, Ordering::SeqCst);
    }
}

impl WorkerTask for FailingWorkerTask {
    fn id(&self) -> &ConnectorTaskId {
        &self.id
    }

    fn status_listener(&self) -> &dyn ThreadSafeTaskStatusListener {
        self.status_listener.as_ref()
    }

    fn metrics(&mut self) -> &mut TaskMetrics {
        &mut self.metrics
    }

    fn initialize(&mut self, _config: TaskConfig) -> Result<(), ConnectError> {
        if self.fail_on_initialize.load(Ordering::SeqCst) {
            return Err(ConnectError::general("Initialize failed"));
        }
        Ok(())
    }

    fn do_start(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }

    fn execute(&mut self) -> Result<(), ConnectError> {
        if self.fail_on_execute.load(Ordering::SeqCst) {
            return Err(ConnectError::general("Execute failed"));
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ConnectError> {
        if self.fail_on_close.load(Ordering::SeqCst) {
            return Err(ConnectError::general("Close failed"));
        }
        Ok(())
    }

    fn commit_offsets(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }

    fn is_stopping(&self) -> bool {
        self.state.lock().unwrap().stopping
    }

    fn is_paused(&self) -> bool {
        self.state.lock().unwrap().paused
    }

    fn is_started(&self) -> bool {
        self.state.lock().unwrap().started
    }

    fn is_failed(&self) -> bool {
        self.state.lock().unwrap().failed
    }

    fn target_state(&self) -> TargetState {
        self.state.lock().unwrap().target_state
    }

    fn set_target_state(&mut self, state: TargetState) {
        self.state.lock().unwrap().target_state = state;
    }

    fn set_stopping(&mut self, stopping: bool) {
        self.state.lock().unwrap().stopping = stopping;
        if stopping {
            self.stop_condvar.notify_all();
        }
    }

    fn set_started(&mut self, started: bool) {
        self.state.lock().unwrap().started = started;
    }

    fn set_paused(&mut self, paused: bool) {
        self.state.lock().unwrap().paused = paused;
    }

    fn set_failed(&mut self, failed: bool, error_message: Option<String>) {
        let mut state = self.state.lock().unwrap();
        state.failed = failed;
        state.error_message = error_message.map(|s| Arc::new(s));
    }

    fn get_error_message(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap()
            .error_message
            .as_ref()
            .map(|s| s.to_string())
    }

    fn await_stop(&self, timeout: Duration) -> bool {
        let state = self.state.lock().unwrap();
        let result = self
            .stop_condvar
            .wait_timeout_while(state, timeout, |s| !s.stopping);
        match result {
            Ok((_state, timeout_result)) => !timeout_result.timed_out(),
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    fn create_test_task_with_listener(listener: Arc<MockTaskStatusListener>) -> SimpleWorkerTask {
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        SimpleWorkerTask::new(id, listener)
    }

    #[test]
    fn test_task_config_creation() {
        let mut config = HashMap::new();
        config.insert("key".to_string(), "value".to_string());
        let task_config = TaskConfig::new(config);

        assert_eq!(task_config.get("key"), Some(&"value".to_string()));
        assert_eq!(task_config.get("missing"), None);
    }

    #[test]
    fn test_task_metrics_record_commit() {
        let mut metrics = TaskMetrics::new();

        metrics.record_commit(100, true);
        metrics.record_commit(200, false);
        metrics.record_commit(50, true);

        assert_eq!(metrics.commit_success_count(), 2);
        assert_eq!(metrics.commit_failure_count(), 1);
        assert_eq!(metrics.average_commit_duration_ms(), 116); // (100+200+50)/3
    }

    #[test]
    fn test_task_metrics_record_batch() {
        let mut metrics = TaskMetrics::new();

        metrics.record_batch(10);
        metrics.record_batch(20);
        metrics.record_batch(30);

        assert_eq!(metrics.batch_count(), 3);
        assert_eq!(metrics.average_batch_size(), 20); // (10+20+30)/3
    }

    /// Test 1: Standard startup and shutdown sequence.
    /// Corresponds to `standardStartup()` in WorkerTaskTest.java.
    #[test]
    fn test_standard_startup() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let mut task = create_test_task_with_listener(listener.clone());

        let config = TaskConfig::default();
        task.initialize(config).unwrap();
        task.start().unwrap();

        // Verify startup was called
        assert!(task.was_initialized());
        assert!(task.was_started());
        assert_eq!(listener.count_event_type("startup"), 1);
        assert_eq!(listener.count_event_type("running"), 1);

        // Shutdown
        task.stop();
        task.close().unwrap();

        assert!(task.is_stopping());
    }

    /// Test 2: Stop before task starts.
    /// Corresponds to `stopBeforeStarting()` in WorkerTaskTest.java.
    #[test]
    fn test_stop_before_starting() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let mut task = create_test_task_with_listener(listener.clone());

        // Stop before starting
        task.stop();

        // Verify initialize and start were not called
        assert!(!task.was_initialized());
        assert!(!task.was_started());

        // No startup event should be recorded
        assert_eq!(listener.count_event_type("startup"), 0);
    }

    /// Test 3: Cancel before stopping completes.
    /// Corresponds to `cancelBeforeStopping()` in WorkerTaskTest.java.
    #[test]
    fn test_cancel_before_stopping() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let mut task = create_test_task_with_listener(listener.clone());

        // Start the task
        let config = TaskConfig::default();
        task.initialize(config).unwrap();
        task.start().unwrap();

        // Verify startup event
        assert_eq!(listener.count_event_type("startup"), 1);

        // Stop without waiting for shutdown
        task.stop();
        // Don't call close() - simulate cancellation

        // Task should be stopping but not have shutdown event yet
        assert!(task.is_stopping());
    }

    /// Test 4: Metrics update on listener events.
    /// Corresponds to `updateMetricsOnListenerEventsForStartupPauseResumeAndShutdown()` in WorkerTaskTest.java.
    #[test]
    fn test_metrics_on_listener_events() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let mut task = create_test_task_with_listener(listener.clone());

        // Startup
        let config = TaskConfig::default();
        task.initialize(config).unwrap();
        task.start().unwrap();

        // Pause
        task.transition_to(TargetState::Paused);
        assert!(task.is_paused());
        assert_eq!(listener.count_event_type("pause"), 1);

        // Resume
        task.transition_to(TargetState::Started);
        assert!(!task.is_paused());
        assert_eq!(listener.count_event_type("running"), 2); // startup + resume

        // Shutdown
        task.stop();
        assert!(task.is_stopping());
    }

    /// Test 5: Transition to paused state.
    #[test]
    fn test_transition_to_paused() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let mut task = create_test_task_with_listener(listener.clone());

        let config = TaskConfig::default();
        task.initialize(config).unwrap();
        task.start().unwrap();

        // Transition to paused
        task.transition_to(TargetState::Paused);

        assert!(task.is_paused());
        assert_eq!(task.target_state(), TargetState::Paused);
        assert_eq!(listener.count_event_type("pause"), 1);
    }

    /// Test 6: Transition to stopped state.
    #[test]
    fn test_transition_to_stopped() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let mut task = create_test_task_with_listener(listener.clone());

        let config = TaskConfig::default();
        task.initialize(config).unwrap();
        task.start().unwrap();

        // Transition to stopped
        task.transition_to(TargetState::Stopped);

        assert!(task.is_stopping());
        assert_eq!(task.target_state(), TargetState::Stopped);
    }

    /// Test 7: Commit offsets and record metrics.
    #[test]
    fn test_commit_offsets() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let mut task = create_test_task_with_listener(listener.clone());

        let config = TaskConfig::default();
        task.initialize(config).unwrap();
        task.start().unwrap();

        // Commit offsets
        task.commit_offsets().unwrap();

        // Verify metrics
        assert_eq!(task.metrics().commit_success_count(), 1);
    }

    /// Test 8: Error handling in execute - verifies get_error_message returns real error.
    /// Corresponds to error propagation tests in WorkerTaskTest.java.
    #[test]
    fn test_error_handling_with_real_error_message() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut task = FailingWorkerTask::new(id, listener.clone());

        task.set_fail_on_execute(true);

        let config = TaskConfig::default();
        task.initialize(config).unwrap();
        task.start().unwrap();

        // Execute should fail
        let result = task.execute();
        assert!(result.is_err());

        // Set failed with error message
        let error = result.unwrap_err();
        let error_msg = error.message().to_string();
        task.set_failed(true, Some(error_msg.clone()));

        // Verify get_error_message returns the real error message
        let stored_error = task.get_error_message();
        assert!(stored_error.is_some());
        assert_eq!(stored_error.unwrap(), "Execute failed");

        // Call on_failure and verify listener received the error message
        task.on_failure(&error);
        assert!(task.is_failed());
        assert_eq!(listener.count_event_type("failure"), 1);
        assert_eq!(
            listener.last_failure_message(),
            Some("Execute failed".to_string())
        );
    }

    /// Test 9: Close resources even on error.
    /// Corresponds to `testCloseClosesManagedResources` and `testCloseClosesManagedResourcesIfSubclassThrows` in WorkerTaskTest.java.
    #[test]
    fn test_close_on_error() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut task = FailingWorkerTask::new(id, listener.clone());

        task.set_fail_on_close(true);

        let config = TaskConfig::default();
        task.initialize(config).unwrap();
        task.start().unwrap();

        // Close should fail but resources should still be released
        let result = task.close();
        assert!(result.is_err());
    }

    #[test]
    fn test_record_batch_metrics() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let mut task = create_test_task_with_listener(listener.clone());

        let config = TaskConfig::default();
        task.initialize(config).unwrap();

        // Record batches
        task.record_batch(10);
        task.record_batch(20);
        task.record_batch(30);

        assert_eq!(task.metrics().batch_count(), 3);
        assert_eq!(task.metrics().average_batch_size(), 20);
    }

    #[test]
    fn test_commit_failure_metrics() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let mut task = create_test_task_with_listener(listener.clone());

        let config = TaskConfig::default();
        task.initialize(config).unwrap();

        // Record commit failures
        task.record_commit_failure(50);
        task.record_commit_failure(100);

        assert_eq!(task.metrics().commit_failure_count(), 2);
        assert_eq!(task.metrics().average_commit_duration_ms(), 75);
    }

    #[test]
    fn test_await_stop() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut task = SimpleWorkerTask::new(id, listener);

        // Task not stopping initially
        let result = task.await_stop(Duration::from_millis(100));
        assert!(!result); // Timeout because not stopping

        // Signal stopped and set stopping
        task.signal_stopped();
        task.set_stopping(true);

        // Now await_stop should return quickly
        let result = task.await_stop(Duration::from_millis(100));
        assert!(result);
    }

    /// Test: Verify error message is properly stored and retrieved.
    #[test]
    fn test_error_message_storage() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let mut task = create_test_task_with_listener(listener.clone());

        // Initially no error
        assert!(task.get_error_message().is_none());

        // Set an error
        task.set_failed(true, Some("Test error message".to_string()));

        // Verify error is stored
        assert!(task.is_failed());
        let error_msg = task.get_error_message();
        assert!(error_msg.is_some());
        assert_eq!(error_msg.unwrap(), "Test error message");

        // Clear error
        task.set_failed(false, None);
        assert!(task.get_error_message().is_none());
    }

    /// Test: do_run properly handles errors and stores error message.
    #[test]
    fn test_do_run_error_handling() {
        let listener = Arc::new(MockTaskStatusListener::new());
        let id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut task = FailingWorkerTask::new(id, listener.clone());

        task.set_fail_on_execute(true);

        let config = TaskConfig::default();
        task.initialize(config).unwrap();

        // Run do_run - it should handle the error
        task.do_run();

        // Verify error was stored
        assert!(task.is_failed());
        let error_msg = task.get_error_message();
        assert!(error_msg.is_some());
        assert_eq!(error_msg.unwrap(), "Execute failed");

        // Verify listener received failure event
        assert_eq!(listener.count_event_type("failure"), 1);
    }
}
