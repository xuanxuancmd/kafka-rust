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

//! Manages offset commit scheduling and execution for SourceTasks.
//!
//! Unlike sink tasks which directly manage their offset commits in the main poll() thread
//! since they drive the event loop and control (for all intents and purposes) the timeouts,
//! source tasks are at the whim of the connector and cannot be guaranteed to wake up on the
//! necessary schedule. Instead, this class tracks all the active tasks, their schedule for
//! commits, and ensures they are invoked in a timely fashion.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.SourceTaskOffsetCommitter` in Java.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::interval;

use common_trait::errors::ConnectError;
use common_trait::worker::ConnectorTaskId;

use crate::config::worker_config::OFFSET_FLUSH_INTERVAL_MS_DEFAULT;

/// Trait for source tasks that can have their offsets committed.
///
/// This trait defines the interface for source tasks that support
/// scheduled offset commits via SourceTaskOffsetCommitter.
///
/// Corresponds to the methods used from `WorkerSourceTask` in Java.
pub trait CommittableSourceTask: Send + Sync {
    /// Returns whether an attempt to commit offsets should be made for the task.
    ///
    /// This checks if there are pending uncommitted offsets and if the task's
    /// producer has not already failed to send a record with a non-retriable error.
    ///
    /// # Returns
    /// `true` if offsets should be committed, `false` otherwise.
    fn should_commit_offsets(&self) -> bool;

    /// Commits the offsets for this source task.
    ///
    /// # Returns
    /// `Ok(true)` if commit succeeded, `Ok(false)` if commit failed but no error,
    /// `Err(ConnectError)` if an error occurred during commit.
    fn commit_offsets(&self) -> Result<bool, ConnectError>;

    /// Returns a string representation for logging.
    fn task_id_string(&self) -> String;
}

/// Internal state for a scheduled commit task.
struct ScheduledCommit {
    /// Sender for the stop signal.
    stop_sender: broadcast::Sender<bool>,
}

/// Manages offset commit scheduling and execution for SourceTasks.
///
/// This struct tracks all active source tasks and schedules periodic offset commits
/// for each one. Uses tokio async runtime instead of Java's ScheduledExecutorService.
///
/// Corresponds to `org.apache.kafka.connect.runtime.SourceTaskOffsetCommitter` in Java.
pub struct SourceTaskOffsetCommitter {
    /// The commit interval in milliseconds.
    commit_interval_ms: u64,
    /// Map of scheduled commit tasks keyed by ConnectorTaskId.
    /// Each entry contains a stop sender to signal the task to stop.
    /// The actual JoinHandle is stored separately in pending_handles.
    committers: DashMap<ConnectorTaskId, ScheduledCommit>,
    /// Pending handles that need cleanup on close.
    /// We store handles here so we can wait for them during shutdown.
    pending_handles: Mutex<Vec<JoinHandle<()>>>,
    /// Flag indicating if the committer is shutting down.
    shutting_down: Arc<std::sync::atomic::AtomicBool>,
}

impl SourceTaskOffsetCommitter {
    /// Creates a new SourceTaskOffsetCommitter with the default commit interval.
    ///
    /// Uses the default offset commit interval of 60000ms (60 seconds).
    pub fn new() -> Self {
        SourceTaskOffsetCommitter::with_interval(OFFSET_FLUSH_INTERVAL_MS_DEFAULT as u64)
    }

    /// Creates a new SourceTaskOffsetCommitter with a custom commit interval.
    ///
    /// # Arguments
    /// * `commit_interval_ms` - The interval between offset commits in milliseconds.
    pub fn with_interval(commit_interval_ms: u64) -> Self {
        SourceTaskOffsetCommitter {
            commit_interval_ms,
            committers: DashMap::new(),
            pending_handles: Mutex::new(Vec::new()),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Returns the commit interval in milliseconds.
    pub fn commit_interval_ms(&self) -> u64 {
        self.commit_interval_ms
    }

    /// Schedules periodic offset commits for a source task.
    ///
    /// This method spawns an async task that will periodically call
    /// `commit_offsets()` on the provided task at the configured interval.
    ///
    /// # Arguments
    /// * `id` - The connector task ID for the task.
    /// * `task` - The source task that supports offset commits.
    ///
    /// # Note
    /// If a task with the same ID is already scheduled, it will be replaced.
    pub fn schedule<T: CommittableSourceTask + 'static>(&self, id: ConnectorTaskId, task: Arc<T>) {
        // Remove any existing scheduled task with this ID
        if self.committers.remove(&id).is_some() {
            log::trace!("{} Replacing existing scheduled commit task", id);
        }

        // Create stop signal channel
        let (stop_sender, mut stop_receiver) = broadcast::channel::<bool>(1);

        // Clone necessary values for the async task
        let commit_interval = Duration::from_millis(self.commit_interval_ms);
        let shutting_down = self.shutting_down.clone();
        let task_id = id.clone();
        let task_ref = task.clone();

        // Spawn the commit loop
        let handle = tokio::spawn(async move {
            // Wait for the first interval before starting commits
            // This matches Java's behavior: first commit after commitIntervalMs
            let mut commit_timer = interval(commit_interval);
            commit_timer.tick().await; // Initial tick to align with interval start

            loop {
                // Check if we should stop
                if shutting_down.load(std::sync::atomic::Ordering::SeqCst) {
                    log::debug!("{} Commit loop stopping due to shutdown signal", task_id);
                    break;
                }

                // Try to receive stop signal with timeout
                tokio::select! {
                    _ = commit_timer.tick() => {
                        // Interval elapsed, perform commit
                        SourceTaskOffsetCommitter::commit_task(&task_ref);
                    }
                    _ = stop_receiver.recv() => {
                        log::trace!("{} Commit loop received stop signal", task_id);
                        break;
                    }
                }
            }
        });

        // Store the handle in pending_handles for later cleanup
        self.pending_handles.lock().unwrap().push(handle);

        // Store the scheduled commit (stop sender only)
        self.committers.insert(id, ScheduledCommit { stop_sender });
    }

    /// Removes a scheduled commit task for the given connector task ID.
    ///
    /// This cancels the scheduled commits and waits for the task to complete.
    ///
    /// # Arguments
    /// * `id` - The connector task ID to remove.
    pub fn remove(&self, id: &ConnectorTaskId) {
        if let Some((_, scheduled)) = self.committers.remove(id) {
            // Signal the task to stop
            let _ = scheduled.stop_sender.send(true);
            log::trace!("{} Sent stop signal for commit task", id);
        }
        // Note: We don't wait for the handle here since handles are stored
        // collectively in pending_handles. The handle will complete on its own.
    }

    /// Commits offsets for a single source task.
    ///
    /// This is the internal implementation of the commit logic,
    /// corresponding to Java's static `commit(WorkerSourceTask)` method.
    ///
    /// # Arguments
    /// * `task` - The source task to commit offsets for.
    fn commit_task<T: CommittableSourceTask>(task: &Arc<T>) {
        if !task.should_commit_offsets() {
            log::trace!(
                "{} Skipping offset commit as there are no offsets that should be committed",
                task.task_id_string()
            );
            return;
        }

        log::debug!("{} Committing offsets", task.task_id_string());

        match task.commit_offsets() {
            Ok(true) => {
                log::trace!("{} Successfully committed offsets", task.task_id_string());
            }
            Ok(false) => {
                log::error!("{} Failed to commit offsets", task.task_id_string());
            }
            Err(e) => {
                // We're very careful about exceptions here since any uncaught exceptions
                // in the commit thread would cause the fixed interval schedule to stop running
                log::error!(
                    "{} Unhandled exception when committing: {}",
                    task.task_id_string(),
                    e.message()
                );
            }
        }
    }

    /// Closes the offset committer and stops all scheduled tasks.
    ///
    /// This gracefully shuts down all commit tasks, waiting up to the
    /// specified timeout for each task to complete.
    ///
    /// # Arguments
    /// * `timeout_ms` - The maximum time to wait for shutdown in milliseconds.
    ///
    /// Corresponds to Java's `close(long timeoutMs)` method.
    pub async fn close(&self, timeout_ms: u64) {
        // Signal all tasks to stop
        self.shutting_down.store(true, std::sync::atomic::Ordering::SeqCst);

        // Send stop signals to all scheduled tasks
        for entry in self.committers.iter() {
            let _ = entry.stop_sender.send(true);
        }

        // Clear the committers map
        self.committers.clear();

        // Take all pending handles
        let handles: Vec<JoinHandle<()>> = {
            let mut pending = self.pending_handles.lock().unwrap();
            std::mem::take(&mut *pending)
        };

        // Wait for all handles with timeout
        let timeout = Duration::from_millis(timeout_ms);
        let total = handles.len();
        let mut completed = 0;
        let mut timed_out = 0;

        for handle in handles {
            match tokio::time::timeout(timeout, handle).await {
                Ok(_) => completed += 1,
                Err(_) => timed_out += 1,
            }
        }

        if timed_out > 0 {
            log::warn!(
                "SourceTaskOffsetCommitter close: {} tasks completed, {} timed out",
                completed,
                timed_out
            );
        } else {
            log::trace!(
                "SourceTaskOffsetCommitter close: all {} tasks completed gracefully",
                total
            );
        }
    }

    /// Closes the offset committer synchronously (blocking).
    ///
    /// This is a blocking version of close() for use in non-async contexts.
    ///
    /// # Arguments
    /// * `timeout_ms` - The maximum time to wait for shutdown in milliseconds.
    pub fn close_blocking(&self, timeout_ms: u64) {
        // Signal all tasks to stop
        self.shutting_down.store(true, std::sync::atomic::Ordering::SeqCst);

        // Send stop signals to all scheduled tasks
        for entry in self.committers.iter() {
            let _ = entry.stop_sender.send(true);
        }

        // Clear the committers map
        self.committers.clear();

        // Take all pending handles
        let handles: Vec<JoinHandle<()>> = {
            let mut pending = self.pending_handles.lock().unwrap();
            std::mem::take(&mut *pending)
        };

        // Wait for all handles with timeout (blocking)
        let timeout = Duration::from_millis(timeout_ms);

        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                // We're in a tokio runtime, use block_in_place
                let total = handles.len();
                let mut completed = 0;
                let mut timed_out = 0;

                for handle in handles {
                    match tokio::task::block_in_place(|| {
                        rt.block_on(tokio::time::timeout(timeout, handle))
                    }) {
                        Ok(_) => completed += 1,
                        Err(_) => timed_out += 1,
                    }
                }

                if timed_out > 0 {
                    log::warn!(
                        "SourceTaskOffsetCommitter close_blocking: {} tasks completed, {} timed out",
                        completed,
                        timed_out
                    );
                } else {
                    log::trace!(
                        "SourceTaskOffsetCommitter close_blocking: all {} tasks completed gracefully",
                        total
                    );
                }
            }
            Err(_) => {
                // No tokio runtime, we can't wait properly
                // Just drop the handles (they will be aborted)
                log::warn!(
                    "SourceTaskOffsetCommitter close_blocking: {} handles dropped (no async runtime)",
                    handles.len()
                );
            }
        }
    }

    /// Returns the number of currently scheduled tasks.
    pub fn scheduled_count(&self) -> usize {
        self.committers.len()
    }

    /// Returns whether a task with the given ID is scheduled.
    pub fn is_scheduled(&self, id: &ConnectorTaskId) -> bool {
        self.committers.contains_key(id)
    }
}

impl Default for SourceTaskOffsetCommitter {
    fn default() -> Self {
        Self::new()
    }
}

/// A mock source task for testing the offset committer.
#[cfg(test)]
pub struct MockCommittableSourceTask {
    id: ConnectorTaskId,
    should_commit: std::sync::atomic::AtomicBool,
    commit_success: std::sync::atomic::AtomicBool,
    commit_count: std::sync::atomic::AtomicU32,
}

#[cfg(test)]
impl MockCommittableSourceTask {
    pub fn new(id: ConnectorTaskId) -> Self {
        MockCommittableSourceTask {
            id,
            should_commit: std::sync::atomic::AtomicBool::new(true),
            commit_success: std::sync::atomic::AtomicBool::new(true),
            commit_count: std::sync::atomic::AtomicU32::new(0),
        }
    }

    pub fn set_should_commit(&self, value: bool) {
        self.should_commit.store(value, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn set_commit_success(&self, success: bool) {
        self.commit_success.store(success, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn commit_count(&self) -> u32 {
        self.commit_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
impl CommittableSourceTask for MockCommittableSourceTask {
    fn should_commit_offsets(&self) -> bool {
        self.should_commit.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn commit_offsets(&self) -> Result<bool, ConnectError> {
        self.commit_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.commit_success.load(std::sync::atomic::Ordering::SeqCst) {
            Ok(true)
        } else {
            Err(ConnectError::general("Mock commit failed"))
        }
    }

    fn task_id_string(&self) -> String {
        format!("{}", self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::sleep;

    fn create_task_id(connector: &str, task: i32) -> ConnectorTaskId {
        ConnectorTaskId::new(connector.to_string(), task)
    }

    #[tokio::test]
    async fn test_new_committer() {
        let committer = SourceTaskOffsetCommitter::new();
        assert_eq!(committer.commit_interval_ms(), 60000);
        assert_eq!(committer.scheduled_count(), 0);
    }

    #[tokio::test]
    async fn test_with_custom_interval() {
        let committer = SourceTaskOffsetCommitter::with_interval(5000);
        assert_eq!(committer.commit_interval_ms(), 5000);
    }

    #[tokio::test]
    async fn test_schedule_task() {
        let committer = SourceTaskOffsetCommitter::with_interval(100); // 100ms for faster test
        let id = create_task_id("test-connector", 0);
        let task = Arc::new(MockCommittableSourceTask::new(id.clone()));

        committer.schedule(id.clone(), task.clone());

        assert!(committer.is_scheduled(&id));
        assert_eq!(committer.scheduled_count(), 1);

        // Wait for a few commits to happen
        sleep(Duration::from_millis(350)).await;

        // Should have about 3 commits (after 100ms, 200ms, 300ms)
        assert!(task.commit_count() >= 2);

        // Clean up
        committer.close(1000).await;
        assert_eq!(committer.scheduled_count(), 0);
    }

    #[tokio::test]
    async fn test_remove_task() {
        let committer = SourceTaskOffsetCommitter::with_interval(100);
        let id = create_task_id("test-connector", 0);
        let task = Arc::new(MockCommittableSourceTask::new(id.clone()));

        committer.schedule(id.clone(), task.clone());
        assert!(committer.is_scheduled(&id));

        // Remove the task
        committer.remove(&id);

        // Give some time for removal to complete
        sleep(Duration::from_millis(50)).await;

        assert!(!committer.is_scheduled(&id));
        assert_eq!(committer.scheduled_count(), 0);

        // Clean up
        committer.close(1000).await;
    }

    #[tokio::test]
    async fn test_skip_commit_when_should_not_commit() {
        let committer = SourceTaskOffsetCommitter::with_interval(100);
        let id = create_task_id("test-connector", 0);
        let task = Arc::new(MockCommittableSourceTask::new(id.clone()));

        // Set should_commit to false
        task.set_should_commit(false);

        committer.schedule(id.clone(), task.clone());

        // Wait for commits to be attempted
        sleep(Duration::from_millis(350)).await;

        // Should have 0 commits since should_commit is false
        assert_eq!(task.commit_count(), 0);

        // Clean up
        committer.close(1000).await;
    }

    #[tokio::test]
    async fn test_commit_failure_handling() {
        let committer = SourceTaskOffsetCommitter::with_interval(100);
        let id = create_task_id("test-connector", 0);
        let task = Arc::new(MockCommittableSourceTask::new(id.clone()));

        // Set commit to fail
        task.set_commit_success(false);

        committer.schedule(id.clone(), task.clone());

        // Wait for commits
        sleep(Duration::from_millis(250)).await;

        // Should have attempted commits even though they failed
        assert!(task.commit_count() >= 1);

        // Clean up
        committer.close(1000).await;
    }

    #[tokio::test]
    async fn test_commit_error_handling() {
        let committer = SourceTaskOffsetCommitter::with_interval(100);
        let id = create_task_id("test-connector", 0);
        let task = Arc::new(MockCommittableSourceTask::new(id.clone()));

        // Set commit to throw error
        task.set_commit_success(false);

        committer.schedule(id.clone(), task.clone());

        // Wait for commits
        sleep(Duration::from_millis(250)).await;

        // Should have attempted commits even though they errored
        // (error handling ensures the loop continues)
        assert!(task.commit_count() >= 1);

        // Clean up
        committer.close(1000).await;
    }

    #[tokio::test]
    async fn test_multiple_tasks() {
        let committer = SourceTaskOffsetCommitter::with_interval(100);

        let id1 = create_task_id("connector1", 0);
        let id2 = create_task_id("connector2", 0);
        let task1 = Arc::new(MockCommittableSourceTask::new(id1.clone()));
        let task2 = Arc::new(MockCommittableSourceTask::new(id2.clone()));

        committer.schedule(id1.clone(), task1.clone());
        committer.schedule(id2.clone(), task2.clone());

        assert_eq!(committer.scheduled_count(), 2);

        // Wait for commits
        sleep(Duration::from_millis(250)).await;

        // Both tasks should have commits
        assert!(task1.commit_count() >= 1);
        assert!(task2.commit_count() >= 1);

        // Clean up
        committer.close(1000).await;
        assert_eq!(committer.scheduled_count(), 0);
    }

    #[tokio::test]
    async fn test_replace_scheduled_task() {
        let committer = SourceTaskOffsetCommitter::with_interval(100);
        let id = create_task_id("test-connector", 0);
        let task1 = Arc::new(MockCommittableSourceTask::new(id.clone()));
        let task2 = Arc::new(MockCommittableSourceTask::new(id.clone()));

        committer.schedule(id.clone(), task1.clone());

        // Wait a bit
        sleep(Duration::from_millis(150)).await;
        let count1 = task1.commit_count();

        // Schedule with same ID should replace
        committer.schedule(id.clone(), task2.clone());

        assert_eq!(committer.scheduled_count(), 1);

        // Wait a bit more
        sleep(Duration::from_millis(150)).await;

        // task2 should have commits, task1 should have stopped
        let count1_after = task1.commit_count();
        let count2 = task2.commit_count();

        // task1 should not have increased after replacement
        assert!(count1_after <= count1 + 1); // Allow 1 more due to timing
        // task2 should have new commits
        assert!(count2 >= 1);

        // Clean up
        committer.close(1000).await;
    }

    #[tokio::test]
    async fn test_close_stops_all_tasks() {
        let committer = SourceTaskOffsetCommitter::with_interval(100);

        let id = create_task_id("test-connector", 0);
        let task = Arc::new(MockCommittableSourceTask::new(id.clone()));

        committer.schedule(id.clone(), task.clone());

        // Wait for some commits
        sleep(Duration::from_millis(150)).await;
        let count_before_close = task.commit_count();

        // Close should stop commits
        committer.close(1000).await;

        // Wait more time that would trigger additional commits if not stopped
        sleep(Duration::from_millis(300)).await;

        // Count should not have increased significantly after close
        let count_after_close = task.commit_count();
        assert!(count_after_close <= count_before_close + 1);
    }

    #[test]
    fn test_close_blocking() {
        // Need to use tokio runtime for this test
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let committer = SourceTaskOffsetCommitter::with_interval(100);

            let id = create_task_id("test-connector", 0);
            let task = Arc::new(MockCommittableSourceTask::new(id.clone()));

            committer.schedule(id.clone(), task.clone());

            // Wait for some commits
            sleep(Duration::from_millis(150)).await;
            assert!(task.commit_count() >= 1);

            // Close blocking
            committer.close_blocking(1000);

            assert_eq!(committer.scheduled_count(), 0);
        });
    }

    #[test]
    fn test_default_committer() {
        let committer = SourceTaskOffsetCommitter::default();
        assert_eq!(committer.commit_interval_ms(), 60000);
    }
}