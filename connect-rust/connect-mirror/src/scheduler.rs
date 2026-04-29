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

//! Scheduler for async task scheduling.
//! 
//! Equivalent to Java's `org.apache.kafka.connect.mirror.Scheduler`.
//! Uses tokio async runtime for task execution.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::task::{JoinHandle, AbortHandle};
use tokio::time::{interval, sleep, timeout};
use anyhow::{Result, anyhow};

/// Task identifier for scheduled tasks.
pub type TaskId = u64;

/// Type alias for async task function.
pub type AsyncTask = Box<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> + Send + Sync>;

/// Scheduler for executing async tasks with configurable timeouts.
/// 
/// Supports:
/// - One-off task execution with timeout
/// - Repeating tasks at fixed intervals
/// - Repeating tasks with initial delay
/// - Task cancellation via stop()
/// 
/// Equivalent to Java's `org.apache.kafka.connect.mirror.Scheduler`.
pub struct Scheduler {
    /// Name for logging/debugging purposes.
    name: String,
    /// Default timeout for task execution.
    timeout: Duration,
    /// Flag indicating if scheduler is stopped.
    stopped: Arc<AtomicBool>,
    /// Map of task handles for cancellation.
    task_handles: Arc<Mutex<HashMap<TaskId, AbortHandle>>>,
    /// Next task ID generator.
    next_task_id: Arc<Mutex<TaskId>>,
}

impl Scheduler {
    /// Creates a new Scheduler with given name and timeout.
    /// 
    /// # Arguments
    /// * `name` - Name for logging/debugging
    /// * `timeout` - Default timeout for task execution
    /// 
    /// # Example
    /// ```ignore
    /// let scheduler = Scheduler::new("mirror-source", Duration::from_secs(30));
    /// ```
    pub fn new(name: String, timeout: Duration) -> Self {
        Scheduler {
            name,
            timeout,
            stopped: Arc::new(AtomicBool::new(false)),
            task_handles: Arc::new(Mutex::new(HashMap::new())),
            next_task_id: Arc::new(Mutex::new(0)),
        }
    }

    /// Generates next task ID.
    fn next_id(&self) -> TaskId {
        let mut id = self.next_task_id.lock().unwrap();
        *id += 1;
        *id
    }

    /// Registers a task handle for later cancellation.
    fn register_handle(&self, id: TaskId, handle: AbortHandle) {
        let mut handles = self.task_handles.lock().unwrap();
        handles.insert(id, handle);
    }

    /// Removes a task handle after completion.
    fn unregister_handle(&self, id: TaskId) {
        let mut handles = self.task_handles.lock().unwrap();
        handles.remove(&id);
    }

    /// Executes a one-off task with timeout.
    /// 
    /// Equivalent to Java's `execute(Task task, String description)`.
    /// 
    /// # Arguments
    /// * `task` - Async task to execute
    /// * `description` - Description for logging
    /// 
    /// # Returns
    /// Result of task execution, or timeout error
    /// 
    /// # Example
    /// ```ignore
    /// scheduler.execute(
    ///     Box::new(|| Box::pin(async { 
    ///         println!("Task executed");
    ///         Ok(())
    ///     })),
    ///     "initial-setup"
    /// ).await?;
    /// ```
    pub async fn execute(&self, task: AsyncTask, description: &str) -> Result<()> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(anyhow!("Scheduler {} is stopped, cannot execute task: {}", self.name, description));
        }

        let task_name = format!("{}-{}", self.name, description);
        let timeout_duration = self.timeout;
        
        // Execute task with timeout
        let result = timeout(timeout_duration, task()).await;
        
        match result {
            Ok(inner_result) => {
                inner_result.map_err(|e| anyhow!("Task {} failed: {}", task_name, e))
            }
            Err(_) => {
                Err(anyhow!("Task {} timed out after {:?}", task_name, timeout_duration))
            }
        }
    }

    /// Executes a one-off task with custom timeout.
    /// 
    /// # Arguments
    /// * `task` - Async task to execute
    /// * `description` - Description for logging
    /// * `timeout_duration` - Custom timeout duration
    pub async fn execute_with_timeout(
        &self, 
        task: AsyncTask, 
        description: &str,
        timeout_duration: Duration
    ) -> Result<()> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(anyhow!("Scheduler {} is stopped, cannot execute task: {}", self.name, description));
        }

        let task_name = format!("{}-{}", self.name, description);
        
        let result = timeout(timeout_duration, task()).await;
        
        match result {
            Ok(inner_result) => {
                inner_result.map_err(|e| anyhow!("Task {} failed: {}", task_name, e))
            }
            Err(_) => {
                Err(anyhow!("Task {} timed out after {:?}", task_name, timeout_duration))
            }
        }
    }

    /// Schedules a task to run repeatedly at fixed interval, starting immediately.
    /// 
    /// Equivalent to Java's `scheduleRepeating(Task task, Duration interval, String description)`.
    /// 
    /// # Arguments
    /// * `task` - Async task to execute repeatedly
    /// * `interval_duration` - Interval between successive executions
    /// * `description` - Description for logging
    /// 
    /// # Returns
    /// Task ID that can be used to cancel the task
    /// 
    /// # Note
    /// The first execution starts immediately. If interval is zero or negative,
    /// no task is scheduled and 0 is returned.
    /// 
    /// # Example
    /// ```ignore
    /// let task_id = scheduler.schedule_repeating(
    ///     Box::new(|| Box::pin(async { 
    ///         println!("Repeating task");
    ///         Ok(())
    ///     })),
    ///     Duration::from_secs(60),
    ///     "sync-configs"
    /// );
    /// ```
    pub fn schedule_repeating(
        &self,
        task: AsyncTask,
        interval_duration: Duration,
        description: &str
    ) -> TaskId {
        if interval_duration.is_zero() {
            return 0;
        }

        self.schedule_internal(task, interval_duration, Duration::ZERO, description)
    }

    /// Schedules a task to run repeatedly at fixed interval with initial delay.
    /// 
    /// Equivalent to Java's `scheduleRepeatingDelayed(Task task, Duration interval, String description)`.
    /// 
    /// # Arguments
    /// * `task` - Async task to execute repeatedly
    /// * `interval_duration` - Interval between successive executions (also used as initial delay)
    /// * `description` - Description for logging
    /// 
    /// # Returns
    /// Task ID that can be used to cancel the task
    /// 
    /// # Note
    /// The first execution is delayed by `interval_duration`. If interval is zero or negative,
    /// no task is scheduled and 0 is returned.
    /// 
    /// # Example
    /// ```ignore
    /// let task_id = scheduler.schedule_repeating_delayed(
    ///     Box::new(|| Box::pin(async { 
    ///         println!("Delayed repeating task");
    ///         Ok(())
    ///     })),
    ///     Duration::from_secs(60),
    ///     "refresh-groups"
    /// );
    /// ```
    pub fn schedule_repeating_delayed(
        &self,
        task: AsyncTask,
        interval_duration: Duration,
        description: &str
    ) -> TaskId {
        if interval_duration.is_zero() {
            return 0;
        }

        self.schedule_internal(task, interval_duration, interval_duration, description)
    }

    /// Internal method to schedule repeating tasks.
    /// 
    /// # Arguments
    /// * `task` - Async task to execute
    /// * `interval_duration` - Interval between executions
    /// * `initial_delay` - Initial delay before first execution
    /// * `description` - Description for logging
    fn schedule_internal(
        &self,
        task: AsyncTask,
        interval_duration: Duration,
        initial_delay: Duration,
        description: &str
    ) -> TaskId {
        if self.stopped.load(Ordering::SeqCst) {
            return 0;
        }

        let task_id = self.next_id();
        let task_name = format!("{}-{}-{}", self.name, description, task_id);
        let stopped = self.stopped.clone();
        let task_arc = Arc::new(task);

        // Spawn async task
        let handle: JoinHandle<()> = tokio::spawn(async move {
            // Wait for initial delay if specified
            if !initial_delay.is_zero() {
                sleep(initial_delay).await;
            }

            // Create interval ticker
            let mut ticker = interval(interval_duration);
            
            // Skip the first immediate tick if we already delayed
            if !initial_delay.is_zero() {
                ticker.tick().await;
            }

            loop {
                // Check if scheduler is stopped
                if stopped.load(Ordering::SeqCst) {
                    break;
                }

                // Wait for next tick
                ticker.tick().await;

                // Check again after tick (scheduler might have been stopped during wait)
                if stopped.load(Ordering::SeqCst) {
                    break;
                }

                // Execute task
                let task_clone = task_arc.clone();
                let result = task_clone().await;
                
                if let Err(e) = result {
                    // Log error but continue running (matches Kafka behavior)
                    eprintln!("Scheduled task {} error: {}", task_name, e);
                }
            }
        });

        // Register handle for cancellation
        self.register_handle(task_id, handle.abort_handle());

        // Spawn cleanup task to unregister when task completes
        let handles = self.task_handles.clone();
        let id = task_id;
        tokio::spawn(async move {
            handle.await.ok();
            handles.lock().unwrap().remove(&id);
        });

        task_id
    }

    /// Stops the scheduler and cancels all scheduled tasks.
    /// 
    /// Equivalent to closing the scheduler in Java.
    /// 
    /// After calling stop(), no new tasks can be scheduled and all
    /// existing scheduled tasks are cancelled.
    pub fn stop(&self) {
        // Mark scheduler as stopped
        self.stopped.store(true, Ordering::SeqCst);

        // Abort all scheduled tasks
        let handles = self.task_handles.lock().unwrap();
        for (_, handle) in handles.iter() {
            handle.abort();
        }
    }

    /// Stops the scheduler and waits for all tasks to complete.
    /// 
    /// # Arguments
    /// * `wait_timeout` - Maximum time to wait for tasks to complete
    pub async fn stop_and_wait(&self, wait_timeout: Duration) -> Result<()> {
        self.stop();

        // Wait a short period for tasks to be aborted
        sleep(wait_timeout).await;

        Ok(())
    }

    /// Checks if the scheduler is stopped.
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    /// Gets the scheduler name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Gets the default timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Cancels a specific scheduled task by ID.
    /// 
    /// # Arguments
    /// * `task_id` - ID of the task to cancel
    /// 
    /// # Returns
    /// true if task was found and cancelled, false otherwise
    pub fn cancel_task(&self, task_id: TaskId) -> bool {
        let mut handles = self.task_handles.lock().unwrap();
        if let Some(handle) = handles.remove(&task_id) {
            handle.abort();
            true
        } else {
            false
        }
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Builder for creating Scheduler instances.
pub struct SchedulerBuilder {
    name: String,
    timeout: Duration,
}

impl SchedulerBuilder {
    /// Creates a new builder.
    pub fn new(name: String) -> Self {
        SchedulerBuilder {
            name,
            timeout: Duration::from_secs(30), // Default timeout
        }
    }

    /// Sets the timeout duration.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Builds the Scheduler.
    pub fn build(self) -> Scheduler {
        Scheduler::new(self.name, self.timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;

    #[test]
    async fn test_scheduler_new() {
        let scheduler = Scheduler::new("test".to_string(), Duration::from_secs(10));
        assert_eq!(scheduler.name(), "test");
        assert_eq!(scheduler.timeout(), Duration::from_secs(10));
        assert!(!scheduler.is_stopped());
    }

    #[test]
    async fn test_scheduler_builder() {
        let scheduler = SchedulerBuilder::new("test-builder".to_string())
            .timeout(Duration::from_secs(60))
            .build();
        assert_eq!(scheduler.name(), "test-builder");
        assert_eq!(scheduler.timeout(), Duration::from_secs(60));
    }

    #[test]
    async fn test_execute_success() {
        let scheduler = Scheduler::new("test".to_string(), Duration::from_secs(10));
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();
        
        let result = scheduler.execute(
            Box::new(move || {
                let c = counter_clone.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
            "increment-counter"
        ).await;
        
        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    async fn test_execute_timeout() {
        let scheduler = Scheduler::new("test".to_string(), Duration::from_millis(50));
        
        let result = scheduler.execute(
            Box::new(|| Box::pin(async {
                sleep(Duration::from_secs(10)).await;
                Ok(())
            })),
            "slow-task"
        ).await;
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("timed out"));
    }

    #[test]
    async fn test_execute_when_stopped() {
        let scheduler = Scheduler::new("test".to_string(), Duration::from_secs(10));
        scheduler.stop();
        
        let result = scheduler.execute(
            Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
            "after-stop"
        ).await;
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("stopped"));
    }

    #[test]
    async fn test_schedule_repeating() {
        let scheduler = Scheduler::new("test".to_string(), Duration::from_secs(10));
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();
        
        let task_id = scheduler.schedule_repeating(
            Box::new(move || {
                let c = counter_clone.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
            Duration::from_millis(50),
            "counter-task"
        );
        
        assert!(task_id > 0);
        
        // Wait for a few executions
        sleep(Duration::from_millis(200)).await;
        
        let count = counter.load(Ordering::SeqCst);
        assert!(count >= 3, "Expected at least 3 executions, got {}", count);
        
        scheduler.stop();
    }

    #[test]
    async fn test_schedule_repeating_delayed() {
        let scheduler = Scheduler::new("test".to_string(), Duration::from_secs(10));
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();
        
        let task_id = scheduler.schedule_repeating_delayed(
            Box::new(move || {
                let c = counter_clone.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
            Duration::from_millis(100),
            "delayed-counter"
        );
        
        assert!(task_id > 0);
        
        // Wait a bit less than initial delay - counter should still be 0
        sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        
        // Wait for initial delay and a few executions
        sleep(Duration::from_millis(300)).await;
        
        let count = counter.load(Ordering::SeqCst);
        assert!(count >= 2, "Expected at least 2 executions after delay, got {}", count);
        
        scheduler.stop();
    }

    #[test]
    async fn test_stop_cancels_tasks() {
        let scheduler = Scheduler::new("test".to_string(), Duration::from_secs(10));
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();
        
        let _task_id = scheduler.schedule_repeating(
            Box::new(move || {
                let c = counter_clone.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(10)).await;
                    Ok(())
                })
            }),
            Duration::from_millis(50),
            "counter-task"
        );
        
        // Let it run a bit
        sleep(Duration::from_millis(100)).await;
        let count_before_stop = counter.load(Ordering::SeqCst);
        
        // Stop scheduler
        scheduler.stop();
        
        // Wait and verify counter doesn't increase further
        sleep(Duration::from_millis(150)).await;
        let count_after_stop = counter.load(Ordering::SeqCst);
        
        // Counter should not have increased significantly after stop
        assert!(count_after_stop <= count_before_stop + 1);
    }

    #[test]
    async fn test_cancel_specific_task() {
        let scheduler = Scheduler::new("test".to_string(), Duration::from_secs(10));
        let counter1 = Arc::new(AtomicU32::new(0));
        let counter2 = Arc::new(AtomicU32::new(0));
        
        let c1 = counter1.clone();
        let task_id1 = scheduler.schedule_repeating(
            Box::new(move || {
                let c = c1.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
            Duration::from_millis(50),
            "task1"
        );
        
        let c2 = counter2.clone();
        let task_id2 = scheduler.schedule_repeating(
            Box::new(move || {
                let c = c2.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
            Duration::from_millis(50),
            "task2"
        );
        
        // Let both tasks run
        sleep(Duration::from_millis(100)).await;
        
        // Cancel task1 only
        assert!(scheduler.cancel_task(task_id1));
        
        // Wait for more time
        sleep(Duration::from_millis(150)).await;
        
        // counter2 should continue incrementing, counter1 should be stopped
        let count1 = counter1.load(Ordering::SeqCst);
        let count2 = counter2.load(Ordering::SeqCst);
        
        assert!(count2 > count1, "Task2 should have more executions than cancelled Task1");
        
        scheduler.stop();
    }

    #[test]
    async fn test_zero_interval_no_task() {
        let scheduler = Scheduler::new("test".to_string(), Duration::from_secs(10));
        
        let task_id = scheduler.schedule_repeating(
            Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
            Duration::ZERO,
            "zero-interval"
        );
        
        assert_eq!(task_id, 0);
    }
}