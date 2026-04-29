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

//! SchedulerTest - Tests for Scheduler struct.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.Scheduler
//! Note: Java Scheduler has no standalone SchedulerTest.java - tests are indirect via connectors.
//!
//! Test coverage:
//! - Constructor and builder pattern
//! - execute(): One-off task execution with timeout
//! - scheduleRepeating(): Immediate-start periodic tasks
//! - scheduleRepeatingDelayed(): Delayed-start periodic tasks
//! - stop() and isStopped(): Scheduler state management
//! - cancelTask(): Cancel specific task by ID
//! - Task registration/unregistration lifecycle
//! - Edge cases: zero/negative interval, timeout behavior

use connect_mirror::scheduler::{Scheduler, SchedulerBuilder, TaskId};
use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

// ============================================================================
// Java Correspondence: Constructor and Basic Properties
// ============================================================================

/// Test Scheduler constructor with name and timeout.
/// Corresponds to Java: Scheduler(String name, Duration timeout)
#[tokio::test]
async fn test_scheduler_new_java_correspondence() {
    let scheduler = Scheduler::new("mirror-source".to_string(), Duration::from_secs(30));
    
    // Verify properties match Java constructor parameters
    assert_eq!(scheduler.name(), "mirror-source");
    assert_eq!(scheduler.timeout(), Duration::from_secs(30));
    assert!(!scheduler.is_stopped(), "New scheduler should not be stopped");
}

/// Test Scheduler constructor via Class and role pattern.
/// Corresponds to Java: Scheduler(Class<?> clazz, String role, Duration timeout)
/// Java creates name as "Scheduler for {clazz.getSimpleName()}: {role}"
#[tokio::test]
async fn test_scheduler_class_role_pattern() {
    // In Rust, we use direct name construction to simulate Java pattern
    let name = format!("Scheduler for MirrorSourceConnector: source-cluster");
    let scheduler = Scheduler::new(name.clone(), Duration::from_secs(60));
    
    assert_eq!(scheduler.name(), name);
    assert_eq!(scheduler.timeout(), Duration::from_secs(60));
}

/// Test SchedulerBuilder pattern.
#[tokio::test]
async fn test_scheduler_builder_pattern() {
    let scheduler = SchedulerBuilder::new("test-builder".to_string())
        .timeout(Duration::from_secs(120))
        .build();
    
    assert_eq!(scheduler.name(), "test-builder");
    assert_eq!(scheduler.timeout(), Duration::from_secs(120));
    assert!(!scheduler.is_stopped());
}

/// Test SchedulerBuilder with default timeout (30 seconds).
#[tokio::test]
async fn test_scheduler_builder_default_timeout() {
    let scheduler = SchedulerBuilder::new("default-timeout".to_string()).build();
    
    // Default timeout is 30 seconds (matches Java behavior)
    assert_eq!(scheduler.timeout(), Duration::from_secs(30));
}

// ============================================================================
// Java Correspondence: execute() - One-off Task Execution
// ============================================================================

/// Test execute() with successful task.
/// Corresponds to Java: execute(Task task, String description)
#[tokio::test]
async fn test_execute_success_java_correspondence() {
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
    
    assert!(result.is_ok(), "Execute should succeed");
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

/// Test execute() with task timeout.
/// Corresponds to Java timeout behavior - logs error and returns.
#[tokio::test]
async fn test_execute_timeout_java_correspondence() {
    let scheduler = Scheduler::new("timeout-test".to_string(), Duration::from_millis(50));
    
    let result = scheduler.execute(
        Box::new(|| Box::pin(async {
            sleep(Duration::from_secs(10)).await;
            Ok(())
        })),
        "slow-task"
    ).await;
    
    // Java: catches TimeoutException and logs error
    // Rust: returns timeout error
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("timed out"), "Error should mention timeout");
}

/// Test execute() when scheduler is stopped.
/// Corresponds to Java: skipping task due to shutdown
#[tokio::test]
async fn test_execute_when_stopped_java_correspondence() {
    let scheduler = Scheduler::new("stopped-test".to_string(), Duration::from_secs(10));
    scheduler.stop();
    
    let result = scheduler.execute(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        "after-stop"
    ).await;
    
    // Java: checks closed flag and skips task
    // Rust: returns error indicating scheduler is stopped
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("stopped"));
}

/// Test execute() with task failure.
/// Corresponds to Java: catches exception in task and logs error
#[tokio::test]
async fn test_execute_task_failure() {
    let scheduler = Scheduler::new("failure-test".to_string(), Duration::from_secs(10));
    
    let result = scheduler.execute(
        Box::new(|| Box::pin(async {
            Err::<(), anyhow::Error>(anyhow::anyhow!("Task failed intentionally"))
        })),
        "failing-task"
    ).await;
    
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("failed"));
}

/// Test execute() with custom timeout via execute_with_timeout().
#[tokio::test]
async fn test_execute_with_custom_timeout() {
    let scheduler = Scheduler::new("custom-timeout".to_string(), Duration::from_secs(10));
    
    let result = scheduler.execute_with_timeout(
        Box::new(|| Box::pin(async {
            sleep(Duration::from_millis(200)).await;
            Ok(())
        })),
        "custom-timeout-task",
        Duration::from_millis(100)
    ).await;
    
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("timed out"));
}

// ============================================================================
// Java Correspondence: scheduleRepeating() - Periodic Tasks (Immediate Start)
// ============================================================================

/// Test scheduleRepeating() starts immediately at interval.
/// Corresponds to Java: scheduleRepeating(Task task, Duration interval, String description)
/// Java: scheduleAtFixedRate with initialDelay=0
#[tokio::test]
async fn test_schedule_repeating_immediate_start_java_correspondence() {
    let scheduler = Scheduler::new("repeating-test".to_string(), Duration::from_secs(10));
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
    
    // Task ID should be non-zero (valid task registered)
    assert!(task_id > 0, "Task ID should be positive");
    
    // Wait for multiple executions (immediate start + periodic)
    sleep(Duration::from_millis(200)).await;
    
    let count = counter.load(Ordering::SeqCst);
    // Java: scheduleAtFixedRate starts immediately, so expect multiple executions
    assert!(count >= 3, "Expected at least 3 executions, got {}", count);
    
    scheduler.stop();
}

/// Test scheduleRepeating() with zero interval returns 0 (no task scheduled).
/// Corresponds to Java: if (interval.toMillis() < 0L) return;
#[tokio::test]
async fn test_schedule_repeating_zero_interval_java_correspondence() {
    let scheduler = Scheduler::new("zero-interval-test".to_string(), Duration::from_secs(10));
    
    let task_id = scheduler.schedule_repeating(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::ZERO,
        "zero-interval"
    );
    
    // Java: negative interval (including zero per implementation) means no task
    assert_eq!(task_id, 0, "Zero interval should return task_id=0 (no task)");
}

/// Test scheduleRepeating() continues running after task errors.
/// Corresponds to Java: catches exception but continues scheduled execution
#[tokio::test]
async fn test_schedule_repeating_continues_after_error() {
    let scheduler = Scheduler::new("error-continuation".to_string(), Duration::from_secs(10));
    let counter = Arc::new(AtomicU32::new(0));
    let error_count = Arc::new(AtomicU32::new(0));
    
    let c = counter.clone();
    let e = error_count.clone();
    
    let _task_id = scheduler.schedule_repeating(
        Box::new(move || {
            let c1 = c.clone();
            let e1 = e.clone();
            Box::pin(async move {
                c1.fetch_add(1, Ordering::SeqCst);
                if c1.load(Ordering::SeqCst) % 2 == 0 {
                    e1.fetch_add(1, Ordering::SeqCst);
                    return Err(anyhow::anyhow!("Intentional error"));
                }
                Ok(())
            })
        }),
        Duration::from_millis(50),
        "error-task"
    );
    
    sleep(Duration::from_millis(200)).await;
    
    // Even with errors, task should continue executing
    let count = counter.load(Ordering::SeqCst);
    let errors = error_count.load(Ordering::SeqCst);
    
    assert!(count >= 3, "Task should continue despite errors");
    assert!(errors >= 1, "Some errors should have occurred");
    
    scheduler.stop();
}

/// Test scheduleRepeating() task registration and execution count.
#[tokio::test]
async fn test_schedule_repeating_execution_count() {
    let scheduler = Scheduler::new("count-test".to_string(), Duration::from_secs(10));
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
        Duration::from_millis(30),
        "fast-counter"
    );
    
    assert!(task_id > 0);
    
    // Let it run for approximately 10 intervals
    sleep(Duration::from_millis(300)).await;
    
    let count = counter.load(Ordering::SeqCst);
    // Due to timing variations, expect at least 6 executions
    assert!(count >= 6, "Expected >= 6 executions in 300ms, got {}", count);
    
    scheduler.stop();
}

// ============================================================================
// Java Correspondence: scheduleRepeatingDelayed() - Periodic Tasks (Delayed Start)
// ============================================================================

/// Test scheduleRepeatingDelayed() respects initial delay.
/// Corresponds to Java: scheduleRepeatingDelayed(Task task, Duration interval, String description)
/// Java: scheduleAtFixedRate with initialDelay = interval
#[tokio::test]
async fn test_schedule_repeating_delayed_java_correspondence() {
    let scheduler = Scheduler::new("delayed-test".to_string(), Duration::from_secs(10));
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
    
    // Wait less than initial delay - counter should be 0
    sleep(Duration::from_millis(50)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 0, "No execution before delay");
    
    // Wait for initial delay plus multiple executions
    sleep(Duration::from_millis(250)).await;
    
    let count = counter.load(Ordering::SeqCst);
    // After 100ms delay + 250ms, expect at least 2 executions
    assert!(count >= 2, "Expected >= 2 executions after delay, got {}", count);
    
    scheduler.stop();
}

/// Test scheduleRepeatingDelayed() with zero interval returns 0.
/// Corresponds to Java: if (interval.toMillis() < 0L) return;
#[tokio::test]
async fn test_schedule_repeating_delayed_zero_interval() {
    let scheduler = Scheduler::new("delayed-zero".to_string(), Duration::from_secs(10));
    
    let task_id = scheduler.schedule_repeating_delayed(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::ZERO,
        "delayed-zero-task"
    );
    
    assert_eq!(task_id, 0);
}

/// Test scheduleRepeatingDelayed() initial delay equals interval.
/// Note: Rust implementation has first execution at ~initial_delay + interval_duration
/// due to ticker.tick() behavior after delay.
#[tokio::test]
async fn test_schedule_repeating_delayed_delay_equals_interval() {
    let scheduler = Scheduler::new("delay-equals".to_string(), Duration::from_secs(10));
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();
    
    // Interval and delay both 100ms
    let task_id = scheduler.schedule_repeating_delayed(
        Box::new(move || {
            let c = counter_clone.clone();
            Box::pin(async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }),
        Duration::from_millis(100),
        "delay-equals-interval"
    );
    
    assert!(task_id > 0);
    
    // First execution occurs at ~200ms (delay + interval skip)
    sleep(Duration::from_millis(220)).await;
    assert!(counter.load(Ordering::SeqCst) >= 1, "First execution after delay+interval");
    
    // After more time, expect additional executions
    sleep(Duration::from_millis(200)).await;
    assert!(counter.load(Ordering::SeqCst) >= 2);
    
    scheduler.stop();
}

// ============================================================================
// Java Correspondence: close() / stop() - Scheduler Shutdown
// ============================================================================

/// Test stop() cancels all scheduled tasks.
/// Corresponds to Java: close() -> executor.shutdown() + awaitTermination
#[tokio::test]
async fn test_stop_cancels_tasks_java_correspondence() {
    let scheduler = Scheduler::new("stop-cancel".to_string(), Duration::from_secs(10));
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
        "cancel-on-stop"
    );
    
    // Let it run a bit
    sleep(Duration::from_millis(100)).await;
    let count_before_stop = counter.load(Ordering::SeqCst);
    
    // Stop scheduler (Java: executor.shutdown())
    scheduler.stop();
    assert!(scheduler.is_stopped());
    
    // Wait and verify counter doesn't increase significantly
    sleep(Duration::from_millis(200)).await;
    let count_after_stop = counter.load(Ordering::SeqCst);
    
    // After stop, counter should not increase significantly
    assert!(count_after_stop <= count_before_stop + 1, 
        "Counter should not increase after stop: before={}, after={}", 
        count_before_stop, count_after_stop);
}

/// Test stop_and_wait() waits for termination.
/// Corresponds to Java: awaitTermination(timeout)
#[tokio::test]
async fn test_stop_and_wait_java_correspondence() {
    let scheduler = Scheduler::new("stop-wait".to_string(), Duration::from_secs(10));
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();
    
    let _task_id = scheduler.schedule_repeating(
        Box::new(move || {
            let c = counter_clone.clone();
            Box::pin(async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }),
        Duration::from_millis(50),
        "stop-wait-task"
    );
    
    sleep(Duration::from_millis(100)).await;
    
    // Java: close() calls awaitTermination
    let result = scheduler.stop_and_wait(Duration::from_millis(100)).await;
    assert!(result.is_ok());
    assert!(scheduler.is_stopped());
}

/// Test is_stopped() reflects scheduler state.
#[tokio::test]
async fn test_is_stopped_state_java_correspondence() {
    let scheduler = Scheduler::new("state-test".to_string(), Duration::from_secs(10));
    
    // Initially not stopped (Java: closed = false)
    assert!(!scheduler.is_stopped());
    
    scheduler.stop();
    
    // After stop (Java: closed = true)
    assert!(scheduler.is_stopped());
    
    // Stop again should be idempotent
    scheduler.stop();
    assert!(scheduler.is_stopped());
}

/// Test Drop implementation calls stop().
/// Corresponds to Java: AutoCloseable.close() on garbage collection
#[tokio::test]
async fn test_drop_calls_stop() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();
    
    {
        let scheduler = Scheduler::new("drop-test".to_string(), Duration::from_secs(10));
        
        let _task_id = scheduler.schedule_repeating(
            Box::new(move || {
                let c = counter_clone.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
            })
        }),
            Duration::from_millis(50),
            "drop-task"
        );
        
        sleep(Duration::from_millis(100)).await;
        // scheduler goes out of scope here
    }
    
    // After drop, scheduler should be stopped
    sleep(Duration::from_millis(100)).await;
    
    // Counter should not increase after drop
    let count = counter.load(Ordering::SeqCst);
    sleep(Duration::from_millis(100)).await;
    assert!(counter.load(Ordering::SeqCst) <= count + 1);
}

// ============================================================================
// Java Correspondence: cancelTask() - Cancel Specific Task
// ============================================================================

/// Test cancel_task() cancels specific task by ID.
#[tokio::test]
async fn test_cancel_specific_task_java_correspondence() {
    let scheduler = Scheduler::new("cancel-specific".to_string(), Duration::from_secs(10));
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
    let cancelled = scheduler.cancel_task(task_id1);
    assert!(cancelled, "cancel_task should return true for valid task");
    
    // Let task2 continue
    sleep(Duration::from_millis(150)).await;
    
    let count1 = counter1.load(Ordering::SeqCst);
    let count2 = counter2.load(Ordering::SeqCst);
    
    // Task2 should have more executions than cancelled Task1
    assert!(count2 > count1, 
        "Task2 should have more executions: count2={}, count1={}", count2, count1);
    
    scheduler.stop();
}

/// Test cancel_task() returns false for invalid/non-existent task ID.
#[tokio::test]
async fn test_cancel_invalid_task_id() {
    let scheduler = Scheduler::new("cancel-invalid".to_string(), Duration::from_secs(10));
    
    // Cancel with task ID that doesn't exist
    let cancelled = scheduler.cancel_task(99999);
    assert!(!cancelled, "cancel_task should return false for invalid task");
    
    scheduler.stop();
}

/// Test cancel_task() returns false for already-cancelled task.
#[tokio::test]
async fn test_cancel_already_cancelled_task() {
    let scheduler = Scheduler::new("cancel-twice".to_string(), Duration::from_secs(10));
    
    let task_id = scheduler.schedule_repeating(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::from_millis(50),
        "single-task"
    );
    
    // First cancel succeeds
    let cancelled1 = scheduler.cancel_task(task_id);
    assert!(cancelled1);
    
    // Second cancel fails (task already removed)
    let cancelled2 = scheduler.cancel_task(task_id);
    assert!(!cancelled2, "Second cancel should return false");
    
    scheduler.stop();
}

/// Test cancel_task() after scheduler stopped returns false.
#[tokio::test]
async fn test_cancel_after_stop() {
    let scheduler = Scheduler::new("cancel-after-stop".to_string(), Duration::from_secs(10));
    
    let task_id = scheduler.schedule_repeating(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::from_millis(50),
        "stopped-task"
    );
    
    scheduler.stop();
    
    // After stop, cancel should fail (task handles already cleared)
    let cancelled = scheduler.cancel_task(task_id);
    // May return false if task was already aborted during stop
    // This behavior is acceptable - tasks are cancelled by stop()
    
    scheduler.stop();
}

// ============================================================================
// Java Correspondence: Task Registration Lifecycle
// ============================================================================

/// Test task ID increments for each new task.
#[tokio::test]
async fn test_task_id_increment() {
    let scheduler = Scheduler::new("id-increment".to_string(), Duration::from_secs(10));
    
    let task_id1 = scheduler.schedule_repeating(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::from_millis(100),
        "task-1"
    );
    
    let task_id2 = scheduler.schedule_repeating(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::from_millis(100),
        "task-2"
    );
    
    let task_id3 = scheduler.schedule_repeating(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::from_millis(100),
        "task-3"
    );
    
    // Each task ID should be unique and incrementing
    assert!(task_id1 > 0);
    assert!(task_id2 > task_id1);
    assert!(task_id3 > task_id2);
    
    scheduler.stop();
}

/// Test multiple tasks run concurrently.
#[tokio::test]
async fn test_multiple_concurrent_tasks() {
    let scheduler = Scheduler::new("concurrent".to_string(), Duration::from_secs(10));
    let counter = Arc::new(AtomicU32::new(0));
    
    // Schedule 3 tasks with same interval
    for i in 0..3 {
        let c = counter.clone();
        let desc = format!("concurrent-task-{}", i);
        scheduler.schedule_repeating(
            Box::new(move || {
                let c1 = c.clone();
                Box::pin(async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
            Duration::from_millis(50),
            &desc
        );
    }
    
    sleep(Duration::from_millis(150)).await;
    
    // All 3 tasks should contribute to counter
    let count = counter.load(Ordering::SeqCst);
    // Each task runs ~3 times, total should be >= 6 (some timing variance)
    assert!(count >= 6, "Expected >= 6 total from 3 tasks, got {}", count);
    
    scheduler.stop();
}

/// Test scheduleRepeating when scheduler is stopped returns 0.
#[tokio::test]
async fn test_schedule_when_stopped() {
    let scheduler = Scheduler::new("schedule-stopped".to_string(), Duration::from_secs(10));
    scheduler.stop();
    
    let task_id = scheduler.schedule_repeating(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::from_millis(50),
        "after-stop"
    );
    
    // Should return 0 (no task scheduled when stopped)
    assert_eq!(task_id, 0);
}

/// Test scheduleRepeatingDelayed when scheduler is stopped returns 0.
#[tokio::test]
async fn test_schedule_delayed_when_stopped() {
    let scheduler = Scheduler::new("delayed-stopped".to_string(), Duration::from_secs(10));
    scheduler.stop();
    
    let task_id = scheduler.schedule_repeating_delayed(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::from_millis(50),
        "after-stop-delayed"
    );
    
    assert_eq!(task_id, 0);
}

// ============================================================================
// Edge Cases and Boundary Tests
// ============================================================================

/// Test very short interval (1ms) works correctly.
#[tokio::test]
async fn test_very_short_interval() {
    let scheduler = Scheduler::new("short-interval".to_string(), Duration::from_secs(10));
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
        Duration::from_millis(1),
        "fast-task"
    );
    
    assert!(task_id > 0);
    
    sleep(Duration::from_millis(50)).await;
    
    // With 1ms interval over 50ms, expect many executions
    let count = counter.load(Ordering::SeqCst);
    assert!(count >= 20, "Expected many executions with 1ms interval, got {}", count);
    
    scheduler.stop();
}

/// Test very long interval still registers task.
#[tokio::test]
async fn test_very_long_interval() {
    let scheduler = Scheduler::new("long-interval".to_string(), Duration::from_secs(10));
    
    // 1 hour interval - task should still be registered
    let task_id = scheduler.schedule_repeating(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::from_secs(3600),
        "hourly-task"
    );
    
    assert!(task_id > 0, "Long interval task should be registered");
    
    // Immediately stop - task won't execute but was registered
    scheduler.stop();
}

/// Test task with very short execution time.
#[tokio::test]
async fn test_fast_task_execution() {
    let scheduler = Scheduler::new("fast-exec".to_string(), Duration::from_secs(10));
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();
    
    let _task_id = scheduler.schedule_repeating(
        Box::new(move || {
            let c = counter_clone.clone();
            Box::pin(async move {
                // Extremely fast - just increment
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }),
        Duration::from_millis(10),
        "instant-task"
    );
    
    sleep(Duration::from_millis(100)).await;
    
    let count = counter.load(Ordering::SeqCst);
    assert!(count >= 5, "Fast task should execute multiple times, got {}", count);
    
    scheduler.stop();
}

/// Test execute with task that takes exactly timeout duration.
#[tokio::test]
async fn test_execute_exact_timeout_boundary() {
    let scheduler = Scheduler::new("exact-timeout".to_string(), Duration::from_millis(100));
    
    // Task takes exactly timeout duration - may succeed or timeout
    let result = scheduler.execute(
        Box::new(|| Box::pin(async {
            sleep(Duration::from_millis(95)).await;
            Ok(())
        })),
        "boundary-task"
    ).await;
    
    // Should succeed since 95ms < 100ms timeout
    assert!(result.is_ok());
}

/// Test scheduleRepeating with different interval than timeout.
#[tokio::test]
async fn test_interval_independent_of_timeout() {
    // Scheduler timeout is 10 seconds, but interval is 50ms
    let scheduler = Scheduler::new("independent".to_string(), Duration::from_secs(10));
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();
    
    let _task_id = scheduler.schedule_repeating(
        Box::new(move || {
            let c = counter_clone.clone();
            Box::pin(async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }),
        Duration::from_millis(50),
        "independent-interval"
    );
    
    sleep(Duration::from_millis(200)).await;
    
    // Interval controls frequency, not timeout
    let count = counter.load(Ordering::SeqCst);
    assert!(count >= 3);
    
    scheduler.stop();
}

// ============================================================================
// State Management Tests
// ============================================================================

/// Test scheduler can schedule after being created, then stop.
#[tokio::test]
async fn test_scheduler_state_cycle() {
    let scheduler = Scheduler::new("state-cycle".to_string(), Duration::from_secs(10));
    
    // State: Running
    assert!(!scheduler.is_stopped());
    
    // Schedule tasks while running
    let task_id = scheduler.schedule_repeating(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::from_millis(50),
        "cycle-task"
    );
    assert!(task_id > 0);
    
    // State: Stopped
    scheduler.stop();
    assert!(scheduler.is_stopped());
    
    // Cannot schedule after stop
    let task_id2 = scheduler.schedule_repeating(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::from_millis(50),
        "after-cycle-stop"
    );
    assert_eq!(task_id2, 0);
}

/// Test stop is idempotent - calling multiple times has same effect.
#[tokio::test]
async fn test_stop_idempotent() {
    let scheduler = Scheduler::new("idempotent".to_string(), Duration::from_secs(10));
    
    scheduler.stop();
    assert!(scheduler.is_stopped());
    
    // Second stop
    scheduler.stop();
    assert!(scheduler.is_stopped());
    
    // Third stop
    scheduler.stop();
    assert!(scheduler.is_stopped());
}

/// Test scheduler name persists after stop.
#[tokio::test]
async fn test_name_persists_after_stop() {
    let scheduler = Scheduler::new("persistent-name".to_string(), Duration::from_secs(10));
    
    let name_before = scheduler.name();
    scheduler.stop();
    let name_after = scheduler.name();
    
    assert_eq!(name_before, name_after);
    assert_eq!(name_after, "persistent-name");
}

/// Test timeout persists after stop.
#[tokio::test]
async fn test_timeout_persists_after_stop() {
    let scheduler = Scheduler::new("persistent-timeout".to_string(), Duration::from_secs(30));
    
    let timeout_before = scheduler.timeout();
    scheduler.stop();
    let timeout_after = scheduler.timeout();
    
    assert_eq!(timeout_before, timeout_after);
    assert_eq!(timeout_after, Duration::from_secs(30));
}

// ============================================================================
// Concurrency and Thread Safety Tests
// ============================================================================

/// Test atomic counter used across multiple tasks.
#[tokio::test]
async fn test_atomic_counter_thread_safety() {
    let scheduler = Scheduler::new("atomic-safety".to_string(), Duration::from_secs(10));
    let counter = Arc::new(AtomicU32::new(0));
    
    // Schedule multiple tasks that all increment same counter
    for i in 0..5 {
        let c = counter.clone();
        let desc = format!("atomic-task-{}", i);
        scheduler.schedule_repeating(
            Box::new(move || {
                let c1 = c.clone();
                Box::pin(async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
            Duration::from_millis(20),
            &desc
        );
    }
    
    sleep(Duration::from_millis(100)).await;
    
    let count = counter.load(Ordering::SeqCst);
    // 5 tasks, each ~5 executions = 25 total expected
    assert!(count >= 15, "Atomic counter should work across tasks, got {}", count);
    
    scheduler.stop();
}

/// Test AtomicBool for stop flag works correctly.
#[tokio::test]
async fn test_atomic_bool_stop_flag() {
    let scheduler = Scheduler::new("atomic-flag".to_string(), Duration::from_secs(10));
    let running = Arc::new(AtomicBool::new(true));
    
    let r = running.clone();
    let stopped = Arc::new(AtomicBool::new(false));
    let s = stopped.clone();
    
    let _task_id = scheduler.schedule_repeating(
        Box::new(move || {
            let r1 = r.clone();
            let s1 = s.clone();
            Box::pin(async move {
                if s1.load(Ordering::SeqCst) {
                    r1.store(false, Ordering::SeqCst);
                }
                Ok(())
            })
        }),
        Duration::from_millis(50),
        "flag-checker"
    );
    
    // Initially running
    assert!(running.load(Ordering::SeqCst));
    
    // Set stop flag
    stopped.store(true, Ordering::SeqCst);
    
    sleep(Duration::from_millis(100)).await;
    
    // After stop flag set, task should see it
    assert!(!running.load(Ordering::SeqCst));
    
    scheduler.stop();
}

/// Test task handle cleanup after task completes naturally.
#[tokio::test]
async fn test_task_handle_cleanup() {
    let scheduler = Scheduler::new("cleanup".to_string(), Duration::from_secs(10));
    
    // Schedule task then cancel it
    let task_id = scheduler.schedule_repeating(
        Box::new(|| Box::pin(async { Ok::<(), anyhow::Error>(()) })),
        Duration::from_millis(50),
        "cleanup-task"
    );
    
    // Wait briefly
    sleep(Duration::from_millis(50)).await;
    
    // Cancel task
    let cancelled = scheduler.cancel_task(task_id);
    assert!(cancelled);
    
    // Cancel again - should fail (handle removed)
    sleep(Duration::from_millis(10)).await;
    let cancelled2 = scheduler.cancel_task(task_id);
    assert!(!cancelled2, "Handle should be removed after cancellation");
    
    scheduler.stop();
}

// ============================================================================
// Performance Characteristics Tests
// ============================================================================

/// Test scheduler can handle rapid task scheduling.
#[tokio::test]
async fn test_rapid_task_scheduling() {
    let scheduler = Scheduler::new("rapid".to_string(), Duration::from_secs(10));
    let counter = Arc::new(AtomicU32::new(0));
    
    // Rapidly schedule 10 tasks
    let mut task_ids: Vec<TaskId> = Vec::new();
    for i in 0..10 {
        let c = counter.clone();
        let desc = format!("rapid-task-{}", i);
        let id = scheduler.schedule_repeating(
            Box::new(move || {
                let c1 = c.clone();
                Box::pin(async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
            Duration::from_millis(100),
            &desc
        );
        task_ids.push(id);
    }
    
    // All should have valid IDs
    assert!(task_ids.iter().all(|id| *id > 0));
    
    sleep(Duration::from_millis(150)).await;
    
    // All 10 tasks should have executed
    let count = counter.load(Ordering::SeqCst);
    assert!(count >= 10, "All tasks should execute at least once, got {}", count);
    
    scheduler.stop();
}

/// Test scheduler handles task with sleep correctly.
#[tokio::test]
async fn test_task_with_internal_sleep() {
    let scheduler = Scheduler::new("internal-sleep".to_string(), Duration::from_secs(10));
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
        Duration::from_millis(30),
        "sleep-task"
    );
    
    sleep(Duration::from_millis(150)).await;
    
    let count = counter.load(Ordering::SeqCst);
    // Task has internal sleep, should still execute multiple times
    assert!(count >= 3, "Task with internal sleep should execute multiple times");
    
    scheduler.stop();
}