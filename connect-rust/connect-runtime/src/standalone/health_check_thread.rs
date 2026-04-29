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

//! Thread that can be used to check for the readiness and liveness of a standalone herder.
//!
//! This corresponds to `org.apache.kafka.connect.runtime.standalone.HealthCheckThread` in Java.
//!
//! @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-1017%3A+Health+check+endpoint+for+Kafka+Connect">KIP-1017</a>

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use crate::herder::StandaloneHerder;
use common_trait::herder::Callback;

/// State shared between the HealthCheckThread and the spawned thread.
struct SharedState {
    /// Queue of callbacks waiting to be processed.
    /// Callbacks must be Send + Sync to be shared between threads.
    callbacks: VecDeque<Arc<dyn Callback<()> + Send + Sync>>,
}

/// Thread that can be used to check for the readiness and liveness of a standalone herder.
///
/// This thread processes health check callbacks in a background thread,
/// verifying that the herder is not deadlocked. For standalone mode,
/// the only criteria for liveness is that the worker isn't deadlocked.
///
/// Thread safety: Uses Mutex + Condvar to implement synchronized wait/notify semantics,
/// matching Java's synchronized(this) + wait()/notifyAll() pattern.
///
/// Corresponds to `org.apache.kafka.connect.runtime.standalone.HealthCheckThread` in Java.
pub struct HealthCheckThread {
    /// Reference to the StandaloneHerder being monitored.
    herder: Arc<Mutex<StandaloneHerder>>,
    /// Shared state protected by mutex (callbacks queue).
    state: Arc<Mutex<SharedState>>,
    /// Condvar for wait/notify semantics.
    condvar: Arc<Condvar>,
    /// Flag to track if thread is running (volatile in Java).
    running: Arc<AtomicBool>,
    /// Thread handle for joining on shutdown.
    thread_handle: Mutex<Option<JoinHandle<()>>>,
}

impl HealthCheckThread {
    /// Creates a new HealthCheckThread.
    ///
    /// The thread is created but not started. Call `start()` to begin processing callbacks.
    ///
    /// # Arguments
    /// * `herder` - The StandaloneHerder to monitor for health.
    pub fn new(herder: Arc<Mutex<StandaloneHerder>>) -> Self {
        HealthCheckThread {
            herder,
            state: Arc::new(Mutex::new(SharedState {
                callbacks: VecDeque::new(),
            })),
            condvar: Arc::new(Condvar::new()),
            running: Arc::new(AtomicBool::new(true)),
            thread_handle: Mutex::new(None),
        }
    }

    /// Starts the health check thread.
    ///
    /// This spawns a background thread that processes health check callbacks.
    /// The thread will continue running until `shut_down()` is called.
    pub fn start(&self) {
        let herder = self.herder.clone();
        let state = self.state.clone();
        let condvar = self.condvar.clone();
        let running = self.running.clone();

        let handle = thread::spawn(move || {
            Self::run_loop(herder, state, condvar, running);
        });

        // Store the handle (thread_handle is already Mutex protected)
        *self.thread_handle.lock().unwrap() = Some(handle);
    }

    /// Internal run loop that processes health check callbacks.
    ///
    /// This corresponds to the `run()` method in Java's HealthCheckThread.
    fn run_loop(
        herder: Arc<Mutex<StandaloneHerder>>,
        state: Arc<Mutex<SharedState>>,
        condvar: Arc<Condvar>,
        running: Arc<AtomicBool>,
    ) {
        // Main loop: process callbacks until shutdown
        loop {
            let callback: Option<Arc<dyn Callback<()> + Send + Sync>>;

            // synchronized(this) block in Java - wait for callback or shutdown
            {
                let mut guard = state.lock().unwrap();
                while running.load(Ordering::SeqCst) && guard.callbacks.is_empty() {
                    guard = condvar.wait(guard).unwrap();
                }

                if !running.load(Ordering::SeqCst) {
                    break;
                }

                callback = guard.callbacks.pop_front();
            }

            // Process the callback outside the main state lock
            if let Some(cb) = callback {
                // In Java, this is done inside synchronized(herder) block
                // to verify herder isn't deadlocked (can acquire the monitor)
                let _herder_guard = herder.lock().unwrap();
                // For liveness check, we just verify we can acquire the lock
                // The callback is invoked with () result (meaning healthy)
                // We catch any exception from the callback and log it (Java style)
                let result: std::thread::Result<()> =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        cb.on_completion(());
                    }));

                if let Err(e) = result {
                    if let Some(s) = e.downcast_ref::<&str>() {
                        log::warn!("Failed to complete health check callback: {}", s);
                    } else if let Some(s) = e.downcast_ref::<String>() {
                        log::warn!("Failed to complete health check callback: {}", s);
                    } else {
                        log::warn!("Failed to complete health check callback: unknown panic");
                    }
                }
                // _herder_guard is released here
            }
        }

        // Shutdown phase: drain remaining callbacks with error
        // In Java, this happens after the main loop breaks
        let shutting_down_msg = "The herder is shutting down";

        // Don't leave callbacks dangling, even if shutdown has already started
        {
            let mut guard = state.lock().unwrap();
            while let Some(cb) = guard.callbacks.pop_front() {
                let result: std::thread::Result<()> =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        cb.on_error(shutting_down_msg.to_string());
                    }));

                if let Err(e) = result {
                    if let Some(s) = e.downcast_ref::<&str>() {
                        log::warn!(
                            "Failed to complete health check callback during shutdown: {}",
                            s
                        );
                    } else if let Some(s) = e.downcast_ref::<String>() {
                        log::warn!(
                            "Failed to complete health check callback during shutdown: {}",
                            s
                        );
                    } else {
                        log::warn!("Failed to complete health check callback during shutdown: unknown panic");
                    }
                }
            }
        }
    }

    /// Check the health of the herder.
    ///
    /// This method may be called at any time after the thread has been constructed,
    /// but callbacks will only be invoked after this thread has been started.
    ///
    /// # Arguments
    /// * `callback` - Callback to invoke after herder health has been verified or if
    ///                an error occurs that indicates herder is unhealthy; may not be null
    ///
    /// # Errors
    /// Returns an error if invoked after `shut_down()`.
    ///
    /// Corresponds to `check(Callback<Void>)` in Java.
    pub fn check(&self, callback: Arc<dyn Callback<()> + Send + Sync>) -> Result<(), String> {
        // synchronized(this) block in Java
        let mut guard = self.state.lock().unwrap();

        if !self.running.load(Ordering::SeqCst) {
            return Err("Cannot check herder health after thread has been shut down".to_string());
        }

        guard.callbacks.push_back(callback);
        self.condvar.notify_all();

        Ok(())
    }

    /// Trigger shutdown of the thread, stop accepting new callbacks via `check()`,
    /// and await the termination of the thread.
    ///
    /// After shutdown, any remaining callbacks in the queue will be invoked
    /// with an error indicating the herder is shutting down.
    ///
    /// Corresponds to `shutDown()` in Java.
    pub fn shut_down(&self) {
        // synchronized(this) block - set running to false and notify
        {
            self.running.store(false, Ordering::SeqCst);
            self.condvar.notify_all();
        }

        // Take the handle from the Mutex (need to release lock before joining)
        let handle = {
            let mut handle_guard = self.thread_handle.lock().unwrap();
            handle_guard.take()
        };

        // Join the thread (like Java's join())
        if let Some(handle) = handle {
            // In Java, if join() throws InterruptedException, it interrupts the thread
            // and preserves the interrupt status
            // In Rust, we don't have thread interruption, but we handle join errors
            match handle.join() {
                Ok(_) => {}
                Err(e) => {
                    log::warn!(
                        "Error joining health check thread (will not wait for termination): {:?}",
                        e
                    );
                }
            }
        }
    }
}

impl Drop for HealthCheckThread {
    fn drop(&mut self) {
        // Ensure thread is shut down on drop
        // This matches Java's implicit cleanup behavior
        self.running.store(false, Ordering::SeqCst);
        self.condvar.notify_all();

        // Try to join the thread, but don't wait indefinitely
        let mut handle_guard = self.thread_handle.lock().unwrap();
        if let Some(handle) = handle_guard.take() {
            // In Rust, we can't truly interrupt a thread like Java
            // Just log if join fails
            if let Err(e) = handle.join() {
                log::warn!("Health check thread did not shut down cleanly: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::herder::standalone_herder::SimpleCallback;
    use crate::worker::{Worker, WorkerConfig};

    fn create_test_herder() -> Arc<Mutex<StandaloneHerder>> {
        let worker = Arc::new(Worker::new(
            WorkerConfig::new("test-worker".to_string()),
            None,
            None,
        ));
        let herder = StandaloneHerder::new(worker, "test-cluster".to_string(), None);
        Arc::new(Mutex::new(herder))
    }

    #[test]
    fn test_health_check_thread_creation() {
        let herder = create_test_herder();
        let thread = HealthCheckThread::new(herder);

        assert!(thread.running.load(Ordering::SeqCst));
        assert!(thread.thread_handle.lock().unwrap().is_none());
    }

    #[test]
    fn test_check_before_start() {
        let herder = create_test_herder();
        let thread = HealthCheckThread::new(herder);

        // Can add callback before start
        let callback = Arc::new(SimpleCallback::new());
        let result = thread.check(callback);
        assert!(result.is_ok());

        // Callback should be in queue
        let guard = thread.state.lock().unwrap();
        assert_eq!(guard.callbacks.len(), 1);
    }

    #[test]
    fn test_check_after_shutdown() {
        let herder = create_test_herder();
        let thread = HealthCheckThread::new(herder);

        thread.shut_down();

        // Cannot add callback after shutdown
        let callback = Arc::new(SimpleCallback::new());
        let result = thread.check(callback);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("shut down"));
    }

    #[test]
    fn test_start_and_shutdown() {
        let herder = create_test_herder();
        let thread = HealthCheckThread::new(herder);

        thread.start();

        // Wait a bit for thread to start
        std::thread::sleep(std::time::Duration::from_millis(100));

        assert!(thread.running.load(Ordering::SeqCst));

        thread.shut_down();

        assert!(!thread.running.load(Ordering::SeqCst));
    }
}
