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

//! Connect - Main entry point for Kafka Connect process.
//!
//! This class ties together all the components of a Kafka Connect process
//! (herder, worker, storage, command interface), managing their lifecycle.
//!
//! Corresponds to Java: `org.apache.kafka.connect.runtime.Connect`

use log::{debug, info};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;

/// Placeholder for Herder trait.
/// In full implementation, this would be common_trait::herder::Herder
pub trait Herder: Send + Sync {
    /// Start the herder.
    fn start(&mut self);

    /// Stop the herder.
    fn stop(&mut self);

    /// Get the herder task (for DistributedHerder).
    fn herder_task(&self) -> Option<HerderTaskHandle>;
}

/// Placeholder for herder task handle.
/// In Java, this is Future<?> representing the herder's background task.
#[derive(Debug, Clone)]
pub struct HerderTaskHandle {
    /// Task ID for tracking.
    id: String,
}

impl HerderTaskHandle {
    pub fn new(id: String) -> Self {
        HerderTaskHandle { id }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

/// Placeholder for REST server.
/// In full implementation, this would be crate::rest::RestServer
pub trait RestServer: Send + Sync {
    /// Initialize resources with the herder.
    fn initialize_resources(&mut self, herder: Arc<Mutex<dyn Herder>>);

    /// Stop the REST server.
    fn stop(&mut self);
}

/// Connect - Main entry point for Kafka Connect.
///
/// This class ties together all the components of a Kafka Connect process
/// (herder, worker, storage, command interface), managing their lifecycle.
///
/// In Java: `public class Connect<H extends Herder>`
/// In Rust, we use trait objects for flexibility.
///
/// Corresponds to Java: `org.apache.kafka.connect.runtime.Connect`
pub struct Connect {
    /// The herder managing connectors and tasks.
    herder: Arc<Mutex<dyn Herder>>,

    /// The herder task handle (for DistributedHerder).
    herder_task: Mutex<Option<HerderTaskHandle>>,

    /// The REST server for the Connect API.
    rest: Arc<Mutex<dyn RestServer>>,

    /// Latch for startup completion.
    /// In Java: `CountDownLatch startLatch = new CountDownLatch(1)`
    started: AtomicBool,

    /// Latch for shutdown completion.
    /// In Java: `CountDownLatch stopLatch = new CountDownLatch(1)`
    stopped: AtomicBool,

    /// Flag indicating shutdown is in progress.
    shutdown: AtomicBool,
}

impl Connect {
    /// Creates a new Connect instance.
    ///
    /// # Arguments
    /// * `herder` - The herder managing connectors and tasks
    /// * `rest` - The REST server for the Connect API
    ///
    /// Corresponds to Java: `Connect(H herder, ConnectRestServer rest)`
    pub fn new(herder: Arc<Mutex<dyn Herder>>, rest: Arc<Mutex<dyn RestServer>>) -> Self {
        debug!("Kafka Connect instance created");

        Connect {
            herder,
            herder_task: Mutex::new(None),
            rest,
            started: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Track task status which have been submitted to work thread.
    ///
    /// Returns the herder task handle for tracking status, or None
    /// if the herder type doesn't have a separate work thread.
    ///
    /// Corresponds to Java: `Connect.herderTask()`
    pub fn herder_task(&self) -> Option<HerderTaskHandle> {
        self.herder_task.lock().unwrap().clone()
    }

    /// Returns the herder.
    ///
    /// Corresponds to Java: `Connect.herder()`
    pub fn herder(&self) -> Arc<Mutex<dyn Herder>> {
        self.herder.clone()
    }

    /// Starts the Kafka Connect process.
    ///
    /// This method:
    /// 1. Starts the herder
    /// 2. Initializes the REST server with herder resources
    /// 3. Tracks the herder task for DistributedHerder
    ///
    /// Corresponds to Java: `Connect.start()`
    pub fn start(&self) {
        info!("Kafka Connect starting");

        // Add shutdown hook would be done via signal handling in Rust
        // Exit.addShutdownHook("connect-shutdown-hook", shutdownHook);

        // Start the herder
        self.herder.lock().unwrap().start();

        // Initialize REST server resources
        self.rest
            .lock()
            .unwrap()
            .initialize_resources(self.herder.clone());

        // Get herder task if this is a DistributedHerder
        // In full implementation, we'd check if herder is DistributedHerder
        let task = self.herder.lock().unwrap().herder_task();
        *self.herder_task.lock().unwrap() = task;

        // Mark as started
        self.started.store(true, Ordering::SeqCst);

        info!("Kafka Connect started");
    }

    /// Stops the Kafka Connect process.
    ///
    /// This method:
    /// 1. Stops the REST server
    /// 2. Stops the herder
    ///
    /// Corresponds to Java: `Connect.stop()`
    pub fn stop(&self) {
        let was_shutting_down = self.shutdown.swap(true, Ordering::SeqCst);

        if !was_shutting_down {
            info!("Kafka Connect stopping");

            // Stop REST server
            self.rest.lock().unwrap().stop();

            // Stop herder
            self.herder.lock().unwrap().stop();

            info!("Kafka Connect stopped");
        }

        // Mark as stopped
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// Waits for the Connect process to stop.
    ///
    /// In Java, this uses `stopLatch.await()`. In Rust, we use
    /// a simple spin loop with sleep.
    ///
    /// Corresponds to Java: `Connect.awaitStop()`
    pub fn await_stop(&self) {
        while !self.stopped.load(Ordering::SeqCst) {
            thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    /// Returns the REST server.
    ///
    /// This is visible for testing in Java.
    ///
    /// Corresponds to Java: `Connect.rest()` (visible for testing)
    pub fn rest(&self) -> Arc<Mutex<dyn RestServer>> {
        self.rest.clone()
    }

    /// Checks if Connect has started.
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Checks if Connect is shutting down.
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Checks if Connect has stopped.
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }
}

/// Shutdown hook for Connect.
///
/// In Java, this is an inner class that runs in a shutdown hook thread.
/// In Rust, this would typically be handled by signal handling.
pub struct ShutdownHook {
    connect: Arc<Connect>,
}

impl ShutdownHook {
    /// Creates a new shutdown hook.
    pub fn new(connect: Arc<Connect>) -> Self {
        ShutdownHook { connect }
    }

    /// Run the shutdown hook.
    ///
    /// Corresponds to Java: `ShutdownHook.run()`
    pub fn run(&self) {
        // Wait for startup to complete
        while !self.connect.is_started() {
            thread::sleep(std::time::Duration::from_millis(100));
        }

        // Stop the Connect process
        self.connect.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock herder for testing.
    struct MockHerder {
        started: bool,
        stopped: bool,
        has_task: bool,
    }

    impl Herder for MockHerder {
        fn start(&mut self) {
            self.started = true;
        }

        fn stop(&mut self) {
            self.stopped = true;
        }

        fn herder_task(&self) -> Option<HerderTaskHandle> {
            if self.has_task {
                Some(HerderTaskHandle::new("test-task".to_string()))
            } else {
                None
            }
        }
    }

    /// Mock REST server for testing.
    struct MockRestServer {
        initialized: bool,
        stopped: bool,
    }

    impl RestServer for MockRestServer {
        fn initialize_resources(&mut self, _herder: Arc<Mutex<dyn Herder>>) {
            self.initialized = true;
        }

        fn stop(&mut self) {
            self.stopped = true;
        }
    }

    #[test]
    fn test_connect_new() {
        let herder = Arc::new(Mutex::new(MockHerder {
            started: false,
            stopped: false,
            has_task: false,
        }));
        let rest = Arc::new(Mutex::new(MockRestServer {
            initialized: false,
            stopped: false,
        }));

        let connect = Connect::new(herder, rest);

        assert!(!connect.is_started());
        assert!(!connect.is_shutting_down());
        assert!(!connect.is_stopped());
    }

    #[test]
    fn test_connect_start() {
        let herder = Arc::new(Mutex::new(MockHerder {
            started: false,
            stopped: false,
            has_task: false,
        }));
        let rest = Arc::new(Mutex::new(MockRestServer {
            initialized: false,
            stopped: false,
        }));

        let connect = Connect::new(herder.clone(), rest.clone());

        connect.start();

        assert!(connect.is_started());
        assert!(herder.lock().unwrap().started);
        assert!(rest.lock().unwrap().initialized);
    }

    #[test]
    fn test_connect_stop() {
        let herder = Arc::new(Mutex::new(MockHerder {
            started: false,
            stopped: false,
            has_task: false,
        }));
        let rest = Arc::new(Mutex::new(MockRestServer {
            initialized: false,
            stopped: false,
        }));

        let connect = Connect::new(herder.clone(), rest.clone());

        connect.start();
        connect.stop();

        assert!(connect.is_shutting_down());
        assert!(connect.is_stopped());
        assert!(herder.lock().unwrap().stopped);
        assert!(rest.lock().unwrap().stopped);
    }

    #[test]
    fn test_connect_herder_task() {
        let herder = Arc::new(Mutex::new(MockHerder {
            started: false,
            stopped: false,
            has_task: true,
        }));
        let rest = Arc::new(Mutex::new(MockRestServer {
            initialized: false,
            stopped: false,
        }));

        let connect = Connect::new(herder, rest);

        connect.start();

        let task = connect.herder_task();
        assert!(task.is_some());
        assert_eq!(task.unwrap().id(), "test-task");
    }

    #[test]
    fn test_connect_no_herder_task() {
        let herder = Arc::new(Mutex::new(MockHerder {
            started: false,
            stopped: false,
            has_task: false,
        }));
        let rest = Arc::new(Mutex::new(MockRestServer {
            initialized: false,
            stopped: false,
        }));

        let connect = Connect::new(herder, rest);

        connect.start();

        let task = connect.herder_task();
        assert!(task.is_none());
    }

    #[test]
    fn test_shutdown_hook() {
        let herder = Arc::new(Mutex::new(MockHerder {
            started: false,
            stopped: false,
            has_task: false,
        }));
        let rest = Arc::new(Mutex::new(MockRestServer {
            initialized: false,
            stopped: false,
        }));

        let connect = Arc::new(Connect::new(herder, rest));
        let hook = ShutdownHook::new(connect.clone());

        // Start the connect
        connect.start();

        // Run the shutdown hook
        hook.run();

        assert!(connect.is_stopped());
    }

    #[test]
    fn test_connect_double_stop() {
        let herder = Arc::new(Mutex::new(MockHerder {
            started: false,
            stopped: false,
            has_task: false,
        }));
        let rest = Arc::new(Mutex::new(MockRestServer {
            initialized: false,
            stopped: false,
        }));

        let connect = Connect::new(herder.clone(), rest.clone());

        connect.start();
        connect.stop();
        connect.stop(); // Second stop should not cause issues

        // Check that herder.stop was only called once
        // (MockHerder doesn't track call count, but shutdown flag prevents double call)
        assert!(connect.is_shutting_down());
    }

    #[test]
    fn test_herder_task_handle() {
        let handle = HerderTaskHandle::new("task-123".to_string());
        assert_eq!(handle.id(), "task-123");

        let cloned = handle.clone();
        assert_eq!(cloned.id(), "task-123");
    }
}
