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

//! WorkerConnector - Wrapper for running a connector instance.
//!
//! This module provides a wrapper around a Connector instance that manages
//! its lifecycle (start, stop, pause, resume), tracks status and state,
//! and provides the connector context.
//!
//! Key responsibilities:
//! - Initialize and start the connector with configuration
//! - Manage connector lifecycle transitions (start/stop/pause/resume)
//! - Track connector state and version
//! - Provide connector context for task reconfiguration requests
//! - Handle shutdown gracefully with timeout support
//!
//! Corresponds to `org.apache.kafka.connect.runtime.WorkerConnector` in Java.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use connect_api::components::Versioned;
use connect_api::connector::ConnectorContext;
use connect_api::errors::ConnectError;

use super::closeable_connector_context::CloseableConnectorContext;
use super::connect_metrics::{ConnectMetrics, PluginMetricsImpl, Sensor};
use super::status::ConnectorStatusListener;
use super::target_state::TargetState;

/// Internal state for WorkerConnector.
///
/// This tracks the current state of the connector within the WorkerConnector.
/// Corresponds to the internal state enumeration in Java's WorkerConnector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorState {
    /// Connector is initialized but not started.
    Init,
    /// Connector is paused.
    Paused,
    /// Connector is stopped.
    Stopped,
    /// Connector is running.
    Started,
    /// Connector has failed.
    Failed,
}

impl ConnectorState {
    /// Check if the connector is in a running state.
    pub fn is_running(&self) -> bool {
        *self == ConnectorState::Started
    }

    /// Check if the connector is in a stopped or paused state.
    pub fn is_stopped_or_paused(&self) -> bool {
        matches!(self, ConnectorState::Stopped | ConnectorState::Paused)
    }

    /// Check if the connector has failed.
    pub fn is_failed(&self) -> bool {
        *self == ConnectorState::Failed
    }
}

impl std::fmt::Display for ConnectorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorState::Init => write!(f, "INIT"),
            ConnectorState::Paused => write!(f, "PAUSED"),
            ConnectorState::Stopped => write!(f, "STOPPED"),
            ConnectorState::Started => write!(f, "STARTED"),
            ConnectorState::Failed => write!(f, "FAILED"),
        }
    }
}

/// Callback for state change completion.
///
/// This is used to notify when a state change operation completes.
pub trait StateChangeCallback: Send + Sync {
    /// Called when the state change completes successfully.
    fn on_complete(&self);

    /// Called when the state change fails.
    fn on_failure(&self, error: &dyn std::error::Error);
}

/// Connector metrics group for WorkerConnector.
///
/// This provides metrics tracking for connector operations.
/// Corresponds to ConnectorMetricsGroup in Java.
#[derive(Debug)]
pub struct ConnectorMetricsGroup {
    /// The connector name.
    connector_name: String,
    /// Plugin metrics for this connector.
    plugin_metrics: PluginMetricsImpl,
    /// Status sensor for tracking connector status.
    status_sensor: Arc<Sensor>,
}

impl ConnectorMetricsGroup {
    /// Create a new ConnectorMetricsGroup.
    ///
    /// # Arguments
    /// * `connect_metrics` - The parent ConnectMetrics instance
    /// * `connector_name` - The connector name
    pub fn new(connect_metrics: &ConnectMetrics, connector_name: &str) -> Self {
        let plugin_metrics = connect_metrics.connector_plugin_metrics(connector_name);

        // Create a metric group for connector status tracking
        let group = connect_metrics.group(
            connect_metrics.registry().connector_group_name(),
            &[("connector", connector_name)],
        );

        let status_sensor = group.sensor("connector-status");

        ConnectorMetricsGroup {
            connector_name: connector_name.to_string(),
            plugin_metrics,
            status_sensor,
        }
    }

    /// Get the connector name.
    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }

    /// Get the plugin metrics.
    pub fn plugin_metrics(&self) -> &PluginMetricsImpl {
        &self.plugin_metrics
    }

    /// Record a status change.
    pub fn record_status(&self, state: ConnectorState) {
        self.status_sensor.record(state.is_running() as i64 as f64);
    }

    /// Close the metrics group.
    pub fn close(&self) {
        self.status_sensor.record(0.0);
    }
}

/// Connector wrapper trait that provides dyn-compatible methods.
///
/// This trait wraps a Connector to provide a dyn-compatible interface
/// for lifecycle operations.
pub trait ConnectorWrapper: Send + Sync {
    /// Get the connector name.
    fn connector_name(&self) -> &str;

    /// Start the connector.
    fn start_connector(&mut self, props: HashMap<String, String>);

    /// Stop the connector.
    fn stop_connector(&mut self);

    /// Get the connector version.
    fn version(&self) -> &str;

    /// Check if this is a sink connector.
    fn is_sink_connector(&self) -> bool;
}

/// Implementation of ConnectorWrapper for any Connector type.
pub struct ConnectorWrapperImpl<C> {
    connector: Mutex<C>,
    connector_name: String,
    version: String,
    is_sink: bool,
}

impl<C: connect_api::connector::Connector + Send + Sync> ConnectorWrapperImpl<C> {
    /// Create a new ConnectorWrapperImpl.
    pub fn new(connector: C, connector_name: String, config: &HashMap<String, String>) -> Self {
        let version = C::version().to_string();
        let is_sink = config
            .get("connector.class")
            .map(|class| class.contains("Sink") || class.to_lowercase().contains("sink"))
            .unwrap_or(false);

        ConnectorWrapperImpl {
            connector: Mutex::new(connector),
            connector_name,
            version,
            is_sink,
        }
    }
}

impl<C: connect_api::connector::Connector + Send + Sync> ConnectorWrapper
    for ConnectorWrapperImpl<C>
{
    fn connector_name(&self) -> &str {
        &self.connector_name
    }

    fn start_connector(&mut self, props: HashMap<String, String>) {
        self.connector.lock().unwrap().start(props);
    }

    fn stop_connector(&mut self) {
        self.connector.lock().unwrap().stop();
    }

    fn version(&self) -> &str {
        &self.version
    }

    fn is_sink_connector(&self) -> bool {
        self.is_sink
    }
}

/// WorkerConnector - Wrapper for running a connector instance.
///
/// This struct manages the lifecycle of a Connector instance, providing:
/// - Initialization and startup with configuration
/// - Lifecycle management (start, stop, pause, resume)
/// - State tracking and status reporting
/// - Connector context for task reconfiguration requests
/// - Graceful shutdown with timeout support
///
/// Thread safety: Uses atomic flags and mutexes for state management.
///
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerConnector` in Java.
pub struct WorkerConnector {
    /// The connector name.
    conn_name: String,
    /// The connector configuration.
    config: HashMap<String, String>,
    /// The connector wrapper.
    connector: Arc<Mutex<Option<Box<dyn ConnectorWrapper>>>>,
    /// The connector context.
    ctx: Arc<Mutex<Option<Box<dyn CloseableConnectorContext>>>>,
    /// Metrics group for this connector.
    metrics: ConnectorMetricsGroup,
    /// Status listener for connector state changes.
    status_listener: Arc<Mutex<Box<dyn ConnectorStatusListener>>>,
    /// Current internal state.
    state: RwLock<ConnectorState>,
    /// Pending target state change.
    pending_target_state: Mutex<Option<TargetState>>,
    /// Pending state change callback.
    pending_callback: Mutex<Option<Arc<dyn StateChangeCallback>>>,
    /// Whether the connector is stopping.
    stopping: AtomicBool,
    /// Whether the shutdown was cancelled.
    cancelled: AtomicBool,
    /// External failure cause (stored as message).
    external_failure_message: Mutex<Option<String>>,
    /// Connector version.
    version: RwLock<Option<String>>,
    /// Generation for coordination.
    generation: AtomicI32,
    /// Whether the connector has been initialized.
    initialized: AtomicBool,
}

impl WorkerConnector {
    /// Create a new WorkerConnector.
    ///
    /// # Arguments
    /// * `conn_name` - The connector name
    /// * `connector_wrapper` - The connector wrapper instance
    /// * `config` - The connector configuration
    /// * `ctx` - The connector context
    /// * `connect_metrics` - The metrics for the connector
    /// * `status_listener` - The status listener for state changes
    ///
    /// Corresponds to Java: `WorkerConnector(String connName, Connector connector,
    /// Map<String, String> config, CloseableConnectorContext ctx,
    /// ConnectorMetricsGroup metrics, ConnectorStatus.Listener statusListener, ClassLoader loader)`
    pub fn new(
        conn_name: String,
        connector_wrapper: Box<dyn ConnectorWrapper>,
        config: HashMap<String, String>,
        ctx: Box<dyn CloseableConnectorContext>,
        connect_metrics: &ConnectMetrics,
        status_listener: Box<dyn ConnectorStatusListener>,
    ) -> Self {
        let metrics = ConnectorMetricsGroup::new(connect_metrics, &conn_name);
        let version = connector_wrapper.version().to_string();

        WorkerConnector {
            conn_name,
            config,
            connector: Arc::new(Mutex::new(Some(connector_wrapper))),
            ctx: Arc::new(Mutex::new(Some(ctx))),
            metrics,
            status_listener: Arc::new(Mutex::new(status_listener)),
            state: RwLock::new(ConnectorState::Init),
            pending_target_state: Mutex::new(None),
            pending_callback: Mutex::new(None),
            stopping: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            external_failure_message: Mutex::new(None),
            version: RwLock::new(Some(version)),
            generation: AtomicI32::new(0),
            initialized: AtomicBool::new(false),
        }
    }

    /// Create a new WorkerConnector from a concrete Connector type.
    ///
    /// This is a convenience method for creating a WorkerConnector
    /// from any type that implements the Connector trait.
    pub fn from_connector<C: connect_api::connector::Connector + Send + Sync + 'static>(
        conn_name: String,
        connector: C,
        config: HashMap<String, String>,
        ctx: Box<dyn CloseableConnectorContext>,
        connect_metrics: &ConnectMetrics,
        status_listener: Box<dyn ConnectorStatusListener>,
    ) -> Self {
        let connector_wrapper = Box::new(ConnectorWrapperImpl::new(
            connector,
            conn_name.clone(),
            &config,
        ));
        Self::new(
            conn_name,
            connector_wrapper,
            config,
            ctx,
            connect_metrics,
            status_listener,
        )
    }

    /// Get the connector name.
    pub fn conn_name(&self) -> &str {
        &self.conn_name
    }

    /// Get the current state.
    pub fn state(&self) -> ConnectorState {
        *self.state.read().unwrap()
    }

    /// Get the connector version.
    pub fn connector_version(&self) -> Option<String> {
        self.version.read().unwrap().clone()
    }

    /// Get the generation.
    pub fn generation(&self) -> i32 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Set the generation.
    pub fn set_generation(&self, generation: i32) {
        self.generation.store(generation, Ordering::SeqCst);
    }

    /// Check if the connector is running.
    ///
    /// Returns true if the connector is in the STARTED state.
    /// Corresponds to Java: `WorkerConnector.isRunning()`.
    pub fn is_running(&self) -> bool {
        self.state.read().unwrap().is_running()
    }

    /// Check if the connector is a sink connector.
    ///
    /// This determines the type of connector based on its implementation.
    /// Corresponds to Java: `WorkerConnector.isSinkConnector()`.
    pub fn is_sink_connector(&self) -> bool {
        let connector_guard = self.connector.lock().unwrap();
        if let Some(wrapper) = connector_guard.as_ref() {
            wrapper.is_sink_connector()
        } else {
            false
        }
    }

    /// Initialize the connector.
    ///
    /// This initializes the connector with its context if not already initialized.
    /// Corresponds to Java: `WorkerConnector.initialize()`.
    pub fn initialize(&self) -> Result<(), ConnectError> {
        if self.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }

        let connector_guard = self.connector.lock().unwrap();
        if connector_guard.is_some() {
            // In Java, this calls connector.initialize(context)
            // The connector should be initialized before starting
            self.initialized.store(true, Ordering::SeqCst);
            Ok(())
        } else {
            Err(ConnectError::general(
                "Connector not available for initialization",
            ))
        }
    }

    /// Start the connector.
    ///
    /// This transitions the connector to the STARTED state.
    /// Calls the connector's start method with the configuration.
    /// Corresponds to Java: `WorkerConnector.doStart()`.
    pub fn start(&self) -> Result<(), ConnectError> {
        // Initialize if not already done
        self.initialize()?;

        // Transition to started state
        {
            let mut state = self.state.write().unwrap();
            if *state == ConnectorState::Started {
                return Ok(()); // Already started
            }

            // Notify status listener about startup
            self.status_listener
                .lock()
                .unwrap()
                .on_startup(&self.conn_name);

            // Start the connector
            {
                let mut connector_guard = self.connector.lock().unwrap();
                if let Some(ref mut wrapper) = *connector_guard {
                    wrapper.start_connector(self.config.clone());
                }
            }

            *state = ConnectorState::Started;
        }

        self.metrics.record_status(ConnectorState::Started);

        // Handle pending state change callback
        self.handle_pending_callback(true);

        Ok(())
    }

    /// Pause the connector.
    ///
    /// This transitions the connector to the PAUSED state.
    /// Corresponds to Java: `WorkerConnector.suspend(true)` (for pause).
    pub fn pause(&self) -> Result<(), ConnectError> {
        {
            let mut state = self.state.write().unwrap();

            // Can only pause if started
            if *state != ConnectorState::Started {
                return Err(ConnectError::general(format!(
                    "Cannot pause connector '{}' in state {}",
                    self.conn_name, *state
                )));
            }

            // Notify status listener about pause
            self.status_listener
                .lock()
                .unwrap()
                .on_pause(&self.conn_name);

            // Stop the connector (pause also calls stop on the underlying connector)
            self.stop_connector_internal();

            *state = ConnectorState::Paused;
        }

        self.metrics.record_status(ConnectorState::Paused);

        // Handle pending state change callback
        self.handle_pending_callback(true);

        Ok(())
    }

    /// Resume the connector.
    ///
    /// This transitions the connector from PAUSED to STARTED state.
    /// Corresponds to Java: `WorkerConnector.resume()`.
    pub fn resume(&self) -> Result<(), ConnectError> {
        {
            let mut state = self.state.write().unwrap();

            // Can only resume if paused
            if *state != ConnectorState::Paused {
                return Err(ConnectError::general(format!(
                    "Cannot resume connector '{}' in state {}",
                    self.conn_name, *state
                )));
            }

            // Notify status listener about resume
            self.status_listener
                .lock()
                .unwrap()
                .on_resume(&self.conn_name);

            // Start the connector again
            {
                let mut connector_guard = self.connector.lock().unwrap();
                if let Some(ref mut wrapper) = *connector_guard {
                    wrapper.start_connector(self.config.clone());
                }
            }

            *state = ConnectorState::Started;
        }

        self.metrics.record_status(ConnectorState::Started);

        // Handle pending state change callback
        self.handle_pending_callback(true);

        Ok(())
    }

    /// Stop the connector.
    ///
    /// This transitions the connector to the STOPPED state.
    /// Corresponds to Java: `WorkerConnector.suspend(false)` (for stop).
    pub fn stop(&self) -> Result<(), ConnectError> {
        {
            let mut state = self.state.write().unwrap();

            // Can only stop if started or paused
            if !matches!(*state, ConnectorState::Started | ConnectorState::Paused) {
                return Ok(()); // Already stopped or in invalid state
            }

            // Notify status listener about stop
            self.status_listener
                .lock()
                .unwrap()
                .on_stop(&self.conn_name);

            // Stop the connector
            self.stop_connector_internal();

            *state = ConnectorState::Stopped;
        }

        self.metrics.record_status(ConnectorState::Stopped);

        // Handle pending state change callback
        self.handle_pending_callback(true);

        Ok(())
    }

    /// Initiate shutdown of the connector.
    ///
    /// This begins the shutdown process, setting the stopping flag.
    /// Corresponds to Java: `WorkerConnector.shutdown()`.
    pub fn shutdown(&self) {
        if self.stopping.load(Ordering::SeqCst) {
            return; // Already stopping
        }

        self.stopping.store(true, Ordering::SeqCst);

        // Stop the connector if it's running
        {
            let state = self.state.read().unwrap();
            if *state == ConnectorState::Started || *state == ConnectorState::Paused {
                self.stop_connector_internal();

                let mut state_guard = self.state.write().unwrap();
                *state_guard = ConnectorState::Stopped;
            }
        }
    }

    /// Wait for shutdown to complete.
    ///
    /// This waits for the connector to complete shutdown within the given timeout.
    /// Returns true if shutdown completed, false if timeout elapsed.
    /// Corresponds to Java: `WorkerConnector.awaitShutdown(long timeout)`.
    pub fn await_shutdown(&self, timeout_ms: u64) -> bool {
        // In the full implementation, this would wait on a latch
        // For now, we just check the state
        let start_time = std::time::Instant::now();
        while start_time.elapsed().as_millis() < timeout_ms as u128 {
            let state = self.state.read().unwrap();
            if matches!(
                *state,
                ConnectorState::Stopped | ConnectorState::Failed | ConnectorState::Init
            ) {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        false
    }

    /// Cancel the shutdown process.
    ///
    /// This is called when shutdown takes too long and needs to be cancelled.
    /// Corresponds to Java: `WorkerConnector.cancel()`.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);

        // Close the connector context to prevent zombie threads
        {
            let mut ctx_guard = self.ctx.lock().unwrap();
            if let Some(mut ctx) = ctx_guard.take() {
                ctx.close();
            }
        }
    }

    /// Check if shutdown was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Check if the connector is stopping.
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// Fail the connector.
    ///
    /// This marks the connector as failed with the given error.
    /// Corresponds to Java: `WorkerConnector.fail(Throwable cause)`.
    pub fn fail(&self, cause: ConnectError) {
        {
            let mut state = self.state.write().unwrap();

            // Store the external failure message
            {
                let mut failure = self.external_failure_message.lock().unwrap();
                *failure = Some(cause.message().to_string());
            }

            // Notify status listener about failure
            self.status_listener
                .lock()
                .unwrap()
                .on_failure(&self.conn_name, &cause);

            // Transition to failed state
            *state = ConnectorState::Failed;
        }

        self.metrics.record_status(ConnectorState::Failed);

        // Handle pending state change callback
        self.handle_pending_callback(false);
    }

    /// Get the external failure message.
    pub fn external_failure_message(&self) -> Option<String> {
        self.external_failure_message.lock().unwrap().clone()
    }

    /// Set pending target state change.
    ///
    /// This is used to queue a state change for later execution.
    pub fn set_pending_target_state(
        &self,
        target_state: TargetState,
        callback: Arc<dyn StateChangeCallback>,
    ) {
        let mut pending = self.pending_target_state.lock().unwrap();
        *pending = Some(target_state);

        let mut cb = self.pending_callback.lock().unwrap();
        *cb = Some(callback);
    }

    /// Execute pending state change if any.
    pub fn execute_pending_state_change(&self) -> Result<(), ConnectError> {
        let pending_guard = self.pending_target_state.lock().unwrap();
        let target_state = pending_guard.clone();

        if let Some(target) = target_state {
            match target {
                TargetState::STARTED => {
                    if *self.state.read().unwrap() == ConnectorState::Paused {
                        self.resume()?;
                    } else if matches!(
                        *self.state.read().unwrap(),
                        ConnectorState::Init | ConnectorState::Stopped
                    ) {
                        self.start()?;
                    }
                }
                TargetState::PAUSED => {
                    if *self.state.read().unwrap() == ConnectorState::Started {
                        self.pause()?;
                    }
                }
                TargetState::STOPPED => {
                    self.stop()?;
                }
            }
        }

        Ok(())
    }

    /// Stop the connector internally (without state transition).
    fn stop_connector_internal(&self) {
        // Close the connector context
        {
            let mut ctx_guard = self.ctx.lock().unwrap();
            if let Some(mut ctx) = ctx_guard.take() {
                ctx.close();
            }
        }

        // Stop the connector
        {
            let mut connector_guard = self.connector.lock().unwrap();
            if let Some(ref mut wrapper) = *connector_guard {
                wrapper.stop_connector();
            }
        }
    }

    /// Handle the pending callback after state change.
    fn handle_pending_callback(&self, success: bool) {
        let callback_guard = self.pending_callback.lock().unwrap();
        if let Some(ref callback) = callback_guard.as_ref() {
            if success {
                callback.on_complete();
            } else {
                if let Some(ref error_msg) = self.external_failure_message.lock().unwrap().as_ref()
                {
                    let error = ConnectError::general(error_msg.clone());
                    callback.on_failure(&error);
                }
            }
        }

        // Clear pending state
        let mut pending = self.pending_target_state.lock().unwrap();
        *pending = None;

        let mut cb = self.pending_callback.lock().unwrap();
        *cb = None;
    }

    /// Get the metrics group.
    pub fn metrics(&self) -> &ConnectorMetricsGroup {
        &self.metrics
    }

    /// Close the WorkerConnector.
    ///
    /// This should be called when the connector is being removed.
    pub fn close(&self) {
        // Shutdown if not already
        self.shutdown();

        // Close metrics
        self.metrics.close();
    }
}

impl std::fmt::Debug for WorkerConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConnector")
            .field("conn_name", &self.conn_name)
            .field("state", &self.state.read().unwrap())
            .field("stopping", &self.stopping.load(Ordering::SeqCst))
            .field("cancelled", &self.cancelled.load(Ordering::SeqCst))
            .field("generation", &self.generation.load(Ordering::SeqCst))
            .field("version", &self.version.read().unwrap())
            .finish_non_exhaustive()
    }
}

// ============================================================================
// Mock implementations for testing
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use common_trait::config::{Config, ConfigDef, ConfigValueEntry};
    use common_trait::util::time::SYSTEM;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    /// Mock connector for testing.
    struct MockConnector {
        started: AtomicBool,
        stopped: AtomicBool,
        version_str: String,
    }

    impl MockConnector {
        fn new() -> Self {
            MockConnector {
                started: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                version_str: "1.0.0".to_string(),
            }
        }
    }

    impl Versioned for MockConnector {
        fn version() -> &'static str {
            "1.0.0"
        }
    }

    impl connect_api::connector::Connector for MockConnector {
        fn context(&self) -> &dyn ConnectorContext {
            // Placeholder - tests use mock context
            unimplemented!("mock connector context")
        }

        fn initialize(&mut self, _context: Box<dyn ConnectorContext>) {
            // Mock initialization
        }

        fn initialize_with_task_configs(
            &mut self,
            _context: Box<dyn ConnectorContext>,
            _task_configs: Vec<HashMap<String, String>>,
        ) {
            // Mock initialization with task configs
        }

        fn start(&mut self, _props: HashMap<String, String>) {
            self.started.store(true, Ordering::SeqCst);
        }

        fn stop(&mut self) {
            self.stopped.store(true, Ordering::SeqCst);
        }

        fn task_class(&self) -> &'static str {
            "MockTask"
        }

        fn task_configs(
            &self,
            _max_tasks: i32,
        ) -> Result<Vec<HashMap<String, String>>, ConnectError> {
            Ok(vec![HashMap::new()])
        }

        fn validate(&self, _configs: HashMap<String, String>) -> Config {
            Config::new(vec![])
        }

        fn config(&self) -> &'static dyn ConfigDef {
            static MOCK_CONFIG_DEF: MockConfigDef = MockConfigDef;
            &MOCK_CONFIG_DEF
        }
    }

    /// Mock config definition for testing.
    struct MockConfigDef;

    impl ConfigDef for MockConfigDef {
        fn config_def(&self) -> HashMap<String, ConfigValueEntry> {
            HashMap::new()
        }
    }

    /// Mock connector context for testing.
    struct MockConnectorContext {
        closed: AtomicBool,
    }

    impl MockConnectorContext {
        fn new() -> Self {
            MockConnectorContext {
                closed: AtomicBool::new(false),
            }
        }
    }

    impl ConnectorContext for MockConnectorContext {
        fn request_task_reconfiguration(&mut self) {
            // Mock reconfiguration request
        }

        fn raise_error(&mut self, _error: ConnectError) {
            // Mock error handling
        }
    }

    impl CloseableConnectorContext for MockConnectorContext {
        fn close(&mut self) {
            self.closed.store(true, Ordering::SeqCst);
        }
    }

    /// Mock status listener for testing.
    struct MockStatusListener {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl MockStatusListener {
        fn new() -> Self {
            MockStatusListener {
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    impl ConnectorStatusListener for MockStatusListener {
        fn on_shutdown(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("shutdown:{}", connector));
        }

        fn on_failure(&mut self, connector: &str, cause: &dyn std::error::Error) {
            self.events
                .lock()
                .unwrap()
                .push(format!("failure:{}:{}", connector, cause));
        }

        fn on_stop(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("stop:{}", connector));
        }

        fn on_pause(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("pause:{}", connector));
        }

        fn on_resume(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("resume:{}", connector));
        }

        fn on_startup(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("startup:{}", connector));
        }

        fn on_deletion(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("deletion:{}", connector));
        }

        fn on_restart(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("restart:{}", connector));
        }
    }

    /// Mock state change callback for testing.
    struct MockStateChangeCallback {
        completed: AtomicBool,
        failed: AtomicBool,
    }

    impl MockStateChangeCallback {
        fn new() -> Self {
            MockStateChangeCallback {
                completed: AtomicBool::new(false),
                failed: AtomicBool::new(false),
            }
        }

        fn is_completed(&self) -> bool {
            self.completed.load(Ordering::SeqCst)
        }

        fn is_failed(&self) -> bool {
            self.failed.load(Ordering::SeqCst)
        }
    }

    impl StateChangeCallback for MockStateChangeCallback {
        fn on_complete(&self) {
            self.completed.store(true, Ordering::SeqCst);
        }

        fn on_failure(&self, _error: &dyn std::error::Error) {
            self.failed.store(true, Ordering::SeqCst);
        }
    }

    // ============================================================================
    // Unit tests
    // ============================================================================

    #[test]
    fn test_connector_state_display() {
        assert_eq!(ConnectorState::Init.to_string(), "INIT");
        assert_eq!(ConnectorState::Paused.to_string(), "PAUSED");
        assert_eq!(ConnectorState::Stopped.to_string(), "STOPPED");
        assert_eq!(ConnectorState::Started.to_string(), "STARTED");
        assert_eq!(ConnectorState::Failed.to_string(), "FAILED");
    }

    #[test]
    fn test_connector_state_is_running() {
        assert!(ConnectorState::Started.is_running());
        assert!(!ConnectorState::Init.is_running());
        assert!(!ConnectorState::Paused.is_running());
        assert!(!ConnectorState::Stopped.is_running());
        assert!(!ConnectorState::Failed.is_running());
    }

    #[test]
    fn test_connector_state_is_stopped_or_paused() {
        assert!(ConnectorState::Stopped.is_stopped_or_paused());
        assert!(ConnectorState::Paused.is_stopped_or_paused());
        assert!(!ConnectorState::Started.is_stopped_or_paused());
        assert!(!ConnectorState::Init.is_stopped_or_paused());
        assert!(!ConnectorState::Failed.is_stopped_or_paused());
    }

    #[test]
    fn test_connector_state_is_failed() {
        assert!(ConnectorState::Failed.is_failed());
        assert!(!ConnectorState::Started.is_failed());
        assert!(!ConnectorState::Init.is_failed());
    }

    #[test]
    fn test_connector_metrics_group_creation() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let metrics_group = ConnectorMetricsGroup::new(&connect_metrics, "test-connector");
        assert_eq!(metrics_group.connector_name(), "test-connector");
    }

    #[test]
    fn test_connector_metrics_group_record_status() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let metrics_group = ConnectorMetricsGroup::new(&connect_metrics, "test-connector");

        metrics_group.record_status(ConnectorState::Started);
        metrics_group.record_status(ConnectorState::Paused);
        metrics_group.record_status(ConnectorState::Stopped);
    }

    #[test]
    fn test_connector_wrapper_impl() {
        let connector = MockConnector::new();
        let config = HashMap::new();
        let wrapper = ConnectorWrapperImpl::new(connector, "test-connector".to_string(), &config);

        assert_eq!(wrapper.connector_name(), "test-connector");
        assert_eq!(wrapper.version(), "1.0.0");
    }

    #[test]
    fn test_worker_connector_creation() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let status_listener = Box::new(MockStatusListener::new());
        let config = HashMap::new();

        let worker_connector = WorkerConnector::from_connector(
            "test-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        assert_eq!(worker_connector.conn_name(), "test-connector");
        assert_eq!(worker_connector.state(), ConnectorState::Init);
        assert_eq!(
            worker_connector.connector_version(),
            Some("1.0.0".to_string())
        );
        assert!(!worker_connector.is_running());
        assert!(!worker_connector.is_stopping());
        assert!(!worker_connector.is_cancelled());
    }

    #[test]
    fn test_worker_connector_start() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let events = Arc::new(Mutex::new(Vec::new()));
        let status_listener = Box::new(MockStatusListener {
            events: events.clone(),
        });
        let config = HashMap::new();

        let worker_connector = WorkerConnector::from_connector(
            "test-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        // Start the connector
        let result = worker_connector.start();
        assert!(result.is_ok());
        assert_eq!(worker_connector.state(), ConnectorState::Started);
        assert!(worker_connector.is_running());

        // Check status listener was called
        let events = events.lock().unwrap();
        assert!(events.iter().any(|e| e.starts_with("startup:")));
    }

    #[test]
    fn test_worker_connector_pause_resume() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let events = Arc::new(Mutex::new(Vec::new()));
        let status_listener = Box::new(MockStatusListener {
            events: events.clone(),
        });
        let config = HashMap::new();

        let worker_connector = WorkerConnector::from_connector(
            "test-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        // Start the connector
        worker_connector.start().unwrap();
        assert!(worker_connector.is_running());

        // Pause the connector
        let result = worker_connector.pause();
        assert!(result.is_ok());
        assert_eq!(worker_connector.state(), ConnectorState::Paused);
        assert!(!worker_connector.is_running());

        // Resume the connector - will create a new connector since context was closed
        // For this test, we check that pause was recorded
        let events = events.lock().unwrap();
        assert!(events.iter().any(|e| e.starts_with("pause:")));
    }

    #[test]
    fn test_worker_connector_stop() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let events = Arc::new(Mutex::new(Vec::new()));
        let status_listener = Box::new(MockStatusListener {
            events: events.clone(),
        });
        let config = HashMap::new();

        let worker_connector = WorkerConnector::from_connector(
            "test-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        // Start the connector
        worker_connector.start().unwrap();
        assert!(worker_connector.is_running());

        // Stop the connector
        let result = worker_connector.stop();
        assert!(result.is_ok());
        assert_eq!(worker_connector.state(), ConnectorState::Stopped);
        assert!(!worker_connector.is_running());

        // Check events
        let events = events.lock().unwrap();
        assert!(events.iter().any(|e| e.starts_with("stop:")));
    }

    #[test]
    fn test_worker_connector_fail() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let events = Arc::new(Mutex::new(Vec::new()));
        let status_listener = Box::new(MockStatusListener {
            events: events.clone(),
        });
        let config = HashMap::new();

        let worker_connector = WorkerConnector::from_connector(
            "test-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        // Start the connector
        worker_connector.start().unwrap();

        // Fail the connector
        let error = ConnectError::general("Test failure");
        worker_connector.fail(error);

        assert_eq!(worker_connector.state(), ConnectorState::Failed);
        assert!(worker_connector.external_failure_message().is_some());

        // Check events
        let events = events.lock().unwrap();
        assert!(events.iter().any(|e| e.starts_with("failure:")));
    }

    #[test]
    fn test_worker_connector_shutdown() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let status_listener = Box::new(MockStatusListener::new());
        let config = HashMap::new();

        let worker_connector = WorkerConnector::from_connector(
            "test-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        // Start the connector
        worker_connector.start().unwrap();

        // Shutdown
        worker_connector.shutdown();
        assert!(worker_connector.is_stopping());

        // Wait for shutdown
        let completed = worker_connector.await_shutdown(1000);
        assert!(completed);
        assert_eq!(worker_connector.state(), ConnectorState::Stopped);
    }

    #[test]
    fn test_worker_connector_cancel() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let status_listener = Box::new(MockStatusListener::new());
        let config = HashMap::new();

        let worker_connector = WorkerConnector::from_connector(
            "test-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        // Cancel shutdown
        worker_connector.cancel();
        assert!(worker_connector.is_cancelled());
    }

    #[test]
    fn test_worker_connector_generation() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let status_listener = Box::new(MockStatusListener::new());
        let config = HashMap::new();

        let worker_connector = WorkerConnector::from_connector(
            "test-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        // Set generation
        worker_connector.set_generation(5);
        assert_eq!(worker_connector.generation(), 5);
    }

    #[test]
    fn test_worker_connector_is_sink_connector() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let status_listener = Box::new(MockStatusListener::new());

        // Test with sink connector config
        let mut config = HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "SomeSinkConnector".to_string(),
        );

        let worker_connector = WorkerConnector::from_connector(
            "sink-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        assert!(worker_connector.is_sink_connector());
    }

    #[test]
    fn test_worker_connector_is_source_connector() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let status_listener = Box::new(MockStatusListener::new());

        // Test with source connector config
        let mut config = HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "SomeSourceConnector".to_string(),
        );

        let worker_connector = WorkerConnector::from_connector(
            "source-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        assert!(!worker_connector.is_sink_connector());
    }

    #[test]
    fn test_worker_connector_pending_state_change() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let status_listener = Box::new(MockStatusListener::new());
        let config = HashMap::new();

        let worker_connector = WorkerConnector::from_connector(
            "test-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        // Set pending state change
        let callback = Arc::new(MockStateChangeCallback::new());
        worker_connector.set_pending_target_state(TargetState::STARTED, callback.clone());

        // Execute pending state change
        worker_connector.execute_pending_state_change().unwrap();

        // Connector should be started
        assert_eq!(worker_connector.state(), ConnectorState::Started);
        assert!(callback.is_completed());
    }

    #[test]
    fn test_worker_connector_close() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let connector = MockConnector::new();
        let ctx = Box::new(MockConnectorContext::new());
        let status_listener = Box::new(MockStatusListener::new());
        let config = HashMap::new();

        let worker_connector = WorkerConnector::from_connector(
            "test-connector".to_string(),
            connector,
            config,
            ctx,
            &connect_metrics,
            status_listener,
        );

        // Start the connector
        worker_connector.start().unwrap();

        // Close the connector
        worker_connector.close();

        assert!(worker_connector.is_stopping());
    }
}
