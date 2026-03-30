//! MirrorMaker main orchestrator
//!
//! This module provides main MirrorMaker class that orchestrates
//! all mirror connectors.

use anyhow::Result;
use std::collections::HashMap;

/// Connector state
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectorState {
    CREATED,
    RUNNING,
    PAUSED,
    STOPPED,
    FAILED,
    DESTROYED,
}

/// Task configuration
#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub task_id: i32,
    pub config: HashMap<String, String>,
}

/// MirrorMaker main orchestrator
///
/// This is the main entry point for MirrorMaker2 functionality.
/// It manages all mirror connectors and their lifecycle.
pub trait MirrorMaker {
    /// Start the MirrorMaker
    fn start(&mut self) -> Result<()>;

    /// Stop the MirrorMaker
    fn stop(&mut self) -> Result<()>;

    /// Wait for the MirrorMaker to stop
    fn await_stop(&mut self) -> Result<()>;

    /// Get connector status
    fn connector_status(&self, conn_name: String) -> Option<ConnectorState>;

    /// Get task configs for a connector
    fn task_configs(&self, conn_name: String) -> Option<Vec<TaskConfig>>;

    /// Main entry point for command line execution
    fn main(&self) -> Result<()>;
}

/// MirrorMaker implementation
pub use crate::maker_impl::{MirrorMakerImpl, SourceAndTarget};
