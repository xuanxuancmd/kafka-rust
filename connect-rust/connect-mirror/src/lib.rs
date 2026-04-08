//! Kafka Connect MirrorMaker2
//!
//! This crate provides MirrorMaker2 functionality for Kafka Connect.

pub mod config;
pub mod connector;
pub mod filter;
pub mod maker;
pub mod maker_impl;
pub mod source_connector_impl;
pub mod task;
pub mod utils;
// Re-export source_task_impl for testing purposes
pub mod source_task_impl;

pub use connector::{MirrorCheckpointConnector, MirrorHeartbeatConnector, MirrorSourceConnector};
pub use maker::{ConnectorState, MirrorMaker, MirrorMakerImpl, TaskConfig};
pub use task::{
    ConsumerRecord, MirrorCheckpointTask, MirrorHeartbeatTask, MirrorSourceTask, RecordMetadata,
};
