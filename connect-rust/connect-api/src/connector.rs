//! Connector interfaces
//!
//! This module defines core connector and task interfaces for Kafka Connect.

use crate::config::{Config, ConfigDef};
use crate::data::{ConnectRecord, Schema, SchemaAndValue, SinkRecord, SourceRecord};
use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};

/// Versioned trait
pub trait Versioned {
    fn version(&self) -> String;
}

/// Configurable trait
pub trait Configurable {
    fn configure(&mut self, configs: HashMap<String, Box<dyn Any>>);
}

/// Closeable trait
pub trait Closeable {
    fn close(&mut self) -> Result<(), Box<dyn Error>>;
}

/// Exactly once support enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactlyOnceSupport {
    Supported,
    Unsupported,
}

/// Connector transaction boundaries enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorTransactionBoundaries {
    Supported,
    Unsupported,
}

/// Topic partition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

/// Offset and metadata
#[derive(Debug, Clone)]
pub struct OffsetAndMetadata {
    pub offset: i64,
    pub metadata: Option<String>,
    pub leader_epoch: Option<i32>,
}

/// Record metadata
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// Connector trait
pub trait Connector: Versioned {
    fn initialize(&mut self, ctx: Box<dyn ConnectorContext>);
    fn start(&mut self, props: HashMap<String, String>);
    fn reconfigure(&mut self, props: HashMap<String, String>);
    fn task_class(&self) -> Box<dyn Any>;
    fn task_configs(&self, max_tasks: i32) -> Vec<HashMap<String, String>>;
    fn stop(&mut self);
    fn validate(&self, connector_configs: HashMap<String, String>) -> crate::config::Config;
    fn config(&self) -> ConfigDef;
}

/// Source connector trait
pub trait SourceConnector: Connector {
    fn exactly_once_support(&self, connector_config: HashMap<String, String>)
        -> ExactlyOnceSupport;
    fn can_define_transaction_boundaries(
        &self,
        connector_config: HashMap<String, String>,
    ) -> ConnectorTransactionBoundaries;
}

/// Sink connector trait
pub trait SinkConnector: Connector {
    fn alter_offsets(
        &self,
        connector_config: HashMap<String, String>,
        offsets: HashMap<TopicPartition, i64>,
    ) -> bool;
}

/// Task trait
pub trait Task: Versioned {
    fn start(&mut self, props: HashMap<String, String>);
    fn stop(&mut self);
}

/// Source task trait
pub trait SourceTask: Task {
    fn initialize(&mut self, context: Box<dyn SourceTaskContext>);
    fn poll(&mut self) -> Result<Vec<SourceRecord>, Box<dyn Error>>;
    fn commit(&mut self) -> Result<(), Box<dyn Error>>;
    fn commit_record(
        &mut self,
        record: SourceRecord,
        metadata: RecordMetadata,
    ) -> Result<(), Box<dyn Error>>;
}

/// Sink task trait
pub trait SinkTask: Task {
    fn initialize(&mut self, context: Box<dyn SinkTaskContext>);
    fn put(&mut self, records: Vec<SinkRecord>) -> Result<(), Box<dyn Error>>;
    fn flush(
        &mut self,
        current_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), Box<dyn Error>>;
}

/// Converter trait
pub trait Converter: Closeable {
    fn configure(&mut self, configs: HashMap<String, Box<dyn Any>>, is_key: bool);
    fn from_connect_data(
        &self,
        topic: String,
        schema: Box<dyn Schema>,
        value: Box<dyn Any>,
    ) -> Result<Vec<u8>, Box<dyn Error>>;
    fn to_connect_data(
        &self,
        topic: String,
        value: Vec<u8>,
    ) -> Result<SchemaAndValue, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
}

/// Header converter trait
pub trait HeaderConverter: Configurable + Closeable {
    fn to_connect_header(
        &self,
        topic: String,
        header_key: String,
        value: Vec<u8>,
    ) -> Result<SchemaAndValue, Box<dyn Error>>;
    fn from_connect_header(
        &self,
        topic: String,
        header_key: String,
        schema: Box<dyn Schema>,
        value: Box<dyn Any>,
    ) -> Result<Vec<u8>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
}

/// Transformation trait
pub trait Transformation<R: ConnectRecord<R>>: Configurable + Closeable {
    fn apply(&mut self, record: R) -> Result<Option<R>, Box<dyn Error>>;
    fn config(&self) -> ConfigDef;
}

/// Predicate trait
pub trait Predicate<R: ConnectRecord<R>>: Configurable + Closeable {
    fn config(&self) -> ConfigDef;
    fn test(&self, record: R) -> bool;
}

/// Connector context trait
pub trait ConnectorContext {
    fn request_task_reconfiguration(&self);
    fn raise_error(&self, e: Box<dyn Error>);
}

/// Source task context trait
pub trait SourceTaskContext {
    fn configs(&self) -> HashMap<String, String>;
}

/// Sink task context trait
pub trait SinkTaskContext {
    fn configs(&self) -> HashMap<String, String>;
    fn offset(&self, tp: TopicPartition, offset: i64);
    fn offsets(&self, offsets: HashMap<TopicPartition, i64>);
}

/// Base connector implementation
///
/// This struct provides a base implementation of the Connector trait that
/// can be extended by specific connector implementations.
pub struct BaseConnector {
    /// Configuration definition
    config_def: Arc<Mutex<ConfigDef>>,
    /// Connector properties
    props: Arc<Mutex<HashMap<String, String>>>,
    /// Whether the connector is initialized
    initialized: Arc<Mutex<bool>>,
    /// Whether the connector is started
    started: Arc<Mutex<bool>>,
    /// Connector context
    context: Arc<Mutex<Option<Box<dyn ConnectorContext>>>>,
    /// Task class type name
    task_class_name: Arc<Mutex<String>>,
    /// Connector version
    version: Arc<Mutex<String>>,
}

impl BaseConnector {
    /// Create a new base
    pub fn new() -> Self {
        Self {
            config_def: Arc::new(Mutex::new(ConfigDef::new())),
            props: Arc::new(Mutex::new(HashMap::new())),
            initialized: Arc::new(Mutex::new(false)),
            started: Arc::new(Mutex::new(false)),
            context: Arc::new(Mutex::new(None)),
            task_class_name: Arc::new(Mutex::new(String::from("BaseTask"))),
            version: Arc::new(Mutex::new(String::from("1.0.0"))),
        }
    }

    /// Create a new base connector with a specific version
    pub fn with_version(version: String) -> Self {
        let connector = Self::new();
        *connector.version.lock().unwrap() = version;
        connector
    }

    /// Create a new base connector with a specific task class name
    pub fn with_task_class(task_class_name: String) -> Self {
        let connector = Self::new();
        *connector.task_class_name.lock().unwrap() = task_class_name;
        connector
    }

    /// Set the configuration definition
    pub fn set_config_def(&mut self, config_def: ConfigDef) {
        *self.config_def.lock().unwrap() = config_def;
    }

    /// Check if the connector is initialized
    pub fn is_initialized(&self) -> bool {
        *self.initialized.lock().unwrap()
    }

    /// Check if the connector is started
    pub fn is_started(&self) -> bool {
        *self.started.lock().unwrap()
    }

    /// Get the connector properties
    pub fn get_props(&self) -> HashMap<String, String> {
        self.props.lock().unwrap().clone()
    }
}

impl Default for BaseConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Versioned for BaseConnector {
    fn version(&self) -> String {
        self.version.lock().unwrap().clone()
    }
}

impl Connector for BaseConnector {
    fn initialize(&mut self, ctx: Box<dyn ConnectorContext>) {
        *self.context.lock().unwrap() = Some(ctx);
        *self.initialized.lock().unwrap() = true;
    }

    fn start(&mut self, props: HashMap<String, String>) {
        *self.props.lock().unwrap() = props;
        *self.started.lock().unwrap() = true;
    }

    fn reconfigure(&mut self, props: HashMap<String, String>) {
        let mut current_props = self.props.lock().unwrap();
        for (key, value) in props {
            current_props.insert(key, value);
        }
    }

    fn task_class(&self) -> Box<dyn Any> {
        let class_name = self.task_class_name.lock().unwrap().clone();
        Box::new(class_name)
    }

    fn task_configs(&self, max_tasks: i32) -> Vec<HashMap<String, String>> {
        let props = self.props.lock().unwrap();
        let num_tasks = if max_tasks > 0 { max_tasks as usize } else { 1 };
        vec![props.clone(); num_tasks]
    }

    fn stop(&mut self) {
        *self.started.lock().unwrap() = false;
    }

    fn validate(&self, connector_configs: HashMap<String, String>) -> Config {
        let mut config = Config::new();
        let config_def = self.config_def.lock().unwrap();

        for (key, value) in connector_configs {
            if config_def.get_config(&key).is_some() {
                config.put(key, value);
            } else {
                config.add_error(format!("Unknown configuration: {}", key));
            }
        }

        config
    }

    fn config(&self) -> ConfigDef {
        self.config_def.lock().unwrap().clone()
    }
}
