//! Config module
//!
//! Provides configuration traits and structs for Connect runtime.

use std::any::Any;
use std::collections::HashMap;
use std::error::Error;

use connect_api::ConfigDef;

/// Worker config
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub config: HashMap<String, String>,
}

impl WorkerConfig {
    /// Create a new worker config
    pub fn new(worker_id: String) -> Self {
        Self {
            worker_id,
            config: HashMap::new(),
        }
    }

    /// Add a config property
    pub fn with_property(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }

    /// Get a config property
    pub fn get(&self, key: &str) -> Option<&String> {
        self.config.get(key)
    }

    /// Get worker ID
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }
}

/// Connector config
#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    pub name: String,
    pub config: HashMap<String, String>,
}

impl ConnectorConfig {
    /// Create a new connector config
    pub fn new(name: String) -> Self {
        Self {
            name,
            config: HashMap::new(),
        }
    }

    /// Add a config property
    pub fn with_property(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }

    /// Get a config property
    pub fn get(&self, key: &str) -> Option<&String> {
        self.config.get(key)
    }

    /// Get connector name
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Task config
#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub task_id: i32,
    pub config: HashMap<String, String>,
}

impl TaskConfig {
    /// Create a new task config
    pub fn new(task_id: i32) -> Self {
        Self {
            task_id,
            config: HashMap::new(),
        }
    }

    /// Add a config property
    pub fn with_property(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }

    /// Get a config property
    pub fn get(&self, key: &str) -> Option<&String> {
        self.config.get(key)
    }

    /// Get task ID

    pub fn task_id(&self) -> i32 {
        self.task_id
    }
}

/// Config provider trait
pub trait ConfigProvider {
    /// Get config value
    fn get(&self, key: &str) -> Option<String>;

    /// Get all config values
    fn all(&self) -> HashMap<String, String>;

    /// Set config value
    fn set(&mut self, key: String, value: String);

    /// Remove config value
    fn remove(&mut self, key: &str) -> Option<String>;
}

/// Config validator trait
pub trait ConfigValidator {
    /// Validate config
    fn validate(&self, config: &HashMap<String, String>) -> Result<(), ConfigValidationError>;

    /// Get config definition
    fn config_def(&self) -> ConfigDef;
}

/// Config validation error
#[derive(Debug, Clone)]
pub struct ConfigValidationError {
    pub message: String,
    pub errors: Vec<String>,
}

impl ConfigValidationError {
    /// Create a new config validation error
    pub fn new(message: String) -> Self {
        Self {
            message,
            errors: Vec::new(),
        }
    }

    /// Add an error
    pub fn add_error(&mut self, error: String) {
        self.errors.push(error);
    }
}

impl std::fmt::Display for ConfigValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)?;
        for error in &self.errors {
            write!(f, "\n  - {}", error)?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigValidationError {}

/// Config transformer trait
pub trait ConfigTransformer {
    /// Transform config
    fn transform(
        &self,
        config: HashMap<String, String>,
    ) -> Result<HashMap<String, String>, Box<dyn Error>>;
}

/// Config merger trait
pub trait ConfigMerger {
    /// Merge configs
    fn merge(
        &self,
        base: HashMap<String, String>,
        r#override: HashMap<String, String>,
    ) -> HashMap<String, String>;
}

/// Default config merger
pub struct DefaultConfigMerger;

impl ConfigMerger for DefaultConfigMerger {
    fn merge(
        &self,
        base: HashMap<String, String>,
        r#override: HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut result = base;
        for (key, value) in r#override {
            result.insert(key, value);
        }
        result
    }
}
