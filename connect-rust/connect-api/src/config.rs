//! Configuration utilities
//!
//! This module defines configuration structures and utilities.

use std::any::Any;
use std::collections::HashMap;

/// Configuration value
#[derive(Clone, Debug)]
pub enum ConfigValue {
    String(String),
    Int(i32),
    Long(i64),
    Double(f64),
    Boolean(bool),
    List(Vec<String>),
}

/// Configuration definition
pub struct ConfigDef {
    configs: HashMap<String, ConfigValue>,
}

impl ConfigDef {
    pub fn new() -> Self {
        Self {
            configs: HashMap::new(),
        }
    }

    pub fn add_config(&mut self, name: String, value: ConfigValue) {
        self.configs.insert(name, value);
    }

    pub fn get_config(&self, name: &str) -> Option<&ConfigValue> {
        self.configs.get(name)
    }

    pub fn configs(&self) -> &HashMap<String, ConfigValue> {
        &self.configs
    }
}

impl Default for ConfigDef {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ConfigDef {
    fn clone(&self) -> Self {
        Self {
            configs: self.configs.clone(),
        }
    }
}

/// Configuration
pub struct Config {
    values: HashMap<String, String>,
    errors: Vec<String>,
}

impl Config {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
            errors: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: HashMap::with_capacity(capacity),
            errors: Vec::new(),
        }
    }

    pub fn put(&mut self, key: String, value: String) {
        self.values.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.values.get(key)
    }

    pub fn get_int(&self, key: &str) -> Option<i32> {
        self.values.get(key).and_then(|v| v.parse().ok())
    }

    pub fn get_long(&self, key: &str) -> Option<i64> {
        self.values.get(key).and_then(|v| v.parse().ok())
    }

    pub fn get_double(&self, key: &str) -> Option<f64> {
        self.values.get(key).and_then(|v| v.parse().ok())
    }

    pub fn get_boolean(&self, key: &str) -> Option<bool> {
        self.values
            .get(key)
            .and_then(|v| match v.to_lowercase().as_str() {
                "true" | "t" | "yes" | "y" | "1" => Some(true),
                "false" | "f" | "no" | "n" | "0" => Some(false),
                _ => None,
            })
    }

    pub fn get_list(&self, key: &str) -> Option<Vec<String>> {
        self.values.get(key).map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
    }

    pub fn values(&self) -> &HashMap<String, String> {
        &self.values
    }

    pub fn keys(&self) -> Vec<String> {
        self.values.keys().cloned().collect()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    pub fn add_error(&mut self, error: String) {
        self.errors.push(error);
    }

    pub fn errors(&self) -> &[String] {
        &self.errors
    }

    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Config {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            errors: self.errors.clone(),
        }
    }
}

/// Configurable trait
pub trait Configurable {
    fn configure(&mut self, configs: HashMap<String, Box<dyn Any>>);
}
