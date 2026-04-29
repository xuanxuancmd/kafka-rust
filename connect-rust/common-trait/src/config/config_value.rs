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

use serde_json::Value;

/// ConfigValue represents a configuration value with validation state.
///
/// This corresponds to `org.apache.kafka.common.config.ConfigValue` in Java.
#[derive(Debug, Clone)]
pub struct ConfigValue {
    name: String,
    value: Option<Value>,
    error_messages: Vec<String>,
    visible: bool,
}

impl ConfigValue {
    /// Creates a new ConfigValue with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        ConfigValue {
            name: name.into(),
            value: None,
            error_messages: vec![],
            visible: true,
        }
    }

    /// Creates a new ConfigValue with the given name and value.
    pub fn with_value(name: impl Into<String>, value: Value) -> Self {
        ConfigValue {
            name: name.into(),
            value: Some(value),
            error_messages: vec![],
            visible: true,
        }
    }

    /// Returns the name of this configuration value.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the value of this configuration.
    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    /// Returns the error messages for this configuration.
    pub fn error_messages(&self) -> &[String] {
        &self.error_messages
    }

    /// Returns whether this configuration is visible.
    pub fn visible(&self) -> bool {
        self.visible
    }

    /// Adds an error message to this configuration.
    pub fn add_error_message(&mut self, message: impl Into<String>) {
        self.error_messages.push(message.into());
    }

    /// Sets the value of this configuration.
    pub fn set_value(&mut self, value: Value) {
        self.value = Some(value);
    }

    /// Sets the visibility of this configuration.
    pub fn set_visible(&mut self, visible: bool) {
        self.visible = visible;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let cv = ConfigValue::new("test");
        assert_eq!(cv.name(), "test");
        assert!(cv.value().is_none());
        assert_eq!(cv.error_messages().len(), 0);
        assert!(cv.visible());
    }

    #[test]
    fn test_with_value() {
        let cv = ConfigValue::with_value("test", Value::String("value".to_string()));
        assert_eq!(cv.name(), "test");
        assert!(cv.value().is_some());
    }

    #[test]
    fn test_add_error_message() {
        let mut cv = ConfigValue::new("test");
        cv.add_error_message("error");
        assert_eq!(cv.error_messages().len(), 1);
    }
}
