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

use crate::config::ConfigValue;

/// Config represents a collection of configuration values.
///
/// This corresponds to `org.apache.kafka.common.config.Config` in Java.
#[derive(Debug, Clone)]
pub struct Config {
    config_values: Vec<ConfigValue>,
}

impl Config {
    /// Creates a new Config with the given configuration values.
    pub fn new(config_values: Vec<ConfigValue>) -> Self {
        Config { config_values }
    }

    /// Returns the configuration values.
    pub fn config_values(&self) -> &[ConfigValue] {
        &self.config_values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let config = Config::new(vec![]);
        assert_eq!(config.config_values().len(), 0);
    }
}
