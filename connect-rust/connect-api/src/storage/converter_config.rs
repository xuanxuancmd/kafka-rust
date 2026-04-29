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

use common_trait::config::{
    AbstractConfig, CaseInsensitiveValidString, ConfigDefBuilder, ConfigDefImportance,
    ConfigDefType,
};

/// Configuration name for converter type.
/// Corresponds to: `ConverterConfig.TYPE_CONFIG` in Java
pub const TYPE_CONFIG: &str = "converter.type";

/// Documentation for converter type configuration.
/// Corresponds to: `ConverterConfig.TYPE_DOC` in Java
const TYPE_DOC: &str = "How this converter will be used.";

/// ConverterConfig for converter configuration.
///
/// This corresponds to `org.apache.kafka.connect.storage.ConverterConfig` in Java.
pub struct ConverterConfig {
    config: AbstractConfig,
}

impl ConverterConfig {
    pub fn new(config: AbstractConfig) -> Self {
        ConverterConfig { config }
    }

    /// Creates a new ConfigDef instance containing the configurations defined by ConverterConfig.
    /// This can be called by subclasses.
    ///
    /// Corresponds to: `ConverterConfig.newConfigDef()` in Java
    pub fn new_config_def() -> ConfigDefBuilder {
        ConfigDefBuilder::new().define_with_validator(
            TYPE_CONFIG,
            ConfigDefType::String,
            None, // NO_DEFAULT_VALUE in Java
            Some(Box::new(CaseInsensitiveValidString::in_values(&[
                "KEY", "VALUE", "HEADER",
            ]))),
            ConfigDefImportance::Low,
            TYPE_DOC,
        )
    }

    pub fn type_config(&self) -> Option<&str> {
        self.config.get_string(TYPE_CONFIG)
    }
}
