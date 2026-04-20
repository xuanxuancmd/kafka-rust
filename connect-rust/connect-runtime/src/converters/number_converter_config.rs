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

//! Configuration options for NumberConverter instances.
//!
//! This corresponds to `org.apache.kafka.connect.converters.NumberConverterConfig` in Java.

use common_trait::config::{AbstractConfig, ConfigDefBuilder};
use connect_api::storage::{ConverterConfig, TYPE_CONFIG};

/// Configuration options for LongConverter, IntegerConverter, ShortConverter,
/// DoubleConverter, and FloatConverter instances.
///
/// This corresponds to `org.apache.kafka.connect.converters.NumberConverterConfig` in Java.
pub struct NumberConverterConfig {
    config: AbstractConfig,
}

impl NumberConverterConfig {
    /// Creates the ConfigDef for NumberConverterConfig.
    ///
    /// Corresponds to `NumberConverterConfig.configDef()` in Java.
    pub fn config_def() -> ConfigDefBuilder {
        ConverterConfig::new_config_def()
    }

    /// Creates a new NumberConverterConfig from the given properties.
    ///
    /// Corresponds to `NumberConverterConfig(Map<String, ?> props)` in Java.
    pub fn new(config: AbstractConfig) -> Self {
        NumberConverterConfig { config }
    }

    /// Returns the converter type.
    pub fn type_config(&self) -> Option<&str> {
        self.config.get_string(TYPE_CONFIG)
    }
}
