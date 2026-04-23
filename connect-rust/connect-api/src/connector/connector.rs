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

use crate::components::Versioned;
use crate::connector::ConnectorContext;
use crate::errors::ConnectError;
use common_trait::config::{Config, ConfigDef};
use std::collections::HashMap;

/// Connector trait for connectors.
///
/// This corresponds to `org.apache.kafka.connect.connector.Connector` in Java.
pub trait Connector: Versioned {
    /// Returns the context for this connector.
    fn context(&self) -> &dyn ConnectorContext;

    /// Initializes this connector with the given context.
    fn initialize(&mut self, context: Box<dyn ConnectorContext>);

    /// Initializes this connector with the given context and task configs.
    /// This version is only used to recover from failures.
    fn initialize_with_task_configs(
        &mut self,
        context: Box<dyn ConnectorContext>,
        task_configs: Vec<HashMap<String, String>>,
    );

    /// Starts this connector with the given properties.
    fn start(&mut self, props: HashMap<String, String>);

    /// Reconfigures this connector.
    /// Default implementation calls stop() followed by start().
    fn reconfigure(&mut self, props: HashMap<String, String>) {
        self.stop();
        self.start(props);
    }

    /// Stops this connector.
    fn stop(&mut self);

    /// Returns the task class for this connector.
    fn task_class(&self) -> &'static str;

    /// Returns the task configurations for this connector.
    fn task_configs(&self, max_tasks: i32) -> Result<Vec<HashMap<String, String>>, ConnectError>;

    /// Validates the configuration for this connector.
    fn validate(&self, configs: HashMap<String, String>) -> Config;

    /// Returns the configuration definition for this connector.
    fn config(&self) -> &'static dyn ConfigDef;
}
