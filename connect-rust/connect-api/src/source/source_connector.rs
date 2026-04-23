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

use crate::connector::Connector;
use crate::errors::ConnectError;
use crate::source::{ConnectorTransactionBoundaries, ExactlyOnceSupport};
use serde_json::Value;
use std::collections::HashMap;

/// SourceConnector trait for source connectors.
///
/// This corresponds to `org.apache.kafka.connect.source.SourceConnector` in Java.
pub trait SourceConnector: Connector {
    /// Returns the exactly once support with the given connector config.
    /// Connector authors should override this to return SUPPORTED or UNSUPPORTED.
    fn exactly_once_support(&self, connector_config: HashMap<String, String>)
        -> ExactlyOnceSupport;

    /// Returns the connector transaction boundaries with the given connector config.
    /// This method is called when transaction.boundary=connector is configured.
    fn can_define_transaction_boundaries(
        &self,
        connector_config: HashMap<String, String>,
    ) -> ConnectorTransactionBoundaries;

    /// Invoked when users request to manually alter/reset the offsets for this connector.
    /// Connectors that manage offsets externally can propagate offset changes in this method.
    /// Returns true if this method has been overridden; false by default.
    fn alter_offsets(
        &self,
        connector_config: HashMap<String, String>,
        offsets: HashMap<HashMap<String, Value>, HashMap<String, Value>>,
    ) -> Result<bool, ConnectError> {
        Ok(false)
    }
}
