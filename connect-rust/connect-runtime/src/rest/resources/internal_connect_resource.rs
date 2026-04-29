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

//! InternalConnectResource - Internal endpoints for connector operations.
//!
//! Extends InternalClusterResource to provide internal connector endpoints.
//! Corresponds to `org.apache.kafka.connect.runtime.rest.resources.InternalConnectResource` in Java.

use std::sync::Arc;

use crate::rest::rest_client::RestClient;
use crate::rest::rest_request_timeout::RestRequestTimeout;

use super::internal_cluster_resource::InternalClusterResource;

/// InternalConnectResource - Internal endpoints for connector operations.
///
/// This resource extends InternalClusterResource and provides the herder
/// for handling internal connector requests.
///
/// Corresponds to `InternalConnectResource` in Java.
pub struct InternalConnectResource {
    /// The internal cluster resource base.
    internal_cluster_resource: InternalClusterResource,
}

impl InternalConnectResource {
    /// Creates a new InternalConnectResource.
    ///
    /// # Arguments
    ///
    /// * `rest_client` - REST client for forwarding requests
    /// * `request_timeout` - Timeout configuration
    pub fn new(rest_client: Arc<RestClient>, request_timeout: Arc<dyn RestRequestTimeout>) -> Self {
        InternalConnectResource {
            internal_cluster_resource: InternalClusterResource::new(rest_client, request_timeout),
        }
    }

    /// Creates a new InternalConnectResource for testing.
    pub fn new_for_test() -> Self {
        let rest_client = Arc::new(RestClient::new("http://localhost:8083".to_string()));
        let timeout =
            Arc::new(crate::rest::rest_request_timeout::ConstantRequestTimeout::default());
        InternalConnectResource::new(rest_client, timeout)
    }

    /// Returns the internal cluster resource.
    pub fn internal_cluster_resource(&self) -> &InternalClusterResource {
        &self.internal_cluster_resource
    }

    /// Returns the request handler.
    pub fn request_handler(&self) -> &crate::rest::herder_request_handler::HerderRequestHandler {
        self.internal_cluster_resource.request_handler()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_internal_connect_resource() {
        let resource = InternalConnectResource::new_for_test();
        assert!(resource.request_handler().request_timeout().timeout_ms() > 0);
    }

    #[test]
    fn test_internal_cluster_resource_accessor() {
        let resource = InternalConnectResource::new_for_test();
        let cluster_resource = resource.internal_cluster_resource();
        assert!(
            cluster_resource
                .request_handler()
                .request_timeout()
                .timeout_ms()
                > 0
        );
    }
}
