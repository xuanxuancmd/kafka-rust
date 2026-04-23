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

//! HerderRequestHandler - Handler for herder REST requests.
//!
//! This handler processes REST requests that interact with the herder,
//! handling request completion, forwarding, and timeout management.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.HerderRequestHandler` in Java.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::rest_client::{HttpResponse, RestClient};
use super::rest_request_timeout::RestRequestTimeout;
use super::errors::{ConnectRestException, HttpStatus};

/// Translator trait for converting HTTP responses.
///
/// Used to transform responses from forwarded requests.
///
/// Corresponds to `Translator` interface in Java's HerderRequestHandler.
pub trait Translator<T, U>: Send {
    /// Translates an HTTP response to the target type.
    fn translate(&self, response: HttpResponse<U>) -> T;
}

/// Identity translator that returns the response body directly.
///
/// Corresponds to `IdentityTranslator` in Java.
pub struct IdentityTranslator<T>(std::marker::PhantomData<T>);

impl<T> IdentityTranslator<T> {
    /// Creates a new IdentityTranslator.
    pub fn new() -> Self {
        IdentityTranslator(std::marker::PhantomData)
    }
}

impl<T> Default for IdentityTranslator<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Default> Translator<T, T> for IdentityTranslator<T> {
    fn translate(&self, response: HttpResponse<T>) -> T {
        response.body.unwrap_or_default()
    }
}

/// Error for request targeting issues.
///
/// Indicates that the request should be forwarded to another worker.
#[derive(Debug)]
pub struct RequestTargetException {
    /// The URL to forward the request to.
    forward_url: Option<String>,
}

impl RequestTargetException {
    /// Creates a new RequestTargetException with the forward URL.
    pub fn new(forward_url: Option<String>) -> Self {
        RequestTargetException { forward_url }
    }

    /// Returns the URL to forward the request to.
    pub fn forward_url(&self) -> Option<&str> {
        self.forward_url.as_deref()
    }
}

impl std::fmt::Display for RequestTargetException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.forward_url {
            Some(url) => write!(f, "Request should be forwarded to: {}", url),
            None => write!(f, "Request should be forwarded, but no target URL available"),
        }
    }
}

impl std::error::Error for RequestTargetException {}

/// Error indicating that a rebalance is needed.
#[derive(Debug)]
pub struct RebalanceNeededException {
    message: String,
}

impl RebalanceNeededException {
    /// Creates a new RebalanceNeededException.
    pub fn new(message: String) -> Self {
        RebalanceNeededException { message }
    }
}

impl std::fmt::Display for RebalanceNeededException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Rebalance needed: {}", self.message)
    }
}

impl std::error::Error for RebalanceNeededException {}

/// Stage information for timeout exceptions.
#[derive(Debug, Clone)]
pub struct Stage {
    /// Stage name.
    name: String,
    /// Stage description.
    description: Option<String>,
}

impl Stage {
    /// Creates a new Stage.
    pub fn new(name: String, description: Option<String>) -> Self {
        Stage { name, description }
    }

    /// Returns a summary of the stage.
    pub fn summarize(&self) -> String {
        match &self.description {
            Some(desc) => format!("Stage '{}': {}", self.name, desc),
            None => format!("Stage '{}'", self.name),
        }
    }
}

/// Staged timeout exception with stage information.
#[derive(Debug)]
pub struct StagedTimeoutException {
    stage: Stage,
}

impl StagedTimeoutException {
    /// Creates a new StagedTimeoutException.
    pub fn new(stage: Stage) -> Self {
        StagedTimeoutException { stage }
    }

    /// Returns the stage that caused the timeout.
    pub fn stage(&self) -> &Stage {
        &self.stage
    }
}

impl std::fmt::Display for StagedTimeoutException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Timeout at {}", self.stage.summarize())
    }
}

impl std::error::Error for StagedTimeoutException {}

/// HerderRequestHandler - Handles herder REST requests.
///
/// This handler provides methods to:
/// - Complete requests with timeout handling
/// - Forward requests to other workers when needed
/// - Handle rebalance and targeting exceptions
///
/// Corresponds to `HerderRequestHandler` in Java.
pub struct HerderRequestHandler {
    /// REST client for forwarding requests.
    rest_client: Arc<RestClient>,
    /// Request timeout configuration.
    request_timeout: Arc<dyn RestRequestTimeout>,
}

impl HerderRequestHandler {
    /// Creates a new HerderRequestHandler.
    ///
    /// # Arguments
    ///
    /// * `rest_client` - REST client for inter-worker communication
    /// * `request_timeout` - Timeout configuration
    pub fn new(rest_client: Arc<RestClient>, request_timeout: Arc<dyn RestRequestTimeout>) -> Self {
        HerderRequestHandler {
            rest_client,
            request_timeout,
        }
    }

    /// Waits for a FutureCallback to complete and returns the result.
    ///
    /// This is a simplified Rust version since we don't have Java's FutureCallback.
    /// In Rust, we use async functions directly.
    ///
    /// # Errors
    ///
    /// Returns ConnectRestException for timeouts, interruptions, or execution errors.
    pub async fn complete_request<T>(
        &self,
        future_result: impl std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>,
    ) -> Result<T, ConnectRestException> {
        let timeout = Duration::from_millis(self.request_timeout.timeout_ms());
        
        match tokio::time::timeout(timeout, future_result).await {
            Ok(result) => {
                match result {
                    Ok(value) => Ok(value),
                    Err(e) => {
                        // Check if it's a specific exception type
                        let err_str = e.to_string();
                        if err_str.contains("Request should be forwarded") {
                            // This would be a RequestTargetException, but we handle it differently
                            Err(ConnectRestException::from_status(
                                HttpStatus::Conflict,
                                err_str,
                            ))
                        } else if err_str.contains("Rebalance") {
                            Err(ConnectRestException::from_status(
                                HttpStatus::Conflict,
                                err_str,
                            ))
                        } else {
                            Err(ConnectRestException::from_status(
                                HttpStatus::InternalServerError,
                                err_str,
                            ))
                        }
                    }
                }
            }
            Err(_) => {
                Err(ConnectRestException::from_status(
                    HttpStatus::InternalServerError,
                    "Request timed out",
                ))
            }
        }
    }

    /// Completes a request or forwards it to another worker.
    ///
    /// If the request succeeds, returns the result.
    /// If it fails with RequestTargetException, forwards to the indicated target.
    /// If it fails with RebalanceNeededException, returns a CONFLICT error.
    ///
    /// # Arguments
    ///
    /// * `future_result` - The async result to wait for
    /// * `path` - The REST path for the request
    /// * `method` - HTTP method (GET, POST, PUT, DELETE)
    /// * `headers` - HTTP headers
    /// * `query_params` - Query parameters
    /// * `body` - Request body (optional)
    /// * `forward` - Whether to allow forwarding (null = recursive forward allowed)
    /// * `translator` - Translator for converting forwarded response
    pub async fn complete_or_forward_request<T, U, F, Tr>(
        &self,
        future_result: F,
        path: &str,
        method: &str,
        headers: HashMap<String, String>,
        query_params: Option<HashMap<String, String>>,
        body: Option<&str>,
        forward: Option<bool>,
        translator: Tr,
    ) -> Result<T, ConnectRestException>
    where
        F: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>,
        U: serde::de::DeserializeOwned + Send,
        Tr: Translator<T, U>,
    {
        // First, try to complete the request directly
        let timeout = Duration::from_millis(self.request_timeout.timeout_ms());
        
        let direct_result = tokio::time::timeout(timeout, future_result).await;
        
        match direct_result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => {
                let err_str = e.to_string();
                
                // Check for RequestTargetException
                if err_str.contains("forward") || err_str.contains("target") {
                    self.handle_request_target_exception(
                        path, method, headers, query_params, body, forward, translator, &err_str
                    ).await
                } else if err_str.contains("Rebalance") || err_str.contains("stale configuration") {
                    Err(ConnectRestException::from_status(
                        HttpStatus::Conflict,
                        "Cannot complete request momentarily due to stale configuration (typically caused by a concurrent config change)",
                    ))
                } else {
                    Err(ConnectRestException::from_status(
                        HttpStatus::InternalServerError,
                        err_str,
                    ))
                }
            }
            Err(_) => {
                Err(ConnectRestException::from_status(
                    HttpStatus::InternalServerError,
                    "Request timed out",
                ))
            }
        }
    }

    /// Handles RequestTargetException by forwarding the request.
    async fn handle_request_target_exception<T, U, Tr>(
        &self,
        path: &str,
        method: &str,
        headers: HashMap<String, String>,
        query_params: Option<HashMap<String, String>>,
        body: Option<&str>,
        forward: Option<bool>,
        translator: Tr,
        err_str: &str,
    ) -> Result<T, ConnectRestException>
    where
        U: serde::de::DeserializeOwned + Send,
        Tr: Translator<T, U>,
    {
        // If forward is false, don't forward
        if forward == Some(false) {
            return Err(ConnectRestException::from_status(
                HttpStatus::Conflict,
                "Cannot complete request because of a conflicting operation (e.g. worker rebalance)",
            ));
        }

        // Extract forward URL from error message or return CONFLICT if no URL
        let forward_url = self.extract_forward_url(err_str);
        
        if forward_url.is_none() {
            return Err(ConnectRestException::from_status(
                HttpStatus::Conflict,
                "Cannot complete request momentarily due to no known leader URL, likely because a rebalance was underway.",
            ));
        }

        // Build the forward URL with path and query params
        let forward_url = forward_url.unwrap();
        let full_forward_url = self.build_forward_url(&forward_url, path, query_params, forward);
        
        // Forward the request
        self.forward_request(&full_forward_url, method, headers, body, translator).await
    }

    /// Extracts the forward URL from an error message.
    fn extract_forward_url(&self, err_str: &str) -> Option<String> {
        // Simple extraction - look for URL patterns in the error message
        // In real implementation, this would come from RequestTargetException.forward_url()
        if err_str.contains("http://") || err_str.contains("https://") {
            // Try to extract URL from message
            let start = err_str.find("http://").or_else(|| err_str.find("https://"));
            if let Some(s) = start {
                let rest = &err_str[s..];
                let end = rest.find(' ').unwrap_or(rest.len());
                Some(rest[..end].to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Builds the full forward URL with path and query parameters.
    fn build_forward_url(
        &self,
        base_url: &str,
        path: &str,
        query_params: Option<HashMap<String, String>>,
        forward: Option<bool>,
    ) -> String {
        let mut url = format!("{}{}", base_url.trim_end_matches('/'), path);
        
        // Add forward query parameter
        let recursive_forward = forward.is_none();
        url = format!("{}?forward={}", url, recursive_forward);
        
        // Add additional query parameters
        if let Some(params) = query_params {
            for (key, value) in params {
                url = format!("{}&{}={}", url, key, value);
            }
        }
        
        url
    }

    /// Forwards a request to another worker.
    async fn forward_request<T, U, Tr>(
        &self,
        url: &str,
        method: &str,
        headers: HashMap<String, String>,
        body: Option<&str>,
        translator: Tr,
    ) -> Result<T, ConnectRestException>
    where
        U: serde::de::DeserializeOwned + Send,
        Tr: Translator<T, U>,
    {
        log::debug!("Forwarding request to {} {}", url, method);
        
        let http_method = match method {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            "PUT" => reqwest::Method::PUT,
            "DELETE" => reqwest::Method::DELETE,
            _ => reqwest::Method::GET,
        };
        
        let result = self.rest_client.send_raw(
            http_method,
            url,
            body.map(|s| s.to_string()),
            Some(headers),
        ).await;
        
        match result {
            Ok((response_text, status)) => {
                if status >= 200 && status < 300 {
                    if status == 204 {
                        // No content - need to handle specially
                        // This would fail for types that require content
                        Err(ConnectRestException::from_status(
                            HttpStatus::InternalServerError,
                            "Forwarded request returned 204 No Content",
                        ))
                    } else {
                        let parsed: U = serde_json::from_str(&response_text)
                            .map_err(|e| ConnectRestException::from_status(
                                HttpStatus::InternalServerError,
                                format!("Failed to parse response: {}", e),
                            ))?;
                        Ok(translator.translate(HttpResponse::new(Some(parsed), status)))
                    }
                } else {
                    Err(ConnectRestException::with_status(
                        status,
                        response_text,
                        None,
                    ))
                }
            }
            Err(e) => {
                Err(ConnectRestException::from_status(
                    HttpStatus::InternalServerError,
                    format!("Forward request failed: {}", e),
                ))
            }
        }
    }

    /// Returns the request timeout.
    pub fn request_timeout(&self) -> &dyn RestRequestTimeout {
        self.request_timeout.as_ref()
    }

    /// Returns the REST client.
    pub fn rest_client(&self) -> &RestClient {
        self.rest_client.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::rest_request_timeout::ConstantRequestTimeout;

    #[test]
    fn test_identity_translator() {
        let response = HttpResponse::new(Some("test data".to_string()), 200);
        let translator = IdentityTranslator::<String>::new();
        let result = translator.translate(response);
        assert_eq!(result, "test data");
    }

    #[test]
    fn test_identity_translator_empty_body() {
        // Verify Default behavior for empty body - returns default value instead of panic
        let response = HttpResponse::<String>::new(None, 204);
        let translator = IdentityTranslator::<String>::new();
        let result = translator.translate(response);
        assert_eq!(result, ""); // String::default() is empty string
    }

    #[test]
    fn test_request_target_exception() {
        let ex = RequestTargetException::new(Some("http://worker2:8083".to_string()));
        assert_eq!(ex.forward_url(), Some("http://worker2:8083"));
        
        let ex_no_url = RequestTargetException::new(None);
        assert!(ex_no_url.forward_url().is_none());
    }

    #[test]
    fn test_rebalance_needed_exception() {
        let ex = RebalanceNeededException::new("Rebalance needed".to_string());
        assert!(ex.to_string().contains("Rebalance"));
    }

    #[test]
    fn test_stage() {
        let stage = Stage::new("request".to_string(), Some("waiting for herder".to_string()));
        let summary = stage.summarize();
        assert!(summary.contains("request"));
        assert!(summary.contains("waiting for herder"));
    }

    #[test]
    fn test_staged_timeout_exception() {
        let stage = Stage::new("timeout_stage".to_string(), None);
        let ex = StagedTimeoutException::new(stage);
        assert!(ex.to_string().contains("timeout_stage"));
    }

    #[tokio::test]
    async fn test_complete_request_success() {
        let rest_client = Arc::new(RestClient::new("http://localhost:8083".to_string()));
        let timeout = Arc::new(ConstantRequestTimeout::new(30_000, 10_000));
        let handler = HerderRequestHandler::new(rest_client, timeout);
        
        let future = async { Ok::<String, Box<dyn std::error::Error + Send + Sync>>("success".to_string()) };
        let result = handler.complete_request(future).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
    }

    #[tokio::test]
    async fn test_complete_request_error() {
        let rest_client = Arc::new(RestClient::new("http://localhost:8083".to_string()));
        let timeout = Arc::new(ConstantRequestTimeout::new(30_000, 10_000));
        let handler = HerderRequestHandler::new(rest_client, timeout);
        
        let future = async { Err::<String, Box<dyn std::error::Error + Send + Sync>>(
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, "test error"))
        ) };
        let result = handler.complete_request(future).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status_code(), 500);
    }

    #[test]
    fn test_build_forward_url() {
        let rest_client = Arc::new(RestClient::new("http://localhost:8083".to_string()));
        let timeout = Arc::new(ConstantRequestTimeout::new(30_000, 10_000));
        let handler = HerderRequestHandler::new(rest_client, timeout);
        
        let url = handler.build_forward_url(
            "http://worker2:8083",
            "/connectors/test/tasks",
            Some(HashMap::from([("sync".to_string(), "true".to_string())])),
            None, // recursive forward
        );
        
        assert!(url.contains("/connectors/test/tasks"));
        assert!(url.contains("forward=true"));
        assert!(url.contains("sync=true"));
    }
}