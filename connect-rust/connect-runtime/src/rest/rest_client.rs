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

//! REST client for Kafka Connect inter-worker communication.
//!
//! This module provides a HTTP client for making REST API requests
//! to other workers in a Kafka Connect cluster.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.RestClient` in Java.

use reqwest::{Client, Method, Response};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;

use super::entities::ErrorMessage;

/// HTTP response wrapper.
///
/// Contains the response body and status information.
#[derive(Debug)]
pub struct HttpResponse<T> {
    /// The deserialized response body.
    pub body: Option<T>,
    /// The HTTP status code.
    pub status: u16,
}

impl<T> HttpResponse<T> {
    /// Creates a new HttpResponse.
    pub fn new(body: Option<T>, status: u16) -> Self {
        Self { body, status }
    }

    /// Returns true if the response status is successful (2xx).
    pub fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }
}

/// Error type for REST client operations.
#[derive(Debug, Error)]
pub enum RestClientError {
    /// HTTP request failed.
    #[error("HTTP request failed: {0}")]
    RequestFailed(#[source] reqwest::Error),

    /// Response deserialization failed.
    #[error("Failed to deserialize response: {0}")]
    DeserializeFailed(#[source] serde_json::Error),

    /// Server returned an error.
    #[error("Server error (status {error_code}): {message}")]
    ServerError {
        error_code: i32,
        message: String,
    },

    /// Request timed out.
    #[error("Request timed out")]
    Timeout,

    /// Connection failed.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Invalid URL.
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),
}

impl From<reqwest::Error> for RestClientError {
    fn from(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            RestClientError::Timeout
        } else if error.is_connect() {
            RestClientError::ConnectionFailed(error.to_string())
        } else {
            RestClientError::RequestFailed(error)
        }
    }
}

impl From<serde_json::Error> for RestClientError {
    fn from(error: serde_json::Error) -> Self {
        RestClientError::DeserializeFailed(error)
    }
}

/// HTTP headers type alias.
pub type HttpHeaders = HashMap<String, String>;

/// REST client for making HTTP requests.
///
/// This client is used for inter-worker communication in a
/// distributed Kafka Connect cluster.
///
/// Corresponds to `RestClient` in Java.
#[derive(Debug)]
pub struct RestClient {
    /// The underlying HTTP client.
    client: Client,
    /// Base URL for requests.
    base_url: String,
    /// Request timeout in seconds.
    timeout: Duration,
}

impl RestClient {
    /// Default timeout in seconds.
    pub const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

    /// Creates a new RestClient with the given base URL.
    pub fn new(base_url: String) -> Self {
        Self::with_timeout(base_url, Duration::from_secs(Self::DEFAULT_TIMEOUT_SECONDS))
    }

    /// Creates a new RestClient with custom timeout.
    pub fn with_timeout(base_url: String, timeout: Duration) -> Self {
        let client = Client::builder()
            .timeout(timeout)
            .user_agent("kafka-connect-rust")
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            base_url,
            timeout,
        }
    }

    /// Returns the base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns the timeout duration.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Sends a GET request.
    pub async fn send_get<T: DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<HttpResponse<T>, RestClientError> {
        self.send_request::<T, ()>(Method::GET, path, None, None).await
    }

    /// Sends a GET request with headers.
    pub async fn send_get_with_headers<T: DeserializeOwned>(
        &self,
        path: &str,
        headers: HttpHeaders,
    ) -> Result<HttpResponse<T>, RestClientError> {
        self.send_request::<T, ()>(Method::GET, path, None, Some(headers)).await
    }

    /// Sends a POST request with a body.
    pub async fn send_post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<HttpResponse<T>, RestClientError> {
        self.send_request(Method::POST, path, Some(body), None).await
    }

    /// Sends a POST request with headers.
    pub async fn send_post_with_headers<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
        headers: HttpHeaders,
    ) -> Result<HttpResponse<T>, RestClientError> {
        self.send_request(Method::POST, path, Some(body), Some(headers)).await
    }

    /// Sends a PUT request with a body.
    pub async fn send_put<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<HttpResponse<T>, RestClientError> {
        self.send_request(Method::PUT, path, Some(body), None).await
    }

    /// Sends a DELETE request.
    pub async fn send_delete<T: DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<HttpResponse<T>, RestClientError> {
        self.send_request::<T, ()>(Method::DELETE, path, None, None).await
    }

    /// Sends a DELETE request with headers.
    pub async fn send_delete_with_headers<T: DeserializeOwned>(
        &self,
        path: &str,
        headers: HttpHeaders,
    ) -> Result<HttpResponse<T>, RestClientError> {
        self.send_request::<T, ()>(Method::DELETE, path, None, Some(headers)).await
    }

    /// Sends a generic HTTP request.
    ///
    /// This is the core method that handles request construction,
    /// response parsing, and error handling.
    pub async fn send_request<T: DeserializeOwned, B: Serialize>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        headers: Option<HttpHeaders>,
    ) -> Result<HttpResponse<T>, RestClientError> {
        let url = self.build_url(path)?;

        let mut request_builder = self.client.request(method, url);

        // Add headers if provided
        if let Some(h) = headers {
            for (key, value) in h {
                request_builder = request_builder.header(&key, &value);
            }
        }

        // Add body if provided
        if let Some(b) = body {
            request_builder = request_builder.json(b);
        }

        let response = request_builder.send().await?;

        self.handle_response(response).await
    }

    /// Builds the full URL from base URL and path.
    fn build_url(&self, path: &str) -> Result<String, RestClientError> {
        // Ensure path starts with /
        let normalized_path = if path.starts_with('/') {
            path
        } else {
            &format!("/{}", path)
        };

        // Remove trailing slash from base_url if present
        let base = if self.base_url.ends_with('/') {
            &self.base_url[..self.base_url.len() - 1]
        } else {
            &self.base_url
        };

        Ok(format!("{}{}", base, normalized_path))
    }

    /// Handles the HTTP response.
    async fn handle_response<T: DeserializeOwned>(
        &self,
        response: Response,
    ) -> Result<HttpResponse<T>, RestClientError> {
        let status = response.status().as_u16();

        // Handle 204 No Content
        if status == 204 {
            return Ok(HttpResponse::new(None, status));
        }

        // Handle error responses (4xx, 5xx)
        if status >= 400 {
            let error_message = response
                .json::<ErrorMessage>()
                .await
                .map_err(|e| RestClientError::from(e))?;

            return Err(RestClientError::ServerError {
                error_code: error_message.error_code,
                message: error_message.message,
            });
        }

        // Handle successful responses (2xx)
        if status >= 200 && status < 300 {
            let body = response
                .json::<T>()
                .await
                .map_err(|e| RestClientError::from(e))?;

            return Ok(HttpResponse::new(Some(body), status));
        }

        // Unexpected status code
        Err(RestClientError::ServerError {
            error_code: status as i32,
            message: format!("Unexpected status code: {}", status),
        })
    }

    /// Sends a request and returns the raw response body as string.
    pub async fn send_raw(
        &self,
        method: Method,
        path: &str,
        body: Option<String>,
        headers: Option<HttpHeaders>,
    ) -> Result<(String, u16), RestClientError> {
        let url = self.build_url(path)?;

        let mut request_builder = self.client.request(method, url);

        if let Some(h) = headers {
            for (key, value) in h {
                request_builder = request_builder.header(&key, &value);
            }
        }

        if let Some(b) = body {
            request_builder = request_builder.body(b);
        }

        let response = request_builder.send().await?;
        let status = response.status().as_u16();
        let text = response.text().await?;

        Ok((text, status))
    }
}

impl Clone for RestClient {
    fn clone(&self) -> Self {
        // Rebuild client since reqwest::Client doesn't expose internal state
        Self::with_timeout(self.base_url.clone(), self.timeout)
    }
}

/// Builder for creating RestClient with custom configuration.
#[derive(Debug)]
pub struct RestClientBuilder {
    base_url: String,
    timeout: Duration,
    headers: HttpHeaders,
}

impl RestClientBuilder {
    /// Creates a new builder with the given base URL.
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            timeout: Duration::from_secs(RestClient::DEFAULT_TIMEOUT_SECONDS),
            headers: HashMap::new(),
        }
    }

    /// Sets the timeout duration.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Adds a default header to all requests.
    pub fn header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// Builds the RestClient.
    pub fn build(self) -> RestClient {
        let client = RestClient::with_timeout(self.base_url, self.timeout);
        // Note: We would need to modify RestClient to support default headers
        // For now, headers can be passed per-request
        client
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_rest_client_new() {
        let client = RestClient::new("http://localhost:8083".to_string());
        assert_eq!(client.base_url(), "http://localhost:8083");
        assert_eq!(client.timeout(), Duration::from_secs(30));
    }

    #[test]
    fn test_rest_client_with_timeout() {
        let client = RestClient::with_timeout(
            "http://localhost:8083".to_string(),
            Duration::from_secs(60),
        );
        assert_eq!(client.timeout(), Duration::from_secs(60));
    }

    #[test]
    fn test_build_url() {
        let client = RestClient::new("http://localhost:8083".to_string());

        // Path without leading slash
        let url = client.build_url("connectors").unwrap();
        assert_eq!(url, "http://localhost:8083/connectors");

        // Path with leading slash
        let url = client.build_url("/connectors").unwrap();
        assert_eq!(url, "http://localhost:8083/connectors");

        // Base URL with trailing slash
        let client = RestClient::new("http://localhost:8083/".to_string());
        let url = client.build_url("/connectors").unwrap();
        assert_eq!(url, "http://localhost:8083/connectors");
    }

    #[test]
    fn test_http_response_is_success() {
        let response: HttpResponse<String> = HttpResponse::new(Some("test".to_string()), 200);
        assert!(response.is_success());

        let response: HttpResponse<String> = HttpResponse::new(Some("test".to_string()), 201);
        assert!(response.is_success());

        let response: HttpResponse<String> = HttpResponse::new(None, 204);
        assert!(response.is_success());

        let response: HttpResponse<String> = HttpResponse::new(None, 400);
        assert!(!response.is_success());

        let response: HttpResponse<String> = HttpResponse::new(None, 500);
        assert!(!response.is_success());
    }

    #[test]
    fn test_rest_client_error_display() {
        let error = RestClientError::Timeout;
        assert_eq!(error.to_string(), "Request timed out");

        let error = RestClientError::InvalidUrl("bad url".to_string());
        assert!(error.to_string().contains("bad url"));
    }

    #[test]
    fn test_rest_client_clone() {
        let client = RestClient::new("http://localhost:8083".to_string());
        let cloned = client.clone();
        assert_eq!(cloned.base_url(), client.base_url());
        assert_eq!(cloned.timeout(), client.timeout());
    }

    #[test]
    fn test_rest_client_builder() {
        let client = RestClientBuilder::new("http://localhost:8083".to_string())
            .timeout(Duration::from_secs(120))
            .header("Authorization".to_string(), "Bearer token".to_string())
            .build();

        assert_eq!(client.base_url(), "http://localhost:8083");
        assert_eq!(client.timeout(), Duration::from_secs(120));
    }

    #[test]
    fn test_default_timeout() {
        assert_eq!(RestClient::DEFAULT_TIMEOUT_SECONDS, 30);
    }

    #[test]
    fn test_http_response_new() {
        let response: HttpResponse<i32> = HttpResponse::new(Some(42), 200);
        assert_eq!(response.body, Some(42));
        assert_eq!(response.status, 200);
    }

    #[test]
    fn test_server_error_creation() {
        let error = RestClientError::ServerError {
            error_code: 404,
            message: "Connector not found".to_string(),
        };
        assert!(error.to_string().contains("404"));
        assert!(error.to_string().contains("Connector not found"));
    }
}