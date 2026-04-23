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

//! ConnectRestException - REST exception with HTTP status code and error code.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.errors.ConnectRestException` in Java.

use std::error::Error;
use std::fmt;

use connect_api::errors::ConnectException;

/// HTTP status codes used in REST responses.
///
/// These correspond to jakarta.ws.rs.core.Response.Status in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpStatus {
    /// 200 OK
    Ok,
    /// 201 Created
    Created,
    /// 202 Accepted
    Accepted,
    /// 204 No Content
    NoContent,
    /// 400 Bad Request
    BadRequest,
    /// 401 Unauthorized
    Unauthorized,
    /// 403 Forbidden
    Forbidden,
    /// 404 Not Found
    NotFound,
    /// 409 Conflict
    Conflict,
    /// 415 Unsupported Media Type
    UnsupportedMediaType,
    /// 500 Internal Server Error
    InternalServerError,
    /// 503 Service Unavailable
    ServiceUnavailable,
}

impl HttpStatus {
    /// Returns the HTTP status code as a number.
    pub fn code(&self) -> u16 {
        match self {
            HttpStatus::Ok => 200,
            HttpStatus::Created => 201,
            HttpStatus::Accepted => 202,
            HttpStatus::NoContent => 204,
            HttpStatus::BadRequest => 400,
            HttpStatus::Unauthorized => 401,
            HttpStatus::Forbidden => 403,
            HttpStatus::NotFound => 404,
            HttpStatus::Conflict => 409,
            HttpStatus::UnsupportedMediaType => 415,
            HttpStatus::InternalServerError => 500,
            HttpStatus::ServiceUnavailable => 503,
        }
    }

    /// Returns the reason phrase for this status.
    pub fn reason(&self) -> &'static str {
        match self {
            HttpStatus::Ok => "OK",
            HttpStatus::Created => "Created",
            HttpStatus::Accepted => "Accepted",
            HttpStatus::NoContent => "No Content",
            HttpStatus::BadRequest => "Bad Request",
            HttpStatus::Unauthorized => "Unauthorized",
            HttpStatus::Forbidden => "Forbidden",
            HttpStatus::NotFound => "Not Found",
            HttpStatus::Conflict => "Conflict",
            HttpStatus::UnsupportedMediaType => "Unsupported Media Type",
            HttpStatus::InternalServerError => "Internal Server Error",
            HttpStatus::ServiceUnavailable => "Service Unavailable",
        }
    }
}

impl fmt::Display for HttpStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.code(), self.reason())
    }
}

/// ConnectRestException - REST exception with HTTP status code and error code.
///
/// This exception is used for REST API errors and includes:
/// - HTTP status code (e.g., 400, 404, 500)
/// - Error code (application-specific code, often same as status code)
///
/// Corresponds to `org.apache.kafka.connect.runtime.rest.errors.ConnectRestException` in Java.
#[derive(Debug)]
pub struct ConnectRestException {
    /// The HTTP status code.
    status_code: u16,
    /// The application error code.
    error_code: i32,
    /// The error message.
    message: String,
    /// Optional cause.
    cause: Option<Box<dyn Error + Send + Sync>>,
}

impl ConnectRestException {
    /// Creates a new ConnectRestException with all parameters.
    ///
    /// # Arguments
    ///
    /// * `status_code` - HTTP status code
    /// * `error_code` - Application error code
    /// * `message` - Error message
    /// * `cause` - Optional cause
    pub fn new(
        status_code: u16,
        error_code: i32,
        message: impl Into<String>,
        cause: Option<Box<dyn Error + Send + Sync>>,
    ) -> Self {
        ConnectRestException {
            status_code,
            error_code,
            message: message.into(),
            cause,
        }
    }

    /// Creates a ConnectRestException from an HttpStatus.
    ///
    /// Uses the status code as both HTTP status and error code.
    pub fn from_status(status: HttpStatus, message: impl Into<String>) -> Self {
        let code = status.code();
        ConnectRestException::new(code, code as i32, message, None)
    }

    /// Creates a ConnectRestException from an HttpStatus with a cause.
    ///
    /// Uses the status code as both HTTP status and error code.
    pub fn from_status_with_cause(
        status: HttpStatus,
        message: impl Into<String>,
        cause: Box<dyn Error + Send + Sync>,
    ) -> Self {
        let code = status.code();
        ConnectRestException::new(code, code as i32, message, Some(cause))
    }

    /// Creates a ConnectRestException with custom error code.
    ///
    /// # Arguments
    ///
    /// * `status_code` - HTTP status code
    /// * `message` - Error message
    /// * `cause` - Optional cause
    ///
    /// Uses status_code as the error_code.
    pub fn with_status(
        status_code: u16,
        message: impl Into<String>,
        cause: Option<Box<dyn Error + Send + Sync>>,
    ) -> Self {
        ConnectRestException::new(status_code, status_code as i32, message, cause)
    }

    /// Returns the HTTP status code.
    pub fn status_code(&self) -> u16 {
        self.status_code
    }

    /// Returns the application error code.
    pub fn error_code(&self) -> i32 {
        self.error_code
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the cause, if any.
    pub fn cause(&self) -> Option<&(dyn Error + Send + Sync)> {
        self.cause.as_deref()
    }
}

impl fmt::Display for ConnectRestException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConnectRestException: status={}, error_code={}, message={}",
            self.status_code, self.error_code, self.message
        )
    }
}

impl Error for ConnectRestException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause
            .as_ref()
            .map(|c| c.as_ref() as &(dyn Error + 'static))
    }
}

impl From<ConnectRestException> for ConnectException {
    fn from(e: ConnectRestException) -> Self {
        ConnectException::new(e.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_status() {
        let ex = ConnectRestException::from_status(HttpStatus::BadRequest, "Invalid request");
        assert_eq!(ex.status_code(), 400);
        assert_eq!(ex.error_code(), 400);
        assert_eq!(ex.message(), "Invalid request");
    }

    #[test]
    fn test_with_status() {
        let ex = ConnectRestException::with_status(404, "Not found", None);
        assert_eq!(ex.status_code(), 404);
        assert_eq!(ex.error_code(), 404);
    }

    #[test]
    fn test_custom_error_code() {
        let ex = ConnectRestException::new(400, 1001, "Custom error", None);
        assert_eq!(ex.status_code(), 400);
        assert_eq!(ex.error_code(), 1001);
    }

    #[test]
    fn test_display() {
        let ex = ConnectRestException::from_status(HttpStatus::NotFound, "Resource not found");
        let display = format!("{}", ex);
        assert!(display.contains("status=404"));
        assert!(display.contains("error_code=404"));
        assert!(display.contains("Resource not found"));
    }

    #[test]
    fn test_http_status() {
        assert_eq!(HttpStatus::BadRequest.code(), 400);
        assert_eq!(HttpStatus::NotFound.code(), 404);
        assert_eq!(HttpStatus::InternalServerError.code(), 500);
        assert_eq!(HttpStatus::BadRequest.reason(), "Bad Request");
    }
}
