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

//! ConnectExceptionMapper - maps exceptions to HTTP responses.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper` in Java.

use std::error::Error;

use super::{BadRequestException, ConnectRestException, HttpStatus};
use crate::rest::entities::ErrorMessage;

/// ConnectExceptionMapper - maps uncaught exceptions to HTTP responses.
///
/// This mapper is responsible for converting exceptions thrown during REST request
/// handling into appropriate HTTP responses with error messages.
///
/// Corresponds to `org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper` in Java.
pub struct ConnectExceptionMapper {
    /// The URI path being accessed (for logging).
    path: Option<String>,
}

impl ConnectExceptionMapper {
    /// Creates a new ConnectExceptionMapper.
    pub fn new() -> Self {
        ConnectExceptionMapper { path: None }
    }

    /// Creates a ConnectExceptionMapper with a specific path.
    pub fn with_path(path: impl Into<String>) -> Self {
        ConnectExceptionMapper {
            path: Some(path.into()),
        }
    }

    /// Maps an exception to an HTTP response.
    ///
    /// # Arguments
    ///
    /// * `exception` - The exception to map
    ///
    /// # Returns
    ///
    /// An ErrorResponse containing the status code and error message.
    pub fn to_response(&self, exception: &(dyn Error + 'static)) -> ErrorResponse {
        // Try to downcast to ConnectRestException
        if let Some(rest_ex) = exception.downcast_ref::<ConnectRestException>() {
            return ErrorResponse::new(
                rest_ex.status_code(),
                ErrorMessage::new(rest_ex.error_code(), rest_ex.message().to_string()),
            );
        }

        // Try to downcast to BadRequestException
        if let Some(bad_req) = exception.downcast_ref::<BadRequestException>() {
            return ErrorResponse::new(
                bad_req.status_code(),
                ErrorMessage::new(bad_req.error_code(), bad_req.message().to_string()),
            );
        }

        // Check for NotFoundException (simulated)
        // In a real implementation, we would check for specific exception types
        // For now, we default to Internal Server Error
        ErrorResponse::new(500, ErrorMessage::new(500, exception.to_string()))
    }

    /// Maps an exception to an ErrorResponse, with context path for logging.
    pub fn to_response_with_context(
        &self,
        exception: &(dyn Error + 'static),
        _path: &str,
    ) -> ErrorResponse {
        // Log the exception (in real implementation, would use actual logging)
        // log::debug!("Uncaught exception in REST call to /{}", path, exception);

        // Check for specific exception types
        if let Some(rest_ex) = exception.downcast_ref::<ConnectRestException>() {
            return ErrorResponse::new(
                rest_ex.status_code(),
                ErrorMessage::new(rest_ex.error_code(), rest_ex.message().to_string()),
            );
        }

        if let Some(bad_req) = exception.downcast_ref::<BadRequestException>() {
            return ErrorResponse::new(
                bad_req.status_code(),
                ErrorMessage::new(bad_req.error_code(), bad_req.message().to_string()),
            );
        }

        // Default: Internal Server Error
        ErrorResponse::new(500, ErrorMessage::new(500, exception.to_string()))
    }
}

impl Default for ConnectExceptionMapper {
    fn default() -> Self {
        Self::new()
    }
}

/// ErrorResponse - the response returned by the mapper.
///
/// Contains the HTTP status code and the error message entity.
#[derive(Debug, Clone)]
pub struct ErrorResponse {
    /// HTTP status code.
    status_code: u16,
    /// Error message entity.
    error_message: ErrorMessage,
}

impl ErrorResponse {
    /// Creates a new ErrorResponse.
    pub fn new(status_code: u16, error_message: ErrorMessage) -> Self {
        ErrorResponse {
            status_code,
            error_message,
        }
    }

    /// Returns the HTTP status code.
    pub fn status_code(&self) -> u16 {
        self.status_code
    }

    /// Returns the error message.
    pub fn error_message(&self) -> &ErrorMessage {
        &self.error_message
    }

    /// Converts to HTTP status enum.
    pub fn status(&self) -> HttpStatus {
        match self.status_code {
            200 => HttpStatus::Ok,
            201 => HttpStatus::Created,
            202 => HttpStatus::Accepted,
            204 => HttpStatus::NoContent,
            400 => HttpStatus::BadRequest,
            401 => HttpStatus::Unauthorized,
            403 => HttpStatus::Forbidden,
            404 => HttpStatus::NotFound,
            409 => HttpStatus::Conflict,
            415 => HttpStatus::UnsupportedMediaType,
            500 => HttpStatus::InternalServerError,
            503 => HttpStatus::ServiceUnavailable,
            _ => HttpStatus::InternalServerError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mapper_connect_rest_exception() {
        let mapper = ConnectExceptionMapper::new();
        let ex = ConnectRestException::from_status(HttpStatus::NotFound, "Connector not found");
        let response = mapper.to_response(&ex);

        assert_eq!(response.status_code(), 404);
        assert_eq!(response.error_message().error_code, 404);
        assert_eq!(&response.error_message().message, "Connector not found");
    }

    #[test]
    fn test_mapper_bad_request_exception() {
        let mapper = ConnectExceptionMapper::new();
        let ex = BadRequestException::new("Invalid parameter");
        let response = mapper.to_response(&ex);

        assert_eq!(response.status_code(), 400);
        assert_eq!(response.error_message().error_code, 400);
    }

    #[test]
    fn test_mapper_generic_error() {
        let mapper = ConnectExceptionMapper::new();
        let ex = std::io::Error::new(std::io::ErrorKind::Other, "IO error");
        let response = mapper.to_response(&ex);

        assert_eq!(response.status_code(), 500);
        assert_eq!(response.error_message().error_code, 500);
    }

    #[test]
    fn test_response_status() {
        let response = ErrorResponse::new(404, ErrorMessage::new(404, "Not found".to_string()));
        assert_eq!(response.status(), HttpStatus::NotFound);

        let response = ErrorResponse::new(400, ErrorMessage::new(400, "Bad request".to_string()));
        assert_eq!(response.status(), HttpStatus::BadRequest);
    }
}
