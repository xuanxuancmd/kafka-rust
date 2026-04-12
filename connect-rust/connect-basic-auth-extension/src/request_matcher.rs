// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
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

//! Request matcher
//!
//! This module provides request matching for internal request bypass.

use regex::Regex;

/// Request matcher
///
/// Matches HTTP requests against method and path pattern.
/// Used to identify internal requests that should bypass authentication.
#[derive(Debug, Clone)]
pub struct RequestMatcher {
    method: String,
    path: Regex,
}

impl RequestMatcher {
    /// Create a new RequestMatcher
    ///
    /// # Arguments
    /// * `method` - HTTP method (GET, POST, PUT, etc.)
    /// * `path_pattern` - Regex pattern for path matching
    ///
    /// # Returns
    /// * RequestMatcher instance
    ///
    /// # Errors
    /// * `regex::Error` - Invalid regex pattern
    pub fn new(method: &str, path_pattern: &str) -> Result<Self, regex::Error> {
        Ok(RequestMatcher {
            method: method.to_string(),
            path: Regex::new(path_pattern)?,
        })
    }

    /// Test if request matches
    ///
    /// # Arguments
    /// * `request_method` - HTTP method of the request
    /// * `request_path` - Path of the request
    ///
    /// # Returns
    /// * `true` if request matches, `false` otherwise
    pub fn test(&self, request_method: &str, request_path: &str) -> bool {
        self.method == request_method && self.path.is_match(request_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_matcher_post_tasks() {
        let matcher = RequestMatcher::new("POST", r"/?connectors/([^/]+)/tasks/?").unwrap();
        assert!(matcher.test("POST", "/connectors/my-connector/tasks"));
        assert!(matcher.test("POST", "/connectors/my-connector/tasks/"));
        assert!(!matcher.test("GET", "/connectors/my-connector/tasks"));
        assert!(!matcher.test("POST", "/connectors/my-connector/other"));
    }

    #[test]
    fn test_request_matcher_put_fence() {
        let matcher = RequestMatcher::new("PUT", r"/?connectors/[^/]+/fence/?").unwrap();
        assert!(matcher.test("PUT", "/connectors/my-connector/fence"));
        assert!(matcher.test("PUT", "/connectors/my-connector/fence/"));
        assert!(!matcher.test("GET", "/connectors/my-connector/fence"));
        assert!(!matcher.test("PUT", "/connectors/my-connector/other"));
    }

    #[test]
    fn test_request_matcher_invalid_regex() {
        let result = RequestMatcher::new("GET", r"[invalid(regex");
        assert!(result.is_err());
    }
}
