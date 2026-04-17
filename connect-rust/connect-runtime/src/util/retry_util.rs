/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::errors::{ConnectException, RetriableException};
use common_trait::errors::{RetriableException as KafkaRetriableException, WakeupException};
use common_trait::time::Time;
use std::time::Duration;
use std::ops::Function;

/// Utility for retrying operations with backoff
pub struct RetryUtil;

impl RetryUtil {
    /// Execute callable at least once, optionally retrying if RetriableException is thrown
    pub async fn retry_until_timeout<T, F>(
        callable: F,
        description: Option<Box<dyn Fn() -> String>>,
        timeout_duration: Option<Duration>,
        retry_backoff_ms: i64,
        time: Arc<dyn Time>,
    ) -> Result<T, Box<dyn std::error::Error + Send>>
    where
        T: Send + 'static,
        F: Fn() -> Result<T, Box<dyn std::error::Error + Send>> + Send + 'static,
    {
        let description_str = description
            .map(|d| d())
            .unwrap_or_else(|| "callable".to_string());

        let timeout_ms = timeout_duration
            .map(|d| d.as_millis())
            .unwrap_or(0);

        let mut retry_backoff_ms = if retry_backoff_ms < 0 {
            log::debug!("Assuming no retry backoff since retryBackoffMs={} is negative", retry_backoff_ms);
            0
        } else {
            retry_backoff_ms
        };

        if timeout_ms <= 0 || retry_backoff_ms >= timeout_ms {
            log::debug!(
                "Executing {} only once, since timeoutMs={} is not larger than retryBackoffMs={}",
                description_str, timeout_ms, retry_backoff_ms
            );
            return callable().await;
        }

        let end = time.milliseconds() + timeout_ms;
        let mut attempt = 0;
        let mut last_error: Option<Box<dyn std::error::Error + Send>> = None;

        loop {
            attempt += 1;
            match callable().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if e.is::<RetriableException>()
                        || e.is::<KafkaRetriableException>()
                        || e.is::<WakeupException>()
                    {
                        log::warn!(
                            "Attempt {} to {} resulted in RetriableException; retrying automatically. Reason: {}",
                            attempt,
                            description_str,
                            e.to_string()
                        );
                        last_error = Some(e);
                    } else {
                        return Err(e);
                    }
                }
            }

            if retry_backoff_ms > 0 {
                let millis_remaining = (end - time.milliseconds()).max(0);
                if millis_remaining < retry_backoff_ms {
                    break;
                }
                time.sleep(retry_backoff_ms).await;
            }

            if time.milliseconds() >= end {
                break;
            }
        }

        let error_msg = if let Some(ref err) = last_error.as_ref() {
            format!("Failed to {} after {} attempts. Reason: {}", description_str, attempt, err.to_string())
        } else {
            format!("Failed to {} after {} attempts", description_str, attempt)
        };
        Err(Box::new(ConnectException::new(error_msg, last_error)))
    }
}
