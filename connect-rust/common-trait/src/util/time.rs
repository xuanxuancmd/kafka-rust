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

//! Time interface abstracting the clock to use in unit testing.
//!
//! This module provides a Time trait and Timer struct that correspond to
//! Java's org.apache.kafka.common.utils.Time and Timer classes.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// An interface abstracting the clock to use in unit testing classes that make use of clock time.
///
/// Implementations of this trait should be thread-safe.
/// This trait is object-safe (dyn compatible) for use with trait objects.
pub trait Time: Send + Sync + std::fmt::Debug {
    /// Returns the current time in milliseconds.
    fn milliseconds(&self) -> i64;

    /// Returns the current value of the running JVM's high-resolution time source, in nanoseconds.
    fn nanoseconds(&self) -> i64;

    /// Returns the value returned by `nanoseconds` converted into milliseconds.
    fn hi_res_clock_ms(&self) -> i64 {
        self.nanoseconds() / 1_000_000
    }

    /// Sleep for the given number of milliseconds.
    fn sleep(&self, ms: i64);

    /// Get a timer which is bound to this time instance and expires after the given timeout.
    fn timer(&self, timeout_ms: i64) -> Timer;
}

/// Wait for a condition using the monitor of a given object.
pub fn wait_object<T, F>(
    time: &dyn Time,
    condvar: &std::sync::Condvar,
    mutex: &std::sync::Mutex<T>,
    condition: F,
    deadline_ms: i64,
) -> bool
where
    F: Fn(&T) -> bool,
{
    let mut guard = mutex.lock().unwrap();
    while !condition(&guard) {
        let now_ms = time.milliseconds();
        if now_ms >= deadline_ms {
            return false;
        }
        let remaining_ms = deadline_ms - now_ms;
        let result = condvar
            .wait_timeout(guard, Duration::from_millis(remaining_ms as u64))
            .unwrap();
        guard = result.0;
    }
    true
}

/// System time implementation using std::time.
#[derive(Debug, Clone)]
pub struct SystemTimeImpl;

impl SystemTimeImpl {
    pub fn new() -> Self {
        SystemTimeImpl
    }
}

impl Default for SystemTimeImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl Time for SystemTimeImpl {
    fn milliseconds(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    fn nanoseconds(&self) -> i64 {
        Instant::now().elapsed().as_nanos() as i64
    }

    fn sleep(&self, ms: i64) {
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms as u64));
        }
    }

    fn timer(&self, timeout_ms: i64) -> Timer {
        Timer::new(SYSTEM.clone(), timeout_ms)
    }
}

/// Global SYSTEM time instance.
pub static SYSTEM: once_cell::sync::Lazy<Arc<SystemTimeImpl>> =
    once_cell::sync::Lazy::new(|| Arc::new(SystemTimeImpl::new()));

/// Helper class which makes blocking methods with a timeout easier to implement.
#[derive(Debug)]
pub struct Timer {
    time: Arc<SystemTimeImpl>,
    start_ms: i64,
    current_time_ms: i64,
    deadline_ms: i64,
    timeout_ms: i64,
}

impl Timer {
    pub fn new(time: Arc<SystemTimeImpl>, timeout_ms: i64) -> Self {
        let mut timer = Timer {
            time,
            start_ms: 0,
            current_time_ms: 0,
            deadline_ms: 0,
            timeout_ms: 0,
        };
        timer.update();
        timer.reset(timeout_ms);
        timer
    }

    pub fn system(timeout_ms: i64) -> Self {
        Self::new(SYSTEM.clone(), timeout_ms)
    }

    pub fn is_expired(&self) -> bool {
        self.current_time_ms >= self.deadline_ms
    }

    pub fn is_expired_by(&self) -> i64 {
        std::cmp::max(0, self.current_time_ms - self.deadline_ms)
    }

    pub fn not_expired(&self) -> bool {
        !self.is_expired()
    }

    pub fn update_and_reset(&mut self, timeout_ms: i64) {
        self.update();
        self.reset(timeout_ms);
    }

    pub fn reset(&mut self, timeout_ms: i64) {
        if timeout_ms < 0 {
            panic!("Invalid negative timeout {}", timeout_ms);
        }
        self.timeout_ms = timeout_ms;
        self.start_ms = self.current_time_ms;
        if self.current_time_ms > i64::MAX - timeout_ms {
            self.deadline_ms = i64::MAX;
        } else {
            self.deadline_ms = self.current_time_ms + timeout_ms;
        }
    }

    pub fn reset_deadline(&mut self, deadline_ms: i64) {
        if deadline_ms < 0 {
            panic!("Invalid negative deadline {}", deadline_ms);
        }
        self.timeout_ms = std::cmp::max(0, deadline_ms - self.current_time_ms);
        self.start_ms = self.current_time_ms;
        self.deadline_ms = deadline_ms;
    }

    pub fn update(&mut self) {
        self.update_with(self.time.milliseconds());
    }

    pub fn update_with(&mut self, current_time_ms: i64) {
        self.current_time_ms = std::cmp::max(current_time_ms, self.current_time_ms);
    }

    pub fn remaining_ms(&self) -> i64 {
        std::cmp::max(0, self.deadline_ms - self.current_time_ms)
    }

    pub fn current_time_ms(&self) -> i64 {
        self.current_time_ms
    }

    pub fn elapsed_ms(&self) -> i64 {
        self.current_time_ms - self.start_ms
    }

    pub fn timeout_ms(&self) -> i64 {
        self.timeout_ms
    }

    pub fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }

    pub fn sleep(&mut self, duration_ms: i64) {
        let sleep_duration_ms = std::cmp::min(duration_ms, self.remaining_ms());
        self.time.sleep(sleep_duration_ms);
        self.update();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;

    #[test]
    fn test_system_time_milliseconds() {
        let time = SystemTimeImpl::new();
        assert!(time.milliseconds() > 0);
    }

    #[test]
    fn test_system_time_nanoseconds() {
        let time = SystemTimeImpl::new();
        let ns1 = time.nanoseconds();
        std::thread::sleep(Duration::from_millis(10));
        let ns2 = time.nanoseconds();
        assert!(ns2 > ns1);
    }

    #[test]
    fn test_timer_basic() {
        let timer = Timer::system(1000);
        assert!(!timer.is_expired());
        assert!(timer.remaining_ms() > 0);
    }

    #[test]
    fn test_timer_reset() {
        let mut timer = Timer::system(100);
        timer.update();
        timer.reset(500);
        assert_eq!(timer.timeout_ms(), 500);
    }

    #[test]
    fn test_wait_object_satisfied() {
        let time = SystemTimeImpl::new();
        let condvar = Condvar::new();
        let mutex = Mutex::new(true);
        assert!(wait_object(
            &time,
            &condvar,
            &mutex,
            |v| *v,
            time.milliseconds() + 1000
        ));
    }

    #[test]
    fn test_system_constant() {
        assert!(SYSTEM.milliseconds() > 0);
    }
}
