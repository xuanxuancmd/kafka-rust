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

//! WorkerMetricsGroup - Worker-level metrics for Kafka Connect runtime.
//!
//! This module provides metrics tracking for worker-level operations,
//! including connector and task startup success/failure rates.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.WorkerMetricsGroup` in Java.

use std::sync::{Arc, Mutex, RwLock};

use common_trait::storage::ConnectorTaskId;

use crate::runtime::connect_metrics::{
    ConnectMetrics, CounterGauge, GaugeValue, MetricGroup, MetricValueSupplier, Sensor,
};
use crate::runtime::connect_metrics_registry::ConnectMetricsRegistry;
use crate::runtime::status::{ConnectorStatusListener, TaskStatusListener};

/// Wrapper for CounterGauge that implements MetricValueSupplier.
/// This allows CounterGauge to be used with add_value_metric.
#[derive(Debug)]
struct CounterMetricSupplier {
    counter: Arc<CounterGauge>,
}

impl CounterMetricSupplier {
    fn new(counter: Arc<CounterGauge>) -> Self {
        CounterMetricSupplier { counter }
    }
}

impl MetricValueSupplier for CounterMetricSupplier {
    fn metric_value(&self, _now_ms: i64) -> f64 {
        self.counter.value()
    }
}

impl GaugeValue for CounterMetricSupplier {
    fn value(&self) -> f64 {
        self.counter.value()
    }
}

/// WorkerMetricsGroup - Tracks worker-level metrics for Kafka Connect.
///
/// This struct manages metrics related to worker operations, including:
/// - Connector count and task count
/// - Connector startup attempts, successes, failures, and success/failure percentages
/// - Task startup attempts, successes, failures, and success/failure percentages
///
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerMetricsGroup` in Java.
pub struct WorkerMetricsGroup {
    /// The metric group for worker metrics
    metric_group: Arc<MetricGroup>,
    /// Sensor for connector startup attempts
    connector_startup_attempts: Arc<Sensor>,
    /// Sensor for connector startup successes
    connector_startup_successes: Arc<Sensor>,
    /// Sensor for connector startup failures
    connector_startup_failures: Arc<Sensor>,
    /// Sensor for connector startup results (for percentage calculation)
    connector_startup_results: Arc<Sensor>,
    /// Sensor for task startup attempts
    task_startup_attempts: Arc<Sensor>,
    /// Sensor for task startup successes
    task_startup_successes: Arc<Sensor>,
    /// Sensor for task startup failures
    task_startup_failures: Arc<Sensor>,
    /// Sensor for task startup results (for percentage calculation)
    task_startup_results: Arc<Sensor>,
    /// Counter gauge for connector startup attempts total
    connector_startup_attempts_counter: Arc<CounterGauge>,
    /// Counter gauge for connector startup successes total
    connector_startup_successes_counter: Arc<CounterGauge>,
    /// Counter gauge for connector startup failures total
    connector_startup_failures_counter: Arc<CounterGauge>,
    /// Counter gauge for task startup attempts total
    task_startup_attempts_counter: Arc<CounterGauge>,
    /// Counter gauge for task startup successes total
    task_startup_successes_counter: Arc<CounterGauge>,
    /// Counter gauge for task startup failures total
    task_startup_failures_counter: Arc<CounterGauge>,
    /// Connect metrics registry
    registry: ConnectMetricsRegistry,
}

impl WorkerMetricsGroup {
    /// Creates a new WorkerMetricsGroup.
    ///
    /// # Arguments
    /// * `connectors` - Map of connector name to WorkerConnector (for counting)
    /// * `tasks` - Map of task ID to WorkerTask (for counting)
    /// * `connect_metrics` - The ConnectMetrics instance to use for creating metrics
    ///
    /// Corresponds to Java: `WorkerMetricsGroup(Map<String, WorkerConnector> connectors, Map<ConnectorTaskId, WorkerTask<?, ?>> tasks, ConnectMetrics connectMetrics)`
    pub fn new(
        connector_count: usize,
        task_count: usize,
        connect_metrics: &ConnectMetrics,
    ) -> Self {
        let registry = connect_metrics.registry().clone();
        let metric_group = connect_metrics.group(registry.worker_group_name(), &[]);

        // Add connector count metric
        let connector_count_gauge = Arc::new(ConnectorCountGauge::new(connector_count));
        metric_group.add_value_metric(registry.connector_count(), connector_count_gauge.clone());

        // Add task count metric
        let task_count_gauge = Arc::new(TaskCountGauge::new(task_count));
        metric_group.add_value_metric(registry.task_count(), task_count_gauge.clone());

        // Create sensors for connector startup metrics
        let connector_startup_attempts = metric_group.sensor("connector-startup-attempts");
        let connector_startup_successes = metric_group.sensor("connector-startup-successes");
        let connector_startup_failures = metric_group.sensor("connector-startup-failures");
        let connector_startup_results = metric_group.sensor("connector-startup-results");

        // Create counter gauges for tracking totals
        let connector_startup_attempts_counter = Arc::new(CounterGauge::new(0.0));
        let connector_startup_successes_counter = Arc::new(CounterGauge::new(0.0));
        let connector_startup_failures_counter = Arc::new(CounterGauge::new(0.0));

        // Add metrics to sensors (wrap CounterGauge in CounterMetricSupplier)
        metric_group.add_value_metric(
            registry.connector_startup_attempts_total(),
            Arc::new(CounterMetricSupplier::new(
                connector_startup_attempts_counter.clone(),
            )),
        );
        metric_group.add_value_metric(
            registry.connector_startup_success_total(),
            Arc::new(CounterMetricSupplier::new(
                connector_startup_successes_counter.clone(),
            )),
        );
        metric_group.add_value_metric(
            registry.connector_startup_failure_total(),
            Arc::new(CounterMetricSupplier::new(
                connector_startup_failures_counter.clone(),
            )),
        );

        // Add percentage metrics for connector startup
        let connector_success_pct_gauge = Arc::new(ConnectorStartupSuccessPercentageGauge::new(
            connector_startup_successes_counter.clone(),
            connector_startup_attempts_counter.clone(),
        ));
        metric_group.add_value_metric(
            registry.connector_startup_success_percentage(),
            connector_success_pct_gauge.clone(),
        );

        let connector_failure_pct_gauge = Arc::new(ConnectorStartupFailurePercentageGauge::new(
            connector_startup_failures_counter.clone(),
            connector_startup_attempts_counter.clone(),
        ));
        metric_group.add_value_metric(
            registry.connector_startup_failure_percentage(),
            connector_failure_pct_gauge.clone(),
        );

        // Create sensors for task startup metrics
        let task_startup_attempts = metric_group.sensor("task-startup-attempts");
        let task_startup_successes = metric_group.sensor("task-startup-successes");
        let task_startup_failures = metric_group.sensor("task-startup-failures");
        let task_startup_results = metric_group.sensor("task-startup-results");

        // Create counter gauges for tracking task totals
        let task_startup_attempts_counter = Arc::new(CounterGauge::new(0.0));
        let task_startup_successes_counter = Arc::new(CounterGauge::new(0.0));
        let task_startup_failures_counter = Arc::new(CounterGauge::new(0.0));

        // Add metrics to sensors (wrap CounterGauge in CounterMetricSupplier)
        metric_group.add_value_metric(
            registry.task_startup_attempts_total(),
            Arc::new(CounterMetricSupplier::new(
                task_startup_attempts_counter.clone(),
            )),
        );
        metric_group.add_value_metric(
            registry.task_startup_success_total(),
            Arc::new(CounterMetricSupplier::new(
                task_startup_successes_counter.clone(),
            )),
        );
        metric_group.add_value_metric(
            registry.task_startup_failure_total(),
            Arc::new(CounterMetricSupplier::new(
                task_startup_failures_counter.clone(),
            )),
        );

        // Add percentage metrics for task startup
        let task_success_pct_gauge = Arc::new(TaskStartupSuccessPercentageGauge::new(
            task_startup_successes_counter.clone(),
            task_startup_attempts_counter.clone(),
        ));
        metric_group.add_value_metric(
            registry.task_startup_success_percentage(),
            task_success_pct_gauge.clone(),
        );

        let task_failure_pct_gauge = Arc::new(TaskStartupFailurePercentageGauge::new(
            task_startup_failures_counter.clone(),
            task_startup_attempts_counter.clone(),
        ));
        metric_group.add_value_metric(
            registry.task_startup_failure_percentage(),
            task_failure_pct_gauge.clone(),
        );

        WorkerMetricsGroup {
            metric_group,
            connector_startup_attempts,
            connector_startup_successes,
            connector_startup_failures,
            connector_startup_results,
            task_startup_attempts,
            task_startup_successes,
            task_startup_failures,
            task_startup_results,
            connector_startup_attempts_counter,
            connector_startup_successes_counter,
            connector_startup_failures_counter,
            task_startup_attempts_counter,
            task_startup_successes_counter,
            task_startup_failures_counter,
            registry,
        }
    }

    /// Closes the WorkerMetricsGroup, removing all metrics.
    ///
    /// Corresponds to Java: `void close()`
    pub fn close(&self) {
        self.metric_group.close();
    }

    /// Returns the metric group for this WorkerMetricsGroup.
    ///
    /// Corresponds to Java: `ConnectMetrics.MetricGroup metricGroup()`
    pub fn metric_group(&self) -> &Arc<MetricGroup> {
        &self.metric_group
    }

    /// Records a connector startup success.
    ///
    /// This increments:
    /// - connector_startup_attempts
    /// - connector_startup_successes
    /// - connector_startup_results (with value 1.0 for success)
    ///
    /// Corresponds to Java: `void recordConnectorStartupSuccess()`
    pub fn record_connector_startup_success(&self) {
        self.connector_startup_attempts_counter.increment(1.0);
        self.connector_startup_successes_counter.increment(1.0);
        self.connector_startup_attempts.record(1.0);
        self.connector_startup_successes.record(1.0);
        self.connector_startup_results.record(1.0);
    }

    /// Records a connector startup failure.
    ///
    /// This increments:
    /// - connector_startup_attempts
    /// - connector_startup_failures
    /// - connector_startup_results (with value 0.0 for failure)
    ///
    /// Corresponds to Java: `void recordConnectorStartupFailure()`
    pub fn record_connector_startup_failure(&self) {
        self.connector_startup_attempts_counter.increment(1.0);
        self.connector_startup_failures_counter.increment(1.0);
        self.connector_startup_attempts.record(1.0);
        self.connector_startup_failures.record(1.0);
        self.connector_startup_results.record(0.0);
    }

    /// Records a task startup success.
    ///
    /// This increments:
    /// - task_startup_attempts
    /// - task_startup_successes
    /// - task_startup_results (with value 1.0 for success)
    ///
    /// Corresponds to Java: `void recordTaskSuccess()`
    pub fn record_task_success(&self) {
        self.task_startup_attempts_counter.increment(1.0);
        self.task_startup_successes_counter.increment(1.0);
        self.task_startup_attempts.record(1.0);
        self.task_startup_successes.record(1.0);
        self.task_startup_results.record(1.0);
    }

    /// Records a task startup failure.
    ///
    /// This increments:
    /// - task_startup_attempts
    /// - task_startup_failures
    /// - task_startup_results (with value 0.0 for failure)
    ///
    /// Corresponds to Java: `void recordTaskFailure()`
    pub fn record_task_failure(&self) {
        self.task_startup_attempts_counter.increment(1.0);
        self.task_startup_failures_counter.increment(1.0);
        self.task_startup_attempts.record(1.0);
        self.task_startup_failures.record(1.0);
        self.task_startup_results.record(0.0);
    }

    /// Wraps a ConnectorStatusListener to record metrics.
    ///
    /// The returned listener will:
    /// - Record connector startup success on on_startup
    /// - Record connector startup failure on on_failure (only if startup hasn't succeeded yet)
    ///
    /// # Arguments
    /// * `delegate_listener` - The delegate listener to wrap
    ///
    /// Corresponds to Java: `ConnectorStatus.Listener wrapStatusListener(ConnectorStatus.Listener delegateListener)`
    pub fn wrap_connector_status_listener(
        &self,
        delegate_listener: Arc<Mutex<dyn ConnectorStatusListener>>,
    ) -> ConnectorStatusListenerWrapper {
        ConnectorStatusListenerWrapper::new(
            delegate_listener,
            Arc::new(WorkerMetricsGroupRef::new(self)),
        )
    }

    /// Wraps a TaskStatusListener to record metrics.
    ///
    /// The returned listener will:
    /// - Record task success on on_startup
    /// - Record task failure on on_failure (only if startup hasn't succeeded yet)
    ///
    /// # Arguments
    /// * `delegate_listener` - The delegate listener to wrap
    ///
    /// Corresponds to Java: `TaskStatus.Listener wrapStatusListener(TaskStatus.Listener delegateListener)`
    pub fn wrap_task_status_listener(
        &self,
        delegate_listener: Arc<Mutex<dyn TaskStatusListener>>,
    ) -> TaskStatusListenerWrapper {
        TaskStatusListenerWrapper::new(
            delegate_listener,
            Arc::new(WorkerMetricsGroupRef::new(self)),
        )
    }

    /// Returns the registry reference.
    pub fn registry(&self) -> &ConnectMetricsRegistry {
        &self.registry
    }

    /// Returns the connector startup attempts counter.
    pub fn connector_startup_attempts_total(&self) -> f64 {
        self.connector_startup_attempts_counter.value()
    }

    /// Returns the connector startup successes counter.
    pub fn connector_startup_success_total(&self) -> f64 {
        self.connector_startup_successes_counter.value()
    }

    /// Returns the connector startup failures counter.
    pub fn connector_startup_failure_total(&self) -> f64 {
        self.connector_startup_failures_counter.value()
    }

    /// Returns the task startup attempts counter.
    pub fn task_startup_attempts_total(&self) -> f64 {
        self.task_startup_attempts_counter.value()
    }

    /// Returns the task startup successes counter.
    pub fn task_startup_success_total(&self) -> f64 {
        self.task_startup_successes_counter.value()
    }

    /// Returns the task startup failures counter.
    pub fn task_startup_failure_total(&self) -> f64 {
        self.task_startup_failures_counter.value()
    }
}

/// Reference wrapper for WorkerMetricsGroup to allow sharing with listeners.
/// This is needed because listeners need to call record methods on the metrics group.
#[derive(Debug)]
pub struct WorkerMetricsGroupRef {
    inner: RwLock<WorkerMetricsGroupInner>,
}

#[derive(Debug)]
struct WorkerMetricsGroupInner {
    connector_startup_attempts_counter: Arc<CounterGauge>,
    connector_startup_successes_counter: Arc<CounterGauge>,
    connector_startup_failures_counter: Arc<CounterGauge>,
    task_startup_attempts_counter: Arc<CounterGauge>,
    task_startup_successes_counter: Arc<CounterGauge>,
    task_startup_failures_counter: Arc<CounterGauge>,
}

impl WorkerMetricsGroupRef {
    fn new(group: &WorkerMetricsGroup) -> Self {
        WorkerMetricsGroupRef {
            inner: RwLock::new(WorkerMetricsGroupInner {
                connector_startup_attempts_counter: group
                    .connector_startup_attempts_counter
                    .clone(),
                connector_startup_successes_counter: group
                    .connector_startup_successes_counter
                    .clone(),
                connector_startup_failures_counter: group
                    .connector_startup_failures_counter
                    .clone(),
                task_startup_attempts_counter: group.task_startup_attempts_counter.clone(),
                task_startup_successes_counter: group.task_startup_successes_counter.clone(),
                task_startup_failures_counter: group.task_startup_failures_counter.clone(),
            }),
        }
    }

    pub fn record_connector_startup_success(&self) {
        let inner = self.inner.read().unwrap();
        inner.connector_startup_attempts_counter.increment(1.0);
        inner.connector_startup_successes_counter.increment(1.0);
    }

    pub fn record_connector_startup_failure(&self) {
        let inner = self.inner.read().unwrap();
        inner.connector_startup_attempts_counter.increment(1.0);
        inner.connector_startup_failures_counter.increment(1.0);
    }

    pub fn record_task_success(&self) {
        let inner = self.inner.read().unwrap();
        inner.task_startup_attempts_counter.increment(1.0);
        inner.task_startup_successes_counter.increment(1.0);
    }

    pub fn record_task_failure(&self) {
        let inner = self.inner.read().unwrap();
        inner.task_startup_attempts_counter.increment(1.0);
        inner.task_startup_failures_counter.increment(1.0);
    }
}

/// ConnectorStatusListenerWrapper - Wraps a ConnectorStatusListener to record metrics.
///
/// This listener wraps a delegate listener and records startup metrics
/// based on connector lifecycle events.
///
/// Corresponds to Java: `WorkerMetricsGroup.ConnectorStatusListener`
pub struct ConnectorStatusListenerWrapper {
    /// The delegate listener to forward events to
    delegate_listener: Arc<Mutex<dyn ConnectorStatusListener>>,
    /// Reference to the WorkerMetricsGroup for recording metrics
    metrics_ref: Arc<WorkerMetricsGroupRef>,
    /// Flag to track if startup has succeeded
    startup_succeeded: Mutex<bool>,
}

impl ConnectorStatusListenerWrapper {
    /// Creates a new ConnectorStatusListenerWrapper.
    pub fn new(
        delegate_listener: Arc<Mutex<dyn ConnectorStatusListener>>,
        metrics_ref: Arc<WorkerMetricsGroupRef>,
    ) -> Self {
        ConnectorStatusListenerWrapper {
            delegate_listener,
            metrics_ref,
            startup_succeeded: Mutex::new(false),
        }
    }

    /// Returns whether startup has succeeded.
    pub fn startup_succeeded(&self) -> bool {
        *self.startup_succeeded.lock().unwrap()
    }
}

impl ConnectorStatusListener for ConnectorStatusListenerWrapper {
    fn on_startup(&mut self, connector: &str) {
        *self.startup_succeeded.lock().unwrap() = true;
        self.metrics_ref.record_connector_startup_success();
        self.delegate_listener.lock().unwrap().on_startup(connector);
    }

    fn on_shutdown(&mut self, connector: &str) {
        self.delegate_listener
            .lock()
            .unwrap()
            .on_shutdown(connector);
    }

    fn on_stop(&mut self, connector: &str) {
        self.delegate_listener.lock().unwrap().on_stop(connector);
    }

    fn on_pause(&mut self, connector: &str) {
        self.delegate_listener.lock().unwrap().on_pause(connector);
    }

    fn on_resume(&mut self, connector: &str) {
        self.delegate_listener.lock().unwrap().on_resume(connector);
    }

    fn on_failure(&mut self, connector: &str, cause: &dyn std::error::Error) {
        // Only record failure if startup hasn't succeeded yet
        // This matches Java behavior: recordConnectorStartupFailure() should not be called
        // if failure happens after a successful startup
        if !self.startup_succeeded() {
            self.metrics_ref.record_connector_startup_failure();
        }
        self.delegate_listener
            .lock()
            .unwrap()
            .on_failure(connector, cause);
    }

    fn on_deletion(&mut self, connector: &str) {
        self.delegate_listener
            .lock()
            .unwrap()
            .on_deletion(connector);
    }

    fn on_restart(&mut self, connector: &str) {
        // Reset startup succeeded flag on restart
        *self.startup_succeeded.lock().unwrap() = false;
        self.delegate_listener.lock().unwrap().on_restart(connector);
    }
}

/// TaskStatusListenerWrapper - Wraps a TaskStatusListener to record metrics.
///
/// This listener wraps a delegate listener and records startup metrics
/// based on task lifecycle events.
///
/// Corresponds to Java: `WorkerMetricsGroup.TaskStatusListener`
pub struct TaskStatusListenerWrapper {
    /// The delegate listener to forward events to
    delegate_listener: Arc<Mutex<dyn TaskStatusListener>>,
    /// Reference to the WorkerMetricsGroup for recording metrics
    metrics_ref: Arc<WorkerMetricsGroupRef>,
    /// Flag to track if startup has succeeded
    startup_succeeded: Mutex<bool>,
}

impl TaskStatusListenerWrapper {
    /// Creates a new TaskStatusListenerWrapper.
    pub fn new(
        delegate_listener: Arc<Mutex<dyn TaskStatusListener>>,
        metrics_ref: Arc<WorkerMetricsGroupRef>,
    ) -> Self {
        TaskStatusListenerWrapper {
            delegate_listener,
            metrics_ref,
            startup_succeeded: Mutex::new(false),
        }
    }

    /// Returns whether startup has succeeded.
    pub fn startup_succeeded(&self) -> bool {
        *self.startup_succeeded.lock().unwrap()
    }
}

impl TaskStatusListener for TaskStatusListenerWrapper {
    fn on_startup(&mut self, id: &ConnectorTaskId) {
        *self.startup_succeeded.lock().unwrap() = true;
        self.metrics_ref.record_task_success();
        self.delegate_listener.lock().unwrap().on_startup(id);
    }

    fn on_pause(&mut self, id: &ConnectorTaskId) {
        self.delegate_listener.lock().unwrap().on_pause(id);
    }

    fn on_resume(&mut self, id: &ConnectorTaskId) {
        self.delegate_listener.lock().unwrap().on_resume(id);
    }

    fn on_failure(&mut self, id: &ConnectorTaskId, cause: &dyn std::error::Error) {
        // Only record failure if startup hasn't succeeded yet
        if !self.startup_succeeded() {
            self.metrics_ref.record_task_failure();
        }
        self.delegate_listener.lock().unwrap().on_failure(id, cause);
    }

    fn on_shutdown(&mut self, id: &ConnectorTaskId) {
        self.delegate_listener.lock().unwrap().on_shutdown(id);
    }

    fn on_deletion(&mut self, id: &ConnectorTaskId) {
        self.delegate_listener.lock().unwrap().on_deletion(id);
    }

    fn on_restart(&mut self, id: &ConnectorTaskId) {
        // Reset startup succeeded flag on restart
        *self.startup_succeeded.lock().unwrap() = false;
        self.delegate_listener.lock().unwrap().on_restart(id);
    }
}

/// Gauge for connector count that can be updated.
#[derive(Debug)]
pub struct ConnectorCountGauge {
    count: Mutex<usize>,
}

impl ConnectorCountGauge {
    pub fn new(count: usize) -> Self {
        ConnectorCountGauge {
            count: Mutex::new(count),
        }
    }

    pub fn set(&self, new_count: usize) {
        *self.count.lock().unwrap() = new_count;
    }
}

impl GaugeValue for ConnectorCountGauge {
    fn value(&self) -> f64 {
        *self.count.lock().unwrap() as f64
    }
}

impl crate::runtime::connect_metrics::MetricValueSupplier for ConnectorCountGauge {
    fn metric_value(&self, _now_ms: i64) -> f64 {
        self.value()
    }
}

/// Gauge for task count that can be updated.
#[derive(Debug)]
pub struct TaskCountGauge {
    count: Mutex<usize>,
}

impl TaskCountGauge {
    pub fn new(count: usize) -> Self {
        TaskCountGauge {
            count: Mutex::new(count),
        }
    }

    pub fn set(&self, new_count: usize) {
        *self.count.lock().unwrap() = new_count;
    }
}

impl GaugeValue for TaskCountGauge {
    fn value(&self) -> f64 {
        *self.count.lock().unwrap() as f64
    }
}

impl crate::runtime::connect_metrics::MetricValueSupplier for TaskCountGauge {
    fn metric_value(&self, _now_ms: i64) -> f64 {
        self.value()
    }
}

/// Gauge for connector startup success percentage.
#[derive(Debug)]
pub struct ConnectorStartupSuccessPercentageGauge {
    successes: Arc<CounterGauge>,
    attempts: Arc<CounterGauge>,
}

impl ConnectorStartupSuccessPercentageGauge {
    pub fn new(successes: Arc<CounterGauge>, attempts: Arc<CounterGauge>) -> Self {
        ConnectorStartupSuccessPercentageGauge {
            successes,
            attempts,
        }
    }
}

impl GaugeValue for ConnectorStartupSuccessPercentageGauge {
    fn value(&self) -> f64 {
        let attempts = self.attempts.value();
        if attempts == 0.0 {
            0.0
        } else {
            (self.successes.value() / attempts) * 100.0
        }
    }
}

impl crate::runtime::connect_metrics::MetricValueSupplier
    for ConnectorStartupSuccessPercentageGauge
{
    fn metric_value(&self, _now_ms: i64) -> f64 {
        self.value()
    }
}

/// Gauge for connector startup failure percentage.
#[derive(Debug)]
pub struct ConnectorStartupFailurePercentageGauge {
    failures: Arc<CounterGauge>,
    attempts: Arc<CounterGauge>,
}

impl ConnectorStartupFailurePercentageGauge {
    pub fn new(failures: Arc<CounterGauge>, attempts: Arc<CounterGauge>) -> Self {
        ConnectorStartupFailurePercentageGauge { failures, attempts }
    }
}

impl GaugeValue for ConnectorStartupFailurePercentageGauge {
    fn value(&self) -> f64 {
        let attempts = self.attempts.value();
        if attempts == 0.0 {
            0.0
        } else {
            (self.failures.value() / attempts) * 100.0
        }
    }
}

impl crate::runtime::connect_metrics::MetricValueSupplier
    for ConnectorStartupFailurePercentageGauge
{
    fn metric_value(&self, _now_ms: i64) -> f64 {
        self.value()
    }
}

/// Gauge for task startup success percentage.
#[derive(Debug)]
pub struct TaskStartupSuccessPercentageGauge {
    successes: Arc<CounterGauge>,
    attempts: Arc<CounterGauge>,
}

impl TaskStartupSuccessPercentageGauge {
    pub fn new(successes: Arc<CounterGauge>, attempts: Arc<CounterGauge>) -> Self {
        TaskStartupSuccessPercentageGauge {
            successes,
            attempts,
        }
    }
}

impl GaugeValue for TaskStartupSuccessPercentageGauge {
    fn value(&self) -> f64 {
        let attempts = self.attempts.value();
        if attempts == 0.0 {
            0.0
        } else {
            (self.successes.value() / attempts) * 100.0
        }
    }
}

impl crate::runtime::connect_metrics::MetricValueSupplier for TaskStartupSuccessPercentageGauge {
    fn metric_value(&self, _now_ms: i64) -> f64 {
        self.value()
    }
}

/// Gauge for task startup failure percentage.
#[derive(Debug)]
pub struct TaskStartupFailurePercentageGauge {
    failures: Arc<CounterGauge>,
    attempts: Arc<CounterGauge>,
}

impl TaskStartupFailurePercentageGauge {
    pub fn new(failures: Arc<CounterGauge>, attempts: Arc<CounterGauge>) -> Self {
        TaskStartupFailurePercentageGauge { failures, attempts }
    }
}

impl GaugeValue for TaskStartupFailurePercentageGauge {
    fn value(&self) -> f64 {
        let attempts = self.attempts.value();
        if attempts == 0.0 {
            0.0
        } else {
            (self.failures.value() / attempts) * 100.0
        }
    }
}

impl crate::runtime::connect_metrics::MetricValueSupplier for TaskStartupFailurePercentageGauge {
    fn metric_value(&self, _now_ms: i64) -> f64 {
        self.value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::connect_metrics::ConnectMetrics;
    use crate::runtime::status::{ConnectorStatusListener, TaskStatusListener};
    use common_trait::util::time::SYSTEM;
    use std::sync::Arc;

    /// Mock connector status listener for testing
    struct MockConnectorStatusListener {
        events: Mutex<Vec<String>>,
    }

    impl MockConnectorStatusListener {
        fn new() -> Self {
            MockConnectorStatusListener {
                events: Mutex::new(Vec::new()),
            }
        }

        fn events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    impl ConnectorStatusListener for MockConnectorStatusListener {
        fn on_startup(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("startup:{}", connector));
        }
        fn on_shutdown(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("shutdown:{}", connector));
        }
        fn on_stop(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("stop:{}", connector));
        }
        fn on_pause(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("pause:{}", connector));
        }
        fn on_resume(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("resume:{}", connector));
        }
        fn on_failure(&mut self, connector: &str, _cause: &dyn std::error::Error) {
            self.events
                .lock()
                .unwrap()
                .push(format!("failure:{}", connector));
        }
        fn on_deletion(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("deletion:{}", connector));
        }
        fn on_restart(&mut self, connector: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("restart:{}", connector));
        }
    }

    /// Mock task status listener for testing
    struct MockTaskStatusListener {
        events: Mutex<Vec<String>>,
    }

    impl MockTaskStatusListener {
        fn new() -> Self {
            MockTaskStatusListener {
                events: Mutex::new(Vec::new()),
            }
        }

        fn events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    impl TaskStatusListener for MockTaskStatusListener {
        fn on_startup(&mut self, id: &ConnectorTaskId) {
            self.events
                .lock()
                .unwrap()
                .push(format!("startup:{}-{}", id.connector(), id.task()));
        }
        fn on_pause(&mut self, id: &ConnectorTaskId) {
            self.events
                .lock()
                .unwrap()
                .push(format!("pause:{}-{}", id.connector(), id.task()));
        }
        fn on_resume(&mut self, id: &ConnectorTaskId) {
            self.events
                .lock()
                .unwrap()
                .push(format!("resume:{}-{}", id.connector(), id.task()));
        }
        fn on_failure(&mut self, id: &ConnectorTaskId, _cause: &dyn std::error::Error) {
            self.events
                .lock()
                .unwrap()
                .push(format!("failure:{}-{}", id.connector(), id.task()));
        }
        fn on_shutdown(&mut self, id: &ConnectorTaskId) {
            self.events
                .lock()
                .unwrap()
                .push(format!("shutdown:{}-{}", id.connector(), id.task()));
        }
        fn on_deletion(&mut self, id: &ConnectorTaskId) {
            self.events
                .lock()
                .unwrap()
                .push(format!("deletion:{}-{}", id.connector(), id.task()));
        }
        fn on_restart(&mut self, id: &ConnectorTaskId) {
            self.events
                .lock()
                .unwrap()
                .push(format!("restart:{}-{}", id.connector(), id.task()));
        }
    }

    #[test]
    fn test_worker_metrics_group_creation() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        assert_eq!(group.connector_startup_attempts_total(), 0.0);
        assert_eq!(group.connector_startup_success_total(), 0.0);
        assert_eq!(group.connector_startup_failure_total(), 0.0);
        assert_eq!(group.task_startup_attempts_total(), 0.0);
        assert_eq!(group.task_startup_success_total(), 0.0);
        assert_eq!(group.task_startup_failure_total(), 0.0);
    }

    #[test]
    fn test_record_connector_startup_success() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        group.record_connector_startup_success();

        assert_eq!(group.connector_startup_attempts_total(), 1.0);
        assert_eq!(group.connector_startup_success_total(), 1.0);
        assert_eq!(group.connector_startup_failure_total(), 0.0);
    }

    #[test]
    fn test_record_connector_startup_failure() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        group.record_connector_startup_failure();

        assert_eq!(group.connector_startup_attempts_total(), 1.0);
        assert_eq!(group.connector_startup_success_total(), 0.0);
        assert_eq!(group.connector_startup_failure_total(), 1.0);
    }

    #[test]
    fn test_record_task_success() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        group.record_task_success();

        assert_eq!(group.task_startup_attempts_total(), 1.0);
        assert_eq!(group.task_startup_success_total(), 1.0);
        assert_eq!(group.task_startup_failure_total(), 0.0);
    }

    #[test]
    fn test_record_task_failure() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        group.record_task_failure();

        assert_eq!(group.task_startup_attempts_total(), 1.0);
        assert_eq!(group.task_startup_success_total(), 0.0);
        assert_eq!(group.task_startup_failure_total(), 1.0);
    }

    #[test]
    fn test_connector_status_listener_wrapper_startup() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        let mock_listener = Arc::new(Mutex::new(MockConnectorStatusListener::new()));
        let wrapper = group.wrap_connector_status_listener(mock_listener.clone());

        // Create a mutable wrapper for testing
        let mut wrapper = wrapper;
        wrapper.on_startup("test-connector");

        assert!(wrapper.startup_succeeded());
        assert_eq!(group.connector_startup_attempts_total(), 1.0);
        assert_eq!(group.connector_startup_success_total(), 1.0);

        // Check delegate was called
        let events = mock_listener.lock().unwrap().events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], "startup:test-connector");
    }

    #[test]
    fn test_connector_status_listener_wrapper_failure_before_startup() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        let mock_listener = Arc::new(Mutex::new(MockConnectorStatusListener::new()));
        let wrapper = group.wrap_connector_status_listener(mock_listener.clone());

        // Failure before startup should be recorded
        let mut wrapper = wrapper;
        let error = std::io::Error::new(std::io::ErrorKind::Other, "test error");
        wrapper.on_failure("test-connector", &error);

        assert!(!wrapper.startup_succeeded());
        assert_eq!(group.connector_startup_attempts_total(), 1.0);
        assert_eq!(group.connector_startup_failure_total(), 1.0);
    }

    #[test]
    fn test_connector_status_listener_wrapper_failure_after_startup() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        let mock_listener = Arc::new(Mutex::new(MockConnectorStatusListener::new()));
        let wrapper = group.wrap_connector_status_listener(mock_listener.clone());

        let mut wrapper = wrapper;
        wrapper.on_startup("test-connector");

        // Failure after startup should NOT be recorded as startup failure
        let error = std::io::Error::new(std::io::ErrorKind::Other, "test error");
        wrapper.on_failure("test-connector", &error);

        // Only startup metrics should be recorded, not failure metrics
        assert_eq!(group.connector_startup_attempts_total(), 1.0);
        assert_eq!(group.connector_startup_success_total(), 1.0);
        assert_eq!(group.connector_startup_failure_total(), 0.0);
    }

    #[test]
    fn test_task_status_listener_wrapper_startup() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        let mock_listener = Arc::new(Mutex::new(MockTaskStatusListener::new()));
        let wrapper = group.wrap_task_status_listener(mock_listener.clone());

        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut wrapper = wrapper;
        wrapper.on_startup(&task_id);

        assert!(wrapper.startup_succeeded());
        assert_eq!(group.task_startup_attempts_total(), 1.0);
        assert_eq!(group.task_startup_success_total(), 1.0);

        // Check delegate was called
        let events = mock_listener.lock().unwrap().events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], "startup:test-connector-0");
    }

    #[test]
    fn test_task_status_listener_wrapper_failure_before_startup() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        let mock_listener = Arc::new(Mutex::new(MockTaskStatusListener::new()));
        let wrapper = group.wrap_task_status_listener(mock_listener.clone());

        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut wrapper = wrapper;
        let error = std::io::Error::new(std::io::ErrorKind::Other, "test error");
        wrapper.on_failure(&task_id, &error);

        assert!(!wrapper.startup_succeeded());
        assert_eq!(group.task_startup_attempts_total(), 1.0);
        assert_eq!(group.task_startup_failure_total(), 1.0);
    }

    #[test]
    fn test_connector_count_gauge() {
        let gauge = ConnectorCountGauge::new(5);
        assert_eq!(gauge.value(), 5.0);

        gauge.set(10);
        assert_eq!(gauge.value(), 10.0);
    }

    #[test]
    fn test_task_count_gauge() {
        let gauge = TaskCountGauge::new(5);
        assert_eq!(gauge.value(), 5.0);

        gauge.set(10);
        assert_eq!(gauge.value(), 10.0);
    }

    #[test]
    fn test_startup_success_percentage() {
        let successes = Arc::new(CounterGauge::new(0.0));
        let attempts = Arc::new(CounterGauge::new(0.0));
        let gauge =
            ConnectorStartupSuccessPercentageGauge::new(successes.clone(), attempts.clone());

        // No attempts = 0%
        assert_eq!(gauge.value(), 0.0);

        // 1 success out of 2 attempts = 50%
        successes.increment(1.0);
        attempts.increment(2.0);
        assert_eq!(gauge.value(), 50.0);

        // 2 successes out of 2 attempts = 100%
        successes.increment(1.0);
        assert_eq!(gauge.value(), 100.0);
    }

    #[test]
    fn test_startup_failure_percentage() {
        let failures = Arc::new(CounterGauge::new(0.0));
        let attempts = Arc::new(CounterGauge::new(0.0));
        let gauge = ConnectorStartupFailurePercentageGauge::new(failures.clone(), attempts.clone());

        // No attempts = 0%
        assert_eq!(gauge.value(), 0.0);

        // 1 failure out of 2 attempts = 50%
        failures.increment(1.0);
        attempts.increment(2.0);
        assert_eq!(gauge.value(), 50.0);

        // 2 failures out of 2 attempts = 100%
        failures.increment(1.0);
        assert_eq!(gauge.value(), 100.0);
    }

    #[test]
    fn test_worker_metrics_group_close() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        // Close should not panic
        group.close();
    }

    #[test]
    fn test_multiple_startup_successes() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        // Record multiple successes
        group.record_connector_startup_success();
        group.record_connector_startup_success();
        group.record_connector_startup_success();

        assert_eq!(group.connector_startup_attempts_total(), 3.0);
        assert_eq!(group.connector_startup_success_total(), 3.0);
        assert_eq!(group.connector_startup_failure_total(), 0.0);
    }

    #[test]
    fn test_mixed_startup_results() {
        let time = SYSTEM.clone();
        let connect_metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = WorkerMetricsGroup::new(0, 0, &connect_metrics);

        // 3 successes, 1 failure
        group.record_connector_startup_success();
        group.record_connector_startup_success();
        group.record_connector_startup_failure();
        group.record_connector_startup_success();

        assert_eq!(group.connector_startup_attempts_total(), 4.0);
        assert_eq!(group.connector_startup_success_total(), 3.0);
        assert_eq!(group.connector_startup_failure_total(), 1.0);
    }
}
