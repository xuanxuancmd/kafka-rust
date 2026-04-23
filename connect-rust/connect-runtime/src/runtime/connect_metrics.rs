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

//! ConnectMetrics - Metrics management for Kafka Connect runtime.
//!
//! This module provides the metrics infrastructure for Kafka Connect,
//! including metric groups, sensors, and metric name templates.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.ConnectMetrics` in Java.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, RwLock};

use common_trait::util::time::Time;
use common_trait::worker::ConnectorTaskId;

use crate::runtime::connect_metrics_registry::{ConnectMetricsRegistry, MetricNameTemplate};

/// JMX prefix for Connect metrics.
/// Corresponds to `JMX_PREFIX` in Java.
pub const JMX_PREFIX: &str = "kafka.connect";

/// Sensor recording level.
/// Corresponds to `Sensor.RecordingLevel` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordingLevel {
    /// INFO level - records most metrics
    Info,
    /// DEBUG level - records all metrics
    Debug,
}

impl RecordingLevel {
    /// Returns the recording level from a string.
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "INFO" => RecordingLevel::Info,
            "DEBUG" => RecordingLevel::Debug,
            _ => RecordingLevel::Info,
        }
    }
}

/// Metric name with group and tags.
///
/// Simplified version of Kafka's `MetricName` class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricName {
    /// The metric name
    name: String,
    /// The metric group name
    group: String,
    /// The metric description
    description: String,
    /// Tags associated with this metric
    tags: HashMap<String, String>,
}

impl Hash for MetricName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.group.hash(state);
        // Hash tags in sorted order for consistency
        let mut tag_keys: Vec<&str> = self.tags.keys().map(|s| s.as_str()).collect();
        tag_keys.sort();
        for key in tag_keys {
            key.hash(state);
            self.tags.get(key).unwrap().hash(state);
        }
    }
}

impl MetricName {
    /// Creates a new metric name.
    pub fn new(
        name: String,
        group: String,
        description: String,
        tags: HashMap<String, String>,
    ) -> Self {
        MetricName {
            name,
            group,
            description,
            tags,
        }
    }

    /// Creates a metric name from a template with tags.
    pub fn from_template(template: &MetricNameTemplate, tags: HashMap<String, String>) -> Self {
        MetricName {
            name: template.name().to_string(),
            group: template.group().to_string(),
            description: template.description().to_string(),
            tags,
        }
    }

    /// Returns the metric name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the metric group name.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Returns the metric description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the tags associated with this metric.
    pub fn tags(&self) -> &HashMap<String, String> {
        &self.tags
    }

    /// Returns a formatted string representation for JMX.
    pub fn mbean_name(&self) -> String {
        let mut tags_str = String::new();
        for (k, v) in &self.tags {
            if !tags_str.is_empty() {
                tags_str.push(',');
            }
            tags_str.push_str(&format!("{}={}", k, v));
        }
        format!(
            "{}:type={},name={},{}",
            JMX_PREFIX, self.group, self.name, tags_str
        )
    }
}

/// Metric group identifier.
///
/// Uniquely identifies a metric group by its name and tags.
/// Corresponds to `MetricGroupId` inner class in Java.
#[derive(Debug, Clone)]
pub struct MetricGroupId {
    /// The group name
    group_name: String,
    /// The tags for this group
    tags: HashMap<String, String>,
    /// Cached hash code
    hash_code: u64,
    /// Cached string representation
    string_repr: String,
}

impl MetricGroupId {
    /// Creates a new metric group identifier.
    ///
    /// # Arguments
    /// * `group_name` - The group name
    /// * `tags` - The tags for this group
    pub fn new(group_name: String, tags: HashMap<String, String>) -> Self {
        // Calculate hash code
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        group_name.hash(&mut hasher);
        // Hash tags in a consistent order
        let mut tag_keys: Vec<&str> = tags.keys().map(|s| s.as_str()).collect();
        tag_keys.sort();
        for key in tag_keys {
            key.hash(&mut hasher);
            tags.get(key).unwrap().hash(&mut hasher);
        }
        let hash_code = hasher.finish();

        // Build string representation
        let mut string_repr = group_name.clone();
        for (key, value) in &tags {
            string_repr.push_str(&format!(";{}={}", key, value));
        }

        MetricGroupId {
            group_name,
            tags,
            hash_code,
            string_repr,
        }
    }

    /// Returns the group name.
    pub fn group_name(&self) -> &str {
        &self.group_name
    }

    /// Returns the tags.
    pub fn tags(&self) -> &HashMap<String, String> {
        &self.tags
    }

    /// Checks if a metric name belongs to this group.
    pub fn includes(&self, metric_name: &MetricName) -> bool {
        self.group_name == metric_name.group() && self.tags == *metric_name.tags()
    }
}

impl PartialEq for MetricGroupId {
    fn eq(&self, other: &Self) -> bool {
        self.group_name == other.group_name && self.tags == other.tags
    }
}

impl Eq for MetricGroupId {}

impl Hash for MetricGroupId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash_code.hash(state);
    }
}

impl std::fmt::Display for MetricGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.string_repr)
    }
}

/// Sensor for recording metric values.
///
/// Sensors are used to record values that can be aggregated
/// (like rates, averages, maximums, etc.)
/// Corresponds to `Sensor` in Kafka metrics.
#[derive(Debug)]
pub struct Sensor {
    /// The sensor name
    name: String,
    /// The recording level
    recording_level: RecordingLevel,
    /// Metrics associated with this sensor
    metrics: RwLock<HashMap<MetricName, Arc<dyn GaugeValue>>>,
}

impl Sensor {
    /// Creates a new sensor.
    pub fn new(name: String, recording_level: RecordingLevel) -> Self {
        Sensor {
            name,
            recording_level,
            metrics: RwLock::new(HashMap::new()),
        }
    }

    /// Returns the sensor name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the recording level.
    pub fn recording_level(&self) -> RecordingLevel {
        self.recording_level
    }

    /// Records a value.
    pub fn record(&self, value: f64) {
        // Simplified implementation - just records the value
        // Full implementation would aggregate over time windows
        log::trace!("Recording value {} for sensor {}", value, self.name);
    }

    /// Adds a metric to this sensor.
    pub fn add_metric(&self, name: MetricName, gauge: Arc<dyn GaugeValue>) {
        self.metrics.write().unwrap().insert(name, gauge);
    }
}

/// Trait for gauge values.
///
/// Gauges return a current value when queried.
pub trait GaugeValue: Send + Sync + std::fmt::Debug {
    /// Returns the current value.
    fn value(&self) -> f64;
}

/// Simple gauge that returns a constant value.
#[derive(Debug)]
pub struct ConstantGauge {
    value: f64,
}

impl ConstantGauge {
    pub fn new(value: f64) -> Self {
        ConstantGauge { value }
    }
}

impl GaugeValue for ConstantGauge {
    fn value(&self) -> f64 {
        self.value
    }
}

/// Counter gauge that can be incremented.
#[derive(Debug)]
pub struct CounterGauge {
    value: Mutex<f64>,
}

impl CounterGauge {
    pub fn new(initial: f64) -> Self {
        CounterGauge {
            value: Mutex::new(initial),
        }
    }

    pub fn increment(&self, delta: f64) {
        let mut val = self.value.lock().unwrap();
        *val += delta;
    }

    pub fn set(&self, new_value: f64) {
        let mut val = self.value.lock().unwrap();
        *val = new_value;
    }
}

impl GaugeValue for CounterGauge {
    fn value(&self) -> f64 {
        *self.value.lock().unwrap()
    }
}

/// Trait for suppliers that provide metric values.
///
/// Corresponds to `LiteralSupplier` in Java.
pub trait MetricValueSupplier: Send + Sync {
    /// Returns the metric value at the given time.
    fn metric_value(&self, now_ms: i64) -> f64;
}

/// A group of metrics.
///
/// Each group maps to a JMX MBean and each metric maps to an MBean attribute.
/// Sensors should be added via the `sensor` methods on this class.
/// Corresponds to `MetricGroup` inner class in Java.
#[derive(Debug)]
pub struct MetricGroup {
    /// The group identifier
    group_id: MetricGroupId,
    /// Sensor names registered in this group
    sensor_names: RwLock<HashSet<String>>,
    /// The sensor prefix for unique names
    sensor_prefix: String,
    /// Metrics in this group
    metrics: RwLock<HashMap<MetricName, Arc<dyn GaugeValue>>>,
    /// Sensors in this group
    sensors: RwLock<HashMap<String, Arc<Sensor>>>,
    /// Reference to the parent ConnectMetrics (for creating metrics)
    connect_metrics: Arc<ConnectMetricsInner>,
}

impl MetricGroup {
    /// Creates a new metric group.
    pub fn new(group_id: MetricGroupId, connect_metrics: Arc<ConnectMetricsInner>) -> Self {
        let sensor_prefix = format!("connect-sensor-group: {};", group_id);
        MetricGroup {
            group_id,
            sensor_names: RwLock::new(HashSet::new()),
            sensor_prefix,
            metrics: RwLock::new(HashMap::new()),
            sensors: RwLock::new(HashMap::new()),
            connect_metrics,
        }
    }

    /// Returns the group identifier.
    pub fn group_id(&self) -> &MetricGroupId {
        &self.group_id
    }

    /// Returns the tags for this group.
    pub fn tags(&self) -> HashMap<String, String> {
        self.group_id.tags.clone()
    }

    /// Creates a metric name from a template.
    pub fn metric_name(&self, template: &MetricNameTemplate) -> MetricName {
        MetricName::from_template(template, self.group_id.tags.clone())
    }

    /// Creates a metric name with the given name.
    pub fn metric_name_simple(&self, name: &str) -> MetricName {
        MetricName::new(
            name.to_string(),
            self.group_id.group_name.clone(),
            String::new(),
            self.group_id.tags.clone(),
        )
    }

    /// Adds a value metric with a supplier.
    pub fn add_value_metric(
        &self,
        template: &MetricNameTemplate,
        supplier: Arc<dyn MetricValueSupplier>,
    ) {
        let metric_name = self.metric_name(template);
        let gauge = Arc::new(SupplierGauge {
            supplier,
            cached_value: Mutex::new(0.0),
        });
        self.metrics.write().unwrap().insert(metric_name, gauge);
    }

    /// Adds an immutable value metric.
    pub fn add_immutable_value_metric(&self, template: &MetricNameTemplate, value: f64) {
        let metric_name = self.metric_name(template);
        let gauge = Arc::new(ConstantGauge::new(value));
        self.metrics.write().unwrap().insert(metric_name, gauge);
    }

    /// Creates or gets a sensor with INFO recording level.
    pub fn sensor(&self, name: &str) -> Arc<Sensor> {
        self.sensor_with_level(name, RecordingLevel::Info)
    }

    /// Creates or gets a sensor with a specific recording level.
    pub fn sensor_with_level(&self, name: &str, recording_level: RecordingLevel) -> Arc<Sensor> {
        let full_name = format!("{}{}", self.sensor_prefix, name);
        let mut sensor_names = self.sensor_names.write().unwrap();
        let mut sensors = self.sensors.write().unwrap();

        if let Some(existing) = sensors.get(&full_name) {
            return existing.clone();
        }

        let sensor = Arc::new(Sensor::new(full_name.clone(), recording_level));
        sensor_names.insert(full_name.clone());
        sensors.insert(full_name, sensor.clone());
        sensor
    }

    /// Close this group, removing all sensors and metrics.
    pub fn close(&self) {
        // Remove sensors
        let sensor_names: Vec<String> = self.sensor_names.write().unwrap().drain().collect();
        for name in sensor_names {
            self.connect_metrics.remove_sensor(&name);
        }

        // Remove metrics
        let metric_names: Vec<MetricName> = self.metrics.write().unwrap().keys().cloned().collect();
        for name in metric_names {
            self.connect_metrics.remove_metric(&name);
        }
    }
}

/// Supplier-based gauge.
struct SupplierGauge {
    supplier: Arc<dyn MetricValueSupplier>,
    cached_value: Mutex<f64>,
}

impl std::fmt::Debug for SupplierGauge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupplierGauge")
            .field("cached_value", &*self.cached_value.lock().unwrap())
            .finish_non_exhaustive()
    }
}

impl GaugeValue for SupplierGauge {
    fn value(&self) -> f64 {
        // Note: In a full implementation, this would call supplier.metric_value(now)
        // For now, we return the cached value
        *self.cached_value.lock().unwrap()
    }
}

/// Inner state for ConnectMetrics.
///
/// This is the internal state that can be shared across MetricGroups.
#[derive(Debug)]
pub struct ConnectMetricsInner {
    /// All metric groups by their ID
    groups_by_name: RwLock<HashMap<MetricGroupId, Arc<MetricGroup>>>,
    /// All sensors
    sensors: RwLock<HashMap<String, Arc<Sensor>>>,
    /// All metrics
    metrics: RwLock<HashMap<MetricName, Arc<dyn GaugeValue>>>,
}

impl ConnectMetricsInner {
    pub fn new() -> Self {
        ConnectMetricsInner {
            groups_by_name: RwLock::new(HashMap::new()),
            sensors: RwLock::new(HashMap::new()),
            metrics: RwLock::new(HashMap::new()),
        }
    }

    pub fn remove_sensor(&self, name: &str) {
        self.sensors.write().unwrap().remove(name);
    }

    pub fn remove_metric(&self, name: &MetricName) {
        self.metrics.write().unwrap().remove(name);
    }
}

/// Connect metrics with configurable reporters.
///
/// This is the main metrics management class for Kafka Connect.
/// It manages metric groups, sensors, and the registry of metric templates.
/// Corresponds to `org.apache.kafka.connect.runtime.ConnectMetrics` in Java.
pub struct ConnectMetrics {
    /// The inner state shared with metric groups
    inner: Arc<ConnectMetricsInner>,
    /// The time provider
    time: Arc<dyn Time>,
    /// The worker ID
    worker_id: String,
    /// The registry of metric templates
    registry: ConnectMetricsRegistry,
}

impl ConnectMetrics {
    /// Creates a new ConnectMetrics instance.
    ///
    /// # Arguments
    /// * `worker_id` - The worker identifier
    /// * `time` - The time provider
    pub fn new(worker_id: String, time: Arc<dyn Time>) -> Self {
        ConnectMetrics {
            inner: Arc::new(ConnectMetricsInner::new()),
            time,
            worker_id,
            registry: ConnectMetricsRegistry::new(),
        }
    }

    /// Creates ConnectMetrics with additional configuration options.
    ///
    /// # Arguments
    /// * `worker_id` - The worker identifier
    /// * `time` - The time provider
    /// * `num_samples` - Number of samples to retain
    /// * `sample_window_ms` - The sample window in milliseconds
    pub fn with_config(
        worker_id: String,
        time: Arc<dyn Time>,
        num_samples: i32,
        sample_window_ms: i64,
    ) -> Self {
        // Note: num_samples and sample_window_ms are not used in this simplified implementation
        // but are included for API compatibility with Java
        ConnectMetrics {
            inner: Arc::new(ConnectMetricsInner::new()),
            time,
            worker_id,
            registry: ConnectMetricsRegistry::new(),
        }
    }

    /// Returns the worker ID.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Returns the time provider.
    pub fn time(&self) -> &Arc<dyn Time> {
        &self.time
    }

    /// Returns the metrics registry.
    pub fn registry(&self) -> &ConnectMetricsRegistry {
        &self.registry
    }

    /// Gets or creates a metric group with the specified name and tags.
    ///
    /// # Arguments
    /// * `group_name` - The metric group name
    /// * `tag_key_values` - Pairs of tag key and values
    pub fn group(&self, group_name: &str, tag_key_values: &[(&str, &str)]) -> Arc<MetricGroup> {
        let tags: HashMap<String, String> = tag_key_values
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let group_id = MetricGroupId::new(group_name.to_string(), tags);

        let mut groups = self.inner.groups_by_name.write().unwrap();
        if let Some(existing) = groups.get(&group_id) {
            return existing.clone();
        }

        let group = Arc::new(MetricGroup::new(group_id.clone(), self.inner.clone()));
        groups.insert(group_id, group.clone());
        group
    }

    /// Creates a metric group ID from a group name and tags.
    pub fn group_id(&self, group_name: &str, tag_key_values: &[(&str, &str)]) -> MetricGroupId {
        let tags: HashMap<String, String> = tag_key_values
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        MetricGroupId::new(group_name.to_string(), tags)
    }

    /// Creates plugin metrics for a connector.
    pub fn connector_plugin_metrics(&self, connector_id: &str) -> PluginMetricsImpl {
        let tags = self.connector_plugin_tags(connector_id);
        PluginMetricsImpl::new(self.inner.clone(), tags)
    }

    /// Creates plugin metrics for a task.
    pub fn task_plugin_metrics(&self, connector_task_id: &ConnectorTaskId) -> PluginMetricsImpl {
        let tags = self.task_plugin_tags(connector_task_id);
        PluginMetricsImpl::new(self.inner.clone(), tags)
    }

    /// Returns connector plugin tags.
    fn connector_plugin_tags(&self, connector_id: &str) -> HashMap<String, String> {
        let mut tags = HashMap::new();
        tags.insert("connector".to_string(), connector_id.to_string());
        tags
    }

    /// Returns task plugin tags.
    fn task_plugin_tags(&self, connector_task_id: &ConnectorTaskId) -> HashMap<String, String> {
        let mut tags = self.connector_plugin_tags(connector_task_id.connector());
        tags.insert("task".to_string(), connector_task_id.task().to_string());
        tags
    }

    /// Stops and unregisters the metrics.
    pub fn stop(&self) {
        // Clear all groups
        let groups: Vec<Arc<MetricGroup>> = self
            .inner
            .groups_by_name
            .write()
            .unwrap()
            .drain()
            .map(|(_, v)| v)
            .collect();
        for group in groups {
            group.close();
        }

        log::debug!(
            "Unregistering Connect metrics with JMX for worker '{}'",
            self.worker_id
        );
    }
}

/// Plugin metrics implementation.
///
/// Provides metrics for plugins (connectors, tasks, converters, etc.)
/// Corresponds to `PluginMetricsImpl` in Java.
#[derive(Debug)]
pub struct PluginMetricsImpl {
    /// The inner metrics state
    inner: Arc<ConnectMetricsInner>,
    /// Tags for this plugin
    tags: HashMap<String, String>,
    /// Metric names registered
    metric_names: Mutex<HashSet<String>>,
}

impl PluginMetricsImpl {
    /// Creates a new PluginMetricsImpl.
    pub fn new(inner: Arc<ConnectMetricsInner>, tags: HashMap<String, String>) -> Self {
        PluginMetricsImpl {
            inner,
            tags,
            metric_names: Mutex::new(HashSet::new()),
        }
    }

    /// Returns the tags for this plugin.
    pub fn tags(&self) -> &HashMap<String, String> {
        &self.tags
    }

    /// Adds a metric with the given name and gauge.
    /// Use this method when you want to provide a custom gauge.
    pub fn add_metric_with_gauge(&self, name: &str, gauge: Arc<dyn GaugeValue>) {
        let metric_name = MetricName::new(
            name.to_string(),
            "plugin-metrics".to_string(),
            String::new(),
            self.tags.clone(),
        );
        self.inner
            .metrics
            .write()
            .unwrap()
            .insert(metric_name, gauge);
        self.metric_names.lock().unwrap().insert(name.to_string());
    }

    /// Removes a metric with the given name.
    pub fn remove_metric(&self, name: &str) {
        let metric_name = MetricName::new(
            name.to_string(),
            "plugin-metrics".to_string(),
            String::new(),
            self.tags.clone(),
        );
        self.inner.metrics.write().unwrap().remove(&metric_name);
        self.metric_names.lock().unwrap().remove(name);
    }

    /// Returns all metric names.
    pub fn metric_names(&self) -> Vec<String> {
        self.metric_names.lock().unwrap().iter().cloned().collect()
    }

    /// Returns the metric group prefix.
    pub fn metric_group_prefix(&self) -> String {
        let mut prefix = String::from("plugin-metrics");
        for (k, v) in &self.tags {
            prefix.push_str(&format!(":{}={}", k, v));
        }
        prefix
    }
}

impl common_trait::metrics::PluginMetrics for PluginMetricsImpl {
    fn metric_group_prefix(&self) -> &str {
        // Note: This returns a static string due to trait signature
        // Actual prefix is dynamic based on tags
        "plugin-metrics"
    }

    fn add_metric(&mut self, name: &str) {
        // Default implementation adds a counter gauge
        let gauge = Arc::new(CounterGauge::new(0.0));
        let metric_name = MetricName::new(
            name.to_string(),
            "plugin-metrics".to_string(),
            String::new(),
            self.tags.clone(),
        );
        self.inner
            .metrics
            .write()
            .unwrap()
            .insert(metric_name, gauge);
        self.metric_names.lock().unwrap().insert(name.to_string());
    }

    fn remove_metric(&mut self, name: &str) {
        let metric_name = MetricName::new(
            name.to_string(),
            "plugin-metrics".to_string(),
            String::new(),
            self.tags.clone(),
        );
        self.inner.metrics.write().unwrap().remove(&metric_name);
        self.metric_names.lock().unwrap().remove(name);
    }

    fn metric_names(&self) -> Vec<String> {
        self.metric_names.lock().unwrap().iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_trait::metrics::PluginMetrics;
    use common_trait::util::time::SYSTEM;

    #[test]
    fn test_metric_name_creation() {
        let tags = HashMap::from([
            ("connector".to_string(), "test-connector".to_string()),
            ("task".to_string(), "0".to_string()),
        ]);
        let name = MetricName::new(
            "status".to_string(),
            "connector-metrics".to_string(),
            "The status".to_string(),
            tags,
        );

        assert_eq!(name.name(), "status");
        assert_eq!(name.group(), "connector-metrics");
        assert_eq!(name.tags().len(), 2);
    }

    #[test]
    fn test_metric_name_mbean() {
        let tags = HashMap::from([("connector".to_string(), "test".to_string())]);
        let name = MetricName::new(
            "status".to_string(),
            "connector-metrics".to_string(),
            "".to_string(),
            tags,
        );

        let mbean = name.mbean_name();
        assert!(mbean.starts_with("kafka.connect:type=connector-metrics"));
        assert!(mbean.contains("name=status"));
        assert!(mbean.contains("connector=test"));
    }

    #[test]
    fn test_metric_group_id() {
        let tags = HashMap::from([("connector".to_string(), "test".to_string())]);
        let group_id = MetricGroupId::new("connector-metrics".to_string(), tags);

        assert_eq!(group_id.group_name(), "connector-metrics");
        assert_eq!(group_id.tags().get("connector"), Some(&"test".to_string()));
    }

    #[test]
    fn test_metric_group_id_equality() {
        let tags1 = HashMap::from([
            ("connector".to_string(), "test".to_string()),
            ("task".to_string(), "0".to_string()),
        ]);
        let tags2 = HashMap::from([
            ("task".to_string(), "0".to_string()),
            ("connector".to_string(), "test".to_string()),
        ]);

        let id1 = MetricGroupId::new("connector-metrics".to_string(), tags1);
        let id2 = MetricGroupId::new("connector-metrics".to_string(), tags2);

        assert_eq!(id1, id2);
    }

    #[test]
    fn test_metric_group_id_includes() {
        let tags = HashMap::from([("connector".to_string(), "test".to_string())]);
        let group_id = MetricGroupId::new("connector-metrics".to_string(), tags.clone());

        let metric_name = MetricName::new(
            "status".to_string(),
            "connector-metrics".to_string(),
            "".to_string(),
            tags,
        );

        assert!(group_id.includes(&metric_name));

        let other_metric = MetricName::new(
            "status".to_string(),
            "other-group".to_string(),
            "".to_string(),
            HashMap::new(),
        );
        assert!(!group_id.includes(&other_metric));
    }

    #[test]
    fn test_connect_metrics_creation() {
        let time = SYSTEM.clone();
        let metrics = ConnectMetrics::new("test-worker".to_string(), time);

        assert_eq!(metrics.worker_id(), "test-worker");
    }

    #[test]
    fn test_connect_metrics_group() {
        let time = SYSTEM.clone();
        let metrics = ConnectMetrics::new("test-worker".to_string(), time);

        let group1 = metrics.group("connector-metrics", &[("connector", "test")]);
        let group2 = metrics.group("connector-metrics", &[("connector", "test")]);

        // Same group ID should return same group
        assert_eq!(group1.group_id(), group2.group_id());

        // Different tags should return different group
        let group3 = metrics.group("connector-metrics", &[("connector", "other")]);
        assert_ne!(group1.group_id(), group3.group_id());
    }

    #[test]
    fn test_metric_group_sensor() {
        let time = SYSTEM.clone();
        let metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = metrics.group("connector-metrics", &[("connector", "test")]);

        let sensor1 = group.sensor("poll-rate");
        let sensor2 = group.sensor("poll-rate");

        // Same name should return same sensor
        assert_eq!(sensor1.name(), sensor2.name());

        // Different name should return different sensor
        let sensor3 = group.sensor("write-rate");
        assert_ne!(sensor1.name(), sensor3.name());
    }

    #[test]
    fn test_metric_group_metric_name() {
        let time = SYSTEM.clone();
        let metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let registry = metrics.registry();
        let group = metrics.group(registry.connector_group_name(), &[("connector", "test")]);

        let metric_name = group.metric_name(registry.connector_status());

        assert_eq!(metric_name.name(), "status");
        assert_eq!(metric_name.group(), "connector-metrics");
        assert_eq!(
            metric_name.tags().get("connector"),
            Some(&"test".to_string())
        );
    }

    #[test]
    fn test_recording_level() {
        assert_eq!(RecordingLevel::from_str("INFO"), RecordingLevel::Info);
        assert_eq!(RecordingLevel::from_str("DEBUG"), RecordingLevel::Debug);
        assert_eq!(RecordingLevel::from_str("info"), RecordingLevel::Info);
        assert_eq!(RecordingLevel::from_str("unknown"), RecordingLevel::Info);
    }

    #[test]
    fn test_constant_gauge() {
        let gauge = ConstantGauge::new(42.0);
        assert_eq!(gauge.value(), 42.0);
    }

    #[test]
    fn test_counter_gauge() {
        let gauge = CounterGauge::new(0.0);
        assert_eq!(gauge.value(), 0.0);

        gauge.increment(5.0);
        assert_eq!(gauge.value(), 5.0);

        gauge.increment(3.0);
        assert_eq!(gauge.value(), 8.0);

        gauge.set(100.0);
        assert_eq!(gauge.value(), 100.0);
    }

    #[test]
    fn test_plugin_metrics_impl() {
        let time = SYSTEM.clone();
        let metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let plugin_metrics = metrics.connector_plugin_metrics("test-connector");

        assert_eq!(
            plugin_metrics.tags().get("connector"),
            Some(&"test-connector".to_string())
        );
        assert!(plugin_metrics.metric_names().is_empty());
    }

    #[test]
    fn test_plugin_metrics_add_remove() {
        let time = SYSTEM.clone();
        let metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let mut plugin_metrics = metrics.connector_plugin_metrics("test-connector");

        plugin_metrics.add_metric("custom-counter");
        assert_eq!(plugin_metrics.metric_names().len(), 1);

        plugin_metrics.remove_metric("custom-counter");
        assert!(plugin_metrics.metric_names().is_empty());
    }

    #[test]
    fn test_connect_metrics_stop() {
        let time = SYSTEM.clone();
        let metrics = ConnectMetrics::new("test-worker".to_string(), time);

        // Create some groups
        let group1 = metrics.group("connector-metrics", &[("connector", "test")]);
        let sensor = group1.sensor("test-sensor");

        // Stop should clear all groups
        metrics.stop();

        // Groups should be cleared
        assert!(metrics.inner.groups_by_name.read().unwrap().is_empty());
    }

    #[test]
    fn test_metric_group_close() {
        let time = SYSTEM.clone();
        let metrics = ConnectMetrics::new("test-worker".to_string(), time);
        let group = metrics.group("connector-metrics", &[("connector", "test")]);

        // Add sensor
        let sensor = group.sensor("test-sensor");
        assert!(!sensor.name().is_empty());

        // Close should remove sensors
        group.close();

        // Sensor names should be cleared
        assert!(group.sensor_names.read().unwrap().is_empty());
    }

    #[test]
    fn test_metric_from_template() {
        let registry = ConnectMetricsRegistry::new();
        let template = registry.source_record_poll_rate();

        let tags = HashMap::from([
            ("connector".to_string(), "test".to_string()),
            ("task".to_string(), "0".to_string()),
        ]);
        let metric_name = MetricName::from_template(template, tags);

        assert_eq!(metric_name.name(), "source-record-poll-rate");
        assert_eq!(metric_name.group(), "source-task-metrics");
    }
}
