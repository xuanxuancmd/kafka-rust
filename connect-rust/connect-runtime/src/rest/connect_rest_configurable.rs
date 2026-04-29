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

//! ConnectRestConfigurable - Wrapper for Axum Router with duplicate registration prevention.
//!
//! This implementation wraps an Axum Router and handles duplicate registrations
//! deterministically by not re-registering them again, similar to how Java's
//! `ConnectRestConfigurable` wraps `ResourceConfig`.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.rest.ConnectRestConfigurable` in Java.

use axum::Router;
use log::warn;
use std::any::type_name;
use std::collections::HashSet;
use std::mem::take;

/// Wrapper around Axum Router that prevents duplicate registrations.
///
/// In Java, this wraps Jersey's `ResourceConfig` to handle duplicate registrations
/// deterministically. In Rust, we adapt this to work with Axum's Router by tracking
/// registered component type names.
///
/// Unlike Java's JAX-RS which has explicit registration methods for components,
/// Axum uses a different approach where routes are added via `.route()` method.
/// This struct provides a compatibility layer for REST extension registration.
pub struct ConnectRestConfigurable {
    /// The underlying Axum Router.
    router: Router,
    /// Set of registered component type names (for duplicate detection).
    registered_types: HashSet<String>,
    /// Properties stored for extension configuration.
    properties: Vec<(String, String)>,
}

impl ConnectRestConfigurable {
    /// Creates a new ConnectRestConfigurable wrapping an empty Router.
    pub fn new() -> Self {
        ConnectRestConfigurable {
            router: Router::new(),
            registered_types: HashSet::new(),
            properties: Vec::new(),
        }
    }

    /// Creates a new ConnectRestConfigurable wrapping the given Router.
    ///
    /// # Arguments
    ///
    /// * `router` - The Axum Router to wrap
    pub fn with_router(router: Router) -> Self {
        ConnectRestConfigurable {
            router,
            registered_types: HashSet::new(),
            properties: Vec::new(),
        }
    }

    /// Adds a property to the configuration.
    ///
    /// In Java JAX-RS, `property()` sets a property on the ResourceConfig.
    /// In this Rust adaptation, properties are stored for later use.
    ///
    /// # Arguments
    ///
    /// * `name` - Property name
    /// * `value` - Property value
    ///
    /// # Returns
    ///
    /// Self reference for chaining
    pub fn property(&mut self, name: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.properties.push((name.into(), value.into()));
        self
    }

    /// Registers a route with the router.
    ///
    /// This method adds routes to the underlying Router without duplicate checking,
    /// since Axum routes are identified by path/method combination, not by component type.
    ///
    /// # Arguments
    ///
    /// * `path` - Route path
    /// * `method_router` - Method router for the path
    ///
    /// # Returns
    ///
    /// Self reference for chaining
    pub fn route(&mut self, path: &str, method_router: axum::routing::MethodRouter) -> &mut Self {
        let router = take(&mut self.router);
        self.router = router.route(path, method_router);
        self
    }

    /// Registers a component type by name.
    ///
    /// This tracks component type registration to prevent duplicates,
    /// similar to Java's `register()` method behavior.
    ///
    /// # Arguments
    ///
    /// * `component_type_name` - The type name of the component to register
    ///
    /// # Returns
    ///
    /// `true` if registration was allowed (not a duplicate), `false` otherwise
    pub fn register_type(&mut self, component_type_name: &str) -> bool {
        if self.registered_types.contains(component_type_name) {
            warn!("The resource {} is already registered", component_type_name);
            return false;
        }
        self.registered_types.insert(component_type_name.to_string());
        true
    }

    /// Registers a component of type T.
    ///
    /// This method checks if the component type has already been registered
    /// and logs a warning if it's a duplicate registration.
    ///
    /// # Returns
    ///
    /// `true` if registration was allowed (not a duplicate), `false` otherwise
    pub fn register<T: 'static>(&mut self) -> bool {
        let type_name = type_name::<T>();
        self.register_type(type_name)
    }

    /// Merges another Router into this configurable.
    ///
    /// This is used to combine routes from REST extensions.
    ///
    /// # Arguments
    ///
    /// * `other` - The Router to merge
    pub fn merge_router(&mut self, other: Router) {
        let router = take(&mut self.router);
        self.router = router.merge(other);
    }

    /// Returns the underlying Router.
    ///
    /// The Router can be used to start the HTTP server.
    pub fn router(&self) -> &Router {
        &self.router
    }

    /// Returns the underlying Router for modification.
    pub fn router_mut(&mut self) -> &mut Router {
        &mut self.router
    }

    /// Takes ownership of the underlying Router.
    ///
    /// After calling this method, the ConnectRestConfigurable should not be used.
    pub fn into_router(self) -> Router {
        self.router
    }

    /// Returns the stored properties.
    pub fn properties(&self) -> &[(String, String)] {
        &self.properties
    }

    /// Returns whether a component type is already registered.
    pub fn is_registered(&self, component_type_name: &str) -> bool {
        self.registered_types.contains(component_type_name)
    }

    /// Returns the set of registered component type names.
    pub fn registered_types(&self) -> &HashSet<String> {
        &self.registered_types
    }

    /// Adds a layer to the router.
    ///
    /// This method provides compatibility with Axum's layer API.
    /// The layer must satisfy Axum's layer requirements.
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer to add
    pub fn layer<L>(&mut self, layer: L) -> &mut Self
    where
        L: tower::Layer<axum::routing::Route> + Clone + Send + Sync + 'static,
        L::Service: tower::Service<axum::http::Request<axum::body::Body>> + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<axum::http::Request<axum::body::Body>>>::Future: Send + 'static,
        <L::Service as tower::Service<axum::http::Request<axum::body::Body>>>::Error: Into<std::convert::Infallible>,
        <L::Service as tower::Service<axum::http::Request<axum::body::Body>>>::Response: axum::response::IntoResponse + Send + 'static,
    {
        let router = take(&mut self.router);
        self.router = router.layer(layer);
        self
    }

    /// Adds an extension to the router.
    ///
    /// This method provides compatibility with Axum's Extension layer.
    ///
    /// # Arguments
    ///
    /// * `extension` - The extension value to add
    pub fn extension<T>(&mut self, extension: T) -> &mut Self
    where
        T: Clone + Send + Sync + 'static,
    {
        let router = take(&mut self.router);
        self.router = router.layer(axum::Extension(extension));
        self
    }
}

impl Default for ConnectRestConfigurable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ConnectRestConfigurable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectRestConfigurable")
            .field("registered_types", &self.registered_types)
            .field("properties", &self.properties)
            .finish_non_exhaustive()
    }
}

impl Clone for ConnectRestConfigurable {
    fn clone(&self) -> Self {
        ConnectRestConfigurable {
            router: self.router.clone(),
            registered_types: self.registered_types.clone(),
            properties: self.properties.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;

    #[test]
    fn test_new() {
        let configurable = ConnectRestConfigurable::new();
        assert!(configurable.registered_types().is_empty());
        assert!(configurable.properties().is_empty());
    }

    #[test]
    fn test_property() {
        let mut configurable = ConnectRestConfigurable::new();
        configurable.property("key1", "value1").property("key2", "value2");

        assert_eq!(configurable.properties().len(), 2);
        assert_eq!(configurable.properties()[0], ("key1".to_string(), "value1".to_string()));
        assert_eq!(configurable.properties()[1], ("key2".to_string(), "value2".to_string()));
    }

    #[test]
    fn test_register_type() {
        let mut configurable = ConnectRestConfigurable::new();

        // First registration should succeed
        assert!(configurable.register_type("MyResource"));
        assert!(configurable.is_registered("MyResource"));

        // Duplicate registration should fail with warning logged
        assert!(!configurable.register_type("MyResource"));
        assert!(configurable.is_registered("MyResource"));
    }

    #[test]
    fn test_register_generic() {
        let mut configurable = ConnectRestConfigurable::new();

        // Register a type
        assert!(configurable.register::<String>());
        assert!(configurable.is_registered(type_name::<String>()));

        // Duplicate should fail
        assert!(!configurable.register::<String>());
    }

    #[test]
    fn test_route() {
        let mut configurable = ConnectRestConfigurable::new();
        configurable.route("/test", get(|| async { "test" }));

        // Router should have the route - verify router exists
        // Note: Axum Router doesn't expose routes() method in 0.7
        // We can verify the router exists and was created
        let _router = configurable.router();
    }

    #[test]
    fn test_clone() {
        let mut configurable = ConnectRestConfigurable::new();
        configurable.property("key", "value");
        configurable.register_type("Resource1");

        let cloned = configurable.clone();
        assert!(cloned.is_registered("Resource1"));
        assert_eq!(cloned.properties()[0].0, "key");
    }

    #[test]
    fn test_default() {
        let configurable = ConnectRestConfigurable::default();
        assert!(configurable.registered_types().is_empty());
    }

    #[test]
    fn test_into_router() {
        let mut configurable = ConnectRestConfigurable::new();
        configurable.route("/test", get(|| async { "test" }));

        let router = configurable.into_router();
        // Router is extracted - verify we can use it
        // Note: Axum Router doesn't expose routes() method
        let _router = router;
    }
}