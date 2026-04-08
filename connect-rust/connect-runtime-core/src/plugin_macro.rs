//! Plugin Registration Macro
//!
//! Provides compile-time plugin registration macros.

/// Register a plugin at compile time
///
/// # Arguments
///
/// * `$name` - Plugin name (string literal)
/// * `$type` - Plugin type (Source, Sink, or Transformation)
/// * `$version` - Plugin version (string literal)
/// * `$class_name` - Class name for Java compatibility (string literal)
/// * `$factory_fn` - Factory function that creates plugin instances
///
/// # Example
///
/// ```ignore
/// use connect_runtime_core::register_plugin;
/// use connect_api::SourceConnector;
///
/// struct MySourceConnector;
/// impl SourceConnector for MySourceConnector {
///     fn version(&self) -> String { "1.0.0".to_string() }
///     fn config_class(&self) -> Box<dyn connect_api::connector::Config> {
///         unimplemented!()
///     }
///     fn init(&mut self, _config: connect_api::connector::Config) {}
///     fn start(&self, _props: std::collections::HashMap<String, String>) -> Result<(), String> { Ok(()) }
///     fn task_class(&self) -> Vec<String> { vec![] }
///     fn init_task(&self, _config: connect_api::connector::Config) -> Result<Box<dyn connect_api::SourceTask>, String> { unimplemented!() }
/// }
///
/// fn create_my_source() -> Result<Box<dyn SourceConnector>, String> {
///     Ok(Box::new(MySourceConnector))
/// }
///
/// register_plugin!(
///     "MySourceConnector",
///     Source,
///     "1.0.0",
///     "com.example.MySourceConnector",
///     create_my_source
/// );
/// ```
#[macro_export]
macro_rules! register_plugin {
    (
        $name:expr,
        Source,
        $version:expr,
        $class_name:expr,
        $factory_fn:expr
    ) => {
        #[doc(hidden)]
        mod __connect_plugin_ {
            use super::*;
            use connect_api::SourceConnector;
            use connect_runtime_core::plugin_registry::{
                register_plugin, PluginDescriptor, PluginFactory, PluginType,
            };
            use std::sync::Arc;

            struct PluginFactoryImpl;

            impl PluginFactory for PluginFactoryImpl {
                fn create_source_connector(&self) -> Result<Box<dyn SourceConnector>, String> {
                    $factory_fn()
                }

                fn create_sink_connector(
                    &self,
                ) -> Result<Box<dyn connect_api::SinkConnector>, String> {
                    Err("This is a source connector".to_string())
                }

                fn descriptor(&self) -> PluginDescriptor {
                    PluginDescriptor {
                        name: $name.to_string(),
                        plugin_type: PluginType::Source,
                        version: $version.to_string(),
                        class_name: $class_name.to_string(),
                    }
                }
            }

            // Register the plugin at program startup
            #[used]
            #[link_section = ".connect_plugins"]
            static __PLUGIN_REGISTER: () = {
                let _ = || {
                    register_plugin(Arc::new(PluginFactoryImpl));
                };
            };
        }
    };

    (
        $name:expr,
        Sink,
        $version:expr,
        $class_name:expr,
        $factory_fn:expr
    ) => {
        #[doc(hidden)]
        mod __connect_plugin_ {
            use super::*;
            use connect_api::SinkConnector;
            use connect_runtime_core::plugin_registry::{
                register_plugin, PluginDescriptor, PluginFactory, PluginType,
            };
            use std::sync::Arc;

            struct PluginFactoryImpl;

            impl PluginFactory for PluginFactoryImpl {
                fn create_source_connector(
                    &self,
                ) -> Result<Box<dyn connect_api::SourceConnector>, String> {
                    Err("This is a sink connector".to_string())
                }

                fn create_sink_connector(&self) -> Result<Box<dyn SinkConnector>, String> {
                    $factory_fn()
                }

                fn descriptor(&self) -> PluginDescriptor {
                    PluginDescriptor {
                        name: $name.to_string(),
                        plugin_type: PluginType::Sink,
                        version: $version.to_string(),
                        class_name: $class_name.to_string(),
                    }
                }
            }

            // Register the plugin at program startup
            #[used]
            #[link_section = ".connect_plugins"]
            static __PLUGIN_REGISTER: () = {
                let _ = || {
                    register_plugin(Arc::new(PluginFactoryImpl));
                };
            };
        }
    };

    (
        $name:expr,
        Transformation,
        $version:expr,
        $class_name:expr,
        $factory_fn:expr
    ) => {
        #[doc(hidden)]
        mod __connect_plugin_ {
            use super::*;
            use connect_api::Transformation;
            use connect_runtime_core::plugin_registry::{
                register_plugin, PluginDescriptor, PluginFactory, PluginType,
            };
            use std::sync::Arc;

            struct PluginFactoryImpl;

            impl PluginFactory for PluginFactoryImpl {
                fn create_source_connector(
                    &self,
                ) -> Result<Box<dyn connect_api::SourceConnector>, String> {
                    Err("This is a transformation".to_string())
                }

                fn create_sink_connector(
                    &self,
                ) -> Result<Box<dyn connect_api::SinkConnector>, String> {
                    Err("This is a transformation".to_string())
                }

                fn descriptor(&self) -> PluginDescriptor {
                    PluginDescriptor {
                        name: $name.to_string(),
                        plugin_type: PluginType::Transformation,
                        version: $version.to_string(),
                        class_name: $class_name.to_string(),
                    }
                }
            }

            // Register the plugin at program startup
            #[used]
            #[link_section = ".connect_plugins"]
            static __PLUGIN_REGISTER: () = {
                let _ = || {
                    register_plugin(Arc::new(PluginFactoryImpl));
                };
            };
        }
    };
}

/// Declare a plugin module with automatic registration
///
/// This macro creates a plugin module and registers it automatically.
///
/// # Example
///
/// ```ignore
/// use connect_runtime_core::declare_plugin;
/// use connect_api::SourceConnector;
///
/// struct MySourcePlugin;
/// impl MySourcePlugin {
///     fn new() -> Self { MySourcePlugin }
/// }
/// impl SourceConnector for MySourcePlugin {
///     fn version(&self) -> String { "1.0.0".to_string() }
///     fn config_class(&self) -> Box<dyn connect_api::connector::Config> { unimplemented!() }
///     fn init(&mut self, _config: connect_api::connector::Config) {}
///     fn start(&self, _props: std::collections::HashMap<String, String>) -> Result<(), String> { Ok(()) }
///     fn task_class(&self) -> Vec<String> { vec![] }
///     fn init_task(&self, _config: connect_api::connector::Config) -> Result<Box<dyn connect_api::SourceTask>, String> { unimplemented!() }
/// }
///
/// declare_plugin!(
///     MySourcePlugin,
///     Source,
///     "1.0.0",
///     "com.example.MySourcePlugin"
/// );
/// ```
#[macro_export]
macro_rules! declare_plugin {
    (
        $struct_name:ident,
        Source,
        $version:expr,
        $class_name:expr
    ) => {
        paste::paste! {
            #[doc(hidden)]
            mod [<__connect_plugin_ $struct_name>] {
                use super::*;
                use connect_runtime_core::plugin_registry::{
                    PluginDescriptor, PluginFactory, PluginType, register_plugin,
                };
                use connect_api::SourceConnector;
                use std::sync::Arc;

                struct PluginFactoryImpl;

                impl PluginFactory for PluginFactoryImpl {
                    fn create_source_connector(&self) -> Result<Box<dyn SourceConnector>, String> {
                        Ok(Box::new($struct_name::new()))
                    }

                    fn create_sink_connector(&self) -> Result<Box<dyn connect_api::SinkConnector>, String> {
                        Err("This is a source connector".to_string())
                    }

                    fn descriptor(&self) -> PluginDescriptor {
                        PluginDescriptor {
                            name: stringify!($struct_name).to_string(),
                            plugin_type: PluginType::Source,
                            version: $version.to_string(),
                            class_name: $class_name.to_string(),
                        }
                    }
                }

                // Register the plugin at program startup
                #[used]
                #[link_section = ".connect_plugins"]
                static [<__PLUGIN_REGISTER_ $struct_name>]: () = {
                    let _ = || {
                        register_plugin(Arc::new(PluginFactoryImpl));
                    };
                };
            }
        }
    };

    (
        $struct_name:ident,
        Sink,
        $version:expr,
        $class_name:expr
    ) => {
        paste::paste! {
            #[doc(hidden)]
            mod [<__connect_plugin_ $struct_name>] {
                use super::*;
                use connect_runtime_core::plugin_registry::{
                    PluginDescriptor, PluginFactory, PluginType, register_plugin,
                };
                use connect_api::SinkConnector;
                use std::sync::Arc;

                struct PluginFactoryImpl;

                impl PluginFactory for PluginFactoryImpl {
                    fn create_source_connector(&self) -> Result<Box<dyn connect_api::SourceConnector>, String> {
                        Err("This is a sink connector".to_string())
                    }

                    fn create_sink_connector(&self) -> Result<Box<dyn SinkConnector>, String> {
                        Ok(Box::new($struct_name::new()))
                    }

                    fn descriptor(&self) -> PluginDescriptor {
                        PluginDescriptor {
                            name: stringify!($struct_name).to_string(),
                            plugin_type: PluginType::Sink,
                            version: $version.to_string(),
                            class_name: $class_name.to_string(),
                        }
                    }
                }

                // Register the plugin at program startup
                #[used]
                #[link_section = ".connect_plugins"]
                static [<__PLUGIN_REGISTER_ $struct_name>]: () = {
                    let _ = || {
                        register_plugin(Arc::new(PluginFactoryImpl));
                    };
                };
            }
        }
    };
}
