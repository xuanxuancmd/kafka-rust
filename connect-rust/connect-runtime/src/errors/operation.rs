//! Operation module
//!
//! Provides the Operation trait for recoverable operations in the connector pipeline.

/// A recoverable operation evaluated in the connector pipeline.
pub trait Operation<V> {
    /// Execute the operation and return the result
    fn call(&self) -> Result<V, Box<dyn std::error::Error + Send + Sync>>;
}

/// Function-based operation implementation
pub struct FunctionOperation<V, F>
where
    F: Fn() -> Result<V, Box<dyn std::error::Error + Send + Sync>> + Send + Sync,
{
    func: F,
}

impl<V, F> FunctionOperation<V, F>
where
    F: Fn() -> Result<V, Box<dyn std::error::Error + Send + Sync>> + Send + Sync,
{
    /// Create a new function-based operation
    pub fn new(func: F) -> Self {
        Self { func }
    }
}

impl<V, F> Operation<V> for FunctionOperation<V, F>
where
    F: Fn() -> Result<V, Box<dyn std::error::Error + Send + Sync>> + Send + Sync,
{
    fn call(&self) -> Result<V, Box<dyn std::error::Error + Send + Sync>> {
        (self.func)()
    }
}
