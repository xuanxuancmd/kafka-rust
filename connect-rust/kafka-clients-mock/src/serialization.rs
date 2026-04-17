//! Mock Serialization Implementation
//!
//! This module provides mock implementations of Serializer and Deserializer for testing purposes.

use common_trait::serialization::{
    DeserializationError, Deserializer, SerializationError, Serializer,
};
use std::sync::{Arc, Mutex};

/// Mock serializer for testing.
pub struct MockSerializer {
    /// Whether serialization should succeed
    should_succeed: Arc<Mutex<bool>>,
    /// Serialized data to return
    mock_data: Arc<Mutex<Vec<u8>>>,
}

impl MockSerializer {
    /// Create a new mock serializer
    pub fn new() -> Self {
        MockSerializer {
            should_succeed: Arc::new(Mutex::new(true)),
            mock_data: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Create a mock serializer that always fails
    pub fn failing() -> Self {
        MockSerializer {
            should_succeed: Arc::new(Mutex::new(false)),
            mock_data: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Set the mock data to return
    pub fn set_mock_data(&self, data: Vec<u8>) {
        let mut mock_data = self.mock_data.lock().unwrap();
        *mock_data = data;
    }

    /// Set whether serialization should succeed
    pub fn set_should_succeed(&self, succeed: bool) {
        let mut should_succeed = self.should_succeed.lock().unwrap();
        *should_succeed = succeed;
    }
}

impl Default for MockSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Serializer<T> for MockSerializer
where
    T: AsRef<[u8]> + Send + 'static,
{
    fn serialize(&self, _value: &T) -> Result<Vec<u8>, SerializationError> {
        let should_succeed = *self.should_succeed.lock().unwrap();
        if !should_succeed {
            return Err(SerializationError::InvalidData(
                "Mock serialization failed".to_string(),
            ));
        }

        // Return the mock data if set, otherwise return empty data
        let mock_data = self.mock_data.lock().unwrap();
        if mock_data.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(mock_data.clone())
        }
    }

    fn close(&self) {
        // Mock close is a no-op
    }
}

/// Mock deserializer for testing.
pub struct MockDeserializer {
    /// Whether deserialization should succeed
    should_succeed: Arc<Mutex<bool>>,
    /// Deserialized data to return
    mock_data: Arc<Mutex<Vec<u8>>>,
}

impl MockDeserializer {
    /// Create a new mock deserializer
    pub fn new() -> Self {
        MockDeserializer {
            should_succeed: Arc::new(Mutex::new(true)),
            mock_data: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Create a mock deserializer that always fails
    pub fn failing() -> Self {
        MockDeserializer {
            should_succeed: Arc::new(Mutex::new(false)),
            mock_data: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Set the mock data to return
    pub fn set_mock_data(&self, data: Vec<u8>) {
        let mut mock_data = self.mock_data.lock().unwrap();
        *mock_data = data;
    }

    /// Set whether deserialization should succeed
    pub fn set_should_succeed(&self, succeed: bool) {
        let mut should_succeed = self.should_succeed.lock().unwrap();
        *should_succeed = succeed;
    }
}

impl Default for MockDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Deserializer<T> for MockDeserializer
where
    T: From<Vec<u8>> + Send + 'static,
{
    fn deserialize(&self, _data: &[u8]) -> Result<T, DeserializationError> {
        let should_succeed = *self.should_succeed.lock().unwrap();
        if !should_succeed {
            return Err(DeserializationError::InvalidData(
                "Mock deserialization failed".to_string(),
            ));
        }

        // Return the mock data if set, otherwise return empty data
        let mock_data = self.mock_data.lock().unwrap();
        if mock_data.is_empty() {
            Ok(T::from(Vec::new()))
        } else {
            Ok(T::from(mock_data.clone()))
        }
    }

    fn close(&self) {
        // Mock close is a no-op
    }
}
