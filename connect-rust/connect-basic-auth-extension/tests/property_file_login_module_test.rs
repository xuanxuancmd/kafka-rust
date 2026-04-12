//! Unit tests for PropertyFileLoginModule

use connect_basic_auth_extension::jaas::{
    Callback, CallbackHandler, LoginModule, Subject,
};
use connect_basic_auth_extension::property_file_login_module::PropertyFileLoginModule;
use connect_basic_auth_extension::error::LoginException;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

#[test]
fn test_successful_authentication() {
    // Create a temporary credentials file
    let temp_file = create_temp_credentials_file(true);
    let file_path = temp_file.to_str().unwrap().to_string();

    // Create module options
    let mut options = HashMap::new();
    options.insert("file".to_string(), file_path);

    // Create and initialize module
    let mut module = PropertyFileLoginModule::new();
    let subject = Subject::new();
    let callback_handler = TestCallbackHandler::new("admin".to_string(), "password".to_string());
    let shared_state = HashMap::new();

    assert!(module
        .initialize(&subject, &callback_handler, &shared_state, &options)
        .is_ok());

    // Login should succeed
    assert!(module.login().unwrap());

    // Commit should return true
    assert!(module.commit());

    // Logout should return true
    assert!(module.logout());

    // Cleanup
    std::fs::remove_file(&temp_file).unwrap();
}

#[test]
fn test_failed_authentication_wrong_password() {
    let temp_file = create_temp_credentials_file(true);
    let file_path = temp_file.to_str().unwrap().to_string();

    let mut options = HashMap::new();
    options.insert("file".to_string(), file_path);

    let mut module = PropertyFileLoginModule::new();
    let subject = Subject::new();
    let callback_handler =
        TestCallbackHandler::new("admin".to_string(), "wrongpassword".to_string());
    let shared_state = HashMap::new();

    assert!(module
        .initialize(&subject, &callback_handler, &shared_state, &options)
        .is_ok());

    // Login should fail
    assert!(!module.login().unwrap());

    // Abort should return true
    assert!(module.abort());

    std::fs::remove_file(&temp_file).unwrap();
}

#[test]
fn test_failed_authentication_user_not_found() {
    let temp_file = create_temp_credentials_file(true);
    let file_path = temp_file.to_str().unwrap().to_string();

    let mut options = HashMap::new();
    options.insert("file".to_string(), file_path);

    let mut module = PropertyFileLoginModule::new();
    let subject = Subject::new();
    let callback_handler =
        TestCallbackHandler::new("nonexistent".to_string(), "password".to_string());
    let shared_state = HashMap::new();

    assert!(module
        .initialize(&subject, &callback_handler, &shared_state, &options)
        .is_ok());

    // Login should fail
    assert!(!module.login().unwrap());

    std::fs::remove_file(&temp_file).unwrap();
}

#[test]
fn test_empty_credentials_file_allows_all() {
    let temp_file = create_temp_credentials_file(false);
    let file_path = temp_file.to_str().unwrap().to_string();

    let mut options = HashMap::new();
    options.insert("file".to_string(), file_path);

    let mut module = PropertyFileLoginModule::new();
    let subject = Subject::new();
    let callback_handler =
        TestCallbackHandler::new("anyuser".to_string(), "anypassword".to_string());
    let shared_state = HashMap::new();

    assert!(module
        .initialize(&subject, &callback_handler, &shared_state, &options)
        .is_ok());

    // Login should succeed even with any credentials
    assert!(module.login().unwrap());

    std::fs::remove_file(&temp_file).unwrap();
}

#[test]
fn test_file_not_found() {
    let mut options = HashMap::new();
    options.insert(
        "file".to_string(),
        "/nonexistent/file.properties".to_string(),
    );

    let mut module = PropertyFileLoginModule::new();
    let subject = Subject::new();
    let callback_handler = TestCallbackHandler::new("admin".to_string(), "password".to_string());
    let shared_state = HashMap::new();

    // Initialize should fail with IO error
    assert!(module
        .initialize(&subject, &callback_handler, &shared_state, &options)
        .is_err());
}

#[test]
fn test_no_file_option() {
    let options = HashMap::new();

    let mut module = PropertyFileLoginModule::new();
    let subject = Subject::new();
    let callback_handler = TestCallbackHandler::new("admin".to_string(), "password".to_string());
    let shared_state = HashMap::new();

    // Initialize should fail with configuration error (missing file option)
    assert!(module
        .initialize(&subject, &callback_handler, &shared_state, &options)
        .is_err());
    
    // Verify it's a ConfigurationError
    match module
        .initialize(&subject, &callback_handler, &shared_state, &options)
    {
        Err(LoginException::ConfigurationError(_)) => {}
        _ => panic!("Expected ConfigurationError"),
    }
}

#[test]
fn test_concurrent_access() {
    let temp_file = create_temp_credentials_file(true);
    let file_path = temp_file.to_str().unwrap().to_string();

    let mut options = HashMap::new();
    options.insert("file".to_string(), file_path);

    // Create multiple modules to test concurrent access
    let mut handles = Vec::new();
    for _ in 0..10 {
        let mut module = PropertyFileLoginModule::new();
        let subject = Subject::new();
        let callback_handler =
            TestCallbackHandler::new("admin".to_string(), "password".to_string());
        let shared_state = HashMap::new();

        assert!(module
            .initialize(&subject, &callback_handler, &shared_state, &options)
            .is_ok());

        let handle = std::thread::spawn(move || {
            assert!(module.login().unwrap());
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    std::fs::remove_file(&temp_file).unwrap();
}

// Helper function to create a temporary credentials file
fn create_temp_credentials_file(include_users: bool) -> PathBuf {
    use std::env;
    let mut temp_file = env::temp_dir();
    temp_file.push(format!("credentials_{}.properties", std::process::id()));

    if include_users {
        let mut file = File::create(&temp_file).unwrap();
        writeln!(file, "admin=password").unwrap();
        writeln!(file, "user1=password1").unwrap();
    } else {
        File::create(&temp_file).unwrap();
    }

    temp_file
}

// Test callback handler
struct TestCallbackHandler {
    username: String,
    password: String,
}

impl TestCallbackHandler {
    fn new(username: String, password: String) -> Self {
        TestCallbackHandler { username, password }
    }
}

impl CallbackHandler for TestCallbackHandler {
    fn handle(
        &self,
        callbacks: &mut [Box<dyn Callback>],
    ) -> Result<(), LoginException> {
        for callback in callbacks.iter_mut() {
            if let Some(name_callback) = callback.as_name_callback() {
                name_callback.set_name(self.username.clone());
            } else if let Some(password_callback) = callback.as_password_callback() {
                password_callback.set_password(self.password.clone());
            }
        }
        Ok(())
    }
}
