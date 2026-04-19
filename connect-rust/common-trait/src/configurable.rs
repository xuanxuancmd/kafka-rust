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

use serde_json::Value;
use std::collections::HashMap;

/// Configurable trait for components that can be configured.
///
/// This corresponds to `org.apache.kafka.common.Configurable` in Java.
pub trait Configurable {
    /// Configure this component with the given key-value pairs.
    ///
    /// # Arguments
    /// * `configs` - A map of configuration key-value pairs
    fn configure(&mut self, configs: HashMap<String, Value>);
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestConfigurable {
        configured: bool,
    }

    impl Configurable for TestConfigurable {
        fn configure(&mut self, configs: HashMap<String, Value>) {
            self.configured = true;
        }
    }

    #[test]
    fn test_configure() {
        let mut configurable = TestConfigurable { configured: false };
        let configs = HashMap::new();
        configurable.configure(configs);
        assert!(configurable.configured);
    }
}
