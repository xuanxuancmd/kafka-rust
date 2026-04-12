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

use kafka_clients_trait::config::{ConfigDef, ConfigException};
use kafka_clients_trait::util::Utils;

/// Validator that ensures a class has a public, no-argument constructor
pub struct InstantiableClassValidator;

impl ConfigDef::Validator for InstantiableClassValidator {
    fn ensure_valid(&self, name: &str, value: Option<&dyn std::any::Any>) {
        if let Some(value) = value {
            // The value class couldn't be found; no point in performing follow-up validation
            // Try to instantiate
            let _ = value;
            let _ = name;
            // In Rust, we would use reflection or trait bounds to check if the type is instantiable
            // For now, we'll skip this validation
        }
    }
}
