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

use crate::util::Callback;
use common_trait::config::ConfigDef;
use common_trait::util::Utils;
use std::any::Any;

/// Validator that ensures a class is a concrete subclass of a given superclass
pub struct ConcreteSubClassValidator {
    expected_super_class: &'static dyn Any,
}

impl ConcreteSubClassValidator {
    /// Create a new validator for the specified superclass
    pub fn for_super_class(expected_super_class: &'static dyn Any) -> Self {
        Self {
            expected_super_class,
        }
    }
}

impl ConfigDef::Validator for ConcreteSubClassValidator {
    fn ensure_valid(&self, _name: &str, value: Option<&dyn Any>) {
        if let Some(value) = value {
            // The value will be null if the class couldn't be found; no point in performing follow-up validation
            Utils::ensure_concrete_subclass(self.expected_super_class, value);
        }
    }

    fn to_string(&self) -> String {
        format!("A concrete subclass of {:?}", self.expected_super_class)
    }
}
