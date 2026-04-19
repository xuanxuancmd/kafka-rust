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

use std::sync::Arc;

use super::Type;

/// Field definition - corresponds to Java: org.apache.kafka.common.protocol.types.Field
pub struct Field {
    pub name: String,
    pub doc_string: Option<String>,
    pub type_ref: Arc<dyn Type>,
    pub has_default_value: bool,
}

impl Field {
    pub fn new(name: impl Into<String>, type_ref: Arc<dyn Type>) -> Self {
        Self {
            name: name.into(),
            doc_string: None,
            type_ref,
            has_default_value: false,
        }
    }

    pub fn with_doc(
        name: impl Into<String>,
        type_ref: Arc<dyn Type>,
        doc_string: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            doc_string: Some(doc_string.into()),
            type_ref,
            has_default_value: false,
        }
    }

    pub fn with_default(
        name: impl Into<String>,
        type_ref: Arc<dyn Type>,
        doc_string: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            doc_string,
            type_ref,
            has_default_value: true,
        }
    }
}

/// A field definition bound to a particular schema - corresponds to Java: org.apache.kafka.common.protocol.types.BoundField
pub struct BoundField {
    pub def: Field,
    pub index: usize,
}

impl BoundField {
    pub fn new(def: Field, index: usize) -> Self {
        Self { def, index }
    }
}

impl std::fmt::Display for BoundField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.def.name, self.def.type_ref.type_name())
    }
}
