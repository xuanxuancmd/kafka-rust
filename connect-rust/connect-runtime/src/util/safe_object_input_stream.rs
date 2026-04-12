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

/// SafeObjectInputStream extends ObjectInputStream with security restrictions
pub struct SafeObjectInputStream {
    // TODO: Implement SafeObjectInputStream with blocked class names
}

impl SafeObjectInputStream {
    /// Default no-deserialize class names
    pub const DEFAULT_NO_DESERIALIZE_CLASS_NAMES: Vec<&str> = vec![
        "org.apache.commons.collections.functors.InvokerTransformer",
        "org.apache.commons.collections.functors.InstantiateTransformer",
        "org.apache.commons.collections4.functors.InvokerTransformer",
        "org.apache.commons.collections4.functors.InstantiateTransformer",
        "org.codehaus.groovy.runtime.ConvertedClosure",
        "org.codehaus.groovy.runtime.MethodClosure",
        "org.springframework.beans.factory.ObjectFactory",
        "com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl",
        "org.apache.xalan.xsltc.trax.TemplatesImpl",
    ];

    /// Create a new SafeObjectInputStream
    pub fn new() -> Self {
        Self {}
    }

    /// Check if a class name is blocked
    pub fn is_blocked(&self, name: &str) -> bool {
        for blocked_name in &Self::DEFAULT_NO_DESERIALIZE_CLASS_NAMES {
            if name.ends_with(blocked_name) {
                return true;
            }
        }
        false
    }
}
