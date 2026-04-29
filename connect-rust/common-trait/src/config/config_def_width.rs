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

/// Width for configuration display.
///
/// This corresponds to `org.apache.kafka.common.config.ConfigDef.Width` in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigDefWidth {
    Short,
    Medium,
    Long,
    None,
}

impl ConfigDefWidth {
    /// Returns the name of this width.
    pub fn name(&self) -> &'static str {
        match self {
            ConfigDefWidth::Short => "SHORT",
            ConfigDefWidth::Medium => "MEDIUM",
            ConfigDefWidth::Long => "LONG",
            ConfigDefWidth::None => "NONE",
        }
    }
}
