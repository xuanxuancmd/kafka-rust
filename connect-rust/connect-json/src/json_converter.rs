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

//! JSON converter for Kafka Connect.
//!
//! Corresponds to: `org.apache.kafka.connect.json.JsonConverter` in Java.

use crate::decimal_format::DecimalFormat;
use crate::json_converter_config::JsonConverterConfig;
use crate::json_deserializer::JsonDeserializer;
use crate::json_schema::{
    Envelope, ARRAY_ITEMS_FIELD_NAME, ARRAY_TYPE_NAME, BOOLEAN_TYPE_NAME, BYTES_TYPE_NAME,
    DOUBLE_TYPE_NAME, ENVELOPE_PAYLOAD_FIELD_NAME, ENVELOPE_SCHEMA_FIELD_NAME, FLOAT_TYPE_NAME,
    INT16_TYPE_NAME, INT32_TYPE_NAME, INT64_TYPE_NAME, INT8_TYPE_NAME, MAP_KEY_FIELD_NAME,
    MAP_TYPE_NAME, MAP_VALUE_FIELD_NAME, SCHEMA_DEFAULT_FIELD_NAME, SCHEMA_DOC_FIELD_NAME,
    SCHEMA_NAME_FIELD_NAME, SCHEMA_OPTIONAL_FIELD_NAME, SCHEMA_PARAMETERS_FIELD_NAME,
    SCHEMA_TYPE_FIELD_NAME, SCHEMA_VERSION_FIELD_NAME, STRING_TYPE_NAME, STRUCT_FIELDS_FIELD_NAME,
    STRUCT_FIELD_NAME_FIELD_NAME, STRUCT_TYPE_NAME,
};
use crate::json_serializer::JsonSerializer;
use common_trait::cache::{Cache, SynchronizedCache};
use common_trait::config::{
    ConfigDef, ConfigDefImportance, ConfigDefType, ConfigDefWidth, ConfigValueEntry,
};
use common_trait::serialization::Deserializer;
use connect_api::components::Versioned;
use connect_api::data::{
    ConnectSchema, Date, Decimal, Schema, SchemaAndValue, SchemaBuilder, SchemaType, Time,
    Timestamp,
};
use connect_api::errors::ConnectError;
use connect_api::storage::{Converter, HeaderConverter};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

// ============================================================================
// Logical Type Converter Functions
// ============================================================================

/// Converts Decimal to JSON.
fn decimal_to_json(value: &Value, config: &JsonConverterConfig) -> Value {
    match config.decimal_format() {
        DecimalFormat::Numeric => value.clone(),
        DecimalFormat::Base64 => match value {
            Value::String(s) => {
                use base64::Engine;
                let encoded = base64::engine::general_purpose::STANDARD.encode(s.as_bytes());
                json!(encoded)
            }
            Value::Number(n) => {
                let bytes = n.to_string().as_bytes().to_vec();
                use base64::Engine;
                json!(base64::engine::general_purpose::STANDARD.encode(&bytes))
            }
            _ => json!(null),
        },
    }
}

/// Converts JSON to Decimal.
fn decimal_to_connect(value: &Value) -> Value {
    if value.is_number() {
        value.clone()
    } else if value.is_string() {
        use base64::Engine;
        let s = value.as_str().unwrap_or("");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s)
            .unwrap_or_default();
        json!(String::from_utf8_lossy(&bytes).to_string())
    } else {
        json!(null)
    }
}

/// Gets logical type converter.
fn get_logical_converter(
    name: &str,
) -> Option<(
    fn(&Value, &JsonConverterConfig) -> Value,
    fn(&Value) -> Value,
)> {
    match name {
        Decimal::LOGICAL_NAME => Some((decimal_to_json, decimal_to_connect)),
        Date::LOGICAL_NAME => Some((
            |v, _| v.clone(),
            |v| if v.is_i64() { v.clone() } else { json!(null) },
        )),
        Time::LOGICAL_NAME => Some((
            |v, _| v.clone(),
            |v| if v.is_i64() { v.clone() } else { json!(null) },
        )),
        Timestamp::LOGICAL_NAME => Some((
            |v, _| v.clone(),
            |v| if v.is_i64() { v.clone() } else { json!(null) },
        )),
        _ => None,
    }
}

// ============================================================================
// JsonConverter Struct
// ============================================================================

/// JSON converter for Kafka Connect data.
pub struct JsonConverter {
    config: Option<JsonConverterConfig>,
    from_connect_schema_cache: Option<SynchronizedCache<String, Value>>,
    to_connect_schema_cache: Option<SynchronizedCache<String, ConnectSchema>>,
    schema: Option<ConnectSchema>,
    serializer: JsonSerializer,
    deserializer: JsonDeserializer,
    enable_blackbird: bool,
}

impl JsonConverter {
    pub fn new() -> Self {
        Self::with_blackbird(true)
    }

    pub fn with_blackbird(enable_blackbird: bool) -> Self {
        JsonConverter {
            config: None,
            from_connect_schema_cache: None,
            to_connect_schema_cache: None,
            schema: None,
            serializer: JsonSerializer::with_blackbird(enable_blackbird),
            deserializer: JsonDeserializer::with_blackbird(enable_blackbird),
            enable_blackbird,
        }
    }

    pub fn size_of_from_connect_schema_cache(&self) -> usize {
        self.from_connect_schema_cache
            .as_ref()
            .map(|c| c.size())
            .unwrap_or(0)
    }

    pub fn size_of_to_connect_schema_cache(&self) -> usize {
        self.to_connect_schema_cache
            .as_ref()
            .map(|c| c.size())
            .unwrap_or(0)
    }

    /// Returns whether blackbird mode is enabled.
    pub fn is_blackbird_enabled(&self) -> bool {
        self.enable_blackbird
    }

    /// Returns whether the converter has been configured.
    pub fn is_configured(&self) -> bool {
        self.config.is_some()
    }

    fn configure_internal(&mut self, configs: HashMap<String, Value>) {
        self.config = Some(JsonConverterConfig::new(configs));
        let config = self.config.as_ref().unwrap();
        self.from_connect_schema_cache =
            Some(SynchronizedCache::new(config.schema_cache_size() as usize));
        self.to_connect_schema_cache =
            Some(SynchronizedCache::new(config.schema_cache_size() as usize));

        if let Some(schema_content) = config.schema_content() {
            if !schema_content.is_empty() {
                let schema_str = String::from_utf8_lossy(schema_content);
                if let Ok(schema_value) = serde_json::from_str::<Value>(&schema_str) {
                    self.schema = self.as_connect_schema_internal(&schema_value);
                }
            }
        }
    }

    pub fn as_json_schema(&self, schema: Option<&dyn Schema>) -> Option<Value> {
        if schema.is_none() {
            return None;
        }
        let schema_ref = schema.unwrap();
        let cache_key = self.schema_cache_key(schema_ref);

        if let Some(cache) = &self.from_connect_schema_cache {
            if let Some(cached) = cache.get(&cache_key) {
                return Some(cached);
            }
        }

        let json_schema = self.build_json_schema(schema_ref);
        if let Some(cache) = &self.from_connect_schema_cache {
            cache.put(cache_key, json_schema.clone());
        }
        Some(json_schema)
    }

    fn build_json_schema(&self, schema: &dyn Schema) -> Value {
        let mut result = Map::new();
        let type_name = match schema.r#type() {
            SchemaType::Boolean => BOOLEAN_TYPE_NAME,
            SchemaType::Int8 => INT8_TYPE_NAME,
            SchemaType::Int16 => INT16_TYPE_NAME,
            SchemaType::Int32 => INT32_TYPE_NAME,
            SchemaType::Int64 => INT64_TYPE_NAME,
            SchemaType::Float32 => FLOAT_TYPE_NAME,
            SchemaType::Float64 => DOUBLE_TYPE_NAME,
            SchemaType::Bytes => BYTES_TYPE_NAME,
            SchemaType::String => STRING_TYPE_NAME,
            SchemaType::Array => ARRAY_TYPE_NAME,
            SchemaType::Map => MAP_TYPE_NAME,
            SchemaType::Struct => STRUCT_TYPE_NAME,
        };
        result.insert(SCHEMA_TYPE_FIELD_NAME.to_string(), json!(type_name));

        match schema.r#type() {
            SchemaType::Array => {
                if let Some(vs) = schema.value_schema() {
                    result.insert(
                        ARRAY_ITEMS_FIELD_NAME.to_string(),
                        self.as_json_schema(Some(vs)).unwrap_or(Value::Null),
                    );
                }
            }
            SchemaType::Map => {
                if let Some(ks) = schema.key_schema() {
                    result.insert(
                        MAP_KEY_FIELD_NAME.to_string(),
                        self.as_json_schema(Some(ks)).unwrap_or(Value::Null),
                    );
                }
                if let Some(vs) = schema.value_schema() {
                    result.insert(
                        MAP_VALUE_FIELD_NAME.to_string(),
                        self.as_json_schema(Some(vs)).unwrap_or(Value::Null),
                    );
                }
            }
            SchemaType::Struct => {
                let fields: Vec<Value> = schema
                    .fields()
                    .iter()
                    .map(|f| {
                        let mut fs = self.as_json_schema(Some(f.schema())).unwrap_or(Value::Null);
                        if let Value::Object(ref mut obj) = fs {
                            obj.insert(STRUCT_FIELD_NAME_FIELD_NAME.to_string(), json!(f.name()));
                        }
                        fs
                    })
                    .collect();
                result.insert(STRUCT_FIELDS_FIELD_NAME.to_string(), Value::Array(fields));
            }
            _ => {}
        }

        result.insert(
            SCHEMA_OPTIONAL_FIELD_NAME.to_string(),
            json!(schema.is_optional()),
        );
        if let Some(n) = schema.name() {
            result.insert(SCHEMA_NAME_FIELD_NAME.to_string(), json!(n));
        }
        if let Some(v) = schema.version() {
            result.insert(SCHEMA_VERSION_FIELD_NAME.to_string(), json!(v));
        }
        if let Some(d) = schema.doc() {
            result.insert(SCHEMA_DOC_FIELD_NAME.to_string(), json!(d));
        }
        if !schema.parameters().is_empty() {
            let params: Map<String, Value> = schema
                .parameters()
                .iter()
                .map(|(k, v)| (k.clone(), json!(v)))
                .collect();
            result.insert(
                SCHEMA_PARAMETERS_FIELD_NAME.to_string(),
                Value::Object(params),
            );
        }
        if let Some(d) = schema.default_value() {
            result.insert(
                SCHEMA_DEFAULT_FIELD_NAME.to_string(),
                self.convert_to_json_internal(schema, d),
            );
        }
        Value::Object(result)
    }

    pub fn as_connect_schema(&self, json_schema: &Value) -> Option<ConnectSchema> {
        if json_schema.is_null() {
            return None;
        }
        let cache_key = serde_json::to_string(json_schema).unwrap_or_default();
        if let Some(cache) = &self.to_connect_schema_cache {
            if let Some(cached) = cache.get(&cache_key) {
                return Some(cached);
            }
        }
        let schema = self.as_connect_schema_internal(json_schema);
        if let Some(cache) = &self.to_connect_schema_cache {
            if let Some(s) = &schema {
                cache.put(cache_key, s.clone());
            }
        }
        schema
    }

    fn as_connect_schema_internal(&self, json_schema: &Value) -> Option<ConnectSchema> {
        if json_schema.is_null() {
            return None;
        }
        let st = json_schema.get(SCHEMA_TYPE_FIELD_NAME);
        if st.is_none() || !st.unwrap().is_string() {
            return None;
        }
        let type_name = st.unwrap().as_str().unwrap();

        let builder = match type_name {
            BOOLEAN_TYPE_NAME => SchemaBuilder::bool(),
            INT8_TYPE_NAME => SchemaBuilder::int8(),
            INT16_TYPE_NAME => SchemaBuilder::int16(),
            INT32_TYPE_NAME => SchemaBuilder::int32(),
            INT64_TYPE_NAME => SchemaBuilder::int64(),
            FLOAT_TYPE_NAME => SchemaBuilder::float32(),
            DOUBLE_TYPE_NAME => SchemaBuilder::float64(),
            BYTES_TYPE_NAME => SchemaBuilder::bytes(),
            STRING_TYPE_NAME => SchemaBuilder::string(),
            ARRAY_TYPE_NAME => {
                let es = json_schema.get(ARRAY_ITEMS_FIELD_NAME);
                if es.is_none() || es.unwrap().is_null() {
                    return None;
                }
                let elem = self.as_connect_schema_internal(es.unwrap())?;
                SchemaBuilder::array(elem)
            }
            MAP_TYPE_NAME => {
                let ks = json_schema.get(MAP_KEY_FIELD_NAME)?;
                let vs = json_schema.get(MAP_VALUE_FIELD_NAME)?;
                let key = self.as_connect_schema_internal(ks)?;
                let val = self.as_connect_schema_internal(vs)?;
                SchemaBuilder::map(key, val)
            }
            STRUCT_TYPE_NAME => {
                let fs = json_schema.get(STRUCT_FIELDS_FIELD_NAME);
                if fs.is_none() || !fs.unwrap().is_array() {
                    return None;
                }
                // Collect all fields first to avoid builder move issues
                let empty_arr = vec![];
                let mut collected_fields: Vec<(String, ConnectSchema)> = Vec::new();
                for f in fs.unwrap().as_array().unwrap_or(&empty_arr) {
                    let fn_node = f.get(STRUCT_FIELD_NAME_FIELD_NAME);
                    if fn_node.is_none() || !fn_node.unwrap().is_string() {
                        continue;
                    }
                    let name = fn_node.unwrap().as_str().unwrap().to_string();
                    let fschema = self.as_connect_schema_internal(f);
                    if fschema.is_none() {
                        continue;
                    }
                    collected_fields.push((name, fschema.unwrap()));
                }
                // Build struct with collected fields using try_fold
                collected_fields
                    .into_iter()
                    .try_fold(SchemaBuilder::struct_schema(), |b, (name, schema)| {
                        b.field(name, schema)
                    })
                    .unwrap_or_else(|_| SchemaBuilder::struct_schema())
            }
            _ => return None,
        };

        let opt = json_schema
            .get(SCHEMA_OPTIONAL_FIELD_NAME)
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let mut b = if opt { builder.optional() } else { builder };

        if let Some(n) = json_schema
            .get(SCHEMA_NAME_FIELD_NAME)
            .and_then(|v| v.as_str())
        {
            b = b.name(n);
        }
        if let Some(v) = json_schema
            .get(SCHEMA_VERSION_FIELD_NAME)
            .and_then(|v| v.as_i64())
        {
            b = b.version(v as i32);
        }
        if let Some(d) = json_schema
            .get(SCHEMA_DOC_FIELD_NAME)
            .and_then(|v| v.as_str())
        {
            b = b.doc(d);
        }

        Some(b.build())
    }

    fn convert_to_json_with_envelope(&self, schema: Option<&dyn Schema>, value: &Value) -> Value {
        let js = self.as_json_schema(schema);
        let jv = if schema.is_some() {
            self.convert_to_json_internal(schema.unwrap(), value)
        } else {
            value.clone()
        };
        Envelope::new(js.unwrap_or(Value::Null), jv).to_json_node()
    }

    fn convert_to_json_without_envelope(
        &self,
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Value {
        if schema.is_some() {
            self.convert_to_json_internal(schema.unwrap(), value)
        } else {
            value.clone()
        }
    }

    fn convert_to_json_internal(&self, schema: &dyn Schema, value: &Value) -> Value {
        if value.is_null() {
            if schema.is_optional() {
                return Value::Null;
            }
            if let Some(d) = schema.default_value() {
                if self
                    .config
                    .as_ref()
                    .map(|c| c.replace_null_with_default())
                    .unwrap_or(false)
                {
                    return self.convert_to_json_internal(schema, d);
                }
            }
            return Value::Null;
        }

        if let Some(name) = schema.name() {
            if let Some((to_json, _)) = get_logical_converter(name) {
                return to_json(
                    value,
                    self.config
                        .as_ref()
                        .unwrap_or(&JsonConverterConfig::new(HashMap::new())),
                );
            }
        }

        match schema.r#type() {
            SchemaType::Int8 => json!(value.as_i64().unwrap_or(0) as i8),
            SchemaType::Int16 => json!(value.as_i64().unwrap_or(0) as i16),
            SchemaType::Int32 => json!(value.as_i64().unwrap_or(0) as i32),
            SchemaType::Int64 => json!(value.as_i64().unwrap_or(0)),
            SchemaType::Float32 => json!(value.as_f64().unwrap_or(0.0) as f32),
            SchemaType::Float64 => json!(value.as_f64().unwrap_or(0.0)),
            SchemaType::Boolean => json!(value.as_bool().unwrap_or(false)),
            SchemaType::String => json!(value.as_str().unwrap_or("").to_string()),
            SchemaType::Bytes => match value {
                Value::String(s) => json!(s.clone()),
                Value::Array(arr) => {
                    let bytes: Vec<u8> = arr
                        .iter()
                        .filter_map(|v| v.as_u64().map(|u| u as u8))
                        .collect();
                    use base64::Engine;
                    json!(base64::engine::general_purpose::STANDARD.encode(&bytes))
                }
                _ => json!(""),
            },
            SchemaType::Array => {
                let empty = vec![];
                let arr = value.as_array().unwrap_or(&empty);
                let vs = schema.value_schema();
                Value::Array(
                    arr.iter()
                        .map(|e| {
                            vs.map(|v| self.convert_to_json_internal(v, e))
                                .unwrap_or_else(|| e.clone())
                        })
                        .collect(),
                )
            }
            SchemaType::Map => {
                let empty = Map::new();
                let obj = value.as_object().unwrap_or(&empty);
                let ks = schema.key_schema();
                let vs = schema.value_schema();
                let is_str_key = ks.map(|k| k.r#type() == SchemaType::String).unwrap_or(true);
                if is_str_key {
                    Value::Object(
                        obj.iter()
                            .map(|(k, v)| {
                                (
                                    k.clone(),
                                    vs.map(|vs| self.convert_to_json_internal(vs, v))
                                        .unwrap_or_else(|| v.clone()),
                                )
                            })
                            .collect(),
                    )
                } else {
                    Value::Array(
                        obj.iter()
                            .map(|(k, v)| {
                                let kj = ks
                                    .map(|ks| self.convert_to_json_internal(ks, &json!(k)))
                                    .unwrap_or_else(|| json!(k));
                                let vj = vs
                                    .map(|vs| self.convert_to_json_internal(vs, v))
                                    .unwrap_or_else(|| v.clone());
                                json!([kj, vj])
                            })
                            .collect(),
                    )
                }
            }
            SchemaType::Struct => {
                let empty = Map::new();
                let obj = value.as_object().unwrap_or(&empty);
                Value::Object(
                    schema
                        .fields()
                        .iter()
                        .map(|f| {
                            let fv = obj.get(f.name()).unwrap_or(&Value::Null);
                            (
                                f.name().to_string(),
                                self.convert_to_json_internal(f.schema(), fv),
                            )
                        })
                        .collect(),
                )
            }
        }
    }

    fn schema_cache_key(&self, schema: &dyn Schema) -> String {
        format!(
            "{}:{}:{}:{}",
            schema.r#type(),
            schema.name().unwrap_or(""),
            schema.version().unwrap_or(-1),
            schema.is_optional()
        )
    }
}

impl Default for JsonConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl Converter for JsonConverter {
    fn configure(&mut self, configs: HashMap<String, Value>, is_key: bool) {
        let mut cm = configs;
        cm.insert(
            "type".to_string(),
            json!(if is_key { "key" } else { "value" }),
        );
        self.configure_internal(cm);
    }

    fn from_connect_data(
        &self,
        topic: &str,
        schema: Option<&dyn Schema>,
        value: Option<&Value>,
    ) -> Result<Option<Vec<u8>>, ConnectError> {
        if schema.is_none() && value.is_none() {
            return Ok(None);
        }
        let v = value.unwrap_or(&Value::Null);
        let jv = if self
            .config
            .as_ref()
            .map(|c| c.schemas_enabled())
            .unwrap_or(true)
        {
            self.convert_to_json_with_envelope(schema, v)
        } else {
            self.convert_to_json_without_envelope(schema, v)
        };
        Ok(self.serializer.serialize(topic, Some(&jv)))
    }

    fn to_connect_data(
        &self,
        topic: &str,
        value: Option<&[u8]>,
    ) -> Result<SchemaAndValue, ConnectError> {
        if value.is_none() {
            return Ok(SchemaAndValue::null());
        }
        let bytes = value.unwrap();
        let jv = self
            .deserializer
            .deserialize(topic, bytes)
            .map_err(|e| ConnectError::data(format!("Failed to deserialize JSON: {}", e)))?;
        let jv = jv.unwrap_or(Value::Null);

        let config = self.config.as_ref();
        if config.is_none() {
            return Ok(SchemaAndValue::new(None, Some(jv)));
        }
        let cfg = config.unwrap();

        if cfg.schemas_enabled() {
            if self.schema.is_some() {
                let s = self.schema.as_ref().unwrap();
                return Ok(SchemaAndValue::new(
                    Some(s.clone()),
                    Some(convert_to_connect(Some(s), &jv, cfg)),
                ));
            }
            if !jv.is_object() {
                return Err(ConnectError::data(
                    "JsonConverter requires schema and payload fields",
                ));
            }
            let obj = jv.as_object().unwrap();
            if !obj.contains_key(ENVELOPE_SCHEMA_FIELD_NAME)
                || !obj.contains_key(ENVELOPE_PAYLOAD_FIELD_NAME)
            {
                return Err(ConnectError::data(
                    "JsonConverter requires schema and payload fields",
                ));
            }
            let s = self.as_connect_schema(obj.get(ENVELOPE_SCHEMA_FIELD_NAME).unwrap());
            let p = obj.get(ENVELOPE_PAYLOAD_FIELD_NAME).unwrap().clone();
            // Store cloned schema to extend lifetime
            let s_cloned = s.clone();
            let schema_ref: Option<&dyn Schema> = s_cloned.as_ref().map(|sc| sc as &dyn Schema);
            return Ok(SchemaAndValue::new(
                s,
                Some(convert_to_connect(schema_ref, &p, cfg)),
            ));
        }
        Ok(SchemaAndValue::new(None, Some(jv)))
    }

    fn config(&self) -> &'static dyn ConfigDef {
        &STATIC_CONFIG_DEF
    }
    fn close(&mut self) {}
}

impl HeaderConverter for JsonConverter {
    fn configure(&mut self, configs: HashMap<String, String>, is_key: bool) {
        let vc: HashMap<String, Value> = configs.into_iter().map(|(k, v)| (k, json!(v))).collect();
        Converter::configure(self, vc, is_key);
    }
    fn from_connect_header(
        &self,
        topic: &str,
        _hk: &str,
        schema: Option<&dyn Schema>,
        value: &Value,
    ) -> Result<Option<Vec<u8>>, ConnectError> {
        self.from_connect_data(topic, schema, Some(value))
    }
    fn to_connect_header(
        &self,
        topic: &str,
        _hk: &str,
        value: Option<&[u8]>,
    ) -> Result<SchemaAndValue, ConnectError> {
        self.to_connect_data(topic, value)
    }
    fn config(&self) -> &'static dyn ConfigDef {
        &STATIC_CONFIG_DEF
    }
    fn close(&mut self) {}
}

impl Versioned for JsonConverter {
    fn version() -> &'static str {
        common_trait::utils::AppInfoParser::version()
    }
}

struct StaticConfigDef;
impl ConfigDef for StaticConfigDef {
    fn config_def(&self) -> HashMap<String, ConfigValueEntry> {
        HashMap::new()
    }
}
static STATIC_CONFIG_DEF: StaticConfigDef = StaticConfigDef;

fn convert_to_connect(
    schema: Option<&dyn Schema>,
    jv: &Value,
    config: &JsonConverterConfig,
) -> Value {
    if jv.is_null() {
        if let Some(s) = schema {
            if let Some(d) = s.default_value() {
                if config.replace_null_with_default() {
                    return d.clone();
                }
            }
            if s.is_optional() {
                return Value::Null;
            }
        }
        return Value::Null;
    }
    if schema.is_some() {
        if let Some(name) = schema.unwrap().name() {
            if let Some((_, to_connect)) = get_logical_converter(name) {
                return to_connect(jv);
            }
        }
    }
    let st = schema.map(|s| s.r#type()).unwrap_or_else(|| match jv {
        Value::Bool(_) => SchemaType::Boolean,
        Value::Number(n) => {
            if n.is_i64() {
                SchemaType::Int64
            } else {
                SchemaType::Float64
            }
        }
        Value::String(_) => SchemaType::String,
        Value::Array(_) => SchemaType::Array,
        Value::Object(_) => SchemaType::Map,
        Value::Null => SchemaType::String,
    });
    convert_to_connect_by_type(st, schema, jv, config)
}

fn convert_to_connect_by_type(
    st: SchemaType,
    schema: Option<&dyn Schema>,
    jv: &Value,
    config: &JsonConverterConfig,
) -> Value {
    match st {
        SchemaType::Boolean => json!(jv.as_bool().unwrap_or(false)),
        SchemaType::Int8 => json!(jv.as_i64().unwrap_or(0) as i8),
        SchemaType::Int16 => json!(jv.as_i64().unwrap_or(0) as i16),
        SchemaType::Int32 => json!(jv.as_i64().unwrap_or(0) as i32),
        SchemaType::Int64 => json!(jv.as_i64().unwrap_or(0)),
        SchemaType::Float32 => json!(jv.as_f64().unwrap_or(0.0) as f32),
        SchemaType::Float64 => json!(jv.as_f64().unwrap_or(0.0)),
        SchemaType::String => json!(jv.as_str().unwrap_or("").to_string()),
        SchemaType::Bytes => match jv {
            Value::String(s) => json!(s.clone()),
            Value::Array(arr) => json!(arr
                .iter()
                .filter_map(|v| v.as_u64().map(|u| u as u8))
                .collect::<Vec<u8>>()),
            _ => json!(Vec::<u8>::new()),
        },
        SchemaType::Array => {
            let empty = vec![];
            let arr = jv.as_array().unwrap_or(&empty);
            let es = schema.and_then(|s| s.value_schema());
            Value::Array(
                arr.iter()
                    .map(|e| convert_to_connect(es, e, config))
                    .collect(),
            )
        }
        SchemaType::Map => {
            let empty = Map::new();
            let obj = jv.as_object().unwrap_or(&empty);
            let vs = schema.and_then(|s| s.value_schema());
            Value::Object(
                obj.iter()
                    .map(|(k, v)| (k.clone(), convert_to_connect(vs, v, config)))
                    .collect(),
            )
        }
        SchemaType::Struct => {
            let empty = Map::new();
            let obj = jv.as_object().unwrap_or(&empty);
            if schema.is_none() {
                return Value::Object(empty);
            }
            Value::Object(
                schema
                    .unwrap()
                    .fields()
                    .iter()
                    .map(|f| {
                        let fv = obj.get(f.name()).unwrap_or(&Value::Null);
                        (
                            f.name().to_string(),
                            convert_to_connect(Some(f.schema()), fv, config),
                        )
                    })
                    .collect(),
            )
        }
    }
}
