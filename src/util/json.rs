use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use anyhow::bail;
use paste::paste;
use serde::de::DeserializeOwned;
use serde_json::{Map, Number, Value};

//region Macros
macro_rules! impl_get_numbers {
    (
        ints:   [ $( $ti:ty ),* $(,)? ],
        floats: [ $( $tf:ty ),* $(,)? ]
    ) => {
        paste! {
            $(
                pub fn [<get_ $ti _def>](&self, path: &str, def: $ti) -> $ti {
                    let Some(Value::Number(n)) = self.resolve_path(path) else {
                        return def;
                    };
                    impl_get_numbers!(@int_as n, $ti, def)
                }

                pub fn [<get_ $ti>](&self, path: &str) -> $ti {
                    self.[<get_ $ti _def>](path, 0 as $ti)
                }
            )*

            $(
                pub fn [<get_ $tf _def>](&self, path: &str, def: $tf) -> $tf {
                    let Some(Value::Number(n)) = self.resolve_path(path) else {
                        return def;
                    };
                    impl_get_numbers!(@float_as n, $tf, def)
                }

                pub fn [<get_ $tf>](&self, path: &str) -> $tf {
                    self.[<get_ $tf _def>](path, 0.0 as $tf)
                }
            )*
        }
    };

    (@int_as $n:ident, u8,  $def:ident) => { $n.as_u64().unwrap_or($def as u64) as u8 };
    (@int_as $n:ident, u16, $def:ident) => { $n.as_u64().unwrap_or($def as u64) as u16 };
    (@int_as $n:ident, u32, $def:ident) => { $n.as_u64().unwrap_or($def as u64) as u32 };
    (@int_as $n:ident, u64, $def:ident) => { $n.as_u64().unwrap_or($def) };
    (@int_as $n:ident, i32, $def:ident) => { $n.as_i64().unwrap_or($def as i64) as i32 };
    (@int_as $n:ident, i64, $def:ident) => { $n.as_i64().unwrap_or($def) };

    (@float_as $n:ident, f32, $def:ident) => {
        $n.as_f64().unwrap_or($def as f64) as f32
    };
    (@float_as $n:ident, f64, $def:ident) => {
        $n.as_f64().unwrap_or($def)
    };
}

macro_rules! impl_set_numbers {
    (
        ints:   [ $( $t_i:ty ),* $(,)? ],
        floats: [ $( $t_f:ty ),* $(,)? ]
    ) => {
        paste! {
            $(
                pub fn [<set_ $t_i>](&mut self, key: &str, value: $t_i) -> &mut Self {
                    *self.resolve_path_mut_create(key) =
                        Value::Number(Number::from(value));
                    self
                }
            )*

            $(
                pub fn [<set_ $t_f>](&mut self, key: &str, value: $t_f) -> &mut Self {
                    *self.resolve_path_mut_create(key) =
                        Value::Number(Number::from_f64(value as f64).unwrap());
                    self
                }
            )*
        }
    };
}
//endregion

/// A wrapper around serde_json::Value that provides quick access to keys through a period-separated path.
#[derive(Debug)]
pub struct JsonWrapper {
    value: Value,
}

impl JsonWrapper {
    pub fn new() -> Self {
        JsonWrapper {
            value: Value::Object(Map::new()),
        }
    }

    pub fn from_value(value: Value) -> anyhow::Result<Self> {
        if !matches!(value, Value::Object(_)) {
            bail!("JsonWrappers can only be constructed from JSON objects");
        }

        Ok(JsonWrapper {
            value
        })
    }

    pub fn from_str(json_str: &str) -> anyhow::Result<Self> {
        let value: Value = serde_json::from_str(json_str)?;

        Self::from_value(value)
    }

    fn resolve_path(&self, path: &str) -> Option<Value> {
        let parts = Self::split_path(path);

        let mut current = &self.value;

        for key in parts {
            match current {
                Value::Object(map) => {
                    current = map.get(&key)?;
                }
                _ => return None,
            }
        }

        Some(current.clone())
    }

    fn resolve_path_mut_create(&mut self, path: &str) -> &mut Value {
        let parts = Self::split_path(path);

        let mut current = &mut self.value;

        for key in parts {
            if !current.is_object() {
                *current = Value::Object(Map::new());
            }

            let obj = current.as_object_mut().unwrap();
            current = obj.entry(key).or_insert(Value::Object(Map::new()));
        }

        current
    }

    impl_get_numbers! {
        ints:   [u8, u16, u32, u64, i32, i64],
        floats: [f32, f64]
    }

    pub fn get_str_def(&self, path: &str, def: &str) -> String {
        let Some(Value::String(s)) = self.resolve_path(path) else {
            return def.to_string();
        };

        s
    }

    pub fn get_str(&self, path: &str) -> String {
        self.get_str_def(path, "")
    }

    pub fn get_bool_def(&self, path: &str, def: bool) -> bool {
        let Some(Value::Bool(b)) = self.resolve_path(path) else {
            return def;
        };

        b
    }

    pub fn get_bool(&self, path: &str) -> bool {
        self.get_bool_def(path, false)
    }

    pub fn get_array<T: DeserializeOwned>(&self, path: &str) -> Vec<T> {
        let Some(Value::Array(arr)) = self.resolve_path(path) else {
            return Vec::new();
        };

        let mut result: Vec<T> = Vec::new();

        for item in arr {
            if let Ok(deserialized) = serde_json::from_value::<T>(item) {
                result.push(deserialized);
            }
        }

        result
    }

    pub fn get_raw_array(&self, path: &str) -> Vec<Value> {
        let Some(Value::Array(arr)) = self.resolve_path(path) else {
            return Vec::new();
        };

        arr
    }

    pub fn get_object_array(&self, path: &str) -> Vec<JsonWrapper> {
        let Some(Value::Array(arr)) = self.resolve_path(path) else {
            return Vec::new();
        };

        let mut obj_arr: Vec<JsonWrapper> = Vec::new();

        for item in arr {
            if let Value::Object(_) = item {
                obj_arr.push(JsonWrapper {
                    value: item,
                });
            }
        }

        obj_arr
    }

    pub fn get_obj_copy(&self, path: &str) -> JsonWrapper {
        let Some(Value::Object(obj)) = self.resolve_path(path) else {
            return Self::new();
        };

        JsonWrapper {
            value: Value::Object(obj),
        }
    }

    impl_set_numbers! {
        ints: [u8, u16, u32, u64, i8, i16, i32, i64],
        floats: [f32, f64]
    }

    pub fn set_bool(&mut self, key: &str, value: bool) -> &mut Self {
        *self.resolve_path_mut_create(key) = Value::Bool(value);
        self
    }

    pub fn set_str(&mut self, key: &str, value: &str) -> &mut Self {
        *self.resolve_path_mut_create(key) = Value::String(value.to_string());
        self
    }

    pub fn set_value(&mut self, key: &str, value: Value) -> &mut Self {
        *self.resolve_path_mut_create(key) = value;
        self
    }

    pub fn has(&self, key: &str) -> bool {
        self.resolve_path(key).is_some()
    }

    fn split_path(path: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut buf = String::new();
        let mut escape = false;

        for c in path.chars() {
            match (c, escape) {
                ('.', false) => {
                    parts.push(buf);
                    buf = String::new();
                }
                ('\\', false) => escape = true,
                (c, _) => {
                    buf.push(c);
                    escape = false;
                }
            }
        }

        if !buf.is_empty() {
            parts.push(buf);
        }

        parts
    }
}

impl Clone for JsonWrapper {
    fn clone(&self) -> Self {
        JsonWrapper {
            value: self.value.clone(),
        }
    }
}

impl Display for JsonWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Ok(json_str) = serde_json::to_string(&self.value) else {
            return Err(std::fmt::Error);
        };

        write!(f, "{json_str}")?;

        Ok(())
    }
}

pub trait FromJson {
    fn from_json(data: &JsonWrapper) -> Self;
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use super::*;

    #[test]
    fn test_json_wrapper() {
        let json = json!({
            "name": "Test",
            "age": 30,
            "is_active": true,
            "scores": [10, 20, 30],
            "address": {
                "city": "Testville",
                "zip": "12345"
            }
        });

        let mut wrapper = JsonWrapper::from_value(json).unwrap();

        assert_eq!(wrapper.get_str("name"), "Test");
        assert_eq!(wrapper.get_u32("age"), 30);
        assert_eq!(wrapper.get_bool("is_active"), true);
        assert_eq!(wrapper.get_array::<u32>("scores"), vec![10, 20, 30]);
        assert_eq!(wrapper.get_str("address.city"), "Testville");
        assert_eq!(wrapper.get_str("address.zip"), "12345");

        wrapper.set_str("name", "Updated");
        assert_eq!(wrapper.get_str("name"), "Updated");
    }

}