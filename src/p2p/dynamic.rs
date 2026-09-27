//! JSON ↔ `DynamicValue` (argumentos y resultados de tools), con las mismas
//! reglas que `dynamicValueFromUnknown` / `dynamicValueToUnknown` del TS:
//! enteros y decimales se distinguen, `null` no se admite.

use std::collections::HashMap;

use serde_json::{Map, Number, Value};

use crate::protocol::fhs::{dynamic_value::Kind, DynamicList, DynamicObject, DynamicValue};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("DynamicValue no admite null (en {0})")]
pub struct NullNotAllowed(pub String);

pub fn from_json(value: &Value) -> Result<DynamicValue, NullNotAllowed> {
    from_json_at(value, "$")
}

fn from_json_at(value: &Value, path: &str) -> Result<DynamicValue, NullNotAllowed> {
    let kind = match value {
        Value::Null => return Err(NullNotAllowed(path.to_string())),
        Value::Bool(b) => Kind::BooleanValue(*b),
        Value::Number(n) => match n.as_i64() {
            Some(i) if n.is_i64() => Kind::IntegerValue(i),
            _ => Kind::NumberValue(n.as_f64().unwrap_or_default()),
        },
        Value::String(s) => Kind::StringValue(s.clone()),
        Value::Array(items) => Kind::ListValue(DynamicList {
            values: items
                .iter()
                .enumerate()
                .map(|(i, item)| from_json_at(item, &format!("{path}[{i}]")))
                .collect::<Result<_, _>>()?,
        }),
        Value::Object(map) => Kind::ObjectValue(DynamicObject {
            fields: map
                .iter()
                .map(|(k, v)| Ok((k.clone(), from_json_at(v, &format!("{path}.{k}"))?)))
                .collect::<Result<HashMap<_, _>, NullNotAllowed>>()?,
        }),
    };
    Ok(DynamicValue { kind: Some(kind) })
}

pub fn to_json(value: &DynamicValue) -> Value {
    match &value.kind {
        None => Value::Null,
        Some(Kind::BooleanValue(b)) => Value::Bool(*b),
        Some(Kind::IntegerValue(i)) => Value::Number((*i).into()),
        Some(Kind::NumberValue(f)) => Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Some(Kind::StringValue(s)) => Value::String(s.clone()),
        Some(Kind::BytesValue(bytes)) => Value::String(base64_encode(bytes)),
        Some(Kind::ListValue(list)) => Value::Array(list.values.iter().map(to_json).collect()),
        Some(Kind::ObjectValue(object)) => {
            let mut map = Map::new();
            let mut keys: Vec<&String> = object.fields.keys().collect();
            keys.sort();
            for key in keys {
                map.insert(key.clone(), to_json(&object.fields[key]));
            }
            Value::Object(map)
        }
        Some(Kind::ArtifactRef(artifact)) => {
            serde_json::json!({ "artifactRef": format!("{artifact:?}") })
        }
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn roundtrips_json_with_integers_and_decimals_apart() {
        let json = serde_json::json!({"entero": 3, "decimal": 0.25, "texto": "sí", "lista": [1, "dos"], "ok": true});
        let value = from_json(&json).unwrap();
        assert_eq!(to_json(&value), json);
    }

    #[test]
    fn rejects_null_with_its_path() {
        assert_eq!(
            from_json(&serde_json::json!({"a": [1, null]})),
            Err(NullNotAllowed("$.a[1]".into()))
        );
    }

    #[test]
    fn decodes_the_ts_fixtures_to_the_same_json() {
        let fixtures: Value =
            serde_json::from_str(include_str!("../../tests/fixtures/wire.json")).unwrap();
        for entry in fixtures["dynamic_values"].as_array().unwrap() {
            let bytes = hex::decode(entry["bytes_hex"].as_str().unwrap()).unwrap();
            let value = DynamicValue::decode(bytes.as_slice()).unwrap();
            assert_eq!(to_json(&value), entry["json"], "{}", entry["name"]);
        }
    }

    #[test]
    fn base64_matches_standard_padding() {
        assert_eq!(base64_encode(b"hola"), "aG9sYQ==");
        assert_eq!(base64_encode(b"abc"), "YWJj");
    }
}
