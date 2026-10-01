//! Opaque keyset cursors (design §3.1). Contents are unstable (§12.1):
//! base64url of a small JSON array holding the last returned sort key.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;

use crate::error::XrpcError;

/// Encodes a sort key.
pub fn encode(key: &[Value]) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(key).unwrap_or_default())
}

fn decode_raw(cursor: &str) -> Result<Vec<Value>, XrpcError> {
    let bad = || XrpcError::invalid("invalid cursor");
    let bytes = URL_SAFE_NO_PAD.decode(cursor.trim()).map_err(|_| bad())?;
    let v: Value = serde_json::from_slice(&bytes).map_err(|_| bad())?;
    match v {
        Value::Array(a) => Ok(a),
        _ => Err(bad()),
    }
}

fn int(v: &Value) -> Result<i64, XrpcError> {
    v.as_i64()
        .ok_or_else(|| XrpcError::invalid("invalid cursor"))
}

fn text(v: &Value) -> Result<String, XrpcError> {
    v.as_str()
        .map(str::to_owned)
        .ok_or_else(|| XrpcError::invalid("invalid cursor"))
}

/// Decodes an `(id, rkey)` cursor.
pub fn id_rkey(cursor: Option<&str>) -> Result<Option<(i64, String)>, XrpcError> {
    let Some(c) = cursor else { return Ok(None) };
    match decode_raw(c)?.as_slice() {
        [a, b] => Ok(Some((int(a)?, text(b)?))),
        _ => Err(XrpcError::invalid("invalid cursor")),
    }
}

/// Decodes an `(id, id, rkey)` cursor.
pub fn id_id_rkey(cursor: Option<&str>) -> Result<Option<(i64, i64, String)>, XrpcError> {
    let Some(c) = cursor else { return Ok(None) };
    match decode_raw(c)?.as_slice() {
        [a, b, r] => Ok(Some((int(a)?, int(b)?, text(r)?))),
        _ => Err(XrpcError::invalid("invalid cursor")),
    }
}

/// Decodes an `(id)` cursor.
pub fn id(cursor: Option<&str>) -> Result<Option<i64>, XrpcError> {
    let Some(c) = cursor else { return Ok(None) };
    match decode_raw(c)?.as_slice() {
        [a] => Ok(Some(int(a)?)),
        _ => Err(XrpcError::invalid("invalid cursor")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trips() {
        let c = encode(&[json!(42), json!("3l2x")]);
        assert_eq!(id_rkey(Some(&c)).unwrap(), Some((42, "3l2x".to_owned())));
        let c = encode(&[json!(1), json!(2), json!("r")]);
        assert_eq!(id_id_rkey(Some(&c)).unwrap(), Some((1, 2, "r".to_owned())));
        assert_eq!(id(Some(&encode(&[json!(7)]))).unwrap(), Some(7));
        assert_eq!(id(None).unwrap(), None);
    }

    #[test]
    fn rejects_garbage() {
        assert!(id_rkey(Some("!!!")).is_err());
        assert!(id_rkey(Some("bm9wZQ")).is_err());
        assert!(id(Some(&encode(&[json!("x")]))).is_err());
        assert!(id_rkey(Some(&encode(&[json!(1), json!(2)]))).is_err());
        assert!(id_id_rkey(Some(&encode(&[json!(1), json!("r")]))).is_err());
    }
}
