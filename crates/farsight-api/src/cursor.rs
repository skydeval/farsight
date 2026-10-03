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

/// Decodes an `(rkey)` cursor.
pub fn rkey(cursor: Option<&str>) -> Result<Option<String>, XrpcError> {
    let Some(c) = cursor else { return Ok(None) };
    match decode_raw(c)?.as_slice() {
        [r] => Ok(Some(text(r)?)),
        _ => Err(XrpcError::invalid("invalid cursor")),
    }
}

/// Decodes a `(microseconds, id)` cursor (history pages: `removed_at` in
/// microseconds since the epoch, then the row id).
pub fn micros_id(cursor: Option<&str>) -> Result<Option<(i64, i64)>, XrpcError> {
    let Some(c) = cursor else { return Ok(None) };
    match decode_raw(c)?.as_slice() {
        [t, i] => Ok(Some((int(t)?, int(i)?))),
        _ => Err(XrpcError::invalid("invalid cursor")),
    }
}

/// First element of a cursor of a UI section's shown-time order (design
/// §8.6). Without the tag such a cursor, `[time, rkey]`, and one of the
/// order the section had before, `[id, rkey]`, are both an integer and a
/// text, and the old one would be read as a time.
pub const SHOWN_TAG: &str = "t";

fn shown_time(v: &Value) -> Result<Option<i64>, XrpcError> {
    if v.is_null() {
        Ok(None)
    } else {
        int(v).map(Some)
    }
}

fn tagged(v: &Value) -> Result<(), XrpcError> {
    if v.as_str() == Some(SHOWN_TAG) {
        Ok(())
    } else {
        Err(XrpcError::invalid("invalid cursor"))
    }
}

/// Encodes a shown-time position: the shown time in microseconds since
/// the epoch (`None` = the row has none and sorts last), the listed
/// account's id where the section breaks ties by it, and the record key.
pub fn encode_shown(time: Option<i64>, id: Option<i64>, rkey: &str) -> String {
    let mut key = vec![Value::from(SHOWN_TAG), Value::from(time)];
    if let Some(id) = id {
        key.push(Value::from(id));
    }
    key.push(Value::from(rkey));
    encode(&key)
}

/// Decodes a `("t", microseconds | null, id, rkey)` cursor.
pub fn shown_id_rkey(
    cursor: Option<&str>,
) -> Result<Option<(Option<i64>, i64, String)>, XrpcError> {
    let Some(c) = cursor else { return Ok(None) };
    match decode_raw(c)?.as_slice() {
        [tag, t, i, r] => {
            tagged(tag)?;
            Ok(Some((shown_time(t)?, int(i)?, text(r)?)))
        }
        _ => Err(XrpcError::invalid("invalid cursor")),
    }
}

/// Decodes a `("t", microseconds | null, rkey)` cursor.
pub fn shown_rkey(cursor: Option<&str>) -> Result<Option<(Option<i64>, String)>, XrpcError> {
    let Some(c) = cursor else { return Ok(None) };
    match decode_raw(c)?.as_slice() {
        [tag, t, r] => {
            tagged(tag)?;
            Ok(Some((shown_time(t)?, text(r)?)))
        }
        _ => Err(XrpcError::invalid("invalid cursor")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn shown_cursors_are_tagged() {
        let c = encode_shown(Some(1_700_000_000_000_001), Some(42), "3l2x");
        assert_eq!(
            shown_id_rkey(Some(&c)).unwrap(),
            Some((Some(1_700_000_000_000_001), 42, "3l2x".to_owned()))
        );
        let last = encode_shown(None, None, "3l2x");
        assert_eq!(
            shown_rkey(Some(&last)).unwrap(),
            Some((None, "3l2x".to_owned()))
        );
        assert_eq!(shown_rkey(None).unwrap(), None);
        // A cursor of the other order never parses, in either direction:
        // nothing is silently read as a time.
        let old = encode(&[json!(42), json!("3l2x")]);
        assert!(shown_rkey(Some(&old)).is_err());
        assert!(shown_id_rkey(Some(&old)).is_err());
        assert!(shown_rkey(Some(&encode(&[json!("3l2x")]))).is_err());
        assert!(id_rkey(Some(&c)).is_err());
        assert!(id_rkey(Some(&last)).is_err());
        assert!(rkey(Some(&last)).is_err());
        // The tag is required even when the shape matches.
        assert!(shown_rkey(Some(&encode(&[json!("x"), json!(1), json!("r")]))).is_err());
        assert!(shown_id_rkey(Some(&encode(&[json!(1), json!(2), json!(3), json!("r")]))).is_err());
    }

    #[test]
    fn round_trips() {
        let c = encode(&[json!(42), json!("3l2x")]);
        assert_eq!(id_rkey(Some(&c)).unwrap(), Some((42, "3l2x".to_owned())));
        let c = encode(&[json!(1), json!(2), json!("r")]);
        assert_eq!(id_id_rkey(Some(&c)).unwrap(), Some((1, 2, "r".to_owned())));
        assert_eq!(id(Some(&encode(&[json!(7)]))).unwrap(), Some(7));
        assert_eq!(id(None).unwrap(), None);
        assert_eq!(
            rkey(Some(&encode(&[json!("3k")]))).unwrap().as_deref(),
            Some("3k")
        );
        assert_eq!(
            micros_id(Some(&encode(&[json!(1_700_000_000_000_001_i64), json!(9)]))).unwrap(),
            Some((1_700_000_000_000_001, 9))
        );
        assert!(micros_id(Some(&encode(&[json!("x"), json!(9)]))).is_err());
        assert!(rkey(Some(&encode(&[json!(1)]))).is_err());
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
