//! Opaque keyset cursors (see `docs/design/api.md`). Contents are unstable:
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

/// Decodes a `(microseconds, id)` cursor (history pages: `removed_at` in
/// microseconds since the epoch, then the row id).
pub fn micros_id(cursor: Option<&str>) -> Result<Option<(i64, i64)>, XrpcError> {
    let Some(c) = cursor else { return Ok(None) };
    match decode_raw(c)?.as_slice() {
        [t, i] => Ok(Some((int(t)?, int(i)?))),
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
        assert_eq!(
            micros_id(Some(&encode(&[json!(1_700_000_000_000_001_i64), json!(9)]))).unwrap(),
            Some((1_700_000_000_000_001, 9))
        );
        assert!(micros_id(Some(&encode(&[json!("x"), json!(9)]))).is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(id_rkey(Some("!!!")).is_err());
        assert!(id_rkey(Some("bm9wZQ")).is_err());
        assert!(id(Some(&encode(&[json!("x")]))).is_err());
        assert!(id_rkey(Some(&encode(&[json!(1), json!(2)]))).is_err());
        assert!(id_id_rkey(Some(&encode(&[json!(1), json!("r")]))).is_err());
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        /// What a client may put where a number belongs.
        fn number() -> impl Strategy<Value = String> {
            prop_oneof![
                any::<i64>().prop_map(|n| n.to_string()),
                any::<u64>().prop_map(|n| n.to_string()),
                any::<f64>().prop_map(|x| format!("{x:?}")),
                "-?[0-9]{1,40}",
                "-?[0-9]{1,5}(\\.[0-9]{1,5})?[eE][+-]?[0-9]{1,4}",
                "\"-?[0-9]{1,20}\"",
                Just("null".to_owned()),
                Just("[1]".to_owned()),
            ]
        }

        fn plain_i64(token: &str) -> Option<i64> {
            token.parse::<i64>().ok().filter(|n| n.to_string() == token)
        }

        fn all_fail(c: &str) -> bool {
            id(Some(c)).is_err()
                && id_rkey(Some(c)).is_err()
                && id_id_rkey(Some(c)).is_err()
                && micros_id(Some(c)).is_err()
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// Any string as a cursor: every decoder returns, and at most
            /// one of the four shapes accepts it apart from the two that
            /// share an arity.
            #[test]
            fn decoding_arbitrary_strings_is_total(
                c in prop_oneof![
                    any::<String>(),
                    "[A-Za-z0-9_=+/ -]{0,64}",
                    prop::collection::vec(any::<u8>(), 0..64).prop_map(|b| URL_SAFE_NO_PAD.encode(b)),
                ],
            ) {
                let accepted = [
                    id(Some(&c)).is_ok(),
                    id_rkey(Some(&c)).is_ok() || micros_id(Some(&c)).is_ok(),
                    id_id_rkey(Some(&c)).is_ok(),
                ];
                prop_assert!(accepted.iter().filter(|a| **a).count() <= 1);
                prop_assert!(!(id_rkey(Some(&c)).is_ok() && micros_id(Some(&c)).is_ok()));
            }

            /// What is encoded is what is decoded, for every integer and
            /// every string, with or without space around the cursor.
            #[test]
            fn encoded_keys_round_trip(
                a in any::<i64>(),
                b in any::<i64>(),
                r in any::<String>(),
                pad in "[ \\t\\n]{0,3}",
            ) {
                let wrap = |c: String| format!("{pad}{c}{pad}");
                let c = wrap(encode(&[json!(a)]));
                prop_assert_eq!(id(Some(&c)).unwrap(), Some(a));
                let c = wrap(encode(&[json!(a), json!(r)]));
                prop_assert_eq!(id_rkey(Some(&c)).unwrap(), Some((a, r.clone())));
                prop_assert!(micros_id(Some(&c)).is_err() && id(Some(&c)).is_err());
                let c = wrap(encode(&[json!(a), json!(b), json!(r)]));
                prop_assert_eq!(id_id_rkey(Some(&c)).unwrap(), Some((a, b, r.clone())));
                prop_assert!(id_rkey(Some(&c)).is_err());
                let c = wrap(encode(&[json!(a), json!(b)]));
                prop_assert_eq!(micros_id(Some(&c)).unwrap(), Some((a, b)));
                prop_assert!(id_rkey(Some(&c)).is_err() && id_id_rkey(Some(&c)).is_err());
            }

            /// A cursor holding anything where an id belongs is accepted
            /// only if that is a plain integer in the `i64` range, and
            /// then yields that integer.
            #[test]
            fn an_id_is_taken_only_as_a_plain_integer(n in number(), m in number()) {
                let one = URL_SAFE_NO_PAD.encode(format!("[{n}]"));
                prop_assert_eq!(id(Some(&one)).ok().flatten(), plain_i64(&n));
                let two = URL_SAFE_NO_PAD.encode(format!("[{n},{m}]"));
                prop_assert_eq!(
                    micros_id(Some(&two)).ok().flatten(),
                    plain_i64(&n).zip(plain_i64(&m))
                );
                let keyed = URL_SAFE_NO_PAD.encode(format!("[{n},\"k\"]"));
                prop_assert_eq!(
                    id_rkey(Some(&keyed)).ok().flatten(),
                    plain_i64(&n).map(|n| (n, "k".to_owned()))
                );
            }

            /// JSON that is not an array of the right length is not a
            /// cursor of any shape.
            #[test]
            fn other_json_is_not_a_cursor(
                text in prop_oneof![
                    Just("null".to_owned()),
                    Just("{}".to_owned()),
                    Just("[]".to_owned()),
                    Just("\"a\"".to_owned()),
                    any::<i64>().prop_map(|n| n.to_string()),
                    prop::collection::vec(any::<i64>(), 4..9)
                        .prop_map(|v| serde_json::to_string(&v).unwrap()),
                ],
            ) {
                prop_assert!(all_fail(&URL_SAFE_NO_PAD.encode(text)));
            }
        }
    }
}
