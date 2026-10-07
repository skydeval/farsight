//! Jetstream wire formats (see `docs/design/firehose.md`): the v1
//! `/subscribe` JSON events and the v2
//! `network.bsky.jetstream.subscribeEvents` `xrpc.v1.json` frames,
//! reduced to one [`InEvent`] shape.
//!
//! v1 event: `{"did", "time_us", "kind", "commit"|"identity"|"account"}`;
//! the cursor and witness time are `time_us`.
//!
//! v2 frame: `{"$type":"message","payload":{"$type":"…#commit", "seq",
//! "did", "time", "witnessedAt"?, …}}`, an `#info` advisory, or
//! `{"$type":"error","error","message"}`. The cursor is `seq`; the witness
//! time is `witnessedAt` (falling back to `time` for older servers).

use chrono::DateTime;
use farsight_core::record::{CommitOp, RecordError, parse_commit};
use farsight_core::{Collection, Did};
use serde_json::Value;

/// The two protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// v1 `/subscribe`.
    V1,
    /// v2 `subscribeEvents`.
    V2,
}

impl Protocol {
    /// Metric / log label.
    pub fn label(self) -> &'static str {
        match self {
            Protocol::V1 => "v1",
            Protocol::V2 => "v2",
        }
    }

    /// The storage code.
    pub fn storage(self) -> farsight_storage::codes::Protocol {
        match self {
            Protocol::V1 => farsight_storage::codes::Protocol::V1,
            Protocol::V2 => farsight_storage::codes::Protocol::V2,
        }
    }
}

/// Why an event is dropped before `apply` (`farsight_ingest_dropped_total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// Failed validation (bad DID, non-TID rev, bad record, …).
    Invalid,
    /// Authority rule: a listitem naming another repo's list.
    ForeignListItem,
}

impl DropReason {
    /// Metric label.
    pub fn label(self) -> &'static str {
        match self {
            DropReason::Invalid => "invalid",
            DropReason::ForeignListItem => "foreign_listitem",
        }
    }
}

/// What an event carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// A validated commit on one of the four collections.
    Commit(CommitOp),
    /// A commit that failed validation; dropped, but its position counts.
    Rejected {
        /// Why.
        reason: DropReason,
        /// The collection if it was readable (metric label).
        collection: Option<Collection>,
        /// Detail for logs.
        detail: String,
    },
    /// A commit on some other collection (not subscribed; ignored).
    OtherCommit,
    /// `identity`.
    Identity(Did),
    /// `account`.
    Account {
        /// The account.
        did: Did,
        /// `active`.
        active: bool,
        /// Upstream status when inactive.
        status: Option<String>,
    },
    /// `#sync` (v2 only).
    Sync(Did),
    /// An event with an unknown kind or an invalid repo DID: skipped.
    Ignored(String),
}

/// One event, with its stream position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InEvent {
    /// v2 `seq`; `None` on v1.
    pub seq: Option<i64>,
    /// Witness time, microseconds since the epoch.
    pub witness_us: i64,
    /// The payload.
    pub body: Body,
}

/// One decoded websocket message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// An event.
    Event(InEvent),
    /// v2 `#info` advisory (e.g. `OutdatedCursor`).
    Info {
        /// Name.
        name: String,
        /// Message.
        message: Option<String>,
    },
    /// v2 error frame; the server closes the stream after it.
    Error {
        /// Error name.
        error: String,
        /// Message.
        message: Option<String>,
    },
}

/// Why a message could not be decoded at all (not even a position).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("undecodable frame: {0}")]
pub struct FrameError(pub String);

fn str_of<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(Value::as_str)
}

/// RFC 3339 → microseconds since the epoch.
pub fn parse_time_us(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp_micros())
}

fn commit_body(did: &str, commit: &Value) -> Body {
    let collection = str_of(commit, "collection").and_then(Collection::from_nsid);
    if collection.is_none() {
        return Body::OtherCommit;
    }
    match parse_commit(did, commit) {
        Ok(op) => Body::Commit(op),
        Err(e) => Body::Rejected {
            reason: match e {
                RecordError::ForeignListItem { .. } => DropReason::ForeignListItem,
                _ => DropReason::Invalid,
            },
            collection,
            detail: e.to_string(),
        },
    }
}

fn repo_did(did: &str) -> Result<Did, Box<Body>> {
    Did::parse(did).map_err(|e| Box::new(Body::Ignored(format!("invalid repo DID {did:?}: {e}"))))
}

fn account_body(did: &str, account: &Value) -> Body {
    match repo_did(did) {
        Ok(did) => Body::Account {
            did,
            active: account
                .get("active")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            status: str_of(account, "status").map(str::to_owned),
        },
        Err(b) => *b,
    }
}

/// Decodes a v1 event (one JSON object).
pub fn decode_v1(bytes: &[u8]) -> Result<Frame, FrameError> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| FrameError(e.to_string()))?;
    let did = str_of(&v, "did").ok_or_else(|| FrameError("missing did".into()))?;
    let witness_us = v
        .get("time_us")
        .and_then(Value::as_i64)
        .ok_or_else(|| FrameError("missing time_us".into()))?;
    let body = match str_of(&v, "kind") {
        Some("commit") => match v.get("commit") {
            Some(c) => commit_body(did, c),
            None => Body::Rejected {
                reason: DropReason::Invalid,
                collection: None,
                detail: "commit event without commit".into(),
            },
        },
        Some("identity") => match repo_did(did) {
            Ok(d) => Body::Identity(d),
            Err(b) => *b,
        },
        Some("account") => match v.get("account") {
            Some(a) => account_body(did, a),
            None => Body::Ignored("account event without account".into()),
        },
        other => Body::Ignored(format!("unknown kind {other:?}")),
    };
    Ok(Frame::Event(InEvent {
        seq: None,
        witness_us,
        body,
    }))
}

const V2_PREFIX: &str = "network.bsky.jetstream.subscribeEvents#";

/// Decodes a v2 `xrpc.v1.json` frame.
pub fn decode_v2(bytes: &[u8]) -> Result<Frame, FrameError> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| FrameError(e.to_string()))?;
    match str_of(&v, "$type") {
        Some("error") => {
            return Ok(Frame::Error {
                error: str_of(&v, "error").unwrap_or("Unknown").to_owned(),
                message: str_of(&v, "message").map(str::to_owned),
            });
        }
        Some("message") => {}
        other => return Err(FrameError(format!("unknown frame type {other:?}"))),
    }
    let p = v
        .get("payload")
        .ok_or_else(|| FrameError("message without payload".into()))?;
    let kind = str_of(p, "$type")
        .and_then(|t| t.strip_prefix(V2_PREFIX))
        .ok_or_else(|| FrameError("payload without a subscribeEvents $type".into()))?;
    if kind == "info" {
        return Ok(Frame::Info {
            name: str_of(p, "name").unwrap_or("").to_owned(),
            message: str_of(p, "message").map(str::to_owned),
        });
    }
    let seq = p
        .get("seq")
        .and_then(Value::as_i64)
        .ok_or_else(|| FrameError("event without seq".into()))?;
    let witness_us = str_of(p, "witnessedAt")
        .or_else(|| str_of(p, "time"))
        .and_then(parse_time_us)
        .ok_or_else(|| FrameError("event without a parseable witnessedAt/time".into()))?;
    let did = str_of(p, "did").ok_or_else(|| FrameError("event without did".into()))?;
    let body = match kind {
        // v2 commit fields (rev, operation, collection, rkey, record) are
        // flat in the payload.
        "commit" => commit_body(did, p),
        "identity" => match repo_did(did) {
            Ok(d) => Body::Identity(d),
            Err(b) => *b,
        },
        "account" => match p.get("account") {
            Some(a) => account_body(did, a),
            None => Body::Ignored("account event without account".into()),
        },
        "sync" => match repo_did(did) {
            Ok(d) => Body::Sync(d),
            Err(b) => *b,
        },
        other => Body::Ignored(format!("unknown kind {other}")),
    };
    Ok(Frame::Event(InEvent {
        seq: Some(seq),
        witness_us,
        body,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use farsight_core::record::{CommitAction, Record};
    use serde_json::json;

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    fn v2(payload: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({"$type": "message", "payload": payload})).unwrap()
    }

    #[test]
    fn v1_commit() {
        let e = json!({"did": A, "time_us": 1_725_911_162_329_308i64, "kind": "commit",
            "commit": {"rev": "3l3qo2vutsw2b", "operation": "create",
                       "collection": "app.bsky.graph.block", "rkey": "3l3qo2vuowo2b",
                       "record": {"$type": "app.bsky.graph.block", "subject": B}}});
        let Frame::Event(ev) = decode_v1(e.to_string().as_bytes()).unwrap() else {
            panic!()
        };
        assert_eq!((ev.seq, ev.witness_us), (None, 1_725_911_162_329_308));
        let Body::Commit(op) = ev.body else {
            panic!("{:?}", ev.body)
        };
        assert!(matches!(
            op.action,
            CommitAction::Upsert {
                record: Record::Block(_),
                ..
            }
        ));
    }

    #[test]
    fn v1_rejections_keep_position() {
        let e = json!({"did": B, "time_us": 7, "kind": "commit",
            "commit": {"rev": "3l3qo2vutsw2b", "operation": "create",
                       "collection": "app.bsky.graph.listitem", "rkey": "3l3qo2vuowo2b",
                       "record": {"subject": B, "list": format!("at://{A}/app.bsky.graph.list/x")}}});
        let Frame::Event(ev) = decode_v1(e.to_string().as_bytes()).unwrap() else {
            panic!()
        };
        assert_eq!(ev.witness_us, 7);
        assert!(matches!(
            ev.body,
            Body::Rejected {
                reason: DropReason::ForeignListItem,
                ..
            }
        ));
        let bad_rev = json!({"did": A, "time_us": 8, "kind": "commit",
            "commit": {"rev": "nope", "operation": "delete",
                       "collection": "app.bsky.graph.block", "rkey": "k"}});
        let Frame::Event(ev) = decode_v1(bad_rev.to_string().as_bytes()).unwrap() else {
            panic!()
        };
        assert!(matches!(
            ev.body,
            Body::Rejected {
                reason: DropReason::Invalid,
                ..
            }
        ));
        assert!(decode_v1(b"{}").is_err());
        assert!(decode_v1(b"not json").is_err());
    }

    #[test]
    fn v1_account_identity() {
        let a = json!({"did": A, "time_us": 5, "kind": "account",
                       "account": {"active": false, "status": "takendown", "seq": 1, "did": A}});
        let Frame::Event(ev) = decode_v1(a.to_string().as_bytes()).unwrap() else {
            panic!()
        };
        assert_eq!(
            ev.body,
            Body::Account {
                did: Did::parse(A).unwrap(),
                active: false,
                status: Some("takendown".into())
            }
        );
        let i = json!({"did": "handle.test", "time_us": 6, "kind": "identity", "identity": {}});
        let Frame::Event(ev) = decode_v1(i.to_string().as_bytes()).unwrap() else {
            panic!()
        };
        assert!(matches!(ev.body, Body::Ignored(_)));
    }

    #[test]
    fn v2_frames() {
        let c = v2(
            json!({"$type": "network.bsky.jetstream.subscribeEvents#commit",
            "seq": 42, "did": A, "time": "2026-09-30T16:00:00.000000Z",
            "witnessedAt": "2026-09-30T16:00:01.000001Z",
            "rev": "3l3qo2vutsw2b", "operation": "delete",
            "collection": "app.bsky.graph.listblock", "rkey": "3l3qo2vuowo2b"}),
        );
        let Frame::Event(ev) = decode_v2(&c).unwrap() else {
            panic!()
        };
        assert_eq!(ev.seq, Some(42));
        assert_eq!(
            ev.witness_us,
            parse_time_us("2026-09-30T16:00:01.000001Z").unwrap()
        );
        assert!(matches!(ev.body, Body::Commit(ref op) if op.action == CommitAction::Delete));

        // `time` is the fallback witness for servers predating witnessedAt.
        let s = v2(
            json!({"$type": "network.bsky.jetstream.subscribeEvents#sync",
            "seq": 43, "did": A, "time": "2026-09-30T16:00:00.5Z", "sync": {"rev": "x"}}),
        );
        let Frame::Event(ev) = decode_v2(&s).unwrap() else {
            panic!()
        };
        assert_eq!(ev.body, Body::Sync(Did::parse(A).unwrap()));
        assert_eq!(
            ev.witness_us,
            parse_time_us("2026-09-30T16:00:00.5Z").unwrap()
        );

        let acct = v2(
            json!({"$type": "network.bsky.jetstream.subscribeEvents#account",
            "seq": 44, "did": A, "time": "2026-09-30T16:00:00Z",
            "account": {"did": A, "seq": 9, "time": "x", "active": true}}),
        );
        let Frame::Event(ev) = decode_v2(&acct).unwrap() else {
            panic!()
        };
        assert!(matches!(
            ev.body,
            Body::Account {
                active: true,
                status: None,
                ..
            }
        ));

        let info = v2(
            json!({"$type": "network.bsky.jetstream.subscribeEvents#info",
            "name": "OutdatedCursor", "message": "resumed at seq 10"}),
        );
        assert_eq!(
            decode_v2(&info).unwrap(),
            Frame::Info {
                name: "OutdatedCursor".into(),
                message: Some("resumed at seq 10".into())
            }
        );
        let err = br#"{"$type":"error","error":"ConsumerTooSlow"}"#;
        assert_eq!(
            decode_v2(err).unwrap(),
            Frame::Error {
                error: "ConsumerTooSlow".into(),
                message: None
            }
        );
        assert!(decode_v2(br#"{"$type":"message","payload":{"$type":"x#commit"}}"#).is_err());
    }
}
