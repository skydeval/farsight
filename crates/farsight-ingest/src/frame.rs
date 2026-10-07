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
//!
//! A frame is read one level at a time: the envelope's members are split
//! without being parsed, and each one the decoder needs is parsed on its
//! own. A commit's `record` is whatever its author wrote, so a record
//! that cannot be parsed (one nested deeper than the parser allows, for
//! instance) makes that commit a rejected one and leaves the frame, and
//! its position, readable. Only a frame whose envelope or position cannot
//! be read is undecodable.

use std::collections::HashMap;

use chrono::DateTime;
use farsight_core::record::{
    CommitAction, CommitOp, RecordError, commit_action, parse_commit_head,
};
use farsight_core::{Collection, Did};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

/// The two Jetstream protocols an instance may speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// v1 `/subscribe`.
    V1,
    /// v2 `subscribeEvents`.
    V2,
}

impl Protocol {
    /// The `v1` / `v2` label used in metrics and logs.
    pub fn label(self) -> &'static str {
        match self {
            Protocol::V1 => "v1",
            Protocol::V2 => "v2",
        }
    }

    /// The same protocol as the storage crate's enum, whose code is
    /// persisted in `firehose_state.protocol`.
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
    /// The `reason` label of `farsight_ingest_dropped_total`.
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
        /// Which rule the commit broke; the `reason` label of the drop
        /// counter.
        reason: DropReason,
        /// The collection if it was readable (metric label).
        collection: Option<Collection>,
        /// Detail for logs.
        detail: String,
        /// The delete applied in the record's place, when the commit is a
        /// create or an update whose repository, key and rev are valid
        /// and only its record is not: at that rev the key holds nothing
        /// Farsight indexes, so a version stored under it is removed.
        removes: Option<Box<CommitOp>>,
    },
    /// A commit on some other collection (not subscribed; ignored).
    OtherCommit,
    /// `identity`.
    Identity(Did),
    /// `account`.
    Account {
        /// The account whose status changed.
        did: Did,
        /// The event's `active` flag; false when the field is absent.
        active: bool,
        /// Upstream status when inactive.
        status: Option<String>,
        /// The `time` of the upstream account event, microseconds since
        /// the epoch, if the event carries a readable one. It is the same
        /// on every instance, unlike the witness time.
        time_us: Option<i64>,
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
    /// What the event carries, already validated.
    pub body: Body,
}

/// One decoded websocket message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// An event.
    Event(InEvent),
    /// v2 `#info` advisory (e.g. `OutdatedCursor`).
    Info {
        /// The advisory's `name`; empty if the frame has none.
        name: String,
        /// Its free-text `message`, if any.
        message: Option<String>,
    },
    /// v2 error frame; the server closes the stream after it.
    Error {
        /// The frame's `error` name; `Unknown` if it has none.
        error: String,
        /// Its free-text `message`, if any.
        message: Option<String>,
    },
}

/// Why a message could not be decoded at all (not even a position).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("undecodable frame: {0}")]
pub struct FrameError(pub String);

/// How far ahead of this machine's clock a witness time may be. Resume
/// cursors, failover gaps and the lag that coverage reports are all
/// measured from `applied_through`, so the allowance is no more than the
/// clocks of two well-kept machines can differ by.
pub const MAX_WITNESS_AHEAD_US: i64 = 5 * 60 * 1_000_000;

/// Checks a witness time an instance reports before anything is computed
/// from it or stored: it is the stream position, kept as a running
/// maximum, so one in the future would stand as `applied_through` until
/// the clock passed it, and one outside the range of the arithmetic done
/// on positions would overflow it. Accepted: the epoch up to
/// [`MAX_WITNESS_AHEAD_US`] past `now_us`.
pub fn check_witness(us: i64, now_us: i64) -> Result<i64, FrameError> {
    if us < 0 || us > now_us.saturating_add(MAX_WITNESS_AHEAD_US) {
        return Err(FrameError(format!("witness time {us} out of range")));
    }
    Ok(us)
}

/// Checks a v2 `seq`: not negative, and with room for the `seq + 1` a
/// resume asks for.
pub fn check_seq(seq: i64) -> Result<i64, FrameError> {
    if !(0..i64::MAX).contains(&seq) {
        return Err(FrameError(format!("seq {seq} out of range")));
    }
    Ok(seq)
}

/// This machine's clock, microseconds since the epoch.
pub(crate) fn now_us() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

/// RFC 3339 → microseconds since the epoch.
pub fn parse_time_us(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp_micros())
}

/// The members of a JSON object, each left as the text it was written
/// in. A name written twice keeps its last value.
type Members<'a> = HashMap<String, &'a RawValue>;

/// Splits a JSON object into its members without parsing them. `None`
/// for anything that is not one JSON object.
fn members(json: &[u8]) -> Option<Members<'_>> {
    serde_json::from_slice(json).ok()
}

/// The members of member `k` of `m`; none if it is absent or not an
/// object.
fn object<'a>(m: &Members<'a>, k: &str) -> Members<'a> {
    m.get(k)
        .and_then(|raw| members(raw.get().as_bytes()))
        .unwrap_or_default()
}

/// Member `k` of `m`, parsed; `None` if it is absent or cannot be parsed.
fn member(m: &Members<'_>, k: &str) -> Option<Value> {
    m.get(k)
        .and_then(|raw| serde_json::from_str(raw.get()).ok())
}

/// Member `k` of `m` if it is a string.
fn text(m: &Members<'_>, k: &str) -> Option<String> {
    match member(m, k) {
        Some(Value::String(s)) => Some(s),
        _ => None,
    }
}

/// Member `k` of `m` if it is an integer written as one.
fn integer(m: &Members<'_>, k: &str) -> Option<i64> {
    member(m, k).as_ref().and_then(Value::as_i64)
}

/// The body of a commit whose members are `commit` (`rev`, `operation`,
/// `collection`, `rkey`, `record`).
fn commit_body(did: &str, commit: &Members<'_>, now_us: i64) -> Body {
    let collection = text(commit, "collection")
        .as_deref()
        .and_then(Collection::from_nsid);
    if collection.is_none() {
        return Body::OtherCommit;
    }
    let rejected = |e: &RecordError, detail: String, removes: Option<CommitOp>| Body::Rejected {
        reason: match e {
            RecordError::ForeignListItem { .. } => DropReason::ForeignListItem,
            _ => DropReason::Invalid,
        },
        collection,
        detail,
        removes: removes.map(Box::new),
    };
    let mut head = Map::new();
    for k in ["collection", "rkey", "rev", "operation"] {
        if let Some(v) = member(commit, k) {
            head.insert(k.to_owned(), v);
        }
    }
    let head = match parse_commit_head(did, &Value::Object(head)) {
        Ok(h) => h,
        Err(e) => return rejected(&e, e.to_string(), None),
    };
    // Writes are ordered by rev, so one from the future is refused whole:
    // neither its record nor a delete in its place is applied at that rev.
    if head.rev.is_ahead_of(now_us) {
        let e = RecordError::Rev(head.rev.encode());
        return rejected(&e, format!("rev {} is ahead of the clock", head.rev), None);
    }
    let record = match commit.get("record") {
        None => Ok(None),
        Some(raw) => serde_json::from_str::<Value>(raw.get())
            .map(Some)
            .map_err(|e| RecordError::Json(e.to_string())),
    };
    match record.and_then(|r| commit_action(&head, r.as_ref())) {
        Ok(action) => Body::Commit(head.with(action)),
        Err(e) => {
            let detail = e.to_string();
            rejected(&e, detail, Some(head.with(CommitAction::Delete)))
        }
    }
}

fn repo_did(did: &str) -> Result<Did, Box<Body>> {
    Did::parse(did).map_err(|e| Box::new(Body::Ignored(format!("invalid repo DID {did:?}: {e}"))))
}

/// The body of an account event whose `account` object has the members
/// `account`.
fn account_body(did: &str, account: &Members<'_>) -> Body {
    match repo_did(did) {
        Ok(did) => Body::Account {
            did,
            active: member(account, "active")
                .as_ref()
                .and_then(Value::as_bool)
                .unwrap_or(false),
            status: text(account, "status"),
            time_us: text(account, "time").as_deref().and_then(parse_time_us),
        },
        Err(b) => *b,
    }
}

/// Decodes a v1 event (one JSON object).
pub fn decode_v1(bytes: &[u8]) -> Result<Frame, FrameError> {
    let v = members(bytes).ok_or_else(|| FrameError("not a JSON object".into()))?;
    let did = text(&v, "did").ok_or_else(|| FrameError("missing did".into()))?;
    let witness_us = integer(&v, "time_us").ok_or_else(|| FrameError("missing time_us".into()))?;
    let now = now_us();
    let witness_us = check_witness(witness_us, now)?;
    let body = match text(&v, "kind").as_deref() {
        Some("commit") => {
            if v.contains_key("commit") {
                commit_body(&did, &object(&v, "commit"), now)
            } else {
                Body::Rejected {
                    reason: DropReason::Invalid,
                    collection: None,
                    detail: "commit event without commit".into(),
                    removes: None,
                }
            }
        }
        Some("identity") => match repo_did(&did) {
            Ok(d) => Body::Identity(d),
            Err(b) => *b,
        },
        Some("account") => {
            if v.contains_key("account") {
                account_body(&did, &object(&v, "account"))
            } else {
                Body::Ignored("account event without account".into())
            }
        }
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
    let v = members(bytes).ok_or_else(|| FrameError("not a JSON object".into()))?;
    match text(&v, "$type").as_deref() {
        Some("error") => {
            return Ok(Frame::Error {
                error: text(&v, "error").unwrap_or_else(|| "Unknown".to_owned()),
                message: text(&v, "message"),
            });
        }
        Some("message") => {}
        other => return Err(FrameError(format!("unknown frame type {other:?}"))),
    }
    if !v.contains_key("payload") {
        return Err(FrameError("message without payload".into()));
    }
    let p = object(&v, "payload");
    let kind = text(&p, "$type")
        .as_deref()
        .and_then(|t| t.strip_prefix(V2_PREFIX))
        .map(str::to_owned)
        .ok_or_else(|| FrameError("payload without a subscribeEvents $type".into()))?;
    if kind == "info" {
        return Ok(Frame::Info {
            name: text(&p, "name").unwrap_or_default(),
            message: text(&p, "message"),
        });
    }
    let seq = integer(&p, "seq").ok_or_else(|| FrameError("event without seq".into()))?;
    let seq = check_seq(seq)?;
    let witness_us = text(&p, "witnessedAt")
        .or_else(|| text(&p, "time"))
        .as_deref()
        .and_then(parse_time_us)
        .ok_or_else(|| FrameError("event without a parseable witnessedAt/time".into()))?;
    let now = now_us();
    let witness_us = check_witness(witness_us, now)?;
    let did = text(&p, "did").ok_or_else(|| FrameError("event without did".into()))?;
    let body = match kind.as_str() {
        // v2 commit fields (rev, operation, collection, rkey, record) are
        // flat in the payload.
        "commit" => commit_body(&did, &p, now),
        "identity" => match repo_did(&did) {
            Ok(d) => Body::Identity(d),
            Err(b) => *b,
        },
        "account" => {
            if p.contains_key("account") {
                account_body(&did, &object(&p, "account"))
            } else {
                Body::Ignored("account event without account".into())
            }
        }
        "sync" => match repo_did(&did) {
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
    use farsight_core::record::Record;
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
                status: Some("takendown".into()),
                time_us: None,
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
                time_us: None,
                ..
            }
        ));
        // The upstream event's own time is kept apart from the witness time.
        let acct = v2(
            json!({"$type": "network.bsky.jetstream.subscribeEvents#account",
            "seq": 45, "did": A, "time": "2026-09-30T16:00:09Z",
            "account": {"did": A, "seq": 9, "time": "2026-09-30T16:00:03Z",
                        "active": false, "status": "deactivated"}}),
        );
        let Frame::Event(ev) = decode_v2(&acct).unwrap() else {
            panic!()
        };
        assert_eq!(
            ev.witness_us,
            parse_time_us("2026-09-30T16:00:09Z").unwrap()
        );
        assert!(matches!(
            ev.body,
            Body::Account { time_us, .. } if time_us == parse_time_us("2026-09-30T16:00:03Z")
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

    /// `depth` arrays inside one another.
    fn nested(depth: usize) -> String {
        format!("{}{}", "[".repeat(depth), "]".repeat(depth))
    }

    #[test]
    fn a_record_that_cannot_be_parsed_is_a_rejected_commit_at_its_position() {
        // Deeper than the JSON parser goes.
        let deep = nested(4_000);
        for op in ["create", "update"] {
            let v1 = format!(
                r#"{{"did":"{A}","time_us":9,"kind":"commit","commit":{{"rev":"3l3qo2vutsw2b",
                    "operation":"{op}","collection":"app.bsky.graph.block","rkey":"3l3qo2vuowo2b",
                    "record":{{"$type":"app.bsky.graph.block","subject":"{B}","x":{deep}}}}}}}"#
            );
            let v2 = format!(
                r#"{{"$type":"message","payload":{{"$type":"{V2_PREFIX}commit","seq":77,
                    "did":"{A}","time":"2026-09-30T16:00:00Z","rev":"3l3qo2vutsw2b",
                    "operation":"{op}","collection":"app.bsky.graph.block","rkey":"3l3qo2vuowo2b",
                    "record":{{"$type":"app.bsky.graph.block","subject":"{B}","x":{deep}}}}}}}"#
            );
            let frames = [
                (decode_v1(v1.as_bytes()), None, 9),
                (
                    decode_v2(v2.as_bytes()),
                    Some(77),
                    parse_time_us("2026-09-30T16:00:00Z").unwrap(),
                ),
            ];
            for (frame, seq, witness_us) in frames {
                let Ok(Frame::Event(ev)) = frame else {
                    panic!("{op}: {frame:?}")
                };
                assert_eq!((ev.seq, ev.witness_us), (seq, witness_us));
                let Body::Rejected {
                    reason,
                    collection,
                    removes,
                    ..
                } = ev.body
                else {
                    panic!("{op}: {:?}", ev.body)
                };
                assert_eq!(reason, DropReason::Invalid);
                assert_eq!(collection, Some(Collection::Block));
                // The key holds no block at that rev: a stored one goes.
                let removes = removes.expect("a delete in the record's place");
                assert_eq!(removes.action, CommitAction::Delete);
                assert_eq!(removes.author.as_str(), A);
                assert_eq!(removes.rkey.as_str(), "3l3qo2vuowo2b");
                assert_eq!(removes.rev.encode(), "3l3qo2vutsw2b");
            }
        }
        // Depth anywhere else the decoder does not read changes nothing.
        let e = format!(
            r#"{{"did":"{A}","time_us":9,"kind":"identity","identity":{deep},"extra":{deep}}}"#
        );
        let Ok(Frame::Event(ev)) = decode_v1(e.as_bytes()) else {
            panic!()
        };
        assert!(matches!(ev.body, Body::Identity(_)));
        // The commit of another collection stays ignored, whatever it holds.
        let e = format!(
            r#"{{"did":"{A}","time_us":9,"kind":"commit","commit":{{"rev":"3l3qo2vutsw2b",
                "operation":"create","collection":"app.bsky.feed.post","rkey":"x","record":{deep}}}}}"#
        );
        let Ok(Frame::Event(ev)) = decode_v1(e.as_bytes()) else {
            panic!()
        };
        assert_eq!(ev.body, Body::OtherCommit);
    }

    #[test]
    fn a_rejected_record_removes_the_version_under_its_key() {
        let commit = |op: &str, record: Value, rev: &str| {
            json!({"did": A, "time_us": 7, "kind": "commit",
                "commit": {"rev": rev, "operation": op,
                           "collection": "app.bsky.graph.listitem", "rkey": "3l3qo2vuowo2b",
                           "record": record}})
        };
        let removes = |e: Value| {
            let Frame::Event(ev) = decode_v1(e.to_string().as_bytes()).unwrap() else {
                panic!()
            };
            match ev.body {
                Body::Rejected {
                    reason, removes, ..
                } => (reason, removes),
                other => panic!("{other:?}"),
            }
        };
        // An update that names another repository's list, and one whose
        // subject is not a DID: the item stored under the key is gone.
        let foreign = json!({"subject": B, "list": format!("at://{B}/app.bsky.graph.list/x")});
        let (reason, r) = removes(commit("update", foreign, "3l3qo2vutsw2b"));
        assert_eq!(reason, DropReason::ForeignListItem);
        assert!(r.is_some_and(
            |op| op.action == CommitAction::Delete && op.collection == Collection::ListItem
        ));
        let bad = json!({"subject": "nobody", "list": format!("at://{A}/app.bsky.graph.list/x")});
        let (reason, r) = removes(commit("create", bad.clone(), "3l3qo2vutsw2b"));
        assert_eq!(reason, DropReason::Invalid);
        assert!(r.is_some());
        // Without a valid rev there is nothing to order a delete by.
        let (_, r) = removes(commit("update", bad, "nope"));
        assert!(r.is_none());
    }

    #[test]
    fn a_rev_from_the_future_is_rejected_whole() {
        use farsight_core::Tid;
        use farsight_core::tid::MAX_REV_AHEAD_US;
        let now = u64::try_from(now_us()).unwrap();
        let commit = |op: &str, rev: Tid| {
            json!({"did": A, "time_us": 7, "kind": "commit",
                "commit": {"rev": rev.encode(), "operation": op,
                           "collection": "app.bsky.graph.block", "rkey": "3l3qo2vuowo2b",
                           "record": {"subject": B}}})
        };
        let body = |e: Value| match decode_v1(e.to_string().as_bytes()).unwrap() {
            Frame::Event(ev) => ev.body,
            other => panic!("{other:?}"),
        };
        let soon = Tid::from_parts(now + MAX_REV_AHEAD_US / 2, 0).unwrap();
        let far = Tid::from_parts(now + 2 * MAX_REV_AHEAD_US, 0).unwrap();
        for op in ["create", "update", "delete"] {
            assert!(matches!(body(commit(op, soon)), Body::Commit(_)), "{op}");
            assert!(
                matches!(
                    body(commit(op, far)),
                    Body::Rejected {
                        reason: DropReason::Invalid,
                        removes: None,
                        ..
                    }
                ),
                "{op}"
            );
        }
    }

    #[test]
    fn positions_out_of_range_are_not_events() {
        let now = 1_800_000_000_000_000i64;
        assert_eq!(check_witness(0, now), Ok(0));
        assert_eq!(check_witness(now, now), Ok(now));
        assert_eq!(
            check_witness(now + MAX_WITNESS_AHEAD_US, now),
            Ok(now + MAX_WITNESS_AHEAD_US)
        );
        for bad in [-1, i64::MIN, now + MAX_WITNESS_AHEAD_US + 1, i64::MAX] {
            assert!(check_witness(bad, now).is_err(), "{bad}");
        }
        assert_eq!(check_seq(0), Ok(0));
        assert_eq!(check_seq(i64::MAX - 1), Ok(i64::MAX - 1));
        for bad in [-1, i64::MIN, i64::MAX] {
            assert!(check_seq(bad).is_err(), "{bad}");
        }

        // The decoders refuse them: the session ends as on any frame that
        // cannot be decoded, and nothing of the frame is stored.
        for time_us in [i64::MIN, -1, i64::MAX] {
            let e = json!({"did": A, "time_us": time_us, "kind": "identity", "identity": {}});
            assert!(decode_v1(e.to_string().as_bytes()).is_err(), "{time_us}");
        }
        let event = |seq: i64, at: &str| {
            v2(
                json!({"$type": "network.bsky.jetstream.subscribeEvents#identity",
                "seq": seq, "did": A, "time": at}),
            )
        };
        assert!(decode_v2(&event(5, "2026-09-30T16:00:00Z")).is_ok());
        assert!(decode_v2(&event(i64::MAX, "2026-09-30T16:00:00Z")).is_err());
        assert!(decode_v2(&event(-3, "2026-09-30T16:00:00Z")).is_err());
        assert!(decode_v2(&event(5, "9999-12-31T23:59:59Z")).is_err());
        assert!(decode_v2(&event(5, "0001-01-01T00:00:00Z")).is_err());
    }

    mod properties {
        use super::*;
        use farsight_core::{RecordKey, Tid};
        use proptest::prelude::*;

        /// What a peer may put where a number belongs.
        fn number() -> impl Strategy<Value = String> {
            prop_oneof![
                any::<i64>().prop_map(|n| n.to_string()),
                any::<u64>().prop_map(|n| n.to_string()),
                (0i64..4_000_000_000_000_000).prop_map(|n| n.to_string()),
                any::<f64>().prop_map(|x| format!("{x:?}")),
                "-?[0-9]{1,40}",
                "-?[0-9]{1,5}(\\.[0-9]{1,5})?[eE][+-]?[0-9]{1,4}",
                "\"-?[0-9]{1,20}\"",
                Just("null".to_owned()),
                Just("true".to_owned()),
                Just("[1]".to_owned()),
                Just("{}".to_owned()),
            ]
        }

        /// The value of a token that is an integer written the one way
        /// JSON and `i64` agree on.
        fn plain_i64(token: &str) -> Option<i64> {
            token.parse::<i64>().ok().filter(|n| n.to_string() == token)
        }

        fn rfc3339(us: i64) -> Option<String> {
            DateTime::from_timestamp_micros(us)
                .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
        }

        fn time() -> impl Strategy<Value = String> {
            prop_oneof![
                (-100_000_000_000_000_000i64..300_000_000_000_000_000)
                    .prop_filter_map("representable", rfc3339),
                (0i64..2_000_000_000_000_000).prop_filter_map("representable", rfc3339),
                "[0-9]{4,6}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,12})?(Z|[+-][0-9]{2}:[0-9]{2})",
                "\\PC{0,24}",
            ]
        }

        const WORDS: [&str; 24] = [
            A,
            "did:web:example.com",
            "handle.test",
            "commit",
            "identity",
            "account",
            "message",
            "error",
            "network.bsky.jetstream.subscribeEvents#commit",
            "network.bsky.jetstream.subscribeEvents#account",
            "network.bsky.jetstream.subscribeEvents#info",
            "network.bsky.jetstream.subscribeEvents#sync",
            "app.bsky.graph.block",
            "app.bsky.graph.listitem",
            "app.bsky.graph.list",
            "create",
            "delete",
            "3l3qo2vutsw2b",
            "2026-09-30T16:00:00Z",
            "9999-12-31T23:59:59Z",
            "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.list/x",
            "app.bsky.graph.defs#modlist",
            "",
            "self",
        ];

        const KEYS: [&str; 24] = [
            "did",
            "time_us",
            "kind",
            "commit",
            "identity",
            "account",
            "$type",
            "payload",
            "seq",
            "time",
            "witnessedAt",
            "rev",
            "operation",
            "collection",
            "rkey",
            "record",
            "subject",
            "list",
            "purpose",
            "name",
            "error",
            "message",
            "active",
            "status",
        ];

        /// JSON built from the names the decoders look for, holding
        /// anything.
        fn json_value() -> impl Strategy<Value = Value> {
            let leaf = prop_oneof![
                Just(Value::Null),
                any::<bool>().prop_map(Value::from),
                any::<i64>().prop_map(Value::from),
                any::<u64>().prop_map(Value::from),
                any::<f64>().prop_map(
                    |x| serde_json::Number::from_f64(x).map_or(Value::Null, Value::Number)
                ),
                prop::sample::select(WORDS.to_vec()).prop_map(Value::from),
                "\\PC{0,12}".prop_map(Value::from),
            ];
            leaf.prop_recursive(4, 48, 8, |inner| {
                prop_oneof![
                    prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
                    prop::collection::vec((prop::sample::select(KEYS.to_vec()), inner), 0..8)
                        .prop_map(|kv| {
                            Value::Object(kv.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
                        }),
                ]
            })
        }

        fn in_range(ev: &InEvent, after: i64) -> bool {
            (0..=after.saturating_add(MAX_WITNESS_AHEAD_US)).contains(&ev.witness_us)
                && ev.seq.is_none_or(|s| (0..i64::MAX).contains(&s))
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// Bytes of any kind: both decoders return.
            #[test]
            fn decoding_arbitrary_bytes_is_total(
                bytes in prop_oneof![
                    prop::collection::vec(any::<u8>(), 0..256),
                    "\\PC{0,128}".prop_map(String::into_bytes),
                ],
            ) {
                let _ = decode_v1(&bytes);
                let _ = decode_v2(&bytes);
            }

            /// JSON of any shape under the names the decoders read: both
            /// return, and an event either one yields has a position in
            /// range.
            #[test]
            fn decoding_arbitrary_json_yields_only_positions_in_range(v in json_value()) {
                let bytes = serde_json::to_vec(&v).unwrap();
                let (one, two) = (decode_v1(&bytes), decode_v2(&bytes));
                let after = now_us();
                for frame in [one, two].into_iter().flatten() {
                    if let Frame::Event(ev) = frame {
                        prop_assert!(in_range(&ev, after), "{:?}", ev);
                    }
                }
            }

            /// A v1 event with anything where `time_us` belongs is an
            /// event exactly when that is a plain integer from the epoch
            /// to the allowance past the clock, and then carries that
            /// integer.
            #[test]
            fn v1_time_us_is_taken_only_as_a_plain_integer_in_range(
                t in number(),
                kind in prop::sample::select(vec!["commit", "identity", "account", "other"]),
            ) {
                let text = format!(
                    r#"{{"did":"{A}","time_us":{t},"kind":"{kind}","account":{{"active":true}}}}"#
                );
                let before = now_us();
                let decoded = decode_v1(text.as_bytes());
                let after = now_us();
                match (&decoded, plain_i64(&t)) {
                    (Ok(Frame::Event(ev)), Some(n)) => {
                        prop_assert_eq!((ev.seq, ev.witness_us), (None, n));
                        prop_assert!(in_range(ev, after));
                    }
                    (Ok(other), _) => prop_assert!(false, "{} gave {:?}", t, other),
                    (Err(_), Some(n)) => {
                        prop_assert!(!(0..=before.saturating_add(MAX_WITNESS_AHEAD_US)).contains(&n));
                    }
                    (Err(_), None) => {}
                }
            }

            /// A v2 event with anything where `seq` and the time belong is
            /// an event exactly when `seq` is a plain integer in
            /// `[0, i64::MAX)` and the time is RFC 3339 from the epoch to
            /// the allowance past the clock; it then carries both.
            #[test]
            fn v2_seq_and_time_are_taken_only_in_range(
                seq in number(),
                at in time(),
                witnessed in any::<bool>(),
                kind in prop::sample::select(vec!["commit", "identity", "account", "sync", "other"]),
            ) {
                let field = if witnessed { "witnessedAt" } else { "time" };
                let text = format!(
                    r#"{{"$type":"message","payload":{{"$type":"{V2_PREFIX}{kind}","seq":{seq},"did":"{A}","{field}":{},"account":{{"active":false}}}}}}"#,
                    serde_json::to_string(&at).unwrap()
                );
                let before = now_us();
                let decoded = decode_v2(text.as_bytes());
                let after = now_us();
                let wanted = plain_i64(&seq)
                    .filter(|n| (0..i64::MAX).contains(n))
                    .zip(parse_time_us(&at));
                match (&decoded, wanted) {
                    (Ok(Frame::Event(ev)), Some((n, us))) => {
                        prop_assert_eq!((ev.seq, ev.witness_us), (Some(n), us));
                        prop_assert!(in_range(ev, after));
                    }
                    (Ok(other), _) => prop_assert!(false, "{} {} gave {:?}", seq, at, other),
                    (Err(_), Some((_, us))) => {
                        prop_assert!(!(0..=before.saturating_add(MAX_WITNESS_AHEAD_US)).contains(&us));
                    }
                    (Err(_), None) => {}
                }
            }

            /// The range checks agree with their definition for every
            /// value and clock, and an accepted `seq` has a successor.
            #[test]
            fn range_checks_are_total(us in any::<i64>(), now in any::<i64>(), seq in any::<i64>()) {
                let ok = us >= 0
                    && i128::from(us) <= i128::from(now) + i128::from(MAX_WITNESS_AHEAD_US);
                match check_witness(us, now) {
                    Ok(w) => prop_assert!(ok && w == us),
                    Err(_) => prop_assert!(!ok),
                }
                match check_seq(seq) {
                    Ok(s) => prop_assert!(s == seq && s >= 0 && s.checked_add(1).is_some()),
                    Err(_) => prop_assert!(seq < 0 || seq == i64::MAX),
                }
            }

            /// Events made from valid parts come back as those parts, on
            /// both protocols.
            #[test]
            fn valid_events_round_trip(
                plc in "[a-z2-7]{24}",
                us in 0i64..1_700_000_000_000_000,
                seq in 0i64..i64::MAX,
                rev_us in 0u64..1_700_000_000_000_000,
                clock in 0u16..1024,
                rkey in "[A-Za-z0-9_:~-][A-Za-z0-9._:~-]{0,20}",
                c in 0usize..4,
                active in any::<bool>(),
                status in prop::option::of("[a-z]{1,12}"),
            ) {
                let did = Did::parse(&format!("did:plc:{plc}")).unwrap();
                let rev = Tid::from_parts(rev_us, clock).unwrap();
                let collection = Collection::ALL[c];
                let at = rfc3339(us).unwrap();
                let deleted = Body::Commit(CommitOp {
                    author: did.clone(),
                    collection,
                    rkey: RecordKey::parse(&rkey).unwrap(),
                    rev,
                    action: farsight_core::CommitAction::Delete,
                });
                let account = Body::Account {
                    did: did.clone(),
                    active,
                    status: status.clone(),
                    time_us: None,
                };

                let commit = json!({"rev": rev.encode(), "operation": "delete",
                    "collection": collection.nsid(), "rkey": rkey});
                let e = json!({"did": did.as_str(), "time_us": us, "kind": "commit", "commit": commit});
                prop_assert_eq!(
                    decode_v1(e.to_string().as_bytes()),
                    Ok(Frame::Event(InEvent { seq: None, witness_us: us, body: deleted.clone() }))
                );
                let e = json!({"did": did.as_str(), "time_us": us, "kind": "account",
                    "account": {"active": active, "status": status}});
                prop_assert_eq!(
                    decode_v1(e.to_string().as_bytes()),
                    Ok(Frame::Event(InEvent { seq: None, witness_us: us, body: account.clone() }))
                );

                let e = v2(json!({"$type": format!("{V2_PREFIX}commit"), "seq": seq,
                    "did": did.as_str(), "time": "2020-01-01T00:00:00Z", "witnessedAt": at,
                    "rev": rev.encode(), "operation": "delete",
                    "collection": collection.nsid(), "rkey": rkey}));
                prop_assert_eq!(
                    decode_v2(&e),
                    Ok(Frame::Event(InEvent { seq: Some(seq), witness_us: us, body: deleted }))
                );
                let e = v2(json!({"$type": format!("{V2_PREFIX}account"), "seq": seq,
                    "did": did.as_str(), "time": at,
                    "account": {"active": active, "status": status}}));
                prop_assert_eq!(
                    decode_v2(&e),
                    Ok(Frame::Event(InEvent { seq: Some(seq), witness_us: us, body: account }))
                );
                for (kind, body) in [("identity", Body::Identity(did.clone())), ("sync", Body::Sync(did.clone()))] {
                    let e = v2(json!({"$type": format!("{V2_PREFIX}{kind}"), "seq": seq,
                        "did": did.as_str(), "time": at}));
                    prop_assert_eq!(
                        decode_v2(&e),
                        Ok(Frame::Event(InEvent { seq: Some(seq), witness_us: us, body }))
                    );
                }
            }
        }
    }
}
