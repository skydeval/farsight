//! Parsing and validation of the four indexed record types, from Jetstream
//! JSON commit events and from `listRecords`/`getRecord` values.
//!
//! Admission rules (design §1.1):
//! - `block`: `subject` is a valid DID.
//! - `listblock`: `subject` is `at://<did>/app.bsky.graph.list/<rkey>`.
//! - `list`: every record; `name` truncated to 128 chars, `purpose` mapped.
//! - `listitem`: `subject` is a DID and `list` a list URI **whose authority
//!   is the listitem's author** (the authority rule). A foreign listitem is
//!   rejected here, at parse time, with [`RecordError::ForeignListItem`].
//!
//! Whether a listitem is *applied* (its list is tracked) is decided later,
//! under the list lock, by the storage crate.

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::aturi::{AtUri, RecordKey};
use crate::did::Did;
use crate::nsid::Collection;
use crate::tid::Tid;

/// Maximum stored list name length, in characters (design §1.1).
pub const MAX_LIST_NAME_CHARS: usize = 128;

/// Why a record or event was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecordError {
    /// The JSON is not an object or is not valid JSON.
    #[error("malformed JSON: {0}")]
    Json(String),
    /// A required field is absent or has the wrong JSON type.
    #[error("missing or mistyped field `{0}`")]
    Field(&'static str),
    /// `$type` is present and names a different collection.
    #[error("record $type {found:?} does not match collection {expected}")]
    TypeMismatch {
        /// The collection the record was written to.
        expected: Collection,
        /// The `$type` it declared.
        found: String,
    },
    /// A `block` or `listitem` subject is not an accepted DID.
    #[error("invalid subject DID: {0}")]
    SubjectDid(String),
    /// A `listblock` subject or `listitem` list is not a list AT-URI.
    #[error("invalid list URI: {0}")]
    ListUri(String),
    /// Authority rule: a listitem naming a list outside its author's repo.
    #[error("listitem by {author} names foreign list {list}")]
    ForeignListItem {
        /// The listitem's author.
        author: String,
        /// The list it named.
        list: String,
    },
    /// The event's repo DID is invalid.
    #[error("invalid repo DID: {0}")]
    RepoDid(String),
    /// The commit rev is not a TID (design §7.1: rejected).
    #[error("rev is not a TID: {0}")]
    Rev(String),
    /// The record key is invalid.
    #[error("invalid record key: {0}")]
    RecordKey(String),
    /// The operation is not create, update or delete.
    #[error("unknown operation: {0}")]
    Operation(String),
}

/// `app.bsky.graph.list` purpose, stored as `lists.purpose` (design §7.1:
/// 1 mod, 2 curate, 3 reference, 0 other).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ListPurpose {
    /// `app.bsky.graph.defs#modlist`.
    Mod,
    /// `app.bsky.graph.defs#curatelist`.
    Curate,
    /// `app.bsky.graph.defs#referencelist`.
    Reference,
    /// Anything else, including a missing purpose.
    Other,
}

impl ListPurpose {
    /// Maps a lexicon purpose token.
    pub fn from_token(s: &str) -> ListPurpose {
        match s {
            "app.bsky.graph.defs#modlist" => ListPurpose::Mod,
            "app.bsky.graph.defs#curatelist" => ListPurpose::Curate,
            "app.bsky.graph.defs#referencelist" => ListPurpose::Reference,
            _ => ListPurpose::Other,
        }
    }

    /// Storage code.
    pub fn code(self) -> i16 {
        match self {
            ListPurpose::Other => 0,
            ListPurpose::Mod => 1,
            ListPurpose::Curate => 2,
            ListPurpose::Reference => 3,
        }
    }

    /// Inverse of [`ListPurpose::code`]; unknown codes map to `Other`.
    pub fn from_code(code: i16) -> ListPurpose {
        match code {
            1 => ListPurpose::Mod,
            2 => ListPurpose::Curate,
            3 => ListPurpose::Reference,
            _ => ListPurpose::Other,
        }
    }

    /// The short API name (`modlist`, `curatelist`, `referencelist`, `other`).
    pub fn api_name(self) -> &'static str {
        match self {
            ListPurpose::Mod => "modlist",
            ListPurpose::Curate => "curatelist",
            ListPurpose::Reference => "referencelist",
            ListPurpose::Other => "other",
        }
    }
}

/// A parsed `app.bsky.graph.block`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRecord {
    /// The blocked account.
    pub subject: Did,
    /// Author-claimed creation time; display only. `None` if absent or
    /// unparseable.
    pub created_at: Option<DateTime<Utc>>,
}

/// A parsed `app.bsky.graph.listblock`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBlockRecord {
    /// The list subscribed to (collection is `app.bsky.graph.list`).
    pub subject: AtUri,
    /// Author-claimed creation time; display only.
    pub created_at: Option<DateTime<Utc>>,
}

/// A parsed `app.bsky.graph.list`. Only the stored fields are kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRecord {
    /// The list's purpose.
    pub purpose: ListPurpose,
    /// Name, truncated to [`MAX_LIST_NAME_CHARS`].
    pub name: Option<String>,
    /// Author-claimed creation time; display only.
    pub created_at: Option<DateTime<Utc>>,
}

/// A parsed `app.bsky.graph.listitem` that satisfies the authority rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListItemRecord {
    /// The listed account.
    pub subject: Did,
    /// The list, always in the item author's own repo.
    pub list: AtUri,
    /// Author-claimed creation time; display only.
    pub created_at: Option<DateTime<Utc>>,
}

/// A parsed record of one of the four collections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    /// `app.bsky.graph.block`.
    Block(BlockRecord),
    /// `app.bsky.graph.listblock`.
    ListBlock(ListBlockRecord),
    /// `app.bsky.graph.list`.
    List(ListRecord),
    /// `app.bsky.graph.listitem`.
    ListItem(ListItemRecord),
}

impl Record {
    /// The collection this record belongs to.
    pub fn collection(&self) -> Collection {
        match self {
            Record::Block(_) => Collection::Block,
            Record::ListBlock(_) => Collection::ListBlock,
            Record::List(_) => Collection::List,
            Record::ListItem(_) => Collection::ListItem,
        }
    }
}

fn str_field<'a>(v: &'a Value, name: &'static str) -> Result<&'a str, RecordError> {
    v.get(name)
        .and_then(Value::as_str)
        .ok_or(RecordError::Field(name))
}

fn created_at(v: &Value) -> Option<DateTime<Utc>> {
    let s = v.get("createdAt")?.as_str()?;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

fn list_uri(s: &str) -> Result<AtUri, RecordError> {
    let uri = AtUri::parse(s).map_err(|_| RecordError::ListUri(s.to_owned()))?;
    if uri.indexed_collection() != Some(Collection::List) {
        return Err(RecordError::ListUri(s.to_owned()));
    }
    Ok(uri)
}

/// Truncates to at most `max` characters (not bytes).
pub fn truncate_chars(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

/// Parses and validates a record value written by `author` to
/// `collection`. Applies the authority rule to listitems.
pub fn parse_record(
    author: &Did,
    collection: Collection,
    value: &Value,
) -> Result<Record, RecordError> {
    if !value.is_object() {
        return Err(RecordError::Json("record is not an object".to_owned()));
    }
    if let Some(t) = value.get("$type") {
        let t = t.as_str().ok_or(RecordError::Field("$type"))?;
        if t != collection.nsid() {
            return Err(RecordError::TypeMismatch {
                expected: collection,
                found: t.to_owned(),
            });
        }
    }
    let created_at = created_at(value);
    match collection {
        Collection::Block => {
            let s = str_field(value, "subject")?;
            let subject = Did::parse(s).map_err(|_| RecordError::SubjectDid(s.to_owned()))?;
            Ok(Record::Block(BlockRecord {
                subject,
                created_at,
            }))
        }
        Collection::ListBlock => {
            let subject = list_uri(str_field(value, "subject")?)?;
            Ok(Record::ListBlock(ListBlockRecord {
                subject,
                created_at,
            }))
        }
        Collection::List => {
            let purpose = value
                .get("purpose")
                .and_then(Value::as_str)
                .map_or(ListPurpose::Other, ListPurpose::from_token);
            let name = value
                .get("name")
                .and_then(Value::as_str)
                .map(|n| truncate_chars(n, MAX_LIST_NAME_CHARS).to_owned());
            Ok(Record::List(ListRecord {
                purpose,
                name,
                created_at,
            }))
        }
        Collection::ListItem => {
            let s = str_field(value, "subject")?;
            let subject = Did::parse(s).map_err(|_| RecordError::SubjectDid(s.to_owned()))?;
            let list = list_uri(str_field(value, "list")?)?;
            if list.authority != *author {
                return Err(RecordError::ForeignListItem {
                    author: author.to_string(),
                    list: list.to_string(),
                });
            }
            Ok(Record::ListItem(ListItemRecord {
                subject,
                list,
                created_at,
            }))
        }
    }
}

/// A commit operation as it appears in a Jetstream event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// `create`.
    Create,
    /// `update`.
    Update,
    /// `delete`.
    Delete,
}

/// What a commit does to one record key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitAction {
    /// Create or update with the parsed new version.
    Upsert {
        /// `Create` or `Update`.
        op: Operation,
        /// The new version.
        record: Record,
    },
    /// Delete.
    Delete,
}

/// One validated commit operation on an indexed collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOp {
    /// The repo (record author).
    pub author: Did,
    /// The collection.
    pub collection: Collection,
    /// The record key.
    pub rkey: RecordKey,
    /// The commit rev.
    pub rev: Tid,
    /// Upsert or delete.
    pub action: CommitAction,
}

/// A Jetstream event, reduced to what Farsight uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JetstreamEvent {
    /// A commit on one of the four collections.
    Commit {
        /// Jetstream's `time_us` (witness time, microseconds).
        time_us: i64,
        /// The validated operation.
        op: CommitOp,
    },
    /// A commit on some other collection (not subscribed; ignored).
    OtherCommit {
        /// Jetstream's `time_us`.
        time_us: i64,
    },
    /// An `identity` event.
    Identity {
        /// The DID whose identity changed.
        did: Did,
        /// Jetstream's `time_us`.
        time_us: i64,
    },
    /// An `account` event.
    Account {
        /// The account.
        did: Did,
        /// Jetstream's `time_us`.
        time_us: i64,
        /// Whether the account is active.
        active: bool,
        /// Upstream status string when inactive (`deactivated`,
        /// `takendown`, …), if given.
        status: Option<String>,
    },
}

/// Parses one Jetstream (v1 `/subscribe`) JSON event.
///
/// Commits on the four collections are fully validated; a commit that
/// fails validation is an error (the caller counts it as dropped,
/// `reason="invalid"` or `"foreign_listitem"`). The v2 envelope is handled
/// by the ingest crate, which hands the inner commit object to
/// [`parse_commit`].
pub fn parse_jetstream_event(json: &str) -> Result<JetstreamEvent, RecordError> {
    let v: Value = serde_json::from_str(json).map_err(|e| RecordError::Json(e.to_string()))?;
    let did_str = str_field(&v, "did")?;
    let time_us = v
        .get("time_us")
        .and_then(Value::as_i64)
        .ok_or(RecordError::Field("time_us"))?;
    let kind = str_field(&v, "kind")?;
    match kind {
        "commit" => {
            let commit = v.get("commit").ok_or(RecordError::Field("commit"))?;
            let collection = str_field(commit, "collection")?;
            if Collection::from_nsid(collection).is_none() {
                return Ok(JetstreamEvent::OtherCommit { time_us });
            }
            let op = parse_commit(did_str, commit)?;
            Ok(JetstreamEvent::Commit { time_us, op })
        }
        "identity" => Ok(JetstreamEvent::Identity {
            did: Did::parse(did_str).map_err(|_| RecordError::RepoDid(did_str.to_owned()))?,
            time_us,
        }),
        "account" => {
            let account = v.get("account").ok_or(RecordError::Field("account"))?;
            let active = account
                .get("active")
                .and_then(Value::as_bool)
                .ok_or(RecordError::Field("active"))?;
            let status = account
                .get("status")
                .and_then(Value::as_str)
                .map(str::to_owned);
            Ok(JetstreamEvent::Account {
                did: Did::parse(did_str).map_err(|_| RecordError::RepoDid(did_str.to_owned()))?,
                time_us,
                active,
                status,
            })
        }
        other => Err(RecordError::Operation(format!(
            "unknown event kind {other}"
        ))),
    }
}

/// Parses a Jetstream commit object (`{rev, operation, collection, rkey,
/// record?}`) for repo `did`. The collection must be one of the four.
pub fn parse_commit(did: &str, commit: &Value) -> Result<CommitOp, RecordError> {
    let author = Did::parse(did).map_err(|_| RecordError::RepoDid(did.to_owned()))?;
    let collection_str = str_field(commit, "collection")?;
    let collection =
        Collection::from_nsid(collection_str).ok_or(RecordError::Field("collection"))?;
    let rkey_str = str_field(commit, "rkey")?;
    let rkey =
        RecordKey::parse(rkey_str).map_err(|_| RecordError::RecordKey(rkey_str.to_owned()))?;
    let rev_str = str_field(commit, "rev")?;
    let rev = Tid::parse(rev_str).map_err(|_| RecordError::Rev(rev_str.to_owned()))?;
    let op = match str_field(commit, "operation")? {
        "create" => Operation::Create,
        "update" => Operation::Update,
        "delete" => Operation::Delete,
        other => return Err(RecordError::Operation(other.to_owned())),
    };
    let action = match op {
        Operation::Delete => CommitAction::Delete,
        Operation::Create | Operation::Update => {
            let value = commit.get("record").ok_or(RecordError::Field("record"))?;
            CommitAction::Upsert {
                op,
                record: parse_record(&author, collection, value)?,
            }
        }
    };
    Ok(CommitOp {
        author,
        collection,
        rkey,
        rev,
        action,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    fn did(s: &str) -> Did {
        Did::parse(s).unwrap()
    }

    #[test]
    fn block() {
        let r = parse_record(
            &did(A),
            Collection::Block,
            &json!({"$type": "app.bsky.graph.block", "subject": B,
                    "createdAt": "2026-03-01T12:00:00.000Z"}),
        )
        .unwrap();
        let Record::Block(b) = r else { panic!() };
        assert_eq!(b.subject.as_str(), B);
        assert!(b.created_at.is_some());
    }

    #[test]
    fn block_rejections() {
        let a = did(A);
        for v in [
            json!({"subject": "alice.bsky.social"}),
            json!({"subject": "did:key:zabc"}),
            json!({"subject": 5}),
            json!({}),
            json!("not an object"),
            json!({"$type": "app.bsky.graph.listblock", "subject": B}),
        ] {
            assert!(
                parse_record(&a, Collection::Block, &v).is_err(),
                "accepted {v}"
            );
        }
        // Bad createdAt is tolerated (display only).
        let r = parse_record(
            &a,
            Collection::Block,
            &json!({"subject": B, "createdAt": "x"}),
        )
        .unwrap();
        let Record::Block(b) = r else { panic!() };
        assert_eq!(b.created_at, None);
    }

    #[test]
    fn listblock() {
        let a = did(A);
        let ok = json!({"subject": format!("at://{B}/app.bsky.graph.list/3k2abc")});
        assert!(matches!(
            parse_record(&a, Collection::ListBlock, &ok),
            Ok(Record::ListBlock(_))
        ));
        for v in [
            json!({"subject": format!("at://{B}/app.bsky.graph.block/3k2abc")}),
            json!({"subject": "at://alice.bsky.social/app.bsky.graph.list/3k2abc"}),
            json!({"subject": B}),
        ] {
            assert!(matches!(
                parse_record(&a, Collection::ListBlock, &v),
                Err(RecordError::ListUri(_))
            ));
        }
    }

    #[test]
    fn list_name_truncated_and_purpose() {
        let long = "é".repeat(200);
        let r = parse_record(
            &did(A),
            Collection::List,
            &json!({"name": long, "purpose": "app.bsky.graph.defs#modlist"}),
        )
        .unwrap();
        let Record::List(l) = r else { panic!() };
        assert_eq!(l.name.unwrap().chars().count(), MAX_LIST_NAME_CHARS);
        assert_eq!(l.purpose, ListPurpose::Mod);
        let r = parse_record(&did(A), Collection::List, &json!({"purpose": "x#y"})).unwrap();
        let Record::List(l) = r else { panic!() };
        assert_eq!(l.purpose, ListPurpose::Other);
        assert_eq!(l.name, None);
        for p in [
            ListPurpose::Mod,
            ListPurpose::Curate,
            ListPurpose::Reference,
            ListPurpose::Other,
        ] {
            assert_eq!(ListPurpose::from_code(p.code()), p);
        }
    }

    #[test]
    fn listitem_authority_rule() {
        let own = json!({"subject": B, "list": format!("at://{A}/app.bsky.graph.list/3k2abc")});
        assert!(matches!(
            parse_record(&did(A), Collection::ListItem, &own),
            Ok(Record::ListItem(_))
        ));
        // Same record in B's repo names A's list: foreign, rejected.
        assert!(matches!(
            parse_record(&did(B), Collection::ListItem, &own),
            Err(RecordError::ForeignListItem { .. })
        ));
        let bad_subject =
            json!({"subject": "bob.test", "list": format!("at://{A}/app.bsky.graph.list/3k")});
        assert!(parse_record(&did(A), Collection::ListItem, &bad_subject).is_err());
        let not_list =
            json!({"subject": B, "list": format!("at://{A}/app.bsky.graph.block/3k2abc")});
        assert!(parse_record(&did(A), Collection::ListItem, &not_list).is_err());
    }

    #[test]
    fn jetstream_commit() {
        let ev = json!({
            "did": A, "time_us": 1_725_911_162_329_308i64, "kind": "commit",
            "commit": {"rev": "3l3qo2vutsw2b", "operation": "create",
                       "collection": "app.bsky.graph.block", "rkey": "3l3qo2vuowo2b",
                       "record": {"$type": "app.bsky.graph.block", "subject": B,
                                  "createdAt": "2024-09-09T19:46:02.102Z"},
                       "cid": "bafyreidc6sydkkbchcyg62v77wbhzvb2mvytlmsychqgwf2xojjtirmzj4"}
        });
        let parsed = parse_jetstream_event(&ev.to_string()).unwrap();
        let JetstreamEvent::Commit { time_us, op } = parsed else {
            panic!()
        };
        assert_eq!(time_us, 1_725_911_162_329_308);
        assert_eq!(op.rev, Tid::parse("3l3qo2vutsw2b").unwrap());
        assert!(matches!(
            op.action,
            CommitAction::Upsert {
                op: Operation::Create,
                record: Record::Block(_)
            }
        ));

        let del = json!({
            "did": A, "time_us": 1, "kind": "commit",
            "commit": {"rev": "3l3qo2vutsw2c", "operation": "delete",
                       "collection": "app.bsky.graph.listitem", "rkey": "3l3qo2vuowo2b"}
        });
        let JetstreamEvent::Commit { op, .. } = parse_jetstream_event(&del.to_string()).unwrap()
        else {
            panic!()
        };
        assert_eq!(op.action, CommitAction::Delete);
    }

    #[test]
    fn jetstream_rejections() {
        let bad_rev = json!({
            "did": A, "time_us": 1, "kind": "commit",
            "commit": {"rev": "not-a-tid", "operation": "delete",
                       "collection": "app.bsky.graph.block", "rkey": "3l3qo2vuowo2b"}
        });
        assert!(matches!(
            parse_jetstream_event(&bad_rev.to_string()),
            Err(RecordError::Rev(_))
        ));
        let foreign = json!({
            "did": B, "time_us": 1, "kind": "commit",
            "commit": {"rev": "3l3qo2vutsw2b", "operation": "create",
                       "collection": "app.bsky.graph.listitem", "rkey": "3l3qo2vuowo2b",
                       "record": {"subject": B,
                                  "list": format!("at://{A}/app.bsky.graph.list/3k2abc")}}
        });
        assert!(matches!(
            parse_jetstream_event(&foreign.to_string()),
            Err(RecordError::ForeignListItem { .. })
        ));
        let bad_repo = json!({
            "did": "alice.test", "time_us": 1, "kind": "commit",
            "commit": {"rev": "3l3qo2vutsw2b", "operation": "delete",
                       "collection": "app.bsky.graph.block", "rkey": "3l3qo2vuowo2b"}
        });
        assert!(matches!(
            parse_jetstream_event(&bad_repo.to_string()),
            Err(RecordError::RepoDid(_))
        ));
        let other = json!({
            "did": A, "time_us": 7, "kind": "commit",
            "commit": {"rev": "3l3qo2vutsw2b", "operation": "create",
                       "collection": "app.bsky.feed.post", "rkey": "x", "record": {}}
        });
        assert_eq!(
            parse_jetstream_event(&other.to_string()).unwrap(),
            JetstreamEvent::OtherCommit { time_us: 7 }
        );
    }

    #[test]
    fn jetstream_account_and_identity() {
        let acc = json!({"did": A, "time_us": 5, "kind": "account",
                         "account": {"active": false, "status": "takendown", "seq": 1}});
        assert_eq!(
            parse_jetstream_event(&acc.to_string()).unwrap(),
            JetstreamEvent::Account {
                did: did(A),
                time_us: 5,
                active: false,
                status: Some("takendown".to_owned())
            }
        );
        let id = json!({"did": A, "time_us": 6, "kind": "identity", "identity": {}});
        assert!(matches!(
            parse_jetstream_event(&id.to_string()).unwrap(),
            JetstreamEvent::Identity { .. }
        ));
    }
}
