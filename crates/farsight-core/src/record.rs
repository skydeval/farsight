//! Parsing and validation of the four indexed record types, from Jetstream
//! JSON commit events and from `listRecords`/`getRecord` values.
//!
//! Admission rules (see `docs/design/README.md`):
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

/// Maximum stored list name length, in characters.
pub const MAX_LIST_NAME_CHARS: usize = 128;
/// Longest stored list description, in characters (the lexicon allows
/// 300 graphemes).
pub const MAX_LIST_DESCRIPTION_CHARS: usize = 300;
/// Longest blob CID stored, in characters.
pub const MAX_BLOB_CID_CHARS: usize = 128;

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
    /// The commit rev is not a TID (rejected).
    #[error("rev is not a TID: {0}")]
    Rev(String),
    /// The record key is invalid.
    #[error("invalid record key: {0}")]
    RecordKey(String),
    /// The operation is not create, update or delete.
    #[error("unknown operation: {0}")]
    Operation(String),
}

/// `app.bsky.graph.list` purpose, stored as `lists.purpose` (1 mod, 2
/// curate, 3 reference, 0 other).
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
    /// Every purpose.
    pub const ALL: [ListPurpose; 4] = [
        ListPurpose::Mod,
        ListPurpose::Curate,
        ListPurpose::Reference,
        ListPurpose::Other,
    ];

    /// Maps a lexicon purpose token.
    pub fn from_token(s: &str) -> ListPurpose {
        match s {
            "app.bsky.graph.defs#modlist" => ListPurpose::Mod,
            "app.bsky.graph.defs#curatelist" => ListPurpose::Curate,
            "app.bsky.graph.defs#referencelist" => ListPurpose::Reference,
            _ => ListPurpose::Other,
        }
    }

    /// The `lists.purpose` storage code: 0 other, 1 mod, 2 curate, 3
    /// reference.
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
    /// Description, truncated to [`MAX_LIST_DESCRIPTION_CHARS`]; `None`
    /// for a missing or blank one. Plain text.
    pub description: Option<String>,
    /// CID of the avatar blob, if the record names one of the usual
    /// shape. The image is never fetched by Farsight.
    pub avatar: Option<String>,
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

/// Whether `s` has the shape of a CID as records write it: base32
/// multibase, at most [`MAX_BLOB_CID_CHARS`] characters.
pub fn is_blob_cid(s: &str) -> bool {
    (8..=MAX_BLOB_CID_CHARS).contains(&s.len())
        && s.starts_with('b')
        && s.bytes().all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7'))
}

/// The description and avatar CID of a list record's JSON, as stored.
pub fn list_about(value: &Value) -> (Option<String>, Option<String>) {
    let description = value
        .get("description")
        .and_then(Value::as_str)
        .map(|d| {
            truncate_chars(d.trim(), MAX_LIST_DESCRIPTION_CHARS)
                .replace('\0', "")
                .trim_end()
                .to_owned()
        })
        .filter(|d| !d.is_empty());
    let avatar = value
        .get("avatar")
        .and_then(|a| a.get("ref"))
        .and_then(|r| r.get("$link"))
        .and_then(Value::as_str)
        .filter(|c| is_blob_cid(c))
        .map(str::to_owned);
    (description, avatar)
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
                .map(|n| truncate_chars(n, MAX_LIST_NAME_CHARS).replace('\0', ""));
            let (description, avatar) = list_about(value);
            Ok(Record::List(ListRecord {
                purpose,
                name,
                description,
                avatar,
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
        /// The record's new version, parsed and validated.
        record: Record,
    },
    /// Delete of the record key. A Jetstream delete carries no record.
    Delete,
}

/// One validated commit operation on an indexed collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOp {
    /// The repo (record author).
    pub author: Did,
    /// Which of the four indexed collections the record is in.
    pub collection: Collection,
    /// The record key.
    pub rkey: RecordKey,
    /// Rev of the commit that carried the operation.
    pub rev: Tid,
    /// Upsert or delete.
    pub action: CommitAction,
}

/// What a commit says apart from its record: whose repository, which
/// key, at which rev, and the operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitHead {
    /// The repo (record author).
    pub author: Did,
    /// Which of the four indexed collections the record is in.
    pub collection: Collection,
    /// The record key.
    pub rkey: RecordKey,
    /// Rev of the commit that carried the operation.
    pub rev: Tid,
    /// `create`, `update` or `delete`.
    pub op: Operation,
}

impl CommitHead {
    /// The commit operation that does `action` to this key at this rev.
    pub fn with(self, action: CommitAction) -> CommitOp {
        CommitOp {
            author: self.author,
            collection: self.collection,
            rkey: self.rkey,
            rev: self.rev,
            action,
        }
    }
}

/// Parses everything of a Jetstream commit object but its record
/// (`{rev, operation, collection, rkey}`) for repo `did`. The collection
/// must be one of the four.
pub fn parse_commit_head(did: &str, commit: &Value) -> Result<CommitHead, RecordError> {
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
    Ok(CommitHead {
        author,
        collection,
        rkey,
        rev,
        op,
    })
}

/// The action of a commit with head `head`: a delete, or the upsert of
/// `record` (the commit's `record` value; `None` if it has none).
pub fn commit_action(
    head: &CommitHead,
    record: Option<&Value>,
) -> Result<CommitAction, RecordError> {
    match head.op {
        Operation::Delete => Ok(CommitAction::Delete),
        op @ (Operation::Create | Operation::Update) => {
            let value = record.ok_or(RecordError::Field("record"))?;
            Ok(CommitAction::Upsert {
                op,
                record: parse_record(&head.author, head.collection, value)?,
            })
        }
    }
}

/// Parses a Jetstream commit object (`{rev, operation, collection, rkey,
/// record?}`) for repo `did`. The collection must be one of the four.
pub fn parse_commit(did: &str, commit: &Value) -> Result<CommitOp, RecordError> {
    let head = parse_commit_head(did, commit)?;
    let action = commit_action(&head, commit.get("record"))?;
    Ok(head.with(action))
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
        assert_eq!((l.description, l.avatar), (None, None));
        let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
        let r = parse_record(
            &did(A),
            Collection::List,
            &json!({
                "name": "n",
                "description": format!("  {}\n", "é".repeat(400)),
                "avatar": {"$type": "blob", "ref": {"$link": cid}, "mimeType": "image/png", "size": 1}
            }),
        )
        .unwrap();
        let Record::List(l) = r else { panic!() };
        assert_eq!(
            l.description.as_deref().map(|d| d.chars().count()),
            Some(300)
        );
        assert_eq!(l.avatar.as_deref(), Some(cid));
        let r = parse_record(
            &did(A),
            Collection::List,
            &json!({"description": "  \n ", "avatar": {"ref": {"$link": "x y"}}}),
        )
        .unwrap();
        let Record::List(l) = r else { panic!() };
        assert_eq!((l.description, l.avatar), (None, None));
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
    fn list_name_nul_stripped() {
        let r = parse_record(&did(A), Collection::List, &json!({"name": "a\u{0}b"})).unwrap();
        let Record::List(l) = r else { panic!() };
        assert_eq!(l.name.as_deref(), Some("ab"));
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
        let commit = json!({"rev": "3l3qo2vutsw2b", "operation": "create",
            "collection": "app.bsky.graph.block", "rkey": "3l3qo2vuowo2b",
            "record": {"$type": "app.bsky.graph.block", "subject": B,
                       "createdAt": "2024-09-09T19:46:02.102Z"},
            "cid": "bafyreidc6sydkkbchcyg62v77wbhzvb2mvytlmsychqgwf2xojjtirmzj4"});
        let op = parse_commit(A, &commit).unwrap();
        assert_eq!(op.rev, Tid::parse("3l3qo2vutsw2b").unwrap());
        assert!(matches!(
            op.action,
            CommitAction::Upsert {
                op: Operation::Create,
                record: Record::Block(_)
            }
        ));

        let del = json!({"rev": "3l3qo2vutsw2c", "operation": "delete",
            "collection": "app.bsky.graph.listitem", "rkey": "3l3qo2vuowo2b"});
        assert_eq!(parse_commit(A, &del).unwrap().action, CommitAction::Delete);
    }

    #[test]
    fn jetstream_rejections() {
        let bad_rev = json!({"rev": "not-a-tid", "operation": "delete",
            "collection": "app.bsky.graph.block", "rkey": "3l3qo2vuowo2b"});
        assert!(matches!(
            parse_commit(A, &bad_rev),
            Err(RecordError::Rev(_))
        ));
        let foreign = json!({"rev": "3l3qo2vutsw2b", "operation": "create",
            "collection": "app.bsky.graph.listitem", "rkey": "3l3qo2vuowo2b",
            "record": {"subject": B,
                       "list": format!("at://{A}/app.bsky.graph.list/3k2abc")}});
        assert!(matches!(
            parse_commit(B, &foreign),
            Err(RecordError::ForeignListItem { .. })
        ));
        let delete = json!({"rev": "3l3qo2vutsw2b", "operation": "delete",
            "collection": "app.bsky.graph.block", "rkey": "3l3qo2vuowo2b"});
        assert!(matches!(
            parse_commit("alice.test", &delete),
            Err(RecordError::RepoDid(_))
        ));
        let other = json!({"rev": "3l3qo2vutsw2b", "operation": "create",
            "collection": "app.bsky.feed.post", "rkey": "x", "record": {}});
        assert!(matches!(
            parse_commit(A, &other),
            Err(RecordError::Field("collection"))
        ));
    }
}
