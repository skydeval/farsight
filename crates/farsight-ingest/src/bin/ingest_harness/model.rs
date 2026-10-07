//! An independent model of what storage must hold after a stream of
//! commits: the LWW rule replayed in plain Rust over every event the
//! ingest received (via the tap), then compared with the real rows.
//!
//! Scope: blocks, listblocks and list records — the three collections
//! whose storage does not depend on list tracking. Keys whose author was
//! purged (account deleted) are excluded, since the purge removes rows by
//! design.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use farsight_core::record::{CommitAction, CommitOp, Record};
use farsight_core::{Collection, Did};
use farsight_ingest::frame::{Body, InEvent};
use sqlx::PgPool;

/// One key's modelled state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyState {
    /// Rev of the stored row, if present.
    pub row: Option<(i64, String)>,
    /// Tombstone rev, if any.
    pub tomb: Option<i64>,
}

/// `(collection code, author, rkey)`.
pub type Key = (i16, String, String);

fn target(r: &Record) -> Option<String> {
    match r {
        Record::Block(b) => Some(b.subject.to_string()),
        Record::ListBlock(lb) => Some(format!("{}/{}", lb.subject.authority, lb.subject.rkey)),
        Record::List(_) => Some(String::new()),
        Record::ListItem(_) => None,
    }
}

/// Folds commits with the LWW rule.
#[derive(Debug, Default)]
pub struct Model {
    pub keys: HashMap<Key, KeyState>,
    pub commits: u64,
}

impl Model {
    pub fn apply(&mut self, op: &CommitOp) {
        if op.collection == Collection::ListItem {
            return;
        }
        self.commits += 1;
        let key = (
            op.collection.code(),
            op.author.to_string(),
            op.rkey.to_string(),
        );
        let w = op.rev.as_i64();
        let st = self.keys.entry(key).or_insert(KeyState {
            row: None,
            tomb: None,
        });
        match &op.action {
            CommitAction::Delete => {
                if st.row.as_ref().is_some_and(|(r, _)| *r < w) {
                    st.row = None;
                }
                st.tomb = Some(st.tomb.map_or(w, |t| t.max(w)));
            }
            CommitAction::Upsert { record, .. } => {
                let wins =
                    st.row.as_ref().is_none_or(|(r, _)| w > *r) && st.tomb.is_none_or(|t| w > t);
                if wins && let Some(t) = target(record) {
                    st.row = Some((w, t));
                }
            }
        }
    }

    pub fn apply_events(&mut self, events: &[InEvent]) {
        for ev in events {
            match &ev.body {
                Body::Commit(op) => self.apply(op),
                // A create or update whose record was rejected stands for
                // the delete of the version under its key.
                Body::Rejected {
                    removes: Some(op), ..
                } => self.apply(op),
                _ => {}
            }
        }
    }
}

/// Mismatches between the model and the database.
#[derive(Debug, Default)]
pub struct Diff {
    pub compared: usize,
    pub excluded_purged: usize,
    pub mismatches: Vec<String>,
}

/// Compares every modelled key with the stored row.
pub async fn compare(pool: &PgPool, model: &Model) -> Result<Diff, sqlx::Error> {
    let deleted: BTreeSet<String> =
        sqlx::query_scalar::<_, String>("SELECT did FROM actors WHERE status = 4")
            .fetch_all(pool)
            .await?
            .into_iter()
            .collect();
    // Load stored rows for the authors in the model, per collection.
    let mut authors: BTreeMap<i16, BTreeSet<&str>> = BTreeMap::new();
    for (c, a, _) in model.keys.keys() {
        authors.entry(*c).or_default().insert(a.as_str());
    }
    let mut stored: HashMap<Key, (i64, String)> = HashMap::new();
    for (c, set) in &authors {
        let dids: Vec<String> = set.iter().map(|s| (*s).to_owned()).collect();
        let sql = match *c {
            1 => {
                "SELECT a.did, b.rkey, b.rev, s.did FROM blocks b JOIN actors a ON a.id = b.author_id
                 JOIN actors s ON s.id = b.subject_id WHERE a.did = ANY($1)"
            }
            2 => {
                "SELECT a.did, b.rkey, b.rev, o.did || '/' || l.rkey FROM list_blocks b
                 JOIN actors a ON a.id = b.author_id JOIN lists l ON l.id = b.list_id
                 JOIN actors o ON o.id = l.owner_id WHERE a.did = ANY($1)"
            }
            3 => {
                "SELECT a.did, l.rkey, l.rev, '' FROM lists l JOIN actors a ON a.id = l.owner_id
                 WHERE a.did = ANY($1) AND l.record_state = 1"
            }
            _ => continue,
        };
        let rows: Vec<(String, String, Option<i64>, String)> =
            sqlx::query_as(sql).bind(&dids).fetch_all(pool).await?;
        for (did, rkey, rev, t) in rows {
            stored.insert((*c, did, rkey), (rev.unwrap_or(-1), t));
        }
    }
    let mut diff = Diff::default();
    for (key, st) in &model.keys {
        if deleted.contains(&key.1) {
            diff.excluded_purged += 1;
            continue;
        }
        diff.compared += 1;
        let got = stored.get(key);
        let ok = match (&st.row, got) {
            (None, None) => true,
            (Some((rev, t)), Some((grev, gt))) => rev == grev && t == gt,
            _ => false,
        };
        if !ok && diff.mismatches.len() < 20 {
            diff.mismatches
                .push(format!("{:?} model {:?} stored {:?}", key, st.row, got));
        } else if !ok {
            diff.mismatches.push(String::new());
        }
    }
    Ok(diff)
}

/// Identity of an event across two connections to the same instance.
pub fn event_key(ev: &InEvent) -> Option<String> {
    match &ev.body {
        Body::Commit(op) => Some(format!(
            "c|{}|{}|{}|{}",
            op.author,
            op.collection.code(),
            op.rkey,
            op.rev.as_i64()
        )),
        Body::Account { did, active, .. } => Some(format!("a|{did}|{}|{active}", ev.witness_us)),
        Body::Identity(did) => Some(format!("i|{did}|{}", ev.witness_us)),
        _ => None,
    }
}

/// A DID that cannot exist on the network (for injected events).
pub fn synthetic_did(n: u64) -> Did {
    let mut tail = String::new();
    let mut v = n;
    for _ in 0..13 {
        tail.push(b"abcdefghijklmnopqrstuvwxyz234567"[(v % 32) as usize] as char);
        v /= 32;
    }
    Did::parse(&format!("did:plc:harnesszzzz{tail}")).expect("valid synthetic did")
}
