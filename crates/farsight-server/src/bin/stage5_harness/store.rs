//! Section 0: the history write path (see `docs/design/history.md`),
//! driven through the real apply path and janitor against a database of
//! its own. Every assertion reads the rows the writers stored.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use farsight_core::record::{BlockRecord, ListBlockRecord, ListItemRecord};
use farsight_core::{AtUri, Collection, Did, Record, RecordKey};
use farsight_storage::apply::{self, ApplyCtx, Batch, Origin, Reconcile, Write, WriteAction};
use farsight_storage::counters::CounterSink;
use farsight_storage::ids::Stamp;
use farsight_storage::keys::Limits;
use farsight_storage::txn::Gates;
use farsight_storage::{history, janitor};
use sqlx::PgPool;

use crate::seed::{self, did};
use crate::support::{Checks, Pg};

type Seen = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);
type HistRow = (
    i16,
    Option<i64>,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

struct Env {
    pool: PgPool,
    limits: Limits,
    counters: CounterSink,
}

fn d(s: &str) -> Did {
    Did::parse(s).expect("did")
}

fn rk(s: &str) -> RecordKey {
    RecordKey::parse(s).expect("rkey")
}

/// A witness time `secs` after a fixed base, on whole microseconds.
fn at(base: DateTime<Utc>, secs: i64) -> DateTime<Utc> {
    base + ChronoDuration::seconds(secs)
}

fn block(author: &str, rkey: &str, subject: &str, stamp: i64, w: DateTime<Utc>) -> Write {
    Write {
        author: d(author),
        collection: Collection::Block,
        rkey: rk(rkey),
        stamp: Stamp::new(stamp),
        witness: Some(w),
        action: WriteAction::Upsert(Record::Block(BlockRecord {
            subject: d(subject),
            created_at: Some(w),
        })),
    }
}

fn listblock(
    author: &str,
    rkey: &str,
    owner: &str,
    list: &str,
    stamp: i64,
    w: DateTime<Utc>,
) -> Write {
    Write {
        author: d(author),
        collection: Collection::ListBlock,
        rkey: rk(rkey),
        stamp: Stamp::new(stamp),
        witness: Some(w),
        action: WriteAction::Upsert(Record::ListBlock(ListBlockRecord {
            subject: AtUri::new(d(owner), Collection::List, rk(list)),
            created_at: Some(w),
        })),
    }
}

fn item(owner: &str, rkey: &str, list: &str, subject: &str, stamp: i64, w: DateTime<Utc>) -> Write {
    Write {
        author: d(owner),
        collection: Collection::ListItem,
        rkey: rk(rkey),
        stamp: Stamp::new(stamp),
        witness: Some(w),
        action: WriteAction::Upsert(Record::ListItem(ListItemRecord {
            subject: d(subject),
            list: AtUri::new(d(owner), Collection::List, rk(list)),
            created_at: Some(w),
        })),
    }
}

fn delete(author: &str, c: Collection, rkey: &str, stamp: i64, w: DateTime<Utc>) -> Write {
    Write {
        author: d(author),
        collection: c,
        rkey: rk(rkey),
        stamp: Stamp::new(stamp),
        witness: Some(w),
        action: WriteAction::Delete,
    }
}

impl Env {
    fn ctx(&self) -> ApplyCtx<'_> {
        ApplyCtx {
            limits: &self.limits,
            gates: Gates::default(),
            counters: &self.counters,
        }
    }

    async fn firehose(&self, writes: Vec<Write>) -> Result<(), String> {
        let mut b = Batch::new(Origin::Firehose);
        b.writes = writes;
        apply::apply(&self.pool, &self.ctx(), &b)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// A whole-collection reconcile of `author` at listing stamp `stamp`
    /// keeping `keep`.
    async fn reconcile(
        &self,
        author: &str,
        c: Collection,
        stamp: i64,
        keep: &[&str],
    ) -> Result<(), String> {
        let read_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        let mut b = Batch::new(Origin::Listing {
            stamp_read_at: read_at,
            deletes_only: false,
        });
        b.reconciles = vec![Reconcile {
            author: d(author),
            collection: c,
            stamp: Stamp::new(stamp),
            after: None,
            through: None,
            keep: keep.iter().map(|k| rk(k)).collect(),
        }];
        apply::apply(&self.pool, &self.ctx(), &b)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn id(&self, did: &str) -> Result<i64, String> {
        seed::actor(&self.pool, did).await
    }

    async fn n(&self, sql: &str) -> Result<i64, String> {
        sqlx::query_scalar(sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("{e}: {sql}"))
    }

    async fn seen(
        &self,
        table: &str,
        col: &str,
        id: i64,
        rkey: &str,
    ) -> Result<Option<Seen>, String> {
        sqlx::query_as(&format!(
            "SELECT first_seen, last_seen FROM {table} WHERE {col} = $1 AND rkey = $2"
        ))
        .bind(id)
        .bind(rkey)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())
    }

    /// `cause, removed_rev, removed_at, first_seen, last_seen` of the
    /// history rows of (`col` = `id`, `rkey`), oldest first.
    async fn hist(
        &self,
        table: &str,
        col: &str,
        id: i64,
        rkey: &str,
    ) -> Result<Vec<HistRow>, String> {
        sqlx::query_as(&format!(
            "SELECT cause, removed_rev, removed_at, first_seen, last_seen FROM {table}
             WHERE {col} = $1 AND rkey = $2 ORDER BY id"
        ))
        .bind(id)
        .bind(rkey)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())
    }
}

/// Runs the section.
pub async fn run(c: &mut Checks, pg: &Pg) -> Result<(), String> {
    c.section("0. history write path (apply, janitor)");
    pg.create_db("stage5_store").await?;
    let pool = pg.pool("stage5_store", 4).await?;
    farsight_storage::migrate(&pool)
        .await
        .map_err(|e| e.to_string())?;
    let e = Env {
        pool: pool.clone(),
        limits: Limits::defaults(),
        counters: CounterSink::new(9),
    };
    // Microsecond precision, as Postgres stores it.
    let base: DateTime<Utc> =
        sqlx::query_scalar("SELECT date_trunc('second', now()) - interval '1 hour'")
            .fetch_one(&pool)
            .await
            .map_err(|e| e.to_string())?;
    let (a, s, s2) = (did("hwa", 1), did("hws", 1), did("hws", 2));

    // ---- witness bounds
    e.firehose(vec![block(&a, "3kb1", &s, 10, at(base, 1))])
        .await?;
    let aid = e.id(&a).await?;
    let sid = e.id(&s).await?;
    c.check(
        "insert stamps first_seen = last_seen = the event's witness",
        e.seen("blocks", "author_id", aid, "3kb1").await?
            == Some((Some(at(base, 1)), Some(at(base, 1)))),
        format!("{:?}", e.seen("blocks", "author_id", aid, "3kb1").await?),
    );
    e.firehose(vec![block(&a, "3kb1", &s, 11, at(base, 5))])
        .await?;
    c.check(
        "a winning same-target update advances last_seen only",
        e.seen("blocks", "author_id", aid, "3kb1").await?
            == Some((Some(at(base, 1)), Some(at(base, 5)))),
        format!("{:?}", e.seen("blocks", "author_id", aid, "3kb1").await?),
    );
    e.firehose(vec![block(&a, "3kb1", &s, 11, at(base, 9))])
        .await?;
    c.check(
        "an upsert that loses LWW (equal stamp) writes nothing: last_seen unchanged",
        e.seen("blocks", "author_id", aid, "3kb1").await?
            == Some((Some(at(base, 1)), Some(at(base, 5)))),
        format!("{:?}", e.seen("blocks", "author_id", aid, "3kb1").await?),
    );
    c.check(
        "re-affirming a live row never writes history",
        e.n("SELECT count(*) FROM blocks_history").await? == 0,
        "0 rows",
    );

    // ---- delete
    e.firehose(vec![delete(
        &a,
        Collection::Block,
        "3kb1",
        20,
        at(base, 30),
    )])
    .await?;
    let h = e.hist("blocks_history", "author_id", aid, "3kb1").await?;
    c.check(
        "firehose delete ⇒ one blocks_history row: cause delete, the delete's rev, removed_at = its witness, bounds copied",
        h == vec![(1, Some(20), at(base, 30), Some(at(base, 1)), Some(at(base, 5)))],
        format!("{h:?}"),
    );
    c.check(
        "the history row carries the removed block's subject",
        e.n(&format!(
            "SELECT count(*) FROM blocks_history WHERE author_id = {aid} AND subject_id = {sid}"
        ))
        .await?
            == 1,
        "subject kept",
    );
    e.firehose(vec![delete(
        &a,
        Collection::Block,
        "3kb1",
        21,
        at(base, 31),
    )])
    .await?;
    c.check(
        "a delete that finds no row writes no history",
        e.n("SELECT count(*) FROM blocks_history").await? == 1,
        "still 1 row",
    );

    // ---- removed_at = GREATEST(w, last_seen)
    e.firehose(vec![block(&a, "3kb2", &s, 30, at(base, 100))])
        .await?;
    e.firehose(vec![delete(
        &a,
        Collection::Block,
        "3kb2",
        31,
        at(base, 40),
    )])
    .await?;
    let h = e.hist("blocks_history", "author_id", aid, "3kb2").await?;
    c.check(
        "removed_at is never before last_seen (a replayed delete witnessed earlier)",
        h.len() == 1 && h[0].2 == at(base, 100),
        format!("{h:?}"),
    );

    // ---- subject change
    e.firehose(vec![block(&a, "3kb3", &s, 40, at(base, 50))])
        .await?;
    e.firehose(vec![block(&a, "3kb3", &s2, 41, at(base, 60))])
        .await?;
    let h = e.hist("blocks_history", "author_id", aid, "3kb3").await?;
    c.check(
        "an update naming another subject records the old target (cause subject_change, the update's rev)",
        h == vec![(2, Some(41), at(base, 60), Some(at(base, 50)), Some(at(base, 50)))]
            && e.n(&format!("SELECT count(*) FROM blocks_history WHERE rkey = '3kb3' AND subject_id = {sid}")).await? == 1,
        format!("{h:?}"),
    );
    c.check(
        "…and the live row's bounds start again",
        e.seen("blocks", "author_id", aid, "3kb3").await?
            == Some((Some(at(base, 60)), Some(at(base, 60)))),
        format!("{:?}", e.seen("blocks", "author_id", aid, "3kb3").await?),
    );

    // ---- reconcile
    let before = e.n("SELECT count(*) FROM blocks_history").await?;
    e.firehose(vec![
        block(&a, "3kb4", &s, 50, at(base, 70)),
        block(&a, "3kb5", &s, 51, at(base, 71)),
    ])
    .await?;
    // No firehose batch has run with progress: the clock is undefined, so
    // the listing's witness is the database's now().
    e.reconcile(&a, Collection::Block, 1_000, &["3kb3", "3kb5"])
        .await?;
    let h = e.hist("blocks_history", "author_id", aid, "3kb4").await?;
    let recent: bool = h.len() == 1 && (Utc::now() - h[0].2).num_seconds().abs() < 120;
    c.check(
        "reconcile ⇒ cause reconcile, no rev, removed_at on the listing clock (database now() while the clock is undefined)",
        h.len() == 1 && h[0].0 == 4 && h[0].1.is_none() && recent,
        format!("{h:?}"),
    );
    c.check(
        "reconcile records exactly the rows it deleted",
        e.n("SELECT count(*) FROM blocks_history").await? == before + 1
            && e.n(&format!(
                "SELECT count(*) FROM blocks WHERE author_id = {aid}"
            ))
            .await?
                == 2,
        "kept rows untouched",
    );
    // With a defined clock, a listing removal is dated on it.
    sqlx::query(
        "INSERT INTO firehose_state (id, source_url, protocol, applied_through, first_applied_at, connected)
         VALUES (1, 'ws://harness', 2, $1, $1, true)",
    )
    .bind(at(base, 500))
    .execute(&pool)
    .await
    .map_err(|e| e.to_string())?;
    e.reconcile(&a, Collection::Block, 2_000, &["3kb3"]).await?;
    let h = e.hist("blocks_history", "author_id", aid, "3kb5").await?;
    c.check(
        "with the clock defined, a reconcile removal is dated at clock() of the transaction's start",
        h.len() == 1 && h[0].2 == at(base, 500) && h[0].1.is_none(),
        format!("{h:?}"),
    );

    // ---- listblocks
    let (o, lb) = (did("hwo", 1), did("hwl", 1));
    e.firehose(vec![listblock(&lb, "3kl1", &o, "mod", 60, at(base, 80))])
        .await?;
    let lbid = e.id(&lb).await?;
    let oid = e.id(&o).await?;
    e.firehose(vec![delete(
        &lb,
        Collection::ListBlock,
        "3kl1",
        61,
        at(base, 90),
    )])
    .await?;
    let target: Option<(i64, String, i16, Option<i64>)> = sqlx::query_as(
        "SELECT list_owner_id, list_rkey, cause, removed_rev FROM list_blocks_history WHERE author_id = $1",
    )
    .bind(lbid)
    .fetch_optional(&pool)
    .await
    .map_err(|e| e.to_string())?;
    c.check(
        "listblock delete ⇒ list_blocks_history row naming the list by owner and rkey",
        target == Some((oid, "mod".to_owned(), 1, Some(61))),
        format!("{target:?}"),
    );
    // Subject change of a listblock: delete + insert, the old list recorded.
    e.firehose(vec![listblock(&lb, "3kl2", &o, "mod", 70, at(base, 91))])
        .await?;
    e.firehose(vec![listblock(&lb, "3kl2", &o, "other", 71, at(base, 92))])
        .await?;
    let t: Vec<(String, i16)> = sqlx::query_as(
        "SELECT list_rkey, cause FROM list_blocks_history WHERE author_id = $1 AND rkey = '3kl2'",
    )
    .bind(lbid)
    .fetch_all(&pool)
    .await
    .map_err(|e| e.to_string())?;
    c.check(
        "listblock update naming another list ⇒ the old list recorded with cause subject_change",
        t == vec![("mod".to_owned(), 2)],
        format!("{t:?}"),
    );

    // ---- listitems: recorded only while the list is tracked or deleted
    let member = did("hwm", 1);
    let mid = e.id(&member).await?;
    let ready = seed::list(&pool, oid, "tracked", 2, 1, 1, false, Some(60)).await?;
    e.firehose(vec![item(
        &o,
        "3ki1",
        "tracked",
        &member,
        80,
        at(base, 110),
    )])
    .await?;
    e.firehose(vec![delete(
        &o,
        Collection::ListItem,
        "3ki1",
        81,
        at(base, 120),
    )])
    .await?;
    let t: Vec<(String, i64, i16, Option<i64>)> = sqlx::query_as(
        "SELECT list_rkey, subject_id, cause, removed_rev FROM list_items_history WHERE owner_id = $1",
    )
    .bind(oid)
    .fetch_all(&pool)
    .await
    .map_err(|e| e.to_string())?;
    c.check(
        "listitem delete on a tracked list ⇒ list_items_history row (list rkey, subject, cause delete)",
        t == vec![("tracked".to_owned(), mid, 1, Some(81))],
        format!("{t:?}"),
    );
    // A drain of a list that merely stopped being tracked records nothing.
    for (i, rkey) in ["3kd1", "3kd2", "3kd3"].iter().enumerate() {
        e.firehose(vec![item(
            &o,
            rkey,
            "tracked",
            &did("hwm", 10 + i as u64),
            90 + i as i64,
            at(base, 130),
        )])
        .await?;
    }
    sqlx::query(
        "UPDATE lists SET track_state = 5, purge_then = 0, listblock_count = 0 WHERE id = $1",
    )
    .bind(ready)
    .execute(&pool)
    .await
    .map_err(|e| e.to_string())?;
    let before = e.n("SELECT count(*) FROM list_items_history").await?;
    janitor::process_purges(&pool, &e.limits, &e.counters, 100)
        .await
        .map_err(|e| e.to_string())?;
    c.check(
        "a purge drain of a list that only stopped being tracked deletes its items and records nothing",
        e.n(&format!("SELECT count(*) FROM list_items WHERE list_id = {ready}")).await? == 0
            && e.n("SELECT count(*) FROM list_items_history").await? == before,
        format!("{before} history rows before and after"),
    );
    // A drain of a deleted list records its members as list_deleted.
    let gone = seed::list(&pool, oid, "deleted", 2, 1, 1, false, Some(60)).await?;
    for (i, rkey) in ["3kg1", "3kg2"].iter().enumerate() {
        e.firehose(vec![item(
            &o,
            rkey,
            "deleted",
            &did("hwm", 20 + i as u64),
            100 + i as i64,
            at(base, 140),
        )])
        .await?;
    }
    sqlx::query("UPDATE lists SET track_state = 5, purge_then = 7, record_state = 2 WHERE id = $1")
        .bind(gone)
        .execute(&pool)
        .await
        .map_err(|e| e.to_string())?;
    janitor::process_purges(&pool, &e.limits, &e.counters, 100)
        .await
        .map_err(|e| e.to_string())?;
    let t: Vec<(String, i16, Option<i64>)> = sqlx::query_as(
        "SELECT rkey, cause, removed_rev FROM list_items_history WHERE owner_id = $1 AND list_rkey = 'deleted' ORDER BY rkey",
    )
    .bind(oid)
    .fetch_all(&pool)
    .await
    .map_err(|e| e.to_string())?;
    c.check(
        "a purge drain of a list whose record is deleted records each member (cause list_deleted, no rev)",
        t == vec![("3kg1".to_owned(), 5, None), ("3kg2".to_owned(), 5, None)],
        format!("{t:?}"),
    );

    // ---- rate
    let tight = {
        let mut l = Limits::defaults();
        l.cfg.history_per_did_per_day = 2;
        l.cfg.history_per_bucket_per_day = 2;
        l
    };
    let r = Env {
        pool: pool.clone(),
        limits: tight,
        counters: CounterSink::new(9),
    };
    let ra = did("hwr", 1);
    for i in 0..4i64 {
        r.firehose(vec![block(
            &ra,
            &format!("3kr{i}"),
            &s,
            200 + i,
            at(base, 150),
        )])
        .await?;
    }
    for i in 0..4i64 {
        r.firehose(vec![delete(
            &ra,
            Collection::Block,
            &format!("3kr{i}"),
            300 + i,
            at(base, 160),
        )])
        .await?;
    }
    let raid = e.id(&ra).await?;
    let (rows, live, charged) = (
        e.n(&format!(
            "SELECT count(*) FROM blocks_history WHERE author_id = {raid}"
        ))
        .await?,
        e.n(&format!(
            "SELECT count(*) FROM blocks WHERE author_id = {raid}"
        ))
        .await?,
        e.n("SELECT COALESCE(max(n), 0) FROM history_rate WHERE key LIKE '%hwr%'")
            .await?,
    );
    c.check(
        "over the daily history rate the removal is applied and not recorded",
        rows == 2 && live == 0 && charged == 2,
        format!("{rows} history rows, {live} live rows, {charged} charged"),
    );

    // ---- disabled
    let off = {
        let mut l = Limits::defaults();
        l.history_enabled = false;
        l
    };
    let dis = Env {
        pool: pool.clone(),
        limits: off,
        counters: CounterSink::new(9),
    };
    let da = did("hwd", 1);
    dis.firehose(vec![block(&da, "3kx1", &s, 400, at(base, 170))])
        .await?;
    let daid = e.id(&da).await?;
    let stamped = e.seen("blocks", "author_id", daid, "3kx1").await?;
    dis.firehose(vec![delete(
        &da,
        Collection::Block,
        "3kx1",
        401,
        at(base, 180),
    )])
    .await?;
    c.check(
        "block_history_enabled = false: bounds are still stamped, no history row is written",
        stamped == Some((Some(at(base, 170)), Some(at(base, 170))))
            && e.n(&format!(
                "SELECT count(*) FROM blocks_history WHERE author_id = {daid}"
            ))
            .await?
                == 0,
        format!("{stamped:?}"),
    );

    // ---- account purge
    let authored = e
        .n(&format!(
            "SELECT count(*) FROM blocks_history WHERE author_id = {aid}"
        ))
        .await?;
    // `a` is also the subject of a removed block by another author.
    let other = did("hwa", 2);
    e.firehose(vec![block(&other, "3ko1", &a, 500, at(base, 190))])
        .await?;
    e.firehose(vec![delete(
        &other,
        Collection::Block,
        "3ko1",
        501,
        at(base, 200),
    )])
    .await?;
    let others_sql = format!("SELECT count(*) FROM blocks_history WHERE author_id <> {aid}");
    let others_before = e.n(&others_sql).await?;
    sqlx::query("UPDATE actors SET status = 4 WHERE id = $1")
        .bind(aid)
        .execute(&pool)
        .await
        .map_err(|e| e.to_string())?;
    let pending_before = janitor::accounts_pending_purge(&pool, 100)
        .await
        .map_err(|e| e.to_string())?;
    janitor::purge_account(&pool, &e.limits, &e.counters, &d(&a))
        .await
        .map_err(|e| e.to_string())?;
    let (left, as_subject, live, others_after) = (
        e.n(&format!(
            "SELECT count(*) FROM blocks_history WHERE author_id = {aid}"
        ))
        .await?,
        e.n(&format!(
            "SELECT count(*) FROM blocks_history WHERE subject_id = {aid}"
        ))
        .await?,
        e.n(&format!(
            "SELECT count(*) FROM blocks WHERE author_id = {aid}"
        ))
        .await?,
        e.n(&others_sql).await?,
    );
    c.check(
        "account purge writes no history and deletes the history the account authored; rows naming it as subject, and other authors' rows, stay",
        authored > 0 && left == 0 && as_subject == 1 && live == 0 && others_after == others_before,
        format!(
            "authored {authored} → {left}; as subject {as_subject}; live {live}; others {others_before} → {others_after}"
        ),
    );
    c.check(
        "a deleted account with authored history counts as pending purge until it is gone",
        pending_before.iter().any(|x| x.as_str() == a)
            && !janitor::accounts_pending_purge(&pool, 100)
                .await
                .map_err(|e| e.to_string())?
                .iter()
                .any(|x| x.as_str() == a),
        format!("{} pending before", pending_before.len()),
    );

    // ---- retention
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO blocks_history (author_id, rkey, subject_id, removed_at, cause)
         SELECT $1, 'old' || g, $2, now() - interval '400 days', 1 FROM generate_series(1, 25) g",
    )
    .bind(lbid)
    .bind(sid)
    .execute(&pool)
    .await
    .map_err(|e| e.to_string())?;
    let total = e.n("SELECT count(*) FROM blocks_history").await?;
    let zero = history::prune(&pool, now, std::time::Duration::ZERO)
        .await
        .map_err(|e| e.to_string())?;
    c.check(
        "retention \"0s\": the pass deletes nothing",
        zero == history::PruneReport::default()
            && e.n("SELECT count(*) FROM blocks_history").await? == total,
        format!("{zero:?}"),
    );
    // The old rows have the highest ids: a walk from the lowest id stops
    // at the first batch without an expired row only if that batch is
    // full, so these are still found.
    let p = history::prune(&pool, now, std::time::Duration::from_secs(365 * 86_400))
        .await
        .map_err(|e| e.to_string())?;
    c.check(
        "retention 365d: rows removed earlier are deleted, later ones kept",
        p.rows[0] == 25 && e.n("SELECT count(*) FROM blocks_history").await? == total - 25,
        format!("{p:?}"),
    );

    // ---- recording windows
    let open = |pool: PgPool| async move {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM history_windows WHERE to_at IS NULL")
            .fetch_one(&pool)
            .await
            .map_err(|e| e.to_string())
    };
    history::sync_window(&pool, true)
        .await
        .map_err(|e| e.to_string())?;
    history::sync_window(&pool, true)
        .await
        .map_err(|e| e.to_string())?;
    let one = open(pool.clone()).await?;
    history::sync_window(&pool, false)
        .await
        .map_err(|e| e.to_string())?;
    let none = open(pool.clone()).await?;
    history::sync_window(&pool, true)
        .await
        .map_err(|e| e.to_string())?;
    let (again, all) = (
        open(pool.clone()).await?,
        e.n("SELECT count(*) FROM history_windows").await?,
    );
    c.check(
        "recording windows: start with history on opens one (once), off closes it, on again opens a new one",
        one == 1 && none == 0 && again == 1 && all == 2,
        format!("open {one} → {none} → {again}; {all} windows"),
    );
    pool.close().await;
    Ok(())
}
