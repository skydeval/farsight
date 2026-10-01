//! Seeding real stored state for Mode A. Synthetic DIDs use a 3-letter
//! prefix and a number spelled in base32 letters, so they are valid
//! `did:plc` identifiers that never collide with the live network.

use sqlx::PgPool;

/// A synthetic `did:plc` (24 base32 characters after the method).
pub fn did(prefix: &str, n: u64) -> String {
    assert_eq!(prefix.len(), 3);
    let digits: String = format!("{n:021}")
        .chars()
        .map(|c| (b'a' + (c as u8 - b'0')) as char)
        .collect();
    format!("did:plc:{prefix}{digits}")
}

/// SQL expression producing [`did`] for `g` (a generate_series column).
fn did_sql(prefix: &str, col: &str) -> String {
    format!(
        "'did:plc:{prefix}' || translate(lpad({col}::text, 21, '0'), '0123456789', 'abcdefghij')"
    )
}

pub async fn exec(pool: &PgPool, sql: &str) -> Result<u64, String> {
    sqlx::query(sql)
        .execute(pool)
        .await
        .map(|r| r.rows_affected())
        .map_err(|e| format!("{e}: {sql}"))
}

pub async fn notify(pool: &PgPool) -> Result<(), String> {
    exec(pool, "SELECT pg_notify('farsight_coverage', '')").await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    Ok(())
}

/// Interns `did` and returns its id.
pub async fn actor(pool: &PgPool, did: &str) -> Result<i64, String> {
    sqlx::query_scalar(
        "INSERT INTO actors (did) VALUES ($1)
         ON CONFLICT (did) DO UPDATE SET did = EXCLUDED.did RETURNING id",
    )
    .bind(did)
    .fetch_one(pool)
    .await
    .map_err(|e| e.to_string())
}

/// A `lists` row; returns its id.
#[allow(clippy::too_many_arguments)]
pub async fn list(
    pool: &PgPool,
    owner_id: i64,
    rkey: &str,
    track_state: i16,
    record_state: i16,
    listblock_count: i32,
    capped: bool,
    fetched_ago_secs: Option<i64>,
) -> Result<i64, String> {
    sqlx::query_scalar(
        "INSERT INTO lists (owner_id, rkey, record_state, purpose, name, listblock_count,
                            track_state, capped, admitted_at, admit_epoch, phase1_epoch,
                            fetched_at, fetched_witness, deferred_by, purge_then)
         VALUES ($1, $2, $3, 1, $2, $4, $5, $6, now() - interval '1 hour', 1, 1,
                 CASE WHEN $7::bigint IS NULL THEN NULL ELSE now() - make_interval(secs => $7) END,
                 CASE WHEN $7::bigint IS NULL THEN NULL ELSE now() - make_interval(secs => $7) END,
                 CASE WHEN $5 = 8 THEN 1 ELSE NULL END, NULL)
         ON CONFLICT (owner_id, rkey) DO UPDATE SET track_state = EXCLUDED.track_state,
           record_state = EXCLUDED.record_state, listblock_count = EXCLUDED.listblock_count,
           capped = EXCLUDED.capped, fetched_witness = EXCLUDED.fetched_witness
         RETURNING id",
    )
    .bind(owner_id)
    .bind(rkey)
    .bind(record_state)
    .bind(listblock_count)
    .bind(track_state)
    .bind(capped)
    .bind(fetched_ago_secs)
    .fetch_one(pool)
    .await
    .map_err(|e| e.to_string())
}

pub async fn block(pool: &PgPool, author: i64, rkey: &str, subject: i64) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev)
         VALUES ($1, $2, $3, now(), 1) ON CONFLICT DO NOTHING",
    )
    .bind(author)
    .bind(rkey)
    .bind(subject)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

pub async fn listblock(
    pool: &PgPool,
    author: i64,
    rkey: &str,
    list: i64,
    witnessed_ago_secs: Option<i64>,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO list_blocks (author_id, rkey, list_id, counted, witnessed_at, created_at, rev)
         VALUES ($1, $2, $3, true,
                 CASE WHEN $4::bigint IS NULL THEN NULL ELSE now() - make_interval(secs => $4) END,
                 now(), 1)
         ON CONFLICT (author_id, rkey) DO UPDATE SET list_id = EXCLUDED.list_id,
           witnessed_at = EXCLUDED.witnessed_at",
    )
    .bind(author)
    .bind(rkey)
    .bind(list)
    .bind(witnessed_ago_secs)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

pub async fn item(
    pool: &PgPool,
    owner: i64,
    rkey: &str,
    list: i64,
    subject: i64,
) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO list_items (owner_id, rkey, list_id, subject_id, created_at, rev)
         VALUES ($1, $2, $3, $4, now(), 1) ON CONFLICT DO NOTHING",
    )
    .bind(owner)
    .bind(rkey)
    .bind(list)
    .bind(subject)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

pub async fn debt(pool: &PgPool, actor: i64, reason: i16) -> Result<(), String> {
    sqlx::query(
        "INSERT INTO relist_debt (actor_id, reason, since_witness) VALUES ($1, $2, now())
         ON CONFLICT DO NOTHING",
    )
    .bind(actor)
    .bind(reason)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

/// The pagination dataset (Phase A): subject S blocked by 1200 accounts;
/// list `biglist` naming S with 1200 listblocks and 1200 more members; S
/// named by 1200 further ready lists.
pub struct Pagination {
    pub subject: String,
    pub subject_id: i64,
    pub big_list: String,
    pub big_list_id: i64,
}

pub const N: i64 = 1200;

pub async fn pagination(pool: &PgPool) -> Result<Pagination, String> {
    let subject = did("pgs", 1);
    let owner = did("pgo", 1);
    let s = actor(pool, &subject).await?;
    let o = actor(pool, &owner).await?;
    exec(
        pool,
        &format!(
            "INSERT INTO actors (did) SELECT {} FROM generate_series(1, {N}) g
             ON CONFLICT DO NOTHING",
            did_sql("pgb", "g")
        ),
    )
    .await?;
    exec(
        pool,
        &format!(
            "INSERT INTO actors (did) SELECT {} FROM generate_series(1, {N}) g
             ON CONFLICT DO NOTHING",
            did_sql("pgm", "g")
        ),
    )
    .await?;
    exec(
        pool,
        &format!(
            "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev)
             SELECT a.id, '3lpg' || lpad(g::text, 9, '0'), {s}, now(), 1
             FROM generate_series(1, {N}) g JOIN actors a ON a.did = {}
             ON CONFLICT DO NOTHING",
            did_sql("pgb", "g")
        ),
    )
    .await?;
    let big = list(pool, o, "biglist", 2, 1, N as i32, false, Some(3600)).await?;
    exec(
        pool,
        &format!(
            "INSERT INTO list_blocks (author_id, rkey, list_id, counted, created_at, rev)
             SELECT a.id, '3llb' || lpad(g::text, 9, '0'), {big}, true, now(), 1
             FROM generate_series(1, {N}) g JOIN actors a ON a.did = {}
             ON CONFLICT DO NOTHING",
            did_sql("pgb", "g")
        ),
    )
    .await?;
    exec(
        pool,
        &format!(
            "INSERT INTO list_items (owner_id, rkey, list_id, subject_id, created_at, rev)
             SELECT {o}, '3lmi' || lpad(g::text, 9, '0'), {big}, a.id, now(), 1
             FROM generate_series(1, {N}) g JOIN actors a ON a.did = {}
             ON CONFLICT DO NOTHING",
            did_sql("pgm", "g")
        ),
    )
    .await?;
    item(pool, o, "3lmisubject", big, s).await?;
    exec(
        pool,
        &format!(
            "INSERT INTO lists (owner_id, rkey, record_state, purpose, name, listblock_count,
                                track_state, admitted_at, fetched_at, fetched_witness)
             SELECT {o}, 'naming' || lpad(g::text, 5, '0'), 1, 2, 'naming ' || g, 0, 2,
                    now() - interval '1 hour', now() - interval '1 hour', now() - interval '1 hour'
             FROM generate_series(1, {N}) g
             ON CONFLICT DO NOTHING"
        ),
    )
    .await?;
    exec(
        pool,
        &format!(
            "INSERT INTO list_items (owner_id, rkey, list_id, subject_id, created_at, rev)
             SELECT {o}, '3lni' || lpad(g::text, 9, '0'), l.id, {s}, now(), 1
             FROM generate_series(1, {N}) g
             JOIN lists l ON l.owner_id = {o} AND l.rkey = 'naming' || lpad(g::text, 5, '0')
             ON CONFLICT DO NOTHING"
        ),
    )
    .await?;
    Ok(Pagination {
        subject,
        subject_id: s,
        big_list: format!("at://{owner}/app.bsky.graph.list/biglist"),
        big_list_id: big,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn dids_are_valid() {
        let d = super::did("pgb", 1200);
        assert!(farsight_core::Did::parse(&d).is_ok(), "{d}");
    }
}
