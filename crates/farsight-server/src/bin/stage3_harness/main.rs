//! `farsight-stage3-harness`: Phase B Mode A — automated API conformance
//! (stage-3 kickoff). Every assertion reads real responses from real
//! `farsight` processes and real stored rows; expectations that depend on
//! data are derived from SQL against the same database.
//!
//! Phases:
//! - **live**: a server with ingest on the public Jetstream, the pagination
//!   dataset, lexicon conformance, pagination, backfill rules, semaphore and
//!   timeout, rate limits and cache headers, client IP, metrics, and finally
//!   the admin reset.
//! - **coverage**: a server whose firehose URL is unreachable, so the
//!   harness alone writes the firehose state; scripted rows, debts and list
//!   states, and the §3.7 coverage rules on every read endpoint.
//! - **cloudflare**: a server reached from a container on a Docker network
//!   inside a Cloudflare range (the §9.3 dashboard warning).

mod seed;
mod support;

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use farsight_api::lexicon::Lexicons;
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::seed::did;
use crate::support::{Checks, Http, Pg, Resp, Server, csrf_of, enc, free_port, set_cookie};

const NS: &str = "app.nearhorizon.farsight";
const PASSWORD: &str = "harness-password-123";
const HOSTNAME: &str = "farsight.test";

fn x(base: &str, method: &str, query: &str) -> String {
    if query.is_empty() {
        format!("{base}/xrpc/{NS}.{method}")
    } else {
        format!("{base}/xrpc/{NS}.{method}?{query}")
    }
}

fn bearer(t: &str) -> String {
    format!("Bearer {t}")
}

struct Ctx {
    lex: Lexicons,
    http: Http,
    admin: String,
}

fn config_toml(
    dsn: &str,
    admin_token: &str,
    firehose: &str,
    reads: &str,
    metrics_bind: &str,
    extra: &str,
) -> String {
    let bcrypt = bcrypt::hash(PASSWORD, 4).expect("bcrypt");
    format!(
        r#"[server]
hostname = "{HOSTNAME}"
contact = "mailto:ops@{HOSTNAME}"

[storage]
database_url = "{dsn}"
budget_bytes = 70000000000

[firehose]
urls = ["{firehose}"]

[access]
reads = "{reads}"
ui = "public_read"

[auth]
admin_token_sha256 = "{}"
admin_password_bcrypt = "{bcrypt}"

[metrics]
bind = "{metrics_bind}"
{extra}
"#,
        farsight_api::auth::hex(&farsight_api::auth::sha256(admin_token))
    )
}

async fn validate(c: &mut Checks, ctx: &Ctx, method: &str, r: &Resp, expect: u16) -> bool {
    let nsid = format!("{NS}.{method}");
    let v = if r.status < 400 {
        ctx.lex.validate_output(&nsid, &r.body)
    } else {
        ctx.lex.validate_error(&nsid, &r.body)
    };
    c.check(
        format!("{method}: HTTP {expect} and the body matches the lexicon"),
        r.status == expect && v.is_empty(),
        if v.is_empty() {
            r.short()
        } else {
            format!(
                "{}; violations: {}",
                r.short(),
                v.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            )
        },
    )
}

// ------------------------------------------------------------------ live

async fn create_key(
    ctx: &Ctx,
    c: &mut Checks,
    base: &str,
    name: &str,
    scopes: &[&str],
) -> Result<(i64, String), String> {
    let auth = bearer(&ctx.admin);
    let r = ctx
        .http
        .post_json(
            &x(base, "admin.createApiKey", ""),
            &[("authorization", &auth)],
            &json!({ "name": name, "scopes": scopes }),
        )
        .await?;
    validate(c, ctx, "admin.createApiKey", &r, 200).await;
    Ok((
        r.body["id"].as_i64().ok_or("no id")?,
        r.body["token"].as_str().ok_or("no token")?.to_owned(),
    ))
}

async fn check_lexicons(
    c: &mut Checks,
    ctx: &Ctx,
    base: &str,
    p: &seed::Pagination,
    key_read: &str,
) -> Result<(), String> {
    c.section("1. lexicon conformance");
    let auth = bearer(&ctx.admin);
    let h = [("authorization", auth.as_str())];
    let s = enc(&p.subject);
    let b1 = did("pgb", 1);
    let reads = [
        ("query.getIncomingBlocks", format!("actor={s}&limit=5")),
        ("query.getIncomingListBlocks", format!("actor={s}&limit=5")),
        ("query.getListsNaming", format!("actor={s}&limit=5")),
        (
            "query.getListMembers",
            format!("list={}&limit=5", enc(&p.big_list)),
        ),
        (
            "query.checkBlocks",
            format!(
                "actor={s}&others={}&others={}",
                enc(&b1),
                enc(&did("pgm", 1))
            ),
        ),
        ("query.getStats", String::new()),
        ("query.getBackfillStatus", format!("actor={s}")),
        ("admin.listErrors", "limit=5".into()),
    ];
    for (m, q) in &reads {
        let r = ctx.http.get(&x(base, m, q), &h).await?;
        validate(c, ctx, m, &r, 200).await;
        if *m == "query.checkBlocks" {
            let hit = r.body["results"].as_array().is_some_and(|a| {
                a.iter()
                    .any(|e| e["did"] == b1.as_str() && e["blocksActor"]["direct"] == true)
            });
            c.check(
                "checkBlocks reports the seeded direct block",
                hit,
                r.short(),
            );
        }
        if *m == "query.getIncomingBlocks" {
            c.check(
                "getIncomingBlocks returns the seeded blocks",
                r.body["blocks"].as_array().is_some_and(|a| a.len() == 5)
                    && r.body["cursor"].is_string(),
                r.short(),
            );
        }
    }
    let r = ctx
        .http
        .post_json(
            &x(base, "admin.requestBackfill", ""),
            &h,
            &json!({ "actor": did("rbq", 0) }),
        )
        .await?;
    validate(c, ctx, "admin.requestBackfill", &r, 202).await;
    for (m, body) in [
        ("admin.startRepair", json!({})),
        ("admin.pauseSweep", json!({ "paused": false })),
        ("admin.restartFirehose", json!({})),
    ] {
        let r = ctx.http.post_json(&x(base, m, ""), &h, &body).await?;
        validate(c, ctx, m, &r, 200).await;
    }
    let (id, _) = create_key(ctx, c, base, "to-revoke", &["read"]).await?;
    let r = ctx
        .http
        .post_json(&x(base, "admin.revokeApiKey", ""), &h, &json!({ "id": id }))
        .await?;
    validate(c, ctx, "admin.revokeApiKey", &r, 200).await;

    // Negatives.
    let r = ctx
        .http
        .get(
            &x(base, "query.getIncomingBlocks", "actor=alice.bsky.social"),
            &[],
        )
        .await?;
    validate(c, ctx, "query.getIncomingBlocks", &r, 400).await;
    c.check(
        "handle-authority actor ⇒ InvalidRequest",
        r.error_name() == Some("InvalidRequest"),
        r.short(),
    );
    let r = ctx
        .http
        .get(
            &x(
                base,
                "query.getIncomingBlocks",
                &format!("actor={s}&cursor=bm9wZQ"),
            ),
            &[],
        )
        .await?;
    c.check(
        "invalid cursor ⇒ InvalidRequest",
        r.status == 400 && r.error_name() == Some("InvalidRequest"),
        r.short(),
    );
    for (m, get) in [
        ("admin.requestBackfill", false),
        ("query.getBackfillStatus", true),
    ] {
        let r = if get {
            ctx.http
                .get(&x(base, m, &format!("actor={s}")), &[])
                .await?
        } else {
            ctx.http
                .post_json(&x(base, m, ""), &[], &json!({ "actor": p.subject }))
                .await?
        };
        validate(c, ctx, m, &r, 401).await;
        c.check(
            format!("{m} without a token ⇒ AuthRequired + WWW-Authenticate: Bearer"),
            r.error_name() == Some("AuthRequired")
                && r.header("www-authenticate").as_deref() == Some("Bearer"),
            r.short(),
        );
    }
    let kauth = bearer(key_read);
    let r = ctx
        .http
        .post_json(
            &x(base, "admin.requestBackfill", ""),
            &[("authorization", &kauth)],
            &json!({ "actor": p.subject }),
        )
        .await?;
    validate(c, ctx, "admin.requestBackfill", &r, 403).await;
    c.check(
        "API key without `backfill` on requestBackfill ⇒ Forbidden",
        r.error_name() == Some("Forbidden"),
        r.short(),
    );
    let r = ctx
        .http
        .get(
            &x(base, "admin.listErrors", ""),
            &[("authorization", &kauth)],
        )
        .await?;
    c.check(
        "API key on an admin procedure ⇒ Forbidden",
        r.status == 403,
        r.short(),
    );
    let r = ctx
        .http
        .get(
            &x(base, "query.getIncomingBlocks", "actor=did%3Aplc%3Ax"),
            &[("authorization", "Bearer fsk_bad")],
        )
        .await?;
    c.check(
        "invalid token ⇒ AuthRequired",
        r.status == 401 && r.error_name() == Some("AuthRequired"),
        r.short(),
    );
    Ok(())
}

/// A boxed hook future (owned state only).
type BoxFut = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>;

/// Pages through `method` collecting `key(item)` until no cursor.
#[allow(clippy::too_many_arguments)]
async fn page_all(
    ctx: &Ctx,
    base: &str,
    method: &str,
    query: &str,
    field: &str,
    limit: usize,
    key: fn(&Value) -> String,
    mut between: Option<(usize, &mut (dyn FnMut() -> BoxFut + Send))>,
) -> Result<Vec<String>, String> {
    let auth = bearer(&ctx.admin);
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    loop {
        let mut q = format!("{query}&limit={limit}");
        if let Some(cu) = &cursor {
            q.push_str(&format!("&cursor={}", enc(cu)));
        }
        let r = ctx
            .http
            .get(&x(base, method, &q), &[("authorization", &auth)])
            .await?;
        if r.status != 200 {
            return Err(r.short());
        }
        let items = r.body[field].as_array().cloned().unwrap_or_default();
        out.extend(items.iter().map(key));
        pages += 1;
        if let Some((after, f)) = between.as_mut() {
            if pages == *after {
                f().await?;
            }
        }
        match r.body["cursor"].as_str() {
            Some(cu) => cursor = Some(cu.to_owned()),
            None => break,
        }
        if pages > 10_000 {
            return Err("runaway paging".into());
        }
    }
    Ok(out)
}

async fn truth(pool: &PgPool, sql: &str, bind: i64) -> Result<Vec<String>, String> {
    sqlx::query_scalar(sql)
        .bind(bind)
        .fetch_all(pool)
        .await
        .map_err(|e| e.to_string())
}

async fn check_pagination(
    c: &mut Checks,
    ctx: &Ctx,
    base: &str,
    pool: &PgPool,
    p: &seed::Pagination,
) -> Result<(), String> {
    c.section("3. pagination");
    let s = format!("actor={}", enc(&p.subject));
    type Case = (
        &'static str,
        String,
        &'static str,
        fn(&Value) -> String,
        &'static str,
        i64,
    );
    let cases: [Case; 4] = [
        (
            "query.getIncomingBlocks",
            s.clone(),
            "blocks",
            |v| v["uri"].as_str().unwrap_or("").to_owned(),
            "SELECT 'at://' || a.did || '/app.bsky.graph.block/' || b.rkey FROM blocks b
             JOIN actors a ON a.id = b.author_id WHERE b.subject_id = $1 ORDER BY b.author_id, b.rkey",
            p.subject_id,
        ),
        (
            "query.getIncomingListBlocks",
            s.clone(),
            "items",
            |v| format!("{} {}", v["list"].as_str().unwrap_or(""), v["listblockUri"].as_str().unwrap_or("")),
            "SELECT 'at://' || o.did || '/app.bsky.graph.list/' || l.rkey || ' at://' || ba.did
                    || '/app.bsky.graph.listblock/' || b.rkey
             FROM lists l JOIN actors o ON o.id = l.owner_id JOIN list_blocks b ON b.list_id = l.id
             JOIN actors ba ON ba.id = b.author_id
             WHERE l.id IN (SELECT list_id FROM list_items WHERE subject_id = $1)
               AND l.track_state IN (2, 3) AND l.record_state = 1
             ORDER BY l.id, b.author_id, b.rkey",
            p.subject_id,
        ),
        (
            "query.getListsNaming",
            s.clone(),
            "lists",
            |v| v["uri"].as_str().unwrap_or("").to_owned(),
            "SELECT 'at://' || o.did || '/app.bsky.graph.list/' || l.rkey FROM lists l
             JOIN actors o ON o.id = l.owner_id
             WHERE l.id IN (SELECT list_id FROM list_items WHERE subject_id = $1)
               AND l.track_state IN (2, 3) AND l.record_state = 1 ORDER BY l.id",
            p.subject_id,
        ),
        (
            "query.getListMembers",
            format!("list={}", enc(&p.big_list)),
            "members",
            |v| format!("{} {}", v["did"].as_str().unwrap_or(""), v["itemUri"].as_str().unwrap_or("")),
            "SELECT a.did || ' at://' || o.did || '/app.bsky.graph.listitem/' || li.rkey
             FROM list_items li JOIN actors a ON a.id = li.subject_id
             JOIN lists l ON l.id = li.list_id JOIN actors o ON o.id = l.owner_id
             WHERE li.list_id = $1 ORDER BY li.subject_id, li.rkey",
            p.big_list_id,
        ),
    ];
    for (m, q, field, key, sql, bind) in cases {
        let want = truth(pool, sql, bind).await?;
        for limit in [10usize, 100, 1000] {
            let got = page_all(ctx, base, m, &q, field, limit, key, None).await?;
            let same = got == want;
            c.check(
                format!("{m}: pages of {limit} concatenate to the stored order"),
                same && want.len() as i64 >= seed::N,
                format!(
                    "{} items via {} pages; stored {}",
                    got.len(),
                    got.len().div_ceil(limit),
                    want.len()
                ),
            );
        }
    }
    // Cursor stability across writes (getIncomingBlocks, pages of 100).
    let want = truth(pool, cases_sql_blocks(), p.subject_id).await?;
    let first_blocker: i64 =
        sqlx::query_scalar("SELECT min(author_id) FROM blocks WHERE subject_id = $1")
            .bind(p.subject_id)
            .fetch_one(pool)
            .await
            .map_err(|e| e.to_string())?;
    let deleted = want[449].clone();
    let del_rkey = deleted.rsplit('/').next().unwrap_or("").to_owned();
    let pool2 = pool.clone();
    let subject_id = p.subject_id;
    let mut writes = move || -> BoxFut {
        let pool = pool2.clone();
        let del_rkey = del_rkey.clone();
        Box::pin(async move {
            // Behind the cursor: a new block by the first blocker.
            seed::block(&pool, first_blocker, "3lpgbehind", subject_id).await?;
            // Ahead of the cursor: delete the 450th row.
            sqlx::query("DELETE FROM blocks WHERE subject_id = $1 AND rkey = $2")
                .bind(subject_id)
                .bind(&del_rkey)
                .execute(&pool)
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        })
    };
    let got = page_all(
        ctx,
        base,
        "query.getIncomingBlocks",
        &s,
        "blocks",
        100,
        |v| v["uri"].as_str().unwrap_or("").to_owned(),
        Some((3, &mut writes)),
    )
    .await?;
    let expected: Vec<String> = want.iter().filter(|u| **u != deleted).cloned().collect();
    let inserted_seen = got.iter().any(|u| u.ends_with("/3lpgbehind"));
    c.check(
        "cursor stays valid across writes: inserted-behind not returned, deleted-ahead skipped",
        got == expected && !inserted_seen,
        format!(
            "{} items; inserted-behind returned: {inserted_seen}; deleted row skipped: {}",
            got.len(),
            !got.contains(&deleted)
        ),
    );
    Ok(())
}

fn cases_sql_blocks() -> &'static str {
    "SELECT 'at://' || a.did || '/app.bsky.graph.block/' || b.rkey FROM blocks b
     JOIN actors a ON a.id = b.author_id WHERE b.subject_id = $1 ORDER BY b.author_id, b.rkey"
}

async fn queue_row(pool: &PgPool, did: &str) -> Result<Option<(i16, i16, String)>, String> {
    sqlx::query_as(
        "SELECT q.tier, q.priority, q.requester FROM backfill_queue q JOIN actors a ON a.id = q.actor_id
         WHERE a.did = $1 AND q.kind = 1",
    )
    .bind(did)
    .fetch_optional(pool)
    .await
    .map_err(|e| e.to_string())
}

async fn check_backfill(
    c: &mut Checks,
    ctx: &Ctx,
    base: &str,
    pool: &PgPool,
    key_bf: (i64, &str),
    key_bfh: (i64, &str),
) -> Result<(), String> {
    c.section("7. backfill endpoints");
    let req = |t: &str, body: Value| {
        let auth = bearer(t);
        let http = ctx.http.clone();
        let url = x(base, "admin.requestBackfill", "");
        async move {
            http.post_json(&url, &[("authorization", &auth)], &body)
                .await
        }
    };
    let (bf_id, bf) = key_bf;
    let (bfh_id, bfh) = key_bfh;
    // Never done ⇒ enqueue (tier 1, requester = the key).
    let r1 = did("rbq", 1);
    let r = req(bf, json!({ "actor": r1 })).await?;
    let q = queue_row(pool, &r1).await?;
    c.check(
        "never done ⇒ enqueued: backfill_queue row (tier 1, requester token:<id>)",
        r.status == 202
            && r.body["enqueued"] == true
            && q == Some((1, 0, format!("token:{bf_id}"))),
        format!("{} queue {q:?}", r.short()),
    );
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM backfill_queue")
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())?;
    let s = ctx
        .http
        .get(
            &x(
                base,
                "query.getBackfillStatus",
                &format!("actor={}", enc(&r1)),
            ),
            &[("authorization", &bearer(bf))],
        )
        .await?;
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM backfill_queue")
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())?;
    c.check(
        "getBackfillStatus reads the queued state and never enqueues",
        s.status == 200 && s.body["repo"]["state"] == "queued" && before == after,
        format!("{} queue rows {before} → {after}", s.short()),
    );
    let unknown = did("rbq", 99);
    let s = ctx
        .http
        .get(
            &x(
                base,
                "query.getBackfillStatus",
                &format!("actor={}", enc(&unknown)),
            ),
            &[("authorization", &bearer(bf))],
        )
        .await?;
    let interned: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM actors WHERE did = $1)")
        .bind(&unknown)
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())?;
    c.check(
        "getBackfillStatus of an unknown account: `never`, nothing interned",
        s.body["repo"]["state"] == "never" && !interned,
        s.short(),
    );
    // Already queued ⇒ no new work; priority raised by the collapse rule.
    let r = req(bfh, json!({ "actor": r1, "priority": "high" })).await?;
    let q = queue_row(pool, &r1).await?;
    c.check(
        "already queued ⇒ no new work; priority raised",
        r.status == 202
            && r.body["enqueued"] == false
            && q == Some((1, 1, format!("token:{bfh_id}"))),
        format!("{} queue {q:?}", r.short()),
    );
    // Downgrade: `high` without backfill:high.
    let r2 = did("rbq", 2);
    let r = req(bf, json!({ "actor": r2, "priority": "high" })).await?;
    let q = queue_row(pool, &r2).await?;
    c.check(
        "`high` without backfill:high ⇒ downgraded to normal",
        r.body["downgraded"] == true
            && r.body["enqueued"] == true
            && q.as_ref().map(|q| q.1) == Some(0),
        format!("{} queue {q:?}", r.short()),
    );
    // Running ⇒ no new work unless force.
    let r3 = did("rbq", 3);
    let id3 = seed::actor(pool, &r3).await?;
    seed::exec(
        pool,
        &format!("INSERT INTO backfill_state (actor_id, state) VALUES ({id3}, 2)"),
    )
    .await?;
    seed::exec(pool, &format!("INSERT INTO job_leases (did, lease_owner, lease_until) VALUES ('{r3}', 'harness', now() + interval '10 minutes')")).await?;
    let r = req(&ctx.admin, json!({ "actor": r3 })).await?;
    let q = queue_row(pool, &r3).await?;
    c.check(
        "running ⇒ no new work",
        r.body["enqueued"] == false && q.is_none() && r.body["repo"]["state"] == "running",
        format!("{} queue {q:?}", r.short()),
    );
    let r = req(&ctx.admin, json!({ "actor": r3, "force": true })).await?;
    let q = queue_row(pool, &r3).await?;
    c.check(
        "running + force ⇒ a waiting entry behind the running job",
        r.body["enqueued"] == true && q.is_some(),
        format!("{} queue {q:?}", r.short()),
    );
    // Done within the fresh window ⇒ no new work unless force.
    let r4 = did("rbq", 4);
    let id4 = seed::actor(pool, &r4).await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO backfill_state (actor_id, state, backfilled_at) VALUES ({id4}, 3, now())"
        ),
    )
    .await?;
    let r = req(&ctx.admin, json!({ "actor": r4 })).await?;
    c.check(
        "done within the fresh window ⇒ no new work",
        r.body["enqueued"] == false && queue_row(pool, &r4).await?.is_none(),
        r.short(),
    );
    let r = req(&ctx.admin, json!({ "actor": r4, "force": true })).await?;
    c.check(
        "done + force ⇒ enqueued",
        r.body["enqueued"] == true && queue_row(pool, &r4).await?.is_some(),
        r.short(),
    );
    // Done but stale ⇒ enqueued.
    let r5 = did("rbq", 5);
    let id5 = seed::actor(pool, &r5).await?;
    seed::exec(pool, &format!("INSERT INTO backfill_state (actor_id, state, backfilled_at) VALUES ({id5}, 3, now() - interval '2 hours')")).await?;
    let r = req(&ctx.admin, json!({ "actor": r5 })).await?;
    let q = queue_row(pool, &r5).await?;
    c.check(
        "done outside the fresh window ⇒ enqueued (requester admin)",
        r.body["enqueued"] == true && q.as_ref().map(|q| q.2.as_str()) == Some("admin"),
        format!("{} queue {q:?}", r.short()),
    );
    // Query-parameter form (D1).
    let r6 = did("rbq", 6);
    let r = ctx
        .http
        .post_form(
            &x(
                base,
                "admin.requestBackfill",
                &format!("actor={}&priority=normal", enc(&r6)),
            ),
            &[("authorization", &bearer(&ctx.admin))],
            &[],
        )
        .await?;
    c.check(
        "query-parameter form accepted (D1)",
        r.status == 202 && r.body["enqueued"] == true && queue_row(pool, &r6).await?.is_some(),
        r.short(),
    );
    Ok(())
}

async fn check_semaphore(
    c: &mut Checks,
    ctx: &Ctx,
    base: &str,
    concurrency: usize,
) -> Result<(), String> {
    c.section("5. semaphore and timeouts");
    let auth = bearer(&ctx.admin);
    let mut holders = Vec::new();
    for _ in 0..concurrency {
        let http = ctx.http.clone();
        let url = x(base, "query.getStats", "_sleep=2.6");
        let auth = auth.clone();
        holders.push(tokio::spawn(async move {
            http.get(&url, &[("authorization", &auth)]).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = ctx
        .http
        .get(&x(base, "query.getStats", ""), &[("authorization", &auth)])
        .await?;
    c.check(
        format!("{concurrency} held query slots ⇒ next request 503 Overloaded after ≥ 2 s, Retry-After: 1"),
        r.status == 503
            && r.error_name() == Some("Overloaded")
            && r.header("retry-after").as_deref() == Some("1")
            && r.elapsed >= Duration::from_millis(1950),
        format!("{} after {:?}", r.short(), r.elapsed),
    );
    let mut ok = 0;
    for h in holders {
        if let Ok(Ok(r)) = h.await {
            if r.status == 200 {
                ok += 1;
            }
        }
    }
    c.check(
        "the held requests themselves complete",
        ok == concurrency,
        format!("{ok}/{concurrency} returned 200"),
    );
    let r = ctx
        .http
        .get(
            &x(base, "query.getStats", "_sleep=4"),
            &[("authorization", &auth)],
        )
        .await?;
    c.check(
        "a query exceeding rate_limit.query_timeout (3 s) ⇒ 503 Overloaded",
        r.status == 503
            && r.error_name() == Some("Overloaded")
            && r.elapsed < Duration::from_millis(3900),
        format!("{} after {:?}", r.short(), r.elapsed),
    );
    Ok(())
}

/// Fires `n` requests concurrently; returns (statuses, elapsed).
async fn burst(
    http: &Http,
    url: &str,
    headers: Vec<(String, String)>,
    n: usize,
    post: Option<Value>,
) -> (Vec<Resp>, Duration) {
    let start = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..n {
        let http = http.clone();
        let url = url.to_owned();
        let headers = headers.clone();
        let post = post.clone();
        tasks.push(tokio::spawn(async move {
            let h: Vec<(&str, &str)> = headers
                .iter()
                .map(|(a, b)| (a.as_str(), b.as_str()))
                .collect();
            match post {
                Some(b) => http.post_json(&url, &h, &b).await,
                None => http.get(&url, &h).await,
            }
        }));
    }
    let mut out = Vec::new();
    for t in tasks {
        if let Ok(Ok(r)) = t.await {
            out.push(r);
        }
    }
    (out, start.elapsed())
}

fn rate_report(rs: &[Resp], elapsed: Duration) -> (usize, usize, String) {
    let ok = rs.iter().filter(|r| r.status < 300).count();
    let limited = rs.iter().filter(|r| r.status == 429).count();
    (
        ok,
        limited,
        format!(
            "{ok} allowed, {limited} limited of {} in {:?}",
            rs.len(),
            elapsed
        ),
    )
}

#[allow(clippy::too_many_arguments)]
async fn saturate(
    c: &mut Checks,
    ctx: &Ctx,
    class: &str,
    url: &str,
    headers: Vec<(String, String)>,
    rate: f64,
    burst_n: usize,
    post: Option<Value>,
    public: bool,
) {
    let n = burst_n + burst_n / 5 + 10;
    let (rs, elapsed) = burst(&ctx.http, url, headers, n, post).await;
    let (ok, limited, detail) = rate_report(&rs, elapsed);
    let max_ok = burst_n as f64 + rate * elapsed.as_secs_f64() + 1.0;
    c.check(
        format!("{class}: 429 at the stated threshold (burst {burst_n}, {rate}/s)"),
        ok >= burst_n && (ok as f64) <= max_ok && limited > 0,
        format!("{detail}; at most {max_ok:.0} allowed"),
    );
    let limited_ok = rs.iter().filter(|r| r.status == 429).all(|r| {
        r.header("retry-after").is_some()
            && r.header("ratelimit-policy")
                .is_some_and(|p| p.contains(&format!("q={burst_n}")))
            && r.header("ratelimit").is_some()
            && r.header("cache-control").as_deref() == Some("no-store")
    });
    c.check(
        format!("{class}: 429 carries Retry-After, RateLimit-Policy, RateLimit and no-store"),
        limited_ok,
        rs.iter()
            .find(|r| r.status == 429)
            .map_or(String::new(), |r| format!("{:?}", r.headers)),
    );
    let ok_resps: Vec<&Resp> = rs.iter().filter(|r| r.status < 300).collect();
    if public {
        c.check(
            format!("{class}: shared-cacheable responses omit per-caller RateLimit headers"),
            ok_resps
                .iter()
                .all(|r| r.header("ratelimit").is_none() && r.header("ratelimit-policy").is_none()),
            format!("{} public responses", ok_resps.len()),
        );
    } else {
        c.check(
            format!("{class}: private responses carry RateLimit headers"),
            ok_resps
                .iter()
                .all(|r| r.header("ratelimit").is_some() && r.header("ratelimit-policy").is_some()),
            format!("{} private responses", ok_resps.len()),
        );
    }
}

fn cf_ip(n: u32) -> String {
    format!("198.51.100.{}", n % 250 + 1)
}

async fn check_rate_limits(
    c: &mut Checks,
    ctx: &Ctx,
    base: &str,
    p: &seed::Pagination,
    key_read: &str,
    key_bf: &str,
) -> Result<(), String> {
    c.section("4. rate limits and cache headers");
    let read_url = x(
        base,
        "query.getIncomingBlocks",
        &format!("actor={}&limit=1", enc(&p.subject)),
    );
    // Anonymous read, keyed by the resolved client (trusted peer + CF header).
    saturate(
        c,
        ctx,
        "anonymous read",
        &read_url,
        vec![("cf-connecting-ip".into(), cf_ip(10))],
        10.0,
        50,
        None,
        true,
    )
    .await;
    saturate(
        c,
        ctx,
        "API-key read",
        &read_url,
        vec![("authorization".into(), bearer(key_read))],
        100.0,
        500,
        None,
        true,
    )
    .await;
    let bf_url = x(base, "admin.requestBackfill", "");
    let body = json!({ "actor": did("rbq", 1) });
    saturate(
        c,
        ctx,
        "requestBackfill (admin)",
        &bf_url,
        vec![("authorization".into(), bearer(&ctx.admin))],
        20.0,
        100,
        Some(body.clone()),
        false,
    )
    .await;
    saturate(
        c,
        ctx,
        "requestBackfill (API key)",
        &bf_url,
        vec![("authorization".into(), bearer(key_bf))],
        5.0,
        20,
        Some(body),
        false,
    )
    .await;
    // UI lookup (anonymous, per IP): 1/s, burst 5.
    let mut statuses = Vec::new();
    let start = Instant::now();
    for _ in 0..8 {
        let r = ctx
            .http
            .get(
                &format!("{base}/lookup/did?q={}", enc(&p.subject)),
                &[("cf-connecting-ip", &cf_ip(20))],
            )
            .await?;
        statuses.push(r.status);
    }
    let ok = statuses.iter().filter(|s| **s == 200).count();
    let max_ok = 5.0 + start.elapsed().as_secs_f64() + 1.0;
    c.check(
        "UI lookup (anonymous): 429 at the stated threshold (burst 5, 1/s)",
        ok >= 5 && (ok as f64) <= max_ok && statuses.contains(&429),
        format!("{statuses:?} in {:?}", start.elapsed()),
    );
    // UI login: 5/min per IP.
    let mut statuses = Vec::new();
    for _ in 0..7 {
        let r = ctx
            .http
            .post_form(
                &format!("{base}/enter"),
                &[("cf-connecting-ip", &cf_ip(21))],
                &[("password", "wrong-password-xx")],
            )
            .await?;
        statuses.push(r.status);
    }
    c.check(
        "UI login attempts: 5 per minute per IP, then 429",
        statuses[..5].iter().all(|s| *s == 401) && statuses[5..].iter().all(|s| *s == 429),
        format!("{statuses:?}"),
    );
    // Cache headers (§9.4).
    let cc = |r: &Resp| r.header("cache-control").unwrap_or_default();
    let ip = cf_ip(30);
    let h = [("cf-connecting-ip", ip.as_str())];
    let r = ctx.http.get(&x(base, "query.getStats", ""), &h).await?;
    c.check(
        "getStats: Cache-Control public, max-age=60",
        cc(&r) == "public, max-age=60",
        cc(&r),
    );
    let r = ctx
        .http
        .get(&read_url, &[("cf-connecting-ip", &cf_ip(31))])
        .await?;
    c.check(
        "per-DID read: Cache-Control public, max-age=30 + CORS",
        cc(&r) == "public, max-age=30"
            && r.header("access-control-allow-origin").as_deref() == Some("*"),
        cc(&r),
    );
    let r = ctx
        .http
        .get(
            &x(
                base,
                "query.getBackfillStatus",
                &format!("actor={}", enc(&p.subject)),
            ),
            &[("authorization", &bearer(&ctx.admin))],
        )
        .await?;
    c.check(
        "getBackfillStatus: Cache-Control no-store, private",
        cc(&r) == "no-store, private",
        cc(&r),
    );
    let r = ctx
        .http
        .get(
            &x(base, "query.getIncomingBlocks", "actor=bad"),
            &[("cf-connecting-ip", &cf_ip(32))],
        )
        .await?;
    c.check(
        "errors: Cache-Control no-store",
        cc(&r) == "no-store" && r.status == 400,
        cc(&r),
    );
    let r = ctx.http.get(&format!("{base}/health"), &[]).await?;
    let r2 = ctx.http.get(&format!("{base}/livez"), &[]).await?;
    c.check(
        "/health and /livez: no-store",
        cc(&r) == "no-store" && cc(&r2) == "no-store",
        format!("{} / {}", cc(&r), cc(&r2)),
    );
    let r = ctx.http.get(&format!("{base}/ops"), &[]).await?;
    c.check(
        "UI admin page without a session ⇒ redirect to /enter",
        r.status == 303 && r.header("location").as_deref() == Some("/enter"),
        r.short(),
    );
    Ok(())
}

async fn check_client_ip(
    c: &mut Checks,
    ctx: &Ctx,
    base: &str,
    p: &seed::Pagination,
) -> Result<(), String> {
    c.section("6. client IP");
    let url = x(
        base,
        "query.getIncomingBlocks",
        &format!("actor={}&limit=1", enc(&p.subject)),
    );
    // Trusted peer (127.0.0.1) with CF-Connecting-IP: keyed by that IP.
    let (rs, _) = burst(
        &ctx.http,
        &url,
        vec![("cf-connecting-ip".into(), "203.0.113.50".into())],
        70,
        None,
    )
    .await;
    let limited = rs.iter().any(|r| r.status == 429);
    let other = ctx
        .http
        .get(&url, &[("cf-connecting-ip", "203.0.113.51")])
        .await?;
    c.check(
        "trusted peer + CF-Connecting-IP ⇒ rate-limited by that IP (another IP is unaffected)",
        limited && other.status == 200,
        format!(
            "first IP limited: {limited}; second IP: HTTP {}",
            other.status
        ),
    );
    // Untrusted peer (127.0.0.2): the header is ignored, keyed by the peer.
    let untrusted = Http::new(Some("127.0.0.2".parse::<IpAddr>().expect("ip")));
    let (rs, _) = burst(
        &untrusted,
        &url,
        vec![("cf-connecting-ip".into(), "203.0.113.60".into())],
        70,
        None,
    )
    .await;
    let limited = rs.iter().any(|r| r.status == 429);
    let other = untrusted
        .get(&url, &[("cf-connecting-ip", "203.0.113.61")])
        .await?;
    c.check(
        "untrusted peer + CF-Connecting-IP ⇒ rate-limited by the peer (forged header ignored)",
        limited && other.status == 429,
        format!(
            "first header limited: {limited}; different header from the same peer: HTTP {}",
            other.status
        ),
    );
    Ok(())
}

async fn check_metrics(c: &mut Checks, ctx: &Ctx, metrics_base: &str) -> Result<(), String> {
    c.section("errors and metrics");
    let r = ctx
        .http
        .get(&format!("{metrics_base}/metrics"), &[])
        .await?;
    for m in farsight_api::metrics::ALL {
        c.check(
            format!("metric {m} exposed"),
            r.text.contains(m),
            String::new(),
        );
    }
    let has_429 = r
        .text
        .lines()
        .any(|l| l.starts_with("farsight_query_requests_total") && l.contains("status=\"429\""));
    let has_rl = r.text.lines().any(|l| {
        l.starts_with("farsight_rate_limited_total")
            && l.contains("anon_read")
            && !l.ends_with(" 0")
    });
    c.check(
        "query_requests_total counts 429s; rate_limited_total counts the class",
        has_429 && has_rl,
        String::new(),
    );
    Ok(())
}

async fn check_reset(
    c: &mut Checks,
    ctx: &Ctx,
    s: &Server,
    pool: &PgPool,
    p: &seed::Pagination,
    dsn: &str,
) -> Result<(), String> {
    c.section("8. reset");
    let base = &s.base;
    let r = ctx
        .http
        .post_form(
            &format!("{base}/enter"),
            &[("cf-connecting-ip", "203.0.113.90")],
            &[("password", PASSWORD)],
        )
        .await?;
    let Some(cookie) = set_cookie(&r, "farsight_admin") else {
        c.check("admin login", false, r.short());
        return Ok(());
    };
    c.check(
        "admin login creates a server-side session",
        r.status == 303,
        r.short(),
    );
    let page = ctx
        .http
        .get(&format!("{base}/reset"), &[("cookie", &cookie)])
        .await?;
    let csrf = csrf_of(&page.text).unwrap_or_default();
    let r = ctx
        .http
        .post_form(
            &format!("{base}/reset"),
            &[("cookie", &cookie)],
            &[("csrf", &csrf), ("hostname", "wrong.example")],
        )
        .await?;
    c.check(
        "reset refuses a wrong hostname",
        r.text.contains("does not match") && s.config_path().exists(),
        r.short(),
    );
    let r = ctx
        .http
        .post_form(
            &format!("{base}/reset"),
            &[("cookie", &cookie)],
            &[("csrf", "bogus"), ("hostname", HOSTNAME)],
        )
        .await?;
    c.check(
        "reset refuses a bad CSRF token",
        r.status == 403 && s.config_path().exists(),
        r.short(),
    );
    let blocks_before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM blocks WHERE subject_id = $1")
            .bind(p.subject_id)
            .fetch_one(pool)
            .await
            .map_err(|e| e.to_string())?;
    let old_token = std::fs::read_to_string(s.token_path()).unwrap_or_default();
    let r = ctx
        .http
        .post_form(
            &format!("{base}/reset"),
            &[("cookie", &cookie)],
            &[("csrf", &csrf), ("hostname", HOSTNAME)],
        )
        .await?;
    c.check(
        "reset with the hostname confirmed",
        r.status == 200 && r.text.contains("Configuration reset"),
        r.short(),
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    let sessions: i64 = sqlx::query_scalar("SELECT count(*) FROM admin_sessions")
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())?;
    let live_keys: i64 =
        sqlx::query_scalar("SELECT count(*) FROM api_tokens WHERE revoked_at IS NULL")
            .fetch_one(pool)
            .await
            .map_err(|e| e.to_string())?;
    let blocks_after: i64 = sqlx::query_scalar("SELECT count(*) FROM blocks WHERE subject_id = $1")
        .bind(p.subject_id)
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())?;
    let new_token = std::fs::read_to_string(s.token_path()).unwrap_or_default();
    c.check(
        "config.toml deleted",
        !s.config_path().exists(),
        s.config_path().display().to_string(),
    );
    c.check(
        "admin sessions revoked and every API key revoked",
        sessions == 0 && live_keys == 0,
        format!("sessions {sessions}, live keys {live_keys}"),
    );
    c.check(
        "a new setup token was written",
        !new_token.is_empty() && new_token != old_token,
        String::new(),
    );
    c.check(
        "database kept",
        blocks_before == blocks_after && blocks_after > 0,
        format!("{blocks_before} → {blocks_after} blocks"),
    );
    let x1 = ctx.http.get(&x(base, "query.getStats", ""), &[]).await?;
    let h = ctx.http.get(&format!("{base}/health"), &[]).await?;
    let st = ctx.http.get(&format!("{base}/setup"), &[]).await?;
    c.check(
        "in-process switch to setup mode: /xrpc ⇒ 503 SetupRequired, /health ⇒ 503 setup, /setup serves",
        x1.status == 503 && x1.error_name() == Some("SetupRequired") && h.status == 503 && h.body["status"] == "setup" && st.text.contains("Setup token"),
        format!("{} | {} | setup {}", x1.short(), h.short(), st.status),
    );
    let old_admin = ctx
        .http
        .get(
            &x(base, "admin.listErrors", ""),
            &[("authorization", &bearer(&ctx.admin))],
        )
        .await?;
    c.check(
        "the old admin token no longer works",
        old_admin.status == 503 || old_admin.status == 401,
        old_admin.short(),
    );
    // The wizard's storage step recognizes the kept database.
    let token = new_token.lines().next().unwrap_or("").to_owned();
    let r = ctx
        .http
        .post_form(&format!("{base}/setup"), &[], &[("token", &token)])
        .await?;
    let Some(sc) = set_cookie(&r, "farsight_setup") else {
        c.check("setup token accepted after reset", false, r.short());
        return Ok(());
    };
    c.check(
        "the new setup token is accepted",
        r.status == 303,
        r.short(),
    );
    let page = ctx
        .http
        .get(&format!("{base}/setup/storage"), &[("cookie", &sc)])
        .await?;
    let page = if page.status == 303 {
        ctx.http
            .get(&format!("{base}/setup/welcome"), &[("cookie", &sc)])
            .await?
    } else {
        page
    };
    let csrf = csrf_of(&page.text).unwrap_or_default();
    let r = ctx
        .http
        .post_form(
            &format!("{base}/setup/storage/test"),
            &[("cookie", &sc)],
            &[("csrf", &csrf), ("dsn", dsn)],
        )
        .await?;
    c.check(
        "the wizard's storage step recognizes the existing database",
        r.text.contains("Existing Farsight database recognized"),
        support::truncate(r.text.split("<pre>").nth(1).unwrap_or(""), 200),
    );
    Ok(())
}

// --------------------------------------------------------------- coverage

/// Keeps `firehose_state` fresh as a connected v2 stream unless paused.
fn spawn_firehose_keeper(pool: PgPool, paused: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if !paused.load(Ordering::Relaxed) {
                let _ = sqlx::query(
                    "INSERT INTO firehose_state (id, source_url, protocol, applied_through, first_applied_at, connected)
                     VALUES (1, 'ws://harness', 2, now(), now() - interval '2 days', true)
                     ON CONFLICT (id) DO UPDATE SET applied_through = now(), connected = true, protocol = 2",
                )
                .execute(&pool)
                .await;
                let _ = sqlx::query("INSERT INTO firehose_clock (server_at, witness_at) VALUES (now(), now()) ON CONFLICT DO NOTHING")
                    .execute(&pool)
                    .await;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
}

struct Cov<'a> {
    ctx: &'a Ctx,
    base: String,
    pool: PgPool,
}

impl Cov<'_> {
    async fn get(&self, method: &str, q: &str) -> Result<Resp, String> {
        self.ctx
            .http
            .get(
                &x(&self.base, method, q),
                &[("authorization", &bearer(&self.ctx.admin))],
            )
            .await
    }

    async fn cov(&self, method: &str, q: &str) -> Result<(Value, Resp), String> {
        let r = self.get(method, q).await?;
        Ok((r.body["freshness"]["coverage"].clone(), r))
    }

    async fn refresh(&self) -> Result<(), String> {
        seed::notify(&self.pool).await
    }
}

fn reasons(cov: &Value) -> BTreeSet<String> {
    cov["reasons"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn level(cov: &Value) -> &str {
    cov["level"].as_str().unwrap_or("?")
}

async fn sql_i64(pool: &PgPool, q: &str) -> Result<i64, String> {
    sqlx::query_scalar(q)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("{e}: {q}"))
}

async fn sql_ts(pool: &PgPool, q: &str) -> Result<String, String> {
    let t: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(q)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("{e}: {q}"))?;
    Ok(farsight_api::freshness::ts(t))
}

async fn phase_coverage(
    c: &mut Checks,
    ctx: &Ctx,
    pg: &Pg,
    metrics_port: u16,
) -> Result<(), String> {
    c.section("2. coverage honesty (scripted rows, debts and list states)");
    pg.create_db("farsight_cov").await?;
    let dsn = pg.url("farsight_cov");
    let port = free_port()?;
    let cfg = config_toml(
        &dsn,
        &ctx.admin,
        "ws://127.0.0.1:9",
        "api_key",
        &format!("127.0.0.1:{metrics_port}"),
        "",
    );
    let server = Server::start("coverage", Some(&cfg), &format!("127.0.0.1:{port}"), &[])?;
    server.wait_live(&ctx.http, Duration::from_secs(60)).await?;
    let pool = pg.pool("farsight_cov", 4).await?;
    let paused = Arc::new(AtomicBool::new(false));
    let keeper = spawn_firehose_keeper(pool.clone(), paused.clone());
    let v = Cov {
        ctx,
        base: server.base.clone(),
        pool: pool.clone(),
    };
    // Baseline: a completed full sweep covering every collection.
    seed::exec(&pool, "INSERT INTO sweep_cycles (kind, source, collections, started_at, effective_start, effective_start_witness, enumerated_at, completed_at, completed_witness)
        VALUES (1, 'relay_collections', '{1,2,3,4}', now() - interval '2 days', now() - interval '2 days', now() - interval '2 days', now() - interval '1 day', now() - interval '1 day', now() - interval '1 day')").await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let completed = sql_ts(
        &pool,
        "SELECT completed_witness FROM sweep_cycles WHERE kind = 1",
    )
    .await?;
    let xd = did("cvx", 1);
    let xid = seed::actor(&pool, &xd).await?;
    let xq = format!("actor={}", enc(&xd));
    v.refresh().await?;

    // a. complete with no exceptions.
    let (cv, r) = v.cov("query.getIncomingBlocks", &xq).await?;
    let ex_zero = cv["exceptions"]
        .as_object()
        .is_some_and(|m| m.values().all(|n| n == 0));
    c.check(
        "complete with no exceptions: level complete, no reasons, completeSince = baseline completed_witness",
        level(&cv) == "complete" && reasons(&cv).is_empty() && cv["completeSince"] == completed.as_str() && ex_zero,
        r.short(),
    );
    c.check(
        "reads = api_key ⇒ Cache-Control private, max-age=30; anonymous ⇒ AuthRequired",
        r.header("cache-control").as_deref() == Some("private, max-age=30")
            && ctx
                .http
                .get(&x(&v.base, "query.getStats", ""), &[])
                .await?
                .status
                == 401,
        r.header("cache-control").unwrap_or_default(),
    );

    // b. complete with each named exception, counted from stored rows.
    let p = seed::actor(&pool, &did("cvp", 1)).await?;
    for (i, reason) in [(1u64, 1i16), (2, 2), (3, 3), (4, 4)] {
        let d = seed::actor(&pool, &did("cvd", i)).await?;
        seed::debt(&pool, d, reason).await?;
    }
    let d5 = seed::actor(&pool, &did("cvd", 5)).await?;
    seed::debt(&pool, d5, 1).await?;
    seed::debt(&pool, d5, 2).await?;
    let a_debt = seed::actor(&pool, &did("cva", 9)).await?;
    seed::debt(&pool, a_debt, 2).await?;
    let l_ready = seed::list(&pool, p, "ready", 2, 1, 1, false, Some(3600)).await?;
    let l_capped = seed::list(&pool, p, "capped", 2, 1, 1, true, Some(3600)).await?;
    let l_unavail = seed::list(&pool, p, "unavail", 4, 1, 1, false, None).await?;
    let _l_missing = seed::list(&pool, p, "missing", 6, 0, 1, false, None).await?;
    let _l_deferred = seed::list(&pool, p, "deferred", 8, 1, 1, false, None).await?;
    let _l_dead = seed::list(&pool, p, "dead", 7, 2, 0, false, None).await?;
    let l_excl = seed::list(&pool, p, "pendexcl", 1, 1, 1, false, None).await?;
    seed::listblock(&pool, a_debt, "3lexcl", l_excl, Some(600)).await?;
    v.refresh().await?;
    let (cv, r) = v.cov("query.getIncomingBlocks", &xq).await?;
    let want = [
        ("unreachableRepos", "SELECT count(DISTINCT actor_id) FROM relist_debt WHERE reason = 1"),
        ("pendingResyncs", "SELECT count(DISTINCT actor_id) FROM relist_debt WHERE reason = 2"),
        ("cappedAuthors", "SELECT count(DISTINCT actor_id) FROM relist_debt WHERE reason = 3"),
        ("refusedAuthors", "SELECT count(DISTINCT actor_id) FROM relist_debt WHERE reason = 4"),
        ("unavailableLists", "SELECT count(*) FROM lists WHERE track_state = 4"),
        ("missingLists", "SELECT count(*) FROM lists WHERE track_state = 6"),
        ("deferredLists", "SELECT count(*) FROM lists WHERE track_state = 8"),
        ("cappedLists", "SELECT count(*) FROM lists WHERE track_state IN (1,2,3,4) AND capped"),
        // §3.7.4: pending lists whose counted listblocks are all by authors with a resync/unreachable debt.
        ("excludedPendingLists", "SELECT count(*) FROM lists l WHERE l.track_state = 1 AND NOT EXISTS (
            SELECT 1 FROM list_blocks b WHERE b.list_id = l.id AND b.counted
              AND NOT EXISTS (SELECT 1 FROM relist_debt d WHERE d.actor_id = b.author_id AND d.reason IN (1, 2)))"),
    ];
    let mut all = true;
    let mut detail = Vec::new();
    for (k, q) in want {
        let w = sql_i64(&pool, q).await?;
        let got = cv["exceptions"][k].as_i64().unwrap_or(-1);
        all &= got == w && w > 0;
        detail.push(format!("{k} {got}/{w}"));
    }
    c.check(
        "complete with every named exception: level stays complete",
        level(&cv) == "complete",
        r.short(),
    );
    c.check(
        "exceptions equal the stored counts (each > 0)",
        all,
        detail.join(", "),
    );
    let pending = sql_i64(&pool, "SELECT count(*) FROM lists WHERE track_state = 1").await?;
    c.check(
        "coverage.pendingLists = stored pending lists",
        cv["pendingLists"].as_i64() == Some(pending),
        format!("{} / {pending}", cv["pendingLists"]),
    );

    // c. partial with each reason.
    let partial_case = |name: &'static str, cv: &Value, r: &Resp| -> (bool, String) {
        (
            level(cv) == "partial" && reasons(cv).contains(name),
            r.short(),
        )
    };
    // sweep_incomplete
    seed::exec(
        &pool,
        "UPDATE sweep_cycles SET completed_at = NULL WHERE kind = 1",
    )
    .await?;
    v.refresh().await?;
    let (cv, r) = v.cov("query.getIncomingBlocks", &xq).await?;
    let (ok, d) = partial_case("sweep_incomplete", &cv, &r);
    c.check("partial + sweep_incomplete (no completed baseline)", ok, d);
    let s = v
        .get(
            "query.getBackfillStatus",
            &format!("actor={}", enc(&did("cvu", 1))),
        )
        .await?;
    c.check(
        "getBackfillStatus without a baseline: never",
        s.body["repo"]["state"] == "never",
        s.short(),
    );
    seed::exec(
        &pool,
        "UPDATE sweep_cycles SET completed_at = now() - interval '1 day' WHERE kind = 1",
    )
    .await?;
    v.refresh().await?;
    let s = v
        .get(
            "query.getBackfillStatus",
            &format!("actor={}", enc(&did("cvu", 1))),
        )
        .await?;
    c.check(
        "getBackfillStatus with a completed baseline: covered_by_sweep",
        s.body["repo"]["state"] == "covered_by_sweep",
        s.short(),
    );
    // firehose_gap, then healed ⇒ completeSince moves.
    seed::exec(&pool, "INSERT INTO firehose_gaps (from_at, to_at, cause) VALUES (now() - interval '1 hour', now() - interval '50 minutes', 2)").await?;
    v.refresh().await?;
    let (cv, r) = v.cov("query.getIncomingBlocks", &xq).await?;
    let (ok, d) = partial_case("firehose_gap", &cv, &r);
    c.check("partial + firehose_gap (unhealed gap after S_C)", ok, d);
    seed::exec(&pool, "UPDATE firehose_gaps SET healed_at = now(), healed_witness = now() - interval '10 minutes'").await?;
    v.refresh().await?;
    let healed = sql_ts(&pool, "SELECT max(healed_witness) FROM firehose_gaps").await?;
    let (cv, r) = v.cov("query.getIncomingBlocks", &xq).await?;
    c.check(
        "healed gap ⇒ complete again, completeSince = the gap's healed_witness",
        level(&cv) == "complete" && cv["completeSince"] == healed.as_str(),
        r.short(),
    );
    // firehose_disconnected / firehose_lagging / sync_events_unavailable
    paused.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    for (name, sql) in [
        (
            "firehose_disconnected",
            "UPDATE firehose_state SET connected = false, applied_through = now()",
        ),
        (
            "firehose_lagging",
            "UPDATE firehose_state SET connected = true, applied_through = now() - interval '10 minutes'",
        ),
        (
            "sync_events_unavailable",
            "UPDATE firehose_state SET connected = true, applied_through = now(), protocol = 1",
        ),
    ] {
        seed::exec(&pool, sql).await?;
        v.refresh().await?;
        let (cv, r) = v.cov("query.getIncomingBlocks", &xq).await?;
        let (ok, d) = partial_case(name, &cv, &r);
        c.check(format!("partial + {name}"), ok, d);
    }
    paused.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    // storage_refusal
    seed::exec(
        &pool,
        "INSERT INTO storage_refusals (from_witness) VALUES (now() - interval '1 minute')",
    )
    .await?;
    v.refresh().await?;
    let (cv, r) = v.cov("query.getIncomingBlocks", &xq).await?;
    let (ok, d) = partial_case("storage_refusal", &cv, &r);
    c.check(
        "partial + storage_refusal (open global refusal interval)",
        ok,
        d,
    );
    seed::exec(&pool, "UPDATE storage_refusals SET to_witness = now()").await?;
    v.refresh().await?;
    let (cv, r) = v.cov("query.getIncomingBlocks", &xq).await?;
    c.check(
        "refusal closed ⇒ complete",
        level(&cv) == "complete",
        r.short(),
    );

    // List scope.
    let pend = seed::list(&pool, p, "pending", 1, 1, 1, false, None).await?;
    let a2 = seed::actor(&pool, &did("cva", 2)).await?;
    seed::listblock(&pool, a2, "3lpend", pend, Some(1800)).await?;
    v.refresh().await?;
    let list_q = |rkey: &str| {
        format!(
            "list={}",
            enc(&format!(
                "at://{}/app.bsky.graph.list/{rkey}",
                did("cvp", 1)
            ))
        )
    };
    for (rkey, want_level, reason, state) in [
        ("pending", "partial", "list_pending", "pending"),
        ("capped", "complete", "list_capped", "ready"),
        ("unavail", "complete", "list_unavailable", "unavailable"),
        ("missing", "complete", "list_missing", "missing"),
        ("deferred", "complete", "list_deferred", "deferred"),
        ("dead", "complete", "list_missing", "dead"),
        ("never-seen", "complete", "list_not_tracked", "untracked"),
    ] {
        let (cv, r) = v.cov("query.getListMembers", &list_q(rkey)).await?;
        c.check(
            format!("getListMembers({rkey}): {want_level} + {reason}, state {state}"),
            level(&cv) == want_level && reasons(&cv).contains(reason) && r.body["state"] == state,
            r.short(),
        );
    }
    let (cv, r) = v.cov("query.getListMembers", &list_q("ready")).await?;
    c.check(
        "getListMembers(ready, covered fetch): complete, no reasons",
        level(&cv) == "complete" && reasons(&cv).is_empty(),
        r.short(),
    );

    // §3.7.4: a pending list with live relevant listblocks caps indexedAt.
    let (cv, r) = v.cov("query.getIncomingListBlocks", &xq).await?;
    let cap = sql_ts(&pool, &format!("SELECT min(witnessed_at) - interval '1 microsecond' FROM list_blocks WHERE list_id = {pend}")).await?;
    c.check(
        "pending list with witnessed listblocks: list endpoints stay complete + list_pending, indexedAt = min(witnessed_at) − 1 µs",
        level(&cv) == "complete" && reasons(&cv).contains("list_pending") && r.body["freshness"]["indexedAt"] == cap.as_str(),
        format!("indexedAt {} want {cap}", r.body["freshness"]["indexedAt"]),
    );
    let a3 = seed::actor(&pool, &did("cva", 3)).await?;
    let hist = seed::list(&pool, p, "historical", 1, 1, 1, false, None).await?;
    seed::listblock(&pool, a3, "3lhist", hist, None).await?;
    v.refresh().await?;
    let (cv, r) = v.cov("query.getListsNaming", &xq).await?;
    let (ok, d) = partial_case("list_pending_historical", &cv, &r);
    c.check(
        "partial + list_pending_historical (relevant listblock first stored by a listing)",
        ok,
        d,
    );
    let (cv, _) = v.cov("query.getIncomingBlocks", &xq).await?;
    c.check(
        "direct-block scope is not affected by pending lists",
        level(&cv) == "complete",
        String::new(),
    );
    seed::exec(
        &pool,
        &format!("DELETE FROM list_blocks WHERE list_id = {hist}"),
    )
    .await?;
    seed::exec(
        &pool,
        &format!("UPDATE lists SET track_state = 0, listblock_count = 0 WHERE id = {hist}"),
    )
    .await?;
    v.refresh().await?;
    // Live rule: a ready list naming X turns pending after the snapshot.
    let o_live = seed::actor(&pool, &did("cvp", 2)).await?;
    let l_live = seed::list(&pool, o_live, "livelist", 2, 1, 1, false, Some(600)).await?;
    seed::item(&pool, o_live, "3lliveitem", l_live, xid).await?;
    v.refresh().await?;
    let (cv, _) = v.cov("query.getListsNaming", &xq).await?;
    let before = level(&cv).to_owned();
    // Flip it without a notification: the snapshot still has the old state.
    seed::exec(
        &pool,
        &format!("UPDATE lists SET track_state = 1 WHERE id = {l_live}"),
    )
    .await?;
    let (cv, r) = v.cov("query.getListsNaming", &xq).await?;
    c.check(
        "live rule: a list naming X that is live pending (unseen by the snapshot) is excluded and makes the answer partial + list_pending",
        before == "complete" && level(&cv) == "partial" && reasons(&cv).contains("list_pending") && r.body["lists"].as_array().is_some_and(Vec::is_empty),
        format!("before {before}; {}", r.short()),
    );
    seed::exec(
        &pool,
        &format!("UPDATE lists SET track_state = 2 WHERE id = {l_live}"),
    )
    .await?;
    v.refresh().await?;

    // checkBlocks per-result coverage.
    let vd = did("cvv", 1);
    let vid = seed::actor(&pool, &vd).await?;
    let o = |n| did("cvo", n);
    let mut oid = Vec::new();
    for n in 1..=7 {
        oid.push(seed::actor(&pool, &o(n)).await?);
    }
    seed::debt(&pool, oid[0], 2).await?; // O1: debt
    seed::listblock(&pool, oid[1], "3lo2", pend, Some(600)).await?; // O2: pending list
    seed::listblock(&pool, oid[2], "3lo3", l_unavail, Some(600)).await?; // O3: unavailable list
    seed::listblock(&pool, oid[3], "3lo4", l_capped, Some(600)).await?; // O4: capped list
    seed::block(&pool, oid[4], "3lo5blk", vid).await?; // O5 blocks V
    seed::listblock(&pool, oid[5], "3lo6", l_ready, Some(600)).await?; // O6 listblocks L_ready…
    seed::item(&pool, p, "3lreadyv", l_ready, vid).await?; // …which names V
    seed::block(&pool, vid, "3lvblk", oid[6]).await?; // V blocks O7
    v.refresh().await?;
    let q = format!(
        "actor={}{}",
        enc(&vd),
        (1..=7)
            .map(|n| format!("&others={}", enc(&o(n))))
            .collect::<String>()
    );
    let r = v.get("query.checkBlocks", &q).await?;
    let pf: Vec<(String, BTreeSet<String>)> = r.body["partialFor"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|e| (e["did"].as_str().unwrap_or("").to_owned(), reasons(e)))
        .collect();
    let has = |d: &str, reason: &str| pf.iter().any(|(x, rs)| x == d && rs.contains(reason));
    c.check(
        "checkBlocks partialFor: party_debt, list_pending, list_unavailable, list_capped for the affected pairs only",
        has(&o(1), "party_debt") && has(&o(2), "list_pending") && has(&o(3), "list_unavailable") && has(&o(4), "list_capped") && pf.len() == 4,
        format!("{pf:?}"),
    );
    let res = r.body["results"].as_array().cloned().unwrap_or_default();
    let find = |d: &str| {
        res.iter()
            .find(|e| e["did"] == d)
            .cloned()
            .unwrap_or(Value::Null)
    };
    let l_ready_uri = format!("at://{}/app.bsky.graph.list/ready", did("cvp", 1));
    c.check(
        "checkBlocks results: direct both ways and via a ready list",
        find(&o(5))["blocksActor"]["direct"] == true
            && find(&o(6))["blocksActor"]["lists"] == json!([l_ready_uri])
            && find(&o(7))["blockedByActor"]["direct"] == true
            && find(&o(1)).is_null(),
        support::truncate(&r.text, 500),
    );
    c.check(
        "checkBlocks response level complete when the viewer side is clean",
        level(&r.body["freshness"]["coverage"]) == "complete",
        r.short(),
    );
    seed::listblock(&pool, vid, "3lvpend", pend, Some(600)).await?;
    v.refresh().await?;
    let r = v.get("query.checkBlocks", &q).await?;
    let cv = r.body["freshness"]["coverage"].clone();
    c.check(
        "viewer holds a listblock on a pending list ⇒ response partial + list_pending",
        level(&cv) == "partial" && reasons(&cv).contains("list_pending"),
        r.short(),
    );
    let (vv, rr) = v
        .cov(
            "query.checkBlocks",
            &format!("actor={}&others={}", enc(&o(1)), enc(&o(5))),
        )
        .await?;
    c.check(
        "party_debt on the response when the viewer itself holds a debt",
        reasons(&vv).contains("party_debt") && level(&vv) == "partial",
        rr.short(),
    );

    // d. assisted from a seeded discovery state; discovery_truncated.
    seed::exec(
        &pool,
        "UPDATE sweep_cycles SET completed_at = NULL WHERE kind = 1",
    )
    .await?;
    let x2 = did("cvx", 2);
    let x2id = seed::actor(&pool, &x2).await?;
    seed::exec(&pool, &format!("INSERT INTO discovery_state (actor_id, state, source, started_at, discovered_witness, completed_at, truncated, refs_found)
        VALUES ({x2id}, 3, 'https://backlinks.test', now() - interval '12 minutes', now() - interval '10 minutes', now() - interval '5 minutes', false, 3)")).await?;
    seed::exec(&pool, &format!("INSERT INTO subject_coverage (actor_id, scope, confirmed_at, refs_found) VALUES ({x2id}, 1, now(), 1), ({x2id}, 2, now(), 2)")).await?;
    let x3 = did("cvx", 3);
    let x3id = seed::actor(&pool, &x3).await?;
    seed::exec(&pool, &format!("INSERT INTO discovery_state (actor_id, state, source, started_at, discovered_witness, completed_at, truncated, refs_found)
        VALUES ({x3id}, 3, 'https://backlinks.test', now() - interval '12 minutes', now() - interval '10 minutes', now() - interval '5 minutes', true, 200000)")).await?;
    v.refresh().await?;
    let dwit = sql_ts(
        &pool,
        &format!("SELECT discovered_witness FROM discovery_state WHERE actor_id = {x2id}"),
    )
    .await?;
    let (cv, r) = v
        .cov("query.getIncomingBlocks", &format!("actor={}", enc(&x2)))
        .await?;
    c.check(
        "assisted: sweep incomplete, discovery completed untruncated with covered(D) ⇒ assisted, completeSince = D",
        level(&cv) == "assisted" && cv["completeSince"] == dwit.as_str(),
        r.short(),
    );
    let (cv, r) = v
        .cov(
            "query.getIncomingListBlocks",
            &format!("actor={}", enc(&x2)),
        )
        .await?;
    let idx = r.body["freshness"]["indexedAt"].as_str().unwrap_or("");
    c.check(
        "assisted list endpoints: other pending lists lower indexedAt to at most D",
        level(&cv) == "assisted" && !idx.is_empty() && idx <= dwit.as_str(),
        format!("indexedAt {idx}, D {dwit}"),
    );
    let (cv, r) = v
        .cov("query.getIncomingBlocks", &format!("actor={}", enc(&x3)))
        .await?;
    let (ok, d) = partial_case("discovery_truncated", &cv, &r);
    c.check("partial + discovery_truncated", ok, d);
    seed::exec(
        &pool,
        "UPDATE sweep_cycles SET completed_at = now() - interval '1 day' WHERE kind = 1",
    )
    .await?;

    // Every read endpoint's freshness validates against the lexicon here too.
    v.refresh().await?;
    for (m, q) in [
        ("query.getIncomingBlocks", xq.clone()),
        ("query.getIncomingListBlocks", xq.clone()),
        ("query.getListsNaming", xq.clone()),
        ("query.getListMembers", list_q("ready")),
        ("query.checkBlocks", q.clone()),
        ("query.getStats", String::new()),
    ] {
        let r = v.get(m, &q).await?;
        validate(c, ctx, m, &r, 200).await;
    }
    keeper.abort();
    drop(server);
    Ok(())
}

// -------------------------------------------------------------- cloudflare

async fn phase_cloudflare(c: &mut Checks, ctx: &Ctx, pg: &Pg) -> Result<(), String> {
    c.section("6b. Cloudflare-share warning (§9.3)");
    // A /29 inside a published Cloudflare range, on a private Docker
    // network: requests from a container on it reach the server from a
    // Cloudflare-range peer that is not trusted.
    let net = "farsight-stage3-cf";
    let _ = support::run(std::process::Command::new("docker").args(["network", "rm", net]));
    support::run(std::process::Command::new("docker").args([
        "network",
        "create",
        "--subnet",
        "197.234.243.248/29",
        "--gateway",
        "197.234.243.249",
        net,
    ]))?;
    let result = async {
        let port = free_port()?;
        let mport = free_port()?;
        let cfg = config_toml(
            &pg.url("farsight_cov"),
            &ctx.admin,
            "ws://127.0.0.1:9",
            "public",
            &format!("127.0.0.1:{mport}"),
            "",
        );
        let server = Server::start(
            "cloudflare",
            Some(&cfg),
            &format!("197.234.243.249:{port}"),
            &[],
        )?;
        server.wait_live(&ctx.http, Duration::from_secs(60)).await?;
        let url = format!("http://197.234.243.249:{port}/xrpc/{NS}.query.getStats");
        support::run(std::process::Command::new("docker").args([
            "run",
            "--rm",
            "--network",
            net,
            "busybox",
            "sh",
            "-c",
            &format!("for i in $(seq 1 30); do wget -q -O /dev/null {url} || true; done"),
        ]))?;
        let r = ctx.http.get(&format!("{}/", server.base), &[]).await?;
        c.check(
            "> 50% of 5-minute requests from untrusted Cloudflare peers ⇒ dashboard warning",
            r.text.contains("came from Cloudflare edges"),
            support::truncate(
                r.text
                    .split("banner bad\">")
                    .nth(1)
                    .unwrap_or("no warning banner"),
                200,
            ),
        );
        Ok::<(), String>(())
    }
    .await;
    let _ = support::run(std::process::Command::new("docker").args(["network", "rm", net]));
    result
}

// -------------------------------------------------------------------- main

async fn phase_live(c: &mut Checks, ctx: &Ctx, pg: &Pg) -> Result<(), String> {
    c.section("live server (ingest on the public Jetstream)");
    pg.create_db("farsight_live").await?;
    let dsn = pg.url("farsight_live");
    let port = free_port()?;
    let mport = free_port()?;
    let concurrency = 4usize;
    let cfg = config_toml(
        &dsn,
        &ctx.admin,
        "wss://jetstream2.us-east.bsky.network",
        "public",
        &format!("127.0.0.1:{mport}"),
        &format!(
            "\n[proxy]\nmode = \"cloudflare\"\ntrusted = [\"127.0.0.1/32\"]\n\n[rate_limit]\nquery_concurrency = {concurrency}\nquery_timeout = \"3s\"\n"
        ),
    );
    let server = Server::start("live", Some(&cfg), &format!("127.0.0.1:{port}"), &[])?;
    server.wait_live(&ctx.http, Duration::from_secs(90)).await?;
    let start = Instant::now();
    let mut healthy = None;
    while start.elapsed() < Duration::from_secs(90) {
        let h = ctx
            .http
            .get(&format!("{}/health", server.base), &[])
            .await?;
        if h.status == 200 {
            healthy = Some(h);
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    c.check(
        "server up from a canned config; /health 200 once the firehose is connected",
        healthy.is_some(),
        healthy.map_or_else(|| server.log_tail(10), |h| h.short()),
    );
    let pool = pg.pool("farsight_live", 4).await?;
    let p = seed::pagination(&pool).await?;
    seed::notify(&pool).await?;
    let (_, key_read) = create_key(ctx, c, &server.base, "read", &["read"]).await?;
    let (bf_id, key_bf) = create_key(ctx, c, &server.base, "bf", &["read", "backfill"]).await?;
    let (bfh_id, key_bfh) = create_key(
        ctx,
        c,
        &server.base,
        "bfh",
        &["read", "backfill", "backfill:high"],
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    check_lexicons(c, ctx, &server.base, &p, &key_read).await?;
    check_pagination(c, ctx, &server.base, &pool, &p).await?;
    check_backfill(
        c,
        ctx,
        &server.base,
        &pool,
        (bf_id, &key_bf),
        (bfh_id, &key_bfh),
    )
    .await?;
    check_semaphore(c, ctx, &server.base, concurrency).await?;
    check_rate_limits(c, ctx, &server.base, &p, &key_read, &key_bf).await?;
    check_client_ip(c, ctx, &server.base, &p).await?;
    check_metrics(c, ctx, &format!("http://127.0.0.1:{mport}")).await?;
    let events: i64 = sql_i64(&pool, "SELECT count(*) FROM firehose_clock")
        .await
        .unwrap_or(0);
    c.check(
        "ingest committed batches during the run (live Jetstream)",
        events > 0,
        format!("{events} firehose_clock rows"),
    );
    check_reset(c, ctx, &server, &pool, &p, &dsn).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let keep = std::env::args().any(|a| a == "--keep");
    let only: Option<String> = std::env::args().skip_while(|a| a != "--only").nth(1);
    println!("== farsight stage-3 harness: Mode A (API conformance)");
    let pg = match Pg::start(keep) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("postgres: {e}");
            return std::process::ExitCode::from(2);
        }
    };
    let mut c = Checks::default();
    let ctx = Ctx {
        lex: Lexicons::load(),
        http: Http::new(None),
        admin: farsight_api::auth::generate(farsight_api::auth::ADMIN_PREFIX),
    };
    let result = async {
        pg.wait_ready(Duration::from_secs(60)).await?;
        let metrics_port = free_port()?;
        if only.as_deref().is_none_or(|o| o == "live") {
            phase_live(&mut c, &ctx, &pg).await?;
        }
        if only.as_deref().is_none_or(|o| o == "coverage") {
            phase_coverage(&mut c, &ctx, &pg, metrics_port).await?;
        }
        if only.as_deref().is_none_or(|o| o == "cloudflare") {
            if only.is_none() {
                // The coverage phase created the database this phase reuses.
            } else {
                pg.create_db("farsight_cov").await?;
            }
            phase_cloudflare(&mut c, &ctx, &pg).await?;
        }
        Ok::<(), String>(())
    }
    .await;
    if let Err(e) = &result {
        c.check("harness ran to completion", false, e.clone());
    }
    pg.stop();
    let (p, f, u) = c.counts();
    println!(
        "RESULT: {} ({p} passed, {f} failed, {u} unverified)",
        if f == 0 { "PASS" } else { "FAIL" }
    );
    if f == 0 {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::from(1)
    }
}
