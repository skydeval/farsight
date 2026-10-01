//! Backfill endpoints (design §3.3) and the unstable admin procedures
//! (§3.4).

use std::sync::Arc;

use axum::body::Bytes;
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use farsight_core::Did;
use farsight_ingest::Control;
use farsight_storage::backfill_api::{self, BackfillStatus, Request, RequestOutcome, Requester};
use farsight_storage::keys::Limits;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::auth::{self, Caller, KEY_PREFIX, SCOPE_BACKFILL_HIGH, SCOPES};
use crate::config_store::{self, EditError};
use crate::error::XrpcError;
use crate::freshness::ts;
use crate::params::{Params, parse_did};
use crate::{ApiState, Reply};

fn status_json(actor: &Did, s: &BackfillStatus) -> Map<String, Value> {
    let mut repo = Map::new();
    repo.insert("state".into(), json!(s.repo.state.api_name()));
    if let Some(p) = s.repo.position {
        repo.insert("position".into(), json!(p));
    }
    if let Some(t) = s.repo.last_backfilled_at {
        repo.insert("lastBackfilledAt".into(), json!(ts(t)));
    }
    if let Some(e) = &s.repo.last_error {
        repo.insert("lastError".into(), json!(e));
    }
    let mut disc = Map::new();
    disc.insert("state".into(), json!(s.discovery.state.api_name()));
    if let Some(t) = s.discovery.completed_at {
        disc.insert("completedAt".into(), json!(ts(t)));
    }
    disc.insert("truncated".into(), json!(s.discovery.truncated));
    if let Some(src) = &s.discovery.source {
        disc.insert("source".into(), json!(src));
    }
    let mut m = Map::new();
    m.insert("actor".into(), json!(actor.as_str()));
    m.insert("repo".into(), Value::Object(repo));
    m.insert("discovery".into(), Value::Object(disc));
    m
}

/// `query.getBackfillStatus`: a pure read (§3.3).
pub async fn get_backfill_status(st: &Arc<ApiState>, p: &Params) -> Result<Reply, XrpcError> {
    let actor = p.did("actor")?;
    let discovery = !st.config.current().config.backfill.backlinks.url.is_empty();
    let mut tx = st.read_tx().await?;
    let s = backfill_api::status(&mut tx, actor.as_str(), discovery).await?;
    tx.rollback().await?;
    Ok(Reply::ok(Value::Object(status_json(&actor, &s))))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackfillInput {
    actor: String,
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    force: bool,
}

fn parse_body<T: for<'de> Deserialize<'de>>(body: &Bytes) -> Result<T, XrpcError> {
    serde_json::from_slice(body).map_err(|e| XrpcError::invalid(format!("invalid input: {e}")))
}

/// `admin.requestBackfill` (§3.3). JSON body, or the query-parameter form
/// (D1).
pub async fn request_backfill(
    st: &Arc<ApiState>,
    caller: &Caller,
    p: &Params,
    body: &Bytes,
) -> Result<Reply, XrpcError> {
    let input = if body.iter().all(u8::is_ascii_whitespace) {
        BackfillInput {
            actor: p
                .get("actor")
                .ok_or_else(|| XrpcError::invalid("missing required field `actor`"))?
                .to_owned(),
            priority: p.get("priority").map(str::to_owned),
            force: p.bool("force")?,
        }
    } else {
        parse_body(body)?
    };
    let actor = parse_did("actor", &input.actor)?;
    let high = match input.priority.as_deref() {
        None | Some("normal") => false,
        Some("high") => true,
        Some(o) => return Err(XrpcError::invalid(format!("unknown priority {o:?}"))),
    };
    let requester = Requester {
        key: caller
            .requester()
            .ok_or_else(|| XrpcError::auth_required("token required"))?,
        may_high: match caller {
            Caller::Admin => true,
            Caller::Key(k) => k.has(SCOPE_BACKFILL_HIGH),
            Caller::Anonymous => false,
        },
    };
    let cfg = st.config.current();
    let source = cfg.config.backfill.backlinks.url.clone();
    let req = Request {
        actor: &actor,
        high,
        force: input.force,
        requester: &requester,
        fresh_window: cfg.config.backfill.request_fresh_window.get(),
        discovery_source: (!source.is_empty()).then_some(source.as_str()),
    };
    let limits = Limits::from_config(&cfg.config);
    let outcome =
        backfill_api::request(&st.pool, &limits, &st.counters, st.gates.load(), &req).await?;
    let (enqueued, downgraded) = match outcome {
        RequestOutcome::Ok {
            enqueued,
            downgraded,
        } => (enqueued, downgraded),
        RequestOutcome::QueueFull => {
            return Err(XrpcError::queue_full(format!(
                "requester already holds {} waiting entries",
                backfill_api::REQUESTER_QUEUE_CAP
            )));
        }
        RequestOutcome::InternRefused => {
            return Err(XrpcError::queue_full(
                "the requester's daily intern rate is exhausted",
            ));
        }
    };
    let mut conn = st.pool.acquire().await?;
    let s = backfill_api::status(&mut conn, actor.as_str(), !source.is_empty()).await?;
    let mut m = status_json(&actor, &s);
    m.insert("enqueued".into(), json!(enqueued));
    m.insert("downgraded".into(), json!(downgraded));
    Ok(Reply {
        status: StatusCode::ACCEPTED,
        body: Value::Object(m),
    })
}

/// `admin.listErrors`.
pub async fn list_errors(st: &Arc<ApiState>, p: &Params) -> Result<Reply, XrpcError> {
    let limit = p.limit()?;
    let before = crate::cursor::id(p.get("cursor"))?;
    let mut tx = st.read_tx().await?;
    let rows = farsight_storage::queries::op_errors(&mut tx, before, limit).await?;
    tx.rollback().await?;
    let errors: Vec<Value> = rows
        .iter()
        .map(|(id, at, component, did, host, message)| {
            let mut m = Map::new();
            m.insert("id".into(), json!(id));
            m.insert("at".into(), json!(ts(*at)));
            m.insert("component".into(), json!(component));
            if let Some(d) = did {
                m.insert("did".into(), json!(d));
            }
            if let Some(h) = host {
                m.insert("host".into(), json!(h));
            }
            m.insert("message".into(), json!(message));
            Value::Object(m)
        })
        .collect();
    let mut body = json!({ "errors": errors });
    if rows.len() as i64 == limit {
        if let Some(last) = rows.last() {
            body["cursor"] = json!(crate::cursor::encode(&[json!(last.0)]));
        }
    }
    Ok(Reply::ok(body))
}

/// `admin.restartFirehose`: drops the session; the reader reconnects from
/// the persisted cursor (§6.3).
pub async fn restart_firehose(st: &Arc<ApiState>) -> Result<Reply, XrpcError> {
    let Some(i) = &st.ingest else {
        return Err(XrpcError::invalid("ingest is not running"));
    };
    let ok = i.control.send(Control::KillSocket).await.is_ok();
    Ok(Reply::ok(json!({ "restarted": ok })))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PauseInput {
    paused: bool,
}

fn edit_error(e: EditError) -> XrpcError {
    match e {
        EditError::Io(m) => XrpcError::internal(m),
        other => XrpcError::invalid(other.to_string()),
    }
}

/// Sets `backfill.sweep.enabled` and notifies the backfill process.
pub async fn set_sweep_paused(st: &ApiState, paused: bool) -> Result<(), XrpcError> {
    st.config
        .edit(|t| {
            let backfill = t
                .entry("backfill")
                .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                .as_table_mut()
                .ok_or("`backfill` is not a table")?;
            let sweep = backfill
                .entry("sweep")
                .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                .as_table_mut()
                .ok_or("`backfill.sweep` is not a table")?;
            sweep.insert("enabled".into(), toml::Value::Boolean(!paused));
            Ok(())
        })
        .await
        .map_err(edit_error)?;
    config_store::notify_config(&st.pool).await?;
    Ok(())
}

/// `admin.pauseSweep`.
pub async fn pause_sweep(st: &Arc<ApiState>, body: &Bytes) -> Result<Reply, XrpcError> {
    let input: PauseInput = parse_body(body)?;
    set_sweep_paused(st, input.paused).await?;
    Ok(Reply::ok(json!({ "paused": input.paused })))
}

/// What a repair request did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairStart {
    /// The repair cycle (new, or the one already pending).
    pub cycle: Option<i64>,
    /// Its `repair_from`.
    pub from: Option<DateTime<Utc>>,
    /// Closed, unhealed gaps it covers.
    pub gaps: i64,
}

/// Requests a repair cycle over every closed, unhealed gap (§7.5: one
/// repair covers all of them from `min(from_at)`; an open gap waits). An
/// unfinished repair cycle is reused. The backfill process runs it.
pub async fn start_repair_cycle(st: &ApiState) -> Result<RepairStart, XrpcError> {
    let mut tx = st.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('farsight:start_repair'))")
        .execute(&mut *tx)
        .await?;
    let (gaps, from): (i64, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT count(*), min(from_at) FROM firehose_gaps
         WHERE healed_at IS NULL AND to_at IS NOT NULL",
    )
    .fetch_one(&mut *tx)
    .await?;
    if gaps == 0 {
        tx.rollback().await?;
        return Ok(RepairStart {
            cycle: None,
            from: None,
            gaps: 0,
        });
    }
    let existing: Option<(i64, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT id, repair_from FROM sweep_cycles WHERE kind = 2 AND completed_at IS NULL
         ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(&mut *tx)
    .await?;
    let (cycle, from) = match existing {
        Some((id, f)) => (id, f),
        None => {
            let source = serde_json::to_value(st.config.current().config.backfill.sweep.source)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_else(|| "relay_collections".to_owned());
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO sweep_cycles (kind, source, collections, started_at, repair_from)
                 VALUES (2, $1, '{1,2,3,4}', now(), $2) RETURNING id",
            )
            .bind(source)
            .bind(from)
            .fetch_one(&mut *tx)
            .await?;
            (id, from)
        }
    };
    sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(RepairStart {
        cycle: Some(cycle),
        from,
        gaps,
    })
}

/// `admin.startRepair`.
pub async fn start_repair(st: &Arc<ApiState>) -> Result<Reply, XrpcError> {
    let r = start_repair_cycle(st).await?;
    let mut m = Map::new();
    if let Some(c) = r.cycle {
        m.insert("cycle".into(), json!(c));
    }
    if let Some(f) = r.from {
        m.insert("from".into(), json!(ts(f)));
    }
    m.insert("gaps".into(), json!(r.gaps));
    Ok(Reply::ok(Value::Object(m)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CreateKeyInput {
    name: String,
    scopes: Vec<String>,
    #[serde(default)]
    read_rps: Option<u32>,
}

/// Creates an API key; returns `(id, token)`. The token is shown once.
pub async fn create_key(
    st: &ApiState,
    name: &str,
    scopes: &[String],
    read_rps: Option<u32>,
) -> Result<(i32, String), XrpcError> {
    let name = name.trim();
    if name.is_empty() || name.len() > 200 {
        return Err(XrpcError::invalid("`name` must be 1–200 characters"));
    }
    if scopes.is_empty() {
        return Err(XrpcError::invalid("at least one scope is required"));
    }
    if let Some(s) = scopes.iter().find(|s| !SCOPES.contains(&s.as_str())) {
        return Err(XrpcError::invalid(format!("unknown scope {s:?}")));
    }
    if read_rps == Some(0) {
        return Err(XrpcError::invalid("`readRps` must be positive"));
    }
    let token = auth::generate(KEY_PREFIX);
    let id = farsight_storage::auth::create_token(
        &st.pool,
        name,
        &auth::sha256(&token),
        scopes,
        read_rps.map(|r| r as f32),
    )
    .await?;
    st.keys.refresh(&st.pool).await?;
    Ok((id, token))
}

/// `admin.createApiKey`.
pub async fn create_api_key(st: &Arc<ApiState>, body: &Bytes) -> Result<Reply, XrpcError> {
    let input: CreateKeyInput = parse_body(body)?;
    let (id, token) = create_key(st, &input.name, &input.scopes, input.read_rps).await?;
    Ok(Reply::ok(json!({ "id": id, "token": token })))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeInput {
    id: i32,
}

/// `admin.revokeApiKey`.
pub async fn revoke_api_key(st: &Arc<ApiState>, body: &Bytes) -> Result<Reply, XrpcError> {
    let input: RevokeInput = parse_body(body)?;
    let revoked = farsight_storage::auth::revoke_token(&st.pool, input.id).await?;
    st.keys.refresh(&st.pool).await?;
    Ok(Reply::ok(json!({ "revoked": revoked })))
}
