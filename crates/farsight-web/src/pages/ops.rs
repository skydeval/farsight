//! The operations page and its actions.

use std::collections::HashMap;
use std::sync::Arc;

use askama::Template;
use axum::extract::{Form, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use farsight_api::admin as api_admin;
use farsight_core::Did;
use farsight_storage::backfill_api::{self, Request, RequestOutcome, Requester};
use farsight_storage::codes::RequesterKey;
use farsight_storage::ids::CycleId;
use farsight_storage::keys::Limits;

use super::dashboard::repair_under_way;
use super::{
    Admin, HandleError, Nav, Return, WebState, check_form, gate, handle_to_did, nav, step_up,
};
use crate::common::render_private;

/// The operations page.
#[derive(Template)]
#[template(path = "ops.html")]
pub struct OpsPage {
    /// Navigation.
    pub nav: Nav,
    /// The session's form token, posted back as the hidden `csrf` field
    /// of every form on the page.
    pub csrf: String,
    /// What the action just posted did, in a green banner; `None` on a
    /// plain view.
    pub notice: Option<String>,
    /// Why the action just posted failed, in a red banner.
    pub error: Option<String>,
    /// A newly created API key, shown once.
    pub new_key: Option<String>,
    /// Recent errors: (when, component, did, message).
    pub errors: Vec<(String, String, String, String)>,
    /// API keys: (id, name, scopes, created, last used, revoked).
    pub keys: Vec<(i32, String, String, String, String, String)>,
    /// `backfill.sweep.enabled`: decides whether the sweep's button
    /// pauses or resumes.
    pub sweep_enabled: bool,
    /// The repair cycle under way: (id, accounts re-read so far).
    pub repair: Option<(CycleId, String)>,
    /// Repairs are held (`backfill.repair.paused`).
    pub repair_paused: bool,
    /// Repairs start by themselves (`backfill.repair.auto_start`).
    pub repair_auto: bool,
    /// Config is env-managed (the toggles are unavailable).
    pub env_managed: bool,
}

async fn ops_render(
    st: &WebState,
    s: &Admin,
    notice: Option<String>,
    error: Option<String>,
    new_key: Option<String>,
) -> Response {
    let cfg = st.api.config.current();
    let mut page = OpsPage {
        nav: nav(&Some(s.clone())),
        csrf: s.csrf.clone(),
        notice,
        error,
        new_key,
        errors: Vec::new(),
        keys: Vec::new(),
        sweep_enabled: cfg.config.backfill.sweep.enabled,
        repair: repair_under_way(st)
            .await
            .map(|(id, done)| (id, crate::public::text::thousands(done))),
        repair_paused: cfg.config.backfill.repair.paused,
        repair_auto: cfg.config.backfill.repair.auto_start,
        env_managed: cfg.from_env_only,
    };
    if let Ok(mut conn) = st.api.pool.acquire().await
        && let Ok(rows) = farsight_storage::queries::op_errors(&mut conn, None, 25).await
    {
        for (_, at, component, did, _, msg) in rows {
            page.errors.push((
                at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                component,
                did.unwrap_or_default(),
                msg,
            ));
        }
    }
    if let Ok(keys) = farsight_storage::auth::list_tokens(&st.api.pool).await {
        for k in keys {
            page.keys.push((
                k.id,
                k.name,
                k.scopes.join(", "),
                k.created_at.format("%Y-%m-%d %H:%M UTC").to_string(),
                k.last_used_at
                    .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
                    .unwrap_or_else(|| "never".into()),
                k.revoked_at
                    .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
                    .unwrap_or_default(),
            ));
        }
    }
    render_private(&page)
}

pub(super) async fn ops_page(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    match gate(&st, &headers).await {
        Ok(s) => ops_render(&st, &s, None, None, None).await,
        Err(r) => r,
    }
}

pub(super) async fn ops_action(
    State(st): State<Arc<WebState>>,
    axum::extract::Path(action): axum::extract::Path<String>,
    headers: HeaderMap,
    Form(form): Form<Vec<(String, String)>>,
) -> Response {
    let s = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    let map: HashMap<String, String> = form.iter().cloned().collect();
    if let Err(r) = check_form(&s, &headers, &map) {
        return r;
    }
    let get = |k: &str| map.get(k).map(|v| v.trim().to_owned()).unwrap_or_default();
    let result: Result<(String, Option<String>), String> = match action.as_str() {
        "backfill" => {
            let q = get("actor");
            let did = if q.starts_with("did:") {
                Did::parse(&q).map_err(HandleError::from)
            } else {
                handle_to_did(&st.safe, &q).await
            };
            match did {
                Err(e) => Err(e.to_string()),
                Ok(did) => {
                    let cfg = st.api.config.current();
                    let source = cfg.config.backfill.backlinks.url.clone();
                    let requester = Requester {
                        key: RequesterKey::Admin,
                        may_high: true,
                    };
                    let req = Request {
                        actor: &did,
                        high: get("priority") == "high",
                        force: map.contains_key("force"),
                        requester: &requester,
                        fresh_window: cfg.config.backfill.request_fresh_window.get(),
                        discovery_source: (!source.is_empty()).then_some(source.as_str()),
                    };
                    match backfill_api::request(
                        &st.api.pool,
                        &Limits::from_config(&cfg.config),
                        &st.api.counters,
                        st.api.gates.load(),
                        &req,
                    )
                    .await
                    {
                        Ok(RequestOutcome::Ok { enqueued, .. }) => Ok((
                            if enqueued {
                                format!("Backfill of {} queued.", did.as_str())
                            } else {
                                format!(
                                    "No new work for {} (already queued, running or fresh; use force).",
                                    did.as_str()
                                )
                            },
                            None,
                        )),
                        Ok(RequestOutcome::QueueFull) => Err("The admin queue is full.".into()),
                        Ok(RequestOutcome::InternRefused) => {
                            Err("The admin's daily intern rate is exhausted.".into())
                        }
                        Err(e) => Err(e.to_string()),
                    }
                }
            }
        }
        "restart-firehose" => match api_admin::restart_firehose(&st.api).await {
            Ok(_) => Ok(("Firehose session restarted.".into(), None)),
            Err(e) => Err(e.message),
        },
        "sweep" => {
            let paused = get("paused") == "true";
            match api_admin::set_sweep_paused(&st.api, paused).await {
                Ok(()) => Ok((
                    if paused {
                        "Sweep paused."
                    } else {
                        "Sweep resumed."
                    }
                    .into(),
                    None,
                )),
                Err(e) => Err(e.message),
            }
        }
        "repair-pause" => {
            let paused = get("paused") == "true";
            match api_admin::set_repair_paused(&st.api, paused).await {
                Ok(()) => Ok((
                    if paused {
                        "Repairs paused. A repair under way keeps its place."
                    } else {
                        "Repairs resumed."
                    }
                    .into(),
                    None,
                )),
                Err(e) => Err(e.message),
            }
        }
        "repair-auto" => {
            let on = get("on") == "true";
            match api_admin::set_repair_auto_start(&st.api, on).await {
                Ok(()) => Ok((
                    if on {
                        "Repairs start automatically: a closed gap gets one at the backfill \
                         process's next look."
                    } else {
                        "Repairs no longer start automatically. A closed gap waits for \
                         \"Start repair\"."
                    }
                    .into(),
                    None,
                )),
                Err(e) => Err(e.message),
            }
        }
        "repair-cancel" => match api_admin::cancel_repair_cycle(&st.api).await {
            Ok(Some(id)) => Ok((
                format!(
                    "Repair cycle {id} cancelled. Its gaps are left unrepaired, and automatic \
                     repairs are now off so that it does not start again."
                ),
                None,
            )),
            Ok(None) => Ok((
                "No repair was under way. Automatic repairs are now off.".into(),
                None,
            )),
            Err(e) => Err(e.message),
        },
        "repair" => match (
            repair_under_way(&st).await,
            api_admin::start_repair_cycle(&st.api).await,
        ) {
            (Some((id, done)), Ok(r)) if r.gaps > 0 && r.cycle == Some(id) => Ok((
                format!(
                    "Repair cycle {id} is already running for {} gap(s): {} accounts re-read so \
                     far. Nothing new was started.",
                    r.gaps,
                    crate::public::text::thousands(done)
                ),
                None,
            )),
            (_, r) => match r {
                Ok(r) if r.gaps == 0 => Ok((
                    "Nothing to repair: no gap is closed and not yet healed. A gap that is \
                     still open (a v1 firehose's, or a disconnection in progress) can be \
                     repaired only once it has closed."
                        .into(),
                    None,
                )),
                Ok(r) => Ok((
                    format!(
                        "Repair cycle {} requested for {} gap(s); the backfill process runs it.",
                        r.cycle.map_or(0, CycleId::get),
                        r.gaps
                    ),
                    None,
                )),
                Err(e) => Err(e.message),
            },
        },
        "keys-create" => {
            // A key outlives the session that made it: a fresh sign-in
            // first.
            if let Err(r) = step_up(&s, Return::Ops) {
                return r;
            }
            let scopes: Vec<String> = form
                .iter()
                .filter(|(k, _)| k == "scope")
                .map(|(_, v)| v.clone())
                .collect();
            let rps = get("read_rps");
            let rps = if rps.is_empty() {
                Ok(None)
            } else {
                rps.parse::<u32>()
                    .map(Some)
                    .map_err(|_| "read rps must be a number".to_owned())
            };
            match rps {
                Err(e) => Err(e),
                Ok(rps) => match api_admin::create_key(&st.api, &get("name"), &scopes, rps).await {
                    Ok((id, token)) => Ok((format!("API key {id} created."), Some(token))),
                    Err(e) => Err(e.message),
                },
            }
        }
        "keys-revoke" => {
            // A revoked key stays revoked after the session has ended: a
            // fresh sign-in first, as for creating one.
            if let Err(r) = step_up(&s, Return::Ops) {
                return r;
            }
            revoke(&st, &get("id")).await
        }
        _ => return (StatusCode::NOT_FOUND, "not found").into_response(),
    };
    match result {
        Ok((notice, key)) => ops_render(&st, &s, Some(notice), None, key).await,
        Err(e) => ops_render(&st, &s, None, Some(e), None).await,
    }
}

/// Revokes the API key with the id written as `id`.
async fn revoke(st: &WebState, id: &str) -> Result<(String, Option<String>), String> {
    match id.parse::<i32>() {
        Ok(id) => match api_admin::revoke_key(&st.api, id).await {
            Ok(true) => Ok((format!("API key {id} revoked."), None)),
            Ok(false) => Err(format!("No live key {id}.")),
            Err(e) => Err(e.message),
        },
        Err(_) => Err("bad key id".into()),
    }
}
