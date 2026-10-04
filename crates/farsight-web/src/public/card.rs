//! Profile cards (design §3.6, §8.6): `GET /card/{did}`, the HTML
//! fragment the script shows when the pointer rests on an account in a
//! row, or the link takes keyboard focus.
//!
//! A card shows what the network publishes about the account and nothing
//! Farsight stores: the avatar, the verified handle, the DID and when the
//! DID was created. In order:
//!
//! 1. the per-address class (`public_ui_card`); over it: 429;
//! 2. one status lookup: an account this instance does not hold, or does
//!    not show, gets the same 404 and nothing is fetched for it;
//! 3. the process-wide budget (`public_ui.card_rps` / `card_burst`): with
//!    none left the *short card* is returned and nothing is fetched;
//! 4. the fetches, through the safe client (§11.3), with one deadline for
//!    the whole card and no retry: the PLC audit log (or the did:web
//!    document), then the handle's forward resolution and the profile
//!    record side by side;
//! 5. the fragment. A part whose fetch failed is left out.
//!
//! **The avatar is never fetched by the server.** The fragment names the
//! blob on the account's own PDS and the visitor's browser fetches it
//! there, so the image is emitted only for an `https` endpoint the safe
//! client has just read the profile record from, and only while
//! `public_ui.show_avatars` is on.
//!
//! Nothing but the handle cache is stored.
//!
//! `GET /admin/card/{did}` serves the same fragment to a signed-in admin
//! from the admin tables ([`admin_route`]). It differs in three ways:
//! without a valid session it is the bare 404 of an unknown path in every
//! `ui` mode — never a redirect, because the caller is a script and a
//! redirect would put the sign-in page into the card; the withheld rule
//! is not applied (the operator's tables show those accounts); and every
//! answer is `no-store, private`. It draws on the same per-address class
//! and the same process-wide budget as the public cards.

use std::sync::Arc;
use std::time::{Duration, Instant};

use askama::Template;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use farsight_api::clientip::ClientIp;
use farsight_api::ratelimit::Class;
use farsight_core::config::{AdminAuth, Config};
use farsight_core::net::{OutboundClient, SafeClient};
use farsight_core::{Did, DidMethod};
use farsight_storage::handles::Cached;
use farsight_storage::queries;
use serde_json::Value;
use url::Url;

use super::handles::{self, claimed_handle};
use super::metrics::{self as m, Page};
use super::text::{Stamp, clean};
use super::{Cache, client_key, finish, parse_did};
use crate::pages::{WebState, resolve_handle};

/// The deadline of all of a card's fetches together.
pub const CARD_DEADLINE: Duration = Duration::from_secs(3);
/// Bucket key of the process-wide card budget.
pub const BUDGET_KEY: &str = "process";
/// How long a complete card may be reused by a browser or an edge cache.
pub const FULL_CARD_MAX_AGE: u32 = 300;
/// Longest avatar CID accepted, in characters.
pub const MAX_CID: usize = 128;

/// How a card request ended (`farsight_public_ui_cards_total`). One per
/// request that reached the card: the first that applies, in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Over the per-address class, or no process budget left: nothing was
    /// fetched.
    RateLimited,
    /// The PLC audit log could not be read in time: no creation date.
    PlcTimeout,
    /// Another fetch failed or timed out (the did:web document, the
    /// handle's resolution, the profile record): that part is left out.
    PdsFailed,
    /// Complete, without an image because `show_avatars` is off.
    AvatarsDisabled,
    /// Complete.
    Served,
}

impl Outcome {
    /// Every outcome.
    pub const ALL: [Outcome; 5] = [
        Outcome::Served,
        Outcome::RateLimited,
        Outcome::PlcTimeout,
        Outcome::PdsFailed,
        Outcome::AvatarsDisabled,
    ];

    /// Metric label.
    pub fn label(self) -> &'static str {
        match self {
            Outcome::Served => "served",
            Outcome::RateLimited => "rate_limited",
            Outcome::PlcTimeout => "plc_timeout",
            Outcome::PdsFailed => "pds_failed",
            Outcome::AvatarsDisabled => "avatars_disabled",
        }
    }
}

/// The fragment.
#[derive(Debug, Template)]
#[template(path = "public_card.html")]
struct CardView {
    did: String,
    handle: Option<String>,
    /// The blob on the account's PDS.
    avatar: Option<String>,
    /// A neutral circle where the image would be, so that cards with and
    /// without one have the same shape.
    placeholder: bool,
    /// The "DID created" line is shown.
    created_line: bool,
    created: Option<Stamp>,
    /// `unknown` (did:web) or `unavailable` (the fetch failed).
    created_words: &'static str,
    note: Option<&'static str>,
    /// The host of the account's PDS, as its identity names it. Not
    /// printed in the card: the account page's header reads it.
    pds_host: Option<String>,
}

/// What the identity fetch says about an account.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Identity {
    /// `createdAt` of the first entry of the PLC audit log.
    pub created: Option<DateTime<Utc>>,
    /// The handle the account claims.
    pub claim: Option<String>,
    /// Its PDS endpoint, as written.
    pub pds: Option<String>,
}

/// Reads a PLC audit log: the creation time from the first entry, and
/// the handle claim and PDS endpoint from the operation of the last entry
/// that is not nullified. `None` when the log cannot be read or that
/// operation is a tombstone.
pub fn read_audit_log(body: &[u8]) -> Option<Identity> {
    let log: Value = serde_json::from_slice(body).ok()?;
    let entries = log.as_array()?;
    let created = entries
        .first()?
        .get("createdAt")?
        .as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc))?;
    let op = entries
        .iter()
        .rev()
        .find(|e| e["nullified"].as_bool() != Some(true))?
        .get("operation")?;
    let (claim, pds) = match op["type"].as_str()? {
        "plc_tombstone" => return None,
        // The legacy operation carries a bare handle and a service URL.
        "create" => (
            op["handle"]
                .as_str()
                .map(str::to_ascii_lowercase)
                .filter(|h| farsight_core::did::is_valid_hostname(h)),
            op["service"].as_str().map(str::to_owned),
        ),
        _ => (
            claimed_handle(op),
            op["services"]["atproto_pds"]["endpoint"]
                .as_str()
                .map(str::to_owned),
        ),
    };
    Some(Identity {
        created: Some(created),
        claim,
        pds,
    })
}

/// Reads a did:web document. There is no creation time to read.
pub fn read_did_document(body: &[u8]) -> Option<Identity> {
    let doc: Value = serde_json::from_slice(body).ok()?;
    let pds = doc["service"].as_array().and_then(|s| {
        s.iter()
            .find(|s| {
                s["id"]
                    .as_str()
                    .is_some_and(|id| id.ends_with("#atproto_pds"))
            })
            .and_then(|s| s["serviceEndpoint"].as_str())
            .map(str::to_owned)
    });
    Some(Identity {
        created: None,
        claim: claimed_handle(&doc),
        pds,
    })
}

/// The PDS endpoint as a base an image may be named on: an `https` URL
/// with a host and no userinfo, query or fragment.
pub fn avatar_base(endpoint: &str) -> Option<Url> {
    let u = Url::parse(endpoint).ok()?;
    (u.scheme() == "https"
        && u.host_str().is_some_and(|h| !h.is_empty())
        && u.username().is_empty()
        && u.password().is_none()
        && u.query().is_none()
        && u.fragment().is_none())
    .then_some(u)
}

/// Whether `s` has the shape of a CID as records write it: base32
/// multibase, at most [`MAX_CID`] characters.
pub fn is_cid(s: &str) -> bool {
    (8..=MAX_CID).contains(&s.len())
        && s.starts_with('b')
        && s.bytes().all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7'))
}

fn xrpc(base: &Url, method: &str, pairs: &[(&str, &str)]) -> Option<Url> {
    let mut u = Url::parse(&format!(
        "{}/xrpc/{method}",
        base.as_str().trim_end_matches('/')
    ))
    .ok()?;
    u.query_pairs_mut().extend_pairs(pairs);
    Some(u)
}

/// The avatar CID of a `getRecord` response for the profile record.
pub fn avatar_cid(body: &[u8]) -> Option<String> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let cid = v["value"]["avatar"]["ref"]["$link"].as_str()?;
    is_cid(cid).then(|| cid.to_owned())
}

/// The image URL of a card, or why there is none. `Err`: the fetch
/// failed. `Ok(None)`: the account has no avatar, or its endpoint is not
/// one an image may be named on.
async fn avatar(
    safe: &SafeClient,
    did: &Did,
    endpoint: Option<&str>,
    deadline: tokio::time::Instant,
) -> Result<Option<String>, ()> {
    let Some(base) = endpoint.and_then(avatar_base) else {
        return Ok(None);
    };
    let Some(url) = xrpc(
        &base,
        "com.atproto.repo.getRecord",
        &[
            ("repo", did.as_str()),
            ("collection", "app.bsky.actor.profile"),
            ("rkey", "self"),
        ],
    ) else {
        return Ok(None);
    };
    let r = match tokio::time::timeout_at(deadline, safe.get(&url)).await {
        Ok(Ok(r)) => r,
        _ => return Err(()),
    };
    match r.status {
        200 => {}
        // No profile record: a definite absence, not a failure.
        400 | 404 => return Ok(None),
        _ => return Err(()),
    }
    // The image is named on the host the record was read from, so its
    // address passed the safe client's checks a moment ago. A redirect to
    // another origin gives no image.
    if r.final_url.origin() != base.origin() {
        return Ok(None);
    }
    let Some(cid) = avatar_cid(&r.body) else {
        return Ok(None);
    };
    Ok(xrpc(
        &base,
        "com.atproto.sync.getBlob",
        &[("did", did.as_str()), ("cid", cid.as_str())],
    )
    .map(String::from))
}

/// The handle of a card: the cached answer, or the account's claim
/// verified forward and cached like a page's. `Err`: the resolution did
/// not finish in time; nothing is cached.
async fn handle(
    st: &WebState,
    cfg: &Config,
    did: &Did,
    claim: Option<&str>,
    deadline: tokio::time::Instant,
) -> Result<Option<String>, ()> {
    if let Some(c) = handles::cached(st, cfg, did).await {
        m::handle_resolution(handles::Outcome::Cached);
        return Ok(match c {
            Cached::Handle(h) => Some(h),
            Cached::None => None,
        });
    }
    let Some(claim) = claim else {
        m::handle_resolution(handles::Outcome::Failed);
        return Ok(handles::settle(st, cfg, did, None).await);
    };
    match tokio::time::timeout_at(deadline, resolve_handle(&st.safe, claim)).await {
        Ok(Ok(back)) if back == *did => {
            m::handle_resolution(handles::Outcome::Resolved);
            Ok(handles::settle(st, cfg, did, Some(claim.to_owned())).await)
        }
        Ok(_) => {
            m::handle_resolution(handles::Outcome::Unverified);
            Ok(handles::settle(st, cfg, did, None).await)
        }
        Err(_) => {
            m::handle_resolution(handles::Outcome::Failed);
            Err(())
        }
    }
}

async fn identity(
    safe: &SafeClient,
    cfg: &Config,
    did: &Did,
    deadline: tokio::time::Instant,
) -> Option<Identity> {
    let url = match did.method() {
        DidMethod::Plc => Url::parse(&format!(
            "{}/{}/log/audit",
            cfg.backfill.plc_url.trim_end_matches('/'),
            did.as_str()
        ))
        .ok()?,
        DidMethod::Web => handles::document_url(cfg, did)?,
    };
    let r = tokio::time::timeout_at(deadline, safe.get(&url))
        .await
        .ok()?
        .ok()?;
    if r.status != 200 {
        return None;
    }
    match did.method() {
        DidMethod::Plc => read_audit_log(&r.body),
        DidMethod::Web => read_did_document(&r.body),
    }
}

/// Who a card is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum For {
    /// Anyone: `/card/{did}`.
    Public,
    /// A signed-in admin: `/admin/card/{did}`.
    Admin,
}

impl For {
    /// The cache class of an answer that would be `cache` on the public
    /// route: nothing made for an admin is ever stored.
    fn cache(self, cache: Cache) -> Cache {
        match self {
            For::Public => cache,
            For::Admin => Cache::Private,
        }
    }
}

fn plain(cfg: &Config, who: For, status: StatusCode, text: &'static str) -> Response {
    finish(
        (status, text).into_response(),
        cfg,
        who.cache(Cache::NoStore),
        false,
    )
}

fn fragment(cfg: &Config, who: For, view: &CardView, cache: Cache) -> Response {
    // A card is a fragment, not a page: never offered to search engines.
    super::page(view, StatusCode::OK, cfg, who.cache(cache), false)
}

/// The short card: the DID, the cached handle if any, and one line.
/// Nothing was fetched for it.
fn short_card(st: &WebState, did: &Did, created_unavailable: bool) -> CardView {
    CardView {
        did: did.to_string(),
        handle: st.public.handles.get(did.as_str()).map(|h| clean(&h)),
        avatar: None,
        placeholder: false,
        created_line: created_unavailable,
        created: None,
        created_words: "unavailable",
        note: Some("Profile not available right now."),
        pds_host: None,
    }
}

async fn card(
    st: &WebState,
    cfg: &Config,
    loaded: &Arc<farsight_core::config::LoadedConfig>,
    who: For,
    client: Option<ClientIp>,
    path: Result<Path<String>, PathRejection>,
) -> Response {
    // 1. The per-address class.
    let limit = Class::PublicCard.limit(cfg, None);
    if let Err((_, retry)) = st
        .api
        .limiter
        .check(Class::PublicCard, &client_key(client), limit)
    {
        farsight_api::metrics::rate_limited(Class::PublicCard);
        m::card(Outcome::RateLimited);
        let mut r = plain(cfg, who, StatusCode::TOO_MANY_REQUESTS, "too many requests");
        r.headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(retry.max(1)));
        return r;
    }
    let Ok(did) = parse_did(path) else {
        return plain(cfg, who, StatusCode::BAD_REQUEST, "not a DID");
    };
    // 2. Is this account shown here? One lookup, in a slot of the global
    // read semaphore like any query; no render slot.
    let busy = || plain(cfg, who, StatusCode::SERVICE_UNAVAILABLE, "busy");
    let shown = {
        let Ok(Ok(_permit)) = tokio::time::timeout(
            farsight_api::PERMIT_WAIT,
            st.api.query_permits.clone().acquire_owned(),
        )
        .await
        else {
            return busy();
        };
        // The operator's tables show withheld accounts and the operator
        // may look at them; an account this instance does not hold gets
        // nothing fetched for it on either route.
        let withheld = match who {
            For::Public => match st.public.withheld(&st.api.pool, loaded).await {
                Ok(w) => Some(w),
                Err(_) => return busy(),
            },
            For::Admin => None,
        };
        let Ok(mut conn) = st.api.pool.acquire().await else {
            return busy();
        };
        match queries::actor(&mut conn, did.as_str()).await {
            Ok(Some(a)) => withheld
                .as_ref()
                .is_none_or(|w| w.reason(did.as_str(), Some(a.status)).is_none()),
            Ok(None) => false,
            Err(_) => return busy(),
        }
    };
    if !shown {
        // Unknown and withheld accounts answer alike.
        return plain(cfg, who, StatusCode::NOT_FOUND, "not found");
    }
    // 3. The process-wide budget.
    let budget = Class::PublicCardBudget.limit(cfg, None);
    if st
        .api
        .limiter
        .check(Class::PublicCardBudget, BUDGET_KEY, budget)
        .is_err()
    {
        m::card(Outcome::RateLimited);
        return fragment(cfg, who, &short_card(st, &did, false), Cache::NoStore);
    }
    // 4. The fetches, under one deadline.
    let deadline = tokio::time::Instant::now() + CARD_DEADLINE;
    let is_plc = did.method() == DidMethod::Plc;
    let Some(ident) = identity(&st.safe, cfg, &did, deadline).await else {
        if is_plc {
            m::card(Outcome::PlcTimeout);
            return fragment(cfg, who, &short_card(st, &did, true), Cache::NoStore);
        }
        // did:web has no creation time to lose; without its document the
        // card has the DID and whatever handle is cached.
        m::card(Outcome::PdsFailed);
        let mut view = short_card(st, &did, true);
        view.created_words = "unknown";
        return fragment(cfg, who, &view, Cache::NoStore);
    };
    let show_avatars = cfg.public_ui.show_avatars;
    let (handle, avatar) = tokio::join!(
        handle(st, cfg, &did, ident.claim.as_deref(), deadline),
        async {
            if show_avatars {
                avatar(&st.safe, &did, ident.pds.as_deref(), deadline).await
            } else {
                Ok(None)
            }
        }
    );
    // 5. The fragment.
    let failed = handle.is_err() || avatar.is_err();
    let view = CardView {
        did: did.to_string(),
        handle: handle.ok().flatten().map(|h| clean(&h)),
        avatar: avatar.ok().flatten(),
        placeholder: show_avatars,
        created_line: true,
        created: ident.created.map(Stamp::of),
        created_words: "unknown",
        note: None,
        pds_host: ident
            .pds
            .as_deref()
            .and_then(|p| Url::parse(p).ok())
            .and_then(|u| u.host_str().map(clean)),
    };
    if failed {
        m::card(Outcome::PdsFailed);
        return fragment(cfg, who, &view, Cache::NoStore);
    }
    m::card(if show_avatars {
        Outcome::Served
    } else {
        Outcome::AvatarsDisabled
    });
    fragment(cfg, who, &view, Cache::Public(FULL_CARD_MAX_AGE))
}

/// `GET /card/{did}`.
pub async fn route(
    State(st): State<Arc<WebState>>,
    client: Option<axum::Extension<ClientIp>>,
    path: Result<Path<String>, PathRejection>,
) -> Response {
    let started = Instant::now();
    let loaded = st.api.config.current();
    let resp = if loaded.config.access.public_ui {
        card(
            &st,
            &loaded.config,
            &loaded,
            For::Public,
            client.map(|c| c.0),
            path,
        )
        .await
    } else {
        crate::common::not_found()
    };
    m::observe(Page::Card, resp.status(), started.elapsed());
    resp
}

/// `GET /admin/card/{did}`: the card for a signed-in admin. It does not go
/// through the admin pages' gate, whose answer to a navigation without a
/// session is a redirect to the sign-in page.
pub async fn admin_route(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    client: Option<axum::Extension<ClientIp>>,
    path: Result<Path<String>, PathRejection>,
) -> Response {
    let loaded = st.api.config.current();
    if loaded.admin_auth() == AdminAuth::Disabled
        || crate::pages::admin(&st, &headers).await.is_none()
    {
        return crate::common::not_found();
    }
    card(
        &st,
        &loaded.config,
        &loaded,
        For::Admin,
        client.map(|c| c.0),
        path,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn log(entries: Value) -> Vec<u8> {
        serde_json::to_vec(&entries).unwrap()
    }

    #[test]
    fn audit_log_gives_creation_claim_and_pds() {
        let body = log(json!([
            {"createdAt": "2023-04-12T04:53:57.057Z", "nullified": false, "operation": {
                "type": "plc_operation", "alsoKnownAs": ["at://old.example"],
                "services": {"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": "https://old.example"}}}},
            {"createdAt": "2024-01-01T00:00:00.000Z", "nullified": false, "operation": {
                "type": "plc_operation", "alsoKnownAs": ["at://Alice.Example"],
                "services": {"atproto_pds": {"endpoint": "https://pds.example"}}}},
            {"createdAt": "2024-02-01T00:00:00.000Z", "nullified": true, "operation": {
                "type": "plc_operation", "alsoKnownAs": ["at://mallory.example"],
                "services": {"atproto_pds": {"endpoint": "https://evil.example"}}}}
        ]));
        let i = read_audit_log(&body).unwrap();
        assert_eq!(
            i.created.unwrap().to_rfc3339(),
            "2023-04-12T04:53:57.057+00:00"
        );
        // The last operation that is not nullified.
        assert_eq!(i.claim.as_deref(), Some("alice.example"));
        assert_eq!(i.pds.as_deref(), Some("https://pds.example"));
    }

    #[test]
    fn legacy_create_and_tombstone() {
        let body = log(json!([
            {"createdAt": "2022-11-17T00:35:16.391Z", "nullified": false, "operation": {
                "type": "create", "handle": "Bob.Example", "service": "https://pds.example",
                "signingKey": "did:key:x", "recoveryKey": "did:key:y", "prev": null}}
        ]));
        let i = read_audit_log(&body).unwrap();
        assert_eq!(i.claim.as_deref(), Some("bob.example"));
        assert_eq!(i.pds.as_deref(), Some("https://pds.example"));
        let dead = log(json!([
            {"createdAt": "2022-11-17T00:35:16.391Z", "nullified": false, "operation": {
                "type": "create", "handle": "bob.example", "service": "https://pds.example"}},
            {"createdAt": "2023-01-01T00:00:00.000Z", "nullified": false, "operation": {
                "type": "plc_tombstone", "prev": "bafy"}}
        ]));
        assert_eq!(read_audit_log(&dead), None);
        assert_eq!(read_audit_log(b"[]"), None);
        assert_eq!(
            read_audit_log(b"{\"message\":\"DID not registered\"}"),
            None
        );
        assert_eq!(read_audit_log(b"<html>"), None);
        // A handle that is not a hostname is no claim.
        let odd = log(json!([
            {"createdAt": "2022-11-17T00:35:16.391Z", "operation": {
                "type": "create", "handle": "<script>", "service": "https://pds.example"}}
        ]));
        assert_eq!(read_audit_log(&odd).unwrap().claim, None);
    }

    #[test]
    fn did_web_document() {
        let doc = serde_json::to_vec(&json!({
            "id": "did:web:alice.example",
            "alsoKnownAs": ["at://alice.example"],
            "service": [
                {"id": "#other", "serviceEndpoint": "https://x.example"},
                {"id": "did:web:alice.example#atproto_pds", "serviceEndpoint": "https://pds.example"}
            ]
        }))
        .unwrap();
        let i = read_did_document(&doc).unwrap();
        assert_eq!(i.created, None);
        assert_eq!(i.claim.as_deref(), Some("alice.example"));
        assert_eq!(i.pds.as_deref(), Some("https://pds.example"));
    }

    #[test]
    fn only_a_plain_https_endpoint_may_carry_the_image() {
        assert!(avatar_base("https://pds.example").is_some());
        assert!(avatar_base("https://pds.example:8443/").is_some());
        for bad in [
            "http://pds.example",
            "https://user:pw@pds.example",
            "https://user@pds.example",
            "https://pds.example/?x=1",
            "https://pds.example/#frag",
            "ftp://pds.example",
            "pds.example",
            "",
        ] {
            assert!(avatar_base(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn cids_and_image_urls() {
        let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
        assert!(is_cid(cid));
        for bad in [
            "",
            "b",
            "Qmabcdefghijklmnopqrstuvwxyz",
            "bafkrei\"><script>",
            "bafkrei/../x",
            "BAFKREIHDWDCEFGH4DQ",
        ] {
            assert!(!is_cid(bad), "{bad}");
        }
        assert!(!is_cid(&format!("b{}", "a".repeat(MAX_CID))));
        let body = serde_json::to_vec(&json!({"uri": "at://x", "value": {
            "avatar": {"$type": "blob", "ref": {"$link": cid}, "mimeType": "image/jpeg", "size": 1}
        }}))
        .unwrap();
        assert_eq!(avatar_cid(&body).as_deref(), Some(cid));
        let none = serde_json::to_vec(&json!({"value": {"displayName": "x"}})).unwrap();
        assert_eq!(avatar_cid(&none), None);
        let odd =
            serde_json::to_vec(&json!({"value": {"avatar": {"ref": {"$link": "x y"}}}})).unwrap();
        assert_eq!(avatar_cid(&odd), None);
        let base = avatar_base("https://pds.example/").unwrap();
        assert_eq!(
            xrpc(
                &base,
                "com.atproto.sync.getBlob",
                &[("did", "did:web:a.example%3A8080"), ("cid", cid)]
            )
            .unwrap()
            .as_str(),
            format!(
                "https://pds.example/xrpc/com.atproto.sync.getBlob?did=did%3Aweb%3Aa.example%253A8080&cid={cid}"
            )
        );
    }

    #[test]
    fn nothing_made_for_an_admin_is_stored() {
        for c in [Cache::Public(FULL_CARD_MAX_AGE), Cache::NoStore] {
            assert_eq!(For::Admin.cache(c), Cache::Private);
            assert_eq!(For::Public.cache(c), c);
        }
        let cfg = Config::default();
        let r = plain(&cfg, For::Admin, StatusCode::NOT_FOUND, "not found");
        assert_eq!(r.headers()[header::CACHE_CONTROL], "no-store, private");
        let r = plain(&cfg, For::Public, StatusCode::NOT_FOUND, "not found");
        assert_eq!(r.headers()[header::CACHE_CONTROL], "no-store");
    }

    #[test]
    fn outcome_labels_are_the_documented_set() {
        let labels: Vec<&str> = Outcome::ALL.iter().map(|o| o.label()).collect();
        assert_eq!(
            labels,
            [
                "served",
                "rate_limited",
                "plc_timeout",
                "pds_failed",
                "avatars_disabled"
            ]
        );
    }
}
