//! A stand-in ATProto identity and authorization server for the stage-7
//! harness: a PLC directory, a PDS's protected-resource metadata, and an
//! authorization server (metadata, PAR, authorize, token) in one process.
//!
//! It enforces what the ATProto OAuth profile makes a server enforce —
//! PAR only, PKCE S256, a valid ES256 DPoP proof on every POST with a
//! server nonce, the same DPoP key at PAR and at the token endpoint,
//! single-use request URIs and codes, client metadata rules — so that a
//! flow that completes here was a well-formed one. It is not a real
//! server: what it accepts shows that Farsight's requests are consistent
//! with the specification as read, not that a deployed server accepts
//! them.
//!
//! It listens on a TEST-NET-2 address (a bridge the harness creates), an
//! address the safe outbound client treats as public, so no rule of that
//! client is bypassed.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// What the harness can change about the server's behaviour.
#[derive(Debug, Clone, Default)]
pub struct Knobs {
    /// Never accept a DPoP proof's nonce (always answer `use_dpop_nonce`).
    pub nonce_never_accepted: bool,
    /// The account that "signs in" at the authorize step; default: the
    /// `login_hint`.
    pub sub: Option<String>,
    /// The user refuses: the callback carries `error=access_denied`.
    pub deny: bool,
    /// The callback's `iss` names another server.
    pub wrong_iss: bool,
    /// `token_type` of the token response (default `DPoP`).
    pub token_type: Option<String>,
    /// Leave `atproto` out of the granted scope.
    pub drop_scope: bool,
    /// Metadata fields to overwrite (`null` removes one).
    pub metadata: Option<Value>,
}

/// One pushed authorization request.
#[derive(Debug, Clone)]
pub struct Pushed {
    pub form: HashMap<String, String>,
    pub jwk: Value,
}

/// What a request looked like, for the harness's assertions.
#[derive(Debug, Clone)]
pub struct Event {
    /// `par` or `token`.
    pub kind: &'static str,
    /// The form posted.
    pub form: HashMap<String, String>,
    /// The DPoP proof's header and claims, when it had a parseable one.
    pub proof: Option<(Value, Value)>,
    /// The status answered.
    pub status: u16,
    /// The OAuth error answered, if any.
    pub error: Option<String>,
}

#[derive(Debug, Default)]
struct Inner {
    knobs: Knobs,
    nonce: String,
    pushed: HashMap<String, Pushed>,
    codes: HashMap<String, Pushed>,
    jtis: HashSet<String>,
    events: Vec<Event>,
    /// Every secret this server issued (codes, tokens), for the log grep.
    issued: Vec<String>,
    /// Where to fetch a hosted client's metadata: (base URL, Host header).
    client_metadata_at: Option<(String, String)>,
    /// Client metadata documents fetched, by `client_id`.
    fetched_metadata: Vec<Value>,
}

/// The stand-in server.
#[derive(Debug)]
pub struct Standin {
    /// `http://198.51.100.1:<port>`.
    pub base: String,
    inner: Mutex<Inner>,
}

fn random() -> String {
    URL_SAFE_NO_PAD.encode(farsight_api::auth::random_bytes::<24>())
}

fn oauth_error(status: StatusCode, error: &str, nonce: Option<&str>) -> Response {
    let mut r = (status, axum::Json(json!({"error": error}))).into_response();
    if let Some(n) = nonce {
        if let Ok(v) = HeaderValue::from_str(n) {
            r.headers_mut().insert("dpop-nonce", v);
        }
    }
    r
}

fn b64_json(s: &str) -> Option<Value> {
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(s).ok()?).ok()
}

/// Verifies a DPoP proof JWT (RFC 9449) for a POST to `htu`: structure,
/// ES256 signature under the embedded P-256 key, method, URL, freshness.
/// Returns the header and claims.
pub fn verify_dpop(jwt: &str, htu: &str) -> Result<(Value, Value), String> {
    let parts: Vec<&str> = jwt.split('.').collect();
    let [h, c, sig] = parts.as_slice() else {
        return Err("not a JWT".into());
    };
    let header = b64_json(h).ok_or("header is not JSON")?;
    let claims = b64_json(c).ok_or("claims are not JSON")?;
    if header["typ"] != "dpop+jwt" || header["alg"] != "ES256" {
        return Err("wrong typ or alg".into());
    }
    let jwk = &header["jwk"];
    if jwk["kty"] != "EC" || jwk["crv"] != "P-256" || jwk.get("d").is_some() {
        return Err("jwk is not a public P-256 key".into());
    }
    let coord = |k: &str| {
        jwk[k]
            .as_str()
            .and_then(|s| URL_SAFE_NO_PAD.decode(s).ok())
            .filter(|b| b.len() == 32)
            .ok_or_else(|| format!("bad jwk.{k}"))
    };
    let mut point = vec![4u8];
    point.extend(coord("x")?);
    point.extend(coord("y")?);
    let sig = URL_SAFE_NO_PAD
        .decode(sig)
        .map_err(|_| "bad signature encoding")?;
    ring::signature::UnparsedPublicKey::new(&ring::signature::ECDSA_P256_SHA256_FIXED, &point)
        .verify(format!("{h}.{c}").as_bytes(), &sig)
        .map_err(|_| "signature does not verify")?;
    if claims["htm"] != "POST" {
        return Err("htm is not POST".into());
    }
    if claims["htu"].as_str() != Some(htu) {
        return Err(format!("htu is {} not {htu}", claims["htu"]));
    }
    let iat = claims["iat"].as_i64().ok_or("no iat")?;
    if (chrono::Utc::now().timestamp() - iat).abs() > 60 {
        return Err("iat is not fresh".into());
    }
    if claims["jti"].as_str().is_none_or(|j| j.len() < 16) {
        return Err("jti missing or short".into());
    }
    Ok((header, claims))
}

fn parse_form(body: &str) -> HashMap<String, String> {
    reqwest::Url::parse(&format!("http://x/?{body}"))
        .map(|u| u.query_pairs().into_owned().collect())
        .unwrap_or_default()
}

impl Standin {
    /// Binds to `addr` (port 0) and serves until the process ends.
    pub async fn start(addr: &str) -> Result<Arc<Standin>, String> {
        let listener = tokio::net::TcpListener::bind((addr, 0))
            .await
            .map_err(|e| format!("binding {addr}: {e}"))?;
        let port = listener.local_addr().map_err(|e| e.to_string())?.port();
        let st = Arc::new(Standin {
            base: format!("http://{addr}:{port}"),
            inner: Mutex::new(Inner {
                nonce: random(),
                ..Inner::default()
            }),
        });
        let app = Router::new()
            .route("/.well-known/oauth-protected-resource", get(resource))
            .route("/.well-known/oauth-authorization-server", get(metadata))
            .route("/oauth/par", post(par))
            .route("/oauth/authorize", get(authorize))
            .route("/oauth/token", post(token))
            .route("/{did}", get(did_document))
            .with_state(st.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(st)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Replaces the knobs.
    pub fn set(&self, k: Knobs) {
        self.lock().knobs = k;
    }

    /// Back to the default behaviour.
    pub fn reset(&self) {
        self.set(Knobs::default());
    }

    /// Where a hosted client's metadata is fetched from: Farsight's base
    /// URL and the `Host` to send (the hostname is not in any DNS).
    pub fn client_metadata_at(&self, base: &str, host: &str) {
        self.lock().client_metadata_at = Some((base.to_owned(), host.to_owned()));
    }

    /// Events since the last call.
    pub fn take_events(&self) -> Vec<Event> {
        std::mem::take(&mut self.lock().events)
    }

    /// Every code and token issued so far.
    pub fn issued(&self) -> Vec<String> {
        self.lock().issued.clone()
    }

    /// Client metadata documents fetched so far.
    pub fn fetched_metadata(&self) -> Vec<Value> {
        self.lock().fetched_metadata.clone()
    }

    /// Checks the DPoP proof of a POST and the nonce policy. `Err` is the
    /// response to send.
    fn dpop(&self, headers: &HeaderMap, path: &str) -> Result<(Value, Value), Response> {
        let jwt = headers
            .get("dpop")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| oauth_error(StatusCode::BAD_REQUEST, "invalid_dpop_proof", None))?;
        let (h, c) = verify_dpop(jwt, &format!("{}{path}", self.base))
            .map_err(|_| oauth_error(StatusCode::BAD_REQUEST, "invalid_dpop_proof", None))?;
        let mut g = self.lock();
        let jti = c["jti"].as_str().unwrap_or_default().to_owned();
        if !g.jtis.insert(jti) {
            return Err(oauth_error(
                StatusCode::BAD_REQUEST,
                "invalid_dpop_proof",
                None,
            ));
        }
        let nonce = g.nonce.clone();
        if g.knobs.nonce_never_accepted || c["nonce"].as_str() != Some(nonce.as_str()) {
            return Err(oauth_error(
                StatusCode::BAD_REQUEST,
                "use_dpop_nonce",
                Some(&nonce),
            ));
        }
        Ok((h, c))
    }

    fn record(
        &self,
        kind: &'static str,
        form: &HashMap<String, String>,
        jwt: Option<&str>,
        r: &Response,
        error: Option<&str>,
    ) {
        let proof = jwt.and_then(|j| {
            let p: Vec<&str> = j.split('.').collect();
            Some((b64_json(p.first()?)?, b64_json(p.get(1)?)?))
        });
        self.lock().events.push(Event {
            kind,
            form: form.clone(),
            proof,
            status: r.status().as_u16(),
            error: error.map(str::to_owned),
        });
    }
}

type St = State<Arc<Standin>>;

async fn did_document(State(st): St, Path(did): Path<String>) -> Response {
    if !did.starts_with("did:plc:") || did.contains("unknown") {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }
    axum::Json(json!({
        "id": did,
        "alsoKnownAs": [],
        "service": [{
            "id": "#atproto_pds",
            "type": "AtprotoPersonalDataServer",
            "serviceEndpoint": st.base,
        }]
    }))
    .into_response()
}

async fn resource(State(st): St) -> Response {
    axum::Json(json!({"resource": st.base, "authorization_servers": [st.base]})).into_response()
}

async fn metadata(State(st): St) -> Response {
    let mut m = json!({
        "issuer": st.base,
        "require_pushed_authorization_requests": true,
        "pushed_authorization_request_endpoint": format!("{}/oauth/par", st.base),
        "authorization_endpoint": format!("{}/oauth/authorize", st.base),
        "token_endpoint": format!("{}/oauth/token", st.base),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none", "private_key_jwt"],
        "scopes_supported": ["atproto"],
        "dpop_signing_alg_values_supported": ["ES256"],
        "authorization_response_iss_parameter_supported": true,
        "client_id_metadata_document_supported": true
    });
    if let Some(Value::Object(over)) = st.lock().knobs.metadata.clone() {
        for (k, v) in over {
            if v.is_null() {
                m.as_object_mut().expect("object").remove(&k);
            } else {
                m[k] = v;
            }
        }
    }
    axum::Json(m).into_response()
}

/// The profile's client rules. A loopback client declares itself in its
/// `client_id`; a hosted client's metadata is fetched and checked.
async fn check_client(st: &Standin, form: &HashMap<String, String>) -> Result<(), String> {
    let client_id = form.get("client_id").ok_or("no client_id")?;
    let redirect = form.get("redirect_uri").ok_or("no redirect_uri")?;
    let id = reqwest::Url::parse(client_id).map_err(|_| "client_id is not a URL")?;
    if id.scheme() == "http" {
        if id.host_str() != Some("localhost") || id.port().is_some() || id.path() != "/" {
            return Err("an http client_id must be exactly http://localhost".into());
        }
        let q: Vec<(String, String)> = id.query_pairs().into_owned().collect();
        let r = reqwest::Url::parse(redirect).map_err(|_| "redirect_uri is not a URL")?;
        if r.scheme() != "http" || !matches!(r.host_str(), Some("127.0.0.1" | "[::1]")) {
            return Err("a loopback redirect_uri must be http://127.0.0.1 or http://[::1]".into());
        }
        // Path components must match; port numbers are not matched.
        let declared = q.iter().filter(|(k, _)| k == "redirect_uri").any(|(_, v)| {
            reqwest::Url::parse(v).is_ok_and(|d| {
                d.scheme() == r.scheme() && d.host_str() == r.host_str() && d.path() == r.path()
            })
        });
        if !declared {
            return Err("redirect_uri is not declared in the client_id".into());
        }
        if !q
            .iter()
            .any(|(k, v)| k == "scope" && v.split(' ').any(|s| s == "atproto"))
        {
            return Err("the client_id does not declare the atproto scope".into());
        }
        return Ok(());
    }
    if id.scheme() != "https"
        || id.port().is_some()
        || id
            .host_str()
            .is_none_or(|h| h.parse::<std::net::IpAddr>().is_ok())
    {
        return Err("a hosted client_id must be https, a domain name, without a port".into());
    }
    let at = st.lock().client_metadata_at.clone();
    let (base, host) = at.ok_or("no route to the client's metadata")?;
    if id.host_str() != Some(host.as_str()) {
        return Err("client_id host is not the known client".into());
    }
    let r = reqwest::Client::new()
        .get(format!("{base}{}", id.path()))
        .header("host", &host)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if r.status() != 200 {
        return Err(format!("client metadata: HTTP {}", r.status()));
    }
    let m: Value = r.json().await.map_err(|_| "client metadata is not JSON")?;
    st.lock().fetched_metadata.push(m.clone());
    let list = |k: &str, want: &str| {
        m[k].as_array()
            .is_some_and(|a| a.iter().any(|v| v.as_str() == Some(want)))
    };
    if m["client_id"].as_str() != Some(client_id.as_str()) {
        return Err("client metadata names another client_id".into());
    }
    if !list("redirect_uris", redirect) {
        return Err("redirect_uri is not in the client metadata".into());
    }
    let r = reqwest::Url::parse(redirect).map_err(|_| "redirect_uri is not a URL")?;
    if r.scheme() != "https" || r.origin() != id.origin() {
        return Err("a web client's redirect_uri must be https on the client_id's origin".into());
    }
    // `refresh_token` is optional; `authorization_code` is not.
    if !list("grant_types", "authorization_code") || !list("response_types", "code") {
        return Err("client metadata lacks authorization_code / code".into());
    }
    if m["dpop_bound_access_tokens"] != true {
        return Err("client metadata does not bind tokens to DPoP".into());
    }
    if !m["scope"]
        .as_str()
        .is_some_and(|s| s.split(' ').any(|x| x == "atproto"))
    {
        return Err("client metadata does not include the atproto scope".into());
    }
    if m["token_endpoint_auth_method"] != "none" {
        return Err("a public client must use token_endpoint_auth_method none".into());
    }
    Ok(())
}

async fn par(State(st): St, headers: HeaderMap, body: String) -> Response {
    let form = parse_form(&body);
    let jwt = headers
        .get("dpop")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let (r, error) = par_inner(&st, &headers, &form).await;
    st.record("par", &form, jwt.as_deref(), &r, error.as_deref());
    r
}

async fn par_inner(
    st: &Standin,
    headers: &HeaderMap,
    form: &HashMap<String, String>,
) -> (Response, Option<String>) {
    let (h, _) = match st.dpop(headers, "/oauth/par") {
        Ok(x) => x,
        Err(r) => return (r, Some("dpop".into())),
    };
    let f = |k: &str| form.get(k).map(String::as_str).unwrap_or_default();
    let bad = |why: &str| {
        (
            oauth_error(StatusCode::BAD_REQUEST, "invalid_request", None),
            Some(why.to_owned()),
        )
    };
    if f("response_type") != "code" {
        return bad("response_type");
    }
    if f("code_challenge_method") != "S256" || f("code_challenge").len() != 43 {
        return bad("pkce");
    }
    if f("state").len() < 16 {
        return bad("state");
    }
    if !f("scope").split(' ').any(|s| s == "atproto") {
        return bad("scope");
    }
    if let Err(e) = check_client(st, form).await {
        return (
            oauth_error(StatusCode::BAD_REQUEST, "invalid_client", None),
            Some(e),
        );
    }
    let uri = format!("urn:ietf:params:oauth:request_uri:req-{}", random());
    let mut g = st.lock();
    g.pushed.insert(
        uri.clone(),
        Pushed {
            form: form.clone(),
            jwk: h["jwk"].clone(),
        },
    );
    let nonce = g.nonce.clone();
    let mut r = (
        StatusCode::CREATED,
        axum::Json(json!({"request_uri": uri, "expires_in": 300})),
    )
        .into_response();
    r.headers_mut()
        .insert("dpop-nonce", HeaderValue::from_str(&nonce).expect("ascii"));
    (r, None)
}

/// The authorize step with the user's consent played automatically: the
/// browser comes back to the client's `redirect_uri` with `code`, `state`
/// and `iss` (or with `error`).
async fn authorize(State(st): St, Query(q): Query<HashMap<String, String>>) -> Response {
    let mut g = st.lock();
    let Some(p) = q.get("request_uri").and_then(|u| g.pushed.remove(u)) else {
        return (StatusCode::BAD_REQUEST, "unknown request_uri").into_response();
    };
    if q.get("client_id") != p.form.get("client_id") {
        return (StatusCode::BAD_REQUEST, "client_id mismatch").into_response();
    }
    let Ok(mut to) = reqwest::Url::parse(&p.form["redirect_uri"]) else {
        return (StatusCode::BAD_REQUEST, "bad redirect_uri").into_response();
    };
    let iss = if g.knobs.wrong_iss {
        "https://another-issuer.example".to_owned()
    } else {
        st.base.clone()
    };
    if g.knobs.deny {
        to.query_pairs_mut()
            .append_pair("error", "access_denied")
            .append_pair("state", &p.form["state"])
            .append_pair("iss", &iss);
    } else {
        let code = format!("cod-{}", random());
        to.query_pairs_mut()
            .append_pair("code", &code)
            .append_pair("state", &p.form["state"])
            .append_pair("iss", &iss);
        g.issued.push(code.clone());
        g.codes.insert(code, p);
    }
    (StatusCode::SEE_OTHER, [(header::LOCATION, to.to_string())]).into_response()
}

async fn token(State(st): St, headers: HeaderMap, body: String) -> Response {
    let form = parse_form(&body);
    let jwt = headers
        .get("dpop")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let (r, error) = token_inner(&st, &headers, &form);
    st.record("token", &form, jwt.as_deref(), &r, error.as_deref());
    r
}

fn token_inner(
    st: &Standin,
    headers: &HeaderMap,
    form: &HashMap<String, String>,
) -> (Response, Option<String>) {
    let (h, _) = match st.dpop(headers, "/oauth/token") {
        Ok(x) => x,
        Err(r) => return (r, Some("dpop".into())),
    };
    let f = |k: &str| form.get(k).map(String::as_str).unwrap_or_default();
    let grant_error = |why: &str| {
        (
            oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", None),
            Some(why.to_owned()),
        )
    };
    if f("grant_type") != "authorization_code" {
        return grant_error("grant_type");
    }
    let mut g = st.lock();
    // A code is single-use: it is gone whatever happens next.
    let Some(p) = g.codes.remove(f("code")) else {
        return grant_error("unknown code");
    };
    if p.jwk != h["jwk"] {
        return grant_error("another DPoP key than at PAR");
    }
    if URL_SAFE_NO_PAD.encode(Sha256::digest(f("code_verifier").as_bytes()))
        != p.form["code_challenge"]
    {
        return grant_error("PKCE verifier does not match");
    }
    if f("client_id") != p.form["client_id"] || f("redirect_uri") != p.form["redirect_uri"] {
        return grant_error("client_id or redirect_uri differs from the pushed request");
    }
    let access = format!("tok-{}", random());
    g.issued.push(access.clone());
    let sub = g
        .knobs
        .sub
        .clone()
        .unwrap_or_else(|| p.form.get("login_hint").cloned().unwrap_or_default());
    let body = json!({
        "access_token": access,
        "token_type": g.knobs.token_type.clone().unwrap_or_else(|| "DPoP".into()),
        "scope": if g.knobs.drop_scope { "transition:generic" } else { "atproto" },
        "sub": sub,
        "expires_in": 300
    });
    (axum::Json(body).into_response(), None)
}
