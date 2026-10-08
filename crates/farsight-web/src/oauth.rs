//! The ATProto OAuth client behind the admin sign-in (see
//! `docs/design/web-ui.md`).
//!
//! Farsight is a public client that asks for the `atproto` scope only: it
//! authenticates one account, the configured admin DID, reads `sub` from
//! the token response and discards the tokens. This module is the whole
//! client: the two client identities (hosted and loopback), discovery of
//! the account's authorization server, pushed authorization requests,
//! PKCE, DPoP proofs with the one nonce retry, the token request, and the
//! in-memory store of flows in progress. The handlers that use it are in
//! [`crate::enter`].
//!
//! Every request goes through the safe outbound client (see
//! `docs/design/security.md`). Nothing here logs a code, a state, a
//! token, a cookie or a key.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use farsight_core::Did;
use farsight_core::did::is_valid_hostname;
use farsight_core::net::{OutboundClient, OutboundResponse, SafeClient, SafeClientConfig};
use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;

/// Path of the client metadata document (hosted mode).
pub const METADATA_PATH: &str = "/.well-known/atproto-oauth-client-metadata";
/// Path the authorization server sends the browser back to.
pub const CALLBACK_PATH: &str = "/enter/callback";
/// The one scope Farsight asks for: authentication, no PDS access.
pub const SCOPE: &str = "atproto";
/// How long a started flow may take.
pub const FLOW_TTL: Duration = Duration::from_secs(600);
/// Flows held at once; a start beyond it evicts the oldest.
pub const MAX_FLOWS: usize = 256;
/// How long a successful discovery is reused.
pub const DISCOVERY_TTL: Duration = Duration::from_secs(300);
/// How long a failed discovery is reused.
pub const DISCOVERY_FAILURE_TTL: Duration = Duration::from_secs(30);
/// How long the admin DID's handle is kept for display.
pub const HANDLE_TTL: Duration = Duration::from_secs(3600);
/// Longest wait for the handle when a page shows it.
pub const HANDLE_WAIT: Duration = Duration::from_secs(2);

/// Why a step of the sign-in's client failed. The texts are for the log
/// and for the operator; none carries a code, a token or a response body.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OAuthError {
    /// The safe client refused the request, or it failed in transport.
    #[error(transparent)]
    Outbound(#[from] farsight_core::net::OutboundError),
    /// A well-known URL could not be formed.
    #[error(transparent)]
    Url(#[from] url::ParseError),
    /// The text is not a DID.
    #[error(transparent)]
    Did(#[from] farsight_core::did::DidError),
    /// Generating the flow's key, or signing with it, failed.
    #[error("{0}")]
    Key(&'static str),
    /// The directory says the DID does not exist.
    #[error("the DID was not found")]
    DidNotFound,
    /// The DID's document could not be read, or does not say what is
    /// needed.
    #[error("{0}")]
    Document(String),
    /// The PDS's or the authorization server's metadata could not be
    /// read, or does not meet what the flow needs.
    #[error("{0}")]
    Metadata(String),
    /// The authorization server refused a request.
    #[error("{0}")]
    Refused(String),
    /// The authorization server's answer is not usable.
    #[error("{0}")]
    Response(&'static str),
    /// The lookup did not finish in its time.
    #[error("the lookup timed out")]
    TimedOut,
}

/// The identity Farsight presents to an authorization server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Client {
    /// `client_id`.
    pub client_id: String,
    /// `redirect_uri`.
    pub redirect_uri: String,
    /// The loopback (development) client rather than the hosted one.
    pub loopback: bool,
}

/// Whether `server.hostname` can be a hosted client: a domain name, so no
/// port, no IP address, no single label.
pub fn hosted_possible(hostname: &str) -> bool {
    is_valid_hostname(hostname)
}

/// Whether a `Host` header names this machine's loopback address:
/// `127.0.0.1` or `[::1]`, with any port. `localhost` is not accepted: the
/// callback arrives on the address, which is a different cookie host.
pub fn is_loopback_host(host: &str) -> bool {
    let port_ok = |p: &str| {
        p.is_empty()
            || p.strip_prefix(':')
                .is_some_and(|n| n.parse::<u16>().is_ok())
    };
    ["127.0.0.1", "[::1]"]
        .iter()
        .any(|a| host.strip_prefix(a).is_some_and(port_ok))
}

/// Whether a request's client address is one a browser on this machine
/// (or tunnelled to it) arrives from: loopback, or a private address —
/// a container runtime delivers connections made to a published port on
/// the host from its bridge's gateway, not from `127.0.0.1`. A public
/// address is somebody else's browser, whatever `Host` it sent.
pub fn is_local_client(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(a) => a.is_loopback() || a.is_private() || a.is_link_local(),
        std::net::IpAddr::V6(a) => {
            if let Some(v4) = a.to_ipv4_mapped() {
                return is_local_client(std::net::IpAddr::V4(v4));
            }
            let first = a.segments()[0];
            a.is_loopback() || (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80
        }
    }
}

/// The client for a request: hosted when its `Host` header names
/// `server.hostname` and that is a domain name; loopback when the header
/// names a loopback address **and** the request's client is local
/// (`local`, from [`is_local_client`] of the resolved client address);
/// otherwise none. The header alone does not select loopback mode: any
/// client can send `Host: 127.0.0.1`.
pub fn client_for(hostname: &str, host_header: &str, local: bool) -> Option<Client> {
    let host = host_header.trim().to_ascii_lowercase();
    if is_loopback_host(&host) {
        if !local {
            return None;
        }
        let redirect_uri = format!("http://{host}{CALLBACK_PATH}");
        let encoded: String =
            url::form_urlencoded::byte_serialize(redirect_uri.as_bytes()).collect();
        return Some(Client {
            client_id: format!("http://localhost?redirect_uri={encoded}&scope={SCOPE}"),
            redirect_uri,
            loopback: true,
        });
    }
    let hostname = hostname.to_ascii_lowercase();
    let host = host.strip_suffix(":443").unwrap_or(&host);
    (hosted_possible(&hostname) && host == hostname).then(|| Client {
        client_id: format!("https://{hostname}{METADATA_PATH}"),
        redirect_uri: format!("https://{hostname}{CALLBACK_PATH}"),
        loopback: false,
    })
}

/// The client metadata document served at [`METADATA_PATH`].
pub fn client_metadata(hostname: &str) -> Value {
    let hostname = hostname.to_ascii_lowercase();
    json!({
        "client_id": format!("https://{hostname}{METADATA_PATH}"),
        "client_name": format!("Farsight admin ({hostname})"),
        "client_uri": format!("https://{hostname}/"),
        "redirect_uris": [format!("https://{hostname}{CALLBACK_PATH}")],
        "grant_types": ["authorization_code"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
        "scope": SCOPE,
        "application_type": "web",
        "dpop_bound_access_tokens": true
    })
}

fn random_token() -> String {
    URL_SAFE_NO_PAD.encode(farsight_api::auth::random_bytes::<32>())
}

/// A new `state`, PKCE verifier or flow cookie value: 256 random bits.
pub fn new_secret() -> String {
    random_token()
}

/// The S256 PKCE challenge of a verifier.
pub fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

/// A DPoP key: one P-256 key pair per flow, never reused, never stored.
pub struct DpopKey {
    pair: EcdsaKeyPair,
    jwk: Value,
}

impl std::fmt::Debug for DpopKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DpopKey(..)")
    }
}

impl DpopKey {
    /// Generates an ES256 (P-256) key pair for one flow's DPoP proofs.
    /// It lives in memory with the flow and is never stored.
    pub fn generate() -> Result<DpopKey, OAuthError> {
        let rng = SystemRandom::new();
        let doc = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
            .map_err(|_| OAuthError::Key("key generation failed"))?;
        let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, doc.as_ref(), &rng)
            .map_err(|_| OAuthError::Key("key generation failed"))?;
        // Uncompressed point: 0x04 || X (32) || Y (32).
        let point = pair.public_key().as_ref();
        if point.len() != 65 || point[0] != 4 {
            return Err(OAuthError::Key("unexpected public key encoding"));
        }
        let jwk = json!({
            "kty": "EC",
            "crv": "P-256",
            "x": URL_SAFE_NO_PAD.encode(&point[1..33]),
            "y": URL_SAFE_NO_PAD.encode(&point[33..65]),
        });
        Ok(DpopKey { pair, jwk })
    }

    /// A DPoP proof (RFC 9449) for one request: ES256, a fresh `jti`, the
    /// method, the URL without query or fragment, the time, and the
    /// server's nonce when it has given one.
    pub fn proof(
        &self,
        method: &str,
        url: &Url,
        nonce: Option<&str>,
    ) -> Result<String, OAuthError> {
        let mut htu = url.clone();
        htu.set_query(None);
        htu.set_fragment(None);
        let header = json!({"typ": "dpop+jwt", "alg": "ES256", "jwk": self.jwk});
        let mut claims = json!({
            "jti": URL_SAFE_NO_PAD.encode(farsight_api::auth::random_bytes::<16>()),
            "htm": method,
            "htu": htu.as_str(),
            "iat": chrono::Utc::now().timestamp(),
        });
        if let Some(n) = nonce {
            claims["nonce"] = Value::String(n.to_owned());
        }
        let signing_input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(header.to_string()),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        );
        let sig = self
            .pair
            .sign(&SystemRandom::new(), signing_input.as_bytes())
            .map_err(|_| OAuthError::Key("signing failed"))?;
        Ok(format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(sig.as_ref())
        ))
    }
}

/// The admin account's authorization server, as discovered and checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Server {
    /// The issuer identifier.
    pub issuer: String,
    /// Pushed-authorization endpoint.
    pub par: Url,
    /// Authorization endpoint (where the browser is sent).
    pub authorize: Url,
    /// Token endpoint.
    pub token: Url,
}

async fn get_json(safe: &SafeClient, url: &Url) -> Result<Value, OAuthError> {
    let r = safe.get(url).await?;
    // A discovery document is read where it was asked for: a redirect is
    // not followed to another document.
    if r.final_url != *url {
        return Err(OAuthError::Metadata(format!(
            "{} redirects elsewhere",
            origin_of(url)
        )));
    }
    if r.status != 200 {
        return Err(OAuthError::Metadata(format!(
            "{}: HTTP {}",
            origin_of(url),
            r.status
        )));
    }
    serde_json::from_slice(&r.body)
        .map_err(|_| OAuthError::Metadata(format!("{}: not JSON", origin_of(url))))
}

fn origin_of(u: &Url) -> String {
    u.origin().ascii_serialization()
}

/// A URL that is an origin and nothing more (no path, query or fragment).
fn bare_origin(s: &str) -> Option<Url> {
    let u = Url::parse(s).ok()?;
    (u.path() == "/" && u.query().is_none() && u.fragment().is_none() && u.host().is_some())
        .then_some(u)
}

/// The PDS endpoint a DID document declares (`#atproto_pds`).
pub fn pds_endpoint(doc: &Value) -> Option<Url> {
    let s = doc["service"].as_array()?.iter().find(|s| {
        s["id"]
            .as_str()
            .is_some_and(|id| id.ends_with("#atproto_pds"))
            && s["type"].as_str() == Some("AtprotoPersonalDataServer")
    })?;
    bare_origin(s["serviceEndpoint"].as_str()?)
}

/// Checks an authorization server's metadata against the issuer it was
/// fetched for and against what the flow needs: the issuer matches, PAR
/// is required, the endpoints are URLs the safe client accepts, and
/// S256, ES256, the `atproto` scope and the `code` response type are
/// supported.
pub fn check_server_metadata(
    issuer: &Url,
    meta: &Value,
    safe: &SafeClientConfig,
) -> Result<Server, OAuthError> {
    let has = |key: &str, want: &str| {
        meta[key]
            .as_array()
            .is_some_and(|a| a.iter().any(|v| v.as_str() == Some(want)))
    };
    let declared = meta["issuer"]
        .as_str()
        .and_then(bare_origin)
        .ok_or_else(|| OAuthError::Metadata("the authorization server names no issuer".into()))?;
    if declared.origin() != issuer.origin() {
        return Err(OAuthError::Metadata(
            "the authorization server's issuer is not the one it was fetched from".into(),
        ));
    }
    if meta["require_pushed_authorization_requests"].as_bool() != Some(true) {
        return Err(OAuthError::Metadata(
            "the authorization server does not require pushed authorization requests".into(),
        ));
    }
    let endpoint = |key: &str| -> Result<Url, OAuthError> {
        let u = meta[key]
            .as_str()
            .and_then(|s| Url::parse(s).ok())
            .ok_or_else(|| {
                OAuthError::Metadata(format!("the authorization server has no {key}"))
            })?;
        farsight_core::net::check_url(&u, safe).map_err(|e| {
            OAuthError::Metadata(format!(
                "the authorization server's {key} is not usable: {e}"
            ))
        })?;
        Ok(u)
    };
    let server = Server {
        issuer: origin_of(issuer),
        par: endpoint("pushed_authorization_request_endpoint")?,
        authorize: endpoint("authorization_endpoint")?,
        token: endpoint("token_endpoint")?,
    };
    for (key, want) in [
        ("code_challenge_methods_supported", "S256"),
        ("dpop_signing_alg_values_supported", "ES256"),
        ("scopes_supported", SCOPE),
        ("response_types_supported", "code"),
    ] {
        if !has(key, want) {
            return Err(OAuthError::Metadata(format!(
                "the authorization server does not list {want} in {key}"
            )));
        }
    }
    Ok(server)
}

async fn did_document(safe: &SafeClient, plc_url: &str, did: &Did) -> Result<Value, OAuthError> {
    let url = crate::public::handles::document_url_at(plc_url, did)
        .ok_or_else(|| OAuthError::Document(format!("{did} has no document URL")))?;
    let r = safe.get(&url).await?;
    match r.status {
        200 => {}
        404 | 410 => return Err(OAuthError::DidNotFound),
        s => {
            return Err(OAuthError::Document(format!(
                "the DID's directory answered HTTP {s}"
            )));
        }
    }
    let doc: Value = serde_json::from_slice(&r.body)
        .map_err(|_| OAuthError::Document("the DID document is not JSON".into()))?;
    // Compared as DIDs: a did:web document may write its host in any case.
    if doc["id"]
        .as_str()
        .and_then(|id| Did::parse(id).ok())
        .as_ref()
        != Some(did)
    {
        return Err(OAuthError::Document(
            "the DID document is for another DID".into(),
        ));
    }
    Ok(doc)
}

/// Finds and checks the authorization server of `did`: its DID document's
/// PDS, that PDS's protected-resource metadata (exactly one authorization
/// server), and that server's metadata. A did:plc document is read from
/// the directory at `plc_url`.
pub async fn discover(safe: &SafeClient, plc_url: &str, did: &Did) -> Result<Server, OAuthError> {
    let doc = did_document(safe, plc_url, did).await?;
    let pds = pds_endpoint(&doc)
        .ok_or_else(|| OAuthError::Document("the DID document names no PDS".into()))?;
    let resource = pds.join("/.well-known/oauth-protected-resource")?;
    let meta = get_json(safe, &resource).await?;
    let issuer = match meta["authorization_servers"].as_array().map(Vec::as_slice) {
        Some([one]) => one.as_str().and_then(bare_origin).ok_or_else(|| {
            OAuthError::Metadata("the PDS names an unusable authorization server".into())
        })?,
        _ => {
            return Err(OAuthError::Metadata(
                "the PDS does not name exactly one authorization server".into(),
            ));
        }
    };
    let server_url = issuer.join("/.well-known/oauth-authorization-server")?;
    let server_meta = get_json(safe, &server_url).await?;
    check_server_metadata(&issuer, &server_meta, safe.config())
}

/// What a DID resolves to, for the operator to confirm (wizard, CLI) and
/// for display.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The handle the document claims, if it resolves back to the DID.
    pub handle: Option<String>,
    /// The PDS host the document declares, if any.
    pub pds: Option<String>,
}

/// Resolves a DID for display. An error means the document could not be
/// read; a document without a handle or a PDS is not an error. A did:plc
/// document is read from the directory at `plc_url`.
pub async fn identity(safe: &SafeClient, plc_url: &str, did: &Did) -> Result<Identity, OAuthError> {
    let doc = did_document(safe, plc_url, did).await?;
    let pds = pds_endpoint(&doc).and_then(|u| u.host_str().map(str::to_owned));
    let handle = match crate::public::handles::claimed_handle(&doc) {
        Some(h) => match crate::pages::handle_to_did(safe, &h).await {
            Ok(back) if back == *did => Some(h),
            _ => None,
        },
        None => None,
    };
    Ok(Identity { handle, pds })
}

fn header<'a>(r: &'a OutboundResponse, name: &str) -> Option<&'a str> {
    r.headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

fn form_body(pairs: &[(&str, &str)]) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish()
}

/// A form POST with a DPoP proof. When the server answers
/// `use_dpop_nonce` with a `DPoP-Nonce` header, the request is repeated
/// once with that nonce. Returns the response and the latest nonce the
/// server gave.
async fn dpop_post(
    safe: &SafeClient,
    url: &Url,
    key: &DpopKey,
    pairs: &[(&str, &str)],
    nonce: Option<String>,
) -> Result<(OutboundResponse, Option<String>), OAuthError> {
    let mut nonce = nonce;
    let mut retried = false;
    loop {
        let proof = key.proof("POST", url, nonce.as_deref())?;
        let r = safe
            .post_form(url, &[("dpop", &proof)], form_body(pairs))
            .await?;
        let given = header(&r, "dpop-nonce").map(str::to_owned);
        let wants_nonce = (r.status == 400 || r.status == 401)
            && serde_json::from_slice::<Value>(&r.body)
                .ok()
                .is_some_and(|b| b["error"].as_str() == Some("use_dpop_nonce"));
        if let Some(n) = given {
            nonce = Some(n);
        }
        if wants_nonce && !retried && nonce.is_some() {
            retried = true;
            continue;
        }
        return Ok((r, nonce));
    }
}

/// The OAuth `error` code of a failed response, for the log. Never the
/// body.
fn oauth_error(r: &OutboundResponse) -> String {
    let code = serde_json::from_slice::<Value>(&r.body)
        .ok()
        .and_then(|b| b["error"].as_str().map(str::to_owned))
        .filter(|c| c.len() <= 64 && c.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'))
        .unwrap_or_else(|| "unreadable".into());
    format!("HTTP {} ({code})", r.status)
}

/// A flow in progress: everything the callback needs, held in memory
/// only, keyed by `state`.
#[derive(Debug)]
pub struct Flow {
    /// The PKCE verifier whose S256 challenge went out with the pushed
    /// request; the token request sends the verifier itself.
    pub verifier: String,
    /// The flow's DPoP key.
    pub key: DpopKey,
    /// Latest DPoP nonce from the authorization server.
    pub nonce: Option<String>,
    /// The issuer the flow was started with.
    pub issuer: String,
    /// Its token endpoint.
    pub token: Url,
    /// The admin DID when the flow started.
    pub did: String,
    /// The client identity used.
    pub client: Client,
    /// SHA-256 of the flow cookie value.
    pub cookie_sha256: [u8; 32],
    /// When the flow was started.
    pub created: Instant,
    /// Where the browser continues after the sign-in: `/admin`, or the
    /// admin page a repeated sign-in was asked from.
    pub back: &'static str,
    /// The stored key of the session that asked for a repeated sign-in;
    /// the new session replaces it.
    pub replaces: Option<[u8; 32]>,
}

/// Pushes the authorization request (PAR) and returns the flow record
/// and the URL to send the browser to.
pub async fn start(
    safe: &SafeClient,
    server: &Server,
    client: Client,
    did: &str,
    state: &str,
    cookie_sha256: [u8; 32],
) -> Result<(Flow, Url), OAuthError> {
    let key = DpopKey::generate()?;
    let verifier = new_secret();
    let challenge = pkce_challenge(&verifier);
    let (r, nonce) = dpop_post(
        safe,
        &server.par,
        &key,
        &[
            ("client_id", &client.client_id),
            ("redirect_uri", &client.redirect_uri),
            ("response_type", "code"),
            ("scope", SCOPE),
            ("state", state),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
            ("login_hint", did),
        ],
        None,
    )
    .await?;
    if !(r.status == 200 || r.status == 201) {
        return Err(OAuthError::Refused(format!(
            "the pushed authorization request was refused: {}",
            oauth_error(&r)
        )));
    }
    let request_uri = serde_json::from_slice::<Value>(&r.body)
        .ok()
        .and_then(|b| b["request_uri"].as_str().map(str::to_owned))
        .ok_or(OAuthError::Response(
            "the pushed authorization response has no request_uri",
        ))?;
    let mut to = server.authorize.clone();
    to.query_pairs_mut()
        .append_pair("client_id", &client.client_id)
        .append_pair("request_uri", &request_uri);
    Ok((
        Flow {
            verifier,
            key,
            nonce,
            issuer: server.issuer.clone(),
            token: server.token.clone(),
            did: did.to_owned(),
            client,
            cookie_sha256,
            created: Instant::now(),
            back: "/admin",
            replaces: None,
        },
        to,
    ))
}

/// Exchanges the code and returns the authenticated DID (`sub`). The
/// response must be a DPoP-bound grant of the `atproto` scope. The tokens
/// are dropped here: they are never returned, stored or logged, and the
/// flow's key — which they are bound to — is dropped with the flow.
pub async fn redeem(safe: &SafeClient, flow: Flow, code: &str) -> Result<String, OAuthError> {
    let (r, _) = dpop_post(
        safe,
        &flow.token,
        &flow.key,
        &[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("code_verifier", &flow.verifier),
            ("client_id", &flow.client.client_id),
            ("redirect_uri", &flow.client.redirect_uri),
        ],
        flow.nonce.clone(),
    )
    .await?;
    if r.status != 200 {
        return Err(OAuthError::Refused(format!(
            "the token request was refused: {}",
            oauth_error(&r)
        )));
    }
    let body: Value = serde_json::from_slice(&r.body)
        .map_err(|_| OAuthError::Response("the token response is not JSON"))?;
    read_token_response(&body)
}

/// The `sub` of a token response that is a DPoP-bound grant of the
/// `atproto` scope.
pub fn read_token_response(body: &Value) -> Result<String, OAuthError> {
    if !body["token_type"]
        .as_str()
        .is_some_and(|t| t.eq_ignore_ascii_case("DPoP"))
    {
        return Err(OAuthError::Response("the token response is not DPoP-bound"));
    }
    if !body["scope"]
        .as_str()
        .is_some_and(|s| s.split_ascii_whitespace().any(|x| x == SCOPE))
    {
        return Err(OAuthError::Response(
            "the token response does not grant the atproto scope",
        ));
    }
    body["sub"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or(OAuthError::Response("the token response has no sub"))
}

/// What a callback's `state` and cookie matched.
#[derive(Debug)]
pub enum Taken {
    /// No such flow, or it is older than the lifetime.
    Absent,
    /// The flow exists but the cookie is not the one it was started
    /// with. The flow is kept: a caller without the cookie cannot spend
    /// someone else's flow.
    WrongCookie,
    /// The flow, removed from the store: its `state` is spent.
    Flow(Box<Flow>),
}

/// Flows in progress, in memory only: a restart drops them all.
#[derive(Debug)]
pub struct FlowStore {
    flows: Mutex<HashMap<String, Flow>>,
    ttl: Duration,
    cap: usize,
}

impl Default for FlowStore {
    fn default() -> Self {
        FlowStore::new(flow_ttl(), MAX_FLOWS)
    }
}

#[cfg(feature = "harness")]
fn flow_ttl() -> Duration {
    // Harness only: lets the harness watch a flow expire without waiting
    // ten minutes.
    std::env::var("FARSIGHT_HARNESS_FLOW_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .map_or(FLOW_TTL, Duration::from_secs)
}

#[cfg(not(feature = "harness"))]
fn flow_ttl() -> Duration {
    FLOW_TTL
}

impl FlowStore {
    /// A store with the given lifetime and size.
    pub fn new(ttl: Duration, cap: usize) -> FlowStore {
        FlowStore {
            flows: Mutex::new(HashMap::new()),
            ttl,
            cap: cap.max(1),
        }
    }

    /// Stores a flow under `state`, evicting the oldest when full.
    pub fn insert(&self, state: String, flow: Flow) {
        let mut m = self.flows.lock().unwrap_or_else(|e| e.into_inner());
        while m.len() >= self.cap {
            let Some(oldest) = m
                .iter()
                .min_by_key(|(_, f)| f.created)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            m.remove(&oldest);
        }
        m.insert(state, flow);
    }

    /// Looks `state` up at `now`. A flow older than the lifetime is
    /// absent whether or not it has been swept. A flow whose cookie hash
    /// matches is removed and returned; otherwise it stays.
    pub fn take(&self, state: &str, cookie_sha256: &[u8; 32], now: Instant) -> Taken {
        use subtle::ConstantTimeEq;
        let mut m = self.flows.lock().unwrap_or_else(|e| e.into_inner());
        let Some(f) = m.get(state) else {
            return Taken::Absent;
        };
        if now.saturating_duration_since(f.created) > self.ttl {
            m.remove(state);
            return Taken::Absent;
        }
        if !bool::from(f.cookie_sha256.ct_eq(cookie_sha256)) {
            return Taken::WrongCookie;
        }
        match m.remove(state) {
            Some(f) => Taken::Flow(Box::new(f)),
            None => Taken::Absent,
        }
    }

    /// Drops flows older than the lifetime.
    pub fn sweep(&self, now: Instant) {
        self.flows
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, f| now.saturating_duration_since(f.created) <= self.ttl);
    }

    /// Flows held now, expired ones not yet swept included.
    pub fn len(&self) -> usize {
        self.flows.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether no flow is held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

type Cached<T> = Mutex<Option<(String, Instant, T)>>;

/// The sign-in's in-memory state: flows, the discovery cache, the admin
/// handle for display, the clock of the `sub`-mismatch warning, and the
/// PLC directory the admin DID is resolved through.
#[derive(Debug)]
pub struct OAuthState {
    /// `backfill.plc_url` as it was when the process started. The admin
    /// DID's document is read from this directory until the next start:
    /// a `plc_url` changed while running (it is otherwise applied at
    /// once) does not decide where the admin signs in.
    plc_url: String,
    /// Flows in progress, keyed by `state`: put at `POST /enter`, taken
    /// once by the callback.
    pub flows: FlowStore,
    discovery: Cached<Result<Server, OAuthError>>,
    handle: Cached<Option<String>>,
    last_mismatch_warning: Mutex<Option<Instant>>,
}

impl OAuthState {
    /// The state of a process whose config named `plc_url` as the PLC
    /// directory at start.
    pub fn new(plc_url: &str) -> OAuthState {
        OAuthState {
            plc_url: plc_url.to_owned(),
            flows: FlowStore::default(),
            discovery: Mutex::new(None),
            handle: Mutex::new(None),
            last_mismatch_warning: Mutex::new(None),
        }
    }

    /// The PLC directory pinned at start.
    pub fn plc_url(&self) -> &str {
        &self.plc_url
    }

    /// The admin DID's authorization server, from the cache when it has
    /// an answer young enough (successes and failures both), otherwise
    /// discovered now through the directory pinned at start. None of the
    /// hosts contacted is chosen by the caller.
    pub async fn server(&self, safe: &SafeClient, did: &Did) -> Result<Server, OAuthError> {
        {
            let c = self.discovery.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((d, at, r)) = c.as_ref() {
                let ttl = if r.is_ok() {
                    DISCOVERY_TTL
                } else {
                    DISCOVERY_FAILURE_TTL
                };
                if d == did.as_str() && at.elapsed() < ttl {
                    return r.clone();
                }
            }
        }
        let r = discover(safe, &self.plc_url, did).await;
        *self.discovery.lock().unwrap_or_else(|e| e.into_inner()) =
            Some((did.as_str().to_owned(), Instant::now(), r.clone()));
        r
    }

    /// The cached handle of `did`, if it was resolved within the hour.
    pub fn cached_handle(&self, did: &str) -> Option<Option<String>> {
        let c = self.handle.lock().unwrap_or_else(|e| e.into_inner());
        c.as_ref()
            .filter(|(d, at, _)| d == did && at.elapsed() < HANDLE_TTL)
            .map(|(_, _, h)| h.clone())
    }

    /// Remembers the handle of `did` (or that it has none).
    pub fn remember_handle(&self, did: &str, handle: Option<String>) {
        *self.handle.lock().unwrap_or_else(|e| e.into_inner()) =
            Some((did.to_owned(), Instant::now(), handle));
    }

    /// The admin DID's verified handle for display: from the cache, or
    /// resolved now with a two-second budget. `None` when it has none,
    /// does not verify, or does not answer in time (a timeout is not
    /// cached).
    pub async fn handle(&self, safe: &SafeClient, did: &str) -> Option<String> {
        if let Some(h) = self.cached_handle(did) {
            return h;
        }
        let parsed = Did::parse(did).ok()?;
        match tokio::time::timeout(HANDLE_WAIT, identity(safe, &self.plc_url, &parsed)).await {
            Ok(r) => {
                let h = r.ok().and_then(|i| i.handle);
                self.remember_handle(did, h.clone());
                h
            }
            Err(_) => None,
        }
    }

    /// Whether a `sub`-mismatch warning may be logged now: at most one a
    /// minute.
    pub fn may_warn_mismatch(&self) -> bool {
        let mut last = self
            .last_mismatch_warning
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if last.is_some_and(|t| t.elapsed() < Duration::from_secs(60)) {
            return false;
        }
        *last = Some(Instant::now());
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use farsight_core::config::Config;

    fn flow(created: Instant, cookie: [u8; 32]) -> Flow {
        Flow {
            verifier: new_secret(),
            key: DpopKey::generate().unwrap(),
            nonce: None,
            issuer: "https://as.example".into(),
            token: Url::parse("https://as.example/token").unwrap(),
            did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(),
            client: client_for("farsight.example", "farsight.example", false).unwrap(),
            cookie_sha256: cookie,
            created,
            back: "/admin",
            replaces: None,
        }
    }

    #[test]
    fn client_modes() {
        let hosted = client_for("farsight.example", "Farsight.Example", false).unwrap();
        assert!(!hosted.loopback);
        assert_eq!(
            hosted.client_id,
            "https://farsight.example/.well-known/atproto-oauth-client-metadata"
        );
        assert_eq!(
            hosted.redirect_uri,
            "https://farsight.example/enter/callback"
        );
        assert_eq!(
            client_for("farsight.example", "farsight.example:443", true),
            Some(hosted)
        );
        // Another name, a port, an IP or a single label is not hosted mode.
        assert_eq!(client_for("farsight.example", "other.example", true), None);
        assert_eq!(
            client_for("farsight.example", "farsight.example:8080", true),
            None
        );
        assert_eq!(
            client_for("farsight.example:8080", "farsight.example:8080", true),
            None
        );
        assert_eq!(client_for("203.0.113.7", "203.0.113.7", true), None);
        assert_eq!(client_for("farsight", "farsight", true), None);
        assert_eq!(client_for("farsight.example", "localhost:8080", true), None);
        assert_eq!(
            client_for("farsight.example", "127.0.0.1.evil.example", true),
            None
        );
        assert_eq!(
            client_for("farsight.example", "127.0.0.1:99999", true),
            None
        );
        // Loopback, whatever the hostname is.
        for (host, redirect) in [
            ("127.0.0.1:18093", "http://127.0.0.1:18093/enter/callback"),
            ("127.0.0.1", "http://127.0.0.1/enter/callback"),
            ("[::1]:8080", "http://[::1]:8080/enter/callback"),
        ] {
            let c = client_for("203.0.113.7:8080", host, true).unwrap();
            // The same Host from a client that is not local: no client.
            assert_eq!(client_for("203.0.113.7:8080", host, false), None);
            assert!(c.loopback);
            assert_eq!(c.redirect_uri, redirect);
            let u = Url::parse(&c.client_id).unwrap();
            assert_eq!(
                (u.scheme(), u.host_str(), u.port(), u.path()),
                ("http", Some("localhost"), None, "/")
            );
            let q: HashMap<_, _> = u.query_pairs().into_owned().collect();
            assert_eq!(q["redirect_uri"], redirect);
            assert_eq!(q["scope"], "atproto");
        }
    }

    #[test]
    fn metadata_document() {
        let m = client_metadata("Farsight.Example");
        assert_eq!(
            m["client_id"],
            "https://farsight.example/.well-known/atproto-oauth-client-metadata"
        );
        assert_eq!(
            m["redirect_uris"],
            json!(["https://farsight.example/enter/callback"])
        );
        assert_eq!(m["grant_types"], json!(["authorization_code"]));
        assert_eq!(m["response_types"], json!(["code"]));
        assert_eq!(m["token_endpoint_auth_method"], "none");
        assert_eq!(m["scope"], "atproto");
        assert_eq!(m["dpop_bound_access_tokens"], true);
    }

    #[test]
    fn pkce_s256_vector() {
        // BASE64URL(SHA-256(verifier)), computed independently.
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mBKjvE0gWk6ztKDL1IVWhHfM3SoFPk"),
            "2ouoWajIwDkqZFHsAWwOTbHQwue9ZmZTF9xCnAR6u40"
        );
        assert_eq!(new_secret().len(), 43);
        assert_ne!(new_secret(), new_secret());
    }

    #[test]
    fn dpop_proof_verifies() {
        let key = DpopKey::generate().unwrap();
        let url = Url::parse("https://as.example/par?x=1#f").unwrap();
        let jwt = key.proof("POST", &url, Some("n-1")).unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let dec = |s: &str| -> Value {
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(s).unwrap()).unwrap()
        };
        let (h, c) = (dec(parts[0]), dec(parts[1]));
        assert_eq!(h["typ"], "dpop+jwt");
        assert_eq!(h["alg"], "ES256");
        assert_eq!(h["jwk"]["crv"], "P-256");
        assert!(h["jwk"].get("d").is_none(), "no private key in the proof");
        assert_eq!(c["htm"], "POST");
        assert_eq!(c["htu"], "https://as.example/par");
        assert_eq!(c["nonce"], "n-1");
        assert!(c["jti"].as_str().unwrap().len() >= 16);
        assert!(c["iat"].as_i64().unwrap() > 1_700_000_000);
        // The signature verifies under the key in the header.
        let mut point = vec![4u8];
        point.extend(
            URL_SAFE_NO_PAD
                .decode(h["jwk"]["x"].as_str().unwrap())
                .unwrap(),
        );
        point.extend(
            URL_SAFE_NO_PAD
                .decode(h["jwk"]["y"].as_str().unwrap())
                .unwrap(),
        );
        ring::signature::UnparsedPublicKey::new(&ring::signature::ECDSA_P256_SHA256_FIXED, &point)
            .verify(
                format!("{}.{}", parts[0], parts[1]).as_bytes(),
                &URL_SAFE_NO_PAD.decode(parts[2]).unwrap(),
            )
            .unwrap();
        // No nonce, no claim; every proof has its own jti and key.
        let plain = dec(key
            .proof("POST", &url, None)
            .unwrap()
            .split('.')
            .nth(1)
            .unwrap());
        assert!(plain.get("nonce").is_none());
        assert_ne!(plain["jti"], c["jti"]);
        assert_ne!(DpopKey::generate().unwrap().jwk, key.jwk);
    }

    fn good_metadata() -> Value {
        json!({
            "issuer": "https://as.example",
            "require_pushed_authorization_requests": true,
            "pushed_authorization_request_endpoint": "https://as.example/oauth/par",
            "authorization_endpoint": "https://as.example/oauth/authorize",
            "token_endpoint": "https://as.example/oauth/token",
            "code_challenge_methods_supported": ["S256"],
            "dpop_signing_alg_values_supported": ["ES256K", "ES256"],
            "scopes_supported": ["atproto", "transition:generic"],
            "response_types_supported": ["code"]
        })
    }

    fn safe_config() -> SafeClientConfig {
        SafeClientConfig::from_config(&Config::default(), "test")
    }

    #[test]
    fn server_metadata_checks() {
        let issuer = Url::parse("https://as.example").unwrap();
        let s = check_server_metadata(&issuer, &good_metadata(), &safe_config()).unwrap();
        assert_eq!(s.issuer, "https://as.example");
        assert_eq!(s.token.as_str(), "https://as.example/oauth/token");
        let bad = |f: &dyn Fn(&mut Value)| {
            let mut m = good_metadata();
            f(&mut m);
            check_server_metadata(&issuer, &m, &safe_config()).unwrap_err()
        };
        bad(&|m| m["issuer"] = json!("https://other.example"));
        bad(&|m| m["issuer"] = json!("https://as.example/path"));
        bad(&|m| m["issuer"] = Value::Null);
        bad(&|m| m["require_pushed_authorization_requests"] = json!(false));
        bad(&|m| m["require_pushed_authorization_requests"] = Value::Null);
        bad(&|m| m["pushed_authorization_request_endpoint"] = Value::Null);
        bad(&|m| m["token_endpoint"] = json!("http://as.example/token"));
        bad(&|m| m["authorization_endpoint"] = json!("https://127.0.0.1/authorize"));
        bad(&|m| m["token_endpoint"] = json!("https://10.0.0.1/token"));
        bad(&|m| m["code_challenge_methods_supported"] = json!(["plain"]));
        bad(&|m| m["dpop_signing_alg_values_supported"] = json!(["ES256K"]));
        bad(&|m| m["scopes_supported"] = json!(["transition:generic"]));
        bad(&|m| m["response_types_supported"] = json!(["token"]));
    }

    #[test]
    fn pds_from_did_document() {
        let doc = json!({"id": "did:plc:x", "service": [
            {"id": "#other", "type": "X", "serviceEndpoint": "https://x.example"},
            {"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example"}
        ]});
        assert_eq!(pds_endpoint(&doc).unwrap().as_str(), "https://pds.example/");
        let with_path = json!({"service": [
            {"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example/x"}
        ]});
        assert_eq!(pds_endpoint(&with_path), None);
        assert_eq!(pds_endpoint(&json!({})), None);
    }

    #[test]
    fn token_response_checks() {
        let ok = json!({"token_type": "DPoP", "scope": "atproto", "sub": "did:plc:x", "access_token": "a"});
        assert_eq!(read_token_response(&ok).unwrap(), "did:plc:x");
        assert_eq!(
            read_token_response(
                &json!({"token_type": "dpop", "scope": "x atproto y", "sub": "did:web:a.example"})
            )
            .unwrap(),
            "did:web:a.example"
        );
        for bad in [
            json!({"token_type": "Bearer", "scope": "atproto", "sub": "did:plc:x"}),
            json!({"token_type": "DPoP", "scope": "atprotox", "sub": "did:plc:x"}),
            json!({"token_type": "DPoP", "sub": "did:plc:x"}),
            json!({"token_type": "DPoP", "scope": "atproto"}),
            json!({"token_type": "DPoP", "scope": "atproto", "sub": ""}),
        ] {
            assert!(read_token_response(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn flow_store_single_use_cookie_and_lifetime() {
        let store = FlowStore::new(Duration::from_secs(600), 4);
        let t0 = Instant::now();
        let (good, other) = ([1u8; 32], [2u8; 32]);
        store.insert("s1".into(), flow(t0, good));
        // Unknown state.
        assert!(matches!(store.take("nope", &good, t0), Taken::Absent));
        // The wrong cookie does not spend the flow.
        assert!(matches!(store.take("s1", &other, t0), Taken::WrongCookie));
        assert_eq!(store.len(), 1);
        // The right one takes it; a replay finds nothing.
        assert!(matches!(store.take("s1", &good, t0), Taken::Flow(_)));
        assert!(matches!(store.take("s1", &good, t0), Taken::Absent));
        // Older than the lifetime: absent although never swept.
        store.insert("s2".into(), flow(t0, good));
        let late = t0 + Duration::from_secs(601);
        assert!(matches!(store.take("s2", &good, late), Taken::Absent));
        assert!(store.is_empty());
        // Just inside the lifetime it is still there.
        store.insert("s3".into(), flow(t0, good));
        assert!(matches!(
            store.take("s3", &good, t0 + Duration::from_secs(600)),
            Taken::Flow(_)
        ));
        // The sweeper removes what expired and nothing else.
        store.insert("old".into(), flow(t0, good));
        store.insert("new".into(), flow(t0 + Duration::from_secs(500), good));
        store.sweep(late);
        assert_eq!(store.len(), 1);
        assert!(matches!(store.take("new", &good, late), Taken::Flow(_)));
    }

    #[test]
    fn flow_store_evicts_the_oldest_when_full() {
        let store = FlowStore::new(FLOW_TTL, MAX_FLOWS);
        let t0 = Instant::now();
        let c = [7u8; 32];
        for i in 0..MAX_FLOWS {
            store.insert(
                format!("s{i}"),
                flow(t0 + Duration::from_millis(i as u64), c),
            );
        }
        assert_eq!(store.len(), MAX_FLOWS);
        let now = t0 + Duration::from_secs(1);
        store.insert("extra".into(), flow(now, c));
        assert_eq!(store.len(), MAX_FLOWS);
        assert!(matches!(store.take("s0", &c, now), Taken::Absent));
        assert!(matches!(store.take("s1", &c, now), Taken::Flow(_)));
        assert!(matches!(store.take("extra", &c, now), Taken::Flow(_)));
    }

    #[test]
    fn mismatch_warning_is_limited() {
        let s = OAuthState::new("https://plc.example");
        assert_eq!(s.plc_url(), "https://plc.example");
        assert!(s.may_warn_mismatch());
        assert!(!s.may_warn_mismatch());
    }

    #[test]
    fn local_clients() {
        for ok in [
            "127.0.0.1",
            "127.8.0.3",
            "::1",
            // A container bridge's gateway, a private network.
            "172.18.0.1",
            "10.0.0.5",
            "192.168.1.20",
            "169.254.1.1",
            "fd00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
            "::ffff:172.17.0.1",
        ] {
            assert!(is_local_client(ok.parse().unwrap()), "{ok}");
        }
        for bad in [
            "203.0.113.9",
            "8.8.8.8",
            "100.64.0.1",
            "172.32.0.1",
            "2606:4700::1111",
            "::ffff:8.8.8.8",
            "0.0.0.0",
            "::",
        ] {
            assert!(!is_local_client(bad.parse().unwrap()), "{bad}");
        }
    }

    #[test]
    fn loopback_hosts() {
        for ok in ["127.0.0.1", "127.0.0.1:8080", "[::1]", "[::1]:18093"] {
            assert!(is_loopback_host(ok), "{ok}");
        }
        for bad in [
            "localhost",
            "localhost:8080",
            "127.0.0.2",
            "127.0.0.1:",
            "127.0.0.1:x",
            "::1",
            "127.0.0.10",
        ] {
            assert!(!is_loopback_host(bad), "{bad}");
        }
    }
}
