//! Safe outbound HTTP client scaffolding (design §11.3).
//!
//! Every request to an address learned from the network (DID documents,
//! did:web hosts, handle domains, PDS endpoints, backlink results) goes
//! through one client that requires `https` (except operator-allowlisted
//! hosts), resolves DNS itself and refuses non-public addresses on every
//! hop (max 3 redirects), and applies the size/time caps of §5.2.
//!
//! [`OutboundClient`] is the contract (tests substitute a fake);
//! [`SafeClient`] is the production implementation.

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use url::Url;

use crate::config::Config;

/// Maximum redirects followed (§11.3).
pub const MAX_REDIRECTS: u8 = 3;
/// Per-request timeout (§5.2 bounds).
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Response body cap (§5.2 bounds).
pub const MAX_BODY_BYTES: u64 = 2 * 1024 * 1024;

/// Settings for [`SafeClient`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafeClientConfig {
    /// Hosts that may be reached over plain `http` (`net.allow_http_hosts`).
    pub allow_http_hosts: Vec<String>,
    /// Redirect limit; every hop is re-checked.
    pub max_redirects: u8,
    /// Whole-request timeout.
    pub timeout: Duration,
    /// Body size cap.
    pub max_body_bytes: u64,
    /// `farsight/<version> (+https://<hostname>; <contact>)` (§5.3).
    pub user_agent: String,
}

impl SafeClientConfig {
    /// Builds the client settings from the loaded config.
    pub fn from_config(config: &Config, version: &str) -> SafeClientConfig {
        SafeClientConfig {
            allow_http_hosts: config
                .net
                .allow_http_hosts
                .iter()
                .map(|h| h.to_ascii_lowercase())
                .collect(),
            max_redirects: MAX_REDIRECTS,
            timeout: REQUEST_TIMEOUT,
            max_body_bytes: MAX_BODY_BYTES,
            user_agent: format!(
                "farsight/{version} (+https://{}; {})",
                config.server.hostname, config.server.contact
            ),
        }
    }
}

/// Why an outbound request was refused or failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OutboundError {
    /// The URL could not be parsed.
    #[error("invalid URL: {0}")]
    InvalidUrl(String),
    /// Scheme other than `https` (or `http` for an allowlisted host).
    #[error("scheme not allowed: {0}")]
    Scheme(String),
    /// URL carries credentials, or has no host.
    #[error("URL not allowed: {0}")]
    Url(String),
    /// The host resolved to (or is) a forbidden address.
    #[error("address {addr} not allowed: {reason}")]
    ForbiddenAddress {
        /// The address.
        addr: IpAddr,
        /// Which rule refused it.
        reason: &'static str,
    },
    /// Redirect limit exceeded.
    #[error("too many redirects")]
    TooManyRedirects,
    /// Response exceeded the body cap.
    #[error("response body exceeds {0} bytes")]
    TooLarge(u64),
    /// Timed out.
    #[error("request timed out")]
    Timeout,
    /// Transport error.
    #[error("transport error: {0}")]
    Transport(String),
}

/// A completed response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundResponse {
    /// HTTP status.
    pub status: u16,
    /// Response headers (lowercased names).
    pub headers: Vec<(String, String)>,
    /// Body, at most `max_body_bytes`.
    pub body: Vec<u8>,
    /// The URL after redirects.
    pub final_url: Url,
}

/// The one way Farsight talks to network-learned addresses. Ingest and
/// backfill take an implementation of this trait so tests can substitute a
/// fake.
pub trait OutboundClient: Send + Sync {
    /// `GET url`, following at most [`MAX_REDIRECTS`] redirects with every
    /// hop checked by [`check_url`] and every resolved address by
    /// [`blocked_ip_reason`].
    fn get(
        &self,
        url: &Url,
    ) -> impl Future<Output = Result<OutboundResponse, OutboundError>> + Send;
}

/// The production [`OutboundClient`]: resolves names itself (refusing
/// non-public addresses), connects by the vetted address with SNI for the
/// name, follows at most [`MAX_REDIRECTS`] redirects re-checking every hop,
/// and enforces the timeout and body cap.
#[derive(Clone)]
pub struct SafeClient {
    config: SafeClientConfig,
    http: reqwest::Client,
    dns: std::sync::Arc<hickory_resolver::TokioResolver>,
}

impl std::fmt::Debug for SafeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SafeClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Resolver used by the HTTP client: every resolved address is checked
/// with [`blocked_ip_reason`]; forbidden ones are dropped, and a name with
/// only forbidden addresses fails.
#[derive(Clone)]
struct VettingResolver {
    dns: std::sync::Arc<hickory_resolver::TokioResolver>,
}

impl reqwest::dns::Resolve for VettingResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let dns = self.dns.clone();
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let ips = dns
                .lookup_ip(host.as_str())
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
            let mut first_refusal = None;
            let mut allowed = Vec::new();
            for ip in ips.iter() {
                match blocked_ip_reason(ip) {
                    None => allowed.push(std::net::SocketAddr::new(ip, 0)),
                    Some(reason) => {
                        first_refusal
                            .get_or_insert(OutboundError::ForbiddenAddress { addr: ip, reason });
                    }
                }
            }
            if allowed.is_empty() {
                let err = first_refusal
                    .unwrap_or_else(|| OutboundError::Transport(format!("{host}: no addresses")));
                return Err(Box::new(err) as Box<dyn std::error::Error + Send + Sync>);
            }
            let addrs: reqwest::dns::Addrs = Box::new(allowed.into_iter());
            Ok(addrs)
        })
    }
}

fn system_resolver() -> hickory_resolver::TokioResolver {
    use hickory_resolver::config::ResolverConfig;
    use hickory_resolver::name_server::TokioConnectionProvider;
    match hickory_resolver::TokioResolver::builder_tokio() {
        Ok(b) => b.build(),
        Err(_) => hickory_resolver::TokioResolver::builder_with_config(
            ResolverConfig::default(),
            TokioConnectionProvider::default(),
        )
        .build(),
    }
}

impl SafeClient {
    /// Creates a client using the system DNS configuration.
    pub fn new(config: SafeClientConfig) -> SafeClient {
        let dns = std::sync::Arc::new(system_resolver());
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .dns_resolver(std::sync::Arc::new(VettingResolver { dns: dns.clone() }))
            .user_agent(config.user_agent.clone())
            .timeout(config.timeout)
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("HTTP client builds");
        SafeClient { config, http, dns }
    }

    /// The client's settings.
    pub fn config(&self) -> &SafeClientConfig {
        &self.config
    }

    /// TXT records of `name` (e.g. `_atproto.<handle>`, design §8.6). A
    /// missing name or record set is an empty list.
    pub async fn txt(&self, name: &str) -> Result<Vec<String>, OutboundError> {
        match self.dns.txt_lookup(name).await {
            Ok(r) => Ok(r
                .iter()
                .map(|t| {
                    t.txt_data()
                        .iter()
                        .map(|d| String::from_utf8_lossy(d).into_owned())
                        .collect::<String>()
                })
                .collect()),
            Err(e) if e.is_no_records_found() => Ok(Vec::new()),
            Err(e) => Err(OutboundError::Transport(e.to_string())),
        }
    }

    /// The addresses `host` resolves to (cap buckets by address block,
    /// §11.2). Resolution only; nothing is contacted.
    pub async fn lookup_ip(&self, host: &str) -> Result<Vec<IpAddr>, OutboundError> {
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![ip]);
        }
        self.dns
            .lookup_ip(host)
            .await
            .map(|r| r.iter().collect())
            .map_err(|e| OutboundError::Transport(e.to_string()))
    }

    async fn get_once(&self, url: &Url) -> Result<OutboundResponse, OutboundError> {
        let mut resp = self
            .http
            .get(url.clone())
            .send()
            .await
            .map_err(map_reqwest)?;
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_ascii_lowercase(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect();
        if resp
            .content_length()
            .is_some_and(|len| len > self.config.max_body_bytes)
        {
            return Err(OutboundError::TooLarge(self.config.max_body_bytes));
        }
        let mut body = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(map_reqwest)? {
            if (body.len() + chunk.len()) as u64 > self.config.max_body_bytes {
                return Err(OutboundError::TooLarge(self.config.max_body_bytes));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(OutboundResponse {
            status,
            headers,
            body,
            final_url: url.clone(),
        })
    }
}

fn map_reqwest(e: reqwest::Error) -> OutboundError {
    if e.is_timeout() {
        return OutboundError::Timeout;
    }
    // A refusal from the vetting resolver surfaces as the error source.
    let mut src: Option<&(dyn std::error::Error + 'static)> = Some(&e);
    while let Some(s) = src {
        if let Some(o) = s.downcast_ref::<OutboundError>() {
            return o.clone();
        }
        src = s.source();
    }
    OutboundError::Transport(e.to_string())
}

impl OutboundClient for SafeClient {
    fn get(
        &self,
        url: &Url,
    ) -> impl Future<Output = Result<OutboundResponse, OutboundError>> + Send {
        let start = url.clone();
        async move {
            let mut url = start;
            let mut hops = 0u8;
            loop {
                check_url(&url, &self.config)?;
                let resp = self.get_once(&url).await?;
                if !(300..400).contains(&resp.status) {
                    return Ok(resp);
                }
                let Some((_, location)) = resp.headers.iter().find(|(k, _)| k == "location") else {
                    return Ok(resp);
                };
                if hops >= self.config.max_redirects {
                    return Err(OutboundError::TooManyRedirects);
                }
                hops += 1;
                url = url
                    .join(location)
                    .map_err(|e| OutboundError::InvalidUrl(e.to_string()))?;
            }
        }
    }
}

/// Checks a URL against the scheme and shape rules (not DNS).
pub fn check_url(url: &Url, config: &SafeClientConfig) -> Result<(), OutboundError> {
    if !url.username().is_empty() || url.password().is_some() {
        return Err(OutboundError::Url(url.to_string()));
    }
    let host = url
        .host_str()
        .ok_or_else(|| OutboundError::Url(url.to_string()))?
        .to_ascii_lowercase();
    match url.scheme() {
        "https" => {}
        "http" if config.allow_http_hosts.contains(&host) => {}
        other => return Err(OutboundError::Scheme(other.to_owned())),
    }
    // Literal IPs are checked here; names are checked after resolution.
    if let Some(url::Host::Ipv4(a)) = url.host() {
        if let Some(reason) = blocked_ip_reason(IpAddr::V4(a)) {
            return Err(OutboundError::ForbiddenAddress {
                addr: IpAddr::V4(a),
                reason,
            });
        }
    }
    if let Some(url::Host::Ipv6(a)) = url.host() {
        if let Some(reason) = blocked_ip_reason(IpAddr::V6(a)) {
            return Err(OutboundError::ForbiddenAddress {
                addr: IpAddr::V6(a),
                reason,
            });
        }
    }
    Ok(())
}

fn v4_reason(a: Ipv4Addr) -> Option<&'static str> {
    let o = a.octets();
    if a.is_unspecified() || o[0] == 0 {
        Some("unspecified / this-network")
    } else if a.is_loopback() {
        Some("loopback")
    } else if a.is_private() {
        Some("private (RFC 1918)")
    } else if a.is_link_local() {
        Some("link-local (incl. cloud metadata)")
    } else if o[0] == 100 && (o[1] & 0xc0) == 64 {
        Some("CGNAT (100.64.0.0/10)")
    } else if a.is_multicast() {
        Some("multicast")
    } else if a.is_broadcast() || o[0] >= 240 {
        Some("reserved / broadcast")
    } else {
        None
    }
}

fn v6_reason(a: Ipv6Addr) -> Option<&'static str> {
    let s = a.segments();
    if a.is_unspecified() {
        Some("unspecified")
    } else if a.is_loopback() {
        Some("loopback")
    } else if (s[0] & 0xfe00) == 0xfc00 {
        Some("unique local (ULA)")
    } else if (s[0] & 0xffc0) == 0xfe80 {
        Some("link-local")
    } else if a.is_multicast() {
        Some("multicast")
    } else if let Some(v4) = a.to_ipv4_mapped() {
        v4_reason(v4)
    } else if s[0] == 0x64 && s[1] == 0xff9b && s[2..6] == [0, 0, 0, 0] {
        // NAT64 well-known prefix: judge the embedded IPv4 address.
        let v4 = Ipv4Addr::new((s[6] >> 8) as u8, s[6] as u8, (s[7] >> 8) as u8, s[7] as u8);
        v4_reason(v4)
    } else {
        None
    }
}

/// Why an address must not be contacted, or `None` if it is public:
/// loopback, private (RFC 1918, ULA), link-local (incl. 169.254.169.254),
/// CGNAT, multicast, unspecified and reserved addresses are refused
/// (§11.3), including IPv4 addresses embedded in IPv6 forms.
pub fn blocked_ip_reason(ip: IpAddr) -> Option<&'static str> {
    match ip {
        IpAddr::V4(a) => v4_reason(a),
        IpAddr::V6(a) => v6_reason(a),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> SafeClientConfig {
        SafeClientConfig {
            allow_http_hosts: vec!["pds.dev.test".to_owned()],
            max_redirects: MAX_REDIRECTS,
            timeout: REQUEST_TIMEOUT,
            max_body_bytes: MAX_BODY_BYTES,
            user_agent: "farsight/test".to_owned(),
        }
    }

    #[test]
    fn blocks_non_public_addresses() {
        for bad in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "100.127.255.255",
            "224.0.0.1",
            "0.0.0.0",
            "0.1.2.3",
            "255.255.255.255",
            "240.0.0.1",
            "::",
            "::1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "ff02::1",
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
            "64:ff9b::a00:1",
        ] {
            let ip: IpAddr = bad.parse().unwrap();
            assert!(blocked_ip_reason(ip).is_some(), "allowed {bad}");
        }
        for ok in [
            "1.1.1.1",
            "100.128.0.1",
            "8.8.8.8",
            "2606:4700::1111",
            "::ffff:8.8.8.8",
        ] {
            let ip: IpAddr = ok.parse().unwrap();
            assert_eq!(blocked_ip_reason(ip), None, "blocked {ok}");
        }
    }

    #[test]
    fn url_rules() {
        let c = cfg();
        let ok = |s: &str| check_url(&Url::parse(s).unwrap(), &c);
        assert!(ok("https://pds.example.com/xrpc/com.atproto.sync.getLatestCommit").is_ok());
        assert!(ok("http://pds.dev.test/xrpc/x").is_ok());
        assert!(matches!(
            ok("http://pds.example.com/"),
            Err(OutboundError::Scheme(_))
        ));
        assert!(matches!(
            ok("ftp://pds.example.com/"),
            Err(OutboundError::Scheme(_))
        ));
        assert!(matches!(
            ok("https://user:pw@pds.example.com/"),
            Err(OutboundError::Url(_))
        ));
        assert!(matches!(
            ok("https://169.254.169.254/latest/meta-data"),
            Err(OutboundError::ForbiddenAddress { .. })
        ));
        assert!(matches!(
            ok("https://[::1]/"),
            Err(OutboundError::ForbiddenAddress { .. })
        ));
    }

    #[test]
    fn user_agent_from_config() {
        let mut config = Config::default();
        config.server.hostname = "farsight.test".to_owned();
        config.server.contact = "mailto:ops@farsight.test".to_owned();
        let c = SafeClientConfig::from_config(&config, "1.0.0");
        assert_eq!(
            c.user_agent,
            "farsight/1.0.0 (+https://farsight.test; mailto:ops@farsight.test)"
        );
    }
}
