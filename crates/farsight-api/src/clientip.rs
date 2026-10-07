//! Client IP resolution (see `docs/design/security.md`), proxy trust, the
//! "behind Cloudflare but not trusting it" detector and stripping of
//! forwarding headers from untrusted peers.

use std::net::{IpAddr, SocketAddr};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, Request};
use farsight_core::config::{ProxyConfig, ProxyMode};
use ipnet::IpNet;

/// Forwarding headers removed from requests of untrusted peers before
/// anything (handlers, logs) sees them.
pub const FORWARDING_HEADERS: [&str; 8] = [
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-proto",
    "x-forwarded-host",
    "x-real-ip",
    "cf-connecting-ip",
    "true-client-ip",
    "cf-visitor",
];

/// The forwarding headers as received, before untrusted ones were
/// stripped. Only the setup wizard's proxy preview reads it; nothing
/// logs it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OriginalForwarding(pub Vec<(String, String)>);

/// The resolved client, attached to every request as an extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientIp {
    /// The resolved client address (rate-limit key, logs).
    pub ip: IpAddr,
    /// Address of the socket peer, with IPv4-mapped IPv6 normalized to
    /// IPv4. Equal to `ip` unless the peer is a trusted proxy that named
    /// another client.
    pub peer: IpAddr,
    /// Whether `peer` is in the trusted set in force. Only then are the
    /// forwarding headers believed, and left on the request.
    pub trusted_peer: bool,
    /// The request reached the proxy (or us) over HTTPS: decides the
    /// cookie `Secure` flag.
    pub https: bool,
}

/// Normalizes IPv4-mapped IPv6 peers (dual-stack sockets) to IPv4.
pub fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    }
}

fn in_any(ip: IpAddr, nets: &[IpNet]) -> bool {
    nets.iter().any(|n| n.contains(&ip))
}

fn header_ip(headers: &HeaderMap, name: &str) -> Option<IpAddr> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<IpAddr>().ok())
        .map(canonical)
}

/// The algorithm:
///
/// ```text
/// if peer ∉ trusted:            client := peer   # all forwarding headers ignored
/// elif mode == cloudflare and CF-Connecting-IP parses:
///                               client := CF-Connecting-IP
/// elif X-Forwarded-For present: walk right→left, skip entries ∈ trusted;
///                               client := first ∉ trusted (else leftmost)
/// else:                         client := peer
/// ```
pub fn resolve(peer: IpAddr, headers: &HeaderMap, mode: ProxyMode, trusted: &[IpNet]) -> IpAddr {
    let peer = canonical(peer);
    if !in_any(peer, trusted) {
        return peer;
    }
    if mode == ProxyMode::Cloudflare
        && let Some(ip) = header_ip(headers, "cf-connecting-ip")
    {
        return ip;
    }
    let xff: Vec<IpAddr> = headers
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .filter_map(|p| p.trim().parse::<IpAddr>().ok().map(canonical))
        .collect();
    if xff.is_empty() {
        return peer;
    }
    xff.iter()
        .rev()
        .copied()
        .find(|ip| !in_any(*ip, trusted))
        .unwrap_or(xff[0])
}

/// Proxy trust in force: the configured CIDRs plus, in Cloudflare mode
/// with `cloudflare_refresh`, the last refreshed Cloudflare ranges.
#[derive(Debug, Default)]
pub struct ProxyTrust {
    refreshed_cf: RwLock<Option<Vec<IpNet>>>,
}

impl ProxyTrust {
    /// The effective trusted set for `cfg`.
    pub fn trusted(&self, cfg: &ProxyConfig) -> Vec<IpNet> {
        let mut v = cfg.trusted.clone();
        if cfg.mode == ProxyMode::Cloudflare
            && cfg.cloudflare_refresh
            && let Some(cf) = self
                .refreshed_cf
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
        {
            v.extend(cf.iter().copied());
        }
        v
    }

    /// Stores a refreshed Cloudflare range set.
    pub fn set_refreshed(&self, nets: Vec<IpNet>) {
        *self.refreshed_cf.write().unwrap_or_else(|e| e.into_inner()) = Some(nets);
    }
}

/// Resolves the client for one request and strips forwarding headers of
/// untrusted peers. In setup mode (`setup = true`) `X-Forwarded-Proto` is
/// honored for the `Secure` flag only, never for the client IP.
pub fn resolve_request<B>(
    req: &mut Request<B>,
    peer: SocketAddr,
    cfg: &ProxyConfig,
    trusted: &[IpNet],
    setup: bool,
) -> ClientIp {
    let peer_ip = canonical(peer.ip());
    let trusted_peer = in_any(peer_ip, trusted);
    let xfp_https = req
        .headers()
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("https"));
    let https = (trusted_peer || setup) && xfp_https;
    let ip = resolve(peer_ip, req.headers(), cfg.mode, trusted);
    if !trusted_peer {
        for h in FORWARDING_HEADERS {
            req.headers_mut().remove(h);
        }
    }
    ClientIp {
        ip,
        peer: peer_ip,
        trusted_peer,
        https,
    }
}

/// Window of the Cloudflare-share detector (5 minutes).
pub const CF_WINDOW: Duration = Duration::from_secs(300);

#[derive(Debug)]
struct CfWindow {
    start: Instant,
    total: u64,
    cf: u64,
    last: Option<(u64, u64)>,
}

/// Counts requests whose peer is a Cloudflare edge while that peer is not
/// trusted: if more than half of a 5-minute window comes from Cloudflare
/// ranges, Farsight is behind Cloudflare without trusting it and every
/// client shares a few rate-limit buckets.
#[derive(Debug)]
pub struct CfTracker {
    cf: Vec<IpNet>,
    w: Mutex<CfWindow>,
}

impl Default for CfTracker {
    fn default() -> Self {
        CfTracker {
            cf: farsight_core::cloudflare::bundled(),
            w: Mutex::new(CfWindow {
                start: Instant::now(),
                total: 0,
                cf: 0,
                last: None,
            }),
        }
    }
}

/// Minimum requests in a window before the detector judges it.
const CF_MIN_REQUESTS: u64 = 20;

impl CfTracker {
    /// Counts one request in the current window, first rolling the window
    /// over when it is older than [`CF_WINDOW`]. A window that ended more
    /// than one window ago is not kept as the last complete one.
    pub fn record(&self, c: &ClientIp) {
        let from_cf = !c.trusted_peer && in_any(c.peer, &self.cf);
        let mut w = self.w.lock().unwrap_or_else(|e| e.into_inner());
        if w.start.elapsed() >= CF_WINDOW {
            if w.start.elapsed() < CF_WINDOW * 2 {
                w.last = Some((w.total, w.cf));
            } else {
                w.last = None;
            }
            w.start = Instant::now();
            w.total = 0;
            w.cf = 0;
        }
        w.total += 1;
        if from_cf {
            w.cf += 1;
        }
    }

    /// `Some(share)` when more than 50% of the last complete window's
    /// requests (or the current one's, before a window completes) came
    /// from untrusted Cloudflare peers.
    pub fn warning(&self) -> Option<f64> {
        let w = self.w.lock().unwrap_or_else(|e| e.into_inner());
        let (total, cf) = w.last.unwrap_or((w.total, w.cf));
        if total < CF_MIN_REQUESTS {
            return None;
        }
        let share = cf as f64 / total as f64;
        (share > 0.5).then_some(share)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn nets(v: &[&str]) -> Vec<IpNet> {
        v.iter().map(|s| s.parse().unwrap()).collect()
    }

    fn h(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut m = HeaderMap::new();
        for (k, v) in pairs {
            m.append(*k, HeaderValue::from_str(v).unwrap());
        }
        m
    }

    #[test]
    fn untrusted_peer_ignores_headers() {
        let t = nets(&["10.0.0.0/8"]);
        let ip = resolve(
            "203.0.113.9".parse().unwrap(),
            &h(&[
                ("cf-connecting-ip", "1.1.1.1"),
                ("x-forwarded-for", "2.2.2.2"),
            ]),
            ProxyMode::Cloudflare,
            &t,
        );
        assert_eq!(ip, "203.0.113.9".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn cloudflare_header_from_trusted_peer() {
        let t = nets(&["10.0.0.0/8"]);
        let ip = resolve(
            "10.1.1.1".parse().unwrap(),
            &h(&[("cf-connecting-ip", "198.51.100.7")]),
            ProxyMode::Cloudflare,
            &t,
        );
        assert_eq!(ip, "198.51.100.7".parse::<IpAddr>().unwrap());
        // In forwarded mode CF-Connecting-IP is not used.
        let ip = resolve(
            "10.1.1.1".parse().unwrap(),
            &h(&[("cf-connecting-ip", "198.51.100.7")]),
            ProxyMode::Forwarded,
            &t,
        );
        assert_eq!(ip, "10.1.1.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn xff_walk() {
        let t = nets(&["10.0.0.0/8"]);
        let peer: IpAddr = "10.0.0.2".parse().unwrap();
        let ip = resolve(
            peer,
            &h(&[("x-forwarded-for", "6.6.6.6, 198.51.100.1, 10.0.0.5")]),
            ProxyMode::Forwarded,
            &t,
        );
        assert_eq!(ip, "198.51.100.1".parse::<IpAddr>().unwrap());
        // Every entry trusted ⇒ leftmost.
        let ip = resolve(
            peer,
            &h(&[("x-forwarded-for", "10.0.0.9, 10.0.0.5")]),
            ProxyMode::Forwarded,
            &t,
        );
        assert_eq!(ip, "10.0.0.9".parse::<IpAddr>().unwrap());
        // Several header lines are one list.
        let ip = resolve(
            peer,
            &h(&[
                ("x-forwarded-for", "7.7.7.7"),
                ("x-forwarded-for", "10.0.0.3"),
            ]),
            ProxyMode::Forwarded,
            &t,
        );
        assert_eq!(ip, "7.7.7.7".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn strips_headers_of_untrusted_peers() {
        let mut req = Request::builder()
            .header("x-forwarded-for", "1.2.3.4")
            .header("x-forwarded-proto", "https")
            .body(())
            .unwrap();
        let cfg = ProxyConfig::default();
        let c = resolve_request(&mut req, "8.8.8.8:1234".parse().unwrap(), &cfg, &[], true);
        assert!(req.headers().get("x-forwarded-for").is_none());
        // Setup mode: the proto still sets the Secure flag.
        assert!(c.https);
        let mut req = Request::builder()
            .header("x-forwarded-proto", "https")
            .body(())
            .unwrap();
        let c = resolve_request(&mut req, "8.8.8.8:1234".parse().unwrap(), &cfg, &[], false);
        assert!(!c.https);
    }

    #[test]
    fn cf_share_detector() {
        let t = CfTracker::default();
        let cf = ClientIp {
            ip: "104.16.0.1".parse().unwrap(),
            peer: "104.16.0.1".parse().unwrap(),
            trusted_peer: false,
            https: false,
        };
        let other = ClientIp {
            peer: "203.0.113.1".parse().unwrap(),
            ..cf
        };
        for _ in 0..15 {
            t.record(&cf);
        }
        for _ in 0..10 {
            t.record(&other);
        }
        assert!(t.warning().is_some());
        let trusted = ClientIp {
            trusted_peer: true,
            ..cf
        };
        let t2 = CfTracker::default();
        for _ in 0..30 {
            t2.record(&trusted);
        }
        assert!(t2.warning().is_none());
    }
}
