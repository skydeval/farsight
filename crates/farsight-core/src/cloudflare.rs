//! The bundled, dated Cloudflare edge ranges (design §9.1, §9.3, §11.2).
//!
//! Used by the wizard's "Cloudflare" proxy preset, by the dashboard's
//! "behind Cloudflare but not trusting it" detection, and as shared
//! anycast ranges that are never used as cap buckets. `proxy.cloudflare_refresh`
//! opts into a daily refresh from Cloudflare's published lists.

use ipnet::IpNet;

/// When the bundled set was taken from <https://www.cloudflare.com/ips/>.
pub const BUNDLED_AS_OF: &str = "2026-10-01";

/// Published IPv4 ranges as of [`BUNDLED_AS_OF`].
pub const IPV4: &[&str] = &[
    "173.245.48.0/20",
    "103.21.244.0/22",
    "103.22.200.0/22",
    "103.31.4.0/22",
    "141.101.64.0/18",
    "108.162.192.0/18",
    "190.93.240.0/20",
    "188.114.96.0/20",
    "197.234.240.0/22",
    "198.41.128.0/17",
    "162.158.0.0/15",
    "104.16.0.0/13",
    "104.24.0.0/14",
    "172.64.0.0/13",
    "131.0.72.0/22",
];

/// Published IPv6 ranges as of [`BUNDLED_AS_OF`].
pub const IPV6: &[&str] = &[
    "2400:cb00::/32",
    "2606:4700::/32",
    "2803:f800::/32",
    "2405:b500::/32",
    "2405:8100::/32",
    "2a06:98c0::/29",
    "2c0f:f248::/32",
];

/// Where the refresh fetches the current lists.
pub const REFRESH_URLS: [&str; 2] = [
    "https://www.cloudflare.com/ips-v4",
    "https://www.cloudflare.com/ips-v6",
];

/// The bundled ranges, parsed.
pub fn bundled() -> Vec<IpNet> {
    IPV4.iter()
        .chain(IPV6)
        .map(|s| s.parse().expect("bundled ranges parse"))
        .collect()
}

/// Parses a published list (one CIDR per line), ignoring blank lines.
/// Returns `None` if any line is not a CIDR (a refresh then keeps the old
/// set).
pub fn parse_list(text: &str) -> Option<Vec<IpNet>> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(|l| l.parse().ok())
        .collect()
}

/// Whether `net` is inside the bundled set.
pub fn contains_net(net: &IpNet) -> bool {
    bundled().iter().any(|cf| cf.contains(net))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_parses() {
        assert_eq!(bundled().len(), IPV4.len() + IPV6.len());
        assert!(contains_net(&"104.16.1.0/24".parse().unwrap()));
        assert!(!contains_net(&"10.0.0.0/8".parse().unwrap()));
        assert_eq!(parse_list("1.2.3.0/24\n\n").map(|v| v.len()), Some(1));
        assert_eq!(parse_list("nope"), None);
    }
}
