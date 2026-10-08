//! The bundled, dated Cloudflare edge ranges (see `docs/design/security.md`).
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

/// Most ranges a published list may hold. The lists hold about twenty.
pub const MAX_LIST_RANGES: usize = 500;

/// Parses a published list (one CIDR per line), ignoring blank lines.
/// Returns `None` if any line is not a CIDR, or is one that may not be
/// trusted as a proxy however it got into the list: broader than /8
/// (IPv4) or /24 (IPv6), the bounds of `proxy.trusted`, or a private,
/// loopback or link-local range. A refresh then keeps the old set, so a
/// list that reads `0.0.0.0/0` never makes every peer a trusted proxy.
pub fn parse_list(text: &str) -> Option<Vec<IpNet>> {
    let nets: Vec<IpNet> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(|l| l.parse().ok())
        .collect::<Option<_>>()?;
    let usable = nets.len() <= MAX_LIST_RANGES
        && nets.iter().all(|n| {
            crate::config::validate_trusted_proxy(n).is_ok() && crate::config::is_public_net(n)
        });
    usable.then_some(nets)
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

    #[test]
    fn a_published_list_is_held_to_the_trusted_proxy_bounds() {
        // The bundled lists pass as they are published.
        assert_eq!(
            parse_list(&IPV4.join("\n")).map(|v| v.len()),
            Some(IPV4.len())
        );
        assert_eq!(
            parse_list(&IPV6.join("\n")).map(|v| v.len()),
            Some(IPV6.len())
        );
        // One line that would trust everyone, or a network that is not
        // the internet's, refuses the whole list.
        for bad in [
            "0.0.0.0/0",
            "::/0",
            "104.16.0.0/13\n0.0.0.0/0",
            "64.0.0.0/2",
            "2000::/3",
            "10.0.0.0/8",
            "192.168.0.0/16",
            "127.0.0.0/8",
            "fd00::/24",
        ] {
            assert_eq!(parse_list(bad), None, "{bad}");
        }
        let many: String = (0..=MAX_LIST_RANGES)
            .map(|i| format!("198.{}.{}.0/24\n", 18 + i / 256, i % 256))
            .collect();
        assert_eq!(parse_list(&many), None);
    }
}
