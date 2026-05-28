//! LAN reachability for the local fs MCP server.
//!
//! OpenCode runs on the same reachable network as AionUi (a LAN, or a
//! mesh/VPN such as Tailscale/WireGuard), so it can dial straight back to
//! the operator's machine — no public tunnel is needed. This module's job
//! is to decide *which local IP to advertise* so that the dial-back
//! actually succeeds.
//!
//! The hard part is that a multi-homed host (Wi-Fi + Ethernet + several
//! `utun`/VPN interfaces) has many local IPs, and only some are reachable
//! back from OpenCode. Rather than guess one and hope, we produce an
//! *ordered list of candidates*; the caller registers each with OpenCode,
//! forces a dial-back, and keeps the first that actually reaches us (see
//! `opencode_mcp::start_and_register`). This makes selection a measurement
//! instead of a guess, and is robust to VPNs, asymmetric routing, and
//! interface changes.
//!
//! Candidate order:
//! 1. `AIONUI_LOCAL_FS_MCP_PUBLIC_URL` env var — explicit override for
//!    weird setups (containers, NAT exceptions, pre-existing tunnels).
//!    When set it is the *only* candidate.
//! 2. The IP the OS would use to reach OpenCode's host (route-source IP).
//!    Almost always the right answer on a flat LAN.
//! 3. Every other non-loopback, non-link-local interface IP (Ethernet,
//!    Wi-Fi, and mesh/VPN addresses like Tailscale `100.x`). These cover
//!    the cases where the route-source IP cannot be dialed back.
//! 4. Loopback last — only works if OpenCode shares this host.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs, UdpSocket};

use reqwest::Url;
use tracing::{info, warn};

use crate::manager::remote::opencode_mcp::PUBLIC_URL_ENV;

/// A reachable endpoint to register with OpenCode, built from a candidate
/// once the MCP server's OS-assigned port is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reachable {
    /// URL the remote OpenCode should dial.
    pub public_url: String,
    /// How the URL was selected (for logging/UI).
    pub provider: &'static str,
}

/// One advertised-IP candidate. `provider` is a human label for logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub ip: IpAddr,
    pub provider: &'static str,
}

/// The plan for exposing the client MCP server: where to bind, and which
/// advertised URLs to try (in order) when registering with OpenCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// User-supplied URL; bind loopback (only the URL ever leaves the box,
    /// reachability is the user's tunnel's problem). Single candidate.
    Override { public_url: String },
    /// Auto-resolved: bind all interfaces, try these advertised IPs in
    /// order until one is verified reachable from OpenCode.
    Auto { candidates: Vec<Candidate> },
}

/// Build the reachability plan. `opencode_base_url` is the OpenCode HTTP
/// base (e.g. `http://192.168.0.5:4096`), used to compute the route-source
/// IP and to order candidates.
pub fn plan(opencode_base_url: &str) -> Plan {
    if let Ok(public) = std::env::var(PUBLIC_URL_ENV) {
        info!(public_url = %public, "using user-supplied URL from {PUBLIC_URL_ENV}");
        return Plan::Override { public_url: public };
    }

    let route_ip = routable_source_ip(opencode_base_url);
    let mut candidates: Vec<Candidate> = Vec::new();

    // 1. Route-source IP first — the OS's own answer to "what address
    //    reaches OpenCode", and the right one on a flat LAN.
    if let Some(ip) = route_ip {
        candidates.push(Candidate {
            ip,
            provider: "lan-route",
        });
    }

    // 2. Every other usable interface IP, so a wrong route guess (VPN
    //    captured the route, asymmetric routing, etc.) still has fallbacks.
    for ip in interface_ips() {
        if Some(ip) == route_ip {
            continue; // already added as lan-route
        }
        if candidates.iter().any(|c| c.ip == ip) {
            continue; // dedup
        }
        candidates.push(Candidate {
            ip,
            provider: classify(ip),
        });
    }

    // 3. Loopback last — only reachable if OpenCode shares this host.
    let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
    if !candidates.iter().any(|c| c.ip == loopback) {
        candidates.push(Candidate {
            ip: loopback,
            provider: "loopback",
        });
    }

    if route_ip.is_none() {
        warn!(
            opencode = %opencode_base_url,
            candidate_count = candidates.len(),
            "could not resolve a route-source IP; relying on interface enumeration + verification. \
             Set {PUBLIC_URL_ENV} to pin a URL if auto-selection fails."
        );
    } else {
        info!(
            candidate_count = candidates.len(),
            "resolved local fs MCP reachability candidates"
        );
    }

    Plan::Auto { candidates }
}

impl Plan {
    /// `SocketAddr` to pass to `LocalFsMcpServer::start`. Always port 0
    /// (OS-assigned).
    pub fn bind_addr(&self) -> SocketAddr {
        match self {
            // Bind all interfaces so any candidate IP routes to the same
            // listener; the registered URL is what picks the interface.
            Self::Auto { .. } => SocketAddr::from(([0, 0, 0, 0], 0)),
            // Override: the public URL is someone else's tunnel proxying
            // in, so we only need loopback locally.
            Self::Override { .. } => SocketAddr::from(([127, 0, 0, 1], 0)),
        }
    }

    /// Ordered list of URLs to register with OpenCode, given the server's
    /// OS-assigned port. The caller tries them in order.
    pub fn reachables(&self, bound_port: u16) -> Vec<Reachable> {
        match self {
            Self::Override { public_url } => vec![Reachable {
                public_url: public_url.clone(),
                provider: "env-override",
            }],
            Self::Auto { candidates } => candidates
                .iter()
                .map(|c| Reachable {
                    public_url: format!("http://{}/", SocketAddr::new(c.ip, bound_port)),
                    provider: c.provider,
                })
                .collect(),
        }
    }
}

/// Classify an interface IP for logging: mesh/VPN (CGNAT 100.64/10, used
/// by Tailscale), private LAN, or other.
fn classify(ip: IpAddr) -> &'static str {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            // 100.64.0.0/10 — CGNAT range, Tailscale and friends.
            if o[0] == 100 && (64..=127).contains(&o[1]) {
                "mesh"
            } else if v4.is_private() {
                "lan"
            } else {
                "interface"
            }
        }
        IpAddr::V6(v6) => {
            // Unique-local fc00::/7 behaves like a private LAN range.
            if (v6.segments()[0] & 0xfe00) == 0xfc00 {
                "lan"
            } else {
                "interface"
            }
        }
    }
}

/// Enumerate usable local interface IPs: skip loopback, link-local,
/// unspecified, and multicast. IPv4 first (most LANs), then IPv6.
fn interface_ips() -> Vec<IpAddr> {
    let Ok(ifaces) = if_addrs::get_if_addrs() else {
        return Vec::new();
    };
    let mut v4: Vec<IpAddr> = Vec::new();
    let mut v6: Vec<IpAddr> = Vec::new();
    for iface in ifaces {
        let ip = iface.ip();
        if !is_usable_candidate(ip) {
            continue;
        }
        match ip {
            IpAddr::V4(_) => v4.push(ip),
            IpAddr::V6(_) => v6.push(ip),
        }
    }
    v4.extend(v6);
    v4
}

/// Whether an interface IP is worth advertising to a remote OpenCode.
fn is_usable_candidate(ip: IpAddr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() {
        return false;
    }
    match ip {
        // 169.254.0.0/16 — IPv4 link-local (APIPA), not routable.
        IpAddr::V4(v4) => !v4.is_link_local(),
        // fe80::/10 — IPv6 link-local, needs a scope id; skip.
        IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) != 0xfe80,
    }
}

/// The local IP the OS would currently use to reach OpenCode. Cheap (no
/// packets sent); the guardian polls this to detect network changes
/// (VPN toggle, DHCP renewal, Wi-Fi handoff) that invalidate the
/// advertised URL.
pub fn current_route_ip(opencode_base_url: &str) -> Option<IpAddr> {
    routable_source_ip(opencode_base_url)
}

/// Open a UDP socket and `connect` it (no packets sent) to OpenCode's
/// host. The OS picks the source IP it would use to route there; we read
/// it off `local_addr`. The standard "what's my IP that can reach X"
/// trick without external lookups.
fn routable_source_ip(opencode_base_url: &str) -> Option<IpAddr> {
    let url = Url::parse(opencode_base_url).ok()?;
    let host = url.host_str()?;
    let port = url.port_or_known_default()?;
    let target = (host, port).to_socket_addrs().ok()?.next()?;

    let bind = match target {
        SocketAddr::V4(_) => "0.0.0.0:0",
        SocketAddr::V6(_) => "[::]:0",
    };
    let socket = UdpSocket::bind(bind).ok()?;
    socket.connect(target).ok()?;
    let local = socket.local_addr().ok()?;
    let ip = local.ip();
    if ip.is_unspecified() { None } else { Some(ip) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn override_plan_is_single_candidate_loopback_bind() {
        // SAFETY: test-only env mutation, serialized within this test.
        unsafe { std::env::set_var(PUBLIC_URL_ENV, "https://my.tunnel/foo") };
        let p = plan("http://192.168.0.5:4096");
        unsafe { std::env::remove_var(PUBLIC_URL_ENV) };

        assert_eq!(
            p,
            Plan::Override {
                public_url: "https://my.tunnel/foo".into()
            }
        );
        assert_eq!(p.bind_addr().ip(), IpAddr::from([127, 0, 0, 1]));
        let r = p.reachables(0);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].public_url, "https://my.tunnel/foo");
        assert_eq!(r[0].provider, "env-override");
    }

    #[test]
    fn auto_plan_binds_all_interfaces_and_builds_urls() {
        let p = Plan::Auto {
            candidates: vec![
                Candidate {
                    ip: IpAddr::from([192, 168, 0, 42]),
                    provider: "lan-route",
                },
                Candidate {
                    ip: IpAddr::from([100, 100, 1, 2]),
                    provider: "mesh",
                },
            ],
        };
        assert_eq!(p.bind_addr().ip(), IpAddr::from([0, 0, 0, 0]));
        let r = p.reachables(12345);
        assert_eq!(r[0].public_url, "http://192.168.0.42:12345/");
        assert_eq!(r[0].provider, "lan-route");
        assert_eq!(r[1].public_url, "http://100.100.1.2:12345/");
        assert_eq!(r[1].provider, "mesh");
    }

    #[test]
    fn auto_plan_always_includes_loopback_last() {
        let p = plan("http://192.168.0.5:4096");
        let Plan::Auto { candidates } = &p else {
            panic!("expected auto plan");
        };
        let last = candidates.last().expect("at least loopback");
        assert_eq!(last.ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(last.provider, "loopback");
        // No duplicate IPs.
        for (i, a) in candidates.iter().enumerate() {
            for b in &candidates[i + 1..] {
                assert_ne!(a.ip, b.ip, "duplicate candidate IP {}", a.ip);
            }
        }
    }

    #[test]
    fn classify_buckets() {
        assert_eq!(classify(IpAddr::from([100, 100, 0, 1])), "mesh");
        assert_eq!(classify(IpAddr::from([192, 168, 1, 1])), "lan");
        assert_eq!(classify(IpAddr::from([10, 0, 0, 1])), "lan");
        assert_eq!(classify(IpAddr::from([8, 8, 8, 8])), "interface");
    }

    #[test]
    fn link_local_and_loopback_are_not_candidates() {
        assert!(!is_usable_candidate(IpAddr::from([169, 254, 1, 1])));
        assert!(!is_usable_candidate(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(is_usable_candidate(IpAddr::from([192, 168, 0, 5])));
    }

    #[test]
    fn routable_ip_for_localhost_is_loopback() {
        let ip = routable_source_ip("http://127.0.0.1:1");
        assert!(ip.is_some());
        assert!(ip.unwrap().is_loopback());
    }

    #[test]
    fn routable_ip_for_garbage_url_is_none() {
        assert!(routable_source_ip("not a url").is_none());
    }
}
