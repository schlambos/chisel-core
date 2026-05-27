//! LAN reachability for the local fs MCP server.
//!
//! OpenCode runs on the same network as AionUi (e.g. 192.168.0.5:4096),
//! so no public tunnel is needed — OpenCode can dial straight back to
//! the operator's machine on its LAN IP. This module figures out which
//! local IP to advertise.
//!
//! Resolution order, per session:
//! 1. `AIONUI_LOCAL_FS_MCP_PUBLIC_URL` env var — explicit override for
//!    weird setups (containers, multi-homed hosts, NAT exceptions).
//! 2. The IP the OS would use to reach OpenCode's host. Discovered with
//!    a UDP socket `connect` (no packets sent) — gives the source IP of
//!    the route that would carry real traffic. Robust on multi-homed
//!    machines, VPNs, Tailscale, etc.
//! 3. Loopback fallback. Logs a warning — only works if OpenCode is on
//!    the same host (rare in the LAN deployment).

use std::net::{IpAddr, SocketAddr, ToSocketAddrs, UdpSocket};

use reqwest::Url;
use tracing::{info, warn};

use crate::manager::remote::opencode_mcp::PUBLIC_URL_ENV;

/// Resolved reachable endpoint for the client MCP server.
#[derive(Debug, Clone)]
pub struct Reachable {
    /// URL the remote OpenCode should dial.
    pub public_url: String,
    /// How the URL was selected (for logging/UI).
    pub provider: &'static str,
}

/// Decide where the MCP server should bind and which URL OpenCode
/// should be told to dial. `opencode_base_url` is the OpenCode HTTP base
/// (e.g. `http://192.168.0.5:4096`).
///
/// On success, returns:
/// - `bind`: the `SocketAddr` to pass to `LocalFsMcpServer::start`.
///   Always uses port 0 (OS-assigned); the host is the routable IP.
/// - `advertised_ip`: the IP that will appear in the registered URL,
///   so callers can build the full URL once the OS-assigned port is
///   known.
pub fn resolve(opencode_base_url: &str) -> Resolution {
    if let Ok(public) = std::env::var(PUBLIC_URL_ENV) {
        info!(public_url = %public, "using user-supplied URL from {PUBLIC_URL_ENV}");
        return Resolution::Override { public_url: public };
    }

    match routable_source_ip(opencode_base_url) {
        Some(ip) => {
            info!(advertised_ip = %ip, "resolved LAN-routable source IP for client MCP");
            Resolution::Lan { advertised_ip: ip }
        }
        None => {
            warn!(
                opencode = %opencode_base_url,
                "could not resolve LAN-routable IP — falling back to loopback. \
            The remote OpenCode will only be able to reach the MCP if it's on the same host. \
            Set {PUBLIC_URL_ENV} to override."
            );
            Resolution::Loopback
        }
    }
}

#[derive(Debug, Clone)]
pub enum Resolution {
    /// User-supplied URL; bind to loopback (only the URL ever leaves).
    Override { public_url: String },
    /// LAN deployment; bind to the routable interface so OpenCode can dial.
    Lan { advertised_ip: IpAddr },
    /// Last resort; only works if OpenCode is local to this host.
    Loopback,
}

impl Resolution {
    /// `SocketAddr` to pass to the MCP server bind.
    pub fn bind_addr(&self) -> SocketAddr {
        match self {
            // Bind to all interfaces so the LAN can reach it; the IP in
            // the registered URL is what locks down which interface
            // OpenCode actually uses.
            Self::Lan { .. } => SocketAddr::from(([0, 0, 0, 0], 0)),
            // Override case: still bind loopback — the public URL is
            // someone else's tunnel that proxies in.
            Self::Override { .. } | Self::Loopback => SocketAddr::from(([127, 0, 0, 1], 0)),
        }
    }

    /// Build the final URL to register with OpenCode, given the
    /// OS-assigned port from the started MCP server.
    pub fn into_reachable(self, bound_port: u16) -> Reachable {
        match self {
            Self::Override { public_url } => Reachable {
                public_url,
                provider: "env-override",
            },
            Self::Lan { advertised_ip } => Reachable {
                public_url: format!("http://{}/", SocketAddr::new(advertised_ip, bound_port)),
                provider: "lan",
            },
            Self::Loopback => Reachable {
                public_url: format!("http://127.0.0.1:{bound_port}/"),
                provider: "loopback-fallback",
            },
        }
    }
}

/// Open a UDP socket and `connect` it (no packets sent) to OpenCode's
/// host. The OS picks the source IP it would use to route there; we
/// read it off `local_addr`. This is the standard trick for "what's my
/// IP that can reach X" without external lookups.
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
    fn resolution_lan_binds_to_all_interfaces() {
        let r = Resolution::Lan {
            advertised_ip: IpAddr::from([192, 168, 0, 42]),
        };
        assert_eq!(r.bind_addr().ip(), IpAddr::from([0, 0, 0, 0]));
        let reach = r.into_reachable(12345);
        assert_eq!(reach.public_url, "http://192.168.0.42:12345/");
        assert_eq!(reach.provider, "lan");
    }

    #[test]
    fn resolution_loopback_binds_loopback() {
        let r = Resolution::Loopback;
        assert_eq!(r.bind_addr().ip(), IpAddr::from([127, 0, 0, 1]));
        let reach = r.into_reachable(99);
        assert_eq!(reach.public_url, "http://127.0.0.1:99/");
    }

    #[test]
    fn resolution_override_passes_url_through() {
        let r = Resolution::Override {
            public_url: "https://my.tunnel/foo".into(),
        };
        let reach = r.into_reachable(0);
        assert_eq!(reach.public_url, "https://my.tunnel/foo");
        assert_eq!(reach.provider, "env-override");
    }

    #[test]
    fn routable_ip_for_localhost_is_loopback() {
        // Connecting to localhost yields a loopback source IP. That's
        // technically "unspecified" intent but the IP is concrete; this
        // test just exercises the resolver doesn't panic.
        let ip = routable_source_ip("http://127.0.0.1:1");
        assert!(ip.is_some());
        assert!(ip.unwrap().is_loopback());
    }

    #[test]
    fn routable_ip_for_garbage_url_is_none() {
        assert!(routable_source_ip("not a url").is_none());
    }
}
