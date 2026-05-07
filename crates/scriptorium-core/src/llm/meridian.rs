//! Local OpenAI-compatible Anthropic proxy ("meridian") integration.
//!
//! Meridian is a local proxy at a user-configured URL that handles Anthropic
//! upstream auth and exposes Claude models via the OpenAI-compatible chat
//! endpoint. When `[meridian].enabled = true` in vault config and
//! `[llm].provider = "claude"`, scriptorium probes the configured URL once
//! at startup and routes Claude requests through it on success — without
//! requiring an `ANTHROPIC_API_KEY`.

use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::config::MeridianConfig;

const PROBE_TIMEOUT: Duration = Duration::from_millis(1500);

/// Probe the meridian endpoint with a short connect-timeout. Blocking —
/// safe to call from the synchronous startup path. Returns `true` only when
/// a TCP connection to the configured `url`'s host:port succeeds inside the
/// timeout. Any failure (DNS, refused, timeout) returns `false` so the
/// caller can transparently fall back to direct Anthropic.
pub fn probe(cfg: &MeridianConfig) -> bool {
    if !cfg.enabled {
        return false;
    }
    let Some(addr) = resolve_host_port(&cfg.url) else {
        return false;
    };
    TcpStream::connect_timeout(&addr, PROBE_TIMEOUT).is_ok()
}

fn resolve_host_port(url: &str) -> Option<SocketAddr> {
    let stripped = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    let host_port = stripped.split('/').next()?;
    let (host, port_str) = match host_port.rsplit_once(':') {
        Some((h, p)) => (h, p),
        None => (
            host_port,
            if url.starts_with("https://") {
                "443"
            } else {
                "80"
            },
        ),
    };
    let port: u16 = port_str.parse().ok()?;
    (host, port).to_socket_addrs().ok()?.next()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_short_circuits() {
        let cfg = MeridianConfig {
            enabled: false,
            url: "http://127.0.0.1:1".into(),
        };
        assert!(!probe(&cfg));
    }

    #[test]
    fn unreachable_url_returns_false() {
        let cfg = MeridianConfig {
            enabled: true,
            url: "http://127.0.0.1:1".into(),
        };
        assert!(!probe(&cfg));
    }
}
