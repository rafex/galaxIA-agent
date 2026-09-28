//! Configuración por variables de entorno, con los mismos nombres que el
//! Navigator TS para que el contenedor sea un reemplazo directo.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use libp2p::Multiaddr;

use crate::ipfs::IpfsConfig;

/// Tope de adjunto del Navigator por defecto; configurable solo hacia abajo
/// del tope de protocolo (`MAX_ATTACHMENT_BYTES`, 32 MB).
pub const DEFAULT_ATTACHMENT_MAX_BYTES: usize = 20 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct AgentConfig {
    pub identity_path: PathBuf,
    pub listen: Vec<Multiaddr>,
    pub announce: Vec<Multiaddr>,
    pub bootstrap: Vec<Multiaddr>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    /// Certificados extra de confianza (`NODE_EXTRA_CA_CERTS` en el TS).
    pub extra_ca: Vec<PathBuf>,
    pub http_host: String,
    pub http_port: u16,
    /// Anunciarse como `navigator` (el Portal lo usará). Por defecto no, para
    /// poder correr en paralelo al Navigator TS sin quitarle el tráfico.
    pub advertise_as_navigator: bool,
    /// IPFS nativo (DEC-0095); `None` sin `IPFS_API_URL`.
    pub ipfs: Option<IpfsConfig>,
    pub attachment_max_bytes: usize,
    /// API de administración (solo loopback) y su token.
    pub admin_addr: SocketAddr,
    pub admin_token_path: PathBuf,
    pub commit: String,
    pub build_date: String,
}

#[derive(Debug, thiserror::Error)]
#[error("{name}: {reason}")]
pub struct ConfigError {
    name: &'static str,
    reason: String,
}

fn var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn multiaddrs(name: &'static str, default: &[&str]) -> Result<Vec<Multiaddr>, ConfigError> {
    let raw = var(name);
    let items: Vec<String> = match raw {
        Some(value) => value
            .split([',', '\n'])
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
        None => default.iter().map(|s| s.to_string()).collect(),
    };
    items
        .into_iter()
        .map(|item| {
            item.parse().map_err(|e| ConfigError {
                name,
                reason: format!("{item}: {e}"),
            })
        })
        .collect()
}

fn is_loopback_host(host: &str) -> bool {
    host == "localhost"
        || host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

impl AgentConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let tls_cert = var("TLS_CERT_PATH").map(PathBuf::from);
        let mut extra_ca: Vec<PathBuf> = var("NODE_EXTRA_CA_CERTS")
            .map(PathBuf::from)
            .into_iter()
            .collect();
        if let Some(cert) = &tls_cert {
            if !extra_ca.contains(cert) {
                extra_ca.push(cert.clone());
            }
        }
        let http_port = match var("PORT") {
            Some(port) => port.parse().map_err(|_| ConfigError {
                name: "PORT",
                reason: format!("{port} no es un puerto"),
            })?,
            None => 8090,
        };
        let identity_path = PathBuf::from(
            var("IDENTITY_KEY_PATH").unwrap_or_else(|| "./.fhs-identity-navigator.json".into()),
        );
        // El libro de pines y el token de admin viven junto a la identidad
        // (volumen `navigator-data`).
        let data_dir = identity_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let http_host = var("HOST").unwrap_or_else(|| "127.0.0.1".into());
        if !is_loopback_host(&http_host)
            && (var("TLS_CERT_PATH").is_none() || var("TLS_KEY_PATH").is_none())
        {
            return Err(ConfigError {
                name: "HOST",
                reason: format!(
                    "{http_host} no es loopback: la API exige TLS_CERT_PATH y TLS_KEY_PATH"
                ),
            });
        }
        let ipfs = match var("IPFS_API_URL") {
            None => None,
            Some(api_url) => {
                galaxia_fhs::ipfs::parse_api_url(&api_url).map_err(|e| ConfigError {
                    name: "IPFS_API_URL",
                    reason: e.to_string(),
                })?;
                let network = var("IPFS_NETWORK").unwrap_or_else(|| "public".into());
                if network != "public" && network != "private" {
                    return Err(ConfigError {
                        name: "IPFS_NETWORK",
                        reason: format!("{network}: debe ser public o private"),
                    });
                }
                Some(IpfsConfig {
                    api_url,
                    token_file: PathBuf::from(var("IPFS_API_TOKEN_FILE").ok_or(ConfigError {
                        name: "IPFS_API_TOKEN_FILE",
                        reason: "obligatoria con IPFS_API_URL".into(),
                    })?),
                    network,
                    ledger_path: data_dir.join("ipfs-pins.json"),
                })
            }
        };
        let protocol_max = galaxia_fhs::p2p::framing::MAX_ATTACHMENT_BYTES;
        let attachment_max_bytes = match var("ATTACHMENT_MAX_BYTES") {
            None => DEFAULT_ATTACHMENT_MAX_BYTES,
            Some(raw) => match raw.parse::<usize>() {
                Ok(n) if n > 0 && n <= protocol_max => n,
                _ => {
                    return Err(ConfigError {
                        name: "ATTACHMENT_MAX_BYTES",
                        reason: format!("{raw}: entre 1 y {protocol_max} (tope de protocolo)"),
                    })
                }
            },
        };
        let admin_addr: SocketAddr = var("ADMIN_ADDR")
            .unwrap_or_else(|| "127.0.0.1:8099".into())
            .parse()
            .map_err(|e| ConfigError {
                name: "ADMIN_ADDR",
                reason: format!("{e}"),
            })?;
        if !admin_addr.ip().is_loopback() {
            return Err(ConfigError {
                name: "ADMIN_ADDR",
                reason: format!("{admin_addr}: la API de administración solo escucha en loopback"),
            });
        }
        Ok(Self {
            admin_token_path: data_dir.join("admin.token"),
            identity_path,
            ipfs,
            attachment_max_bytes,
            admin_addr,
            listen: multiaddrs("FHS_LISTEN_ADDRS", &["/ip4/0.0.0.0/tcp/4010/tls/ws"])?,
            announce: multiaddrs("FHS_ANNOUNCE_ADDRS", &[])?,
            bootstrap: multiaddrs("FHS_BOOTSTRAP_ADDRS", &[])?,
            tls_key: var("TLS_KEY_PATH").map(PathBuf::from),
            tls_cert,
            extra_ca,
            http_host,
            http_port,
            advertise_as_navigator: matches!(
                var("FHS_ADVERTISE_AS_NAVIGATOR").as_deref(),
                Some("1" | "true" | "yes")
            ),
            commit: var("COMMIT_HASH")
                .unwrap_or_else(|| option_env!("GALAXIA_COMMIT").unwrap_or("dev").into()),
            build_date: var("BUILD_DATE").unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_hosts() {
        for host in ["127.0.0.1", "::1", "[::1]", "localhost", "127.0.0.2"] {
            assert!(is_loopback_host(host), "{host}");
        }
        for host in ["0.0.0.0", "192.168.1.139", "::", "example.com"] {
            assert!(!is_loopback_host(host), "{host}");
        }
    }
}
