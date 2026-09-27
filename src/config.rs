//! Configuración por variables de entorno, con los mismos nombres que el
//! Navigator TS para que el contenedor sea un reemplazo directo.

use std::path::PathBuf;

use libp2p::Multiaddr;

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
        Ok(Self {
            identity_path: PathBuf::from(
                var("IDENTITY_KEY_PATH").unwrap_or_else(|| "./.fhs-identity-navigator.json".into()),
            ),
            listen: multiaddrs("FHS_LISTEN_ADDRS", &["/ip4/0.0.0.0/tcp/4010/tls/ws"])?,
            announce: multiaddrs("FHS_ANNOUNCE_ADDRS", &[])?,
            bootstrap: multiaddrs("FHS_BOOTSTRAP_ADDRS", &[])?,
            tls_key: var("TLS_KEY_PATH").map(PathBuf::from),
            tls_cert,
            extra_ca,
            http_host: var("HOST").unwrap_or_else(|| "127.0.0.1".into()),
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
