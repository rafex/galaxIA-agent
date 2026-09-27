//! TLS de `/tls/ws` a partir de los PEM del laboratorio (`TLS_CERT_PATH`,
//! `TLS_KEY_PATH`). El mismo certificado sirve para escuchar (el navegador
//! se conecta con WSS) y se agrega como raíz de confianza para marcar a Atlas
//! y a los providers, que usan el mismo certificado autofirmado
//! (equivalente a `NODE_EXTRA_CA_CERTS` en el TS).

use std::path::Path;

use libp2p::websocket::tls;

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("no se pudo leer {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("{path}: {reason}")]
    Pem { path: String, reason: String },
    #[error("configuración TLS inválida: {0}")]
    Config(String),
}

fn read(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::Io {
        path: path.display().to_string(),
        source,
    })
}

/// Certificados DER de un archivo PEM.
pub fn load_certs(path: &Path) -> Result<Vec<Vec<u8>>, TlsError> {
    let pem = read(path)?;
    let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut pem.as_slice()).collect();
    let certs = certs.map_err(|e| TlsError::Pem {
        path: path.display().to_string(),
        reason: e.to_string(),
    })?;
    if certs.is_empty() {
        return Err(TlsError::Pem {
            path: path.display().to_string(),
            reason: "sin certificados".into(),
        });
    }
    Ok(certs.into_iter().map(|c| c.to_vec()).collect())
}

/// Llave privada DER (PKCS#8, PKCS#1 o SEC1) de un archivo PEM.
pub fn load_key(path: &Path) -> Result<Vec<u8>, TlsError> {
    let pem = read(path)?;
    let key = rustls_pemfile::private_key(&mut pem.as_slice())
        .map_err(|e| TlsError::Pem {
            path: path.display().to_string(),
            reason: e.to_string(),
        })?
        .ok_or_else(|| TlsError::Pem {
            path: path.display().to_string(),
            reason: "sin llave privada".into(),
        })?;
    Ok(key.secret_der().to_vec())
}

/// Config TLS del transporte websocket: servidor (si hay llave) y confianza
/// en los certificados dados, además de las raíces públicas.
pub fn websocket_config(
    cert_path: Option<&Path>,
    key_path: Option<&Path>,
    trust: &[&Path],
) -> Result<tls::Config, TlsError> {
    let mut builder = tls::Config::builder();
    if let (Some(cert_path), Some(key_path)) = (cert_path, key_path) {
        let certs = load_certs(cert_path)?
            .into_iter()
            .map(tls::Certificate::new);
        let key = tls::PrivateKey::new(load_key(key_path)?);
        builder
            .server(key, certs)
            .map_err(|e| TlsError::Config(e.to_string()))?;
    }
    for path in trust {
        for der in load_certs(path)? {
            builder
                .add_trust(&tls::Certificate::new(der))
                .map_err(|e| TlsError::Config(e.to_string()))?;
        }
    }
    Ok(builder.finish())
}
