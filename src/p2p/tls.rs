//! TLS de `/tls/ws` a partir de los PEM del laboratorio (`TLS_CERT_PATH`,
//! `TLS_KEY_PATH`, `NODE_EXTRA_CA_CERTS`).
//!
//! - **Servidor:** certificado y llave del nodo, para que el navegador pueda
//!   conectarse por WSS.
//! - **Cliente:** cada certificado de confianza se agrega como raíz (sirve con
//!   la PKI de `galaxIA-E2E`) **y** se fija (pinning). El certificado actual del
//!   laboratorio es autofirmado con `CA:TRUE` y se usa como certificado de
//!   servidor: webpki lo rechaza (`CaUsedAsEndEntity`) aunque Node lo acepta.
//!   Un certificado fijado se acepta si es exactamente el mismo (DER), sin
//!   dejar de verificar la firma del handshake TLS. La identidad del par la
//!   garantiza además Noise (PeerId) por encima de TLS.

use std::path::Path;
use std::sync::Arc;

use libp2p::websocket::tls;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};

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
pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
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
    Ok(certs)
}

/// Llave privada (PKCS#8, PKCS#1 o SEC1) de un archivo PEM.
pub fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let pem = read(path)?;
    rustls_pemfile::private_key(&mut pem.as_slice())
        .map_err(|e| TlsError::Pem {
            path: path.display().to_string(),
            reason: e.to_string(),
        })?
        .ok_or_else(|| TlsError::Pem {
            path: path.display().to_string(),
            reason: "sin llave privada".into(),
        })
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Acepta los certificados fijados tal cual; el resto, validación webpki.
#[derive(Debug)]
struct PinnedOrWebPki {
    pinned: Vec<CertificateDer<'static>>,
    webpki: Arc<WebPkiServerVerifier>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedOrWebPki {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if self
            .pinned
            .iter()
            .any(|pinned| pinned.as_ref() == end_entity.as_ref())
        {
            return Ok(ServerCertVerified::assertion());
        }
        self.webpki
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Config TLS del transporte websocket: servidor (si hay certificado y llave)
/// y cliente que confía en (y fija) los certificados de `trust`.
pub fn websocket_config(
    cert_path: Option<&Path>,
    key_path: Option<&Path>,
    trust: &[&Path],
) -> Result<tls::Config, TlsError> {
    let provider = provider();

    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut pinned = Vec::new();
    for path in trust {
        for cert in load_certs(path)? {
            // Como raíz puede fallar (p. ej. un certificado de servidor); el
            // pinning cubre ese caso.
            let _ = roots.add(cert.clone());
            pinned.push(cert);
        }
    }
    let webpki = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|e| TlsError::Config(e.to_string()))?;
    let client = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::Config(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedOrWebPki {
            pinned,
            webpki,
            provider: provider.clone(),
        }))
        .with_no_client_auth();

    let server = match (cert_path, key_path) {
        (Some(cert_path), Some(key_path)) => Some(
            rustls::ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| TlsError::Config(e.to_string()))?
                .with_no_client_auth()
                .with_single_cert(load_certs(cert_path)?, load_key(key_path)?)
                .map_err(|e| TlsError::Config(e.to_string()))?,
        ),
        _ => None,
    };
    Ok(tls::Config::from_rustls(client, server))
}
