//! API de administración del Navigator (DEC-0095): solo en loopback
//! (`ADMIN_ADDR`, por defecto `127.0.0.1:8099`) y con token *bearer* en
//! `/data/admin.token` (0600, se genera al primer arranque).
//!
//! - `GET  /admin/ipfs/pins`: el libro de pines.
//! - `POST /admin/ipfs/release?cid=<cid>`: quita `reuse`. Nunca hace unpin:
//!   lo hace el siguiente barrido. `200 {cleanup_scheduled, release_after}`,
//!   `404` CID desconocido, `409` CID sin `reuse`.

use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::ipfs::{ledger::ReleaseError, IpfsService};

#[derive(Clone)]
struct AdminState {
    token: Arc<String>,
    ipfs: Option<IpfsService>,
}

/// Lee el token o lo crea (0600) si no existe.
pub fn load_or_create_token(path: &Path) -> std::io::Result<String> {
    if let Ok(token) = std::fs::read_to_string(path) {
        let token = token.trim().to_string();
        if !token.is_empty() {
            return Ok(token);
        }
    }
    if let Some(dir) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)?.write_all(token.as_bytes())?;
    tracing::info!("[admin] token nuevo en {}", path.display());
    Ok(token)
}

/// Comparación en tiempo constante.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn authorized(headers: &HeaderMap, token: &str) -> bool {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|given| same(given.as_bytes(), token.as_bytes()))
}

pub fn router(token: String, ipfs: Option<IpfsService>) -> Router {
    Router::new()
        .route("/admin/ipfs/pins", get(pins))
        .route("/admin/ipfs/release", post(release))
        .with_state(AdminState {
            token: Arc::new(token),
            ipfs,
        })
}

/// Escucha solo en loopback; cualquier otra dirección es error.
pub async fn serve(addr: SocketAddr, token: String, ipfs: Option<IpfsService>) {
    if !addr.ip().is_loopback() {
        tracing::error!("[admin] {addr} no es loopback: API de administración deshabilitada");
        return;
    }
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!("[admin] no se pudo escuchar en {addr}: {error}");
            return;
        }
    };
    tracing::info!("[admin] API de administración en http://{addr}");
    if let Err(error) = axum::serve(listener, router(token, ipfs)).await {
        tracing::error!("[admin] {error}");
    }
}

fn deny(what: &str) -> Response {
    tracing::warn!("[admin] {what}: sin token válido");
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "token inválido"})),
    )
        .into_response()
}

fn no_ipfs() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": "IPFS no configurado en este Navigator"})),
    )
        .into_response()
}

async fn pins(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &state.token) {
        return deny("GET /admin/ipfs/pins");
    }
    tracing::info!("[admin] GET /admin/ipfs/pins");
    match &state.ipfs {
        Some(ipfs) => Json(ipfs.ledger()).into_response(),
        None => no_ipfs(),
    }
}

#[derive(Deserialize)]
struct ReleaseQuery {
    cid: String,
}

async fn release(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Query(query): Query<ReleaseQuery>,
) -> Response {
    if !authorized(&headers, &state.token) {
        return deny("POST /admin/ipfs/release");
    }
    let Some(ipfs) = &state.ipfs else {
        return no_ipfs();
    };
    let outcome = ipfs.release_reuse(&query.cid);
    tracing::info!(
        "[admin] POST /admin/ipfs/release cid={} → {:?}",
        query.cid,
        outcome
    );
    match outcome {
        Ok(outcome) => Json(outcome).into_response(),
        Err(ReleaseError::Unknown) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "CID desconocido"})),
        )
            .into_response(),
        Err(ReleaseError::NotReuse) => (
            StatusCode::CONFLICT,
            Json(json!({"error": "el CID no está marcado como reuse"})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_token_must_match_exactly() {
        let mut headers = HeaderMap::new();
        assert!(!authorized(&headers, "abc"));
        headers.insert("authorization", "Bearer abc".parse().unwrap());
        assert!(authorized(&headers, "abc"));
        headers.insert("authorization", "Bearer abcd".parse().unwrap());
        assert!(!authorized(&headers, "abc"));
        headers.insert("authorization", "abc".parse().unwrap());
        assert!(!authorized(&headers, "abc"));
    }

    #[test]
    fn token_is_created_once_with_private_permissions() {
        let path = std::env::temp_dir()
            .join(format!("admin-{}", uuid::Uuid::new_v4()))
            .join("admin.token");
        let first = load_or_create_token(&path).unwrap();
        assert_eq!(first.len(), 64);
        assert_eq!(load_or_create_token(&path).unwrap(), first);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }
}
