use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    extract::State,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use galaxia_agent::{
    agent::SovereignAgent,
    atlas::AtlasClient,
    config::AgentConfig,
    events::EventBus,
    fhs::UnconfiguredFhsTransport,
    p2p::{self, identity::NodeIdentity, node::NodeHandle},
    policy::AgentRequest,
};
use serde_json::{json, Value};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    agent: SovereignAgent<UnconfiguredFhsTransport>,
    node: NodeHandle,
    config: Arc<AgentConfig>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    // rustls exige elegir proveedor criptográfico explícitamente.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let config = Arc::new(AgentConfig::from_env()?);
    let identity = NodeIdentity::load_or_create(&config.identity_path)?;
    tracing::info!("DID: {} · PeerId: {}", identity.did, identity.peer_id);

    let trust: Vec<&std::path::Path> = config.extra_ca.iter().map(|p| p.as_path()).collect();
    let tls = p2p::tls::websocket_config(
        config.tls_cert.as_deref(),
        config.tls_key.as_deref(),
        &trust,
    )?;
    if config.bootstrap.is_empty() {
        tracing::warn!("FHS_BOOTSTRAP_ADDRS vacío: el nodo queda aislado");
    }
    let node = p2p::node::start(p2p::node::NodeConfig {
        identity: identity.clone(),
        listen: config.listen.clone(),
        announce: config.announce.clone(),
        bootstrap: config.bootstrap.clone(),
        tls,
        advertise: config
            .advertise_as_navigator
            .then(|| p2p::wire::navigator_beacon("Navigator FHS")),
    })?;
    if !config.advertise_as_navigator {
        tracing::info!(
            "sin anunciarse como navigator (FHS_ADVERTISE_AS_NAVIGATOR=true para el cambio)"
        );
    }

    let agent = SovereignAgent::new(
        AtlasClient::default(),
        Arc::new(UnconfiguredFhsTransport),
        EventBus::new(256),
    );
    let state = AppState {
        agent,
        node,
        config: config.clone(),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/v1/chat", post(chat))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", config.http_host, config.http_port).parse()?;
    let handle = axum_server::Handle::new();
    tokio::spawn({
        let handle = handle.clone();
        async move {
            shutdown_signal().await;
            handle.graceful_shutdown(Some(Duration::from_secs(5)));
        }
    });
    match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => {
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
            tracing::info!("API de observabilidad en https://{addr}");
            axum_server::bind_rustls(addr, tls)
                .handle(handle)
                .serve(app.into_make_service())
                .await?;
        }
        _ => {
            tracing::info!("API de observabilidad en http://{addr} (sin TLS_KEY_PATH)");
            axum_server::bind(addr)
                .handle(handle)
                .serve(app.into_make_service())
                .await?;
        }
    }
    Ok(())
}

/// Mismo formato que el `/health` del Navigator TS (doctor.sh lee `multiaddrs`).
async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let multiaddrs = state
        .node
        .status()
        .await
        .map(|s| s.multiaddrs)
        .unwrap_or_default();
    Json(json!({
        "ok": true,
        "fhsVersion": "0.1",
        "version": state.config.commit,
        "buildDate": state.config.build_date,
        "did": state.node.identity.did,
        "multiaddrs": multiaddrs,
        "runtime": "galaxia-agent",
    }))
}

/// Mismo formato que el `/status` del Navigator TS: `nodeStatus` + `knownPeers`.
async fn status(State(state): State<AppState>) -> impl IntoResponse {
    let mut body = json!({ "did": state.node.identity.did });
    if let (Some(status), Value::Object(map)) = (state.node.status().await, &mut body) {
        if let Ok(Value::Object(fields)) = serde_json::to_value(status) {
            map.extend(fields);
        }
        map.insert(
            "knownPeers".into(),
            serde_json::to_value(state.node.peers.known_peers()).unwrap_or_default(),
        );
    }
    Json(body)
}

/// SIGTERM (podman stop) o Ctrl+C. Sin esto el contenedor no se detenía y
/// podman lo mataba a los 10 s (mismo problema que el Navigator TS).
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("apagando galaxia-agent");
}

/// Transición: se reemplaza por la sesión del Portal por libp2p.
async fn chat(
    State(state): State<AppState>,
    Json(request): Json<AgentRequest>,
) -> impl IntoResponse {
    match state.agent.run(request).await {
        Ok(content) => Json(json!({"content": content})),
        Err(error) => Json(json!({"error": error})),
    }
}
