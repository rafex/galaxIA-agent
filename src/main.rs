use axum::{
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use galaxia_agent::{
    agent::SovereignAgent, atlas::AtlasClient, events::EventBus, fhs::UnconfiguredFhsTransport,
    policy::AgentRequest,
};
use std::{net::SocketAddr, sync::Arc};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    agent: SovereignAgent<UnconfiguredFhsTransport>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let events = EventBus::new(256);
    let agent = SovereignAgent::new(
        AtlasClient::default(),
        Arc::new(UnconfiguredFhsTransport),
        events.clone(),
    );
    let state = AppState { agent };
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/chat", post(chat))
        .route("/v1/events", get(events_endpoint))
        .route("/ws", get(ws))
        .with_state(state);
    let addr: SocketAddr = std::env::var("GALAXIA_AGENT_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8090".into())
        .parse()
        .expect("GALAXIA_AGENT_BIND");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind agent API");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("serve agent API");
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

async fn chat(
    State(state): State<AppState>,
    Json(request): Json<AgentRequest>,
) -> impl IntoResponse {
    match state.agent.run(request).await {
        Ok(content) => Json(serde_json::json!({"content": content})),
        Err(error) => Json(serde_json::json!({"error": error})),
    }
}
async fn events_endpoint(State(_state): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({"events":"websocket /ws"}))
}
async fn ws(State(_state): State<AppState>, upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade.on_upgrade(|_socket| async {})
}
