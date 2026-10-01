use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use galaxia_agent::{
    config::AgentConfig,
    ipfs::IpfsService,
    p2p::{self, identity::NodeIdentity, node::NodeHandle},
};
use serde_json::{json, Value};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    node: NodeHandle,
    config: Arc<AgentConfig>,
    ipfs: Option<IpfsService>,
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
        role: p2p::node::Role::Navigator,
        agent_version: format!("galaxia-agent/{}", env!("CARGO_PKG_VERSION")),
        identity: identity.clone(),
        listen: config.listen.clone(),
        announce: config.announce.clone(),
        bootstrap: config.bootstrap.clone(),
        tls,
        advertise: config
            .advertise_as_navigator
            .then(|| p2p::wire::navigator_beacon("Navigator FHS")),
        dht_beacon: Some(p2p::wire::navigator_beacon("Navigator FHS")),
    })?;
    if !config.advertise_as_navigator {
        tracing::info!(
            "sin anunciarse como navigator (FHS_ADVERTISE_AS_NAVIGATOR=true para el cambio)"
        );
    }

    let vetoed: std::collections::HashSet<String> = std::env::var("FHS_VETOED_PROVIDERS")
        .unwrap_or_default()
        .split(',')
        .map(|d| d.trim().split('#').next().unwrap_or_default().to_string())
        .filter(|d| !d.is_empty())
        .collect();
    // DIDs de los nodos de cálculo permitidos: un DID inválido impide arrancar.
    let calc_nodes: Vec<String> = std::env::var("FHS_CALC_NODES")
        .unwrap_or_default()
        .split(',')
        .map(|d| d.trim().split('#').next().unwrap_or_default().to_string())
        .filter(|d| !d.is_empty())
        .collect();
    for did in calc_nodes.iter().filter(|d| d.as_str() != "*") {
        p2p::identity::peer_id_of_did(did).map_err(|e| format!("FHS_CALC_NODES: {did}: {e}"))?;
    }
    if !calc_nodes.is_empty() {
        tracing::info!("nodos de cálculo permitidos: {}", calc_nodes.len());
    }
    let ipfs = match &config.ipfs {
        Some(ipfs_config) => {
            let service = IpfsService::start(ipfs_config)?;
            tracing::info!(
                "IPFS nativo en red {} vía Kubo local (libro {})",
                ipfs_config.network,
                ipfs_config.ledger_path.display()
            );
            Some(service)
        }
        None => {
            tracing::info!("sin IPFS_API_URL: los adjuntos viajan inline");
            None
        }
    };
    let admin_token = galaxia_agent::admin::load_or_create_token(&config.admin_token_path)?;
    tokio::spawn(galaxia_agent::admin::serve(
        config.admin_addr,
        admin_token,
        ipfs.clone(),
    ));
    tokio::spawn(galaxia_agent::session::serve(
        node.clone(),
        galaxia_agent::session::SessionDefaults {
            preferences: galaxia_agent::runtime::agent::Preferences {
                vetoed: Arc::new(vetoed),
                calc_nodes: Arc::new(calc_nodes),
                ..Default::default()
            },
            ipfs: ipfs.clone(),
            attachment_max_bytes: config.attachment_max_bytes,
        },
    ));

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("probe") {
        return probe(&node, &args[1..]).await;
    }

    let state = AppState {
        node,
        config: config.clone(),
        ipfs,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
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
        map.insert(
            "ipfs".into(),
            state.ipfs.as_ref().map_or(Value::Null, IpfsService::status),
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

/// `galaxia-agent probe chat "<texto>"` · `probe tool <capability> <tool> '<json>'`:
/// ejecuta una misión real contra la red y termina (diagnóstico).
async fn probe(node: &NodeHandle, args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use galaxia_agent::p2p::{client, dynamic};
    use galaxia_agent::protocol::fhs::Message;
    use std::io::Write;

    // Un ciclo completo de anuncios (cada provider se anuncia cada 30 s).
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(35)
        && !(!node.peers.stars().is_empty() && node.peers.satellites().len() >= 3)
    {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!("providers conocidos: {}", node.peers.all().len());
    match args.first().map(String::as_str) {
        Some("chat") => {
            let text = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "Hola, ¿quién eres?".into());
            let t0 = std::time::Instant::now();
            let mut first: Option<Duration> = None;
            let outcome = client::chat(
                node,
                client::ChatRequest {
                    messages: vec![
                        Message {
                            role: "system".into(),
                            content: "Responde en español, breve.".into(),
                            ..Default::default()
                        },
                        Message {
                            role: "user".into(),
                            content: text,
                            ..Default::default()
                        },
                    ],
                    tools: vec![],
                    model: String::new(),
                    preferred_provider: None,
                    timeout: Duration::from_secs(300),
                },
                |delta| {
                    first.get_or_insert(t0.elapsed());
                    print!("{delta}");
                    let _ = std::io::stdout().flush();
                },
            )
            .await?;
            println!(
                "\n— Star {} · primer delta {:?} · total {:?}",
                outcome.provider,
                first,
                t0.elapsed()
            );
        }
        Some("portal") => {
            use galaxia_agent::p2p::{framing, wire};
            use galaxia_agent::protocol::fhs::{
                envelope::Payload, AgentStartMessage, ChatRequestMessage, KbDecisionMessage,
            };
            let addr: libp2p::Multiaddr = args
                .get(1)
                .ok_or("falta la multiaddr del agente")?
                .parse()?;
            let question = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| "¿Qué dice el artículo 3 sobre la educación?".into());
            let peer = node.dial(addr).await?;
            let mut stream = node
                .stream_control()
                .open_stream(peer, NodeHandle::fhs_protocol())
                .await?;
            let send = |payload| wire::sealed_envelope(&node.identity, "", payload);
            framing::write_envelope(&mut stream, &send(Payload::Handshake(Default::default())))
                .await?;
            let ack = framing::read_verified(&mut stream).await?;
            println!(
                "handshake: {}",
                matches!(ack.and_then(|e| e.payload), Some(Payload::HandshakeAck(_)))
            );
            framing::write_envelope(
                &mut stream,
                &send(Payload::AgentStart(AgentStartMessage {
                    session_id: "probe-portal".into(),
                    scope: "community".into(),
                    ..Default::default()
                })),
            )
            .await?;
            framing::write_envelope(
                &mut stream,
                &send(Payload::ChatRequest(ChatRequestMessage {
                    mission_id: "probe-portal".into(),
                    messages: vec![Message {
                        role: "user".into(),
                        content: question,
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
            )
            .await?;
            let t0 = std::time::Instant::now();
            while let Some(envelope) = framing::read_verified(&mut stream).await? {
                match envelope.payload {
                    Some(Payload::AssistantDelta(d)) => {
                        print!("{}", d.delta);
                        let _ = std::io::stdout().flush();
                    }
                    Some(Payload::KbRecommended(kb)) => {
                        println!(
                            "  · kbRecommended {:?} → acepto",
                            kb.candidates
                                .iter()
                                .map(|c| &c.provider_name)
                                .collect::<Vec<_>>()
                        );
                        framing::write_envelope(
                            &mut stream,
                            &send(Payload::KbDecision(KbDecisionMessage {
                                mission_id: kb.mission_id,
                                r#use: true,
                            })),
                        )
                        .await?;
                    }
                    Some(Payload::AssistantCompleted(done)) => {
                        println!(
                            "\n  · assistantCompleted {:?} · {:?}",
                            done.provenance,
                            t0.elapsed()
                        );
                        break;
                    }
                    Some(Payload::Error(e)) => {
                        println!("  · error {}: {}", e.code, e.message);
                        break;
                    }
                    other => println!(
                        "  · {:?}",
                        other.map(|p| format!("{p:?}").chars().take(120).collect::<String>())
                    ),
                }
            }
        }
        Some("turn") => {
            use galaxia_agent::runtime::{
                agent::{AgentRuntime, Preferences, Turn},
                events::{AgentEvent, EventSink},
            };
            struct Printer;
            impl EventSink for Printer {
                fn emit(&self, event: AgentEvent) {
                    match event {
                        AgentEvent::AssistantDelta { text } => {
                            print!("{text}");
                            let _ = std::io::stdout().flush();
                        }
                        other => println!("  · {other:?}"),
                    }
                }
            }
            let question = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "¿Qué dice el artículo 3 sobre la educación?".into());
            let printer = Printer;
            let preferences = Preferences::default();
            let mut runtime = AgentRuntime::new(node.clone(), &printer, "probe-conv");
            let (candidates, by_llm) = runtime.resolve_kb_candidates(&question, &preferences).await;
            println!(
                "KB recomendadas (por LLM: {by_llm}): {:?}",
                candidates
                    .iter()
                    .map(|c| &c.provider_name)
                    .collect::<Vec<_>>()
            );
            let t0 = std::time::Instant::now();
            let answer = runtime
                .run(
                    Turn {
                        message: question,
                        kb_provider_ids: candidates.iter().map(|c| c.provider_id.clone()).collect(),
                        ..Default::default()
                    },
                    &preferences,
                )
                .await?;
            println!("\n— {} caracteres · {:?}", answer.len(), t0.elapsed());
        }
        Some("rig") => {
            use galaxia_agent::llm;
            let text = args.get(1).cloned().unwrap_or_else(|| "Hola".into());
            let model = llm::StarModel::new(node.clone(), "", None, llm::DEFAULT_LLM_TIMEOUT);
            let request = llm::request(
                &[
                    Message {
                        role: "system".into(),
                        content: "Responde en español, breve.".into(),
                        ..Default::default()
                    },
                    Message {
                        role: "user".into(),
                        content: text,
                        ..Default::default()
                    },
                ],
                &[],
                0.7,
            );
            let t0 = std::time::Instant::now();
            let response = model
                .complete_streaming(request, |delta| {
                    print!("{delta}");
                    let _ = std::io::stdout().flush();
                })
                .await?;
            println!(
                "\n— vía Rig · Star {:?} · {} caracteres · {:?}",
                model.executed_by(),
                llm::text_of(&response).len(),
                t0.elapsed()
            );
        }
        Some("tool") => {
            let capability = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "knowledge.query".into());
            let tool = args.get(2).cloned().unwrap_or_else(|| "kb_query".into());
            let json: serde_json::Value =
                serde_json::from_str(args.get(3).map(String::as_str).unwrap_or("{}"))?;
            let outcome = client::call_tool(
                node,
                client::ToolRequest {
                    capability,
                    extra_capabilities: vec![],
                    tool_name: tool,
                    arguments: dynamic::from_json(&json)?,
                    preferred_provider: None,
                    timeout: Duration::from_secs(120),
                    mission_id: None,
                    allowed_provider_dids: None,
                },
            )
            .await?;
            let result = outcome
                .result
                .as_ref()
                .map(dynamic::to_json)
                .unwrap_or_default();
            println!(
                "— Satellite {}\n{}",
                outcome.provider,
                serde_json::to_string_pretty(&result)?
            );
        }
        _ => println!(
            "uso: galaxia-agent probe chat \"texto\" | probe tool <capability> <tool> '<json>'"
        ),
    }
    Ok(())
}
