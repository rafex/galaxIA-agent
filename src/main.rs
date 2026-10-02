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
    // Autorización por uso (SPEC-AUTH-0001): lista de nodos verificados por el
    // operador y bitácora de auditoría (sin contenido).
    let trusted_nodes: std::collections::HashSet<String> = std::env::var("FHS_TRUSTED_NODES")
        .unwrap_or_default()
        .split(',')
        .map(|d| d.trim().split('#').next().unwrap_or_default().to_string())
        .filter(|d| !d.is_empty())
        .collect();
    // Comandos autodescubiertos (SPEC-CMD-0001): el registro cerrado y la
    // política de admisión; un registro o un DID inválidos impiden arrancar.
    let commands = Arc::new(galaxia_agent::runtime::commands::CommandEngine::from_env(
        trusted_nodes.clone(),
    )?);
    tracing::info!(
        "comandos: registro v{} ({}), admisión abierta: {}",
        commands.registry.version,
        &commands.registry.digest[..12],
        match &commands.policy.open {
            galaxia_agent::commands::OpenNodes::None => "ninguna".to_string(),
            galaxia_agent::commands::OpenNodes::All => "cualquier nodo".to_string(),
            galaxia_agent::commands::OpenNodes::List(list) => format!("{} nodos", list.len()),
        }
    );
    let audit_path = std::env::var("AUTH_AUDIT_PATH")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| {
            config
                .admin_token_path
                .parent()
                .map(|dir| dir.join("authorization-audit.log"))
        });
    let authorizer = galaxia_agent::authorization::Authorizer::new(
        Arc::new(trusted_nodes),
        audit_path.as_deref(),
        None,
    );
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
                ..Default::default()
            },
            ipfs: ipfs.clone(),
            attachment_max_bytes: config.attachment_max_bytes,
            authorizer,
            commands,
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

/// `galaxia-agent probe chat "<texto>"` · `probe tool <capability> <tool> '<json>'` ·
/// `probe portal <multiaddr> "<pregunta>"`: ejecuta una misión real contra la
/// red y termina (diagnóstico). Todo sale por el `Dispatcher`, con la misma
/// puerta de autorización: sin interfaz, la política es `FHS_AUTH_POLICY`
/// (por defecto **deniega**; `allow-synthetic` solo con datos sintéticos).
async fn probe(node: &NodeHandle, args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use galaxia_agent::authorization::dispatcher::{
        Dispatcher, LlmRequest, Outbound, ToolCallSpec,
    };
    use galaxia_agent::authorization::{Authorizer, Ctx, HeadlessPolicy, ItemSpec, DEFAULT_TTL};
    use galaxia_agent::p2p::dynamic;
    use galaxia_agent::protocol::fhs::{
        AuthorizationDataClass as DataClass, AuthorizationDestination as Destination,
    };
    use galaxia_agent::runtime::events::Collected;
    use galaxia_agent::runtime::providers::{self, Scope};
    use std::io::Write;

    // Un ciclo completo de anuncios (cada provider se anuncia cada 30 s).
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(35)
        && !(!node.peers.stars().is_empty() && node.peers.satellites().len() >= 3)
    {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!("providers conocidos: {}", node.peers.all().len());
    let policy = HeadlessPolicy::from_env();
    let authorizer = Authorizer::new(Arc::default(), None, Some(policy));
    let dispatcher = Dispatcher::new(node.clone());
    match args.first().map(String::as_str) {
        Some("chat") => {
            let text = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "Hola, ¿quién eres?".into());
            let star = node
                .peers
                .stars()
                .into_iter()
                .next()
                .ok_or("no hay Stars conocidos")?;
            // El mensaje literal al Star elegido es el consentimiento implícito.
            let grant = authorizer.implicit_user_message(
                &star,
                Some(Scope::Community),
                &std::collections::HashSet::new(),
                &text,
            )?;
            let t0 = std::time::Instant::now();
            let mut first: Option<Duration> = None;
            let outcome = dispatcher
                .llm(
                    LlmRequest {
                        star_did: &star.did,
                        model: "",
                        timeout: Duration::from_secs(300),
                        temperature: 0.7,
                        system: "Responde en español, breve.",
                        user_text: &text,
                        user_grant: &grant,
                        notes: &[],
                        blocks: vec![],
                        history: vec![],
                        tool_outputs: vec![],
                        tool_notes: vec![],
                        tools: &[],
                    },
                    |delta| {
                        first.get_or_insert(t0.elapsed());
                        print!("{delta}");
                        let _ = std::io::stdout().flush();
                    },
                )
                .await?;
            println!(
                "\n— Star {:?} · primer delta {:?} · total {:?}",
                outcome.executed_by,
                first,
                t0.elapsed()
            );
        }
        Some("portal") => {
            use galaxia_agent::p2p::{framing, wire};
            use galaxia_agent::protocol::fhs::{
                envelope::Payload, AgentStartMessage, AuthorizationDecisionMessage,
                AuthorizationItemDecision, ChatRequestMessage, Message,
            };
            let addr: libp2p::Multiaddr = args
                .get(1)
                .ok_or("falta la multiaddr del agente")?
                .parse()?;
            let question = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| "¿Qué dice el artículo 3 sobre la educación?".into());
            let allow = policy == HeadlessPolicy::AllowSynthetic;
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
                    Some(Payload::AuthorizationRequested(request)) => {
                        println!(
                            "  · autorización {}: {:?} → {}",
                            request.authorization_id,
                            request
                                .items
                                .iter()
                                .map(|i| format!("{} → {}", i.capability_id, i.provider_name))
                                .collect::<Vec<_>>(),
                            if allow { "permito (política sintética)" } else { "deniego" }
                        );
                        framing::write_envelope(
                            &mut stream,
                            &send(Payload::AuthorizationDecision(AuthorizationDecisionMessage {
                                authorization_id: request.authorization_id,
                                batch_digest: request.batch_digest,
                                decisions: request
                                    .items
                                    .iter()
                                    .map(|i| AuthorizationItemDecision {
                                        item_id: i.item_id.clone(),
                                        allow,
                                    })
                                    .collect(),
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
        Some("tool") => {
            let capability = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| "knowledge.query".into());
            let tool_name = args.get(2).cloned().unwrap_or_else(|| "kb_query".into());
            let json: serde_json::Value =
                serde_json::from_str(args.get(3).map(String::as_str).unwrap_or("{}"))?;
            let arguments = dynamic::from_json(&json)?;
            let tool = providers::tools_for(&node.peers, &[capability.as_str()], None)
                .into_iter()
                .find(|t| t.name == tool_name)
                .ok_or("ningún nodo ofrece esa herramienta")?;
            let digest = galaxia_agent::authorization::tool_args_digest(&arguments)?;
            let mut item = ItemSpec::new(
                "probe-0",
                capability.clone(),
                tool.provider_id.clone(),
                tool.provider_name.clone(),
                DataClass::ToolArgs,
                "argumentos de la sonda (datos sintéticos)",
                digest,
            );
            item.destination = Destination::Network;
            let sink = Collected::default();
            let ctx = Ctx {
                session: "probe",
                conversation: "probe",
                turn: "probe",
                sink: &sink,
            };
            let resolution = authorizer.request(&ctx, vec![item], DEFAULT_TTL).await?;
            let Some(grant) = resolution.grant("probe-0") else {
                return Err("denegado: la sonda no tiene interfaz; define FHS_AUTH_POLICY=allow-synthetic solo si los datos son sintéticos".into());
            };
            let outcome = dispatcher
                .tool_call(
                    grant,
                    ToolCallSpec {
                        capability: &capability,
                        extra_capabilities: &[],
                        tool_name: &tool.name,
                        provider_did: &tool.provider_id,
                        timeout: Duration::from_secs(120),
                        outbound: Outbound::Args {
                            domain: galaxia_agent::authorization::DOMAIN_TOOL_ARGS,
                            value: &arguments,
                        },
                        contract: None,
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
            "uso: galaxia-agent probe chat \"texto\" | probe tool <capability> <tool> '<json>' | probe portal <multiaddr> \"pregunta\""
        ),
    }
    Ok(())
}
