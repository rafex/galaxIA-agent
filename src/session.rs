//! Sesión del Portal por libp2p (`/fhs/v1/0.1.0`): puerto de
//! `portal-session.ts`.
//!
//! El navegador abre un stream al Navigator, hace handshake y manda
//! `agentStart`, `chatRequest`, `authorization.decision` y `chatCancel`. Cada
//! turno corre en su propia tarea para que la sesión siga leyendo (cancelar,
//! decidir una autorización) mientras el LLM responde. A diferencia del TS,
//! `chatCancel` aborta de verdad el turno en curso.
//!
//! Autorización por uso (SPEC-AUTH-0001): el runtime pide la autorización y
//! la espera; esta sesión solo transporta `authorization.requested` hacia el
//! Portal y entrega la decisión al [`Authorizer`], que la consume una vez y
//! solo para esta sesión.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use futures::{AsyncReadExt, StreamExt};
use libp2p::PeerId;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::authorization::Authorizer;
use crate::ipfs::IpfsService;
use crate::p2p::{framing, node::NodeHandle, peer_cache::now_ms, wire};
use crate::protocol::fhs::{
    self, envelope::Payload, AgentStartMessage, ArtifactRef, ChatRequestMessage, DocumentContext,
    FhsErrorCode, HandshakeAckMessage,
};
use crate::runtime::agent::{
    AgentRuntime, AuthContext, IpfsPreference, IpfsTurn, Preferences, RagSource, Turn,
};
use crate::runtime::calc;
use crate::runtime::events::{AgentEvent, EventSink};
use crate::runtime::providers::Scope;

/// Valores por defecto de cada sesión (vetos, espera máxima) y recursos
/// compartidos.
#[derive(Clone)]
pub struct SessionDefaults {
    pub preferences: Preferences,
    /// IPFS de este Navigator (`None` sin `IPFS_API_URL`).
    pub ipfs: Option<IpfsService>,
    /// Tope de un adjunto (`ATTACHMENT_MAX_BYTES`).
    pub attachment_max_bytes: usize,
    /// Emisor único de permisos y tabla de decisiones pendientes.
    pub authorizer: Authorizer,
}

impl Default for SessionDefaults {
    fn default() -> Self {
        Self {
            preferences: Preferences::default(),
            ipfs: None,
            attachment_max_bytes: crate::config::DEFAULT_ATTACHMENT_MAX_BYTES,
            authorizer: Authorizer::for_tests(),
        }
    }
}

/// Traduce los eventos del runtime a Envelopes hacia el Portal.
struct SessionSink {
    tx: mpsc::UnboundedSender<Payload>,
    conversation: String,
}

impl EventSink for SessionSink {
    fn emit(&self, event: AgentEvent) {
        let mission_id = self.conversation.clone();
        let payload = match event {
            AgentEvent::Status { status, .. } => {
                Payload::AgentStatus(fhs::AgentStatusMessage { mission_id, status })
            }
            AgentEvent::LlmSelected { provider_id, model } => {
                Payload::StarSelected(fhs::StarSelectedMessage {
                    mission_id,
                    provider_id,
                    model,
                })
            }
            AgentEvent::ToolSelected {
                capability,
                provider_id,
            } => Payload::ToolSelected(fhs::ToolSelectedMessage {
                mission_id,
                provider_id,
                capability_id: capability,
            }),
            AgentEvent::AssistantDelta { text } => {
                Payload::AssistantDelta(fhs::AssistantDeltaMessage {
                    mission_id,
                    delta: text,
                })
            }
            AgentEvent::OcrExtracted { filename, text } => {
                Payload::OcrExtracted(fhs::OcrExtractedMessage {
                    mission_id,
                    filename,
                    text,
                })
            }
            AgentEvent::AuthorizationRequested {
                authorization_id,
                conversation_id,
                turn_id,
                expires_at,
                batch_digest,
                items,
            } => Payload::AuthorizationRequested(fhs::AuthorizationRequestedMessage {
                authorization_id,
                conversation_id,
                turn_id,
                expires_at,
                batch_digest,
                items,
            }),
            AgentEvent::AuthorizationResolved {
                authorization_id,
                outcome,
                items,
            } => Payload::AuthorizationResolved(fhs::AuthorizationResolvedMessage {
                authorization_id,
                outcome,
                items,
            }),
            AgentEvent::AssistantCompleted { provenance } => {
                Payload::AssistantCompleted(fhs::AssistantCompletedMessage {
                    mission_id,
                    content: String::new(),
                    provenance: Some(fhs::ProvenanceInfo {
                        provider_id: provenance.llm_provider_id,
                        model: provenance.model,
                        tool_provider_ids: provenance
                            .tools
                            .into_iter()
                            .map(|t| t.provider_id)
                            .collect(),
                        data_exported: provenance.data_exported,
                        jurisdiction: provenance.jurisdiction,
                        ..Default::default()
                    }),
                })
            }
            AgentEvent::Error { code, message } => {
                error_payload(&mission_id, code.as_str(), &message)
            }
            // Como en el TS: no se reenvían al Portal.
            AgentEvent::ToolRunning { .. }
            | AgentEvent::ToolCompleted { .. }
            | AgentEvent::ToolError { .. }
            | AgentEvent::ProviderFailover { .. } => return,
        };
        let _ = self.tx.send(payload);
    }
}

fn error_payload(mission_id: &str, code: &str, message: &str) -> Payload {
    let code = match code {
        "CANCELLED" => FhsErrorCode::Cancelled,
        "INVALID_ARGUMENTS" => FhsErrorCode::InvalidArguments,
        "OVERLOADED" => FhsErrorCode::Overloaded,
        "UNSUPPORTED_CAPABILITY" => FhsErrorCode::UnsupportedCapability,
        "UPSTREAM_UNAVAILABLE" => FhsErrorCode::UpstreamUnavailable,
        // Sin autorización no se envía nada: no es un fallo interno.
        "AUTHORIZATION_DENIED" | "AUTHORIZATION_EXPIRED" | "AUTHORIZATION_CANCELLED" => {
            FhsErrorCode::Unauthorized
        }
        _ => FhsErrorCode::InternalError,
    };
    Payload::Error(fhs::ErrorMessage {
        code: code as i32,
        message: format!("{mission_id}: {message}"),
    })
}

struct SessionState {
    /// Clave de la sesión (stream) para las cuotas de IPFS.
    key: String,
    session_id: Option<String>,
    preferences: Preferences,
    rag_active: HashSet<String>,
    active: HashMap<String, JoinHandle<()>>,
    ipfs: Option<IpfsService>,
    attachment_max_bytes: usize,
    authorizer: Authorizer,
}

/// Acepta sesiones del Portal mientras el nodo exista.
pub async fn serve(node: NodeHandle, defaults: SessionDefaults) {
    let mut incoming = match node.stream_control().accept(NodeHandle::fhs_protocol()) {
        Ok(incoming) => incoming,
        Err(error) => {
            tracing::error!("no se pudo aceptar {}: {error}", wire::FHS_STREAM_PROTOCOL);
            return;
        }
    };
    while let Some((peer, stream)) = incoming.next().await {
        let Some(permit) = node.admit_stream(peer) else {
            drop(stream);
            continue;
        };
        let (node, defaults) = (node.clone(), defaults.clone());
        tokio::spawn(async move {
            let _permit = permit;
            run_session(node, peer, stream, defaults).await;
        });
    }
}

async fn run_session(
    node: NodeHandle,
    peer: PeerId,
    stream: libp2p::Stream,
    defaults: SessionDefaults,
) {
    let (mut reader, mut writer) = stream.split();
    let first = match framing::read_verified(&mut reader).await {
        Ok(Some(envelope)) => envelope,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!("[portal-session] stream de {peer} inválido: {error}");
            return;
        }
    };
    if !matches!(first.payload, Some(Payload::Handshake(_))) {
        tracing::warn!("[portal-session] stream sin handshake desde {peer}");
        return;
    }
    let remote = first.source_peer_id.clone();
    let opened = std::time::Instant::now();
    tracing::info!("[portal-session] sesión abierta: {remote} desde {peer}");

    let (tx, mut rx) = mpsc::unbounded_channel::<Payload>();
    let writer_task = {
        let identity = node.identity.clone();
        let remote = remote.clone();
        tokio::spawn(async move {
            while let Some(payload) = rx.recv().await {
                let envelope = wire::sealed_envelope(&identity, &remote, payload);
                if let Err(error) = framing::write_envelope(&mut writer, &envelope).await {
                    tracing::warn!("[portal-session] no se pudo escribir a {remote}: {error}");
                    break;
                }
            }
        })
    };
    let _ = tx.send(Payload::HandshakeAck(HandshakeAckMessage {
        fhs_version: wire::FHS_WIRE_VERSION.into(),
        lease_seconds: 300,
        heartbeat_seconds: 30,
        lease_expires: now_ms() + 300_000,
        accepted_services: 0,
        trust_level: "community".into(),
    }));

    let state = Arc::new(Mutex::new(SessionState {
        key: Uuid::new_v4().to_string(),
        session_id: None,
        preferences: defaults.preferences.clone(),
        rag_active: HashSet::new(),
        active: HashMap::new(),
        ipfs: defaults.ipfs.clone(),
        attachment_max_bytes: defaults.attachment_max_bytes,
        authorizer: defaults.authorizer.clone(),
    }));
    loop {
        let envelope = match framing::read_verified(&mut reader).await {
            Ok(Some(envelope)) => envelope,
            Ok(None) => break,
            Err(error) => {
                tracing::warn!("[portal-session] error en la sesión {remote}: {error}");
                break;
            }
        };
        match envelope.payload {
            Some(Payload::AgentStart(start)) => {
                let mut s = state.lock().expect("session");
                s.session_id = Some(start.session_id.clone())
                    .filter(|id| !id.is_empty())
                    .or_else(|| s.session_id.clone())
                    .or_else(|| Some(Uuid::new_v4().to_string()));
                s.preferences = preferences_from_start(&start, &defaults.preferences);
            }
            Some(Payload::ChatRequest(request)) => handle_chat(&node, &state, &tx, request),
            Some(Payload::AuthorizationDecision(decision)) => {
                let (authorizer, session) = {
                    let s = state.lock().expect("session");
                    (s.authorizer.clone(), s.key.clone())
                };
                // La decisión se usa una sola vez y solo en esta sesión; las
                // desconocidas, repetidas o de otro lote se ignoran.
                if let Err(error) = authorizer.decide(&session, &decision) {
                    tracing::warn!("[portal-session] decisión de autorización ignorada: {error}");
                }
            }
            Some(Payload::AuthorizationStatusRequest(request)) => {
                let (authorizer, session) = {
                    let s = state.lock().expect("session");
                    (s.authorizer.clone(), s.key.clone())
                };
                let _ = tx.send(Payload::AuthorizationResolved(
                    authorizer.status(&session, &request.authorization_id),
                ));
            }
            Some(Payload::ChatCancel(cancel)) => {
                let handles: Vec<JoinHandle<()>> = {
                    let mut s = state.lock().expect("session");
                    let id = if cancel.mission_id.is_empty() {
                        s.session_id.clone().unwrap_or_default()
                    } else {
                        cancel.mission_id.clone()
                    };
                    s.authorizer.cancel_conversation(&s.key, &id);
                    s.active.remove(&id).into_iter().collect()
                };
                for handle in handles {
                    handle.abort();
                }
                let id = state
                    .lock()
                    .expect("session")
                    .session_id
                    .clone()
                    .unwrap_or(cancel.mission_id);
                let _ = tx.send(error_payload(&id, "CANCELLED", "Misión cancelada"));
            }
            _ => {}
        }
    }
    {
        let mut s = state.lock().expect("session");
        s.authorizer.cancel_session(&s.key);
        for (_, handle) in s.active.drain() {
            handle.abort();
        }
    }
    drop(tx);
    let _ = writer_task.await;
    tracing::info!(
        "[portal-session] sesión cerrada: {remote} (duró {} s)",
        opened.elapsed().as_secs()
    );
}

fn preferences_from_start(start: &AgentStartMessage, defaults: &Preferences) -> Preferences {
    Preferences {
        model: start.model.clone(),
        scope: Scope::parse(&start.scope).or(defaults.scope),
        kb: start.kb.clone(),
        kb_max_per_question: usize::try_from(start.kb_max_per_question)
            .ok()
            .filter(|n| *n > 0)
            .unwrap_or(1),
        rag_source: if start.rag_source == fhs::RagSource::Network as i32 {
            RagSource::Network
        } else {
            RagSource::Local
        },
        max_wait: defaults.max_wait,
        vetoed: defaults.vetoed.clone(),
        calc_nodes: defaults.calc_nodes.clone(),
        ipfs: IpfsPreference {
            enabled: start.ipfs_enabled,
            network: start.ipfs_network.clone(),
            reuse: start.ipfs_retention == "reuse",
        },
    }
}

/// Un adjunto por mensaje, inline y dentro del tope; el backend lo valida
/// aunque el Portal ya lo haga.
fn validate_artifacts(artifacts: &[ArtifactRef], max_bytes: usize) -> Result<(), String> {
    if artifacts.len() > 1 {
        return Err("Solo se admite un adjunto por mensaje".into());
    }
    match artifacts.first().and_then(|a| a.transport.as_ref()) {
        None if artifacts.is_empty() => Ok(()),
        Some(fhs::artifact_ref::Transport::Inline(inline)) => {
            if inline.data.len() > max_bytes {
                Err(format!(
                    "El adjunto supera el máximo de {} MB",
                    max_bytes / (1024 * 1024)
                ))
            } else {
                Ok(())
            }
        }
        _ => Err("El adjunto debe enviarse inline".into()),
    }
}

enum TurnWork {
    /// Adjunto nuevo: OCR y, según la fuente de RAG, indexar y seguir.
    Attachment(Turn),
    /// Responder (KB, RAG y contexto piden su propia autorización).
    Run(Turn),
    /// `/calc`: valida, pide autorización y calcula en el nodo móvil.
    Calc(Turn),
}

fn handle_chat(
    node: &NodeHandle,
    state: &Arc<Mutex<SessionState>>,
    tx: &mpsc::UnboundedSender<Payload>,
    request: ChatRequestMessage,
) {
    let (conversation, mut preferences) = {
        let mut s = state.lock().expect("session");
        let conversation = s
            .session_id
            .clone()
            .or_else(|| Some(request.mission_id.clone()).filter(|id| !id.is_empty()))
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        s.session_id = Some(conversation.clone());
        (conversation, s.preferences.clone())
    };
    let Some(last) = request
        .messages
        .last()
        .filter(|m| m.role == "user" && !m.content.trim().is_empty())
    else {
        let _ = tx.send(error_payload(
            &conversation,
            "INVALID_ARGUMENTS",
            "El último mensaje debe ser del usuario y no estar vacío",
        ));
        return;
    };
    let max_bytes = state.lock().expect("session").attachment_max_bytes;
    if let Err(message) = validate_artifacts(&request.artifacts, max_bytes) {
        let _ = tx.send(error_payload(&conversation, "INVALID_ARGUMENTS", &message));
        return;
    }
    if !request.model.is_empty() {
        preferences.model = request.model.clone();
    }
    let document_id = Some(request.document_id.clone())
        .filter(|id| !id.is_empty())
        .or_else(|| {
            request
                .document_context
                .as_ref()
                .map(|c| c.document_id.clone())
                .filter(|id| !id.is_empty())
        });
    let rag_active = state
        .lock()
        .expect("session")
        .rag_active
        .contains(&conversation);
    let turn = Turn {
        message: last.content.clone(),
        artifacts: request.artifacts.clone(),
        document_context: request
            .document_context
            .clone()
            .filter(|c: &DocumentContext| !c.chunks.is_empty()),
        document_id,
        rag_active,
    };
    let work = if turn.artifacts.is_empty() && calc::parse_command(&turn.message).is_some() {
        TurnWork::Calc(turn)
    } else if turn.artifacts.is_empty() {
        TurnWork::Run(turn)
    } else {
        TurnWork::Attachment(turn)
    };
    spawn_turn(node, state, tx, conversation, work, preferences);
}

fn spawn_turn(
    node: &NodeHandle,
    state: &Arc<Mutex<SessionState>>,
    tx: &mpsc::UnboundedSender<Payload>,
    conversation: String,
    work: TurnWork,
    preferences: Preferences,
) {
    let node = node.clone();
    let tx = tx.clone();
    let task_state = state.clone();
    let id = conversation.clone();
    let turn_id = Uuid::new_v4().to_string();
    let (service, session_key, authorizer) = {
        let s = state.lock().expect("session");
        (s.ipfs.clone(), s.key.clone(), s.authorizer.clone())
    };
    let ipfs_turn = IpfsTurn {
        guard: service.as_ref().map(|svc| svc.release_guard(&turn_id)),
        service: service.clone(),
        session: session_key.clone(),
        turn_id: turn_id.clone(),
    };
    let auth = AuthContext {
        authorizer,
        session: session_key,
        turn_id: turn_id.clone(),
    };
    let handle = tokio::spawn(async move {
        let sink = SessionSink {
            tx: tx.clone(),
            conversation: id.clone(),
        };
        let mut runtime = AgentRuntime::new(node, &sink, id.clone(), auth).with_ipfs(ipfs_turn);
        let turn = match work {
            TurnWork::Calc(turn) => {
                if let Err(error) = runtime.run_calc(&turn.message, &preferences).await {
                    sink.emit(AgentEvent::Error {
                        code: error.code.into(),
                        message: error.message,
                    });
                }
                task_state.lock().expect("session").active.remove(&id);
                return;
            }
            TurnWork::Attachment(turn) => {
                match attachment(&mut runtime, &sink, &task_state, turn, &preferences).await {
                    Some(turn) => turn,
                    None => {
                        task_state.lock().expect("session").active.remove(&id);
                        return;
                    }
                }
            }
            TurnWork::Run(turn) => turn,
        };
        if let Err(error) = runtime.run(turn, &preferences).await {
            sink.emit(AgentEvent::Error {
                code: error.code.into(),
                message: error.message,
            });
        }
        task_state.lock().expect("session").active.remove(&id);
    });
    // Registro síncrono: el barrido de IPFS sabe si el turno sigue vivo
    // aunque se pierda el aviso de fin de turno.
    if let Some(service) = &service {
        service.turns().register(&turn_id, handle.abort_handle());
    }
    state
        .lock()
        .expect("session")
        .active
        .insert(conversation, handle);
}

/// Adjunto recién subido: OCR; con RAG local el Portal indexa y reenvía la
/// pregunta, con RAG de red se indexa aquí y se sigue con la misma pregunta.
async fn attachment(
    runtime: &mut AgentRuntime<'_>,
    sink: &SessionSink,
    state: &Arc<Mutex<SessionState>>,
    mut turn: Turn,
    preferences: &Preferences,
) -> Option<Turn> {
    let artifact: ArtifactRef = turn.artifacts.first()?.clone();
    let (filename, text) = match runtime.process_attachment(&artifact, preferences).await {
        Ok(result) => result,
        Err(error) => {
            sink.emit(AgentEvent::Error {
                code: error.code.into(),
                message: error.message,
            });
            return None;
        }
    };
    sink.emit(AgentEvent::OcrExtracted {
        filename,
        text: text.clone(),
    });
    if preferences.rag_source == RagSource::Local {
        return None;
    }
    if !runtime
        .index_document(
            &text,
            "user-upload",
            turn.document_id.as_deref(),
            preferences,
        )
        .await
    {
        sink.emit(AgentEvent::Error {
            code: "RAG_UNAVAILABLE".into(),
            message: "No se pudo indexar el documento en el RAG de la red".into(),
        });
        return None;
    }
    state
        .lock()
        .expect("session")
        .rag_active
        .insert(runtime.conversation_id().to_string());
    if turn.message.trim().is_empty() {
        return None;
    }
    turn.artifacts.clear();
    turn.rag_active = true;
    Some(turn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::events::{Provenance, ToolProvenance};

    #[test]
    fn maps_events_to_portal_payloads() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = SessionSink {
            tx,
            conversation: "conv".into(),
        };
        sink.emit(AgentEvent::AssistantDelta {
            text: "hola".into(),
        });
        sink.emit(AgentEvent::ToolRunning {
            name: "x".into(),
            provider_id: "p".into(),
        });
        sink.emit(AgentEvent::AssistantCompleted {
            provenance: Provenance {
                llm_provider_id: "did:star".into(),
                llm_provider_name: "Star".into(),
                model: "auto".into(),
                tools: vec![ToolProvenance {
                    capability: "knowledge.query".into(),
                    provider_id: "did:kb".into(),
                    provider_name: "KB".into(),
                }],
                data_exported: true,
                jurisdiction: "red local comunitaria".into(),
            },
        });
        sink.emit(AgentEvent::Error {
            code: "CANCELLED".into(),
            message: "Misión cancelada".into(),
        });

        assert!(
            matches!(rx.try_recv().unwrap(), Payload::AssistantDelta(d) if d.delta == "hola" && d.mission_id == "conv")
        );
        match rx.try_recv().unwrap() {
            Payload::AssistantCompleted(done) => {
                let p = done.provenance.unwrap();
                assert_eq!(p.tool_provider_ids, ["did:kb"]);
                assert!(p.data_exported);
            }
            other => panic!("{other:?}"),
        }
        match rx.try_recv().unwrap() {
            Payload::Error(e) => {
                assert_eq!(e.code, FhsErrorCode::Cancelled as i32);
                assert_eq!(e.message, "conv: Misión cancelada");
            }
            other => panic!("{other:?}"),
        }
        assert!(rx.try_recv().is_err(), "ToolRunning no se reenvía");
    }

    #[test]
    fn one_inline_attachment_within_the_limit() {
        let inline = |n: usize| ArtifactRef {
            transport: Some(fhs::artifact_ref::Transport::Inline(fhs::InlineArtifact {
                data: vec![0; n],
                filename: "a.pdf".into(),
            })),
        };
        assert!(validate_artifacts(&[], 10).is_ok());
        assert!(validate_artifacts(&[inline(10)], 10).is_ok());
        assert!(validate_artifacts(&[inline(11)], 10).is_err());
        assert_eq!(
            validate_artifacts(&[inline(1), inline(1)], 10).unwrap_err(),
            "Solo se admite un adjunto por mensaje"
        );
        let ipfs = ArtifactRef {
            transport: Some(fhs::artifact_ref::Transport::Ipfs(Default::default())),
        };
        assert!(validate_artifacts(&[ipfs], 10).is_err());
        assert!(matches!(
            error_payload("c", "INVALID_ARGUMENTS", "x"),
            Payload::Error(e) if e.code == FhsErrorCode::InvalidArguments as i32
        ));
    }

    #[test]
    fn preferences_follow_agent_start() {
        let start = AgentStartMessage {
            scope: "local".into(),
            kb: "did:kb".into(),
            kb_max_per_question: 0,
            rag_source: fhs::RagSource::Network as i32,
            ipfs_enabled: true,
            ipfs_network: "public".into(),
            ipfs_retention: "reuse".into(),
            ..Default::default()
        };
        let p = preferences_from_start(&start, &Preferences::default());
        assert!(p.ipfs.enabled && p.ipfs.reuse);
        assert_eq!(p.ipfs.network, "public");
        assert_eq!(p.scope, Some(Scope::Local));
        assert_eq!(p.kb, "did:kb");
        assert_eq!(p.kb_max_per_question, 1);
        assert_eq!(p.rag_source, RagSource::Network);
        let invalid = preferences_from_start(
            &AgentStartMessage {
                scope: "x".into(),
                ..Default::default()
            },
            &Preferences::default(),
        );
        assert_eq!(invalid.scope, Some(Scope::Community));
    }
}
