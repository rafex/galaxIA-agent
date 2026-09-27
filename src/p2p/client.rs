//! Misiones de punta a punta hacia Star y Satellites: subasta, marcado,
//! stream directo `/fhs/v1/0.1.0`, handshake y mensajes de la misión.
//! Equivale a `P2pLlmGateway` + `P2pMcpHost` + `dialProvider` del TS.

use std::time::{Duration, Instant};

use libp2p::{Multiaddr, PeerId};

use crate::p2p::framing::{self, FrameError};
use crate::p2p::mission::{self, MissionRequest, DEFAULT_BID_DEADLINE};
use crate::p2p::node::{peer_id_of, with_peer_id, NodeHandle};
use crate::p2p::wire::{self, FHS_WIRE_VERSION};
use crate::protocol::fhs::{
    envelope::Payload, ChatRequestMessage, DynamicValue, HandshakeMessage, Message, ToolCall,
    ToolCallFunction, ToolCallRequestMessage, ToolDefinition,
};

const DIAL_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum MissionError {
    #[error("no hay providers que pujen por {0}")]
    NoBids(String),
    #[error("no se pudo conectar con {provider}: {detail}")]
    Dial { provider: String, detail: String },
    #[error("no se pudo abrir el stream FHS con {0}: {1}")]
    Stream(String, String),
    #[error("stream FHS: {0}")]
    Frame(#[from] FrameError),
    #[error("{0} cerró el stream antes de terminar")]
    Closed(String),
    #[error("respuesta inesperada de {provider}: {detail}")]
    Unexpected { provider: String, detail: String },
    #[error("{provider}: {detail}")]
    Remote { provider: String, detail: String },
    #[error("tiempo agotado esperando a {0}")]
    Timeout(String),
}

pub struct ChatOutcome {
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
    /// DID del Star que ejecutó.
    pub provider: String,
    pub dispatch_ms: Option<u64>,
}

pub struct ToolOutcome {
    pub result: Option<DynamicValue>,
    /// DID del Satellite que ejecutó.
    pub provider: String,
    pub dispatch_ms: Option<u64>,
}

fn is_loopback(addr: &str) -> bool {
    let host = addr.split('/').nth(2).unwrap_or_default();
    host == "localhost" || host == "::1" || host.starts_with("127.")
}

/// Marca al provider probando sus multiaddrs en orden, loopback al final
/// (E2E-027: la primera dirección anunciada suele ser 127.0.0.1).
pub async fn dial_provider(
    node: &NodeHandle,
    provider_did: &str,
    addrs: &[String],
) -> Result<PeerId, MissionError> {
    if addrs.is_empty() {
        return Err(MissionError::Dial {
            provider: provider_did.into(),
            detail: "no anunció multiaddrs".into(),
        });
    }
    let mut ordered: Vec<&String> = addrs.iter().collect();
    ordered.sort_by_key(|a| is_loopback(a));
    let mut failures = Vec::new();
    for raw in ordered {
        let Ok(addr) = raw.parse::<Multiaddr>() else {
            failures.push(format!("{raw} (multiaddr inválida)"));
            continue;
        };
        if let Some(peer) = peer_id_of(&addr) {
            if node.is_connected(&peer) {
                return Ok(peer);
            }
        }
        match tokio::time::timeout(DIAL_ATTEMPT_TIMEOUT, node.dial(addr)).await {
            Ok(Ok(peer)) => return Ok(peer),
            Ok(Err(error)) => failures.push(format!("{raw} ({error})")),
            Err(_) => failures.push(format!(
                "{raw} (sin respuesta en {} s)",
                DIAL_ATTEMPT_TIMEOUT.as_secs()
            )),
        }
    }
    Err(MissionError::Dial {
        provider: provider_did.into(),
        detail: failures.join("; "),
    })
}

async fn open_session(
    node: &NodeHandle,
    provider: &str,
    addrs: &[String],
) -> Result<libp2p::Stream, MissionError> {
    let peer = dial_provider(node, provider, addrs).await?;
    let mut control = node.stream_control();
    let mut stream = control
        .open_stream(peer, NodeHandle::fhs_protocol())
        .await
        .map_err(|e| MissionError::Stream(provider.into(), e.to_string()))?;

    let listen_addrs = node
        .status()
        .await
        .map(|s| s.multiaddrs)
        .unwrap_or_default();
    let handshake = wire::sealed_envelope(
        &node.identity,
        "",
        Payload::Handshake(HandshakeMessage {
            fhs_version: FHS_WIRE_VERSION.into(),
            listen_addrs,
            beacon: Some(wire::navigator_beacon("Navigator FHS")),
            delegation_token: None,
        }),
    );
    framing::write_envelope(&mut stream, &handshake).await?;
    match framing::read_verified(&mut stream).await? {
        Some(envelope) if matches!(envelope.payload, Some(Payload::HandshakeAck(_))) => Ok(stream),
        Some(envelope) => Err(MissionError::Unexpected {
            provider: provider.into(),
            detail: format!(
                "esperaba handshake_ack y llegó {:?}",
                payload_case(&envelope.payload)
            ),
        }),
        None => Err(MissionError::Closed(provider.into())),
    }
}

fn payload_case(payload: &Option<Payload>) -> &'static str {
    match payload {
        None => "nada",
        Some(Payload::HandshakeAck(_)) => "handshake_ack",
        Some(Payload::Error(_)) => "error",
        Some(Payload::ChatDelta(_)) => "chat_delta",
        Some(Payload::ChatCompleted(_)) => "chat_completed",
        Some(Payload::ChatError(_)) => "chat_error",
        Some(Payload::DispatchAck(_)) => "dispatch_ack",
        Some(Payload::ToolResult(_)) => "tool_result",
        Some(Payload::ToolError(_)) => "tool_error",
        Some(_) => "otro",
    }
}

pub struct ChatRequest {
    pub messages: Vec<Message>,
    pub tools: Vec<ToolDefinition>,
    pub model: String,
    /// Star que el runtime eligió (gana si puja).
    pub preferred_provider: Option<String>,
    pub timeout: Duration,
}

/// Misión `chat`: entrega cada delta a `on_delta` mientras Star genera.
pub async fn chat(
    node: &NodeHandle,
    request: ChatRequest,
    mut on_delta: impl FnMut(&str),
) -> Result<ChatOutcome, MissionError> {
    let started = Instant::now();
    let winner = mission::run_mission_cycle(
        node,
        MissionRequest {
            mission_type: "chat",
            required_capabilities: vec!["chat".into()],
            preferred_model: Some(request.model.clone()).filter(|m| !m.is_empty()),
            preferred_provider: request.preferred_provider.clone(),
            bid_deadline: DEFAULT_BID_DEADLINE,
        },
    )
    .await
    .ok_or_else(|| MissionError::NoBids("chat".into()))?;
    let provider = winner.bid.provider_did.clone();
    let mission_id = winner.mission_id.clone();

    let work = async {
        let mut stream = open_session(node, &provider, &winner.bid.provider_multiaddrs).await?;
        let chat = wire::sealed_envelope(
            &node.identity,
            "",
            Payload::ChatRequest(ChatRequestMessage {
                mission_id: mission_id.clone(),
                messages: request.messages,
                tools: request.tools,
                model: request.model,
                ..Default::default()
            }),
        );
        framing::write_envelope(&mut stream, &chat).await?;
        let mut content = String::new();
        let mut dispatch_ms = None;
        loop {
            let Some(envelope) = framing::read_verified(&mut stream).await? else {
                return Err(MissionError::Closed(provider.clone()));
            };
            match envelope.payload {
                Some(Payload::DispatchAck(_)) => {
                    dispatch_ms = Some(started.elapsed().as_millis() as u64)
                }
                Some(Payload::ChatDelta(delta)) => {
                    if !delta.delta.is_empty() {
                        content.push_str(&delta.delta);
                        on_delta(&delta.delta);
                    }
                }
                Some(Payload::ChatCompleted(done)) => {
                    if content.is_empty() {
                        content = done.content;
                    }
                    return Ok(ChatOutcome {
                        content,
                        tool_calls: done.tool_calls,
                        provider: provider.clone(),
                        dispatch_ms,
                    });
                }
                Some(Payload::ChatError(error)) => {
                    return Err(MissionError::Remote {
                        provider: provider.clone(),
                        detail: error.error,
                    })
                }
                Some(Payload::Error(error)) => {
                    return Err(MissionError::Remote {
                        provider: provider.clone(),
                        detail: error.message,
                    })
                }
                _ => {}
            }
        }
    };
    tokio::time::timeout(request.timeout, work)
        .await
        .map_err(|_| MissionError::Timeout(provider.clone()))?
}

pub struct ToolRequest {
    pub capability: String,
    pub tool_name: String,
    pub arguments: DynamicValue,
    /// Satellite que el runtime eligió (gana si puja).
    pub preferred_provider: Option<String>,
    pub timeout: Duration,
}

/// Misión `tool_call`: una llamada, un resultado.
pub async fn call_tool(
    node: &NodeHandle,
    request: ToolRequest,
) -> Result<ToolOutcome, MissionError> {
    let started = Instant::now();
    let winner = mission::run_mission_cycle(
        node,
        MissionRequest {
            mission_type: "tool_call",
            required_capabilities: vec![request.capability.clone()],
            preferred_model: None,
            preferred_provider: request.preferred_provider.clone(),
            bid_deadline: DEFAULT_BID_DEADLINE,
        },
    )
    .await
    .ok_or_else(|| MissionError::NoBids(request.capability.clone()))?;
    let provider = winner.bid.provider_did.clone();
    let mission_id = winner.mission_id.clone();

    let work = async {
        let mut stream = open_session(node, &provider, &winner.bid.provider_multiaddrs).await?;
        let call = wire::sealed_envelope(
            &node.identity,
            "",
            Payload::ToolCall(ToolCallRequestMessage {
                mission_id: mission_id.clone(),
                tool_calls: vec![ToolCall {
                    id: mission_id.clone(),
                    r#type: "function".into(),
                    function: Some(ToolCallFunction {
                        name: request.tool_name,
                        arguments: Some(request.arguments),
                    }),
                }],
            }),
        );
        framing::write_envelope(&mut stream, &call).await?;
        let mut dispatch_ms = None;
        loop {
            let Some(envelope) = framing::read_verified(&mut stream).await? else {
                return Err(MissionError::Closed(provider.clone()));
            };
            match envelope.payload {
                Some(Payload::DispatchAck(_)) => {
                    dispatch_ms = Some(started.elapsed().as_millis() as u64)
                }
                Some(Payload::ToolResult(result)) => {
                    return Ok(ToolOutcome {
                        result: result.result,
                        provider: provider.clone(),
                        dispatch_ms,
                    })
                }
                Some(Payload::ToolError(error)) => {
                    return Err(MissionError::Remote {
                        provider: provider.clone(),
                        detail: error.error,
                    })
                }
                Some(Payload::Error(error)) => {
                    return Err(MissionError::Remote {
                        provider: provider.clone(),
                        detail: error.message,
                    })
                }
                _ => {}
            }
        }
    };
    tokio::time::timeout(request.timeout, work)
        .await
        .map_err(|_| MissionError::Timeout(provider.clone()))?
}

/// Dirección para marcar: agrega `/p2p/<peer>` si hace falta (útil en tests).
pub fn dialable(addr: Multiaddr, peer: PeerId) -> Multiaddr {
    with_peer_id(addr, peer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_detection_matches_the_ts_rule() {
        assert!(is_loopback("/ip4/127.0.0.1/tcp/4003/tls/ws/p2p/x"));
        assert!(is_loopback("/dns4/localhost/tcp/1"));
        assert!(!is_loopback("/ip4/192.168.1.167/tcp/4003/tls/ws"));
    }
}
