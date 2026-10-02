//! El único código del Navigator que envía contenido del usuario a otro nodo.
//!
//! Publica ofertas, abre streams con providers, llama al Star y sube a IPFS
//! **solo** con un [`Grant`] vigente cuyo digest coincide con los bytes que
//! realmente van a salir y cuyo DID es el único que puede recibirlos. El
//! permiso se consume de forma atómica antes de escribir en el transporte;
//! un fallo posterior nunca lo devuelve (SPEC-AUTH-0001).
//!
//! `tests/outbound_boundary.rs` verifica que ningún otro archivo llame a las
//! funciones crudas del SDK.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use galaxia_fhs::authorization as digest;
use serde_json::Value;

use super::{Grant, GrantError};
use crate::ipfs::IpfsService;
use crate::llm::{self, StarModel};
use crate::p2p::{client, node::NodeHandle};
use crate::protocol::fhs::{
    artifact_ref, dynamic_value::Kind, ArtifactRef, AuthorizationOutcome as Outcome, DynamicObject,
    DynamicValue, Message, ToolDefinition,
};

pub const CAPABILITY_IPFS_UPLOAD: &str = "ipfs.upload";
pub const CAPABILITY_CHAT: &str = "chat";
const QUESTION_MARKER: &str = "\n\n[Pregunta del usuario]\n";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DispatchError {
    #[error("{0}")]
    Grant(#[from] GrantError),
    #[error("{0}")]
    Digest(String),
    /// Falló la misión. `maybe_sent`: el contenido pudo haberse escrito; no se
    /// reintenta con el mismo permiso.
    #[error("{error}")]
    Mission { error: String, maybe_sent: bool },
}

impl From<digest::DigestError> for DispatchError {
    fn from(error: digest::DigestError) -> Self {
        Self::Digest(error.to_string())
    }
}

/// Qué va a salir, de modo que el digest se calcula **de lo que se envía**.
pub enum Outbound<'a> {
    /// Los argumentos tal cual (valor dinámico canónico `cv1`).
    Args {
        domain: &'static str,
        value: &'a DynamicValue,
    },
    /// Un comando (SPEC-CMD-0001): el digest es el cv1 de `{ args, tool }` y los
    /// argumentos que salen son `args`. `spec.tool_name` debe ser `tool`.
    Command {
        tool: &'a str,
        args: &'a DynamicValue,
    },
    /// Un archivo: los argumentos son `{file: artifact}` y el digest es el de
    /// los bytes reales. Un artefacto IPFS debe ser uno que este Dispatcher
    /// subió con permiso y con esos mismos bytes.
    Document {
        artifact: &'a ArtifactRef,
        bytes: &'a [u8],
    },
}

pub struct ToolCallSpec<'a> {
    pub capability: &'a str,
    pub extra_capabilities: &'a [String],
    pub tool_name: &'a str,
    /// DID al que el runtime cree que va; debe ser el del `Grant`.
    pub provider_did: &'a str,
    pub timeout: Duration,
    pub outbound: Outbound<'a>,
    /// Contrato vigente del nodo, solo para comandos: debe ser el autorizado.
    pub contract: Option<&'a super::Contract>,
}

/// Fragmentos derivados (texto de KB, RAG, documento) que van al LLM.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedBlock {
    pub header: String,
    pub chunks: Vec<String>,
}

impl DerivedBlock {
    pub fn new(header: impl Into<String>, chunks: Vec<String>) -> Self {
        Self {
            header: header.into(),
            chunks,
        }
    }

    /// Lo que cubre el permiso: los fragmentos en orden de envío.
    pub fn digest(&self) -> [u8; 32] {
        let refs: Vec<&str> = self.chunks.iter().map(String::as_str).collect();
        digest::chunks_digest(digest::DOMAIN_DERIVED_TEXT, &refs)
    }

    pub fn chars(&self) -> usize {
        self.chunks.iter().map(|c| c.chars().count()).sum()
    }

    fn render(&self) -> String {
        format!("{}\n{}", self.header, self.chunks.join("\n---\n"))
    }
}

/// Resultado de una herramienta que vuelve al LLM.
pub struct ToolOutputPart<'a> {
    pub call_id: &'a str,
    pub content: &'a str,
    pub grant: &'a Grant,
}

pub struct LlmRequest<'a> {
    pub star_did: &'a str,
    pub model: &'a str,
    pub timeout: Duration,
    pub temperature: f64,
    pub system: &'a str,
    /// El mensaje literal del usuario y su permiso (implícito o explícito).
    pub user_text: &'a str,
    pub user_grant: &'a Grant,
    /// Texto estático del Navigator (sin datos del usuario) tras la pregunta.
    pub notes: &'a [String],
    pub blocks: Vec<(&'a DerivedBlock, &'a Grant)>,
    /// Mensajes del propio modelo (con sus llamadas a herramientas).
    pub history: Vec<Message>,
    pub tool_outputs: Vec<ToolOutputPart<'a>>,
    /// Respuestas estáticas del Navigator a llamadas que no se ejecutaron
    /// (denegadas o fuera del registro): no llevan datos del usuario.
    pub tool_notes: Vec<(String, String)>,
    pub tools: &'a [ToolDefinition],
}

pub struct LlmOutcome {
    pub text: String,
    pub calls: Vec<(String, String, Value)>,
    pub executed_by: Option<String>,
}

#[derive(Clone)]
pub struct Dispatcher {
    node: NodeHandle,
    /// CIDs subidos con permiso en este turno y el digest de sus bytes.
    uploaded: Arc<Mutex<HashMap<String, [u8; 32]>>>,
}

fn file_arguments(artifact: &ArtifactRef) -> DynamicValue {
    let mut object = DynamicObject::default();
    object.fields.insert(
        "file".into(),
        DynamicValue {
            kind: Some(Kind::ArtifactRef(artifact.clone())),
        },
    );
    DynamicValue {
        kind: Some(Kind::ObjectValue(object)),
    }
}

/// Renderiza lo que ve el LLM. Solo aquí se mezcla el contenido del usuario
/// con el derivado, de modo que lo enviado es exactamente lo autorizado.
pub fn render_user_content(
    user_text: &str,
    blocks: &[(&DerivedBlock, &Grant)],
    notes: &[String],
) -> String {
    let mut content = String::new();
    if !blocks.is_empty() {
        content.push_str(
            &blocks
                .iter()
                .map(|(block, _)| block.render())
                .collect::<Vec<_>>()
                .join("\n\n"),
        );
        content.push_str(QUESTION_MARKER);
    }
    content.push_str(user_text);
    for note in notes {
        content.push_str("\n\n");
        content.push_str(note);
    }
    content
}

impl Dispatcher {
    pub fn new(node: NodeHandle) -> Self {
        Self {
            node,
            uploaded: Arc::default(),
        }
    }

    pub fn node(&self) -> &NodeHandle {
        &self.node
    }

    /// DID de este Navigator (destino del ítem de subida a su Kubo local).
    pub fn local_did(&self) -> String {
        self.node.identity.did.clone()
    }

    /// Llamada a una herramienta de un provider (OCR, RAG, KB, `/calc`…).
    pub async fn tool_call(
        &self,
        grant: &Grant,
        spec: ToolCallSpec<'_>,
    ) -> Result<client::ToolOutcome, DispatchError> {
        let (arguments, digest_now) = match spec.outbound {
            Outbound::Args { domain, value } => {
                (value.clone(), digest::value_digest(domain, value)?)
            }
            Outbound::Command { tool, args } => {
                if tool != spec.tool_name {
                    return Err(DispatchError::Digest(
                        "la herramienta no es la del comando autorizado".into(),
                    ));
                }
                (
                    args.clone(),
                    galaxia_fhs::commands::command_args_digest(tool, args)?,
                )
            }
            Outbound::Document { artifact, bytes } => {
                let digest_now = digest::document_digest(bytes);
                if let Some(artifact_ref::Transport::Ipfs(ipfs)) = &artifact.transport {
                    let known = self
                        .uploaded
                        .lock()
                        .expect("uploaded")
                        .get(&ipfs.cid)
                        .copied();
                    if known != Some(digest_now) {
                        return Err(DispatchError::Digest(
                            "el CID no corresponde a un archivo subido con permiso".into(),
                        ));
                    }
                }
                (file_arguments(artifact), digest_now)
            }
        };
        grant.verify_contract(spec.contract)?;
        grant.consume(spec.capability, spec.provider_did, digest_now)?;
        let did = grant.provider_did().to_string();
        let outcome = client::call_tool(
            &self.node,
            client::ToolRequest {
                capability: spec.capability.to_string(),
                extra_capabilities: spec.extra_capabilities.to_vec(),
                tool_name: spec.tool_name.to_string(),
                arguments,
                preferred_provider: Some(did.clone()),
                timeout: spec.timeout,
                mission_id: None,
                allowed_provider_dids: Some(vec![did.clone()]),
            },
        )
        .await;
        match outcome {
            Ok(outcome) if outcome.provider == did => {
                grant.finish(Outcome::Sent, "");
                Ok(outcome)
            }
            Ok(outcome) => {
                let error = format!("respondió {} y no el nodo autorizado", outcome.provider);
                grant.finish(Outcome::Failed, &error);
                Err(DispatchError::Mission {
                    error,
                    maybe_sent: true,
                })
            }
            Err(error) => {
                let maybe_sent = !matches!(
                    error,
                    client::MissionError::NoBids(_)
                        | client::MissionError::Dial { .. }
                        | client::MissionError::Stream(..)
                );
                grant.finish(Outcome::Failed, &error.to_string());
                Err(DispatchError::Mission {
                    error: error.to_string(),
                    maybe_sent,
                })
            }
        }
    }

    /// Sube el archivo al Kubo local (IPFS). El CID queda registrado con el
    /// digest de sus bytes para que solo ese contenido se pueda leer por CID.
    pub async fn ipfs_upload(
        &self,
        grant: &Grant,
        service: &IpfsService,
        session: &str,
        turn_id: &str,
        bytes: Vec<u8>,
        reuse: bool,
    ) -> Result<String, DispatchError> {
        let digest_now = digest::document_digest(&bytes);
        grant.consume(CAPABILITY_IPFS_UPLOAD, &self.local_did(), digest_now)?;
        match service.upload(session, turn_id, bytes, reuse).await {
            Ok(cid) => {
                self.uploaded
                    .lock()
                    .expect("uploaded")
                    .insert(cid.clone(), digest_now);
                grant.finish(Outcome::Sent, &cid);
                Ok(cid)
            }
            Err(error) => {
                grant.finish(Outcome::Failed, error.code);
                Err(DispatchError::Mission {
                    error: format!("{}: {}", error.code, error.message),
                    maybe_sent: true,
                })
            }
        }
    }

    /// Llamada al Star con el mensaje del usuario y, si los hay, los bloques
    /// derivados y las salidas de herramientas, cada uno con su permiso.
    pub async fn llm(
        &self,
        request: LlmRequest<'_>,
        on_delta: impl FnMut(&str),
    ) -> Result<LlmOutcome, DispatchError> {
        // Valida todo antes de consumir nada (todo o nada).
        let user_digest = digest::user_message_digest(request.user_text);
        let mut checks: Vec<(&Grant, &str, [u8; 32])> =
            vec![(request.user_grant, CAPABILITY_CHAT, user_digest)];
        for (block, grant) in &request.blocks {
            checks.push((grant, CAPABILITY_CHAT, block.digest()));
        }
        for part in &request.tool_outputs {
            checks.push((
                part.grant,
                CAPABILITY_CHAT,
                digest::text_digest(digest::DOMAIN_TOOL_OUTPUT, part.content),
            ));
        }
        for (grant, capability, digest_now) in &checks {
            grant.check(capability, request.star_did, *digest_now)?;
        }
        for (grant, capability, digest_now) in &checks {
            grant.consume(capability, request.star_did, *digest_now)?;
        }

        let mut messages = vec![
            Message {
                role: "system".into(),
                content: request.system.to_string(),
                ..Default::default()
            },
            Message {
                role: "user".into(),
                content: render_user_content(request.user_text, &request.blocks, request.notes),
                ..Default::default()
            },
        ];
        messages.extend(request.history.iter().cloned());
        for part in &request.tool_outputs {
            messages.push(Message {
                role: "tool".into(),
                content: part.content.to_string(),
                tool_call_id: part.call_id.to_string(),
                tool_calls: vec![],
            });
        }
        for (call_id, note) in &request.tool_notes {
            messages.push(Message {
                role: "tool".into(),
                content: note.clone(),
                tool_call_id: call_id.clone(),
                tool_calls: vec![],
            });
        }
        let model = StarModel::new(
            self.node.clone(),
            request.model.to_string(),
            Some(request.star_did.to_string()),
            request.timeout,
        );
        let completion = model
            .complete_streaming(
                llm::request(&messages, request.tools, request.temperature),
                on_delta,
            )
            .await;
        let finish = |outcome: Outcome, reason: &str| {
            for (grant, _, _) in &checks {
                grant.finish(outcome, reason);
            }
        };
        match completion {
            Ok(response) => {
                finish(Outcome::Sent, "");
                Ok(LlmOutcome {
                    text: llm::text_of(&response),
                    calls: llm::tool_calls_of(&response),
                    executed_by: model.executed_by(),
                })
            }
            Err(error) => {
                finish(Outcome::Failed, &error.to_string());
                Err(DispatchError::Mission {
                    error: error.to_string(),
                    maybe_sent: true,
                })
            }
        }
    }
}
