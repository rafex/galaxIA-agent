//! Un turno de chat: el equivalente de `AgentRuntime.run` del TS.
//!
//! La orquestación es determinista (no la decide el LLM): contexto de
//! documento (RAG local o de red), KB, prompt y una sola ronda de tools. El LLM
//! se usa a través de Rig (`llm::StarModel`), con streaming cuando la llamada
//! no ofrece tools.
//!
//! **Autorización por uso (SPEC-AUTHZ-0001, DEC-0099):** este runtime no envía
//! nada por su cuenta. Cada salida de contenido del usuario hacia otro nodo
//! pide un permiso (`Authorizer`) y se despacha solo por el `Dispatcher`, que
//! comprueba el digest de lo que sale y fija el nodo. Lo que una operación
//! produce (texto de OCR, fragmentos de KB/RAG, resultado de una herramienta)
//! se autoriza en una etapa posterior, con su digest real, antes de reenviarse.
//!
//! Diferencias deliberadas con el TS (defectos que no se copian):
//! - el OCR con failover apunta al siguiente provider y pide una autorización
//!   nueva para él (no vuelve a subastar ni envía a un nodo no visto);
//! - los resultados del RAG por red se leen como arreglo (en el TS nunca
//!   aportaban fragmentos, mismo defecto que E2E-029);
//! - la procedencia lleva nombres y `data_exported` real;
//! - ya no se fusionan varias KBs por el RAG de red: sus fragmentos van
//!   directos al LLM, cada uno con su permiso.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use galaxia_fhs::authorization as digest;
use serde_json::{json, Value};

use crate::authorization::dispatcher::{
    DerivedBlock, Dispatcher, LlmOutcome, LlmRequest, Outbound, ToolCallSpec, ToolOutputPart,
    CAPABILITY_CHAT, CAPABILITY_IPFS_UPLOAD,
};
use crate::authorization::{
    Authorizer, Ctx, Grant, ItemOutcome, ItemSpec, Resolution, DEFAULT_TTL,
};
use crate::ipfs::{self, IpfsService, ReleaseGuard};
use crate::llm;
use crate::p2p::{dynamic, node::NodeHandle};
use crate::protocol::fhs::{
    self, artifact_ref, ArtifactRef, AuthorizationDataClass as DataClass,
    AuthorizationDestination as Destination, AuthorizationRetention as Retention, DocumentContext,
    DynamicValue, Message, ToolDefinition, ToolInputSchema,
};
use crate::runtime::commands::{self as engine_cmd, CommandEngine};
use crate::runtime::events::{AgentEvent, EventSink, KbCandidate, Provenance, ToolProvenance};
use crate::runtime::kb;
use crate::runtime::providers::{self, LoadedTool, Scope};
use galaxia_fhs::commands as cmd;

pub const SYSTEM_PROMPT: &str = "Eres un asistente útil de una red soberana de IA comunitaria. \
Responde siempre en español. \
Si recibes fragmentos de una base de conocimiento o de documentos, responde con base en los que se \
relacionan con la pregunta e ignora los que no tengan relación; si ninguno la responde, dilo. \
Si necesitas usar una herramienta, hazlo UNA SOLA VEZ y luego responde con la información obtenida. \
No repitas llamadas a herramientas. \
Sé conciso: responde en un máximo de 5 oraciones salvo que el usuario pida más detalle.";

const TEMPERATURE: f64 = 0.7;

/// Registro de herramientas que el LLM puede pedir por su cuenta. Una que no
/// esté aquí se deniega; las de efectos externos no están (SPEC-AUTHZ-0001).
/// Los comandos de chat (`/nombre`) no pasan por el LLM: los atiende la tabla
/// de comandos autodescubiertos (SPEC-CMD-0001).
const LLM_TOOL_CAPABILITIES: [&str; 2] = ["knowledge.query", "document.query"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RagSource {
    Local,
    Network,
}

/// Adjuntos por IPFS pedidos en `agentStart` (DEC-0095).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IpfsPreference {
    pub enabled: bool,
    /// `public` o `private`.
    pub network: String,
    /// `retention: reuse`: el CID queda fijado hasta que el operador lo libere.
    pub reuse: bool,
}

#[derive(Clone, Debug)]
pub struct Preferences {
    /// Modelo pedido; vacío o `auto` = el que ofrezca el Star.
    pub model: String,
    pub scope: Option<Scope>,
    /// KB elegida a mano (modo manual). Vacío = modo recomendado. También
    /// pide autorización: elegir una KB no exime del permiso.
    pub kb: String,
    pub kb_max_per_question: usize,
    pub rag_source: RagSource,
    pub max_wait: Duration,
    /// DIDs vetados (`FHS_VETOED_PROVIDERS`).
    pub vetoed: Arc<HashSet<String>>,
    pub ipfs: IpfsPreference,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            model: String::new(),
            scope: Some(Scope::Community),
            kb: String::new(),
            kb_max_per_question: 1,
            rag_source: RagSource::Local,
            max_wait: llm::DEFAULT_LLM_TIMEOUT,
            vetoed: Arc::default(),
            ipfs: IpfsPreference::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Turn {
    pub message: String,
    pub artifacts: Vec<ArtifactRef>,
    pub document_context: Option<DocumentContext>,
    pub document_id: Option<String>,
    /// La conversación tiene un documento indexado en el RAG de red.
    pub rag_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct RuntimeError {
    pub code: &'static str,
    pub message: String,
}

impl RuntimeError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

struct UsedTool {
    capability: String,
    provider_id: String,
    provider_name: String,
}

/// Con quién y para qué turno se piden los permisos.
pub struct AuthContext {
    pub authorizer: Authorizer,
    /// Sesión del Portal (una decisión solo vale para la sesión que la recibió).
    pub session: String,
    pub turn_id: String,
}

pub struct AgentRuntime<'a> {
    node: NodeHandle,
    dispatcher: Dispatcher,
    events: &'a dyn EventSink,
    conversation_id: String,
    auth: AuthContext,
    used_tools: Vec<UsedTool>,
    last_ocr_error: Option<String>,
    ipfs: Option<IpfsTurn>,
}

/// Contexto IPFS de un turno de la sesión del Portal.
pub struct IpfsTurn {
    /// `None`: este Navigator no tiene IPFS (el turno falla cerrado si el
    /// usuario lo pidió).
    pub service: Option<IpfsService>,
    /// Sesión del Portal (cuota de una subida en curso por sesión).
    pub session: String,
    pub turn_id: String,
    /// Fija la gracia de los leases del turno al soltarse el runtime.
    pub guard: Option<ReleaseGuard>,
}

struct ResolvedLlm {
    provider_id: String,
    provider_name: String,
    model: String,
}

fn args_digest(domain: &str, value: &DynamicValue) -> Result<[u8; 32], RuntimeError> {
    digest::value_digest(domain, value)
        .map_err(|e| RuntimeError::new("INVALID_ARGUMENTS", e.to_string()))
}

fn size_label(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.0} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} bytes")
    }
}

impl<'a> AgentRuntime<'a> {
    pub fn new(
        node: NodeHandle,
        events: &'a dyn EventSink,
        conversation_id: impl Into<String>,
        auth: AuthContext,
    ) -> Self {
        Self {
            dispatcher: Dispatcher::new(node.clone()),
            node,
            events,
            conversation_id: conversation_id.into(),
            auth,
            used_tools: Vec::new(),
            last_ocr_error: None,
            ipfs: None,
        }
    }

    /// Turno de una sesión del Portal: habilita subir adjuntos por IPFS.
    pub fn with_ipfs(mut self, ipfs: IpfsTurn) -> Self {
        self.ipfs = Some(ipfs);
        self
    }

    pub fn conversation_id(&self) -> &str {
        &self.conversation_id
    }

    fn emit(&self, event: AgentEvent) {
        self.events.emit(event);
    }

    fn status(&self, status: &str, message: &str) {
        self.emit(AgentEvent::Status {
            status: status.into(),
            message: message.into(),
        });
    }

    // ── Autorización ────────────────────────────────────────────────────────

    /// Pide autorización para un lote y espera la decisión.
    async fn ask(&self, items: Vec<ItemSpec>) -> Result<Resolution, RuntimeError> {
        let ctx = Ctx {
            session: &self.auth.session,
            conversation: &self.conversation_id,
            turn: &self.auth.turn_id,
            sink: self.events,
        };
        self.auth
            .authorizer
            .request(&ctx, items, DEFAULT_TTL)
            .await
            .map_err(|e| RuntimeError::new("INTERNAL_ERROR", e.to_string()))
    }

    fn denial(resolution: &Resolution, item_id: &str) -> RuntimeError {
        let outcome = resolution
            .items
            .iter()
            .find(|r| r.item_id == item_id)
            .map(|r| &r.outcome);
        match outcome {
            Some(ItemOutcome::Expired) => RuntimeError::new(
                "AUTHORIZATION_EXPIRED",
                "No se envió nada: la autorización venció.",
            ),
            Some(ItemOutcome::Cancelled) => RuntimeError::new(
                "AUTHORIZATION_CANCELLED",
                "No se envió nada: la solicitud se canceló.",
            ),
            _ => RuntimeError::new(
                "AUTHORIZATION_DENIED",
                "No se envió nada: no autorizaste este envío.",
            ),
        }
    }

    fn destination_of(&self, did: &str) -> Destination {
        match self
            .node
            .peers
            .get(did)
            .map(|p| providers::provider_scope(&p))
        {
            Some(Scope::Local) => Destination::Local,
            Some(Scope::Community) => Destination::Community,
            Some(Scope::External) => Destination::External,
            _ => Destination::Network,
        }
    }

    fn tool_item(
        &self,
        id: &str,
        tool: &LoadedTool,
        class: DataClass,
        summary: String,
        digest: [u8; 32],
    ) -> ItemSpec {
        let mut item = ItemSpec::new(
            id,
            tool.capability.clone(),
            tool.provider_id.clone(),
            tool.provider_name.clone(),
            class,
            summary,
            digest,
        );
        item.destination = self.destination_of(&tool.provider_id);
        item
    }

    /// El mensaje literal del usuario al Star elegido (consentimiento
    /// implícito, P5), emitido como un `Grant` como cualquier otro.
    fn implicit_grant(
        &self,
        llm: &ResolvedLlm,
        prefs: &Preferences,
        message: &str,
    ) -> Result<Grant, RuntimeError> {
        let star =
            self.node.peers.get(&llm.provider_id).ok_or_else(|| {
                RuntimeError::new("NO_LLM", "El Star elegido ya no está disponible")
            })?;
        self.auth
            .authorizer
            .implicit_user_message(&star, prefs.scope, &prefs.vetoed, message)
            .map_err(|e| RuntimeError::new("NO_LLM", e.to_string()))
    }

    // ── Envío (todo por el Dispatcher) ──────────────────────────────────────

    /// Llamada a una herramienta con un `Grant`; emite los eventos de progreso.
    async fn dispatch_tool(
        &mut self,
        grant: &Grant,
        tool: &LoadedTool,
        outbound: Outbound<'_>,
        prefs: &Preferences,
        silent: bool,
        extra_capabilities: &[String],
    ) -> Result<Value, String> {
        self.dispatch_tool_with(
            grant,
            tool,
            outbound,
            prefs,
            silent,
            extra_capabilities,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_tool_with(
        &mut self,
        grant: &Grant,
        tool: &LoadedTool,
        outbound: Outbound<'_>,
        prefs: &Preferences,
        silent: bool,
        extra_capabilities: &[String],
        contract: Option<&crate::authorization::Contract>,
    ) -> Result<Value, String> {
        let started = Instant::now();
        if !silent {
            self.emit(AgentEvent::ToolRunning {
                name: tool.name.clone(),
                provider_id: tool.provider_id.clone(),
            });
        }
        let outcome = self
            .dispatcher
            .tool_call(
                grant,
                ToolCallSpec {
                    capability: &tool.capability,
                    extra_capabilities,
                    tool_name: &tool.name,
                    provider_did: &tool.provider_id,
                    timeout: prefs.max_wait,
                    outbound,
                    contract,
                },
            )
            .await;
        match outcome {
            Ok(outcome) => {
                if !silent {
                    self.emit(AgentEvent::ToolCompleted {
                        name: tool.name.clone(),
                        duration_ms: started.elapsed().as_millis() as u64,
                        success: true,
                    });
                }
                let provider_name = if outcome.provider == tool.provider_id {
                    tool.provider_name.clone()
                } else {
                    outcome.provider.clone()
                };
                self.used_tools.push(UsedTool {
                    capability: tool.capability.clone(),
                    provider_id: outcome.provider,
                    provider_name,
                });
                Ok(outcome
                    .result
                    .as_ref()
                    .map(dynamic::to_json)
                    .unwrap_or(Value::Null))
            }
            Err(error) => {
                let message = error.to_string();
                if !silent {
                    self.emit(AgentEvent::ToolError {
                        name: tool.name.clone(),
                        error: message.clone(),
                    });
                }
                Err(message)
            }
        }
    }

    /// Llamada al Star por el Dispatcher; con streaming solo si no hay tools.
    async fn llm_call(
        &self,
        request: LlmRequest<'_>,
        emit_answer: bool,
    ) -> Result<LlmOutcome, RuntimeError> {
        let stream = emit_answer && request.tools.is_empty();
        let mut streamed = false;
        let outcome = self
            .dispatcher
            .llm(request, |delta| {
                if stream {
                    streamed = true;
                    self.emit(AgentEvent::AssistantDelta {
                        text: delta.to_string(),
                    });
                }
            })
            .await
            .map_err(|e| RuntimeError::new("LLM_ERROR", e.to_string()))?;
        if emit_answer && !streamed && outcome.calls.is_empty() && !outcome.text.is_empty() {
            self.emit(AgentEvent::AssistantDelta {
                text: outcome.text.clone(),
            });
        }
        Ok(outcome)
    }

    // ── Un turno de pregunta ────────────────────────────────────────────────

    /// Ejecuta un turno completo y devuelve el texto de la respuesta.
    pub async fn run(
        &mut self,
        turn: Turn,
        preferences: &Preferences,
    ) -> Result<String, RuntimeError> {
        self.used_tools.clear();
        self.status("classifying", "Analizando la petición");
        let capabilities = classify_intent(&turn.message);

        self.status("resolving-model", "Eligiendo modelo");
        self.settle_stars(preferences).await;
        let llm = self.resolve_llm(preferences)?;

        self.status("resolving-tools", "Buscando herramientas");
        for capability in &capabilities {
            self.settle_tools(capability, preferences).await;
        }
        // Solo las herramientas del registro pueden ofrecerse al LLM.
        let tools: Vec<LoadedTool> =
            providers::tools_for(&self.node.peers, &capabilities, preferences.scope)
                .into_iter()
                .filter(|t| LLM_TOOL_CAPABILITIES.contains(&t.capability.as_str()))
                .collect();

        let mut blocks: Vec<DerivedBlock> = Vec::new();
        let mut notes: Vec<String> = Vec::new();

        // Contexto de documento que el propio Portal recuperó (RAG local).
        if let Some(context) = &turn.document_context {
            let chunks: Vec<String> = context
                .chunks
                .iter()
                .filter(|c| !c.text.trim().is_empty())
                .map(|c| {
                    let name = if c.filename.is_empty() {
                        context.filename.as_str()
                    } else {
                        c.filename.as_str()
                    };
                    format!("[{name} · fragmento {}]\n{}", c.chunk_index + 1, c.text)
                })
                .collect();
            if !chunks.is_empty() {
                blocks.push(DerivedBlock::new(
                    "[Fragmentos relevantes del documento recuperados por RAG local]",
                    chunks,
                ));
            }
        }

        // RAG de red del documento indexado en esta conversación.
        if turn.rag_active && preferences.rag_source == RagSource::Network {
            if let Some(chunks) = self
                .query_rag(&turn.message, preferences, 3, turn.document_id.as_deref())
                .await
            {
                blocks.push(DerivedBlock::new(
                    "[Fragmentos relevantes del documento indexado]",
                    chunks,
                ));
            }
        }

        // KB: se pide autorización para consultar (también si la fijó el usuario).
        let (kb_blocks, kb_asked) = self.collect_kb_blocks(&turn.message, preferences).await?;
        let kb_found = !kb_blocks.is_empty();
        blocks.extend(kb_blocks);
        if kb_asked && !kb_found {
            notes.push(
                "(No se consultó ninguna base de conocimiento: no fue autorizada o no devolvió fragmentos.)"
                    .into(),
            );
        }

        // Etapa de seguimiento: los fragmentos ya existen; se autoriza enviarlos al Star.
        let authorized = self.authorize_blocks(&llm, blocks).await?;
        let blocks_sent: Vec<DerivedBlock> = authorized.iter().map(|(b, _)| b.clone()).collect();

        let user_grant = self.implicit_grant(&llm, preferences, &turn.message)?;
        let tool_defs: Vec<ToolDefinition> = tools.iter().map(tool_definition).collect();
        let first = self
            .llm_call(
                LlmRequest {
                    star_did: &llm.provider_id,
                    model: &llm.model,
                    timeout: preferences.max_wait,
                    temperature: TEMPERATURE,
                    system: SYSTEM_PROMPT,
                    user_text: &turn.message,
                    user_grant: &user_grant,
                    notes: &notes,
                    blocks: authorized.iter().map(|(b, g)| (b, g)).collect(),
                    history: vec![],
                    tool_outputs: vec![],
                    tool_notes: vec![],
                    tools: &tool_defs,
                },
                true,
            )
            .await?;
        let mut answer = first.text.clone();
        let mut executed_by = first.executed_by.clone();
        if !first.calls.is_empty() {
            let (text, by) = self
                .tool_round(
                    &llm,
                    preferences,
                    &turn,
                    &tools,
                    &blocks_sent,
                    &notes,
                    &first,
                )
                .await?;
            answer = text;
            executed_by = by.or(executed_by);
        }

        let executed_by = executed_by.unwrap_or_else(|| llm.provider_id.clone());
        let llm_name = if executed_by == llm.provider_id {
            llm.provider_name.clone()
        } else {
            executed_by.clone()
        };
        self.emit(AgentEvent::AssistantCompleted {
            provenance: Provenance {
                llm_provider_id: executed_by,
                llm_provider_name: llm_name,
                model: llm.model.clone(),
                tools: self.provenance_tools(),
                data_exported: !self.used_tools.is_empty(),
                jurisdiction: "red local comunitaria".into(),
            },
        });
        Ok(answer)
    }

    fn provenance_tools(&self) -> Vec<ToolProvenance> {
        self.used_tools
            .iter()
            .map(|t| ToolProvenance {
                capability: t.capability.clone(),
                provider_id: t.provider_id.clone(),
                provider_name: t.provider_name.clone(),
            })
            .collect()
    }

    /// Autoriza enviar al Star los fragmentos derivados que ya existen.
    async fn authorize_blocks(
        &self,
        llm: &ResolvedLlm,
        blocks: Vec<DerivedBlock>,
    ) -> Result<Vec<(DerivedBlock, Grant)>, RuntimeError> {
        if blocks.is_empty() {
            return Ok(vec![]);
        }
        let destination = self.destination_of(&llm.provider_id);
        let items: Vec<ItemSpec> = blocks
            .iter()
            .enumerate()
            .map(|(i, block)| {
                let mut item = ItemSpec::new(
                    format!("ctx-{i}"),
                    CAPABILITY_CHAT,
                    llm.provider_id.clone(),
                    llm.provider_name.clone(),
                    DataClass::DerivedText,
                    format!(
                        "{} fragmento(s), {} caracteres, derivados de tus documentos o bases de conocimiento",
                        block.chunks.len(),
                        block.chars()
                    ),
                    block.digest(),
                );
                item.destination = destination;
                item
            })
            .collect();
        let resolution = self.ask(items).await?;
        Ok(blocks
            .into_iter()
            .enumerate()
            .filter_map(|(i, block)| {
                resolution
                    .grant(&format!("ctx-{i}"))
                    .map(|grant| (block, grant.clone()))
            })
            .collect())
    }

    /// Una sola ronda de herramientas pedidas por el LLM. Cada llamada pide
    /// su autorización con los argumentos visibles, y su resultado (que vuelve
    /// al LLM) se autoriza en una etapa de seguimiento.
    #[allow(clippy::too_many_arguments)]
    async fn tool_round(
        &mut self,
        llm: &ResolvedLlm,
        prefs: &Preferences,
        turn: &Turn,
        tools: &[LoadedTool],
        blocks_sent: &[DerivedBlock],
        notes: &[String],
        first: &LlmOutcome,
    ) -> Result<(String, Option<String>), RuntimeError> {
        let history = vec![Message {
            role: "assistant".into(),
            content: first.text.clone(),
            tool_call_id: String::new(),
            tool_calls: first
                .calls
                .iter()
                .map(|(id, name, args)| fhs::ToolCall {
                    id: id.clone(),
                    r#type: "function".into(),
                    function: Some(fhs::ToolCallFunction {
                        name: name.clone(),
                        arguments: dynamic::from_json(args).ok(),
                    }),
                })
                .collect(),
        }];

        // 1. Qué se acepta: solo el registro; lo demás se deniega sin enviar nada.
        let mut tool_notes: Vec<(String, String)> = Vec::new();
        let mut accepted: Vec<(String, LoadedTool, DynamicValue)> = Vec::new();
        let mut seen = HashSet::new();
        for (id, name, args) in &first.calls {
            if !seen.insert(format!("{name}:{args}")) {
                continue;
            }
            let Some(tool) = tools.iter().find(|t| &t.name == name).cloned() else {
                self.emit(AgentEvent::ToolError {
                    name: name.clone(),
                    error: format!("Herramienta desconocida: {name}"),
                });
                tool_notes.push((
                    id.clone(),
                    json!({"error": "herramienta no permitida"}).to_string(),
                ));
                continue;
            };
            if prefs.vetoed.contains(&tool.provider_id) {
                tool_notes.push((id.clone(), json!({"error": "proveedor vetado"}).to_string()));
                continue;
            }
            match dynamic::from_json(args) {
                Ok(value) => accepted.push((id.clone(), tool, value)),
                Err(error) => {
                    tool_notes.push((id.clone(), json!({"error": error.to_string()}).to_string()))
                }
            }
        }

        // 2. Autorización por llamada, con los argumentos canónicos.
        let mut outputs: Vec<(String, String)> = Vec::new();
        if !accepted.is_empty() {
            let mut items = Vec::new();
            for (i, (_, tool, value)) in accepted.iter().enumerate() {
                let count = match &value.kind {
                    Some(fhs::dynamic_value::Kind::ObjectValue(o)) => o.fields.len(),
                    _ => 0,
                };
                items.push(self.tool_item(
                    &format!("tool-{i}"),
                    tool,
                    DataClass::ToolArgs,
                    format!(
                        "el modelo pidió «{}» con {count} argumento(s); su resultado volverá al modelo",
                        tool.name
                    ),
                    args_digest(digest::DOMAIN_TOOL_ARGS, value)?,
                ));
            }
            let resolution = self.ask(items).await?;
            for (i, (id, tool, value)) in accepted.iter().enumerate() {
                match resolution.grant(&format!("tool-{i}")) {
                    Some(grant) => {
                        let content = match self
                            .dispatch_tool(
                                grant,
                                tool,
                                Outbound::Args {
                                    domain: digest::DOMAIN_TOOL_ARGS,
                                    value,
                                },
                                prefs,
                                false,
                                &[],
                            )
                            .await
                        {
                            Ok(result) => extract_text(&result),
                            Err(error) => json!({"error": error}).to_string(),
                        };
                        outputs.push((id.clone(), content));
                    }
                    None => tool_notes.push((
                        id.clone(),
                        json!({"error": "el usuario no autorizó esta herramienta"}).to_string(),
                    )),
                }
            }
        }

        // 3. Etapa de seguimiento: los resultados (y el contexto que se
        //    reenvía) ya existen; se autoriza enviarlos de vuelta al Star.
        let destination = self.destination_of(&llm.provider_id);
        let mut items = Vec::new();
        for (i, (_, content)) in outputs.iter().enumerate() {
            let mut item = ItemSpec::new(
                format!("out-{i}"),
                CAPABILITY_CHAT,
                llm.provider_id.clone(),
                llm.provider_name.clone(),
                DataClass::ToolOutputToLlm,
                format!(
                    "resultado de una herramienta, {} caracteres",
                    content.chars().count()
                ),
                digest::text_digest(digest::DOMAIN_TOOL_OUTPUT, content),
            );
            item.destination = destination;
            items.push(item);
        }
        for (j, block) in blocks_sent.iter().enumerate() {
            let mut item = ItemSpec::new(
                format!("ctx-{j}"),
                CAPABILITY_CHAT,
                llm.provider_id.clone(),
                llm.provider_name.clone(),
                DataClass::DerivedText,
                format!(
                    "reenvío de {} fragmento(s), {} caracteres",
                    block.chunks.len(),
                    block.chars()
                ),
                block.digest(),
            );
            item.destination = destination;
            items.push(item);
        }
        let resolution = if items.is_empty() {
            None
        } else {
            Some(self.ask(items).await?)
        };
        let mut granted_outputs: Vec<(String, String, Grant)> = Vec::new();
        for (i, (id, content)) in outputs.into_iter().enumerate() {
            match resolution
                .as_ref()
                .and_then(|r| r.grant(&format!("out-{i}")))
            {
                Some(grant) => granted_outputs.push((id, content, grant.clone())),
                None => tool_notes.push((
                    id,
                    json!({"error": "el usuario no autorizó enviar este resultado"}).to_string(),
                )),
            }
        }
        let granted_blocks: Vec<(&DerivedBlock, Grant)> = blocks_sent
            .iter()
            .enumerate()
            .filter_map(|(j, block)| {
                resolution
                    .as_ref()
                    .and_then(|r| r.grant(&format!("ctx-{j}")))
                    .map(|g| (block, g.clone()))
            })
            .collect();

        // 4. Segunda llamada al Star.
        let user_grant = self.implicit_grant(llm, prefs, &turn.message)?;
        let second = self
            .llm_call(
                LlmRequest {
                    star_did: &llm.provider_id,
                    model: &llm.model,
                    timeout: prefs.max_wait,
                    temperature: TEMPERATURE,
                    system: SYSTEM_PROMPT,
                    user_text: &turn.message,
                    user_grant: &user_grant,
                    notes,
                    blocks: granted_blocks.iter().map(|(b, g)| (*b, g)).collect(),
                    history,
                    tool_outputs: granted_outputs
                        .iter()
                        .map(|(id, content, grant)| ToolOutputPart {
                            call_id: id,
                            content,
                            grant,
                        })
                        .collect(),
                    tool_notes,
                    tools: &[],
                },
                true,
            )
            .await?;
        Ok((second.text, second.executed_by))
    }

    // ── Adjuntos (OCR, IPFS, RAG de red) ────────────────────────────────────

    /// Recién arrancado, los Stars pueden no haberse anunciado todavía.
    async fn settle_stars(&self, preferences: &Preferences) {
        let scope = preferences.scope;
        self.node
            .peers
            .settle(|peers| !providers::stars(peers, scope).is_empty())
            .await;
    }

    async fn settle_tools(&self, capability: &str, preferences: &Preferences) {
        let scope = preferences.scope;
        self.node
            .peers
            .settle(|peers| !providers::tools_for(peers, &[capability], scope).is_empty())
            .await;
    }

    fn resolve_llm(&self, preferences: &Preferences) -> Result<ResolvedLlm, RuntimeError> {
        let stars: Vec<_> = providers::stars(&self.node.peers, preferences.scope)
            .into_iter()
            .filter(|s| !preferences.vetoed.contains(&s.did))
            .collect();
        let star = stars.first().ok_or_else(|| {
            RuntimeError::new(
                "NO_LLM",
                "No hay Stars disponibles en el ámbito de privacidad elegido",
            )
        })?;
        let model = if preferences.model.is_empty() || preferences.model == "auto" {
            star.beacon
                .models
                .first()
                .map(|m| m.id.clone())
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| "auto".into())
        } else {
            preferences.model.clone()
        };
        self.emit(AgentEvent::LlmSelected {
            provider_id: star.did.clone(),
            model: model.clone(),
        });
        Ok(ResolvedLlm {
            provider_id: star.did.clone(),
            provider_name: star.name(),
            model,
        })
    }

    /// Valida que se puede subir a IPFS y devuelve el servicio y la red.
    fn ipfs_service(
        &self,
        preferences: &Preferences,
    ) -> Result<(IpfsService, String), RuntimeError> {
        let network = if preferences.ipfs.network.is_empty() {
            "public".to_string()
        } else {
            preferences.ipfs.network.clone()
        };
        let service = self
            .ipfs
            .as_ref()
            .and_then(|ipfs| ipfs.service.clone())
            .ok_or_else(|| {
                RuntimeError::new(
                    "UNSUPPORTED_CAPABILITY",
                    "IPFS no disponible en este Navigator",
                )
            })?;
        if service.network() != network {
            return Err(RuntimeError::new(
                "UNSUPPORTED_CAPABILITY",
                format!("Red IPFS {network} no configurada en este Navigator"),
            ));
        }
        Ok((service, network))
    }

    /// OCR para la sesión (adjunto recién subido). Una sola tarjeta con la
    /// subida a IPFS (si se pidió) y el nodo de OCR; el failover a otro nodo
    /// pide una autorización nueva. Devuelve `(nombre del archivo, texto)`.
    pub async fn process_attachment(
        &mut self,
        artifact: &ArtifactRef,
        preferences: &Preferences,
    ) -> Result<(String, String), RuntimeError> {
        let Some(artifact_ref::Transport::Inline(inline)) = &artifact.transport else {
            return Err(RuntimeError::new(
                "INVALID_ARGUMENTS",
                "El adjunto debe llegar inline; el Navigator lo sube a IPFS si se pidió",
            ));
        };
        let bytes = inline.data.clone();
        let filename = inline.filename.clone();
        self.settle_tools("document.ocr", preferences).await;

        let ipfs_on = preferences.ipfs.enabled;
        let (service, network) = if ipfs_on {
            let (service, network) = self.ipfs_service(preferences)?;
            (Some(service), network)
        } else {
            (None, String::new())
        };
        let extra: Vec<String> = if ipfs_on {
            vec![format!("ipfs.native.{network}")]
        } else {
            vec![]
        };
        let extra_refs: Vec<&str> = extra.iter().map(String::as_str).collect();
        let candidates: Vec<LoadedTool> = providers::tools_with(
            &self.node.peers,
            "document.ocr",
            &extra_refs,
            preferences.scope,
        )
        .into_iter()
        .filter(|t| !preferences.vetoed.contains(&t.provider_id))
        .collect();
        if candidates.is_empty() {
            return Err(RuntimeError::new(
                "NO_OCR_PROVIDER",
                if ipfs_on {
                    "Ningún OCR con acceso a IPFS"
                } else {
                    "No hay un Satélite de OCR disponible"
                },
            ));
        }

        let doc_digest = digest::document_digest(&bytes);
        let retention = if ipfs_on && preferences.ipfs.reuse {
            Retention::Reuse
        } else {
            Retention::Ephemeral
        };
        let label = format!("{filename} · {}", size_label(bytes.len()));
        let mut items = Vec::new();
        if ipfs_on {
            let mut upload = ItemSpec::new(
                "ipfs-upload",
                CAPABILITY_IPFS_UPLOAD,
                self.dispatcher.local_did(),
                "Kubo de este Navigator",
                DataClass::Document,
                format!("{label}: se sube a IPFS ({network}); en la red pública cualquiera con el CID puede leerlo"),
                doc_digest,
            );
            upload.destination = if network == "public" {
                Destination::PublicIpfs
            } else {
                Destination::Network
            };
            upload.retention = retention;
            upload.public_network = network == "public";
            items.push(upload);
        }
        let mut ocr = self.tool_item(
            "ocr-0",
            &candidates[0],
            DataClass::Document,
            format!("{label}: se envía para extraer su texto"),
            doc_digest,
        );
        ocr.retention = retention;
        if ipfs_on {
            ocr.depends_on = vec!["ipfs-upload".into()];
        }
        items.push(ocr);

        let resolution = self.ask(items).await?;
        let Some(first_grant) = resolution.grant("ocr-0").cloned() else {
            return Err(Self::denial(&resolution, "ocr-0"));
        };
        let mut current = artifact.clone();
        if ipfs_on {
            let grant = resolution
                .grant("ipfs-upload")
                .ok_or_else(|| Self::denial(&resolution, "ipfs-upload"))?;
            let ipfs = self.ipfs.as_ref().expect("ipfs");
            let service = service.expect("servicio IPFS");
            let (session, turn_id) = (ipfs.session.clone(), ipfs.turn_id.clone());
            let cid = self
                .dispatcher
                .ipfs_upload(
                    grant,
                    &service,
                    &session,
                    &turn_id,
                    bytes.clone(),
                    preferences.ipfs.reuse,
                )
                .await
                .map_err(|e| RuntimeError::new("IPFS_ERROR", e.to_string()))?;
            current = ArtifactRef {
                transport: Some(artifact_ref::Transport::Ipfs(fhs::IpfsArtifact {
                    cid,
                    network: network.clone(),
                    gateway_url: ipfs::gateway_hint(&network).into(),
                    filename: filename.clone(),
                    retention: if preferences.ipfs.reuse {
                        "reuse".into()
                    } else {
                        "ephemeral".into()
                    },
                })),
            };
        }

        self.last_ocr_error = None;
        let mut grant = first_grant;
        for (i, tool) in candidates.iter().enumerate() {
            self.emit(AgentEvent::ToolSelected {
                capability: tool.capability.clone(),
                provider_id: tool.provider_id.clone(),
            });
            match self
                .dispatch_tool(
                    &grant,
                    tool,
                    Outbound::Document {
                        artifact: &current,
                        bytes: &bytes,
                    },
                    preferences,
                    false,
                    &extra,
                )
                .await
            {
                Ok(result) => {
                    let text = extract_text(&result);
                    if !text.trim().is_empty() {
                        // El OCR ya terminó de leer: el CID solo necesita la gracia corta.
                        if let Some(guard) = self.ipfs.as_mut().and_then(|i| i.guard.as_mut()) {
                            guard.succeeded();
                        }
                        return Ok((filename, text));
                    }
                    self.last_ocr_error = Some("el OCR no devolvió texto".into());
                }
                Err(error) => self.last_ocr_error = Some(error),
            }
            // Failover: otro nodo recibe el archivo solo con una autorización nueva.
            let Some(next) = candidates.get(i + 1) else {
                break;
            };
            self.emit(AgentEvent::ProviderFailover {
                capability: tool.capability.clone(),
                from: tool.provider_id.clone(),
                reason: format!(
                    "{} → {}",
                    self.last_ocr_error.clone().unwrap_or_default(),
                    next.provider_id
                ),
            });
            let mut item = self.tool_item(
                &format!("ocr-{}", i + 1),
                next,
                DataClass::Document,
                format!("{label}: el nodo anterior falló; se enviaría a este otro"),
                doc_digest,
            );
            item.failover = true;
            item.retention = retention;
            let resolution = self.ask(vec![item]).await?;
            match resolution.grant(&format!("ocr-{}", i + 1)) {
                Some(next_grant) => grant = next_grant.clone(),
                None => {
                    return Err(RuntimeError::new(
                        "OCR_FAILED",
                        format!(
                            "No se pudo procesar el archivo adjunto: {} (no autorizaste probar con otro nodo)",
                            self.last_ocr_error.clone().unwrap_or_default()
                        ),
                    ))
                }
            }
        }
        Err(RuntimeError::new(
            "OCR_FAILED",
            format!(
                "No se pudo procesar el archivo adjunto: {}",
                self.last_ocr_error.clone().unwrap_or_default()
            ),
        ))
    }

    /// Indexa el texto (derivado del OCR, ya existente) en el RAG de red tras
    /// una autorización de seguimiento.
    pub async fn index_document(
        &mut self,
        text: &str,
        source: &str,
        document_id: Option<&str>,
        preferences: &Preferences,
    ) -> bool {
        self.settle_tools("document.index", preferences).await;
        let Some(tool) =
            providers::tools_for(&self.node.peers, &["document.index"], preferences.scope)
                .into_iter()
                .find(|t| !preferences.vetoed.contains(&t.provider_id))
        else {
            return false;
        };
        let args = json!({"text": text, "conversationId": self.conversation_id, "documentId": document_id.unwrap_or_default(), "source": source});
        let Ok(arguments) = dynamic::from_json(&args) else {
            return false;
        };
        let Ok(arguments_digest) = args_digest(digest::DOMAIN_DERIVED_TEXT, &arguments) else {
            return false;
        };
        let item = self.tool_item(
            "rag-index",
            &tool,
            DataClass::DerivedText,
            format!(
                "texto del documento ({} caracteres) para indexarlo en el RAG de red",
                text.chars().count()
            ),
            arguments_digest,
        );
        let Ok(resolution) = self.ask(vec![item]).await else {
            return false;
        };
        let Some(grant) = resolution.grant("rag-index").cloned() else {
            return false;
        };
        self.dispatch_tool(
            &grant,
            &tool,
            Outbound::Args {
                domain: digest::DOMAIN_DERIVED_TEXT,
                value: &arguments,
            },
            preferences,
            true,
            &[],
        )
        .await
        .is_ok()
    }

    /// Recupera fragmentos del RAG de red (la pregunta sale con autorización).
    async fn query_rag(
        &mut self,
        query: &str,
        preferences: &Preferences,
        top_k: usize,
        document_id: Option<&str>,
    ) -> Option<Vec<String>> {
        self.settle_tools("document.query", preferences).await;
        let tool = providers::tools_for(&self.node.peers, &["document.query"], preferences.scope)
            .into_iter()
            .find(|t| !preferences.vetoed.contains(&t.provider_id))?;
        let args = json!({"query": query, "conversationId": self.conversation_id, "documentId": document_id.unwrap_or_default(), "top_k": top_k});
        let arguments = dynamic::from_json(&args).ok()?;
        let item = self.tool_item(
            "rag-query",
            &tool,
            DataClass::Query,
            format!(
                "tu pregunta ({} caracteres) para buscar en el documento indexado",
                query.chars().count()
            ),
            args_digest(digest::DOMAIN_QUERY, &arguments).ok()?,
        );
        let resolution = self.ask(vec![item]).await.ok()?;
        let grant = resolution.grant("rag-query")?.clone();
        let result = self
            .dispatch_tool(
                &grant,
                &tool,
                Outbound::Args {
                    domain: digest::DOMAIN_QUERY,
                    value: &arguments,
                },
                preferences,
                true,
                &[],
            )
            .await
            .ok()?;
        let chunks: Vec<String> = kb::chunks_from(&result)
            .iter()
            .map(|c| c["text"].as_str().unwrap_or_default().to_string())
            .filter(|t| !t.trim().is_empty())
            .collect();
        (!chunks.is_empty()).then_some(chunks)
    }

    // ── KB ──────────────────────────────────────────────────────────────────

    /// KB candidatas: la fijada a mano o las recomendadas (por cobertura o por
    /// el LLM). Devuelve los bloques de fragmentos de las autorizadas y si se
    /// llegó a pedir autorización.
    async fn collect_kb_blocks(
        &mut self,
        question: &str,
        preferences: &Preferences,
    ) -> Result<(Vec<DerivedBlock>, bool), RuntimeError> {
        let candidates: Vec<KbCandidate> = if !preferences.kb.is_empty() {
            providers::kb_providers(&self.node.peers, preferences.scope)
                .iter()
                .filter(|k| k.provider_id == preferences.kb)
                .map(candidate)
                .collect()
        } else {
            self.resolve_kb_candidates(question, preferences).await.0
        };
        let mut found: Vec<(String, LoadedTool, String)> = Vec::new();
        let args = json!({"query": question, "topK": 3, "top_k": 3});
        let arguments = dynamic::from_json(&args)
            .map_err(|e| RuntimeError::new("INVALID_ARGUMENTS", e.to_string()))?;
        let arguments_digest = args_digest(digest::DOMAIN_QUERY, &arguments)?;
        let mut items = Vec::new();
        for (i, candidate) in candidates.iter().enumerate() {
            let Some(peer) = self.node.peers.get(&candidate.provider_id) else {
                continue;
            };
            let Some(tool) = providers::advertised_tools(&peer)
                .into_iter()
                .find(|t| kb::is_kb_capability(&t.capability))
            else {
                continue;
            };
            let id = format!("kb-{i}");
            items.push(self.tool_item(
                &id,
                &tool,
                DataClass::Query,
                format!(
                    "tu pregunta ({} caracteres) para consultar «{}»",
                    question.chars().count(),
                    candidate.provider_name
                ),
                arguments_digest,
            ));
            found.push((id, tool, candidate.provider_name.clone()));
        }
        if items.is_empty() {
            return Ok((vec![], false));
        }
        let resolution = self.ask(items).await?;
        let mut blocks = Vec::new();
        for (id, tool, name) in found {
            let Some(grant) = resolution.grant(&id) else {
                continue;
            };
            let Ok(result) = self
                .dispatch_tool(
                    grant,
                    &tool,
                    Outbound::Args {
                        domain: digest::DOMAIN_QUERY,
                        value: &arguments,
                    },
                    preferences,
                    true,
                    &[],
                )
                .await
            else {
                continue;
            };
            let chunks: Vec<String> = kb::chunks_from(&result)
                .iter()
                .map(|c| {
                    let body = c["text"].as_str().unwrap_or_default();
                    match c.pointer("/citation/documentTitle").and_then(Value::as_str) {
                        Some(title) => format!("[Fuente: {title}]\n{body}"),
                        None => body.to_string(),
                    }
                })
                .filter(|c| !c.trim().is_empty())
                .collect();
            if !chunks.is_empty() {
                blocks.push(DerivedBlock::new(
                    format!(
                        "[Fragmentos de la base de conocimiento «{name}» elegida para esta pregunta]"
                    ),
                    chunks,
                ));
            }
        }
        Ok((blocks, true))
    }

    /// KBs a recomendar (SPEC-KB-0002): top-N por cobertura; si ninguna
    /// supera el umbral, el LLM elige una vez entre todas (con el mensaje del
    /// usuario como consentimiento implícito al Star elegido).
    pub async fn resolve_kb_candidates(
        &self,
        question: &str,
        preferences: &Preferences,
    ) -> (Vec<KbCandidate>, bool) {
        let scope = preferences.scope;
        self.node
            .peers
            .settle(|peers| !providers::kb_providers(peers, scope).is_empty())
            .await;
        let kbs = providers::kb_providers(&self.node.peers, preferences.scope);
        let mut scored: Vec<(f64, &providers::KbProvider)> = kbs
            .iter()
            .map(|k| {
                (
                    kb::match_score(question, &kb::match_text(&k.description, &k.tags)),
                    k,
                )
            })
            .filter(|(score, _)| *score >= kb::KB_MATCH_THRESHOLD)
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0));
        let top: Vec<KbCandidate> = scored
            .into_iter()
            .take(preferences.kb_max_per_question.max(1))
            .map(|(_, k)| candidate(k))
            .collect();
        if !top.is_empty() || kbs.is_empty() {
            return (top, false);
        }
        match self.choose_kb_with_llm(question, &kbs, preferences).await {
            Some(chosen) => (vec![candidate(chosen)], true),
            None => (vec![], false),
        }
    }

    async fn choose_kb_with_llm<'k>(
        &self,
        question: &str,
        kbs: &'k [providers::KbProvider],
        preferences: &Preferences,
    ) -> Option<&'k providers::KbProvider> {
        let llm = self.resolve_llm_quiet(preferences)?;
        let grant = self.implicit_grant(&llm, preferences, question).ok()?;
        let list = kbs
            .iter()
            .map(|k| format!("- id: \"{}\" — {}", k.provider_id, k.description))
            .collect::<Vec<_>>()
            .join("\n");
        let system = format!(
            "Tienes disponibles las siguientes bases de conocimiento:\n{list}\n\nNinguna coincidió claramente con la pregunta según un análisis automático previo. Responde ÚNICAMENTE con un JSON de la forma {{\"kbId\": \"<id>\"}} si alguna aplica, o {{\"kbId\": null}} si ninguna aplica. No agregues texto adicional, ni explicación, ni markdown."
        );
        let outcome = self
            .llm_call(
                LlmRequest {
                    star_did: &llm.provider_id,
                    model: &llm.model,
                    timeout: preferences.max_wait,
                    temperature: TEMPERATURE,
                    system: &system,
                    user_text: question,
                    user_grant: &grant,
                    notes: &[],
                    blocks: vec![],
                    history: vec![],
                    tool_outputs: vec![],
                    tool_notes: vec![],
                    tools: &[],
                },
                false,
            )
            .await
            .ok()?;
        let trimmed = outcome
            .text
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        let id = serde_json::from_str::<Value>(trimmed)
            .ok()?
            .get("kbId")?
            .as_str()?
            .to_string();
        kbs.iter().find(|k| k.provider_id == id)
    }

    fn resolve_llm_quiet(&self, preferences: &Preferences) -> Option<ResolvedLlm> {
        let star = providers::stars(&self.node.peers, preferences.scope)
            .into_iter()
            .find(|s| !preferences.vetoed.contains(&s.did))?;
        Some(ResolvedLlm {
            provider_id: star.did.clone(),
            provider_name: star.name(),
            model: "auto".into(),
        })
    }

    // ── Comandos autodescubiertos (SPEC-CMD-0001) ───────────────────────────

    /// Respuesta local del Navigator (sin red ni LLM): `/ayuda`, errores de
    /// uso, comandos desconocidos. La procedencia es la real: sin herramientas.
    pub fn respond_locally(&mut self, text: String) -> String {
        self.used_tools.clear();
        self.emit(AgentEvent::AssistantDelta { text: text.clone() });
        self.emit(AgentEvent::AssistantCompleted {
            provenance: Provenance {
                llm_provider_id: String::new(),
                llm_provider_name: String::new(),
                model: String::new(),
                tools: Vec::new(),
                data_exported: false,
                jurisdiction: "red local comunitaria".into(),
            },
        });
        text
    }

    /// `/nombre args`: resuelve en la tabla de comandos vigentes, valida con el
    /// descriptor, pide la autorización por uso y ejecuta con el ciclo completo
    /// restringido al nodo autorizado. No interviene ningún LLM.
    pub async fn run_command(
        &mut self,
        engine: &CommandEngine,
        name: &str,
        rest: &str,
        preferences: &Preferences,
    ) -> Result<String, RuntimeError> {
        if engine_cmd::is_help(name) {
            let text = engine_cmd::help_text(&engine.table(&self.node.peers));
            return Ok(self.respond_locally(text));
        }
        // Durante el arranque un anuncio puede tardar un ciclo en llegar.
        self.node
            .peers
            .settle(|peers| {
                matches!(
                    engine.table(peers).resolve(name),
                    cmd::Resolution::Active(_)
                )
            })
            .await;
        let table = engine.table(&self.node.peers);
        let command = match table.resolve(name) {
            cmd::Resolution::Active(command) => command.clone(),
            cmd::Resolution::Conflict(nodes) => {
                return Ok(self.respond_locally(engine_cmd::conflict_text(name, nodes)));
            }
            cmd::Resolution::Unknown => {
                return Ok(self.respond_locally(engine_cmd::unknown_text(name)));
            }
        };
        let descriptor = &command.descriptor;
        let args = match cmd::parse_args(descriptor, rest) {
            Ok(args) => args,
            Err(error) => {
                let text = format!("{}. Uso: {}", error.message(), cmd::usage(descriptor));
                return Ok(self.respond_locally(text));
            }
        };
        let Some(entry) = engine_cmd::pick_node(&self.node, &command) else {
            return Ok(self.respond_locally(engine_cmd::no_node_text(name)));
        };
        let node_did = entry.did.clone();
        let node_name = engine_cmd::display_name(&entry);
        let value = cmd::args_value(&args);
        let tool_name = descriptor.tool_name.clone();
        let capability = descriptor.capability_id.clone();
        let command_digest = cmd::command_args_digest(&tool_name, &value)
            .map_err(|e| RuntimeError::new("INVALID_ARGUMENTS", e.to_string()))?;
        let contract = crate::authorization::Contract {
            fingerprint: command.fingerprint.clone(),
            tool: tool_name.clone(),
            registry_digest: engine.registry.digest.clone(),
        };
        let chars: usize = args
            .iter()
            .map(|(_, v)| match v {
                cmd::ArgValue::String(s) | cmd::ArgValue::Enum(s) | cmd::ArgValue::Number(s) => {
                    s.chars().count()
                }
                cmd::ArgValue::Integer(i) => i.to_string().len(),
                cmd::ArgValue::Boolean(b) => b.to_string().len(),
            })
            .sum();
        let plural = if args.len() == 1 {
            "argumento"
        } else {
            "argumentos"
        };
        let item_id = "cmd-0";
        let mut item = ItemSpec::new(
            item_id,
            capability.clone(),
            node_did.clone(),
            node_name.clone(),
            DataClass::CommandArgs,
            format!(
                "comando /{name} · {} {plural} · {chars} caracteres",
                args.len()
            ),
            command_digest,
        );
        item.destination = self.destination_of(&node_did);
        item.contract = Some(contract.clone());
        self.used_tools.clear();
        let resolution = self.ask(vec![item]).await?;

        let line = match resolution.grant(item_id).cloned() {
            None => match Self::denial(&resolution, item_id).code {
                "AUTHORIZATION_EXPIRED" => format!(
                    "No se ejecutó /{name}: la autorización venció. No se envió nada al nodo."
                ),
                _ => format!(
                    "No se ejecutó /{name}: no autorizaste el envío. No se envió nada al nodo."
                ),
            },
            Some(grant) => {
                self.emit(AgentEvent::ToolSelected {
                    capability: capability.clone(),
                    provider_id: node_did.clone(),
                });
                let tool = LoadedTool {
                    name: tool_name.clone(),
                    capability: capability.clone(),
                    provider_id: node_did.clone(),
                    provider_name: node_name.clone(),
                    description: String::new(),
                };
                let mut limited = preferences.clone();
                limited.max_wait = preferences.max_wait.min(Duration::from_secs(15));
                let shown = rest.trim();
                match self
                    .dispatch_tool_with(
                        &grant,
                        &tool,
                        Outbound::Command {
                            tool: &tool_name,
                            args: &value,
                        },
                        &limited,
                        true,
                        &[],
                        Some(&contract),
                    )
                    .await
                {
                    Ok(result) => {
                        let outcome = dynamic::from_json(&result)
                            .map_err(|e| e.to_string())
                            .and_then(|v| cmd::validate_result(descriptor, &v));
                        match outcome {
                            Ok(text) => {
                                format!("Resultado de /{name}: {shown} → {text} (por {node_name})")
                            }
                            Err(text) => format!("No se pudo ejecutar /{name}: {text}"),
                        }
                    }
                    Err(error) => {
                        let text = engine.error_text(&capability, &error);
                        format!("No se pudo ejecutar /{name}: {text}")
                    }
                }
            }
        };
        self.emit(AgentEvent::AssistantDelta { text: line.clone() });
        self.emit(AgentEvent::AssistantCompleted {
            provenance: Provenance {
                llm_provider_id: String::new(),
                llm_provider_name: String::new(),
                model: String::new(),
                tools: self.provenance_tools(),
                data_exported: !self.used_tools.is_empty(),
                jurisdiction: "red local comunitaria".into(),
            },
        });
        Ok(line)
    }
}

fn candidate(k: &providers::KbProvider) -> KbCandidate {
    KbCandidate {
        provider_id: k.provider_id.clone(),
        provider_name: k.provider_name.clone(),
        description: k.description.clone(),
    }
}

/// Capabilities que la pregunta sugiere (`classifyIntent` del TS).
pub fn classify_intent(message: &str) -> Vec<&'static str> {
    let text = message.to_lowercase();
    let mut capabilities = Vec::new();
    if ["ocr", "texto", "imagen", "foto"]
        .iter()
        .any(|k| text.contains(k))
    {
        capabilities.push("document.ocr");
    }
    if text.contains("resumen") || text.contains("resume") {
        capabilities.push("text.summarize");
    }
    capabilities
}

fn tool_definition(tool: &LoadedTool) -> ToolDefinition {
    ToolDefinition {
        name: tool.name.clone(),
        description: tool.description.clone(),
        input_schema: Some(ToolInputSchema {
            r#type: "object".into(),
            ..Default::default()
        }),
    }
}

pub fn artifact_filename(artifact: &ArtifactRef) -> String {
    match &artifact.transport {
        Some(artifact_ref::Transport::Inline(inline)) => inline.filename.clone(),
        Some(artifact_ref::Transport::Ipfs(ipfs)) => ipfs.filename.clone(),
        None => String::new(),
    }
}

/// Texto de un resultado de tool (`extractText` del TS): cadenas tal cual,
/// `{result}` y `{content:[{text}]}` se desenvuelven, lo demás como JSON.
pub fn extract_text(value: &Value) -> String {
    match value {
        Value::String(s) => match serde_json::from_str::<Value>(s) {
            Ok(inner @ Value::Object(_)) if inner.get("result").is_some() => {
                extract_text(&inner["result"])
            }
            _ => s.clone(),
        },
        Value::Object(map) => {
            if let Some(result) = map.get("result") {
                return extract_text(result);
            }
            if let Some(Value::Array(content)) = map.get("content") {
                return content
                    .iter()
                    .filter_map(|c| c.get("text").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("\n");
            }
            value.to_string()
        }
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_like_the_ts() {
        assert_eq!(
            classify_intent("Extrae el texto de esta foto"),
            ["document.ocr"]
        );
        assert_eq!(classify_intent("hazme un resumen"), ["text.summarize"]);
        assert!(classify_intent("hola").is_empty());
    }

    #[test]
    fn extract_text_unwraps_known_shapes() {
        assert_eq!(extract_text(&json!("texto OCR")), "texto OCR");
        assert_eq!(extract_text(&json!({"result": "x"})), "x");
        assert_eq!(
            extract_text(&json!({"content": [{"text": "a"}, {"text": "b"}]})),
            "a\nb"
        );
        assert_eq!(extract_text(&json!([{"text": "k"}])), "[{\"text\":\"k\"}]");
    }

    #[test]
    fn llm_tools_are_a_closed_registry() {
        // Una herramienta fuera del registro nunca se ofrece ni se ejecuta.
        for denied in [
            "document.ocr",
            "document.index",
            "ipfs.upload",
            "text.summarize",
        ] {
            assert!(!LLM_TOOL_CAPABILITIES.contains(&denied), "{denied}");
        }
    }

    #[test]
    fn size_labels_are_readable() {
        assert_eq!(size_label(10), "10 bytes");
        assert_eq!(size_label(2048), "2 KB");
        assert_eq!(size_label(3 * 1024 * 1024), "3.0 MB");
    }
}
