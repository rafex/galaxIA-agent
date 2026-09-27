//! Un turno de chat: el equivalente de `AgentRuntime.run` del TS.
//!
//! La orquestación es determinista (no la decide el LLM): OCR si hay
//! adjunto, contexto de documento (RAG local o de red), KB confirmada por el
//! usuario, prompt y una sola ronda de tools. El LLM se usa a través de Rig
//! (`llm::StarModel`), con streaming cuando la llamada no ofrece tools.
//!
//! Diferencias deliberadas con el TS (defectos que no se copian):
//! - el OCR con failover apunta al siguiente provider (no vuelve a subastar);
//! - los resultados del RAG por red se leen como arreglo (en el TS nunca
//!   aportaban fragmentos, mismo defecto que E2E-029);
//! - la procedencia lleva nombres y `data_exported` real.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::llm::{self, StarModel};
use crate::p2p::{client, dynamic, node::NodeHandle};
use crate::protocol::fhs::{
    self, artifact_ref, dynamic_value::Kind, ArtifactRef, DocumentContext, DynamicObject,
    DynamicValue, Message, ToolDefinition, ToolInputSchema,
};
use crate::runtime::events::{AgentEvent, EventSink, KbCandidate, Provenance, ToolProvenance};
use crate::runtime::kb;
use crate::runtime::providers::{self, LoadedTool, Scope};

pub const SYSTEM_PROMPT: &str = "Eres un asistente útil de una red soberana de IA comunitaria. \
Responde siempre en español. \
Si recibes fragmentos de una base de conocimiento o de documentos, responde con base en los que se \
relacionan con la pregunta e ignora los que no tengan relación; si ninguno la responde, dilo. \
Si necesitas usar una herramienta, hazlo UNA SOLA VEZ y luego responde con la información obtenida. \
No repitas llamadas a herramientas.";

const QUESTION_MARKER: &str = "\n\n[Pregunta del usuario]\n";
const TEMPERATURE: f64 = 0.7;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RagSource {
    Local,
    Network,
}

#[derive(Clone, Debug)]
pub struct Preferences {
    /// Modelo pedido; vacío o `auto` = el que ofrezca el Star.
    pub model: String,
    pub scope: Option<Scope>,
    /// KB elegida a mano (modo manual). Vacío = modo recomendado.
    pub kb: String,
    pub kb_max_per_question: usize,
    pub rag_source: RagSource,
    pub max_wait: Duration,
    /// DIDs vetados (`FHS_VETOED_PROVIDERS`).
    pub vetoed: Arc<HashSet<String>>,
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
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Turn {
    pub message: String,
    pub artifacts: Vec<ArtifactRef>,
    pub document_context: Option<DocumentContext>,
    pub document_id: Option<String>,
    /// KBs confirmadas por el usuario para esta pregunta.
    pub kb_provider_ids: Vec<String>,
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

pub struct AgentRuntime<'a> {
    node: NodeHandle,
    events: &'a dyn EventSink,
    conversation_id: String,
    used_tools: Vec<UsedTool>,
    last_ocr_error: Option<String>,
}

struct ResolvedLlm {
    provider_id: String,
    provider_name: String,
    model: String,
}

impl<'a> AgentRuntime<'a> {
    pub fn new(
        node: NodeHandle,
        events: &'a dyn EventSink,
        conversation_id: impl Into<String>,
    ) -> Self {
        Self {
            node,
            events,
            conversation_id: conversation_id.into(),
            used_tools: Vec::new(),
            last_ocr_error: None,
        }
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

    /// Ejecuta un turno completo y devuelve el texto de la respuesta.
    pub async fn run(
        &mut self,
        turn: Turn,
        preferences: &Preferences,
    ) -> Result<String, RuntimeError> {
        self.used_tools.clear();
        self.status("classifying", "Analizando la petición");
        let mut capabilities = classify_intent(&turn.message);
        if !turn.artifacts.is_empty() && !capabilities.contains(&"document.ocr") {
            capabilities.push("document.ocr");
        }

        self.status("resolving-model", "Eligiendo modelo");
        self.settle_stars(preferences).await;
        let llm = self.resolve_llm(preferences)?;

        self.status("resolving-tools", "Buscando herramientas");
        for capability in &capabilities {
            self.settle_tools(capability, preferences).await;
        }
        let mut tools = providers::tools_for(&self.node.peers, &capabilities, preferences.scope);

        let mut user_content = turn.message.clone();
        let chunks = turn
            .document_context
            .as_ref()
            .map(|c| c.chunks.as_slice())
            .unwrap_or_default();
        if !chunks.is_empty() {
            let doc = turn.document_context.as_ref().expect("document context");
            let block = chunks
                .iter()
                .filter(|c| !c.text.trim().is_empty())
                .map(|c| {
                    let name = if c.filename.is_empty() {
                        doc.filename.as_str()
                    } else {
                        c.filename.as_str()
                    };
                    format!("[{name} · fragmento {}]\n{}", c.chunk_index + 1, c.text)
                })
                .collect::<Vec<_>>()
                .join("\n---\n");
            if !block.is_empty() {
                user_content = format!("[Fragmentos relevantes del documento recuperados por RAG local]\n{block}{QUESTION_MARKER}{}", turn.message);
            }
            // Sin adjunto real en este turno: el OCR no debe ofrecerse.
            tools.retain(|t| t.capability != "document.ocr");
        } else if !turn.artifacts.is_empty() {
            let ocr: Vec<LoadedTool> = tools
                .iter()
                .filter(|t| t.capability == "document.ocr")
                .cloned()
                .collect();
            tools.retain(|t| t.capability != "document.ocr");
            if !ocr.is_empty() {
                let text = self
                    .run_ocr(&ocr, &turn.artifacts[0], preferences, true)
                    .await;
                // El OCR nunca entra completo al prompt: queda para RAG.
                user_content = match text {
                    Some(_) => format!("{}\n\n(El documento fue procesado por OCR y queda disponible para recuperación RAG.)", turn.message),
                    None => format!("{}\n\n(No se pudo extraer texto del archivo adjunto.)", turn.message),
                };
            }
        }

        if turn.rag_active && preferences.rag_source == RagSource::Network {
            if let Some(context) = self
                .query_rag(
                    &turn.message,
                    preferences,
                    3,
                    turn.document_id.as_deref(),
                    &[],
                )
                .await
            {
                user_content = format!("[Fragmentos relevantes del documento indexado]\n{context}{QUESTION_MARKER}{user_content}");
            }
        }

        if !turn.kb_provider_ids.is_empty() {
            if let Some(context) = self
                .query_kbs(&turn.kb_provider_ids, &turn.message, preferences)
                .await
            {
                let block = format!(
                    "[Fragmentos de la base de conocimiento elegida para esta pregunta]\n{context}"
                );
                user_content = match user_content.find(QUESTION_MARKER) {
                    Some(i) => format!("{}\n\n{block}{}", &user_content[..i], &user_content[i..]),
                    None => format!("{block}{QUESTION_MARKER}{user_content}"),
                };
            }
        }

        let mut messages = vec![
            Message {
                role: "system".into(),
                content: SYSTEM_PROMPT.into(),
                ..Default::default()
            },
            Message {
                role: "user".into(),
                content: user_content,
                ..Default::default()
            },
        ];
        let tool_defs: Vec<ToolDefinition> = tools.iter().map(tool_definition).collect();
        let model = StarModel::new(
            self.node.clone(),
            llm.model.clone(),
            Some(llm.provider_id.clone()),
            preferences.max_wait,
        );

        let (text, calls) = self.call_llm(&model, &messages, &tool_defs, true).await?;
        let mut answer = text;
        if !calls.is_empty() {
            messages.push(Message {
                role: "assistant".into(),
                content: answer.clone(),
                tool_call_id: String::new(),
                tool_calls: calls
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
            });
            let mut executed = HashSet::new();
            for (id, name, args) in &calls {
                if !executed.insert(format!("{name}:{args}")) {
                    continue;
                }
                let content = self
                    .execute_tool_call(name, args, &tools, &turn.artifacts, preferences)
                    .await;
                messages.push(Message {
                    role: "tool".into(),
                    content,
                    tool_call_id: id.clone(),
                    tool_calls: vec![],
                });
            }
            answer = self.call_llm(&model, &messages, &[], true).await?.0;
        }

        let executed_by = model
            .executed_by()
            .unwrap_or_else(|| llm.provider_id.clone());
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
                tools: self
                    .used_tools
                    .iter()
                    .map(|t| ToolProvenance {
                        capability: t.capability.clone(),
                        provider_id: t.provider_id.clone(),
                        provider_name: t.provider_name.clone(),
                    })
                    .collect(),
                data_exported: !self.used_tools.is_empty(),
                jurisdiction: "red local comunitaria".into(),
            },
        });
        Ok(answer)
    }

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

    /// Llamada al LLM por Rig. Transmite en vivo solo si no hay tools (si el
    /// modelo terminara pidiendo una, el texto a medias ya estaría en el chat).
    async fn call_llm(
        &self,
        model: &StarModel,
        messages: &[Message],
        tools: &[ToolDefinition],
        emit_answer: bool,
    ) -> Result<(String, Vec<(String, String, Value)>), RuntimeError> {
        let stream = emit_answer && tools.is_empty();
        let mut streamed = false;
        let request = llm::request(messages, tools, TEMPERATURE);
        let response = model
            .complete_streaming(request, |delta| {
                if stream {
                    streamed = true;
                    self.emit(AgentEvent::AssistantDelta {
                        text: delta.to_string(),
                    });
                }
            })
            .await
            .map_err(|e| RuntimeError::new("LLM_ERROR", e.to_string()))?;
        let text = llm::text_of(&response);
        let calls = llm::tool_calls_of(&response);
        if emit_answer && !streamed && calls.is_empty() && !text.is_empty() {
            self.emit(AgentEvent::AssistantDelta { text: text.clone() });
        }
        Ok((text, calls))
    }

    async fn call_tool(
        &mut self,
        tool: &LoadedTool,
        arguments: DynamicValue,
        preferences: &Preferences,
        silent: bool,
    ) -> Result<Value, String> {
        let started = Instant::now();
        if !silent {
            self.emit(AgentEvent::ToolRunning {
                name: tool.name.clone(),
                provider_id: tool.provider_id.clone(),
            });
        }
        let outcome = client::call_tool(
            &self.node,
            client::ToolRequest {
                capability: tool.capability.clone(),
                tool_name: tool.name.clone(),
                arguments,
                preferred_provider: Some(tool.provider_id.clone()),
                timeout: preferences.max_wait,
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

    async fn execute_tool_call(
        &mut self,
        name: &str,
        args: &Value,
        tools: &[LoadedTool],
        artifacts: &[ArtifactRef],
        preferences: &Preferences,
    ) -> String {
        let Some(tool) = tools.iter().find(|t| t.name == name).cloned() else {
            self.emit(AgentEvent::ToolError {
                name: name.into(),
                error: format!("Herramienta desconocida: {name}"),
            });
            return json!({"error": format!("Herramienta desconocida: {name}")}).to_string();
        };
        if preferences.vetoed.contains(&tool.provider_id) {
            let error = format!("Provider vetado: {}", tool.provider_id);
            self.emit(AgentEvent::ToolError {
                name: name.into(),
                error: error.clone(),
            });
            return json!({"error": error}).to_string();
        }
        let mut arguments = match dynamic::from_json(args) {
            Ok(value) => value,
            Err(error) => return json!({"error": error.to_string()}).to_string(),
        };
        if tool.capability == "document.ocr" {
            if let Some(artifact) = artifacts.first() {
                insert_field(&mut arguments, "file", artifact_value(artifact));
            }
        }
        match self.call_tool(&tool, arguments, preferences, false).await {
            Ok(result) => extract_text(&result),
            Err(error) => json!({"error": error}).to_string(),
        }
    }

    /// OCR directo sobre el adjunto, con failover al siguiente provider.
    async fn run_ocr(
        &mut self,
        tools: &[LoadedTool],
        artifact: &ArtifactRef,
        preferences: &Preferences,
        announce: bool,
    ) -> Option<String> {
        self.last_ocr_error = None;
        for (i, tool) in tools.iter().enumerate() {
            if announce {
                self.emit(AgentEvent::ToolSelected {
                    capability: tool.capability.clone(),
                    provider_id: tool.provider_id.clone(),
                });
            }
            let mut arguments = DynamicValue {
                kind: Some(Kind::ObjectValue(DynamicObject::default())),
            };
            insert_field(&mut arguments, "file", artifact_value(artifact));
            match self
                .call_tool(tool, arguments, preferences, !announce)
                .await
            {
                Ok(result) => {
                    let text = extract_text(&result);
                    if !text.trim().is_empty() {
                        return Some(text);
                    }
                    self.last_ocr_error = Some("el OCR no devolvió texto".into());
                }
                Err(error) => self.last_ocr_error = Some(error),
            }
            if let Some(next) = tools.get(i + 1) {
                self.emit(AgentEvent::ProviderFailover {
                    capability: tool.capability.clone(),
                    from: tool.provider_id.clone(),
                    reason: format!(
                        "{} → {}",
                        self.last_ocr_error.clone().unwrap_or_default(),
                        next.provider_id
                    ),
                });
            }
        }
        None
    }

    /// OCR para la sesión (adjunto recién subido); emite `ocr.extracted`.
    pub async fn extract_ocr_text(
        &mut self,
        artifact: &ArtifactRef,
        preferences: &Preferences,
    ) -> Result<(String, String), RuntimeError> {
        self.settle_tools("document.ocr", preferences).await;
        let tools = providers::tools_for(&self.node.peers, &["document.ocr"], preferences.scope);
        if tools.is_empty() {
            return Err(RuntimeError::new(
                "NO_OCR_PROVIDER",
                "No hay un Satellite de OCR disponible",
            ));
        }
        match self.run_ocr(&tools, artifact, preferences, true).await {
            Some(text) => Ok((artifact_filename(artifact), text)),
            None => Err(RuntimeError::new(
                "OCR_FAILED",
                format!(
                    "No se pudo procesar el archivo adjunto: {}",
                    self.last_ocr_error.clone().unwrap_or_default()
                ),
            )),
        }
    }

    /// Indexa texto en el RAG de red (`document.index`).
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
                .next()
        else {
            return false;
        };
        let args = json!({"text": text, "conversationId": self.conversation_id, "documentId": document_id.unwrap_or_default(), "source": source});
        let Ok(arguments) = dynamic::from_json(&args) else {
            return false;
        };
        self.call_tool(&tool, arguments, preferences, true)
            .await
            .is_ok()
    }

    /// Recupera fragmentos del RAG de red; `labels` nombra fuentes `kb:<id>`.
    async fn query_rag(
        &mut self,
        query: &str,
        preferences: &Preferences,
        top_k: usize,
        document_id: Option<&str>,
        labels: &[(String, String)],
    ) -> Option<String> {
        self.settle_tools("document.query", preferences).await;
        let tool = providers::tools_for(&self.node.peers, &["document.query"], preferences.scope)
            .into_iter()
            .next()?;
        let args = json!({"query": query, "conversationId": self.conversation_id, "documentId": document_id.unwrap_or_default(), "top_k": top_k});
        let result = self
            .call_tool(&tool, dynamic::from_json(&args).ok()?, preferences, true)
            .await
            .ok()?;
        let chunks = kb::chunks_from(&result);
        if chunks.is_empty() {
            return None;
        }
        Some(
            chunks
                .iter()
                .map(|c| {
                    let text = c["text"].as_str().unwrap_or_default();
                    let label = c
                        .get("source")
                        .and_then(Value::as_str)
                        .and_then(|s| s.strip_prefix("kb:"))
                        .and_then(|id| {
                            labels
                                .iter()
                                .find(|(pid, _)| pid == id)
                                .map(|(_, name)| name.clone())
                        });
                    match label {
                        Some(name) => format!("[Fuente: {name}]\n{text}"),
                        None => text.to_string(),
                    }
                })
                .collect::<Vec<_>>()
                .join("\n---\n"),
        )
    }

    /// Consulta las KBs confirmadas y fusiona por el RAG de red; si no hay
    /// RAG para fusionar, usa los fragmentos tal cual (E2E-029).
    async fn query_kbs(
        &mut self,
        kb_ids: &[String],
        query: &str,
        preferences: &Preferences,
    ) -> Option<String> {
        let mut direct = Vec::new();
        let mut labels = Vec::new();
        let mut any_indexed = false;
        for kb_id in kb_ids {
            let Some(peer) = self.node.peers.get(kb_id) else {
                continue;
            };
            let Some(tool) = providers::advertised_tools(&peer)
                .into_iter()
                .find(|t| kb::is_kb_capability(&t.capability))
            else {
                continue;
            };
            labels.push((kb_id.clone(), tool.provider_name.clone()));
            let args = json!({"query": query, "topK": 3, "top_k": 3});
            let Ok(result) = self
                .call_tool(&tool, dynamic::from_json(&args).ok()?, preferences, true)
                .await
            else {
                continue;
            };
            let chunks = kb::chunks_from(&result);
            if chunks.is_empty() {
                continue;
            }
            let text = chunks
                .iter()
                .map(|c| {
                    let body = c["text"].as_str().unwrap_or_default();
                    match c.pointer("/citation/documentTitle").and_then(Value::as_str) {
                        Some(title) => format!("[Fuente: {title}]\n{body}"),
                        None => body.to_string(),
                    }
                })
                .collect::<Vec<_>>()
                .join("\n---\n");
            direct.push(format!("[Fuente: {}]\n{text}", tool.provider_name));
            if self
                .index_document(&text, &format!("kb:{kb_id}"), None, preferences)
                .await
            {
                any_indexed = true;
            }
        }
        if any_indexed {
            let fused = self
                .query_rag(query, preferences, (kb_ids.len() * 2).max(3), None, &labels)
                .await;
            if fused.is_some() {
                return fused;
            }
        }
        (!direct.is_empty()).then(|| direct.join("\n---\n"))
    }

    /// KBs a recomendar (SPEC-KB-0002): top-N por cobertura; si ninguna
    /// supera el umbral, el LLM elige una vez entre todas (sin salir al chat).
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
        let list = kbs
            .iter()
            .map(|k| format!("- id: \"{}\" — {}", k.provider_id, k.description))
            .collect::<Vec<_>>()
            .join("\n");
        let messages = vec![
            Message {
                role: "system".into(),
                content: format!(
                    "Tienes disponibles las siguientes bases de conocimiento:\n{list}\n\nNinguna coincidió claramente con la pregunta según un análisis automático previo. Responde ÚNICAMENTE con un JSON de la forma {{\"kbId\": \"<id>\"}} si alguna aplica, o {{\"kbId\": null}} si ninguna aplica. No agregues texto adicional, ni explicación, ni markdown."
                ),
                ..Default::default()
            },
            Message { role: "user".into(), content: question.into(), ..Default::default() },
        ];
        let model = StarModel::new(
            self.node.clone(),
            llm.model,
            Some(llm.provider_id),
            preferences.max_wait,
        );
        let (text, _) = self.call_llm(&model, &messages, &[], false).await.ok()?;
        let trimmed = text
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
            .next()?;
        Some(ResolvedLlm {
            provider_id: star.did.clone(),
            provider_name: star.name(),
            model: "auto".into(),
        })
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

fn artifact_value(artifact: &ArtifactRef) -> DynamicValue {
    DynamicValue {
        kind: Some(Kind::ArtifactRef(artifact.clone())),
    }
}

pub fn artifact_filename(artifact: &ArtifactRef) -> String {
    match &artifact.transport {
        Some(artifact_ref::Transport::Inline(inline)) => inline.filename.clone(),
        Some(artifact_ref::Transport::Ipfs(ipfs)) => ipfs.filename.clone(),
        None => String::new(),
    }
}

fn insert_field(value: &mut DynamicValue, key: &str, field: DynamicValue) {
    if let Some(Kind::ObjectValue(object)) = &mut value.kind {
        object.fields.insert(key.to_string(), field);
    } else {
        let mut object = DynamicObject::default();
        object.fields.insert(key.to_string(), field);
        value.kind = Some(Kind::ObjectValue(object));
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
    fn insert_field_builds_ocr_arguments() {
        let mut args = DynamicValue {
            kind: Some(Kind::ObjectValue(DynamicObject::default())),
        };
        let artifact = ArtifactRef {
            transport: Some(artifact_ref::Transport::Inline(fhs::InlineArtifact {
                data: vec![1, 2],
                filename: "a.pdf".into(),
            })),
        };
        insert_field(&mut args, "file", artifact_value(&artifact));
        match args.kind {
            Some(Kind::ObjectValue(object)) => assert!(matches!(
                object.fields["file"].kind,
                Some(Kind::ArtifactRef(_))
            )),
            _ => panic!("esperaba objeto"),
        }
        assert_eq!(artifact_filename(&artifact), "a.pdf");
    }
}
