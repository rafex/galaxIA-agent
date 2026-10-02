//! Star como `CompletionModel` de Rig sobre la red FHS real.
//!
//! El runtime habla con el LLM a través de Rig: arma un `CompletionRequest`
//! (preámbulo, historial con roles reales, tools) y este modelo lo traduce a
//! un `ChatRequestMessage` FHS, ejecuta la misión `chat` por P2P y devuelve
//! la respuesta como `CompletionResponse`. Reemplaza al adaptador anterior,
//! que aplanaba todo el historial en un JSON dentro de un solo mensaje y
//! mandaba `tools: []`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rig::completion::{
    AssistantContent, CompletionError, CompletionModel, CompletionRequest, CompletionResponse,
    Usage,
};
use rig::message::{
    Message as RigMessage, Text, ToolCall as RigToolCall, ToolFunction, ToolResultContent,
    UserContent,
};
use rig::streaming::{
    RawStreamingChoice, StreamFinal, StreamingCompletionResponse, StreamingResult,
};
use serde_json::{json, Map, Value};

use crate::p2p::{client, dynamic, node::NodeHandle};
use crate::protocol::fhs::{
    self, Message, ToolCall, ToolCallFunction, ToolDefinition, ToolInputSchema,
};

pub const DEFAULT_LLM_TIMEOUT: Duration = Duration::from_secs(310);

#[derive(Clone)]
pub struct StarModel {
    node: NodeHandle,
    model: String,
    /// Star que el runtime eligió (gana si puja).
    preferred_provider: Option<String>,
    timeout: Duration,
    executed_by: Arc<Mutex<Option<String>>>,
}

impl StarModel {
    pub(crate) fn new(
        node: NodeHandle,
        model: impl Into<String>,
        preferred_provider: Option<String>,
        timeout: Duration,
    ) -> Self {
        Self {
            node,
            model: model.into(),
            preferred_provider,
            timeout,
            executed_by: Arc::default(),
        }
    }

    /// DID del Star que ejecutó la última llamada.
    pub fn executed_by(&self) -> Option<String> {
        self.executed_by.lock().expect("executed_by").clone()
    }

    /// Completa el request entregando cada fragmento de texto a `on_delta`.
    pub(crate) async fn complete_streaming(
        &self,
        request: CompletionRequest,
        on_delta: impl FnMut(&str),
    ) -> Result<CompletionResponse, CompletionError> {
        let (messages, tools) = to_fhs(&request);
        let outcome = client::chat(
            &self.node,
            client::ChatRequest {
                messages,
                tools,
                model: request.model.clone().unwrap_or_else(|| self.model.clone()),
                preferred_provider: self.preferred_provider.clone(),
                // El Star fijado es el único que puede recibir el contenido.
                allowed_provider_dids: self.preferred_provider.clone().map(|did| vec![did]),
                timeout: self.timeout,
            },
            on_delta,
        )
        .await
        .map_err(|e| CompletionError::ProviderError(e.to_string()))?;
        *self.executed_by.lock().expect("executed_by") = Some(outcome.provider.clone());
        let mut choice = Vec::new();
        if !outcome.content.is_empty() {
            choice.push(AssistantContent::Text(Text::new(outcome.content)));
        }
        for call in outcome.tool_calls {
            let function = call.function.unwrap_or_default();
            let arguments = function
                .arguments
                .as_ref()
                .map(dynamic::to_json)
                .unwrap_or_else(|| json!({}));
            choice.push(AssistantContent::ToolCall(RigToolCall::from_wire(
                call.id,
                ToolFunction::new(function.name, arguments),
            )));
        }
        Ok(
            CompletionResponse::new(choice, Usage::default(), "fhs.star")
                .with_model(self.model.clone()),
        )
    }
}

impl CompletionModel for StarModel {
    async fn completion(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse, CompletionError> {
        self.complete_streaming(request, |_| {}).await
    }

    async fn stream(
        &self,
        request: CompletionRequest,
    ) -> Result<StreamingCompletionResponse, CompletionError> {
        let (tx, rx) =
            tokio::sync::mpsc::unbounded_channel::<Result<RawStreamingChoice, CompletionError>>();
        let model = self.clone();
        tokio::spawn(async move {
            let deltas = tx.clone();
            let result = model
                .complete_streaming(request, |text| {
                    let _ = deltas.send(Ok(RawStreamingChoice::Message(text.to_string())));
                })
                .await;
            let _ = match result {
                Ok(response) => tx.send(Ok(RawStreamingChoice::FinalResponse(
                    StreamFinal::new("fhs.star", response.usage).with_model(model.model.clone()),
                ))),
                Err(error) => tx.send(Err(error)),
            };
        });
        let stream: StreamingResult = Box::pin(tokio_stream_from(rx));
        Ok(StreamingCompletionResponse::stream("fhs.star", stream))
    }
}

fn tokio_stream_from<T: Send + 'static>(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<T>,
) -> impl futures::Stream<Item = T> + Send {
    futures::stream::poll_fn(move |cx| rx.poll_recv(cx))
}

/// `CompletionRequest` de Rig → mensajes y tools FHS.
pub fn to_fhs(request: &CompletionRequest) -> (Vec<Message>, Vec<ToolDefinition>) {
    let mut messages = Vec::new();
    if let Some(preamble) = request.preamble.as_ref().filter(|p| !p.is_empty()) {
        messages.push(Message {
            role: "system".into(),
            content: preamble.clone(),
            ..Default::default()
        });
    }
    for message in &request.chat_history {
        match message {
            RigMessage::System { content } => {
                messages.push(Message {
                    role: "system".into(),
                    content: content.clone(),
                    ..Default::default()
                });
            }
            RigMessage::User { content } => {
                let mut text = String::new();
                for part in content {
                    match part {
                        UserContent::Text(t) => text.push_str(&t.text),
                        UserContent::ToolResult(result) => {
                            let id = result
                                .provider
                                .as_ref()
                                .map(|p| p.call_id.clone())
                                .unwrap_or_else(|| result.call.as_str().to_string());
                            messages.push(Message {
                                role: "tool".into(),
                                content: tool_result_text(&result.content),
                                tool_call_id: id,
                                tool_calls: vec![],
                            });
                        }
                        _ => {}
                    }
                }
                if !text.is_empty() {
                    messages.push(Message {
                        role: "user".into(),
                        content: text,
                        ..Default::default()
                    });
                }
            }
            RigMessage::Assistant { content, .. } => {
                let mut text = String::new();
                let mut tool_calls = Vec::new();
                for part in content {
                    match part {
                        AssistantContent::Text(t) => text.push_str(&t.text),
                        AssistantContent::ToolCall(call) => tool_calls.push(ToolCall {
                            id: call
                                .provider
                                .as_ref()
                                .map(|p| p.call_id.clone())
                                .unwrap_or_else(|| call.id.as_str().to_string()),
                            r#type: "function".into(),
                            function: Some(ToolCallFunction {
                                name: call.function.name.clone(),
                                arguments: dynamic::from_json(&call.function.arguments).ok(),
                            }),
                        }),
                        _ => {}
                    }
                }
                messages.push(Message {
                    role: "assistant".into(),
                    content: text,
                    tool_call_id: String::new(),
                    tool_calls,
                });
            }
        }
    }
    let tools = request
        .tools
        .iter()
        .map(|tool| ToolDefinition {
            name: tool.name.clone(),
            description: tool.description.clone(),
            input_schema: Some(schema_from_json(&tool.parameters)),
        })
        .collect();
    (messages, tools)
}

fn tool_result_text(content: &[ToolResultContent]) -> String {
    content
        .iter()
        .map(|part| match part {
            ToolResultContent::Text(t) => t.text.clone(),
            ToolResultContent::Json { value, .. } => value.to_string(),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// JSON Schema → `ToolInputSchema` (lo inverso de `schemaFromLocal` del TS).
pub fn schema_from_json(value: &Value) -> ToolInputSchema {
    let object = value.as_object();
    let get = |key: &str| object.and_then(|o| o.get(key));
    ToolInputSchema {
        r#type: get("type")
            .and_then(Value::as_str)
            .unwrap_or("object")
            .to_string(),
        description: get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        properties: get("properties")
            .and_then(Value::as_object)
            .map(|props| {
                props
                    .iter()
                    .map(|(k, v)| (k.clone(), schema_from_json(v)))
                    .collect()
            })
            .unwrap_or_default(),
        required: get("required")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default(),
        enum_values: get("enum")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(String::from)
                            .unwrap_or_else(|| v.to_string())
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// `ToolInputSchema` → JSON Schema.
pub fn schema_to_json(schema: &ToolInputSchema) -> Value {
    let mut out = Map::new();
    out.insert(
        "type".into(),
        json!(if schema.r#type.is_empty() {
            "object"
        } else {
            schema.r#type.as_str()
        }),
    );
    if !schema.description.is_empty() {
        out.insert("description".into(), json!(schema.description));
    }
    if !schema.properties.is_empty() || schema.r#type.is_empty() || schema.r#type == "object" {
        let props: Map<String, Value> = schema
            .properties
            .iter()
            .map(|(k, v)| (k.clone(), schema_to_json(v)))
            .collect();
        out.insert("properties".into(), Value::Object(props));
    }
    if !schema.required.is_empty() {
        out.insert("required".into(), json!(schema.required));
    }
    if !schema.enum_values.is_empty() {
        out.insert("enum".into(), json!(schema.enum_values));
    }
    Value::Object(out)
}

/// Mensajes FHS de un turno → historial de Rig (para armar el request).
pub fn rig_history(messages: &[Message]) -> Vec<RigMessage> {
    let mut history = Vec::new();
    for message in messages {
        match message.role.as_str() {
            "system" => history.push(RigMessage::System {
                content: message.content.clone(),
            }),
            "user" => history.push(RigMessage::user(message.content.clone())),
            "assistant" => {
                let mut content = Vec::new();
                if !message.content.is_empty() {
                    content.push(AssistantContent::Text(Text::new(message.content.clone())));
                }
                for call in &message.tool_calls {
                    let function = call.function.clone().unwrap_or_default();
                    let args = function
                        .arguments
                        .as_ref()
                        .map(dynamic::to_json)
                        .unwrap_or_else(|| json!({}));
                    content.push(AssistantContent::ToolCall(RigToolCall::from_wire(
                        call.id.clone(),
                        ToolFunction::new(function.name, args),
                    )));
                }
                if !content.is_empty() {
                    history.push(RigMessage::Assistant { id: None, content });
                }
            }
            "tool" => {
                // El nombre de la tool sale de la llamada que este resultado responde.
                let name = messages
                    .iter()
                    .flat_map(|m| m.tool_calls.iter())
                    .find(|c| c.id == message.tool_call_id)
                    .and_then(|c| c.function.as_ref().map(|f| f.name.clone()))
                    .unwrap_or_default();
                history.push(RigMessage::User {
                    content: vec![UserContent::tool_result_from_wire(
                        message.tool_call_id.clone(),
                        name,
                        vec![ToolResultContent::text(message.content.clone())],
                    )],
                });
            }
            _ => {}
        }
    }
    history
}

/// Llamadas del modelo en la respuesta de Rig (nombre, argumentos JSON, id).
pub fn tool_calls_of(response: &CompletionResponse) -> Vec<(String, String, Value)> {
    response
        .choice
        .iter()
        .filter_map(|c| match c {
            AssistantContent::ToolCall(call) => Some((
                call.provider
                    .as_ref()
                    .map(|p| p.call_id.clone())
                    .unwrap_or_else(|| call.id.as_str().to_string()),
                call.function.name.clone(),
                call.function.arguments.clone(),
            )),
            _ => None,
        })
        .collect()
}

pub fn text_of(response: &CompletionResponse) -> String {
    response
        .choice
        .iter()
        .filter_map(|c| match c {
            AssistantContent::Text(t) => Some(t.text.clone()),
            _ => None,
        })
        .collect()
}

/// Utilidad para construir un `CompletionRequest` de Rig desde mensajes FHS.
pub fn request(
    messages: &[Message],
    tools: &[fhs::ToolDefinition],
    temperature: f64,
) -> CompletionRequest {
    CompletionRequest {
        model: None,
        preamble: None,
        chat_history: rig_history(messages),
        documents: vec![],
        tools: tools
            .iter()
            .map(|t| rig::completion::ToolDefinition {
                name: t.name.clone(),
                description: t.description.clone(),
                parameters: t
                    .input_schema
                    .as_ref()
                    .map(schema_to_json)
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
            })
            .collect(),
        temperature: Some(temperature),
        max_tokens: None,
        tool_choice: None,
        additional_params: None,
        output_schema: None,
        record_telemetry_content: false,
    }
}

#[allow(dead_code)]
fn _assert_hashmap(_: HashMap<String, ToolInputSchema>) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_roundtrips_through_json() {
        let json = json!({"type": "object", "properties": {"query": {"type": "string", "description": "texto"}}, "required": ["query"]});
        assert_eq!(schema_to_json(&schema_from_json(&json)), json);
    }

    #[test]
    fn rig_request_keeps_roles_tool_calls_and_results() {
        let messages = vec![
            Message {
                role: "system".into(),
                content: "Responde en español.".into(),
                ..Default::default()
            },
            Message {
                role: "user".into(),
                content: "¿Qué dice la KB?".into(),
                ..Default::default()
            },
            Message {
                role: "assistant".into(),
                content: String::new(),
                tool_call_id: String::new(),
                tool_calls: vec![ToolCall {
                    id: "call_1".into(),
                    r#type: "function".into(),
                    function: Some(ToolCallFunction {
                        name: "kb_query".into(),
                        arguments: dynamic::from_json(&json!({"query": "educación"})).ok(),
                    }),
                }],
            },
            Message {
                role: "tool".into(),
                content: "Artículo 3…".into(),
                tool_call_id: "call_1".into(),
                tool_calls: vec![],
            },
        ];
        let tools = vec![ToolDefinition {
            name: "kb_query".into(),
            description: "KB".into(),
            input_schema: None,
        }];
        let (fhs_messages, fhs_tools) = to_fhs(&request(&messages, &tools, 0.7));
        let roles: Vec<&str> = fhs_messages.iter().map(|m| m.role.as_str()).collect();
        assert_eq!(roles, ["system", "user", "assistant", "tool"]);
        assert_eq!(fhs_messages[2].tool_calls[0].id, "call_1");
        assert_eq!(fhs_messages[3].tool_call_id, "call_1");
        assert_eq!(fhs_messages[3].content, "Artículo 3…");
        assert_eq!(fhs_tools[0].name, "kb_query");
    }
}
