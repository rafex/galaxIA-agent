use crate::{mission::MissionAssignment, policy::RequestPlan, protocol::fhs};
use async_trait::async_trait;
use prost::Message;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FhsError {
    #[error("provider FHS no disponible: {0}")]
    Unavailable(String),
    #[error("error de serialización FHS: {0}")]
    Encoding(#[from] prost::EncodeError),
}

#[derive(Clone, Debug)]
pub struct StarResult {
    pub content: String,
    pub tool_calls: Vec<RemoteToolCall>,
}
#[derive(Clone, Debug)]
pub struct RemoteToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

#[async_trait]
pub trait FhsTransport: Send + Sync {
    async fn chat(
        &self,
        assignment: &MissionAssignment,
        request: fhs::ChatRequestMessage,
    ) -> Result<StarResult, FhsError>;
    async fn tool(
        &self,
        assignment: &MissionAssignment,
        request: fhs::ToolCallRequestMessage,
    ) -> Result<serde_json::Value, FhsError>;
}

#[derive(Clone, Default)]
pub struct UnconfiguredFhsTransport;

#[async_trait]
impl FhsTransport for UnconfiguredFhsTransport {
    async fn chat(
        &self,
        _assignment: &MissionAssignment,
        _request: fhs::ChatRequestMessage,
    ) -> Result<StarResult, FhsError> {
        Err(FhsError::Unavailable(
            "FHS transport aún no configurado".into(),
        ))
    }
    async fn tool(
        &self,
        _assignment: &MissionAssignment,
        _request: fhs::ToolCallRequestMessage,
    ) -> Result<serde_json::Value, FhsError> {
        Err(FhsError::Unavailable(
            "FHS transport aún no configurado".into(),
        ))
    }
}

#[allow(deprecated)]
pub fn chat_request(plan: &RequestPlan, model: &str) -> fhs::ChatRequestMessage {
    let context = if plan.context.is_empty() {
        None
    } else {
        Some(fhs::DocumentContext {
            filename: String::new(),
            text: String::new(),
            document_id: String::new(),
            chunks: plan
                .context
                .iter()
                .map(|chunk| fhs::DocumentChunk {
                    chunk_id: chunk.chunk_id.clone(),
                    filename: chunk.filename.clone(),
                    chunk_index: chunk.chunk_index as i32,
                    text: chunk.text.clone(),
                    score: chunk.score,
                    source: chunk.source.clone().unwrap_or_default(),
                })
                .collect(),
            source: if matches!(plan.rag_source, crate::policy::RagSource::Network) {
                2
            } else {
                1
            },
            embedding_model: String::new(),
            embedding_dimensions: 0,
        })
    };
    fhs::ChatRequestMessage {
        mission_id: plan.mission_id.clone(),
        messages: vec![fhs::Message {
            role: "user".into(),
            content: plan.message.clone(),
            tool_call_id: String::new(),
            tool_calls: vec![],
        }],
        tools: vec![],
        model: model.into(),
        artifacts: vec![],
        document_context: context,
        document_id: plan.document_id.clone().unwrap_or_default(),
    }
}

pub fn encode_envelope(envelope: &fhs::Envelope) -> Result<Vec<u8>, FhsError> {
    let mut bytes = Vec::new();
    envelope.encode(&mut bytes)?;
    Ok(bytes)
}
