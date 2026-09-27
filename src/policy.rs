use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PrivacyScope {
    Local,
    Network,
    Community,
    External,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RagSource {
    Local,
    Network,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelPreferences {
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_scope")]
    pub scope: PrivacyScope,
    #[serde(default = "default_rag")]
    pub rag_source: RagSource,
    #[serde(default = "default_timeout_ms")]
    pub max_wait_ms: u64,
    #[serde(default = "default_context_chars")]
    pub max_context_chars: usize,
}

fn default_model() -> String {
    "auto".into()
}
fn default_scope() -> PrivacyScope {
    PrivacyScope::Community
}
fn default_rag() -> RagSource {
    RagSource::Local
}
fn default_timeout_ms() -> u64 {
    30_000
}
fn default_context_chars() -> usize {
    12_000
}

impl Default for ModelPreferences {
    fn default() -> Self {
        Self {
            model: default_model(),
            scope: default_scope(),
            rag_source: default_rag(),
            max_wait_ms: default_timeout_ms(),
            max_context_chars: default_context_chars(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentRequest {
    pub conversation_id: String,
    pub request_id: String,
    pub message: String,
    #[serde(default)]
    pub preferences: ModelPreferences,
    #[serde(default)]
    pub document_id: Option<String>,
    #[serde(default)]
    pub document_context: Vec<DocumentChunk>,
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DocumentChunk {
    pub chunk_id: String,
    pub filename: String,
    pub chunk_index: usize,
    pub text: String,
    #[serde(default)]
    pub score: f32,
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttachmentRef {
    pub filename: String,
    pub size_bytes: usize,
    pub digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestPlan {
    pub conversation_id: String,
    pub request_id: String,
    pub mission_id: String,
    pub message: String,
    pub scope: PrivacyScope,
    pub rag_source: RagSource,
    pub context: Vec<DocumentChunk>,
    pub attachments: Vec<AttachmentRef>,
    pub max_wait_ms: u64,
    pub max_tool_rounds: u8,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PolicyError {
    #[error("conversationId es obligatorio")]
    MissingConversation,
    #[error("requestId es obligatorio")]
    MissingRequest,
    #[error("la petición está vacía")]
    EmptyMessage,
    #[error("el contexto recuperado supera el límite configurado")]
    ContextTooLarge,
    #[error("el texto OCR completo no puede entrar en el plan")]
    FullOcrRejected,
}

impl RequestPlan {
    pub fn build(request: AgentRequest) -> Result<Self, PolicyError> {
        if request.conversation_id.trim().is_empty() {
            return Err(PolicyError::MissingConversation);
        }
        if request.request_id.trim().is_empty() {
            return Err(PolicyError::MissingRequest);
        }
        if request.message.trim().is_empty() {
            return Err(PolicyError::EmptyMessage);
        }
        let context_chars: usize = request
            .document_context
            .iter()
            .map(|chunk| chunk.text.len())
            .sum();
        if context_chars > request.preferences.max_context_chars {
            return Err(PolicyError::ContextTooLarge);
        }
        if request
            .document_context
            .iter()
            .any(|chunk| chunk.text.len() > request.preferences.max_context_chars)
        {
            return Err(PolicyError::FullOcrRejected);
        }
        Ok(Self {
            conversation_id: request.conversation_id,
            request_id: request.request_id,
            mission_id: Uuid::new_v4().to_string(),
            message: request.message,
            scope: request.preferences.scope,
            rag_source: request.preferences.rag_source,
            context: request.document_context,
            attachments: request.attachments,
            max_wait_ms: request.preferences.max_wait_ms,
            max_tool_rounds: 3,
        })
    }

    pub fn prompt_context(&self) -> String {
        self.context
            .iter()
            .map(|chunk| {
                format!(
                    "[{} · fragmento {}]\n{}",
                    chunk.filename,
                    chunk.chunk_index + 1,
                    chunk.text
                )
            })
            .collect::<Vec<_>>()
            .join("\n---\n")
    }

    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).expect("RequestPlan is serializable")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> AgentRequest {
        AgentRequest {
            conversation_id: "c".into(),
            request_id: "r".into(),
            message: "pregunta".into(),
            preferences: ModelPreferences::default(),
            document_id: None,
            document_context: vec![],
            attachments: vec![],
        }
    }

    #[test]
    fn rejects_full_ocr_and_accepts_bounded_chunks() {
        let mut req = request();
        req.preferences.max_context_chars = 20;
        req.document_context = vec![DocumentChunk {
            chunk_id: "1".into(),
            filename: "x.pdf".into(),
            chunk_index: 0,
            text: "fragmento".into(),
            score: 0.9,
            source: None,
        }];
        assert!(RequestPlan::build(req).is_ok());
    }

    #[test]
    fn caps_tool_rounds() {
        let plan = RequestPlan::build(request()).unwrap();
        assert_eq!(plan.max_tool_rounds, 3);
    }
}
