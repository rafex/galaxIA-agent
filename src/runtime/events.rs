//! Eventos del runtime hacia la sesión del Portal (`agent/events.ts`).

use serde::Serialize;

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolProvenance {
    pub capability: String,
    pub provider_id: String,
    pub provider_name: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Provenance {
    pub llm_provider_id: String,
    pub llm_provider_name: String,
    pub model: String,
    pub tools: Vec<ToolProvenance>,
    /// Si se enviaron datos a tools federadas.
    pub data_exported: bool,
    pub jurisdiction: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct KbCandidate {
    pub provider_id: String,
    pub provider_name: String,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AgentEvent {
    Status {
        status: String,
        message: String,
    },
    LlmSelected {
        provider_id: String,
        model: String,
    },
    ToolSelected {
        capability: String,
        provider_id: String,
    },
    ToolRunning {
        name: String,
        provider_id: String,
    },
    ToolCompleted {
        name: String,
        duration_ms: u64,
        success: bool,
    },
    ToolError {
        name: String,
        error: String,
    },
    ProviderFailover {
        capability: String,
        from: String,
        reason: String,
    },
    AssistantDelta {
        text: String,
    },
    AssistantCompleted {
        provenance: Provenance,
    },
    OcrExtracted {
        filename: String,
        text: String,
    },
    /// `authorization.requested` hacia el cliente (SPEC-AUTH-0001).
    AuthorizationRequested {
        authorization_id: String,
        conversation_id: String,
        turn_id: String,
        expires_at: i64,
        batch_digest: Vec<u8>,
        items: Vec<crate::protocol::fhs::AuthorizationItem>,
    },
    /// `authorization.resolved`: lote y estado por ítem.
    AuthorizationResolved {
        authorization_id: String,
        outcome: i32,
        items: Vec<crate::protocol::fhs::AuthorizationItemStatus>,
    },
    Error {
        code: String,
        message: String,
    },
}

/// Destino de los eventos de un turno.
pub trait EventSink: Send + Sync {
    fn emit(&self, event: AgentEvent);
}

impl EventSink for tokio::sync::mpsc::UnboundedSender<AgentEvent> {
    fn emit(&self, event: AgentEvent) {
        let _ = self.send(event);
    }
}

/// Recolector en memoria (tests y diagnóstico).
#[derive(Default)]
pub struct Collected(pub std::sync::Mutex<Vec<AgentEvent>>);

impl EventSink for Collected {
    fn emit(&self, event: AgentEvent) {
        self.0.lock().expect("eventos").push(event);
    }
}
