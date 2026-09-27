use crate::{
    atlas::AtlasClient,
    events::{AgentEvent, EventBus},
    fhs::FhsTransport,
    mission::{assign_with_failover, MissionError, MissionOffer},
    policy::{AgentRequest, RequestPlan},
    protocol::fhs,
    star::StarCompletionModel,
};
use rig::{completion::Prompt, AgentBuilder};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct SovereignAgent<T> {
    atlas: AtlasClient,
    transport: Arc<T>,
    events: EventBus,
}

impl<T: FhsTransport + 'static> SovereignAgent<T> {
    pub fn new(atlas: AtlasClient, transport: Arc<T>, events: EventBus) -> Self {
        Self {
            atlas,
            transport,
            events,
        }
    }

    pub async fn run(&self, request: AgentRequest) -> Result<String, String> {
        let plan = RequestPlan::build(request).map_err(|e| e.to_string())?;
        self.emit(&plan, "agent.status", json!({"status":"classifying"}), None);
        let providers = self.atlas.providers_for("chat", &plan.scope).await;
        let offer = MissionOffer {
            mission_id: plan.mission_id.clone(),
            mission_type: "chat".into(),
            required_capabilities: vec!["chat".into()],
            preferred_model: None,
            scope: plan.scope.clone(),
            bid_deadline_ms: plan.max_wait_ms,
        };
        let events = self.events.clone();
        let transport = self.transport.clone();
        let conversation_id = plan.conversation_id.clone();
        let request_id = plan.request_id.clone();
        let result = assign_with_failover(&offer, providers, |assignment| {
            let transport = transport.clone();
            let plan = plan.clone();
            let events = events.clone();
            let conversation_id = conversation_id.clone();
            let request_id = request_id.clone();
            async move {
                events.emit(AgentEvent { event: "llm.selected".into(), conversation_id, request_id, mission_id: Some(assignment.mission_id.clone()), provider_id: Some(assignment.provider.provider_id.clone()), data: json!({"model": assignment.provider.models.first().cloned().unwrap_or_else(|| "auto".into()), "attempt": assignment.attempt}) });
                let model = assignment.provider.models.first().cloned().unwrap_or_else(|| "auto".into());
                let model_adapter = StarCompletionModel::new(
                    transport.clone(),
                    assignment.clone(),
                    model,
                    plan.clone(),
                );
                let prompt = format!("Responde en español.\n{}\n\n{}", plan.message, plan.prompt_context());
                let answer = AgentBuilder::new(model_adapter).preamble("La política de privacidad, el RAG, el provider y el límite de contexto ya fueron decididos por el controlador Rust.").default_max_turns(plan.max_tool_rounds as usize).build().prompt(prompt).await.map_err(|_| MissionError::Exhausted)?;
                Ok(answer)
            }
        }).await.map_err(|error| error.to_string())?;
        self.emit(
            &plan,
            "assistant.completed",
            json!({"content": result}),
            None,
        );
        Ok(result)
    }

    fn emit(
        &self,
        plan: &RequestPlan,
        event: &str,
        data: serde_json::Value,
        provider_id: Option<String>,
    ) {
        self.events.emit(AgentEvent {
            event: event.into(),
            conversation_id: plan.conversation_id.clone(),
            request_id: plan.request_id.clone(),
            mission_id: Some(plan.mission_id.clone()),
            provider_id,
            data,
        });
    }
}

pub fn portal_agent_start(session_id: String, scope: String) -> fhs::AgentStartMessage {
    fhs::AgentStartMessage {
        session_id,
        scope,
        model: String::new(),
        kb: String::new(),
        kb_max_per_question: 1,
        ipfs_enabled: false,
        ipfs_network: "private".into(),
        ipfs_retention: "session".into(),
        rag_source: 1,
    }
}
