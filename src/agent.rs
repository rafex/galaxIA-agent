use crate::{
    atlas::AtlasClient,
    document::DocumentAgent,
    events::{AgentEvent, EventBus},
    fhs::FhsTransport,
    mission::{MissionManager, MissionOffer},
    policy::{AgentRequest, PolicyAgent, RequestPlan},
    protocol::fhs,
    response::ResponseAgent,
    retrieval::RetrievalAgent,
};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct SovereignAgent<T> {
    atlas: AtlasClient,
    policy: PolicyAgent,
    documents: DocumentAgent,
    retrieval: RetrievalAgent,
    missions: MissionManager,
    response: ResponseAgent<T>,
    events: EventBus,
}

impl<T: FhsTransport + 'static> SovereignAgent<T> {
    pub fn new(atlas: AtlasClient, transport: Arc<T>, events: EventBus) -> Self {
        Self {
            atlas,
            policy: PolicyAgent::default(),
            documents: DocumentAgent::default(),
            retrieval: RetrievalAgent,
            missions: MissionManager::default(),
            response: ResponseAgent::new(transport),
            events,
        }
    }

    pub async fn run(&self, request: AgentRequest) -> Result<String, String> {
        let mut plan = self.policy.build_plan(request).map_err(|e| e.to_string())?;
        self.emit(
            &plan,
            "agent.status",
            json!({"status":"classifying", "agent":"policy"}),
            None,
        );
        self.documents.inspect(&plan).map_err(|e| e.to_string())?;
        self.emit(
            &plan,
            "agent.status",
            json!({"status":"retrieving", "agent":"document"}),
            None,
        );
        let retrieved = self.retrieval.apply(&mut plan);
        self.emit(
            &plan,
            "agent.status",
            json!({
                "status":"context-ready",
                "agent":"retrieval",
                "ragSource": format!("{:?}", retrieved.source).to_lowercase(),
                "chunks": retrieved.chunks.len()
            }),
            None,
        );
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
        let response = self.response.clone();
        let conversation_id = plan.conversation_id.clone();
        let request_id = plan.request_id.clone();
        let result = self.missions.assign_with_failover(&offer, providers, |assignment| {
            let response = response.clone();
            let plan = plan.clone();
            let events = events.clone();
            let conversation_id = conversation_id.clone();
            let request_id = request_id.clone();
            async move {
                events.emit(AgentEvent { event: "llm.selected".into(), conversation_id, request_id, mission_id: Some(assignment.mission_id.clone()), provider_id: Some(assignment.provider.provider_id.clone()), data: json!({"model": assignment.provider.models.first().cloned().unwrap_or_else(|| "auto".into()), "attempt": assignment.attempt}) });
                response.answer(&plan, assignment).await
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

/// Nombre arquitectónico de la fachada Navigator durante la transición.
pub type SupervisorAgent<T> = SovereignAgent<T>;

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
