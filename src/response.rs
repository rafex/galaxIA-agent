use crate::{
    fhs::FhsTransport,
    mission::{MissionAssignment, MissionError},
    policy::RequestPlan,
    star::StarCompletionModel,
};
use rig::{completion::Prompt, AgentBuilder};
use std::sync::Arc;

/// Agente de respuesta: único componente que solicita generación a Star.
pub struct ResponseAgent<T> {
    transport: Arc<T>,
}

impl<T> Clone for ResponseAgent<T> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
        }
    }
}

impl<T: FhsTransport + 'static> ResponseAgent<T> {
    pub fn new(transport: Arc<T>) -> Self {
        Self { transport }
    }

    pub async fn answer(
        &self,
        plan: &RequestPlan,
        assignment: MissionAssignment,
    ) -> Result<String, MissionError> {
        let model = assignment
            .provider
            .models
            .first()
            .cloned()
            .unwrap_or_else(|| "auto".into());
        let model_adapter =
            StarCompletionModel::new(self.transport.clone(), assignment, model, plan.clone());
        let prompt = format!(
            "Responde en español.\n{}\n\n{}",
            plan.message,
            plan.prompt_context()
        );
        AgentBuilder::new(model_adapter)
            .preamble("La política de privacidad, el RAG, el provider y el límite de contexto ya fueron decididos por agentes deterministas de Rust.")
            .default_max_turns(plan.max_tool_rounds as usize)
            .build()
            .prompt(prompt)
            .await
            .map_err(|_| MissionError::Provider("Star no pudo completar la petición".into()))
    }
}
