use crate::{
    fhs::{chat_request, FhsError, FhsTransport, StarResult},
    mission::MissionAssignment,
    policy::RequestPlan,
};
use async_trait::async_trait;
use futures::stream;
use rig::{
    completion::{
        AssistantContent, CompletionError, CompletionModel, CompletionRequest, CompletionResponse,
        Usage,
    },
    message::{Text, ToolCall, ToolFunction},
    streaming::{RawStreamingChoice, StreamFinal, StreamingCompletionResponse, StreamingResult},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct StarCompletionModel<T> {
    transport: Arc<T>,
    assignment: MissionAssignment,
    model: String,
    plan: RequestPlan,
}

impl<T: FhsTransport> StarCompletionModel<T> {
    pub fn new(
        transport: Arc<T>,
        assignment: MissionAssignment,
        model: impl Into<String>,
        plan: RequestPlan,
    ) -> Self {
        Self {
            transport,
            assignment,
            model: model.into(),
            plan,
        }
    }

    async fn execute(&self, request: CompletionRequest) -> Result<StarResult, CompletionError> {
        let content = serde_json::to_string(&request.chat_history).unwrap_or_default();
        let mut plan = self.plan.clone();
        plan.message = content;
        self.transport
            .chat(&self.assignment, chat_request(&plan, &self.model))
            .await
            .map_err(fhs_error)
    }
}

fn fhs_error(error: FhsError) -> CompletionError {
    CompletionError::ProviderError(error.to_string())
}

fn normalized(result: StarResult, model: &str) -> CompletionResponse {
    let mut choice = Vec::new();
    if !result.content.is_empty() {
        choice.push(AssistantContent::Text(Text::new(result.content)));
    }
    for call in result.tool_calls {
        choice.push(AssistantContent::ToolCall(ToolCall::from_wire(
            call.id,
            ToolFunction::new(call.name, call.arguments),
        )));
    }
    CompletionResponse::new(choice, Usage::default(), "fhs.star").with_model(model.to_string())
}

impl<T: FhsTransport> CompletionModel for StarCompletionModel<T> {
    async fn completion(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse, CompletionError> {
        Ok(normalized(self.execute(request).await?, &self.model))
    }

    async fn stream(
        &self,
        request: CompletionRequest,
    ) -> Result<StreamingCompletionResponse, CompletionError> {
        let response = normalized(self.execute(request).await?, &self.model);
        let text = response
            .choice
            .iter()
            .filter_map(|item| match item {
                AssistantContent::Text(text) => Some(text.text.clone()),
                _ => None,
            })
            .collect::<String>();
        let final_response =
            StreamFinal::new("fhs.star", response.usage).with_model(self.model.clone());
        let items: Vec<Result<RawStreamingChoice, CompletionError>> = vec![
            Ok(RawStreamingChoice::Message(text)),
            Ok(RawStreamingChoice::FinalResponse(final_response)),
        ];
        let stream: StreamingResult = Box::pin(stream::iter(items));
        Ok(StreamingCompletionResponse::stream("fhs.star", stream))
    }
}

#[async_trait]
pub trait StarExecutor: Send + Sync {
    async fn complete(
        &self,
        plan: &RequestPlan,
        provider: MissionAssignment,
        model: &str,
    ) -> Result<StarResult, FhsError>;
}
