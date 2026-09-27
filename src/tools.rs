use crate::{
    fhs::{FhsTransport, RemoteToolCall},
    mission::MissionAssignment,
};
use rig::tool::{DynamicTool, ToolExecutionError, ToolOutput};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone)]
pub struct RemoteToolFactory<T> {
    transport: Arc<T>,
}

impl<T: FhsTransport + 'static> RemoteToolFactory<T> {
    pub fn new(transport: Arc<T>) -> Self {
        Self { transport }
    }

    pub fn dynamic_tool(
        &self,
        assignment: MissionAssignment,
        name: String,
        description: String,
        parameters: Value,
    ) -> DynamicTool {
        let transport = self.transport.clone();
        DynamicTool::new(
            name.clone(),
            description,
            parameters,
            move |_context, arguments| {
                let transport = transport.clone();
                let assignment = assignment.clone();
                let name = name.clone();
                Box::pin(async move {
                    let call = RemoteToolCall {
                        id: uuid::Uuid::new_v4().to_string(),
                        name,
                        arguments,
                    };
                    let request = crate::protocol::fhs::ToolCallRequestMessage {
                        mission_id: assignment.mission_id.clone(),
                        tool_calls: vec![crate::protocol::fhs::ToolCall {
                            id: call.id,
                            r#type: "function".into(),
                            function: Some(crate::protocol::fhs::ToolCallFunction {
                                name: call.name,
                                arguments: Some(crate::protocol::fhs::DynamicValue {
                                    kind: Some(
                                        crate::protocol::fhs::dynamic_value::Kind::StringValue(
                                            serde_json::to_string(&call.arguments)
                                                .unwrap_or_default(),
                                        ),
                                    ),
                                }),
                            }),
                        }],
                    };
                    transport
                        .tool(&assignment, request)
                        .await
                        .map(ToolOutput::json)
                        .map_err(|error| ToolExecutionError::other(error.to_string()))
                })
            },
        )
    }
}
