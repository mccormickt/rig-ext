//! Provider-neutral durable representation of a completed provider stream.

use rig::{
    completion::{AssistantContent, CompletionResponse},
    streaming::PartKind,
};
use serde::{Deserialize, Serialize};

/// One event of a provider stream, in rig's part model.
///
/// `part` is the position the part's content takes in the reply's `choice`.
/// `Final` carries the response the provider's end folded into; it is not a
/// stream event and is always the last item of a transcript.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamItem {
    Start {
        part: u32,
        kind: PartKind,
    },
    Text {
        part: u32,
        text: String,
    },
    Reasoning {
        part: u32,
        text: String,
    },
    Arguments {
        part: u32,
        json: String,
    },
    End {
        part: u32,
        content: AssistantContent,
    },
    Unknown {
        value: serde_json::Value,
    },
    Final {
        response: CompletionResponse,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamTranscript {
    /// Every event the stream produced, in arrival order, ending with the
    /// `Final` item.
    pub items: Vec<StreamItem>,
    /// The response the provider's end folded into.
    pub response: CompletionResponse,
}

/// Apply a completed stream with the request's exact tool authorization.
pub fn apply(
    agent: &mut rig::agent::AgentRun,
    request: &rig::completion::CompletionRequest,
    transcript: StreamTranscript,
) -> Result<(), String> {
    use rig::{agent::run::StreamedTurn, completion::ResponseIdentity};
    use std::collections::BTreeSet;
    let executable: BTreeSet<_> = request.tools.iter().map(|tool| tool.name.clone()).collect();
    let allowed = match request.tool_choice.as_ref() {
        Some(rig::message::ToolChoice::None) => BTreeSet::new(),
        Some(rig::message::ToolChoice::Specific { function_names }) => function_names
            .iter()
            .filter(|name| executable.contains(*name))
            .cloned()
            .collect(),
        Some(rig::message::ToolChoice::Auto | rig::message::ToolChoice::Required) | None => {
            executable.clone()
        }
    };
    for content in &transcript.response.choice {
        if let rig::completion::AssistantContent::ToolCall(call) = content {
            let name = call.function.name.as_str();
            if !executable.contains(name) {
                return Err(format!(
                    "provider streamed a call to tool `{name}`, which is not registered"
                ));
            }
            if !allowed.contains(name) {
                return Err(format!(
                    "provider streamed a call to tool `{name}`, which `tool_choice` does not authorize"
                ));
            }
        }
    }
    let response = &transcript.response;
    agent
        .record_streamed_completion_call(
            response.usage,
            ResponseIdentity {
                message_id: response.message_id.clone(),
                response_id: response.response_id.clone(),
                provider_request_id: response.provider_request_id.clone(),
            },
            response.finish_reason(),
            response.raw.clone(),
        )
        .map_err(|e| e.to_string())?;
    agent
        .streamed_turn(StreamedTurn {
            message_id: response.message_id.clone(),
            choice: response.choice.clone(),
            executable_tool_names: executable,
            allowed_tool_names: allowed,
            finish_reason: response.finish_reason(),
        })
        .map_err(|e| e.to_string())
}
