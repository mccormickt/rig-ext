use std::collections::BTreeSet;

use futures::StreamExt;
use rig::{
    agent::ModelTurn,
    completion::{CompletionModel, CompletionRequest, GetTokenUsage},
    message::ToolChoice,
    streaming::StreamedAssistantContent,
};

use crate::streaming::{StreamItem, StreamTranscript};

/// Execute a provider request and retain the exact tool authorization sets
/// used to validate the resulting turn.
pub async fn complete<M: CompletionModel>(
    model: &M,
    request: CompletionRequest,
) -> Result<ModelTurn, String> {
    let executable: BTreeSet<_> = request.tools.iter().map(|tool| tool.name.clone()).collect();
    let allowed = match request.tool_choice.as_ref() {
        Some(ToolChoice::None) => BTreeSet::new(),
        Some(ToolChoice::Specific { function_names }) => function_names
            .iter()
            .filter(|name| executable.contains(*name))
            .cloned()
            .collect(),
        Some(ToolChoice::Auto | ToolChoice::Required) | None => executable.clone(),
    };
    let response = model
        .completion(request)
        .await
        .map_err(|error| error.to_string())?;
    Ok(ModelTurn::new(
        response.message_id,
        response.choice,
        response.usage,
        executable,
        allowed,
    ))
}

/// Consume the provider stream to EOF inside one activity. The transcript is
/// not visible to orchestration until this activity completes.
pub async fn stream<M: CompletionModel>(
    model: &M,
    request: CompletionRequest,
) -> Result<StreamTranscript, String> {
    let mut stream = model
        .stream(request)
        .await
        .map_err(|error| error.to_string())?;
    let mut items = Vec::new();
    let mut saw_final = false;
    while let Some(item) = stream.next().await {
        let item = item.map_err(|error| error.to_string())?;
        if saw_final {
            return Err("provider emitted stream content after Final".into());
        }
        let normalized = match item {
            StreamedAssistantContent::Text(text) => StreamItem::Text { text },
            StreamedAssistantContent::ToolCall {
                tool_call,
                internal_call_id,
            } => StreamItem::ToolCall {
                tool_call,
                internal_call_id,
            },
            StreamedAssistantContent::ToolCallDelta {
                id,
                internal_call_id,
                content,
            } => StreamItem::ToolCallDelta {
                id,
                internal_call_id,
                content,
            },
            StreamedAssistantContent::Reasoning(reasoning) => StreamItem::Reasoning { reasoning },
            StreamedAssistantContent::ReasoningDelta { id, reasoning } => {
                StreamItem::ReasoningDelta { id, reasoning }
            }
            StreamedAssistantContent::Final(response) => {
                if saw_final {
                    return Err("provider emitted multiple Final events".into());
                }
                saw_final = true;
                StreamItem::FinalUsage {
                    usage: response.token_usage(),
                }
            }
            StreamedAssistantContent::Unknown(value) => StreamItem::Unknown { value },
        };
        items.push(normalized);
    }
    Ok(StreamTranscript {
        items,
        message_id: stream.message_id.clone(),
        final_choice: stream.choice.clone(),
    })
}
