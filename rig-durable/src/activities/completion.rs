use std::collections::BTreeSet;

#[cfg(feature = "duroxide")]
use futures::StreamExt;
#[cfg(feature = "duroxide")]
use rig::streaming::StreamedAssistantContent;
use rig::{
    agent::ModelTurn,
    completion::{CompletionModel, CompletionRequest},
    message::ToolChoice,
};

#[cfg(feature = "duroxide")]
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
    let finish_reason = response.finish_reason();
    let turn = ModelTurn::new(
        response.message_id,
        response.choice,
        response.usage,
        executable,
        allowed,
    )
    .with_identity(response.response_id, response.provider_request_id)
    .with_finish_reason(finish_reason)
    .with_raw(response.raw);
    Ok(turn)
}

/// Consume the provider stream to EOF inside one activity. The transcript is
/// not visible to orchestration until this activity completes.
#[cfg(feature = "duroxide")]
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
                internal_call_id,
                content,
            } => StreamItem::ToolCallDelta {
                internal_call_id,
                content,
            },
            StreamedAssistantContent::Reasoning { reasoning, id } => {
                StreamItem::Reasoning { reasoning, id }
            }
            StreamedAssistantContent::ReasoningDelta {
                id,
                provider_id,
                reasoning,
            } => StreamItem::ReasoningDelta {
                id,
                provider_id,
                reasoning,
            },
            StreamedAssistantContent::Final(response) => {
                if saw_final {
                    return Err("provider emitted multiple Final events".into());
                }
                saw_final = true;
                StreamItem::Final { response }
            }
            StreamedAssistantContent::Unknown(value) => StreamItem::Unknown {
                value: value.value().clone(),
            },
        };
        items.push(normalized);
    }
    if !saw_final {
        return Err("provider stream ended before Final".into());
    }
    Ok(StreamTranscript {
        items,
        message_id: stream.message_id.clone(),
        final_choice: stream.choice.clone(),
    })
}
