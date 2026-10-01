use std::collections::BTreeSet;

use rig::{DynModel, completion::CompletionRequest, message::ToolChoice, operation::Completion};

#[cfg(feature = "duroxide")]
use crate::streaming::{StreamItem, StreamTranscript};
#[cfg(feature = "duroxide")]
use futures::StreamExt;

/// Execute a provider request and retain the exact tool authorization sets
/// used to validate the resulting turn.
pub async fn complete(
    model: &DynModel<Completion>,
    request: CompletionRequest,
) -> Result<rig::agent::ModelTurn, String> {
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
        .call(request)
        .await
        .map_err(|error| error.to_string())?;
    let turn = rig::agent::ModelTurn::new(
        response.message_id.clone(),
        response.choice.clone(),
        response.usage,
        executable,
        allowed,
        response.raw.clone(),
    )
    .with_identity(
        response.response_id.clone(),
        response.provider_request_id.clone(),
    )
    .with_finish_reason(response.finish_reason());
    Ok(turn)
}

/// Consume the provider stream to EOF inside one activity. The transcript is
/// not visible to orchestration until this activity completes.
#[cfg(feature = "duroxide")]
pub async fn stream(
    model: &DynModel<Completion>,
    request: CompletionRequest,
) -> Result<StreamTranscript, String> {
    let mut stream = model.stream(request).map_err(|error| error.to_string())?;
    let mut items = Vec::new();
    while let Some(item) = stream.next().await {
        let item = item.map_err(|error| error.to_string())?;
        let event = match item {
            rig::streaming::Item::Event(event) => event,
            rig::streaming::Item::Unknown(payload) => {
                items.push(StreamItem::Unknown {
                    value: payload.value().clone(),
                });
                continue;
            }
        };
        let stored = match event {
            rig::streaming::StreamEvent::Start { part, kind } => StreamItem::Start {
                part: part.index() as u32,
                kind,
            },
            rig::streaming::StreamEvent::Text { part, text } => StreamItem::Text {
                part: part.index() as u32,
                text,
            },
            rig::streaming::StreamEvent::Reasoning { part, text } => StreamItem::Reasoning {
                part: part.index() as u32,
                text,
            },
            rig::streaming::StreamEvent::Arguments { part, json } => StreamItem::Arguments {
                part: part.index() as u32,
                json,
            },
            rig::streaming::StreamEvent::End { part, content } => StreamItem::End {
                part: part.index() as u32,
                content,
            },
        };
        items.push(stored);
    }
    let response = stream.finish().await.map_err(|error| error.to_string())?;
    items.push(StreamItem::Final {
        response: response.clone(),
    });
    Ok(StreamTranscript { items, response })
}
