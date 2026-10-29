use rig::{DynModel, completion::CompletionRequest, operation::Completion};

use crate::streaming::{StreamItem, StreamTranscript};
use futures::StreamExt;

/// The provider response retained by the backend activity.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CompletionOutput {
    pub response: rig::completion::CompletionResponse,
}

/// Execute a provider request. The persistent runner validates the response
/// against the request it planned.
pub async fn complete(
    model: &DynModel<Completion>,
    request: CompletionRequest,
) -> Result<CompletionOutput, String> {
    let response = model
        .call(request)
        .await
        .map_err(|error| error.to_string())?;
    Ok(CompletionOutput { response })
}

/// Consume the provider stream to EOF inside one activity. The transcript is
/// not visible to orchestration until this activity completes.
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
            rig::streaming::StreamEvent::Start { part, kind, name } => StreamItem::Start {
                part: part.index() as u32,
                kind,
                name,
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
