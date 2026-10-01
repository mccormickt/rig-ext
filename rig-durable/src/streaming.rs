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
