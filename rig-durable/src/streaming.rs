//! Provider-neutral durable representation of a completed provider stream.

use rig::{
    OneOrMany,
    completion::{AssistantContent, GetTokenUsage, Usage},
    message::{Reasoning, Text, ToolCall},
    streaming::{StreamedAssistantContent, ToolCallDeltaContent},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamItem {
    Text {
        text: Text,
    },
    ToolCall {
        tool_call: ToolCall,
        internal_call_id: String,
    },
    ToolCallDelta {
        id: String,
        internal_call_id: String,
        content: ToolCallDeltaContent,
    },
    Reasoning {
        reasoning: Reasoning,
    },
    ReasoningDelta {
        id: Option<String>,
        reasoning: String,
    },
    FinalUsage {
        usage: Usage,
    },
    Unknown {
        value: serde_json::Value,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamTranscript {
    pub items: Vec<StreamItem>,
    pub message_id: Option<String>,
    pub final_choice: OneOrMany<AssistantContent>,
}

#[derive(Clone, Debug)]
pub(crate) struct TranscriptFinal(pub Usage);

impl GetTokenUsage for TranscriptFinal {
    fn token_usage(&self) -> Usage {
        self.0
    }
}

impl StreamItem {
    pub(crate) fn as_rig(&self) -> StreamedAssistantContent<TranscriptFinal> {
        match self {
            Self::Text { text } => StreamedAssistantContent::Text(text.clone()),
            Self::ToolCall {
                tool_call,
                internal_call_id,
            } => StreamedAssistantContent::ToolCall {
                tool_call: tool_call.clone(),
                internal_call_id: internal_call_id.clone(),
            },
            Self::ToolCallDelta {
                id,
                internal_call_id,
                content,
            } => StreamedAssistantContent::ToolCallDelta {
                id: id.clone(),
                internal_call_id: internal_call_id.clone(),
                content: content.clone(),
            },
            Self::Reasoning { reasoning } => StreamedAssistantContent::Reasoning(reasoning.clone()),
            Self::ReasoningDelta { id, reasoning } => StreamedAssistantContent::ReasoningDelta {
                id: id.clone(),
                reasoning: reasoning.clone(),
            },
            Self::FinalUsage { usage } => StreamedAssistantContent::Final(TranscriptFinal(*usage)),
            Self::Unknown { value } => StreamedAssistantContent::Unknown(value.clone()),
        }
    }
}
