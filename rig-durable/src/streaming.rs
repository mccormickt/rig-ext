//! Provider-neutral durable representation of a completed provider stream.

use rig::{
    completion::AssistantContent,
    message::{Reasoning, Text, ToolCall},
    streaming::{StreamFinal, StreamedAssistantContent, ToolCallDeltaContent, UnknownPayload},
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
        internal_call_id: String,
        content: ToolCallDeltaContent,
    },
    Reasoning {
        reasoning: Reasoning,
        id: String,
    },
    ReasoningDelta {
        id: String,
        provider_id: Option<String>,
        reasoning: String,
    },
    Final {
        response: StreamFinal,
    },
    Unknown {
        value: serde_json::Value,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamTranscript {
    pub items: Vec<StreamItem>,
    pub message_id: Option<String>,
    pub final_choice: Vec<AssistantContent>,
}

impl StreamItem {
    pub(crate) fn as_rig(&self) -> StreamedAssistantContent {
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
                internal_call_id,
                content,
            } => StreamedAssistantContent::ToolCallDelta {
                internal_call_id: internal_call_id.clone(),
                content: content.clone(),
            },
            Self::Reasoning { reasoning, id } => StreamedAssistantContent::Reasoning {
                reasoning: reasoning.clone(),
                id: id.clone(),
            },
            Self::ReasoningDelta {
                id,
                provider_id,
                reasoning,
            } => StreamedAssistantContent::ReasoningDelta {
                id: id.clone(),
                provider_id: provider_id.clone(),
                reasoning: reasoning.clone(),
            },
            Self::Final { response } => StreamedAssistantContent::Final(response.clone()),
            Self::Unknown { value } => {
                StreamedAssistantContent::Unknown(UnknownPayload::new(value.clone()))
            }
        }
    }
}
