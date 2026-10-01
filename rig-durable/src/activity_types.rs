use rig::message::ToolResultContent;
use serde::{Deserialize, Serialize};

#[cfg(feature = "duroxide")]
use crate::streaming::StreamTranscript;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolActivityInput {
    pub name: String,
    pub arguments: String,
    pub invocation: ToolInvocation,
}

/// Stable identity for one logical tool call. Retries receive the same value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInvocation {
    pub execution_id: String,
    /// Identifies the prompt within a long-lived durable execution.
    pub prompt_index: u64,
    pub turn: usize,
    pub call_index: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolActivityOutput {
    pub content: Vec<ToolResultContent>,
    pub is_error: bool,
}

#[cfg(feature = "duroxide")]
pub type StreamingCompletionOutput = StreamTranscript;
