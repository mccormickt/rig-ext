use rig::{OneOrMany, message::ToolResultContent};
use serde::{Deserialize, Serialize};

use crate::streaming::StreamTranscript;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolActivityInput {
    pub name: String,
    pub arguments: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolActivityOutput {
    pub content: OneOrMany<ToolResultContent>,
    pub is_error: bool,
}

pub type StreamingCompletionOutput = StreamTranscript;
