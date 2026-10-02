use rig::{agent::AgentRun, completion::Message};
use serde::{Deserialize, Serialize};

use crate::{config::ConfigSnapshot, outcome::ToolOutcome};

const CONTINUATION_FORMAT_VERSION: u32 = 1;

/// Persisted orchestration input.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentInput {
    pub prompt: Message,
    #[serde(default)]
    pub history: Vec<Message>,
    /// Configuration retained for this execution. When present, the worker
    /// resolves it against its registration and fails closed on drift. The
    /// orchestration adds its live configuration at the first checkpoint when
    /// the client supplied none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<ConfigSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    continuation: Option<ContinuationEnvelope>,
}

/// Crate-owned, versioned wire state used only by `continue_as_new`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ContinuationEnvelope {
    V1 {
        format_version: u32,
        generation: u32,
        operations: u32,
        last_model_turn: usize,
        #[serde(default)]
        prompt_index: u64,
        agent_run: AgentRun,
        /// Dispositions of the tool calls made before this continuation.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tool_outcomes: Vec<ToolOutcome>,
    },
}

/// State restored from a continuation.
pub(crate) struct ResumedRun {
    pub agent: AgentRun,
    pub generation: u32,
    pub last_model_turn: usize,
    pub prompt_index: u64,
    pub tool_outcomes: Vec<ToolOutcome>,
}

impl AgentInput {
    pub fn new(prompt: impl Into<Message>) -> Self {
        Self {
            prompt: prompt.into(),
            history: Vec::new(),
            snapshot: None,
            continuation: None,
        }
    }

    pub fn with_snapshot(mut self, snapshot: ConfigSnapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    pub(crate) fn is_resume(&self) -> bool {
        self.continuation.is_some()
    }

    pub(crate) fn into_run(self, max_turns: usize) -> Result<ResumedRun, String> {
        match self.continuation {
            None => Ok(ResumedRun {
                agent: AgentRun::new(self.prompt)
                    .with_history(self.history)
                    .max_turns(max_turns),
                generation: 0,
                last_model_turn: 0,
                prompt_index: 0,
                tool_outcomes: Vec::new(),
            }),
            Some(ContinuationEnvelope::V1 {
                format_version,
                generation,
                operations: _,
                last_model_turn,
                prompt_index,
                agent_run,
                tool_outcomes,
            }) if format_version == CONTINUATION_FORMAT_VERSION => Ok(ResumedRun {
                agent: agent_run,
                generation,
                last_model_turn,
                prompt_index,
                tool_outcomes,
            }),
            Some(_) => Err("unsupported agent continuation format version".into()),
        }
    }

    pub(crate) fn resume(
        agent_run: AgentRun,
        generation: u32,
        operations: u32,
        last_model_turn: usize,
        prompt_index: u64,
        snapshot: Option<ConfigSnapshot>,
        tool_outcomes: Vec<ToolOutcome>,
    ) -> Self {
        Self {
            // Start-only fields are retained for a stable, human-readable wire shape.
            prompt: Message::user(""),
            history: Vec::new(),
            snapshot,
            continuation: Some(ContinuationEnvelope::V1 {
                format_version: CONTINUATION_FORMAT_VERSION,
                generation,
                operations,
                last_model_turn,
                prompt_index,
                agent_run,
                tool_outcomes,
            }),
        }
    }
}
