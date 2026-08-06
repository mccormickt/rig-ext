use rig::{agent::AgentRun, completion::Message};
use serde::{Deserialize, Serialize};

const CONTINUATION_FORMAT_VERSION: u32 = 1;

/// Persisted orchestration input.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentInput {
    pub prompt: Message,
    #[serde(default)]
    pub history: Vec<Message>,
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
        agent_run: AgentRun,
    },
}

impl AgentInput {
    pub fn new(prompt: impl Into<Message>) -> Self {
        Self {
            prompt: prompt.into(),
            history: Vec::new(),
            continuation: None,
        }
    }

    pub(crate) fn is_resume(&self) -> bool {
        self.continuation.is_some()
    }

    pub(crate) fn into_run(self, max_turns: usize) -> Result<(AgentRun, u32, usize), String> {
        match self.continuation {
            None => Ok((
                AgentRun::new(self.prompt)
                    .with_history(self.history)
                    .max_turns(max_turns),
                0,
                0,
            )),
            Some(ContinuationEnvelope::V1 {
                format_version,
                generation,
                operations: _,
                last_model_turn,
                agent_run,
            }) if format_version == CONTINUATION_FORMAT_VERSION => {
                Ok((agent_run, generation, last_model_turn))
            }
            Some(_) => Err("unsupported agent continuation format version".into()),
        }
    }

    pub(crate) fn resume(
        agent_run: AgentRun,
        generation: u32,
        operations: u32,
        last_model_turn: usize,
    ) -> Self {
        Self {
            // Start-only fields are retained for a stable, human-readable wire shape.
            prompt: Message::user(""),
            history: Vec::new(),
            continuation: Some(ContinuationEnvelope::V1 {
                format_version: CONTINUATION_FORMAT_VERSION,
                generation,
                operations,
                last_model_turn,
                agent_run,
            }),
        }
    }
}
