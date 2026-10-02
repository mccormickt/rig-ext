//! Host-visible result of a prompt: Rig's response plus the ordered
//! disposition of every tool call the run made.

use rig::agent::PromptResponse;
use serde::{Deserialize, Serialize};

use crate::{
    activity_types::ToolActivityOutput,
    result::{Interruption, ToolDisposition},
};

pub const OUTCOME_FORMAT_VERSION: u32 = 1;

/// How the disposition of a call was established.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeSource {
    /// A tool activity under the logical contract retained the disposition.
    Retained,
    /// Derived from the activity's error flag under the legacy contract.
    Derived,
    /// A routed activity, sub-orchestration, or sub-agent returned a value.
    Routed,
    /// A human denied the call; the tool did not run.
    Denied,
    /// Rig resolved the call before dispatch (for example an invalid call).
    Preresolved,
}

/// Disposition of one tool call, in dispatch order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOutcome {
    pub prompt_index: u64,
    pub turn: usize,
    pub call_index: usize,
    pub tool_name: String,
    pub tool_call_id: String,
    pub disposition: ToolDisposition,
    pub source: OutcomeSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interruption: Option<Interruption>,
}

/// Position of a call inside a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallPosition {
    pub prompt_index: u64,
    pub turn: usize,
    pub call_index: usize,
}

impl ToolOutcome {
    fn new(
        position: CallPosition,
        call: &rig::message::ToolCall,
        disposition: ToolDisposition,
        source: OutcomeSource,
    ) -> Self {
        Self {
            prompt_index: position.prompt_index,
            turn: position.turn,
            call_index: position.call_index,
            tool_name: call.function.name.to_string(),
            tool_call_id: call.id.to_string(),
            disposition,
            source,
            interruption: None,
        }
    }

    /// Outcome of a Rig tool activity. Logical-contract outputs carry their
    /// retained disposition; legacy outputs only report whether the model
    /// should treat the content as an error.
    pub fn from_activity(
        position: CallPosition,
        call: &rig::message::ToolCall,
        output: &ToolActivityOutput,
    ) -> Self {
        match &output.result {
            Some(result) => {
                let mut outcome =
                    Self::new(position, call, result.disposition, OutcomeSource::Retained);
                outcome.interruption = result.interruption.clone();
                outcome
            }
            None => Self::new(
                position,
                call,
                if output.is_error {
                    ToolDisposition::Error
                } else {
                    ToolDisposition::Success
                },
                OutcomeSource::Derived,
            ),
        }
    }

    pub fn routed(position: CallPosition, call: &rig::message::ToolCall) -> Self {
        Self::new(
            position,
            call,
            ToolDisposition::Success,
            OutcomeSource::Routed,
        )
    }

    pub fn denied(position: CallPosition, call: &rig::message::ToolCall) -> Self {
        Self::new(
            position,
            call,
            ToolDisposition::Refused,
            OutcomeSource::Denied,
        )
    }

    pub fn preresolved(position: CallPosition, call: &rig::message::ToolCall) -> Self {
        Self::new(
            position,
            call,
            ToolDisposition::Error,
            OutcomeSource::Preresolved,
        )
    }
}

/// Response of one prompt with its tool outcomes. Rig's [`PromptResponse`]
/// is unchanged; the outcomes are additive.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DurableResponse {
    pub format_version: u32,
    pub response: PromptResponse,
    /// Every tool call of the prompt, in dispatch order. When a backend
    /// limit stopped retention, `tool_outcomes_truncated` is set and the
    /// list holds the earliest calls.
    #[serde(default)]
    pub tool_outcomes: Vec<ToolOutcome>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub tool_outcomes_truncated: bool,
}

impl DurableResponse {
    pub fn new(response: PromptResponse, tool_outcomes: Vec<ToolOutcome>) -> Self {
        Self {
            format_version: OUTCOME_FORMAT_VERSION,
            response,
            tool_outcomes,
            tool_outcomes_truncated: false,
        }
    }

    pub fn output(&self) -> &str {
        &self.response.output
    }

    /// Drop outcomes from the end until the serialized response fits
    /// `max_bytes`, and record that it happened.
    pub fn fit_within(mut self, max_bytes: usize) -> Self {
        while serialized_len(&self) > max_bytes && !self.tool_outcomes.is_empty() {
            self.tool_outcomes.pop();
            self.tool_outcomes_truncated = true;
        }
        self
    }
}

fn serialized_len<T: Serialize>(value: &T) -> usize {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use rig::{
        message::{ToolCall, ToolFunction, ToolName},
        tool::{ToolExecutionError, ToolResult},
    };

    use super::*;
    use crate::{policy::MetadataRetention, result::DurableToolResult};

    fn call() -> ToolCall {
        ToolCall::from_wire(
            "call-1",
            ToolFunction::new(ToolName::new("pay").unwrap(), serde_json::json!({})),
        )
    }

    const POSITION: CallPosition = CallPosition {
        prompt_index: 2,
        turn: 1,
        call_index: 0,
    };

    #[test]
    fn retained_dispositions_win_over_the_error_flag() {
        let (result, _) = DurableToolResult::from_rig(
            ToolResult::failed(ToolExecutionError::refused("no")),
            &Default::default(),
            &MetadataRetention::none(),
        );
        let output = ToolActivityOutput::from_result("pay", result);
        let outcome = ToolOutcome::from_activity(POSITION, &call(), &output);
        assert_eq!(outcome.disposition, ToolDisposition::Refused);
        assert_eq!(outcome.source, OutcomeSource::Retained);
        assert_eq!(outcome.prompt_index, 2);
        assert_eq!(outcome.tool_call_id, "call-1");

        let legacy = ToolActivityOutput {
            content: Vec::new(),
            is_error: true,
            result: None,
        };
        let outcome = ToolOutcome::from_activity(POSITION, &call(), &legacy);
        assert_eq!(outcome.disposition, ToolDisposition::Error);
        assert_eq!(outcome.source, OutcomeSource::Derived);
    }

    #[test]
    fn fitting_truncates_from_the_end_and_says_so() {
        let outcomes: Vec<_> = (0..5)
            .map(|index| {
                ToolOutcome::routed(
                    CallPosition {
                        call_index: index,
                        ..POSITION
                    },
                    &call(),
                )
            })
            .collect();
        let response = DurableResponse::new(
            PromptResponse::new("done", rig::completion::Usage::default()),
            outcomes,
        );
        let full = serialized_len(&response);
        let fitted = response.clone().fit_within(full);
        assert!(!fitted.tool_outcomes_truncated);
        let fitted = response.fit_within(full - 1);
        assert!(fitted.tool_outcomes_truncated);
        assert_eq!(fitted.tool_outcomes.len(), 4);
        assert_eq!(fitted.tool_outcomes[3].call_index, 3);
    }
}
