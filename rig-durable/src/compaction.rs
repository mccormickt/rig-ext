//! Bound the active model context separately from the audit transcript.
//!
//! A [`ContextState`] keeps the full transcript and, optionally, one
//! [`CompactionRecord`] that stands in for a prefix of it. Compaction happens
//! only between completed prompts: the cutoff always falls on a prompt
//! boundary, so assistant turns stay with their tool results and the active
//! context stays a canonical transcript. The transcript itself is never
//! shortened by compaction.

use rig::{
    completion::{Message, Usage},
    message::UserContent,
    transcript::{TranscriptError, validate_canonical},
};
use serde::{Deserialize, Serialize};

pub const COMPACTION_FORMAT_VERSION: u32 = 1;

const DEFAULT_INSTRUCTIONS: &str = "You compact conversation history for an assistant that will \
continue the conversation. Write a concise summary that preserves user goals, decisions, facts, \
tool results, and open questions. Do not add commentary.";

/// When and how to compact the active context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionPolicy {
    /// Compact when the active context holds more messages than this.
    pub max_context_messages: usize,
    /// Minimum number of recent transcript messages kept verbatim. The
    /// cutoff moves to the latest prompt boundary that keeps at least this
    /// many messages.
    pub keep_recent_messages: usize,
    /// Version of the summarization instructions and model settings. It is
    /// recorded with every summary.
    pub version: String,
    /// System instructions for the summarization model call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

impl CompactionPolicy {
    pub fn new(max_context_messages: usize, keep_recent_messages: usize) -> Self {
        Self {
            max_context_messages: max_context_messages.max(1),
            keep_recent_messages,
            version: "1".into(),
            instructions: None,
        }
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    pub fn instructions(mut self, instructions: impl Into<String>) -> Self {
        self.instructions = Some(instructions.into());
        self
    }

    pub fn instructions_text(&self) -> &str {
        self.instructions.as_deref().unwrap_or(DEFAULT_INSTRUCTIONS)
    }
}

/// One applied summary. Transcript messages before `cutoff` are represented
/// by `summary` in the active context.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactionRecord {
    pub format_version: u32,
    /// Number of transcript messages the summary stands in for.
    pub cutoff: usize,
    pub policy_version: String,
    pub summary: String,
    pub usage: Usage,
    /// Transcript messages summarized in this round, after the prior cutoff.
    pub input_messages: usize,
}

/// Input of the summarization activity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactionRequest {
    /// The cutoff this summary will stand in for.
    pub cutoff: usize,
    pub policy_version: String,
    pub instructions: String,
    /// Summary that already covers the transcript before `messages`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_summary: Option<String>,
    /// Transcript messages between the prior cutoff and `cutoff`.
    pub messages: Vec<Message>,
}

/// Output of the summarization activity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactionOutput {
    pub cutoff: usize,
    pub policy_version: String,
    pub summary: String,
    pub usage: Usage,
    pub input_messages: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CompactionError {
    #[error("summary cutoff {cutoff} does not advance past the applied cutoff {applied}")]
    Stale { cutoff: usize, applied: usize },
    #[error("summary cutoff {cutoff} exceeds the transcript length {len}")]
    OutOfRange { cutoff: usize, len: usize },
    #[error("summary cutoff {cutoff} is not a prompt boundary")]
    NotABoundary { cutoff: usize },
    #[error("unsupported compaction record format version {0}")]
    UnsupportedFormat(u32),
}

/// Audit transcript plus the summary that currently stands in for its prefix.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContextState {
    /// Every message, in order. Compaction never removes from it.
    pub transcript: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CompactionRecord>,
}

impl ContextState {
    pub fn new(transcript: Vec<Message>) -> Self {
        Self {
            transcript,
            compaction: None,
        }
    }

    pub fn applied_cutoff(&self) -> usize {
        self.compaction
            .as_ref()
            .map(|record| record.cutoff)
            .unwrap_or(0)
    }

    pub fn append(&mut self, messages: impl IntoIterator<Item = Message>) {
        self.transcript.extend(messages);
    }

    /// Messages the next prompt is built on: the summary, if any, followed
    /// by the transcript after the cutoff.
    pub fn active_context(&self) -> Vec<Message> {
        match &self.compaction {
            Some(record) => {
                let mut context = Vec::with_capacity(self.transcript.len() - record.cutoff + 1);
                context.push(summary_message(record));
                context.extend(self.transcript[record.cutoff..].iter().cloned());
                context
            }
            None => self.transcript.clone(),
        }
    }

    /// Validate that the active context is a canonical transcript.
    pub fn validate_active_context(&self) -> Result<(), TranscriptError> {
        validate_canonical(&self.active_context())
    }

    /// Decide whether the policy requires a summary now, and of what.
    pub fn plan(&self, policy: &CompactionPolicy) -> Option<CompactionRequest> {
        let active_len = self.active_context().len();
        if active_len <= policy.max_context_messages {
            return None;
        }
        let applied = self.applied_cutoff();
        let latest_allowed = self
            .transcript
            .len()
            .checked_sub(policy.keep_recent_messages)?;
        let cutoff = (applied + 1..=latest_allowed)
            .rev()
            .find(|&index| is_prompt_boundary(&self.transcript, index))?;
        Some(CompactionRequest {
            cutoff,
            policy_version: policy.version.clone(),
            instructions: policy.instructions_text().to_owned(),
            prior_summary: self
                .compaction
                .as_ref()
                .map(|record| record.summary.clone()),
            messages: self.transcript[applied..cutoff].to_vec(),
        })
    }

    /// Apply a finished summary. A summary whose cutoff does not advance past
    /// the applied cutoff is stale and leaves the context unchanged.
    pub fn apply(&mut self, output: CompactionOutput) -> Result<(), CompactionError> {
        let applied = self.applied_cutoff();
        if output.cutoff <= applied {
            return Err(CompactionError::Stale {
                cutoff: output.cutoff,
                applied,
            });
        }
        if output.cutoff > self.transcript.len() {
            return Err(CompactionError::OutOfRange {
                cutoff: output.cutoff,
                len: self.transcript.len(),
            });
        }
        if !is_prompt_boundary(&self.transcript, output.cutoff) {
            return Err(CompactionError::NotABoundary {
                cutoff: output.cutoff,
            });
        }
        self.compaction = Some(CompactionRecord {
            format_version: COMPACTION_FORMAT_VERSION,
            cutoff: output.cutoff,
            policy_version: output.policy_version,
            summary: output.summary,
            usage: output.usage,
            input_messages: output.input_messages,
        });
        Ok(())
    }

    /// Restore a record retained by an earlier execution.
    pub fn with_compaction(
        mut self,
        record: Option<CompactionRecord>,
    ) -> Result<Self, CompactionError> {
        if let Some(record) = &record {
            if record.format_version != COMPACTION_FORMAT_VERSION {
                return Err(CompactionError::UnsupportedFormat(record.format_version));
            }
            if record.cutoff > self.transcript.len() {
                return Err(CompactionError::OutOfRange {
                    cutoff: record.cutoff,
                    len: self.transcript.len(),
                });
            }
        }
        self.compaction = record;
        Ok(self)
    }
}

/// A cutoff is valid when the message at that index starts a new prompt: a
/// user message that carries no tool result, or a system message. The index
/// equal to the transcript length is the end boundary.
pub fn is_prompt_boundary(transcript: &[Message], index: usize) -> bool {
    match transcript.get(index) {
        None => index == transcript.len(),
        Some(Message::System { .. }) => true,
        Some(Message::User { content }) => !content
            .iter()
            .any(|item| matches!(item, UserContent::ToolResult(_))),
        Some(Message::Assistant { .. }) => false,
    }
}

/// The message that stands in for the compacted prefix.
pub fn summary_message(record: &CompactionRecord) -> Message {
    Message::user(format!(
        "Summary of the earlier conversation (compacted, policy version {}):\n{}",
        record.policy_version, record.summary
    ))
}

#[cfg(test)]
mod tests {
    use rig::message::{AssistantContent, ToolCall, ToolFunction, ToolName, ToolResultContent};

    use super::*;

    fn tool_exchange(id: &str) -> Vec<Message> {
        let call = ToolCall::from_wire(
            id,
            ToolFunction::new(
                ToolName::new("add").unwrap(),
                serde_json::json!({"x": 1, "y": 2}),
            ),
        );
        vec![
            Message::Assistant {
                id: None,
                content: vec![AssistantContent::ToolCall(call.clone())],
            },
            Message::User {
                content: vec![UserContent::tool_result(
                    call.id.clone(),
                    call.function.name.clone(),
                    vec![ToolResultContent::text("3")],
                )],
            },
        ]
    }

    /// prompt, tool call, tool result, answer, prompt, answer, prompt, tool
    /// call, tool result, answer.
    fn transcript() -> Vec<Message> {
        let mut messages = vec![Message::user("first")];
        messages.extend(tool_exchange("call-1"));
        messages.push(Message::assistant("three"));
        messages.push(Message::user("second"));
        messages.push(Message::assistant("ok"));
        messages.push(Message::user("third"));
        messages.extend(tool_exchange("call-2"));
        messages.push(Message::assistant("three again"));
        messages
    }

    fn output(cutoff: usize, summary: &str) -> CompactionOutput {
        CompactionOutput {
            cutoff,
            policy_version: "1".into(),
            summary: summary.into(),
            usage: Usage::default(),
            input_messages: cutoff,
        }
    }

    #[test]
    fn cutoff_lands_on_a_prompt_boundary_and_keeps_tool_groups_together() {
        let state = ContextState::new(transcript());
        // 10 messages; keep at least 3 recent. Latest allowed cutoff is 7,
        // which splits the second tool exchange; the plan backs up to 6.
        let plan = state.plan(&CompactionPolicy::new(4, 3)).unwrap();
        assert_eq!(plan.cutoff, 6);
        assert_eq!(plan.messages.len(), 6);
        assert!(plan.prior_summary.is_none());
        validate_canonical(&plan.messages).unwrap();
    }

    #[test]
    fn active_context_after_compaction_is_canonical_and_transcript_is_retained() {
        let mut state = ContextState::new(transcript());
        state.apply(output(6, "user asked twice")).unwrap();
        let context = state.active_context();
        assert_eq!(context.len(), 5);
        validate_canonical(&context).unwrap();
        assert!(matches!(&context[0], Message::User { .. }));
        assert_eq!(state.transcript.len(), 10);
        assert_eq!(state.transcript, transcript());

        // Later rounds summarize only the messages after the applied cutoff
        // and carry the prior summary.
        state.append([Message::user("fourth"), Message::assistant("done")]);
        let plan = state.plan(&CompactionPolicy::new(3, 2)).unwrap();
        assert_eq!(plan.cutoff, 10);
        assert_eq!(plan.messages.len(), 4);
        assert_eq!(plan.prior_summary.as_deref(), Some("user asked twice"));
    }

    #[test]
    fn stale_summary_does_not_move_the_cutoff_backwards() {
        let mut state = ContextState::new(transcript());
        state.apply(output(6, "newer")).unwrap();
        let stale = state.apply(output(4, "older"));
        assert_eq!(
            stale.unwrap_err(),
            CompactionError::Stale {
                cutoff: 4,
                applied: 6
            }
        );
        assert_eq!(
            state.apply(output(6, "same cutoff")).unwrap_err(),
            CompactionError::Stale {
                cutoff: 6,
                applied: 6
            }
        );
        assert_eq!(state.compaction.as_ref().unwrap().summary, "newer");
        assert_eq!(state.applied_cutoff(), 6);
    }

    #[test]
    fn summaries_that_split_a_tool_exchange_are_rejected() {
        let mut state = ContextState::new(transcript());
        assert_eq!(
            state.apply(output(2, "mid exchange")).unwrap_err(),
            CompactionError::NotABoundary { cutoff: 2 }
        );
        assert_eq!(
            state.apply(output(11, "beyond")).unwrap_err(),
            CompactionError::OutOfRange {
                cutoff: 11,
                len: 10
            }
        );
        assert!(state.compaction.is_none());
    }

    #[test]
    fn no_plan_when_within_budget_or_nothing_can_be_kept() {
        let state = ContextState::new(transcript());
        assert!(state.plan(&CompactionPolicy::new(10, 2)).is_none());
        assert!(state.plan(&CompactionPolicy::new(2, 20)).is_none());
    }
}
