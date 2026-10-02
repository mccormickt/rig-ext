use rig::message::ToolResultContent;
use serde::{Deserialize, Serialize};

#[cfg(feature = "duroxide")]
use crate::streaming::StreamTranscript;
use crate::{
    identity::{AttemptMetadata, LogicalCallKey},
    policy::ToolPolicy,
    result::DurableToolResult,
};

/// Selects the wire contract between an orchestration and its tool activity.
///
/// Changing the contract of a registered orchestration version changes its
/// activity names and payloads, so existing histories can no longer replay.
/// Select the contract per version and keep old versions registered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationContract {
    /// Byte-identical payloads of registrations made before tool policies
    /// existed. Every tool runs with application-managed replay safety.
    #[default]
    Legacy,
    /// Payloads carry the logical call key, attempt metadata, and the tool
    /// policy, and results retain their disposition.
    Logical,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolActivityInput {
    pub name: String,
    pub arguments: String,
    pub invocation: ToolInvocation,
    /// Policy the activity enforces. Absent under the legacy contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<ToolPolicy>,
}

/// Stable identity for one logical tool call. Retries receive the same value.
///
/// Durable Rig tools receive this value through their [`rig::tool::ToolContext`].
/// For idempotent external writes, use the logical key, not the physical
/// execution ID or the provider's tool-call ID. The logical contract is required.
///
/// ```
/// use rig::tool::ToolContext;
/// use rig_durable::ToolInvocation;
///
/// fn idempotency_key(context: &ToolContext) -> Result<String, String> {
///     let invocation = context.require::<ToolInvocation>().map_err(|e| e.to_string())?;
///     let key = invocation.logical_key.as_ref().ok_or("logical contract required")?;
///     Ok(key.digest())
/// }
/// ```
///
/// Pass the digest to the external service's idempotency mechanism. Merely
/// reading it does not prevent repeated effects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInvocation {
    /// Backend execution identity; changes across continue-as-new.
    pub execution_id: String,
    /// Identifies the prompt within a long-lived durable execution.
    pub prompt_index: u64,
    pub turn: usize,
    pub call_index: usize,
    /// Identity fixed across retries and continuations. Idempotent tools use
    /// it as their external idempotency key. Absent under the legacy contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_key: Option<LogicalCallKey>,
    /// Physical attempt information for diagnosis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<AttemptMetadata>,
}

impl ToolInvocation {
    pub fn legacy(execution_id: String, prompt_index: u64, turn: usize, call_index: usize) -> Self {
        Self {
            execution_id,
            prompt_index,
            turn,
            call_index,
            logical_key: None,
            attempt: None,
        }
    }
}

impl rig::tool::ContextValue for ToolInvocation {
    const KEY: &'static str = "rig_durable.invocation";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolActivityOutput {
    pub content: Vec<ToolResultContent>,
    pub is_error: bool,
    /// Versioned result with its host-visible disposition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<DurableToolResult>,
}

impl ToolActivityOutput {
    pub fn from_result(tool_name: &str, result: DurableToolResult) -> Self {
        Self {
            content: result.model_content(tool_name),
            is_error: result.is_error_for_model(),
            result: Some(result),
        }
    }
}

#[cfg(feature = "duroxide")]
pub type StreamingCompletionOutput = StreamTranscript;
