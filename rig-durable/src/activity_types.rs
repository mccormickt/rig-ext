use rig::message::ToolResultContent;
use serde::{Deserialize, Serialize};

#[cfg(feature = "duroxide")]
use crate::streaming::StreamTranscript;
use crate::{
    identity::{AttemptMetadata, LogicalCallKey},
    policy::ToolPolicy,
    result::DurableToolResult,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolActivityInput {
    pub name: String,
    pub arguments: String,
    pub invocation: ToolInvocation,
    /// Recorded policy. Live registration is checked only before new execution.
    pub policy: ToolPolicy,
    /// Native code-mode planner/projection that admitted this call, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_identity: Option<String>,
}

/// Stable identity for one logical tool call. Retries receive the same value.
///
/// Durable Rig tools receive this value through their [`rig::tool::ToolContext`].
/// For idempotent external writes, use the logical key, not the physical
/// execution ID or the provider's tool-call ID.
///
/// ```
/// use rig::tool::ToolContext;
/// use rig_durable::ToolInvocation;
///
/// fn idempotency_key(context: &ToolContext) -> Result<String, String> {
///     let invocation = context.require::<ToolInvocation>().map_err(|e| e.to_string())?;
///     Ok(invocation.logical_key.digest())
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
    /// it as their external idempotency key.
    pub logical_key: LogicalCallKey,
    /// Physical attempt information for diagnosis.
    pub attempt: AttemptMetadata,
}

impl rig::tool::ContextValue for ToolInvocation {
    const KEY: &'static str = "rig_durable.invocation";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolActivityOutput {
    pub content: Vec<ToolResultContent>,
    pub is_error: bool,
    /// Versioned result with its host-visible disposition.
    pub result: DurableToolResult,
}

impl ToolActivityOutput {
    pub fn from_result(tool_name: &str, result: DurableToolResult) -> Self {
        Self {
            content: result.model_content(tool_name),
            is_error: result.is_error_for_model(),
            result,
        }
    }
}

#[cfg(feature = "duroxide")]
pub type StreamingCompletionOutput = StreamTranscript;
