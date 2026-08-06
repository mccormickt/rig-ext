use std::num::NonZeroU32;

use duroxide::RetryPolicy;
use rig::message::ToolChoice;

use crate::tools::ToolCatalog;

pub const DEFAULT_APPROVAL_QUEUE: &str = "rig-duroxide-approvals";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum CompletionMode {
    #[default]
    Blocking,
    Streaming,
}

/// Determines when an agent execution starts a fresh Duroxide history window.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum CheckpointPolicy {
    /// Never checkpoint. This is the default for backwards compatibility.
    #[default]
    Disabled,
    /// Checkpoint after this many completed model operations or tool batches.
    Every(NonZeroU32),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckpointConfig {
    pub policy: CheckpointPolicy,
    /// Optional orchestration version to select for the new execution.
    pub target_version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovalConfig {
    pub enabled: bool,
    pub queue_name: String,
}

impl Default for ApprovalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            queue_name: DEFAULT_APPROVAL_QUEUE.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CompletionSettings {
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub tool_choice: Option<ToolChoice>,
    pub additional_params: Option<serde_json::Value>,
}

impl Default for CompletionSettings {
    fn default() -> Self {
        Self {
            temperature: None,
            max_tokens: None,
            tool_choice: Some(ToolChoice::Auto),
            additional_params: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DurableAgentConfig {
    pub preamble: Option<String>,
    pub tools: ToolCatalog,
    pub max_turns: usize,
    pub completion: CompletionSettings,
    pub completion_mode: CompletionMode,
    pub completion_retry: RetryPolicy,
    pub approval: ApprovalConfig,
    pub checkpoint: CheckpointConfig,
}

impl Default for DurableAgentConfig {
    fn default() -> Self {
        Self {
            preamble: None,
            tools: ToolCatalog::default(),
            max_turns: 8,
            completion: CompletionSettings::default(),
            completion_mode: CompletionMode::default(),
            completion_retry: RetryPolicy::default(),
            approval: ApprovalConfig::default(),
            checkpoint: CheckpointConfig::default(),
        }
    }
}
