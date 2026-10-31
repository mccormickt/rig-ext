use std::{num::NonZeroU32, time::Duration};

use crate::retry::{BackoffStrategy, RetryPolicy};
use rig::{completion::ToolDefinition, message::ToolChoice};
use serde::{Deserialize, Serialize};

use crate::{
    compaction::CompactionConfig,
    policy::ToolPolicy,
    tools::{ToolCatalog, ToolEntry, ToolRoute},
};

pub const DEFAULT_APPROVAL_QUEUE: &str = "rig-duroxide-approvals";
pub const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// Complete native start/continuation input limit. This leaves 1 MiB below
/// Temporal's 2 MiB payload limit for SDK envelopes. Duroxide uses the same cap.
pub const EXECUTION_MAX_BYTES: usize = 1024 * 1024;
/// Leaves headroom below serde_json's 128-container decoder limit.
pub const EXECUTION_MAX_DEPTH: usize = 64;

/// Encode a complete trusted execution envelope with bounded bytes and depth.
#[cfg(any(feature = "duroxide", feature = "temporal"))]
pub(crate) fn encode_execution(value: &impl Serialize) -> Result<String, String> {
    struct Writer {
        bytes: Vec<u8>,
        depth: usize,
        quoted: bool,
        escaped: bool,
    }
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > EXECUTION_MAX_BYTES.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("execution input exceeds 1 MiB"));
            }
            for &byte in bytes {
                if self.quoted {
                    if self.escaped {
                        self.escaped = false;
                    } else if byte == b'\\' {
                        self.escaped = true;
                    } else if byte == b'"' {
                        self.quoted = false;
                    }
                } else {
                    match byte {
                        b'"' => self.quoted = true,
                        b'{' | b'[' => {
                            self.depth += 1;
                            if self.depth > EXECUTION_MAX_DEPTH {
                                return Err(std::io::Error::other(
                                    "execution input exceeds 64 JSON containers",
                                ));
                            }
                        }
                        b'}' | b']' => self.depth -= 1,
                        _ => {}
                    }
                }
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        depth: 0,
        quoted: false,
        escaped: false,
    };
    serde_json::to_writer(&mut writer, value).map_err(|error| error.to_string())?;
    String::from_utf8(writer.bytes).map_err(|error| error.to_string())
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionMode {
    #[default]
    Blocking,
    Streaming,
}

/// Determines when an agent execution starts a fresh Duroxide history window.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointPolicy {
    /// Never checkpoint. This is the default.
    #[default]
    Disabled,
    /// Checkpoint after this many completed model operations or tool batches.
    Every(NonZeroU32),
}

/// Bound each Duroxide event-history window with continue-as-new.
///
/// Each completed model activity or tool batch counts as one operation.
/// The checkpoint retains agent state, usage, and pending decisions; it does
/// not shrink the conversation payload. Pass this value to
/// `DurableAgentBuilder::checkpoint` with the `duroxide` feature.
///
/// ```
/// use std::num::NonZeroU32;
/// use rig_durable::{CheckpointConfig, CheckpointPolicy};
///
/// let checkpoint = CheckpointConfig {
///     policy: CheckpointPolicy::Every(NonZeroU32::new(20).unwrap()),
///     target_version: None,
/// };
/// assert!(matches!(checkpoint.policy, CheckpointPolicy::Every(_)));
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointConfig {
    pub policy: CheckpointPolicy,
    /// Optional application orchestration version to select for the new execution.
    /// This cannot migrate between crate-owned Rig persistence generations.
    pub target_version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    /// Compaction of the active context between completed prompts in a
    /// session. Single runs do not compact.
    pub compaction: Option<CompactionConfig>,
    #[cfg(feature = "codemode")]
    pub code_mode: Option<crate::codemode::CodeModeConfig>,
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
            compaction: None,
            #[cfg(feature = "codemode")]
            code_mode: None,
        }
    }
}

impl DurableAgentConfig {
    /// Serializable copy of every setting that shapes an execution's history.
    pub fn snapshot(&self) -> ConfigSnapshot {
        ConfigSnapshot {
            format_version: SNAPSHOT_FORMAT_VERSION,
            preamble: self.preamble.clone(),
            tools: self
                .tools
                .0
                .values()
                .map(|entry| ToolSnapshot {
                    definition: entry.definition.clone(),
                    route: entry.route.clone(),
                    retry: RetrySnapshot::from(&entry.retry),
                    tag: entry.tag.clone(),
                    requires_approval: entry.requires_approval,
                    policy: entry.policy.clone(),
                })
                .collect(),
            max_turns: self.max_turns,
            completion: self.completion.clone(),
            completion_mode: self.completion_mode.clone(),
            completion_retry: RetrySnapshot::from(&self.completion_retry),
            approval: self.approval.clone(),
            checkpoint: self.checkpoint.clone(),
            compaction: self.compaction.clone(),
            #[cfg(feature = "codemode")]
            code_mode: self.code_mode.clone(),
        }
    }

    /// Restore retained configuration without vetoing recorded results.
    ///
    /// The activity executor checks live tool registration only before new
    /// execution. Raw handlers remain subject to application versioning.
    pub fn resolve(&self, snapshot: &ConfigSnapshot) -> Result<Self, SnapshotError> {
        if snapshot.format_version != SNAPSHOT_FORMAT_VERSION {
            return Err(SnapshotError::UnsupportedFormat(snapshot.format_version));
        }
        let mut tools = ToolCatalog::default();
        for tool in &snapshot.tools {
            tools.insert(ToolEntry {
                definition: tool.definition.clone(),
                route: tool.route.clone(),
                retry: tool.retry.to_policy(),
                tag: tool.tag.clone(),
                requires_approval: tool.requires_approval,
                policy: tool.policy.clone(),
            });
        }
        Ok(Self {
            preamble: snapshot.preamble.clone(),
            tools,
            max_turns: snapshot.max_turns,
            completion: snapshot.completion.clone(),
            completion_mode: snapshot.completion_mode.clone(),
            completion_retry: snapshot.completion_retry.to_policy(),
            approval: snapshot.approval.clone(),
            checkpoint: snapshot.checkpoint.clone(),
            compaction: snapshot.compaction.clone(),
            #[cfg(feature = "codemode")]
            code_mode: snapshot.code_mode.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    #[error("unsupported configuration snapshot format version {0}")]
    UnsupportedFormat(u32),
}

/// Versioned, serializable configuration retained with an execution.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigSnapshot {
    pub format_version: u32,
    pub preamble: Option<String>,
    /// Ordered by tool name.
    pub tools: Vec<ToolSnapshot>,
    pub max_turns: usize,
    pub completion: CompletionSettings,
    pub completion_mode: CompletionMode,
    pub completion_retry: RetrySnapshot,
    pub approval: ApprovalConfig,
    pub checkpoint: CheckpointConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CompactionConfig>,
    #[cfg(feature = "codemode")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_mode: Option<crate::codemode::CodeModeConfig>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolSnapshot {
    pub definition: ToolDefinition,
    pub route: ToolRoute,
    pub retry: RetrySnapshot,
    pub tag: Option<String>,
    pub requires_approval: bool,
    pub policy: ToolPolicy,
}

/// Serializable retry settings, with durations in milliseconds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RetrySnapshot {
    pub max_attempts: u32,
    pub backoff: BackoffSnapshot,
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum BackoffSnapshot {
    None,
    Fixed {
        delay_ms: u64,
    },
    Linear {
        base_ms: u64,
        max_ms: u64,
    },
    Exponential {
        base_ms: u64,
        multiplier: f64,
        max_ms: u64,
    },
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl From<&RetryPolicy> for RetrySnapshot {
    fn from(policy: &RetryPolicy) -> Self {
        Self {
            max_attempts: policy.max_attempts,
            backoff: match &policy.backoff {
                BackoffStrategy::None => BackoffSnapshot::None,
                BackoffStrategy::Fixed { delay } => BackoffSnapshot::Fixed {
                    delay_ms: millis(*delay),
                },
                BackoffStrategy::Linear { base, max } => BackoffSnapshot::Linear {
                    base_ms: millis(*base),
                    max_ms: millis(*max),
                },
                BackoffStrategy::Exponential {
                    base,
                    multiplier,
                    max,
                } => BackoffSnapshot::Exponential {
                    base_ms: millis(*base),
                    multiplier: *multiplier,
                    max_ms: millis(*max),
                },
            },
            timeout_ms: policy.timeout.map(millis),
        }
    }
}

impl RetrySnapshot {
    pub fn to_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_attempts: self.max_attempts.max(1),
            backoff: match &self.backoff {
                BackoffSnapshot::None => BackoffStrategy::None,
                BackoffSnapshot::Fixed { delay_ms } => BackoffStrategy::Fixed {
                    delay: Duration::from_millis(*delay_ms),
                },
                BackoffSnapshot::Linear { base_ms, max_ms } => BackoffStrategy::Linear {
                    base: Duration::from_millis(*base_ms),
                    max: Duration::from_millis(*max_ms),
                },
                BackoffSnapshot::Exponential {
                    base_ms,
                    multiplier,
                    max_ms,
                } => BackoffStrategy::Exponential {
                    base: Duration::from_millis(*base_ms),
                    multiplier: *multiplier,
                    max: Duration::from_millis(*max_ms),
                },
            },
            timeout: self.timeout_ms.map(Duration::from_millis),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::tools::activity_tool;

    fn definition(name: &str) -> ToolDefinition {
        ToolDefinition {
            name: rig::message::ToolName::new(name).unwrap(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object"}),
            annotations: Default::default(),
        }
    }

    fn config() -> DurableAgentConfig {
        let mut tools = ToolCatalog::default();
        tools.insert(activity_tool(
            definition("pay"),
            "PayActivity",
            RetryPolicy::new(2),
            ToolPolicy::read_only().implementation_version("3"),
        ));
        DurableAgentConfig {
            tools,
            max_turns: 3,
            ..Default::default()
        }
    }

    #[test]
    fn snapshot_round_trips_and_drives_the_resolved_configuration() {
        let live = config();
        let snapshot = live.snapshot();
        let json = serde_json::to_string(&snapshot).unwrap();
        let snapshot: ConfigSnapshot = serde_json::from_str(&json).unwrap();

        let mut drifted = live.clone();
        drifted.max_turns = 99;
        drifted.tools.0.get_mut("pay").unwrap().requires_approval = true;
        let resolved = drifted.resolve(&snapshot).unwrap();
        assert_eq!(resolved.max_turns, 3);
        assert!(!resolved.tools.get("pay").unwrap().requires_approval);
        assert_eq!(resolved.tools.get("pay").unwrap().retry.max_attempts, 2);
        assert_eq!(
            RetrySnapshot::from(&resolved.completion_retry),
            RetrySnapshot::from(&RetryPolicy::default())
        );
        let mut unsupported = serde_json::to_value(snapshot).unwrap();
        let field = if cfg!(feature = "codemode") {
            "unavailable_capability"
        } else {
            "code_mode"
        };
        unsupported[field] = serde_json::json!({});
        assert!(serde_json::from_value::<ConfigSnapshot>(unsupported).is_err());
    }

    #[test]
    fn snapshots_restore_without_live_registration_vetoes() {
        let live = config();
        let snapshot = live.snapshot();

        let mut renamed = live.clone();
        renamed.tools = ToolCatalog::default();
        assert_eq!(renamed.resolve(&snapshot).unwrap().snapshot(), snapshot);

        let mut upgraded = live.clone();
        upgraded.tools.0.get_mut("pay").unwrap().policy =
            ToolPolicy::idempotent().implementation_version("4");
        assert_eq!(upgraded.resolve(&snapshot).unwrap().snapshot(), snapshot);

        let mut rerouted = live;
        rerouted.tools.0.get_mut("pay").unwrap().route = ToolRoute::RigTool;
        assert_eq!(rerouted.resolve(&snapshot).unwrap().snapshot(), snapshot);
    }
}
