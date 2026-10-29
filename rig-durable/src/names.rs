//! Stable runtime names. These deliberately avoid Duroxide's reserved prefixes.

use crate::activity_types::InvocationContract;

/// Crate-owned persistence generation, independent of application semver.
pub const PERSISTENCE_GENERATION: u32 = 3;
pub const ORCHESTRATION: &str = "RigDurableAgentG3V1";
pub const COMPLETION_ACTIVITY: &str = "RigModelCompletionG3V1";
pub const STREAMING_COMPLETION_ACTIVITY: &str = "RigModelStreamingCompletionG3V1";
/// Tool activity under [`InvocationContract::Legacy`].
pub const TOOL_ACTIVITY: &str = "RigToolExecutionG3V1";
/// Tool activity under [`InvocationContract::Logical`].
pub const LOGICAL_TOOL_ACTIVITY: &str = "RigToolExecutionG3V2";
/// Long-lived session orchestration.
pub const SESSION_ORCHESTRATION: &str = "RigDurableAgentSessionG3V1";
/// Summarization activity used by completed-prompt compaction.
pub const COMPACTION_ACTIVITY: &str = "RigContextCompactionG3V1";

#[derive(Clone, Debug)]
pub(crate) struct RuntimeNames {
    pub orchestration: String,
    pub completion_activity: String,
    pub streaming_completion_activity: String,
    /// Tool activity for the legacy contract.
    pub tool_activity: String,
    /// Tool activity for the logical contract.
    pub logical_tool_activity: String,
    pub session_orchestration: String,
    pub compaction_activity: String,
}

impl RuntimeNames {
    pub(crate) fn legacy() -> Self {
        Self {
            orchestration: ORCHESTRATION.into(),
            completion_activity: COMPLETION_ACTIVITY.into(),
            streaming_completion_activity: STREAMING_COMPLETION_ACTIVITY.into(),
            tool_activity: TOOL_ACTIVITY.into(),
            logical_tool_activity: LOGICAL_TOOL_ACTIVITY.into(),
            session_orchestration: SESSION_ORCHESTRATION.into(),
            compaction_activity: COMPACTION_ACTIVITY.into(),
        }
    }

    pub(crate) fn for_agent(name: &str, version: &str) -> Self {
        let orchestration = format!("rig-duroxide::g{PERSISTENCE_GENERATION}::agent::{name}");
        let activity_prefix =
            format!("rig-duroxide::g{PERSISTENCE_GENERATION}::activity::{name}::{version}");
        Self {
            orchestration,
            completion_activity: format!("{activity_prefix}::completion::v1"),
            streaming_completion_activity: format!("{activity_prefix}::streaming::v1"),
            tool_activity: format!("{activity_prefix}::tool::v1"),
            logical_tool_activity: format!("{activity_prefix}::tool::v2"),
            session_orchestration: format!(
                "rig-duroxide::g{PERSISTENCE_GENERATION}::session::{name}"
            ),
            compaction_activity: format!("{activity_prefix}::compaction::v1"),
        }
    }

    pub(crate) fn tool_activity(&self, contract: InvocationContract) -> &str {
        match contract {
            InvocationContract::Legacy => &self.tool_activity,
            InvocationContract::Logical => &self.logical_tool_activity,
        }
    }
}
