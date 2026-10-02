//! Stable runtime names. These deliberately avoid Duroxide's reserved prefixes.

use crate::activity_types::InvocationContract;

pub const ORCHESTRATION: &str = "RigDurableAgentV1";
pub const COMPLETION_ACTIVITY: &str = "RigModelCompletionV1";
pub const STREAMING_COMPLETION_ACTIVITY: &str = "RigModelStreamingCompletionV1";
/// Tool activity under [`InvocationContract::Legacy`].
pub const TOOL_ACTIVITY: &str = "RigToolExecutionV1";
/// Tool activity under [`InvocationContract::Logical`].
pub const LOGICAL_TOOL_ACTIVITY: &str = "RigToolExecutionV2";

#[derive(Clone, Debug)]
pub(crate) struct RuntimeNames {
    pub orchestration: String,
    pub completion_activity: String,
    pub streaming_completion_activity: String,
    /// Tool activity for the legacy contract.
    pub tool_activity: String,
    /// Tool activity for the logical contract.
    pub logical_tool_activity: String,
}

impl RuntimeNames {
    pub(crate) fn legacy() -> Self {
        Self {
            orchestration: ORCHESTRATION.into(),
            completion_activity: COMPLETION_ACTIVITY.into(),
            streaming_completion_activity: STREAMING_COMPLETION_ACTIVITY.into(),
            tool_activity: TOOL_ACTIVITY.into(),
            logical_tool_activity: LOGICAL_TOOL_ACTIVITY.into(),
        }
    }

    pub(crate) fn for_agent(name: &str, version: &str) -> Self {
        let orchestration = format!("rig-duroxide::agent::{name}");
        let activity_prefix = format!("rig-duroxide::activity::{name}::{version}");
        Self {
            orchestration,
            completion_activity: format!("{activity_prefix}::completion::v1"),
            streaming_completion_activity: format!("{activity_prefix}::streaming::v1"),
            tool_activity: format!("{activity_prefix}::tool::v1"),
            logical_tool_activity: format!("{activity_prefix}::tool::v2"),
        }
    }

    pub(crate) fn tool_activity(&self, contract: InvocationContract) -> &str {
        match contract {
            InvocationContract::Legacy => &self.tool_activity,
            InvocationContract::Logical => &self.logical_tool_activity,
        }
    }
}
