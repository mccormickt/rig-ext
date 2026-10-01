//! Stable runtime names. These deliberately avoid Duroxide's reserved prefixes.

pub const ORCHESTRATION: &str = "RigDurableAgentV1";
pub const COMPLETION_ACTIVITY: &str = "RigModelCompletionV1";
pub const STREAMING_COMPLETION_ACTIVITY: &str = "RigModelStreamingCompletionV1";
pub const TOOL_ACTIVITY: &str = "RigToolExecutionV1";

#[derive(Clone, Debug)]
pub(crate) struct RuntimeNames {
    pub orchestration: String,
    pub completion_activity: String,
    pub streaming_completion_activity: String,
    pub tool_activity: String,
}

impl RuntimeNames {
    pub(crate) fn legacy() -> Self {
        Self {
            orchestration: ORCHESTRATION.into(),
            completion_activity: COMPLETION_ACTIVITY.into(),
            streaming_completion_activity: STREAMING_COMPLETION_ACTIVITY.into(),
            tool_activity: TOOL_ACTIVITY.into(),
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
        }
    }
}
