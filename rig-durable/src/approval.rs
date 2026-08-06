use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub approval_id: String,
    pub tool_name: String,
    pub arguments: serde_json::Value,
    pub tool_call_id: String,
    pub call_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ApprovalDecision {
    Approve {
        approval_id: String,
    },
    Deny {
        approval_id: String,
        reason: Option<String>,
    },
}

impl ApprovalDecision {
    pub fn approval_id(&self) -> &str {
        match self {
            Self::Approve { approval_id } | Self::Deny { approval_id, .. } => approval_id,
        }
    }
}
