//! Logical identity of one tool call and physical metadata of one attempt.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const CANONICAL_FORMAT: &str = "v1";

/// Identity of one logical tool call. It is fixed across activity retries,
/// redelivery, continue-as-new, and worker upgrades.
///
/// Do not use a provider tool-call ID alone as an idempotency key; providers
/// can repeat it across prompts.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogicalCallKey {
    /// Identifies the logical execution. Backend continuations (Duroxide
    /// `continue_as_new`, Temporal continue-as-new) keep this value.
    pub logical_execution_id: String,
    /// Identifies the submission (prompt) within the logical execution.
    pub submission_id: String,
    /// Model-call index within the submission.
    pub model_turn: usize,
    /// Position of the call in the model turn.
    pub call_index: usize,
}

impl LogicalCallKey {
    /// Build the submission identity used for a prompt index.
    pub fn submission_for_prompt(prompt_index: u64) -> String {
        format!("prompt-{prompt_index}")
    }

    /// Unambiguous textual form. Free-form components are length-prefixed so
    /// no choice of delimiter inside them can collide with another key.
    pub fn canonical(&self) -> String {
        format!(
            "{CANONICAL_FORMAT};{}:{};{}:{};{};{}",
            self.logical_execution_id.len(),
            self.logical_execution_id,
            self.submission_id.len(),
            self.submission_id,
            self.model_turn,
            self.call_index
        )
    }

    /// Hex SHA-256 of the canonical form, for stores that need a fixed-size
    /// key.
    pub fn digest(&self) -> String {
        format!("{:x}", Sha256::digest(self.canonical().as_bytes()))
    }
}

/// Physical attempt information. Useful for diagnosis; never an idempotency
/// key.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptMetadata {
    /// Backend execution or run identity of the scheduling workflow.
    pub backend_execution_id: String,
    /// One-based activity attempt, when the backend reports it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_attempt: Option<u32>,
}

/// Hex SHA-256 of the canonical JSON form of `arguments`.
pub fn arguments_digest(arguments: &serde_json::Value) -> Result<String, serde_json::Error> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(arguments)?)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(execution: &str, submission: &str, turn: usize, call: usize) -> LogicalCallKey {
        LogicalCallKey {
            logical_execution_id: execution.into(),
            submission_id: submission.into(),
            model_turn: turn,
            call_index: call,
        }
    }

    #[test]
    fn canonical_form_is_stable_and_length_prefixed() {
        assert_eq!(
            key("run-1", "prompt-0", 2, 3).canonical(),
            "v1;5:run-1;8:prompt-0;2;3"
        );
    }

    #[test]
    fn delimiters_inside_components_do_not_collide() {
        let a = key("run;1", "p", 1, 0);
        let b = key("run", "1;p", 1, 0);
        let c = key("run;1;1:p", "", 1, 0);
        assert_ne!(a.canonical(), b.canonical());
        assert_ne!(a.canonical(), c.canonical());
        assert_ne!(b.canonical(), c.canonical());
    }

    #[test]
    fn submissions_and_positions_separate_keys() {
        let base = key("run", "prompt-0", 1, 0);
        assert_ne!(base.canonical(), key("run", "prompt-1", 1, 0).canonical());
        assert_ne!(base.canonical(), key("run", "prompt-0", 2, 0).canonical());
        assert_ne!(base.canonical(), key("run", "prompt-0", 1, 1).canonical());
        assert_eq!(base.digest(), key("run", "prompt-0", 1, 0).digest());
    }

    #[test]
    fn attempt_metadata_omits_an_unknown_attempt() {
        let json = serde_json::to_string(&AttemptMetadata {
            backend_execution_id: "exec".into(),
            activity_attempt: None,
        })
        .unwrap();
        assert_eq!(json, r#"{"backend_execution_id":"exec"}"#);
    }
}
