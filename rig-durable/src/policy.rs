//! Backend-neutral tool replay policy.
//!
//! Replay safety answers one question: after an attempt whose outcome is not
//! durably recorded, may the backend run the tool again? Retry timing and
//! scheduling stay backend-specific. Approval is a separate control and does
//! not change replay safety.

use std::collections::BTreeSet;

use rig::tool::{ToolErrorKind, ToolExecutionError};
use serde::{Deserialize, Serialize};

/// Default size limit for retained result metadata, in serialized bytes.
pub const DEFAULT_METADATA_MAX_BYTES: usize = 16 * 1024;

/// Side-effect contract declared by trusted application code.
///
/// This is a declaration about the tool implementation, not something the
/// crate infers from a name, description, or MCP annotation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplaySafety {
    /// Retries may repeat the tool; the application owns idempotency. This is
    /// the behavior of registrations made before replay safety existed, and
    /// the default so those registrations keep their policy.
    #[default]
    ApplicationManaged,
    /// The tool performs no external write. It can run again, but once an
    /// attempt's result is durably recorded replay uses that result.
    ReadOnly,
    /// The tool uses the supplied logical call key so repeated attempts have
    /// one external effect.
    Idempotent,
    /// Never repeat an attempt whose effect is uncertain. Requires an
    /// invocation guard store; a redelivered attempt returns an interrupted
    /// result instead of running the tool again.
    InterruptOnUncertain,
}

impl ReplaySafety {
    /// Whether another attempt may run after an attempt whose outcome was not
    /// recorded.
    pub const fn permits_repeat(self) -> bool {
        !matches!(self, Self::InterruptOnUncertain)
    }

    /// Whether this policy needs an invocation guard store at the activity.
    pub const fn requires_guard(self) -> bool {
        matches!(self, Self::InterruptOnUncertain)
    }
}

/// Which `ToolResultContext` keys an activity may persist with a result.
///
/// Retention is opt-in and size-bounded. Inbound `ToolContext` values and
/// credentials are never persisted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRetention {
    keys: BTreeSet<String>,
    max_bytes: usize,
}

impl Default for MetadataRetention {
    fn default() -> Self {
        Self {
            keys: BTreeSet::new(),
            max_bytes: DEFAULT_METADATA_MAX_BYTES,
        }
    }
}

impl MetadataRetention {
    /// Retain no result metadata.
    pub fn none() -> Self {
        Self::default()
    }

    /// Retain the result metadata stored under `key`.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.insert(key.into());
        self
    }

    /// Limit the serialized size of retained metadata.
    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    pub fn keys(&self) -> &BTreeSet<String> {
        &self.keys
    }

    pub fn limit(&self) -> usize {
        self.max_bytes
    }

    pub fn retains(&self, key: &str) -> bool {
        self.keys.contains(key)
    }
}

/// Backend-neutral policy for one tool registration.
///
/// This policy declares requirements; it does not add idempotency to a Rig tool.
/// An idempotent tool must use [`crate::ToolInvocation::logical_key`] when it
/// sends an external request. A guarded tool needs an [`crate::InvocationGuardStore`]
/// shared by all workers that can execute it. Do not use a process-local store
/// for effects that must remain guarded after a worker restart.
///
/// ```
/// use rig_durable::{MetadataRetention, ReplaySafety, ToolPolicy};
///
/// let policy = ToolPolicy::idempotent()
///     .implementation_version("payments-v1")
///     .retain_metadata(MetadataRetention::none().key("receipt_id").max_bytes(1024));
/// assert_eq!(policy.safety(), ReplaySafety::Idempotent);
/// assert!(policy.metadata().retains("receipt_id"));
/// assert!(!policy.metadata().retains("authorization"));
/// ```
///
/// Use `.tool_with(tool, ToolOptions::default().policy(policy))` on Duroxide
/// with the logical contract, or `.tool_with_policy(tool, policy)` on Temporal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolPolicy {
    replay_safety: ReplaySafety,
    implementation_version: String,
    #[serde(default)]
    metadata: MetadataRetention,
}

impl Default for ToolPolicy {
    fn default() -> Self {
        Self::new(ReplaySafety::ApplicationManaged)
    }
}

impl ToolPolicy {
    /// A policy with the given replay safety, implementation version `1`,
    /// and no retained metadata.
    pub fn new(replay_safety: ReplaySafety) -> Self {
        Self {
            replay_safety,
            implementation_version: "1".into(),
            metadata: MetadataRetention::default(),
        }
    }

    pub fn read_only() -> Self {
        Self::new(ReplaySafety::ReadOnly)
    }

    pub fn idempotent() -> Self {
        Self::new(ReplaySafety::Idempotent)
    }

    pub fn interrupt_on_uncertain() -> Self {
        Self::new(ReplaySafety::InterruptOnUncertain)
    }

    pub fn replay_safety(mut self, replay_safety: ReplaySafety) -> Self {
        self.replay_safety = replay_safety;
        self
    }

    /// Identify the tool implementation. Approval decisions and invocation
    /// claims are bound to it, and a retained configuration snapshot rejects
    /// a worker whose registration has a different version.
    pub fn implementation_version(mut self, version: impl Into<String>) -> Self {
        self.implementation_version = version.into();
        self
    }

    pub fn retain_metadata(mut self, metadata: MetadataRetention) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn safety(&self) -> ReplaySafety {
        self.replay_safety
    }

    pub fn version(&self) -> &str {
        &self.implementation_version
    }

    pub fn metadata(&self) -> &MetadataRetention {
        &self.metadata
    }
}

/// What an activity does with a returned tool error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorHandling {
    /// Fail the attempt so the backend may schedule another one.
    Retry,
    /// Record the error as the attempt's terminal result.
    Record,
}

/// Whether a returned error means another attempt might help.
///
/// An explicit `retryable = false` always wins. Provider failures without a
/// verdict are treated as infrastructure failures, matching existing
/// behavior. Any other error without a verdict is recorded.
pub fn error_is_retryable(error: &ToolExecutionError) -> bool {
    match error.retryable() {
        Some(verdict) => verdict,
        None => matches!(error.kind(), ToolErrorKind::Provider),
    }
}

/// Combine error retryability with replay safety.
///
/// Retryability decides whether another attempt might help. Replay safety
/// decides whether another attempt is permitted. A policy that never repeats
/// an uncertain effect records every returned error, because the error may
/// have arrived after the effect was admitted.
pub fn error_handling(error: &ToolExecutionError, safety: ReplaySafety) -> ErrorHandling {
    if error_is_retryable(error) && safety.permits_repeat() {
        ErrorHandling::Retry
    } else {
        ErrorHandling::Record
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_non_retryable_provider_error_is_recorded() {
        let error = ToolExecutionError::provider("quota exhausted").with_retryable(false);
        assert!(!error_is_retryable(&error));
        for safety in [
            ReplaySafety::ApplicationManaged,
            ReplaySafety::ReadOnly,
            ReplaySafety::Idempotent,
            ReplaySafety::InterruptOnUncertain,
        ] {
            assert_eq!(error_handling(&error, safety), ErrorHandling::Record);
        }
    }

    #[test]
    fn retryable_errors_are_retried_only_when_replay_permits_a_repeat() {
        let network = ToolExecutionError::network("connection reset");
        assert!(error_is_retryable(&network));
        assert_eq!(
            error_handling(&network, ReplaySafety::ReadOnly),
            ErrorHandling::Retry
        );
        assert_eq!(
            error_handling(&network, ReplaySafety::Idempotent),
            ErrorHandling::Retry
        );
        assert_eq!(
            error_handling(&network, ReplaySafety::InterruptOnUncertain),
            ErrorHandling::Record
        );
    }

    #[test]
    fn provider_without_verdict_is_retryable_and_other_is_not() {
        assert!(error_is_retryable(&ToolExecutionError::provider(
            "upstream"
        )));
        assert!(!error_is_retryable(&ToolExecutionError::other("unknown")));
        assert!(!error_is_retryable(&ToolExecutionError::invalid_args(
            "bad"
        )));
        assert!(error_is_retryable(
            &ToolExecutionError::other("flaky").with_retryable(true)
        ));
    }

    #[test]
    fn policy_round_trips_and_defaults_to_application_managed() {
        let policy = ToolPolicy::interrupt_on_uncertain()
            .implementation_version("2026.1")
            .retain_metadata(MetadataRetention::none().key("receipt").max_bytes(512));
        let json = serde_json::to_string(&policy).unwrap();
        let decoded: ToolPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, policy);
        assert!(decoded.metadata().retains("receipt"));
        assert_eq!(decoded.metadata().limit(), 512);

        let legacy: ToolPolicy =
            serde_json::from_str(r#"{"replay_safety":"read_only","implementation_version":"1"}"#)
                .unwrap();
        assert_eq!(legacy.safety(), ReplaySafety::ReadOnly);
        assert_eq!(
            ToolPolicy::default().safety(),
            ReplaySafety::ApplicationManaged
        );
    }
}
