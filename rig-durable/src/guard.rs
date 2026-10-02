//! Invocation guard: an atomic claim and conditional settlement ledger that
//! lets an activity refuse to repeat an uncertain effect.
//!
//! One configured attempt is not an at-most-once guarantee. A backend can
//! redeliver an activity after a worker loses its lease or dies before it
//! reports completion. The guard closes that window at the activity:
//!
//! 1. Claim the logical call key atomically before any I/O, persisting the
//!    final arguments digest, policy, and implementation version.
//! 2. Only the attempt that created the claim performs the effect.
//! 3. Store the result under the claim before returning it to the backend.
//! 4. Redelivery returns the stored terminal result, or reports an existing
//!    unsettled claim as interrupted. It never takes over the claim.
//! 5. Settlement is conditional on the claim token, so a slow earlier attempt
//!    cannot overwrite a later record.
//!
//! The crate does not read or alter backend-private tables. Applications
//! supply a store that implements [`InvocationGuardStore`]; the in-memory
//! store is for tests and single-process use. Any store must pass
//! [`conformance::run`].

use std::{collections::HashMap, sync::Mutex};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    identity::{AttemptMetadata, LogicalCallKey},
    policy::ToolPolicy,
    result::DurableToolResult,
};

#[derive(Debug, Error)]
pub enum GuardError {
    /// The store could not confirm whether the write happened. Execution must
    /// not proceed.
    #[error("invocation guard store failed: {0}")]
    Store(String),
}

/// Data persisted when an attempt claims a logical call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRequest {
    pub key: LogicalCallKey,
    pub tool_name: String,
    pub arguments_digest: String,
    pub policy: ToolPolicy,
    pub attempt: AttemptMetadata,
    /// Unique per attempt; the only credential that can settle the claim.
    pub claim_token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ClaimState {
    Claimed,
    Settled { result: DurableToolResult },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClaimRecord {
    pub claim: ClaimRequest,
    pub state: ClaimState,
}

#[derive(Clone, Debug)]
pub enum ClaimOutcome {
    /// This attempt owns the claim and may perform the effect.
    Created,
    /// Another attempt already claimed the key.
    Existing(Box<ClaimRecord>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettleOutcome {
    Settled,
    /// The claim is missing, already settled, or owned by a different token.
    Rejected,
}

/// Atomic create-if-absent claims with conditional settlement.
pub trait InvocationGuardStore: Send + Sync {
    /// Create the claim if no record exists for `claim.key`. Must be atomic
    /// with respect to concurrent claims for the same key.
    fn claim(&self, claim: ClaimRequest) -> BoxFuture<'_, Result<ClaimOutcome, GuardError>>;

    /// Record a terminal result only when the record is still `Claimed` and
    /// `claim_token` matches.
    fn settle(
        &self,
        key: &LogicalCallKey,
        claim_token: &str,
        result: DurableToolResult,
    ) -> BoxFuture<'_, Result<SettleOutcome, GuardError>>;

    fn get(&self, key: &LogicalCallKey) -> BoxFuture<'_, Result<Option<ClaimRecord>, GuardError>>;
}

/// Process-local store for tests and single-worker deployments. It is not
/// durable across process restarts.
#[derive(Default)]
pub struct InMemoryGuardStore {
    records: Mutex<HashMap<String, ClaimRecord>>,
}

impl InMemoryGuardStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, HashMap<String, ClaimRecord>>, GuardError> {
        self.records
            .lock()
            .map_err(|_| GuardError::Store("guard store mutex poisoned".into()))
    }
}

impl InvocationGuardStore for InMemoryGuardStore {
    fn claim(&self, claim: ClaimRequest) -> BoxFuture<'_, Result<ClaimOutcome, GuardError>> {
        Box::pin(async move {
            let mut records = self.lock()?;
            let canonical = claim.key.canonical();
            if let Some(existing) = records.get(&canonical) {
                return Ok(ClaimOutcome::Existing(Box::new(existing.clone())));
            }
            records.insert(
                canonical,
                ClaimRecord {
                    claim,
                    state: ClaimState::Claimed,
                },
            );
            Ok(ClaimOutcome::Created)
        })
    }

    fn settle(
        &self,
        key: &LogicalCallKey,
        claim_token: &str,
        result: DurableToolResult,
    ) -> BoxFuture<'_, Result<SettleOutcome, GuardError>> {
        let canonical = key.canonical();
        let claim_token = claim_token.to_owned();
        Box::pin(async move {
            let mut records = self.lock()?;
            let Some(record) = records.get_mut(&canonical) else {
                return Ok(SettleOutcome::Rejected);
            };
            if record.claim.claim_token != claim_token
                || !matches!(record.state, ClaimState::Claimed)
            {
                return Ok(SettleOutcome::Rejected);
            }
            record.state = ClaimState::Settled { result };
            Ok(SettleOutcome::Settled)
        })
    }

    fn get(&self, key: &LogicalCallKey) -> BoxFuture<'_, Result<Option<ClaimRecord>, GuardError>> {
        let canonical = key.canonical();
        Box::pin(async move { Ok(self.lock()?.get(&canonical).cloned()) })
    }
}

/// Conformance suite every store adapter must pass.
pub mod conformance {
    use std::sync::Arc;

    use rig::tool::{ToolOutput, ToolResult, portable::ToolResultContext};

    use super::*;
    use crate::{policy::MetadataRetention, result::ToolDisposition};

    fn key(suffix: &str) -> LogicalCallKey {
        LogicalCallKey {
            logical_execution_id: format!("conformance-{suffix}"),
            submission_id: "prompt-0".into(),
            model_turn: 1,
            call_index: 0,
        }
    }

    fn request(key: LogicalCallKey, token: &str) -> ClaimRequest {
        ClaimRequest {
            key,
            tool_name: "pay".into(),
            arguments_digest: "digest".into(),
            policy: ToolPolicy::interrupt_on_uncertain(),
            attempt: AttemptMetadata {
                backend_execution_id: token.into(),
                activity_attempt: Some(1),
            },
            claim_token: token.into(),
        }
    }

    fn settled(text: &str) -> DurableToolResult {
        DurableToolResult::from_rig(
            ToolResult::success(ToolOutput::text(text)),
            &ToolResultContext::default(),
            &MetadataRetention::none(),
        )
        .0
    }

    /// Run every conformance check against `store`. Panics with a message on
    /// the first failed check.
    pub async fn run(store: Arc<dyn InvocationGuardStore>) -> Result<(), String> {
        first_claim_wins(&*store).await?;
        settlement_is_conditional(&*store).await?;
        concurrent_claims_have_one_winner(store).await
    }

    async fn first_claim_wins(store: &dyn InvocationGuardStore) -> Result<(), String> {
        let key = key("first");
        match store
            .claim(request(key.clone(), "a"))
            .await
            .map_err(|error| error.to_string())?
        {
            ClaimOutcome::Created => {}
            ClaimOutcome::Existing(_) => return Err("fresh key reported an existing claim".into()),
        }
        match store
            .claim(request(key.clone(), "b"))
            .await
            .map_err(|error| error.to_string())?
        {
            ClaimOutcome::Existing(record) => {
                if record.claim.claim_token != "a" {
                    return Err("second claim did not observe the first token".into());
                }
                if !matches!(record.state, ClaimState::Claimed) {
                    return Err("unsettled claim reported as settled".into());
                }
            }
            ClaimOutcome::Created => return Err("second claim took over the key".into()),
        }
        Ok(())
    }

    async fn settlement_is_conditional(store: &dyn InvocationGuardStore) -> Result<(), String> {
        let key = key("settle");
        store
            .claim(request(key.clone(), "owner"))
            .await
            .map_err(|error| error.to_string())?;
        let rejected = store
            .settle(&key, "intruder", settled("stolen"))
            .await
            .map_err(|error| error.to_string())?;
        if rejected != SettleOutcome::Rejected {
            return Err("settlement with a foreign token was accepted".into());
        }
        let accepted = store
            .settle(&key, "owner", settled("first"))
            .await
            .map_err(|error| error.to_string())?;
        if accepted != SettleOutcome::Settled {
            return Err("owner settlement was rejected".into());
        }
        let late = store
            .settle(&key, "owner", settled("second"))
            .await
            .map_err(|error| error.to_string())?;
        if late != SettleOutcome::Rejected {
            return Err("a settled claim accepted a second settlement".into());
        }
        let record = store
            .get(&key)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("settled claim disappeared")?;
        match record.state {
            ClaimState::Settled { result } => {
                if result.disposition != ToolDisposition::Success
                    || result.model_content("pay")
                        != vec![rig::message::ToolResultContent::text("first")]
                {
                    return Err("stored result is not the first settlement".into());
                }
            }
            ClaimState::Claimed => return Err("settled claim still reports claimed".into()),
        }
        match store
            .claim(request(key, "late-redelivery"))
            .await
            .map_err(|error| error.to_string())?
        {
            ClaimOutcome::Existing(record)
                if matches!(record.state, ClaimState::Settled { .. }) =>
            {
                Ok(())
            }
            _ => Err("redelivery after settlement did not return the stored result".into()),
        }
    }

    async fn concurrent_claims_have_one_winner(
        store: Arc<dyn InvocationGuardStore>,
    ) -> Result<(), String> {
        let key = key("race");
        let attempts = (0..16).map(|index| {
            let store = Arc::clone(&store);
            let key = key.clone();
            async move { store.claim(request(key, &format!("worker-{index}"))).await }
        });
        let outcomes = futures::future::join_all(attempts).await;
        let created = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(ClaimOutcome::Created)))
            .count();
        if created != 1 {
            return Err(format!("expected one winning claim, found {created}"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn in_memory_store_passes_conformance() {
        conformance::run(Arc::new(InMemoryGuardStore::new()))
            .await
            .unwrap();
    }
}
