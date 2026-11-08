//! Temporary checked prefixes, not durable journals. Adapters must bound the
//! response body before parsing and measure elapsed time outside the guest.

use std::time::Duration;

use rig_core::wasm_compat::{WasmBoxedFuture, WasmCompatSend, WasmCompatSync};
use serde::{Deserialize, Serialize};

use super::{Decision, ReplayError, ReplayInput, ReplayState, ScriptStep, validate_source};
use crate::bridge::{Reply, Request, WorkerOutcome};
use crate::projection::bounded_json_with_depth;
use crate::{CatalogEntry, Limits, ScriptOutput};

pub const VERSION: u32 = 1;
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ExchangeBudget {
    pub bytes: usize,
    pub work: u64,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerRequest {
    pub version: u32,
    pub code: String,
    pub catalog: Vec<CatalogEntry>,
    pub limits: Limits,
    pub prefix: Vec<PrefixEntry>,
    pub budget: ExchangeBudget,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrefixEntry {
    pub batch: EventBatch,
    pub reply: Reply,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventBatch {
    pub events: Vec<Request>,
    pub boundary: Boundary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Boundary {
    Idle { output: ScriptOutput },
    Done(WorkerOutcome),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerResponse {
    pub version: u32,
    pub batches: Vec<EventBatch>,
    pub usage: RunnerUsage,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RunnerUsage {
    /// Actual bounded response body length, measured by the adapter before
    /// decoding. The adapter must replace the guest's placeholder value.
    pub response_bytes: usize,
    /// All executed work, including every reconstructed prefix.
    pub work: u64,
    /// Adapter-measured exchange time, including transport and startup. Never
    /// use guest CPU clocks: Workers clocks can freeze during guest execution.
    pub elapsed: Duration,
}

// Decode the envelope separately from guest JSON, which can contain values
// serde_json cannot represent (depth or unpaired surrogates).
pub fn decode_response(body: &[u8], limits: Limits) -> Result<RunnerResponse, ReplayError> {
    use crate::{ExecutionStatus, ScriptDiagnostic};

    #[derive(Deserialize)]
    struct Envelope {
        version: u32,
        batches: Vec<Batch>,
        usage: RunnerUsage,
    }
    #[derive(Deserialize)]
    struct Batch {
        events: Vec<Request>,
        boundary: WireBoundary,
    }
    #[derive(Deserialize)]
    enum WireBoundary {
        Idle { output: ScriptOutput },
        Done(WireOutcome),
    }
    #[derive(Deserialize)]
    struct WireOutcome {
        status: ExecutionStatus,
        output: ScriptOutput,
        // Require the field even when null.
        #[serde(deserialize_with = "return_text")]
        returned: Option<String>,
        diagnostic: Option<ScriptDiagnostic>,
        over_budget_calls: u32,
    }
    fn return_text<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<String>, D::Error> {
        Option::<String>::deserialize(deserializer)
    }

    let envelope: Envelope = serde_json::from_slice(body).map_err(|_| ReplayError::Protocol)?;
    Ok(RunnerResponse {
        version: envelope.version,
        usage: envelope.usage,
        batches: envelope
            .batches
            .into_iter()
            .map(|batch| EventBatch {
                events: batch.events,
                boundary: match batch.boundary {
                    WireBoundary::Idle { output } => Boundary::Idle { output },
                    WireBoundary::Done(wire) => {
                        let mut outcome = WorkerOutcome {
                            status: wire.status,
                            output: wire.output,
                            returned: None,
                            diagnostic: wire.diagnostic,
                            over_budget_calls: wire.over_budget_calls,
                        };
                        if let Some(json) = wire.returned {
                            match serde_json::from_str(&json) {
                                Ok(value) => outcome.returned = Some(value),
                                Err(_) => {
                                    outcome.status = ExecutionStatus::ScriptError;
                                    let message = "the return value is not valid JSON";
                                    let bound =
                                        limits.message_bytes.min(limits.output_bytes).min(8192);
                                    outcome.diagnostic = Some(ScriptDiagnostic {
                                        message: message[..message.len().min(bound)].into(),
                                        line: None,
                                        column: None,
                                        stack: None,
                                    });
                                }
                            }
                        }
                        Boundary::Done(outcome)
                    }
                },
            })
            .collect(),
    })
}

/// The runner must check each expected batch before supplying that batch's
/// reply, reject unknown reply targets and early termination, drain all jobs
/// after each reply, and return the full prefix plus exactly one new batch.
/// It must enforce the remaining budget even when no response arrives.
pub trait ReplayRunner: WasmCompatSend + WasmCompatSync {
    fn run<'a>(
        &'a self,
        request: RunnerRequest,
    ) -> WasmBoxedFuture<'a, Result<RunnerResponse, ReplayError>>;
}

pub struct CheckedPlanner<R> {
    runner: R,
    budget: ExchangeBudget,
}

impl<R: ReplayRunner> CheckedPlanner<R> {
    pub fn new(runner: R, budget: ExchangeBudget) -> Self {
        Self { runner, budget }
    }

    pub async fn plan(&self, input: ReplayInput<'_>) -> Result<ScriptStep, ReplayError> {
        validate_source(input.code, input.limits)?;
        let mut state =
            ReplayState::new(input.catalog, input.limits, input.admitted, input.history)?;
        let mut budget = self.budget;
        // This protocol crosses JavaScript JSON; integers must round-trip exactly.
        if budget.bytes == 0
            || budget.bytes as u128 > MAX_SAFE_INTEGER as u128
            || budget.work == 0
            || budget.work > MAX_SAFE_INTEGER
            || budget.elapsed.is_zero()
            || budget.elapsed.as_millis() > MAX_SAFE_INTEGER as u128
            || input.limits.wall_time.as_secs() > MAX_SAFE_INTEGER
            || [
                input.limits.source_bytes,
                input.limits.memory_bytes,
                input.limits.message_bytes,
                input.limits.output_bytes,
            ]
            .into_iter()
            .any(|value| value as u128 > MAX_SAFE_INTEGER as u128)
        {
            return Err(ReplayError::BudgetExceeded);
        }
        budget.elapsed = budget.elapsed.min(input.limits.wall_time);
        let mut prefix = Vec::new();
        loop {
            // Check borrowed inputs before allocating owned transport copies.
            bounded_json_with_depth(
                &(input.code, input.catalog.entries(), &prefix),
                budget.bytes,
                128,
            )
            .map_err(|_| ReplayError::BudgetExceeded)?;
            let mut request = RunnerRequest {
                version: VERSION,
                code: input.code.into(),
                catalog: input.catalog.entries().to_vec(),
                limits: input.limits,
                prefix: prefix.clone(),
                budget,
            };
            let bytes = bounded_json_with_depth(&request, budget.bytes, 128)
                .map_err(|_| ReplayError::BudgetExceeded)?
                .len();
            budget.bytes = budget
                .bytes
                .checked_sub(bytes)
                .ok_or(ReplayError::BudgetExceeded)?;
            request.budget = budget;
            if budget.bytes == 0 || budget.work == 0 || budget.elapsed.is_zero() {
                return Err(ReplayError::BudgetExceeded);
            }
            let response = self.runner.run(request).await?;
            bounded_json_with_depth(&response, budget.bytes, 128)
                .map_err(|_| ReplayError::BudgetExceeded)?;
            budget.bytes = budget
                .bytes
                .checked_sub(response.usage.response_bytes)
                .ok_or(ReplayError::BudgetExceeded)?;
            budget.work = budget
                .work
                .checked_sub(response.usage.work)
                .ok_or(ReplayError::BudgetExceeded)?;
            budget.elapsed = budget
                .elapsed
                .checked_sub(response.usage.elapsed)
                .ok_or(ReplayError::BudgetExceeded)?;
            if response.version != VERSION
                || response.usage.work == 0
                || response.usage.response_bytes == 0
                || response.batches.len() != prefix.len() + 1
                || !response
                    .batches
                    .iter()
                    .zip(&prefix)
                    .all(|(batch, entry)| batch == &entry.batch)
            {
                return Err(ReplayError::Protocol);
            }
            let batch = response
                .batches
                .into_iter()
                .last()
                .ok_or(ReplayError::Protocol)?;
            validate_batch(&batch, input.limits)?;
            if let Some(previous) = prefix.last() {
                let Boundary::Idle { output: previous } = &previous.batch.boundary else {
                    return Err(ReplayError::Protocol);
                };
                let output = match &batch.boundary {
                    Boundary::Idle { output } => output,
                    Boundary::Done(outcome) => &outcome.output,
                };
                if !output.text.starts_with(&previous.text)
                    || (previous.truncated && output != previous)
                {
                    return Err(ReplayError::Protocol);
                }
            }
            for event in &batch.events {
                state.observe(event.clone())?;
            }
            match &batch.boundary {
                Boundary::Done(outcome) => return state.finish(outcome.clone()),
                Boundary::Idle { .. } => match state.idle()? {
                    Decision::Step(step) => return Ok(step),
                    Decision::Reply(reply) => prefix.push(PrefixEntry { batch, reply }),
                },
            }
        }
    }
}

fn validate_batch(batch: &EventBatch, limits: Limits) -> Result<(), ReplayError> {
    if batch.events.len() > limits.max_calls as usize
        || batch.events.iter().any(|event| match event {
            Request::Idle => true,
            Request::Call {
                name, arguments, ..
            } => {
                name.len() > crate::catalog::MAX_NAME_BYTES
                    || arguments.len() > limits.message_bytes
            }
            Request::Rejected { name, .. } => name.len() > crate::catalog::MAX_NAME_BYTES,
        })
    {
        return Err(ReplayError::Protocol);
    }
    let output = match &batch.boundary {
        Boundary::Idle { output } => output,
        Boundary::Done(outcome) => {
            if outcome.returned.as_ref().is_some_and(|value| {
                bounded_json_with_depth(value, limits.message_bytes, 128).is_err()
            }) {
                return Err(ReplayError::Protocol);
            }
            &outcome.output
        }
    };
    if output.text.len() > limits.output_bytes {
        return Err(ReplayError::Protocol);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
