//! Disposable script steps for durable hosts. No tools execute in this module.
//!
//! Persist each returned call and its result before the next step. Pass the
//! same source, catalog, limits, admissions, and delivery journal on recovery.
//! Results settle in journal order, not call order. `Date`, random numbers, weak references,
//! and finalizers are unavailable. A step's wall-time limit covers VM work only;
//! the host owns tool deadlines, authorization, retries, and cancellation.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use rig_core::tool::{ToolExecutionError, ToolResult};
use serde::{Deserialize, Serialize};

use crate::bridge::{self, HostChannels, Reply, Request, WorkerConfig, WorkerOutcome};
use crate::{CallRecord, CallStatus, Catalog, ExecutionReport, Limits, ScriptDelivery};

/// One script call. The ordinal is stable when the script is replayed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScriptCall {
    /// Zero-based call position, including guest-rejected calls.
    pub ordinal: u32,
    /// Exact catalog tool name.
    pub name: String,
    /// Script-supplied arguments. Validate these before execution.
    pub arguments: serde_json::Value,
    /// Whether the script requested a raw result envelope.
    pub raw: bool,
}

/// A call and its durably recorded, policy-filtered result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettledCall {
    /// Original call, used to reject replay divergence before new I/O.
    pub call: ScriptCall,
    /// Only model-visible output; do not include private host metadata.
    pub result: ToolResult,
}

/// The next durable operation, or a terminal script outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScriptStep {
    /// Unresolved calls issued before the VM became idle, including existing
    /// admissions. The host admits only new calls and retains existing futures.
    Calls(Vec<ScriptCall>),
    /// Terminal outcome, including script errors and resource-limit failures.
    Done(ExecutionReport),
}

/// A script cannot start or its recorded history does not match.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// Invalid limits or source, or worker startup failure.
    #[error(transparent)]
    Execution(#[from] crate::ExecutionError),
    /// A catalog input schema cannot be compiled.
    #[error(transparent)]
    InputSchema(#[from] crate::InputSchemaError),
    /// History differs from the script, or contains duplicate call ordinals.
    #[error("script replay diverged from its recorded calls")]
    Diverged,
    /// Returning without awaiting a new tool call is not a durable operation.
    #[error("durable scripts must await every new tool call")]
    UnawaitedCall,
    /// The worker stopped without a terminal outcome.
    #[error("script worker stopped without an outcome")]
    WorkerStopped,
}

/// Run from the beginning, replay recorded results, and stop before new I/O.
/// The caller must version the source, catalog, limits, and engine together.
/// `admitted` contains all accepted calls, including queued and pending calls.
/// `history` is the delivery journal in completion order; it may be sparse.
/// Terminal reports for admitted calls without results are provisional: drain
/// retained futures and replay with their results to record discarded replies.
/// This function has no dispatcher and cannot invoke a host tool.
pub async fn step(
    code: &str,
    catalog: &Catalog,
    limits: Limits,
    admitted: &[ScriptCall],
    history: &[SettledCall],
) -> Result<ScriptStep, ReplayError> {
    limits.validate().map_err(crate::ExecutionError::from)?;
    if code.len() > limits.source_bytes {
        return Err(crate::ExecutionError::SourceTooLarge {
            size: code.len(),
            limit: limits.source_bytes,
        }
        .into());
    }
    let mut state = ReplayState::new(catalog, limits, admitted, history)?;
    let (worker, mut host) = bridge::channels(&limits);
    let handle = crate::runtime::quickjs::spawn(
        WorkerConfig {
            code: code.into(),
            catalog: Arc::new(catalog.clone()),
            limits,
            deadline: Instant::now() + limits.wall_time,
        },
        worker,
    )
    .map_err(crate::ExecutionError::from)?;
    let stop = bridge::CancelOnDrop(host.cancel.clone());
    let result = state.drive(&mut host).await;
    drop(stop);
    drop(host.replies);
    let _ = handle.join();
    result
}

struct RecordedCall<'a> {
    call: &'a ScriptCall,
    result: Option<&'a ToolResult>,
    issued: bool,
}

struct Journal<'a> {
    calls: BTreeMap<u32, RecordedCall<'a>>,
    remaining: &'a [SettledCall],
}

impl<'a> Journal<'a> {
    fn new(
        admitted: &'a [ScriptCall],
        history: &'a [SettledCall],
        max_calls: u32,
    ) -> Result<Self, ReplayError> {
        if admitted.len() > max_calls as usize {
            return Err(ReplayError::Diverged);
        }
        let mut calls = BTreeMap::new();
        for call in admitted {
            let recorded = RecordedCall {
                call,
                result: None,
                issued: false,
            };
            if calls.insert(call.ordinal, recorded).is_some() {
                return Err(ReplayError::Diverged);
            }
        }
        for item in history {
            let recorded = calls
                .get_mut(&item.call.ordinal)
                .ok_or(ReplayError::Diverged)?;
            if recorded.call != &item.call || recorded.result.replace(&item.result).is_some() {
                return Err(ReplayError::Diverged);
            }
        }
        Ok(Self {
            calls,
            remaining: history,
        })
    }

    fn observe(&mut self, call: &ScriptCall) -> Result<(), ReplayError> {
        if let Some(recorded) = self.calls.get_mut(&call.ordinal) {
            if recorded.call != call {
                return Err(ReplayError::Diverged);
            }
            recorded.issued = true;
        } else if self
            .calls
            .last_key_value()
            .is_some_and(|(&last, _)| call.ordinal <= last)
        {
            return Err(ReplayError::Diverged);
        }
        Ok(())
    }

    fn verify_issued(&self) -> Result<(), ReplayError> {
        if self.calls.values().any(|call| !call.issued) {
            return Err(ReplayError::Diverged);
        }
        Ok(())
    }
}

struct ReplayState<'a> {
    journal: Journal<'a>,
    validators: BTreeMap<String, jsonschema::Validator>,
    pending: BTreeMap<u32, ScriptCall>,
    local: BTreeMap<u32, ToolResult>,
    records: BTreeMap<u32, CallRecord>,
    limits: Limits,
}

impl<'a> ReplayState<'a> {
    fn new(
        catalog: &Catalog,
        limits: Limits,
        admitted: &'a [ScriptCall],
        history: &'a [SettledCall],
    ) -> Result<Self, ReplayError> {
        let journal = Journal::new(admitted, history, limits.max_calls)?;
        let validators = catalog
            .entries()
            .iter()
            .map(|entry| {
                jsonschema::validator_for(entry.input_schema())
                    .map(|validator| (entry.name().to_owned(), validator))
                    .map_err(|error| crate::InputSchemaError {
                        name: entry.name().into(),
                        reason: error.to_string(),
                    })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            journal,
            validators,
            pending: BTreeMap::new(),
            local: BTreeMap::new(),
            records: BTreeMap::new(),
            limits,
        })
    }

    async fn drive(&mut self, host: &mut HostChannels) -> Result<ScriptStep, ReplayError> {
        let mut requests_open = true;
        loop {
            tokio::select! {
                biased;
                request = host.requests.recv(), if requests_open => match request {
                    None => requests_open = false,
                    Some(Request::Call { ordinal, name, arguments, raw }) => {
                        let call = ScriptCall {
                            ordinal, name, raw,
                            arguments: serde_json::from_str(&arguments)
                                .map_err(|_| ReplayError::Diverged)?,
                        };
                        self.accept(call)?;
                    }
                    Some(Request::Rejected { ordinal, name, kind }) => {
                        if self.journal.calls.contains_key(&ordinal) { return Err(ReplayError::Diverged); }
                        let mut record = CallRecord::new(ordinal, name, CallStatus::Rejected, ScriptDelivery::Delivered);
                        record.error_kind = Some(kind);
                        self.records.insert(ordinal, record);
                    }
                    Some(Request::Idle) => {
                        if let Some(step) = self.idle(&host.replies)? {
                            return Ok(step);
                        }
                    }
                },
                outcome = &mut host.done => {
                    let outcome = outcome.map_err(|_| ReplayError::WorkerStopped)?;
                    return self.finish(outcome);
                }
            }
        }
    }

    fn accept(&mut self, call: ScriptCall) -> Result<(), ReplayError> {
        if self
            .validators
            .get(&call.name)
            .is_some_and(|validator| validator.is_valid(&call.arguments))
        {
            self.journal.observe(&call)?;
        } else {
            if self.journal.calls.contains_key(&call.ordinal) {
                return Err(ReplayError::Diverged);
            }
            self.local.insert(
                call.ordinal,
                ToolResult::failed(ToolExecutionError::invalid_args(
                    "tool arguments do not match the declared input schema",
                )),
            );
        }
        self.pending.insert(call.ordinal, call);
        Ok(())
    }

    fn has_new_calls(&self) -> bool {
        self.pending
            .keys()
            .any(|id| !self.local.contains_key(id) && !self.journal.calls.contains_key(id))
    }

    fn unresolved(&mut self) -> Result<ScriptStep, ReplayError> {
        self.journal.verify_issued()?;
        let calls = std::mem::take(&mut self.pending)
            .into_values()
            .filter(|call| !self.local.contains_key(&call.ordinal))
            .collect();
        Ok(ScriptStep::Calls(calls))
    }

    fn idle(
        &mut self,
        replies: &std::sync::mpsc::Sender<Reply>,
    ) -> Result<Option<ScriptStep>, ReplayError> {
        if self.journal.remaining.is_empty() && self.has_new_calls() {
            return self.unresolved().map(Some);
        }
        let next = if let Some((&ordinal, result)) = self.local.first_key_value() {
            Some((ordinal, result, false))
        } else {
            self.journal
                .remaining
                .first()
                .map(|item| (item.call.ordinal, &item.result, true))
        };
        let Some((ordinal, result, journal)) = next else {
            self.journal.verify_issued()?;
            if self.pending.is_empty() {
                return Err(ReplayError::WorkerStopped);
            }
            return self.unresolved().map(Some);
        };
        let call = self.pending.get(&ordinal).ok_or(ReplayError::Diverged)?;
        let payload = bridge::project(&call.name, result, call.raw, self.limits.message_bytes);
        let (payload, delivery) =
            bridge::bound_payload(&call.name, payload, self.limits.message_bytes);
        let (status, error_kind) = bridge::status_of(result);
        let mut record = CallRecord::new(ordinal, &call.name, status, delivery);
        record.error_kind = error_kind;
        self.records.insert(ordinal, record);
        replies
            .send(Reply { ordinal, payload })
            .map_err(|_| ReplayError::WorkerStopped)?;
        self.pending.remove(&ordinal);
        self.local.remove(&ordinal);
        if journal {
            self.journal.remaining = &self.journal.remaining[1..];
        }
        Ok(None)
    }

    fn finish(&mut self, outcome: WorkerOutcome) -> Result<ScriptStep, ReplayError> {
        self.journal.verify_issued()?;
        if self.has_new_calls() {
            return Err(ReplayError::UnawaitedCall);
        }
        for (ordinal, call) in std::mem::take(&mut self.pending) {
            let result = self
                .journal
                .calls
                .get(&ordinal)
                .and_then(|recorded| recorded.result)
                .or_else(|| self.local.get(&ordinal));
            let record = if let Some(result) = result {
                let (status, error_kind) = bridge::status_of(result);
                let mut record =
                    CallRecord::new(ordinal, call.name, status, ScriptDelivery::Discarded);
                record.error_kind = error_kind;
                record
            } else {
                CallRecord::new(
                    ordinal,
                    call.name,
                    CallStatus::NotStarted,
                    ScriptDelivery::None,
                )
            };
            self.records.insert(ordinal, record);
        }
        let mut report = ExecutionReport::new(outcome.status);
        report.output = outcome.output;
        report.returned = outcome.returned;
        report.diagnostic = outcome.diagnostic;
        report.over_budget_calls = outcome.over_budget_calls;
        report.calls = std::mem::take(&mut self.records).into_values().collect();
        Ok(ScriptStep::Done(report))
    }
}
