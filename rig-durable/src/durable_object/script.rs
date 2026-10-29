//! SQL adapter for the shared scheduler. Only local effect futures live here.

use std::{rc::Rc, task::Poll};

use futures::{
    FutureExt,
    future::{LocalBoxFuture, poll_fn},
};
use rig::tool::{ToolExecutionError, ToolOutput, ToolResult};
use rig_codemode::{
    Catalog, CatalogEntry, Limits,
    replay::{ReplayInput, ReplayPlanner, ScriptCall},
};
use rig_core::tool::ToolResultContext;
use serde::{Deserialize, Serialize};

use super::{
    Builder, Call, Engine, Error, STORAGE_FAILURE, SqlDatabase, Wake, bounded_call_output,
    child::ChildInvocation, storage, storage_failure_output,
};
use crate::{
    AttemptMetadata, DurableAgentConfig, DurableToolResult, InterruptionReason, LogicalCallKey,
    ToolActivityInput, ToolActivityOutput, ToolInvocation, ToolPolicy,
    codemode::{CodeModeConfig, SchedulerAction, SchedulerState, ScriptInput},
    config::ToolSnapshot,
    tools::{ToolCatalog, ToolEntry, ToolRoute},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ScriptDefinition {
    pub runtime_identity: String,
    pub config: CodeModeConfig,
    pub tools: Vec<ToolSnapshot>,
}

impl ScriptDefinition {
    fn digest(&self) -> Result<String, Error> {
        Ok(crate::identity::arguments_digest(&serde_json::to_value(
            self,
        )?)?)
    }
}

pub(super) struct Registration {
    pub limits: Limits,
    pub planner: Rc<dyn ReplayPlanner>,
}

pub(super) struct Runtime {
    pub definition: ScriptDefinition,
    pub planner: Rc<dyn ReplayPlanner>,
}

impl Builder {
    /// Expose tools and children only through the durable `codemode` tool.
    /// The planner runs replay, not effects. Keep its runtime identity and
    /// registered tool versions available while their histories remain active.
    pub fn code_mode(mut self, limits: Limits, planner: impl ReplayPlanner + 'static) -> Self {
        self.code_mode = Some(Registration {
            limits,
            planner: Rc::new(planner),
        });
        self
    }

    pub(super) fn prepare_script(&self) -> Result<(DurableAgentConfig, Option<Runtime>), Error> {
        let mut config = self.config.clone();
        let Some(registration) = &self.code_mode else {
            return Ok((config, None));
        };
        if registration.planner.runtime_identity().is_empty() {
            return Err(Error::Invalid("script runtime identity is empty".into()));
        }
        for entry in config.tools.0.values() {
            if entry.requires_approval {
                return Err(Error::Invalid("nested approvals are not supported".into()));
            }
            if entry.tag.is_some() || entry.retry.timeout.is_some() {
                return Err(Error::Invalid(
                    "Durable Objects do not support tool worker tags or timeouts".into(),
                ));
            }
        }
        let code = CodeModeConfig {
            limits: registration.limits,
            definitions: config.tools.definitions(),
        };
        code.validate().map_err(Error::Invalid)?;
        let definition = ScriptDefinition {
            runtime_identity: registration.planner.runtime_identity().into(),
            config: code,
            tools: config.snapshot().tools,
        };
        config.tools = ToolCatalog::default();
        config.tools.insert(ToolEntry {
            definition: definition.config.definition().map_err(Error::Invalid)?,
            route: ToolRoute::RigTool,
            retry: crate::RetryPolicy::new(1),
            tag: None,
            requires_approval: false,
            policy: ToolPolicy::idempotent().implementation_version(definition.digest()?),
        });
        Ok((
            config,
            Some(Runtime {
                definition,
                planner: registration.planner.clone(),
            }),
        ))
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ScriptRecord {
    parent: LogicalCallKey,
    definition: ScriptDefinition,
    scheduler: SchedulerState,
}

#[derive(Clone, Serialize, Deserialize)]
struct ScriptEffect {
    call: ScriptCall,
    input: ToolActivityInput,
    child: Option<ChildInvocation>,
    started: bool,
    output: Option<ToolActivityOutput>,
    attempt: u32,
    deadline_ms: Option<u64>,
}

fn script_key(parent: &LogicalCallKey) -> String {
    format!("script:{}", parent.canonical())
}
fn effect_key(parent: &LogicalCallKey, ordinal: u32) -> String {
    format!("{};effect:{ordinal}", script_key(parent))
}

fn script_result(output: &ToolActivityOutput, name: &str) -> Result<ToolResult, Error> {
    let durable = output
        .result
        .as_ref()
        .ok_or_else(|| Error::Invalid("script effect result is missing".into()))?;
    Ok(durable.result.clone().unwrap_or_else(|| {
        ToolResult::failed(
            ToolExecutionError::other(crate::result::interrupted_feedback(name))
                .with_retryable(false),
        )
    }))
}

fn drain_capacity_error(error: Error) -> Error {
    match error {
        error @ Error::Limit { .. } => Error::Storage(format!(
            "script cannot retain its drain checkpoint: {error}; increase the checkpoint limit"
        )),
        error @ Error::Depth { .. } => Error::Storage(format!(
            "script cannot retain its drain checkpoint: {error}; migrate the retained checkpoint to restore depth headroom"
        )),
        error => error,
    }
}

impl<D: SqlDatabase, W: Wake> Engine<D, W> {
    pub(super) fn check_script_registration(&self, call: &Call) -> Result<(), Error> {
        let Some(parent) = &call.input.invocation.logical_key else {
            return Ok(());
        };
        let Some(record) = storage::read::<ScriptRecord>(&self.db, &script_key(parent))? else {
            return Ok(());
        };
        let runtime = self
            .script
            .as_ref()
            .ok_or_else(|| Error::Incompatible("script planner is not registered".into()))?;
        if record.definition.digest()? != runtime.definition.digest()? {
            return Err(Error::Incompatible(
                "script runtime, catalog, or configuration mismatch".into(),
            ));
        }
        // Preflight every retained script before any batch member starts.
        self.check_script_capacity(&record)
            .map_err(drain_capacity_error)?;
        Ok(())
    }

    /// Committed scheduler state for one outer model tool call. A planner
    /// wrapper sees this same delivery prefix before the next replay step.
    pub fn script_state(&self, parent: &LogicalCallKey) -> Result<Option<SchedulerState>, Error> {
        Ok(
            storage::read::<ScriptRecord>(&self.db, &script_key(parent))?
                .map(|record| record.scheduler),
        )
    }

    fn script_record(&self, key: &str) -> Result<ScriptRecord, Error> {
        storage::read(&self.db, key)?
            .ok_or_else(|| Error::Storage("script checkpoint is missing".into()))
    }

    fn save_script(&self, key: &str, record: &ScriptRecord) -> Result<(), Error> {
        storage::write(&self.db, key, record, self.max_bytes)
    }

    fn check_script_capacity(&self, record: &ScriptRecord) -> Result<(), Error> {
        let encoded = storage::encode(record, self.max_bytes)?;
        let bytes = encoded.len();
        let state = &record.scheduler;
        // Bound the payload itself, independent of its current position.
        // Arguments/results can move through ScriptEffect, SettledCall,
        // StepInput, SchedulerAction and the outer Call/report. Those fixed
        // envelopes use fewer than 32 containers; encode reserves another
        // 32 below serde's recursion guard. Never spend future headroom just
        // because a payload currently sits in a shallower queued position.
        for call in state
            .queued()
            .iter()
            .chain(state.active())
            .chain(&state.input().admitted)
        {
            storage::check_depth(&serde_json::to_string(&call.arguments)?, 64)?;
        }
        for settled in state.ready().iter().chain(&state.input().history) {
            storage::check_depth(&serde_json::to_string(&settled.call.arguments)?, 64)?;
            storage::check_depth(&serde_json::to_string(&settled.result)?, 64)?;
        }
        if let SchedulerAction::Complete(Ok(report)) = state.action() {
            storage::check_depth(&serde_json::to_string(report)?, 64)?;
        }
        let current_control =
            serde_json::to_vec(&(state.action(), state.failure(), state.is_draining()))?.len();
        let mut control = current_control;
        for message in std::iter::once(STORAGE_FAILURE).chain(state.failure()) {
            for action in [
                SchedulerAction::Execute,
                SchedulerAction::Checkpoint {
                    error: message.into(),
                },
                SchedulerAction::Complete(Err(message.into())),
            ] {
                control = control.max(serde_json::to_vec(&(action, Some(message), false))?.len());
            }
        }
        let nonempty = [
            !state.queued().is_empty(),
            !state.active().is_empty(),
            !state.ready().is_empty(),
            !state.input().history.is_empty(),
        ]
        .into_iter()
        .filter(|value| *value)
        .count();
        // Normalize comma space for merging all work into the delivery journal.
        let mut required = bytes
            .saturating_sub(current_control)
            .saturating_add(control)
            .saturating_add(nonempty.saturating_sub(1));
        for call in state.queued().iter().chain(state.active()) {
            let mut effect = self.admit_script_effect(record, call.clone())?;
            let mut result_bytes = 0;
            for interruption in [None, Some(InterruptionReason::ToolUnavailable)] {
                let output = storage_failure_output(&call.name, interruption);
                result_bytes = result_bytes
                    .max(serde_json::to_vec(&script_result(&output, &call.name)?)?.len());
                effect.output = Some(output);
                // Reserve scalar growth in attempt counters and retry deadlines.
                storage::encode(&effect, storage::ROW_BYTES - 256)?;
            }
            // A call moves from queued/active into one SettledCall. Its JSON
            // envelope and array redistribution need less than 64 bytes.
            required = required.saturating_add(result_bytes).saturating_add(64);
        }
        if required > self.max_bytes {
            return Err(Error::Limit {
                bytes: required,
                max_bytes: self.max_bytes,
            });
        }
        Ok(())
    }

    fn script_effect(&self, parent: &LogicalCallKey, ordinal: u32) -> Result<ScriptEffect, Error> {
        let rows = self.db.exec(
            "SELECT data FROM rig_script_effects WHERE key = ?",
            &[effect_key(parent, ordinal)],
        )?;
        let row = rows
            .first()
            .ok_or_else(|| Error::Storage("script effect is missing".into()))?;
        Ok(serde_json::from_str(storage::text(row)?)?)
    }

    fn save_script_effect(
        &self,
        parent: &LogicalCallKey,
        effect: &ScriptEffect,
    ) -> Result<(), Error> {
        self.db.exec("INSERT INTO rig_script_effects (key, data) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET data = excluded.data",
            &[effect_key(parent, effect.call.ordinal), storage::encode(effect, storage::ROW_BYTES)?])?;
        Ok(())
    }

    pub(super) fn script_deadline(&self, call: &Call) -> Result<Option<u64>, Error> {
        let Some(parent) = &call.input.invocation.logical_key else {
            return Ok(None);
        };
        let Some(record) = storage::read::<ScriptRecord>(&self.db, &script_key(parent))? else {
            return Ok(None);
        };
        let mut deadline = None;
        for call in record.scheduler.active() {
            if let Some(value) = self.script_effect(parent, call.ordinal)?.deadline_ms {
                deadline = Some(deadline.map_or(value, |d: u64| d.min(value)));
            }
        }
        Ok(deadline)
    }

    fn admit_script_effect(
        &self,
        record: &ScriptRecord,
        call: ScriptCall,
    ) -> Result<ScriptEffect, Error> {
        let entry = record
            .definition
            .tools
            .iter()
            .find(|entry| entry.definition.name == call.name)
            .ok_or_else(|| Error::Invalid("script tool is not registered".into()))?;
        let key = LogicalCallKey {
            logical_execution_id: format!("script:{}", record.parent.canonical()),
            submission_id: record.parent.submission_id.clone(),
            model_turn: record.parent.model_turn,
            call_index: call.ordinal as usize,
        };
        let input = ToolActivityInput {
            name: call.name.clone(),
            arguments: serde_json::to_string(&call.arguments)?,
            invocation: ToolInvocation {
                execution_id: record.parent.logical_execution_id.clone(),
                prompt_index: 0,
                turn: record.parent.model_turn,
                call_index: call.ordinal as usize,
                logical_key: Some(key),
                attempt: Some(AttemptMetadata {
                    backend_execution_id: record.parent.logical_execution_id.clone(),
                    activity_attempt: Some(1),
                }),
            },
            policy: Some(entry.policy.clone()),
        };
        let child = self.children.get(&call.name).map(|child| {
            ChildInvocation::new(
                &record.parent,
                Some(u64::from(call.ordinal)),
                child.definition.clone(),
                input.arguments.clone(),
            )
        });
        Ok(ScriptEffect {
            call,
            input,
            child,
            started: false,
            output: None,
            attempt: 1,
            deadline_ms: None,
        })
    }

    /// Resume the shared scheduler. An invocation retains live local effects
    /// while planning fast branches. SQL retains every decision across eviction.
    pub(super) async fn script(&self, outer: &Call) -> Result<Option<ToolActivityOutput>, Error> {
        self.check_call_capacity(outer)?;
        let runtime = self
            .script
            .as_ref()
            .ok_or_else(|| Error::Incompatible("script planner is not registered".into()))?;
        let parent = outer
            .input
            .invocation
            .logical_key
            .clone()
            .ok_or_else(|| Error::Invalid("script parent identity is missing".into()))?;
        let key = script_key(&parent);
        let input: ScriptInput = match serde_json::from_str(&outer.input.arguments) {
            Ok(input) => input,
            Err(error) => {
                let (result, _) = DurableToolResult::from_rig(
                    ToolResult::failed(
                        ToolExecutionError::invalid_args(error.to_string()).with_retryable(false),
                    ),
                    &ToolResultContext::default(),
                    &crate::policy::MetadataRetention::none(),
                );
                return Ok(Some(bounded_call_output(
                    outer,
                    ToolActivityOutput::from_result(&outer.input.name, result),
                )?));
            }
        };
        let creation = self.db.transaction(|| {
            if storage::read::<ScriptRecord>(&self.db, &key)?.is_none() {
                let record = ScriptRecord {
                    parent: parent.clone(),
                    definition: runtime.definition.clone(),
                    scheduler: SchedulerState::new(input.code, runtime.definition.config.limits)
                        .map_err(Error::Invalid)?,
                };
                self.check_script_capacity(&record)?;
                self.save_script(&key, &record)?;
            }
            Ok(())
        });
        match creation {
            Err(Error::Limit { .. } | Error::Depth { .. }) => {
                return Ok(Some(bounded_call_output(
                    outer,
                    storage_failure_output(&outer.input.name, None),
                )?));
            }
            result => result?,
        }
        type Pending<'a> = (
            u32,
            LocalBoxFuture<'a, (ScriptEffect, Result<Option<ToolActivityOutput>, String>)>,
        );
        let mut pending: Vec<Pending<'_>> = Vec::new();
        loop {
            let mut record = self.script_record(&key)?;
            if record.definition.digest()? != runtime.definition.digest()? {
                return Err(Error::Incompatible(
                    "script runtime, catalog, or configuration mismatch".into(),
                ));
            }
            self.check_script_capacity(&record)
                .map_err(drain_capacity_error)?;
            match record.scheduler.action().clone() {
                SchedulerAction::Plan | SchedulerAction::Checkpoint { .. } => {
                    let input = record.scheduler.input();
                    let catalog = Catalog::new(
                        record
                            .definition
                            .config
                            .definitions
                            .iter()
                            .map(CatalogEntry::from_definition),
                    )
                    .map_err(|e| Error::Invalid(e.to_string()))?;
                    let result = runtime
                        .planner
                        .plan(ReplayInput {
                            code: &input.code,
                            catalog: &catalog,
                            limits: record.definition.config.limits,
                            admitted: &input.admitted,
                            history: &input.history,
                        })
                        .await
                        .map_err(|e| e.to_string());
                    let before = record.clone();
                    if matches!(
                        record.scheduler.action(),
                        SchedulerAction::Checkpoint { .. }
                    ) {
                        record.scheduler.finish_checkpoint();
                    } else {
                        record.scheduler.apply_plan(result);
                    }
                    if let Err(error) = self.check_script_capacity(&record) {
                        if !matches!(error, Error::Limit { .. } | Error::Depth { .. }) {
                            return Err(error);
                        }
                        record = before;
                        record.scheduler.apply_plan(Err(STORAGE_FAILURE.into()));
                        self.check_script_capacity(&record)
                            .map_err(drain_capacity_error)?;
                    }
                    self.db.transaction(|| self.save_script(&key, &record))?;
                }
                SchedulerAction::Complete(result) => {
                    let result = match result {
                        Ok(report) => {
                            ToolResult::success(ToolOutput::json(serde_json::to_value(report)?))
                        }
                        Err(error) => ToolResult::failed(
                            ToolExecutionError::other(error).with_retryable(false),
                        ),
                    };
                    let (result, _) = DurableToolResult::from_rig(
                        result,
                        &ToolResultContext::default(),
                        &crate::policy::MetadataRetention::none(),
                    );
                    let output = ToolActivityOutput::from_result(&outer.input.name, result);
                    return Ok(Some(bounded_call_output(outer, output)?));
                }
                SchedulerAction::Execute => {
                    while let Some(call) = record.scheduler.admit_next() {
                        let effect = self.admit_script_effect(&record, call)?;
                        self.db.transaction(|| {
                            self.save_script_effect(&parent, &effect)?;
                            self.save_script(&key, &record)
                        })?;
                    }
                    if record.scheduler.deliver_next().is_some() {
                        self.db.transaction(|| self.save_script(&key, &record))?;
                        continue;
                    }
                    for call in record.scheduler.active().to_vec() {
                        if pending.iter().any(|(ordinal, _)| *ordinal == call.ordinal) {
                            continue;
                        }
                        let mut effect = self.script_effect(&parent, call.ordinal)?;
                        if effect
                            .deadline_ms
                            .is_some_and(|deadline| deadline > self.wake.now_ms())
                        {
                            continue;
                        }
                        let entry = record
                            .definition
                            .tools
                            .iter()
                            .find(|entry| entry.definition.name == call.name)
                            .ok_or_else(|| Error::Invalid("script tool is missing".into()))?;
                        if effect.started
                            && effect.child.is_none()
                            && !entry.policy.safety().permits_repeat()
                        {
                            effect.output = Some(ToolActivityOutput::from_result(
                                &call.name,
                                DurableToolResult::interrupted(
                                    InterruptionReason::ClaimHeld,
                                    "tool was interrupted and may have partially run",
                                ),
                            ));
                            self.commit_script_effect(&key, &parent, effect, Ok(None))?;
                            continue;
                        }
                        effect.started = true;
                        effect.deadline_ms = None;
                        if let Some(attempt) = &mut effect.input.invocation.attempt {
                            attempt.activity_attempt = Some(effect.attempt);
                        }
                        self.db
                            .transaction(|| self.save_script_effect(&parent, &effect))?;
                        let ordinal = call.ordinal;
                        pending.push((
                            ordinal,
                            async move {
                                let result = if let Some(child) = &mut effect.child {
                                    self.poll_child(&effect.call.name, child).await
                                } else {
                                    self.executor
                                        .execute_committed(effect.input.clone())
                                        .await
                                        .map(Some)
                                };
                                (effect, result)
                            }
                            .boxed_local(),
                        ));
                    }
                    // A recovered interrupted result may now be ready.
                    if !self.script_record(&key)?.scheduler.ready().is_empty() {
                        continue;
                    }
                    if pending.is_empty() {
                        return Ok(None);
                    }
                    pending.sort_by_key(|(ordinal, _)| {
                        record
                            .scheduler
                            .active()
                            .iter()
                            .position(|call| call.ordinal == *ordinal)
                    });
                    let deadline = self.script_deadline(outer)?;
                    let timer = async {
                        if let Some(deadline) = deadline {
                            self.wake
                                .delay(deadline.saturating_sub(self.wake.now_ms()))
                                .await;
                        } else {
                            futures::future::pending::<()>().await;
                        }
                    };
                    futures::pin_mut!(timer);
                    let observations = poll_fn(|cx| {
                        let mut ready = Vec::new();
                        let mut index = 0;
                        while index < pending.len() {
                            if let Poll::Ready(value) = pending[index].1.as_mut().poll(cx) {
                                drop(pending.remove(index));
                                ready.push(value);
                            } else {
                                index += 1;
                            }
                        }
                        if !ready.is_empty() {
                            Poll::Ready(ready)
                        } else if timer.as_mut().poll(cx).is_ready() {
                            Poll::Ready(Vec::new())
                        } else {
                            Poll::Pending
                        }
                    })
                    .await;
                    for (effect, result) in observations {
                        self.commit_script_effect(&key, &parent, effect, result)?;
                    }
                }
            }
        }
    }

    fn commit_script_effect(
        &self,
        key: &str,
        parent: &LogicalCallKey,
        mut effect: ScriptEffect,
        result: Result<Option<ToolActivityOutput>, String>,
    ) -> Result<(), Error> {
        self.db.transaction(|| {
            let mut record = self.script_record(key)?;
            match result {
                Ok(Some(output)) => effect.output = Some(output),
                Ok(None) if effect.output.is_none() => {
                    effect.deadline_ms = Some(self.wake.now_ms().saturating_add(2000))
                }
                Ok(None) => {}
                Err(error) => {
                    let entry = record
                        .definition
                        .tools
                        .iter()
                        .find(|entry| entry.definition.name == effect.call.name)
                        .ok_or_else(|| Error::Invalid("script tool is missing".into()))?;
                    if effect.attempt < entry.retry.max_attempts {
                        effect.deadline_ms = Some(
                            self.wake.now_ms().saturating_add(
                                entry
                                    .retry
                                    .to_policy()
                                    .delay_for_attempt(effect.attempt)
                                    .as_millis() as u64,
                            ),
                        );
                        effect.attempt += 1;
                    } else {
                        let (result, _) = DurableToolResult::from_rig(
                            ToolResult::failed(
                                ToolExecutionError::other(error)
                                    .with_model_feedback("The nested durable tool failed.")
                                    .with_retryable(false),
                            ),
                            &ToolResultContext::default(),
                            &crate::policy::MetadataRetention::none(),
                        );
                        effect.output =
                            Some(ToolActivityOutput::from_result(&effect.call.name, result));
                    }
                }
            }
            if let Some(output) = &effect.output {
                let before = record.scheduler.clone();
                record
                    .scheduler
                    .complete(
                        effect.call.ordinal,
                        Ok(script_result(output, &effect.call.name)?),
                    )
                    .map_err(Error::Invalid)?;
                let capacity = storage::encode(&effect, storage::ROW_BYTES)
                    .map(|_| ())
                    .and_then(|()| self.check_script_capacity(&record));
                if let Err(error) = capacity {
                    if !matches!(error, Error::Limit { .. } | Error::Depth { .. }) {
                        return Err(error);
                    }
                    let interruption = output
                        .result
                        .as_ref()
                        .and_then(|result| result.interruption.as_ref())
                        .map(|i| i.reason);
                    let output = storage_failure_output(&effect.call.name, interruption);
                    record.scheduler = before;
                    record
                        .scheduler
                        .complete(
                            effect.call.ordinal,
                            Ok(script_result(&output, &effect.call.name)?),
                        )
                        .map_err(Error::Invalid)?;
                    effect.output = Some(output);
                    self.check_script_capacity(&record)
                        .map_err(drain_capacity_error)?;
                }
            }
            self.save_script_effect(parent, &effect)?;
            self.save_script(key, &record)
        })
    }
}
