//! Backend-neutral script configuration and durable delivery scheduling.
//! Backend adapters persist step inputs/results and execute individual calls.

use std::{collections::VecDeque, future::Future, task::Poll};

use futures::future::poll_fn;
use rig::{
    completion::ToolDefinition,
    tool::{ToolExecutionError, ToolResult},
};
use rig_codemode::{
    Catalog, CatalogEntry, ExecutionReport, Limits,
    replay::{ScriptCall, ScriptStep, SettledCall},
};
use serde::{Deserialize, Serialize};

#[cfg(feature = "duroxide")]
pub(crate) mod duroxide;

#[derive(Clone, Serialize, Deserialize)]
pub struct ScriptInput {
    pub code: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StepInput {
    pub code: String,
    pub admitted: Vec<ScriptCall>,
    pub history: Vec<SettledCall>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CodeModeConfig {
    pub limits: Limits,
    pub definitions: Vec<ToolDefinition>,
    /// Planner and durable host projection identity captured at construction.
    pub runtime_identity: String,
}

impl CodeModeConfig {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn native(limits: Limits, definitions: Vec<ToolDefinition>) -> Self {
        use rig_codemode::replay::NativeReplayPlanner;
        Self {
            limits,
            definitions,
            runtime_identity: Self::runtime_identity(&NativeReplayPlanner),
        }
    }

    pub fn runtime_identity(planner: &dyn rig_codemode::replay::ReplayPlanner) -> String {
        format!("{}/rig-durable/projection-v1", planner.runtime_identity())
    }

    fn catalog(&self) -> Result<Catalog, String> {
        let catalog = Catalog::new(self.definitions.iter().map(CatalogEntry::from_definition))
            .map_err(|error| error.to_string())?;
        if catalog.contains("codemode") {
            return Err("a nested tool cannot have the name codemode".into());
        }
        Ok(catalog)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.runtime_identity.is_empty() {
            return Err("missing code-mode runtime identity".into());
        }
        self.limits.validate().map_err(|error| error.to_string())?;
        self.catalog()?;
        Ok(())
    }

    pub fn definition(&self) -> Result<ToolDefinition, String> {
        Ok(ToolDefinition {
            annotations: Default::default(),
            name: rig::message::ToolName::new("codemode").map_err(|error| error.to_string())?,
            description: format!(
                "Run a durable JavaScript async function body. Call tools with await tools[name](args). \
                 Use Promise.all for independent sub-agents, or Promise.race for the first settled result. \
                 Results follow recorded host delivery order. Race losers are not cancelled; the host \
                 waits for all admitted calls before returning the report. Use answers to branch or start \
                 later calls. text(value) emits output; return supplies a JSON value. searchTools and \
                 describeTool discover tools. Date, Math.random, timers, network, and filesystem are \
                 unavailable. Await calls or compose them in awaited promises. Returns an execution \
                 report; check its status.\n\n{}",
                self.catalog()?.render_declarations(8192)
            ),
            parameters: serde_json::json!({"type":"object", "properties":{"code":{"type":"string"}},
                "required":["code"], "additionalProperties":false}),
        })
    }

    /// Native QuickJS convenience planner. Portable adapters supply a planner.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn step(&self, input: StepInput) -> Result<ScriptStep, String> {
        use rig_codemode::replay::{NativeReplayPlanner, ReplayInput, ReplayPlanner};
        let planner = NativeReplayPlanner;
        if self.runtime_identity != Self::runtime_identity(&planner) {
            return Err("incompatible code-mode runtime identity".into());
        }
        planner
            .plan(ReplayInput {
                code: &input.code,
                catalog: &self.catalog()?,
                limits: self.limits,
                admitted: &input.admitted,
                history: &input.history,
            })
            .await
            .map_err(|error| error.to_string())
    }
}

/// The next adapter operation. Persist state before execution or planning.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SchedulerAction {
    Plan,
    Execute,
    /// Persist the drained history through the planner; ignore its output.
    Checkpoint {
        error: String,
    },
    Complete(Result<ExecutionReport, String>),
}

/// Serializable scheduling decisions, without futures or backend handles.
///
/// Adapters own persistence and effects. Save admissions before starting them,
/// observations before delivery, and delivery before replaying the script.
/// Resume retained [`Self::active`] calls after reopen without admitting them
/// again. Record simultaneous observations in admission order, and deliver
/// retained ready results before observing more completions. Reserve all free
/// execution slots with [`Self::admit_next`] before each ready delivery.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchedulerState {
    input: StepInput,
    limits: Limits,
    queued: VecDeque<ScriptCall>,
    active: Vec<ScriptCall>,
    ready: VecDeque<SettledCall>,
    action: SchedulerAction,
    draining: bool,
    failure: Option<String>,
}

impl SchedulerState {
    pub fn new(code: String, limits: Limits) -> Result<Self, String> {
        limits.validate().map_err(|error| error.to_string())?;
        Ok(Self {
            input: StepInput {
                code,
                admitted: Vec::new(),
                history: Vec::new(),
            },
            limits,
            queued: VecDeque::new(),
            active: Vec::new(),
            ready: VecDeque::new(),
            action: SchedulerAction::Plan,
            draining: false,
            failure: None,
        })
    }

    pub fn input(&self) -> &StepInput {
        &self.input
    }

    pub fn queued(&self) -> &VecDeque<ScriptCall> {
        &self.queued
    }

    /// Admission-ordered operations that occupy slots, including after reopen.
    pub fn active(&self) -> &[ScriptCall] {
        &self.active
    }

    /// Host-observed completions in delivery order. These still occupy slots.
    pub fn ready(&self) -> &VecDeque<SettledCall> {
        &self.ready
    }

    pub fn action(&self) -> &SchedulerAction {
        &self.action
    }

    pub fn is_draining(&self) -> bool {
        self.draining
    }

    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    fn has_work(&self) -> bool {
        !self.queued.is_empty() || !self.active.is_empty() || !self.ready.is_empty()
    }

    /// Apply a persisted planner result only when [`Self::action`] is `Plan`.
    /// New calls cause another plan before effects. A `Done` report is
    /// provisional while work remains: drain, then plan the final history.
    pub fn apply_plan(&mut self, result: Result<ScriptStep, String>) -> &SchedulerAction {
        if !matches!(self.action, SchedulerAction::Plan) {
            return &self.action;
        }
        self.draining = false;
        self.action = match result {
            Err(error) if !self.has_work() => SchedulerAction::Complete(Err(error)),
            Err(error) => {
                self.failure = Some(error);
                self.draining = true;
                SchedulerAction::Execute
            }
            Ok(ScriptStep::Done(report)) if !self.has_work() => {
                SchedulerAction::Complete(Ok(report))
            }
            Ok(ScriptStep::Done(_)) => {
                self.draining = true;
                SchedulerAction::Execute
            }
            Ok(ScriptStep::Calls(calls)) => {
                let admitted_before = self.input.admitted.len();
                for call in calls {
                    if !self
                        .input
                        .admitted
                        .iter()
                        .any(|item| item.ordinal == call.ordinal)
                    {
                        self.input.admitted.push(call.clone());
                        self.queued.push_back(call);
                    }
                }
                if self.input.admitted.len() != admitted_before {
                    SchedulerAction::Plan
                } else if self.has_work() {
                    SchedulerAction::Execute
                } else {
                    SchedulerAction::Complete(Err(
                        "script is waiting without an admitted call".into()
                    ))
                }
            }
        };
        &self.action
    }

    /// Reserve one execution slot. Ready results count toward the limit.
    pub fn admit_next(&mut self) -> Option<ScriptCall> {
        if !matches!(self.action, SchedulerAction::Execute)
            || self.active.len() + self.ready.len() >= self.limits.max_in_flight as usize
        {
            return None;
        }
        let call = self.queued.pop_front()?;
        self.active.push(call.clone());
        Some(call)
    }

    /// Record a policy-filtered completion in host-observed order.
    /// Returns `false` for a ready or delivered duplicate (first result wins).
    /// Unknown or queued ordinals return an error without changing state.
    /// Completing during planning does not move ahead of retained ready results.
    pub fn complete(
        &mut self,
        ordinal: u32,
        result: Result<ToolResult, String>,
    ) -> Result<bool, String> {
        if self.ready.iter().any(|item| item.call.ordinal == ordinal)
            || self
                .input
                .history
                .iter()
                .any(|item| item.call.ordinal == ordinal)
        {
            return Ok(false);
        }
        let index = self
            .active
            .iter()
            .position(|call| call.ordinal == ordinal)
            .ok_or_else(|| format!("script completion {ordinal} is not active"))?;
        let call = self.active.remove(index);
        self.ready.push_back(SettledCall {
            call,
            result: result.unwrap_or_else(|error| {
                ToolResult::failed(
                    ToolExecutionError::other(error)
                        .with_model_feedback("The nested durable tool failed.")
                        .with_retryable(false),
                )
            }),
        });
        Ok(true)
    }

    /// Deliver one retained observation. While draining, continue execution
    /// until all queued, active, and ready work is gone; otherwise replan.
    pub fn deliver_next(&mut self) -> Option<SettledCall> {
        if !matches!(self.action, SchedulerAction::Execute) {
            return None;
        }
        let settled = self.ready.pop_front()?;
        self.input.history.push(settled.clone());
        if !self.draining || !self.has_work() {
            self.action = match &self.failure {
                Some(error) => SchedulerAction::Checkpoint {
                    error: error.clone(),
                },
                None => SchedulerAction::Plan,
            };
        }
        Some(settled)
    }

    /// Finish the failure checkpoint regardless of its result. It cannot admit
    /// more calls or replace the original planning error.
    pub fn finish_checkpoint(&mut self) {
        if let SchedulerAction::Checkpoint { error } = &self.action {
            self.action = SchedulerAction::Complete(Err(error.clone()));
        }
    }
}

/// Schedule a script through backend-supplied plan and tool operations.
/// `plan` must persist its input and output before returning and run a replay
/// planner outside workflow code. `execute` must schedule
/// a durable operation for each call, not perform external I/O in workflow code.
/// The plan activity persists the selected delivery before the guest sees it.
/// Fixed-order polling breaks ready-result ties without waker-order dependence.
/// Pending futures remain owned here, including after a guest race terminates.
/// On a planning failure, drain admitted calls and checkpoint the final history
/// through `plan` before returning the original error. Ignore that checkpoint's
/// output: it cannot admit further calls.
/// An execution `Err` is a host failure and uses the same drain path. Ordinary
/// tool failures must return `Ok(ToolResult::failed(...))`.
pub async fn run<P, PF, E, EF>(
    code: String,
    limits: Limits,
    plan: P,
    execute: E,
) -> Result<ExecutionReport, String>
where
    P: Fn(StepInput) -> PF,
    PF: Future<Output = Result<ScriptStep, String>>,
    E: Fn(ScriptCall) -> EF,
    EF: Future<Output = Result<ToolResult, String>>,
{
    let mut state = SchedulerState::new(code, limits)?;
    let mut active = Vec::new();
    let mut failure: Option<String> = None;
    loop {
        match state.action().clone() {
            SchedulerAction::Plan => {
                let result = plan(state.input().clone()).await;
                state.apply_plan(match &failure {
                    Some(error) => Err(error.clone()),
                    None => result,
                });
                continue;
            }
            SchedulerAction::Complete(result) => return result,
            SchedulerAction::Checkpoint { .. } => {
                let _ = plan(state.input().clone()).await;
                state.finish_checkpoint();
                continue;
            }
            SchedulerAction::Execute => {}
        }
        while let Some(call) = state.admit_next() {
            let ordinal = call.ordinal;
            active.push((ordinal, Box::pin(execute(call))));
        }
        if state.ready().is_empty() {
            let completions = poll_fn(|cx| {
                let mut completions = Vec::new();
                let mut index = 0;
                while index < active.len() {
                    if let Poll::Ready(result) = active[index].1.as_mut().poll(cx) {
                        let (ordinal, _) = active.remove(index);
                        completions.push((ordinal, result));
                    } else {
                        index += 1;
                    }
                }
                if completions.is_empty() {
                    Poll::Pending
                } else {
                    Poll::Ready(completions)
                }
            })
            .await;
            for (ordinal, result) in completions {
                if let Err(error) = &result {
                    failure.get_or_insert_with(|| error.clone());
                }
                state.complete(ordinal, result)?;
            }
        }
        state.deliver_next();
    }
}

#[cfg(any(feature = "duroxide", feature = "temporal"))]
pub(crate) fn activity_result(
    output: crate::ToolActivityOutput,
    name: &str,
) -> Result<ToolResult, String> {
    let durable = output.result;
    Ok(durable.result.unwrap_or_else(|| {
        ToolResult::failed(
            ToolExecutionError::other(crate::result::interrupted_feedback(name))
                .with_retryable(false),
        )
    }))
}
