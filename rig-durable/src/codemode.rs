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

#[derive(Clone, Serialize, Deserialize)]
pub struct StepInput {
    pub code: String,
    pub admitted: Vec<ScriptCall>,
    pub history: Vec<SettledCall>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CodeModeConfig {
    pub limits: Limits,
    pub definitions: Vec<ToolDefinition>,
}

impl CodeModeConfig {
    fn catalog(&self) -> Result<Catalog, String> {
        let catalog = Catalog::new(self.definitions.iter().map(CatalogEntry::from_definition))
            .map_err(|error| error.to_string())?;
        if catalog.contains("codemode") {
            return Err("a nested tool cannot have the name codemode".into());
        }
        Ok(catalog)
    }

    pub fn validate(&self) -> Result<(), String> {
        self.limits.validate().map_err(|error| error.to_string())?;
        self.catalog()?;
        Ok(())
    }

    pub fn definition(&self) -> Result<ToolDefinition, String> {
        Ok(ToolDefinition {
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

    pub async fn step(&self, input: StepInput) -> Result<ScriptStep, String> {
        rig_codemode::replay::step(
            &input.code,
            &self.catalog()?,
            self.limits,
            &input.admitted,
            &input.history,
        )
        .await
        .map_err(|error| error.to_string())
    }
}

/// Schedule a script through backend-supplied plan and tool operations.
/// `plan` must persist its input and output before returning and run
/// [`CodeModeConfig::step`] outside workflow code. `execute` must schedule
/// a durable operation for each call, not perform external I/O in workflow code.
/// The plan activity persists the selected delivery before the guest sees it.
/// Fixed-order polling breaks ready-result ties without waker-order dependence.
/// Pending futures remain owned here, including after a guest race terminates.
/// On a planning failure, drain admitted calls and checkpoint the final history
/// through `plan` before returning the original error. Ignore that checkpoint's
/// output: it cannot admit further calls.
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
    limits.validate().map_err(|error| error.to_string())?;
    let mut state = StepInput {
        code,
        admitted: Vec::new(),
        history: Vec::new(),
    };
    let mut queued = VecDeque::new();
    let mut active = Vec::new();
    let mut ready = VecDeque::new();
    loop {
        let mut failure = None;
        let done = match plan(state.clone()).await {
            Err(error) => {
                if active.is_empty() && queued.is_empty() && ready.is_empty() {
                    return Err(error);
                }
                failure = Some(error);
                true
            }
            Ok(ScriptStep::Calls(calls)) => {
                let admitted_before = state.admitted.len();
                for call in calls {
                    if !state
                        .admitted
                        .iter()
                        .any(|item| item.ordinal == call.ordinal)
                    {
                        state.admitted.push(call.clone());
                        queued.push_back(call);
                    }
                }
                if state.admitted.len() != admitted_before {
                    continue;
                }
                false
            }
            Ok(ScriptStep::Done(report)) => {
                if active.is_empty() && queued.is_empty() && ready.is_empty() {
                    return Ok(report);
                }
                true
            }
        };
        loop {
            while active.len() + ready.len() < limits.max_in_flight as usize {
                let Some(call) = queued.pop_front() else {
                    break;
                };
                let future = Box::pin(execute(call.clone()));
                active.push((call, future));
            }
            if active.is_empty() && ready.is_empty() {
                return Err("script is waiting without an admitted call".into());
            }
            if ready.is_empty() {
                poll_fn(|cx| {
                    let mut index = 0;
                    while index < active.len() {
                        if let Poll::Ready(result) = active[index].1.as_mut().poll(cx) {
                            let (call, _) = active.remove(index);
                            ready.push_back((call, result));
                        } else {
                            index += 1;
                        }
                    }
                    if ready.is_empty() {
                        Poll::Pending
                    } else {
                        Poll::Ready(())
                    }
                })
                .await;
            }
            #[expect(
                clippy::expect_used,
                reason = "the wait completes only with a nonempty ready queue"
            )]
            let (call, result) = ready.pop_front().expect("a completion is ready");
            state.history.push(SettledCall {
                call,
                result: result.unwrap_or_else(|error| {
                    ToolResult::failed(
                        ToolExecutionError::other(error)
                            .with_model_feedback("The nested durable tool failed.")
                            .with_retryable(false),
                    )
                }),
            });
            if !done || (active.is_empty() && queued.is_empty() && ready.is_empty()) {
                break;
            }
        }
        if let Some(error) = failure {
            let _ = plan(state).await;
            return Err(error);
        }
    }
}

#[cfg(any(feature = "duroxide", feature = "temporal"))]
pub(crate) fn activity_result(
    output: crate::ToolActivityOutput,
    name: &str,
) -> Result<ToolResult, String> {
    let durable = output
        .result
        .ok_or("tool activity omitted its durable result")?;
    Ok(durable.result.unwrap_or_else(|| {
        ToolResult::failed(
            ToolExecutionError::other(crate::result::interrupted_feedback(name))
                .with_retryable(false),
        )
    }))
}
