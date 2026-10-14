use rig_codemode::replay::{ReplayError, ScriptCall, ScriptStep, SettledCall};
use rig_codemode::{Catalog, Limits};
use rig_core::tool::{ToolOutput, ToolResult};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
pub struct Input {
    pub code: String,
    pub catalog: Catalog,
    pub limits: Limits,
    #[serde(default)]
    pub admitted: Vec<ScriptCall>,
    #[serde(default)]
    pub history: Vec<Settlement>,
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub budget_work: Option<u64>,
    #[serde(default)]
    pub budget_ms: Option<u64>,
}

#[derive(Deserialize)]
pub struct Settlement {
    pub call: ScriptCall,
    pub answer: String,
}

impl Input {
    pub fn history(&self) -> Vec<SettledCall> {
        self.history
            .iter()
            .map(|item| SettledCall {
                call: item.call.clone(),
                result: ToolResult::success(ToolOutput::text(&item.answer)),
            })
            .collect()
    }
}

pub fn output(result: Result<ScriptStep, ReplayError>) -> Value {
    match result {
        Ok(mut step) => {
            if let ScriptStep::Done(report) = &mut step {
                report.elapsed = std::time::Duration::ZERO;
            }
            json!({"step": step})
        }
        Err(error) => json!({"error": error.to_string()}),
    }
}

#[cfg(target_family = "wasm")]
use worker::*;

#[cfg(target_family = "wasm")]
#[event(fetch)]
pub async fn fetch(mut request: Request, env: Env, _: Context) -> Result<Response> {
    use rig_codemode::replay::{ReplayInput, ReplayPlanner};
    use rig_codemode::worker_replay::{ReplayBudget, WorkerReplayPlanner};
    let input: Input = serde_json::from_str(&request.text().await?)
        .map_err(|error| Error::RustError(error.to_string()))?;
    let mut budget = ReplayBudget::default();
    if let Some(bytes) = input.budget_bytes {
        budget.bytes = bytes;
    }
    if let Some(work) = input.budget_work {
        budget.work = work;
    }
    if let Some(ms) = input.budget_ms {
        budget.elapsed = std::time::Duration::from_millis(ms);
    }
    let planner = WorkerReplayPlanner::from_env(&env, "LOADER", budget)?;
    let history = input.history();
    let result = planner
        .plan(ReplayInput {
            code: &input.code,
            catalog: &input.catalog,
            limits: input.limits,
            admitted: &input.admitted,
            history: &history,
        })
        .await;
    Response::from_json(&output(result))
}
