//! A bounded, in-memory host for the fixed example script. This is not a durable
//! journal: a production durable host must persist admissions and results.

use rig_celld::mcp::Error;
use rig_codemode::{
    Catalog, CatalogEntry, DynamicToolDispatcher, HostDispatcher, Invocation,
    replay::{ReplayInput, ReplayPlanner, ScriptStep, SettledCall},
    worker_replay::{ReplayBudget, WorkerReplayPlanner},
};
use rig_core::tool::ToolContext;
use serde_json::{Value, json};
use worker::Env;

pub async fn run(env: &Env, path: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let tools = super::client(&env.var("MCP_ENDPOINT")?.to_string(), path)?
        .list_tools()
        .await?;
    let names: Vec<_> = tools
        .iter()
        .map(|tool| tool.definition().name.to_string())
        .collect();
    let lookup = tools
        .iter()
        .find(|tool| tool.definition().name == "lookup")
        .ok_or("lookup is missing")?;
    let invalid_input_blocked = matches!(
        lookup.call(json!({"region": 9}), &Default::default()).await,
        Err(Error::Arguments)
    );
    let catalog = Catalog::new(tools.iter().map(|tool| {
        let wire = tool.definition();
        let mut entry = CatalogEntry::new(
            wire.name.to_string(),
            wire.description.as_deref().unwrap_or(""),
            Value::Object(wire.input_schema.as_ref().clone()),
        );
        if let Some(output) = &wire.output_schema {
            entry = entry.with_output_schema(Value::Object(output.as_ref().clone()));
        }
        entry
    }))?;
    let dispatcher =
        DynamicToolDispatcher::new(tools.into_iter().map(|tool| tool.into_dynamic_tool()))?;
    let planner = WorkerReplayPlanner::from_env(env, "LOADER", ReplayBudget::default())?;
    let script = include_str!("../../stock.js");
    let mut admitted = Vec::new();
    let mut history = Vec::new();
    for _ in 0..8 {
        match planner
            .plan(ReplayInput {
                code: script,
                catalog: &catalog,
                limits: Default::default(),
                admitted: &admitted,
                history: &history,
            })
            .await?
        {
            ScriptStep::Done(report) => {
                return Ok(json!({
                    "runtime": "worker-replay", "runtime_identity": planner.runtime_identity(),
                    "tools": names, "invalid_input_blocked": invalid_input_blocked,
                    "script": script, "transcript": report.render_text(), "report": report,
                }));
            }
            ScriptStep::Calls(calls) => {
                for call in calls {
                    // This fixed sequential script settles every admitted call.
                    if admitted.iter().any(|prior| prior.ordinal == call.ordinal) {
                        return Err("duplicate admission".into());
                    }
                    admitted.push(call.clone());
                    let outcome = dispatcher
                        .dispatch(Invocation::new(
                            "mcp-demo",
                            call.ordinal,
                            &call.name,
                            call.arguments.clone(),
                            ToolContext::new(),
                        ))
                        .await;
                    history.push(SettledCall {
                        call,
                        result: outcome.result,
                    });
                }
            }
        }
    }
    Err("example planning limit exceeded".into())
}
