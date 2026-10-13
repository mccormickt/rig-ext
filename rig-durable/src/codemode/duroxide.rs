//! Duroxide schedules script steps as pure activities and each nested effect
//! as its own activity or child orchestration. No VM runs in orchestration code.

use duroxide::{
    OrchestrationContext,
    runtime::registry::{ActivityRegistry, OrchestrationRegistry},
};
use rig::{
    agent::PromptResponse,
    tool::{ToolExecutionError, ToolOutput, ToolResult},
};
use rig_codemode::{
    Limits,
    replay::{ScriptCall, ScriptStep},
};

use crate::{
    AgentInput, InvocationContract, ToolActivityInput, ToolActivityOutput, ToolInvocation,
    identity::{AttemptMetadata, LogicalCallKey},
    names::RuntimeNames,
    orchestration::schedule_with_retry,
    tools::{ToolCatalog, ToolEntry, ToolRoute, sub_orchestration_tool},
};

use super::{CodeModeConfig, ScriptInput, StepInput};

pub(crate) fn registries(
    names: RuntimeNames,
    version: &str,
    tools: ToolCatalog,
    contract: InvocationContract,
    limits: Limits,
) -> Result<(ToolEntry, ActivityRegistry, OrchestrationRegistry), String> {
    limits.validate().map_err(|error| error.to_string())?;
    for entry in tools.0.values() {
        if entry.requires_approval {
            return Err("code-mode nested calls do not support human approval".into());
        }
        if matches!(
            entry.route,
            ToolRoute::SubOrchestration { .. } | ToolRoute::DurableAgent { .. }
        ) && (entry.tag.is_some()
            || entry.retry.max_attempts != 1
            || entry.retry.timeout.is_some())
        {
            return Err(
                "code-mode child workflows do not support parent retries, tags, or timeouts".into(),
            );
        }
    }
    let config = CodeModeConfig {
        limits,
        definitions: tools.definitions(),
    };
    config.validate()?;
    let plan = format!("{}::codemode-step::v1", names.tool_activity);
    let workflow = format!("{}::codemode::v1", names.orchestration);
    let entry = sub_orchestration_tool(config.definition()?, &workflow, Some(version.into()));
    let activity = ActivityRegistry::builder()
        .register_typed(&plan, move |_ctx, input: StepInput| {
            let config = config.clone();
            async move { config.step(input).await }
        })
        .build();
    let orchestration = OrchestrationRegistry::builder()
        .register_versioned_typed(&workflow, version, move |ctx, input: ScriptInput| {
            let tools = tools.clone();
            let names = names.clone();
            let plan = plan.clone();
            async move { run(ctx, input, tools, names, plan, contract, limits).await }
        })
        .build();
    Ok((entry, activity, orchestration))
}

async fn run(
    ctx: OrchestrationContext,
    input: ScriptInput,
    tools: ToolCatalog,
    names: RuntimeNames,
    plan: String,
    contract: InvocationContract,
    limits: Limits,
) -> Result<serde_json::Value, String> {
    let report = super::run(
        input.code,
        limits,
        |state| {
            ctx.set_custom_status(
                serde_json::json!({
                    "phase":"codemode", "completed_calls":state.history.len(),
                    "admitted_calls":state.admitted.len()
                })
                .to_string(),
            );
            ctx.schedule_activity_typed::<_, ScriptStep>(&plan, &state)
        },
        |call| {
            let ctx = &ctx;
            let tools = &tools;
            let names = &names;
            async move { execute(ctx, &call, tools, names, contract).await }
        },
    )
    .await?;
    serde_json::to_value(report).map_err(|error| error.to_string())
}

async fn execute(
    ctx: &OrchestrationContext,
    call: &ScriptCall,
    tools: &ToolCatalog,
    names: &RuntimeNames,
    contract: InvocationContract,
) -> Result<ToolResult, String> {
    let entry = tools
        .get(&call.name)
        .ok_or("script called an unregistered tool")?;
    let args = serde_json::to_string(&call.arguments).map_err(|error| error.to_string())?;
    let result = match &entry.route {
        ToolRoute::RigTool => {
            let raw = schedule_with_retry(
                ctx,
                names.tool_activity(contract),
                |attempt| {
                    let execution_id = format!("{}:{}", ctx.instance_id(), ctx.execution_id());
                    let mut invocation =
                        ToolInvocation::legacy(execution_id.clone(), 0, 1, call.ordinal as usize);
                    if contract == InvocationContract::Logical {
                        invocation.logical_key = Some(LogicalCallKey {
                            logical_execution_id: ctx.instance_id(),
                            submission_id: LogicalCallKey::submission_for_prompt(0),
                            model_turn: 1,
                            call_index: call.ordinal as usize,
                        });
                        invocation.attempt = Some(AttemptMetadata {
                            backend_execution_id: execution_id,
                            activity_attempt: Some(attempt),
                        });
                    }
                    serde_json::to_string(&ToolActivityInput {
                        name: call.name.clone(),
                        arguments: args.clone(),
                        invocation,
                        policy: (contract == InvocationContract::Logical)
                            .then(|| entry.policy.clone()),
                    })
                    .map_err(|error| error.to_string())
                },
                entry.retry.clone(),
                entry.tag.as_deref(),
            )
            .await;
            match raw {
                Ok(raw) => {
                    let output: ToolActivityOutput =
                        serde_json::from_str(&raw).map_err(|error| error.to_string())?;
                    super::activity_result(output, &call.name)
                }
                Err(error) => Err(error),
            }
        }
        ToolRoute::Activity { activity_name } => schedule_with_retry(
            ctx,
            activity_name,
            |_| Ok(args.clone()),
            entry.retry.clone(),
            entry.tag.as_deref(),
        )
        .await
        .map(|raw| ToolResult::success(json_or_text(raw))),
        ToolRoute::SubOrchestration {
            orchestration_name,
            version,
        } => ctx
            .schedule_sub_orchestration_versioned(orchestration_name, version.clone(), args)
            .await
            .map(|raw| ToolResult::success(json_or_text(raw))),
        ToolRoute::DurableAgent {
            orchestration_name,
            version,
        } => {
            let prompt = call
                .arguments
                .get("prompt")
                .and_then(serde_json::Value::as_str)
                .ok_or("sub-agent requires a string prompt")?;
            let input = serde_json::to_string(&AgentInput::new(prompt))
                .map_err(|error| error.to_string())?;
            match ctx
                .schedule_sub_orchestration_versioned(
                    orchestration_name,
                    Some(version.clone()),
                    input,
                )
                .await
            {
                Ok(raw) => {
                    let response: PromptResponse =
                        serde_json::from_str(&raw).map_err(|error| error.to_string())?;
                    Ok(ToolResult::success(ToolOutput::text(response.output())))
                }
                Err(error) => Err(error),
            }
        }
    };
    Ok(result.unwrap_or_else(|error| {
        ToolResult::failed(
            ToolExecutionError::other(error)
                .with_model_feedback("The nested durable tool failed.")
                .with_retryable(false),
        )
    }))
}

fn json_or_text(raw: String) -> ToolOutput {
    serde_json::from_str(&raw)
        .map(ToolOutput::json)
        .unwrap_or_else(|_| ToolOutput::text(raw))
}
