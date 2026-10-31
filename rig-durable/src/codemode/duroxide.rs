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
use serde::{Deserialize, Serialize};

use crate::{
    AgentInput, ToolActivityInput, ToolInvocation,
    activities::tool::ToolActivityResult,
    config::{ConfigSnapshot, DurableAgentConfig, encode_execution},
    identity::{AttemptMetadata, LogicalCallKey},
    names::RuntimeNames,
    orchestration::schedule_with_retry,
    tools::{ToolCatalog, ToolEntry, ToolRoute},
};

use super::{CodeModeConfig, StepInput};

/// Trusted backend input, separate from the model's source-only arguments.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ExecutionInput {
    pub code: String,
    pub parent: LogicalCallKey,
    pub definition: ConfigSnapshot,
}

#[derive(Clone, Serialize, Deserialize)]
struct PlanningInput {
    config: CodeModeConfig,
    step: StepInput,
}

pub(crate) fn registries(
    names: RuntimeNames,
    version: &str,
    mut definition: DurableAgentConfig,
    limits: Limits,
) -> Result<(ToolEntry, ActivityRegistry, OrchestrationRegistry), String> {
    limits.validate().map_err(|error| error.to_string())?;
    for entry in definition.tools.0.values() {
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
    let config = CodeModeConfig::native(limits, definition.tools.definitions());
    config.validate()?;
    definition.code_mode = Some(config.clone());
    let plan = format!("{}::codemode-step::v1", names.tool_activity);
    let workflow = format!("{}::codemode::v1", names.orchestration);
    let entry = ToolEntry {
        definition: config.definition()?,
        route: ToolRoute::CodeMode {
            orchestration_name: workflow.clone(),
            version: version.into(),
            snapshot: Box::new(definition.snapshot()),
        },
        retry: crate::RetryPolicy::new(1),
        tag: None,
        requires_approval: false,
        policy: crate::ToolPolicy::application_managed(),
    };
    let activity = ActivityRegistry::builder()
        .register_typed(&plan, move |_ctx, input: PlanningInput| async move {
            encode_execution(&input)?;
            input.config.step(input.step).await
        })
        .build();
    let orchestration = OrchestrationRegistry::builder()
        .register_versioned_typed(&workflow, version, move |ctx, input: ExecutionInput| {
            let names = names.clone();
            let plan = plan.clone();
            async move { run(ctx, input, names, plan).await }
        })
        .build();
    Ok((entry, activity, orchestration))
}

async fn run(
    ctx: OrchestrationContext,
    input: ExecutionInput,
    names: RuntimeNames,
    plan: String,
) -> Result<serde_json::Value, String> {
    encode_execution(&input)?;
    let definition = DurableAgentConfig::default()
        .resolve(&input.definition)
        .map_err(|error| error.to_string())?;
    let config = definition.code_mode.ok_or("missing code-mode definition")?;
    let tools = definition.tools;
    let parent = input.parent;
    let report = super::run(
        input.code,
        config.limits,
        |state| {
            ctx.set_custom_status(
                serde_json::json!({
                    "phase":"codemode", "completed_calls":state.history.len(),
                    "admitted_calls":state.admitted.len()
                })
                .to_string(),
            );
            let input = PlanningInput {
                config: config.clone(),
                step: state,
            };
            let ctx = &ctx;
            let plan = &plan;
            async move {
                let payload = encode_execution(&input)?;
                let raw = ctx.schedule_activity(plan, payload).await?;
                serde_json::from_str::<ScriptStep>(&raw).map_err(|error| error.to_string())
            }
        },
        |call| {
            let ctx = &ctx;
            let tools = &tools;
            let names = &names;
            let parent = &parent;
            let runtime_identity = &config.runtime_identity;
            async move { execute(ctx, &call, tools, names, parent, runtime_identity).await }
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
    parent: &LogicalCallKey,
    runtime_identity: &str,
) -> Result<ToolResult, String> {
    let entry = tools
        .get(&call.name)
        .ok_or("script called an unregistered tool")?;
    let args = serde_json::to_string(&call.arguments).map_err(|error| error.to_string())?;
    let key = LogicalCallKey {
        logical_execution_id: parent.canonical(),
        submission_id: "codemode".into(),
        model_turn: 1,
        call_index: call.ordinal as usize,
    };
    entry.check_route_policy()?;
    let result = match &entry.route {
        #[cfg(feature = "durable-object")]
        ToolRoute::DurableObject { .. } => {
            return Err("Duroxide does not support Durable Object tool routes".into());
        }
        ToolRoute::RigTool => {
            let raw = schedule_with_retry(
                ctx,
                &names.tool_activity,
                |attempt| {
                    let execution_id = format!("{}:{}", ctx.instance_id(), ctx.execution_id());
                    let invocation = ToolInvocation {
                        execution_id: execution_id.clone(),
                        prompt_index: 0,
                        turn: 1,
                        call_index: call.ordinal as usize,
                        logical_key: key.clone(),
                        attempt: AttemptMetadata {
                            backend_execution_id: execution_id,
                            activity_attempt: Some(attempt),
                        },
                    };
                    serde_json::to_string(&ToolActivityInput {
                        name: call.name.clone(),
                        arguments: args.clone(),
                        invocation,
                        policy: entry.policy.clone(),
                        runtime_identity: Some(runtime_identity.into()),
                    })
                    .map_err(|error| error.to_string())
                },
                entry.retry.clone(),
                entry.tag.as_deref(),
            )
            .await;
            match raw {
                Ok(raw) => {
                    let output: ToolActivityResult =
                        serde_json::from_str(&raw).map_err(|error| error.to_string())?;
                    super::activity_result(output.map_err(|error| error.to_string())?, &call.name)
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
            snapshot,
        } => {
            let prompt = call
                .arguments
                .get("prompt")
                .and_then(serde_json::Value::as_str)
                .ok_or("sub-agent requires a string prompt")?;
            let input = encode_execution(
                &AgentInput::new(prompt)
                    .with_scope(key.canonical())
                    .with_snapshot(*snapshot.clone()),
            )?;
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
        ToolRoute::CodeMode { .. } => {
            return Err("direct nested code-mode routes are unsupported; use a typed child".into());
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use duroxide::{
        Action, Event, EventKind,
        runtime::replay_engine::{ReplayEngine, TurnResult},
    };
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Recording {
        instance: String,
        history: Vec<Event>,
    }

    #[test]
    fn replays_recorded_codemode_histories_and_new_schedule_prefixes() {
        for (bytes, agent, max_in_flight) in [
            (
                include_bytes!("../../tests/fixtures/replay/duroxide-codemode-children.json")
                    .as_slice(),
                "workflow",
                Limits::default().max_in_flight,
            ),
            (
                include_bytes!("../../tests/fixtures/replay/duroxide-codemode-race.json")
                    .as_slice(),
                "race",
                2,
            ),
        ] {
            let recording: Recording = serde_json::from_slice(bytes).unwrap();
            let mut tools = ToolCatalog::default();
            // Planner activities are replayed, not executed. Only routing and
            // policy settings affect the workflow's emitted schedule payloads.
            for (name, child) in [
                ("research", Some("researcher")),
                ("assess", Some("assessor")),
                ("review", Some("reviewer")),
                ("interruptible", None),
                ("probe", None),
            ] {
                tools.insert(ToolEntry {
                    definition: rig::completion::ToolDefinition {
                        name: rig::message::ToolName::new(name).unwrap(),
                        description: "Recorded nested tool".into(),
                        parameters: serde_json::json!({"type":"object"}),
                        annotations: Default::default(),
                    },
                    route: child.map_or(ToolRoute::RigTool, |child| ToolRoute::DurableAgent {
                        orchestration_name: RuntimeNames::for_agent(child, "1.0.0").orchestration,
                        version: "1.0.0".into(),
                        snapshot: Box::new(DurableAgentConfig::default().snapshot()),
                    }),
                    retry: crate::RetryPolicy::new(1),
                    tag: None,
                    requires_approval: false,
                    policy: if name == "interruptible" {
                        crate::ToolPolicy::read_only()
                    } else {
                        crate::ToolPolicy::application_managed()
                    },
                });
            }
            let (_, _, registry) = registries(
                RuntimeNames::for_agent(agent, "1.0.0"),
                "1.0.0",
                DurableAgentConfig {
                    tools,
                    ..Default::default()
                },
                Limits {
                    max_in_flight,
                    ..Default::default()
                },
            )
            .unwrap();
            let EventKind::OrchestrationStarted {
                name,
                version,
                input,
                ..
            } = recording.history[0].kind.clone()
            else {
                panic!("missing start")
            };
            let handler = registry
                .resolve_handler_exact(&name, &semver::Version::parse(&version).unwrap())
                .unwrap();
            // Stop before each recorded schedule so the adapter must emit it
            // anew. This checks child identity, which historical replay binds
            // from history instead of comparing with the emitted identity.
            for (index, expected) in recording.history.iter().enumerate().filter(|(_, event)| {
                matches!(
                    event.kind,
                    EventKind::ActivityScheduled { .. }
                        | EventKind::SubOrchestrationScheduled { .. }
                )
            }) {
                let mut replay = ReplayEngine::new(
                    recording.instance.clone(),
                    1,
                    recording.history[..index].to_vec(),
                );
                let result = replay.execute_orchestration(
                    handler.clone(),
                    input.clone(),
                    name.clone(),
                    version.clone(),
                    "prefix-replay",
                );
                assert!(
                    matches!(result, TurnResult::Continue),
                    "{agent} prefix {index}: {result:?}"
                );
                let actual = replay
                    .history_delta()
                    .iter()
                    .find(|event| event.event_id == expected.event_id)
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(&actual.kind).unwrap(),
                    serde_json::to_value(&expected.kind).unwrap(),
                    "{agent} schedule {}",
                    expected.event_id
                );
                for action in replay.pending_actions() {
                    if let Action::StartSubOrchestration { version, .. } = action {
                        assert_eq!(version.as_deref(), Some("1.0.0"));
                    }
                }
            }
            let mut history = recording.history;
            let terminal = history.pop().unwrap();
            let EventKind::OrchestrationCompleted { output: expected } = terminal.kind else {
                panic!("missing completion")
            };
            let mut replay = ReplayEngine::new(recording.instance, terminal.execution_id, history);
            let result = replay.execute_orchestration(handler, input, name, version, "full-replay");
            let TurnResult::Completed(actual) = result else {
                panic!("{agent}: {result:?}")
            };
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&actual).unwrap(),
                serde_json::from_str::<serde_json::Value>(&expected).unwrap()
            );
            assert!(
                replay.pending_actions().is_empty(),
                "replay must not schedule effects"
            );
        }
    }
}
