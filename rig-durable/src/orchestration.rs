use std::{collections::BTreeSet, future::Future, pin::Pin, time::Duration};

use duroxide::{Either2, OrchestrationContext, RetryPolicy};
use rig::{
    agent::run::{StreamedTurnAssembler, StreamedTurnEvent},
    agent::{AgentRun, AgentRunStep, InvalidToolCallAction, PendingToolCall, PromptResponse},
    completion::{CompletionRequest, ResponseIdentity},
    message::{ToolResultContent, UserContent},
};
use serde::{Deserialize, Serialize};

use crate::{
    activity_types::{ToolActivityInput, ToolActivityOutput, ToolInvocation},
    approval::ApprovalDecision,
    config::{CheckpointPolicy, CompletionMode, DurableAgentConfig},
    driver::{self, CompletionOptions},
    names::RuntimeNames,
    streaming::StreamTranscript,
    tools::ToolRoute,
    types::AgentInput,
};

type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<UserContent, String>> + Send + 'a>>;

pub(crate) const STEERING_QUEUE_NAME: &str = "RigAgentSteeringV1";

#[derive(Serialize, Deserialize)]
pub(crate) struct SteeringCommand {
    pub command_id: String,
    pub message: rig::completion::Message,
}

pub async fn run(
    ctx: OrchestrationContext,
    input: AgentInput,
    config: DurableAgentConfig,
) -> Result<PromptResponse, String> {
    run_with_names(ctx, input, config, RuntimeNames::legacy()).await
}

pub(crate) async fn run_with_names(
    ctx: OrchestrationContext,
    input: AgentInput,
    config: DurableAgentConfig,
    names: RuntimeNames,
) -> Result<PromptResponse, String> {
    let is_resume = input.is_resume();
    let (mut agent, generation, mut model_turn, mut prompt_index) =
        input.into_run(config.max_turns)?;
    if !is_resume && let Some(tool_choice) = config.completion.tool_choice.clone() {
        agent = agent.with_tool_choice(tool_choice);
    }
    let mut operations = 0;
    let mut last_steering_id = ctx
        .get_custom_status()
        .as_deref()
        .and_then(last_steering_id_from_status);
    loop {
        match agent.next_step().map_err(|error| error.to_string())? {
            AgentRunStep::CallModel {
                prompt,
                history,
                turn,
            } => {
                model_turn = turn;
                set_status(
                    &ctx,
                    serde_json::json!({"phase":"model","turn":turn}),
                    last_steering_id.as_deref(),
                );
                let request = driver::completion_request(
                    prompt,
                    history,
                    CompletionOptions {
                        preamble: config.preamble.clone(),
                        tools: config.tools.definitions(),
                        temperature: config.completion.temperature,
                        max_tokens: config.completion.max_tokens,
                        tool_choice: config.completion.tool_choice.clone(),
                        additional_params: config.completion.additional_params.clone(),
                    },
                );
                match config.completion_mode {
                    CompletionMode::Blocking => {
                        let turn = ctx
                            .schedule_activity_with_retry_typed(
                                &names.completion_activity,
                                &request,
                                config.completion_retry.clone(),
                            )
                            .await?;
                        driver::apply_model_turn(&mut agent, turn)?;
                    }
                    CompletionMode::Streaming => {
                        let transcript = ctx
                            .schedule_activity_with_retry_typed(
                                &names.streaming_completion_activity,
                                &request,
                                config.completion_retry.clone(),
                            )
                            .await?;
                        apply_streamed_turn(&mut agent, &request, transcript)?;
                    }
                }
                operations += 1;
                if should_checkpoint(&agent, operations, &config) {
                    return checkpoint(
                        &ctx,
                        agent,
                        generation,
                        model_turn,
                        prompt_index,
                        &config,
                        last_steering_id.as_deref(),
                    )
                    .await;
                }
            }
            AgentRunStep::CallTools { calls } => {
                let mut resolved = Vec::with_capacity(calls.len());
                for (call_index, pending) in calls.into_iter().enumerate() {
                    resolved.push(
                        resolve_approval(
                            &ctx,
                            pending,
                            &config,
                            prompt_index,
                            model_turn,
                            call_index,
                        )
                        .await?,
                    );
                }
                set_status(
                    &ctx,
                    serde_json::json!({"phase":"tools","count":resolved.len()}),
                    last_steering_id.as_deref(),
                );
                let futures: Vec<ToolFuture<'_>> = resolved
                    .into_iter()
                    .enumerate()
                    .map(|(call_index, pending)| {
                        Box::pin(execute_tool_call(
                            &ctx,
                            pending,
                            &config,
                            &names,
                            prompt_index,
                            model_turn,
                            call_index,
                        )) as ToolFuture<'_>
                    })
                    .collect();
                let results = ctx
                    .join(futures)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;
                agent.tool_results(results).map_err(|e| e.to_string())?;
                operations += 1;
                if should_checkpoint(&agent, operations, &config) {
                    return checkpoint(
                        &ctx,
                        agent,
                        generation,
                        model_turn,
                        prompt_index,
                        &config,
                        last_steering_id.as_deref(),
                    )
                    .await;
                }
            }
            AgentRunStep::Done(response) => {
                if let Some((prompt, command_id)) = take_steering(&ctx).await {
                    if let Some(command_id) = command_id {
                        last_steering_id = Some(command_id);
                    }
                    set_status(
                        &ctx,
                        serde_json::json!({"phase":"steering_accepted"}),
                        last_steering_id.as_deref(),
                    );
                    let history = agent.full_history();
                    prompt_index = prompt_index.checked_add(1).ok_or("prompt index overflow")?;
                    agent = AgentRun::new(prompt)
                        .with_history(history)
                        .max_turns(config.max_turns);
                    if let Some(tool_choice) = config.completion.tool_choice.clone() {
                        agent = agent.with_tool_choice(tool_choice);
                    }
                    model_turn = 0;
                    if should_checkpoint(&agent, operations, &config) {
                        return checkpoint(
                            &ctx,
                            agent,
                            generation,
                            model_turn,
                            prompt_index,
                            &config,
                            last_steering_id.as_deref(),
                        )
                        .await;
                    }
                    continue;
                }
                set_status(
                    &ctx,
                    serde_json::json!({"phase":"completed"}),
                    last_steering_id.as_deref(),
                );
                return Ok(response);
            }
        }
    }
}

fn apply_streamed_turn(
    agent: &mut AgentRun,
    request: &CompletionRequest,
    transcript: StreamTranscript,
) -> Result<(), String> {
    let executable: BTreeSet<_> = request.tools.iter().map(|tool| tool.name.clone()).collect();
    let allowed = match request.tool_choice.as_ref() {
        Some(rig::message::ToolChoice::None) => BTreeSet::new(),
        Some(rig::message::ToolChoice::Specific { function_names }) => function_names
            .iter()
            .filter(|name| executable.contains(*name))
            .cloned()
            .collect(),
        Some(rig::message::ToolChoice::Auto | rig::message::ToolChoice::Required) | None => {
            executable.clone()
        }
    };
    let mut assembler = StreamedTurnAssembler::new(executable, allowed);
    let mut usage = None;
    let mut final_response = None;
    for wire_item in &transcript.items {
        if let crate::streaming::StreamItem::Final { response } = wire_item {
            final_response = Some(response.clone());
        }
        let item = wire_item.as_rig();
        for event in assembler.ingest(&item).map_err(|error| error.to_string())? {
            match event {
                StreamedTurnEvent::EmitIngested | StreamedTurnEvent::EmitToolCallDelta { .. } => {}
                StreamedTurnEvent::Completed {
                    usage: event_usage, ..
                } => {
                    if usage.replace(event_usage).is_some() {
                        return Err("stream transcript contains multiple FinalUsage events".into());
                    }
                }
                StreamedTurnEvent::InvalidToolCall(invalid) => {
                    let partial = assembler.partial_turn(transcript.message_id.clone());
                    let _context = agent.streamed_invalid_tool_call_context(&partial, &invalid);
                    let resolution = agent
                        .resolve_streamed_invalid_tool_call(
                            &partial,
                            &invalid,
                            InvalidToolCallAction::fail(),
                        )
                        .map_err(|error| error.to_string())?;
                    assembler.resolve_pending_invalid(&resolution);
                }
            }
        }
    }
    if let Some(error) = assembler.pending_delta_error() {
        return Err(error.to_string());
    }
    let final_response = final_response.ok_or("stream transcript ended before Final")?;
    let message_id = transcript
        .message_id
        .clone()
        .or_else(|| final_response.message_id.clone());
    agent
        .record_streamed_completion_call(
            final_response.usage,
            ResponseIdentity {
                message_id: message_id.clone(),
                response_id: final_response.response_id.clone(),
                provider_request_id: final_response.provider_request_id.clone(),
            },
            final_response.finish_reason,
            final_response.raw,
        )
        .map_err(|error| error.to_string())?;
    let turn = assembler.finish(message_id, &transcript.final_choice);
    agent.streamed_turn(turn).map_err(|error| error.to_string())
}

fn should_checkpoint(agent: &AgentRun, operations: u32, config: &DurableAgentConfig) -> bool {
    let CheckpointPolicy::Every(threshold) = config.checkpoint.policy else {
        return false;
    };
    if operations < threshold.get() {
        return false;
    }
    // `is_done` becomes true only after advancing AwaitingAdvance. Probe a clone
    // so a final text model response does not create an empty final execution.
    !matches!(agent.clone().next_step(), Ok(AgentRunStep::Done(_)))
}

async fn take_steering(
    ctx: &OrchestrationContext,
) -> Option<(rig::completion::Message, Option<String>)> {
    loop {
        match ctx
            .select2(
                ctx.dequeue_event(STEERING_QUEUE_NAME),
                ctx.schedule_timer(Duration::ZERO),
            )
            .await
        {
            Either2::First(message) => {
                if let Ok(command) = serde_json::from_str::<SteeringCommand>(&message) {
                    return Some((command.message, Some(command.command_id)));
                }
                match serde_json::from_str(&message) {
                    Ok(message) => return Some((message, None)),
                    Err(_) => continue,
                }
            }
            Either2::Second(()) => return None,
        }
    }
}

async fn checkpoint(
    ctx: &OrchestrationContext,
    agent: AgentRun,
    generation: u32,
    model_turn: usize,
    prompt_index: u64,
    config: &DurableAgentConfig,
    last_steering_id: Option<&str>,
) -> Result<PromptResponse, String> {
    let generation = generation
        .checked_add(1)
        .ok_or("checkpoint generation overflow")?;
    set_status(
        ctx,
        serde_json::json!({"phase":"checkpoint","generation":generation}),
        last_steering_id,
    );
    let input = AgentInput::resume(agent, generation, 0, model_turn, prompt_index);
    let payload = serde_json::to_string(&input).map_err(|error| error.to_string())?;
    let raw = match &config.checkpoint.target_version {
        Some(version) => ctx.continue_as_new_versioned(version, payload).await?,
        None => ctx.continue_as_new(payload).await?,
    };
    serde_json::from_str(&raw).map_err(|error| error.to_string())
}

async fn resolve_approval(
    ctx: &OrchestrationContext,
    pending: PendingToolCall,
    config: &DurableAgentConfig,
    prompt_index: u64,
    turn: usize,
    call_index: usize,
) -> Result<PendingToolCall, String> {
    if pending.preresolved_result.is_some() {
        return Ok(pending);
    }
    let call = &pending.tool_call;
    let entry = config
        .tools
        .get(&call.function.name)
        .ok_or_else(|| format!("tool `{}` is not in catalog", call.function.name))?;
    if !entry.requires_approval {
        return Ok(pending);
    }
    if !config.approval.enabled {
        return Err(format!(
            "tool `{}` requires approval, but approvals are disabled",
            call.function.name
        ));
    }

    let request = driver::approval_request(call, prompt_index, turn, call_index)?;
    let mut status = serde_json::json!({"phase":"approval","request":request});
    if let Some(last_steering_id) = ctx
        .get_custom_status()
        .as_deref()
        .and_then(last_steering_id_from_status)
    {
        status["last_steering_id"] = last_steering_id.into();
    }
    let status = status.to_string();
    if status.len() > 256 * 1024 {
        return Err("approval request exceeds Duroxide's custom status limit".into());
    }
    ctx.set_custom_status(status);

    loop {
        let raw = ctx.dequeue_event(&config.approval.queue_name).await;
        let Ok(decision) = serde_json::from_str::<ApprovalDecision>(&raw) else {
            continue;
        };
        if decision.approval_id() != request.approval_id {
            continue;
        }
        return match decision {
            ApprovalDecision::Approve { .. } => Ok(pending),
            ApprovalDecision::Deny { reason, .. } => Ok(driver::deny_tool(pending, reason)),
        };
    }
}

fn set_status(
    ctx: &OrchestrationContext,
    mut status: serde_json::Value,
    last_steering_id: Option<&str>,
) {
    if let Some(last_steering_id) = last_steering_id {
        status["last_steering_id"] = last_steering_id.into();
    }
    ctx.set_custom_status(status.to_string());
}

fn last_steering_id_from_status(status: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(status)
        .ok()?
        .get("last_steering_id")?
        .as_str()
        .map(str::to_owned)
}

async fn execute_tool_call(
    ctx: &OrchestrationContext,
    pending: PendingToolCall,
    config: &DurableAgentConfig,
    names: &RuntimeNames,
    prompt_index: u64,
    turn: usize,
    call_index: usize,
) -> Result<UserContent, String> {
    if let Some(result) = pending.preresolved_result {
        return Ok(result);
    }

    let call = pending.tool_call;
    let entry = config
        .tools
        .get(&call.function.name)
        .ok_or_else(|| format!("tool `{}` is not in catalog", call.function.name))?;
    let arguments = serde_json::to_string(&call.function.arguments).map_err(|e| e.to_string())?;
    let content = match &entry.route {
        ToolRoute::RigTool => {
            let payload = serde_json::to_string(&ToolActivityInput {
                name: call.function.name.clone(),
                arguments,
                invocation: ToolInvocation {
                    execution_id: format!("{}:{}", ctx.instance_id(), ctx.execution_id()),
                    prompt_index,
                    turn,
                    call_index,
                },
            })
            .map_err(|e| e.to_string())?;
            let raw = schedule_with_retry(
                ctx,
                &names.tool_activity,
                payload,
                entry.retry.clone(),
                entry.tag.as_deref(),
            )
            .await?;
            serde_json::from_str::<ToolActivityOutput>(&raw)
                .map_err(|e| e.to_string())?
                .content
        }
        ToolRoute::Activity { activity_name } => {
            let raw = schedule_with_retry(
                ctx,
                activity_name,
                arguments,
                entry.retry.clone(),
                entry.tag.as_deref(),
            )
            .await?;
            let content = serde_json::from_str(&raw)
                .map(ToolResultContent::json)
                .unwrap_or_else(|_| ToolResultContent::text(raw));
            vec![content]
        }
        ToolRoute::SubOrchestration {
            orchestration_name,
            version,
        } => {
            if entry.tag.is_some() {
                return Err(format!(
                    "sub-orchestration tool `{}` does not support worker tags",
                    call.function.name
                ));
            }
            if entry.retry.max_attempts != 1 {
                return Err(format!(
                    "sub-orchestration tool `{}` does not support parent-side retries",
                    call.function.name
                ));
            }
            if entry.retry.timeout.is_some() {
                return Err(format!(
                    "sub-orchestration tool `{}` does not support parent-side timeouts",
                    call.function.name
                ));
            }
            let raw = ctx
                .schedule_sub_orchestration_versioned(
                    orchestration_name,
                    version.clone(),
                    arguments,
                )
                .await?;
            let content = serde_json::from_str(&raw)
                .map(ToolResultContent::json)
                .unwrap_or_else(|_| ToolResultContent::text(raw));
            vec![content]
        }
        ToolRoute::DurableAgent {
            orchestration_name,
            version,
        } => {
            let prompt = call
                .function
                .arguments
                .get("prompt")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    format!(
                        "durable sub-agent tool `{}` requires a string `prompt` argument",
                        call.function.name
                    )
                })?;
            let input =
                serde_json::to_string(&AgentInput::new(prompt)).map_err(|e| e.to_string())?;
            let raw = ctx
                .schedule_sub_orchestration_versioned(
                    orchestration_name,
                    Some(version.clone()),
                    input,
                )
                .await?;
            let response: PromptResponse =
                serde_json::from_str(&raw).map_err(|error| error.to_string())?;
            vec![ToolResultContent::text(response.output)]
        }
    };

    Ok(driver::tool_result(&call, content))
}

async fn schedule_with_retry(
    ctx: &OrchestrationContext,
    name: &str,
    input: String,
    policy: RetryPolicy,
    tag: Option<&str>,
) -> Result<String, String> {
    let mut last_error = String::new();
    for attempt in 1..=policy.max_attempts {
        let activity = ctx.schedule_activity(name, &input);
        let activity = match tag {
            Some(tag) => activity.with_tag(tag),
            None => activity,
        };
        let result = if let Some(timeout) = policy.timeout {
            let deadline = async {
                ctx.schedule_timer(timeout).await;
                Err::<String, String>("timeout: activity timed out".into())
            };
            match ctx.select2(activity, deadline).await {
                Either2::First(result) => result,
                Either2::Second(result) => return result,
            }
        } else {
            activity.await
        };

        match result {
            Ok(output) => return Ok(output),
            Err(error) => {
                last_error = error;
                if attempt < policy.max_attempts {
                    let delay = policy.delay_for_attempt(attempt);
                    if !delay.is_zero() {
                        ctx.schedule_timer(delay).await;
                    }
                }
            }
        }
    }
    Err(last_error)
}
