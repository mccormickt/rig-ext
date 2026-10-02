use std::{collections::BTreeSet, future::Future, pin::Pin, time::Duration};

use duroxide::{Either2, OrchestrationContext, RetryPolicy};
use rig::{
    agent::{AgentRun, AgentRunStep, PendingToolCall, PromptResponse, run::StreamedTurn},
    completion::{CompletionRequest, ResponseIdentity},
    message::{ToolResultContent, UserContent},
};
use serde::{Deserialize, Serialize};

use crate::{
    activity_types::{InvocationContract, ToolActivityInput, ToolActivityOutput, ToolInvocation},
    approval::ApprovalDecision,
    config::{CheckpointPolicy, CompletionMode, ConfigSnapshot, DurableAgentConfig},
    driver::{self, CompletionOptions},
    identity::{AttemptMetadata, LogicalCallKey},
    names::RuntimeNames,
    outcome::{CallPosition, DurableResponse, ToolOutcome},
    policy::ReplaySafety,
    streaming::StreamTranscript,
    tools::{ToolEntry, ToolRoute},
    types::{AgentInput, ResumedRun},
};

type ToolFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(UserContent, ToolOutcome), String>> + Send + 'a>>;

pub(crate) const STEERING_QUEUE_NAME: &str = "RigAgentSteeringV1";

/// KV key under which a single run retains its [`DurableResponse`].
pub const RUN_RESULT_KEY: &str = "rig_durable.run.result.v1";

/// Largest value this crate writes to Duroxide's per-instance KV store. It
/// leaves headroom under Duroxide's 64 KiB value limit.
pub(crate) const KV_VALUE_LIMIT: usize = 60 * 1024;

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
    let config = match &input.snapshot {
        Some(snapshot) => config
            .resolve(snapshot)
            .map_err(|error| error.to_string())?,
        None => config,
    };
    let snapshot = match (input.snapshot.clone(), config.contract) {
        (Some(snapshot), _) => Some(snapshot),
        (None, InvocationContract::Logical) => Some(config.snapshot()),
        (None, InvocationContract::Legacy) => None,
    };
    let ResumedRun {
        mut agent,
        generation,
        last_model_turn,
        prompt_index,
        mut tool_outcomes,
    } = input.into_run(config.max_turns)?;
    if !is_resume && let Some(tool_choice) = config.completion.tool_choice.clone() {
        agent = agent.with_tool_choice(tool_choice);
    }
    let mut cursor = Cursor {
        prompt_index,
        model_turn: last_model_turn,
    };
    let engine = Engine {
        ctx: &ctx,
        config: &config,
        names: &names,
    };
    let mut operations = 0;
    let mut last_steering_id = ctx
        .get_custom_status()
        .as_deref()
        .and_then(last_steering_id_from_status);
    loop {
        let steering_id = last_steering_id.clone();
        let decorate = move |status: &mut serde_json::Value| {
            if let Some(id) = &steering_id {
                status["last_steering_id"] = id.as_str().into();
            }
        };
        match engine
            .advance(&mut agent, &mut cursor, &mut tool_outcomes, &decorate)
            .await?
        {
            Step::Continue => {
                operations += 1;
                if should_checkpoint(&agent, operations, &config) {
                    return checkpoint(
                        &ctx,
                        agent,
                        generation,
                        &cursor,
                        tool_outcomes,
                        &config,
                        snapshot.as_ref(),
                        &decorate,
                    )
                    .await;
                }
            }
            Step::Done(response) => {
                let response = *response;
                if let Some((prompt, command_id)) = take_steering(&ctx).await {
                    if let Some(command_id) = command_id {
                        last_steering_id = Some(command_id);
                    }
                    let steering_id = last_steering_id.clone();
                    let decorate = move |status: &mut serde_json::Value| {
                        if let Some(id) = &steering_id {
                            status["last_steering_id"] = id.as_str().into();
                        }
                    };
                    set_status(
                        &ctx,
                        serde_json::json!({"phase":"steering_accepted"}),
                        &decorate,
                    );
                    let history = agent.full_history();
                    cursor.prompt_index = cursor
                        .prompt_index
                        .checked_add(1)
                        .ok_or("prompt index overflow")?;
                    agent = AgentRun::new(prompt)
                        .with_history(history)
                        .max_turns(config.max_turns);
                    if let Some(tool_choice) = config.completion.tool_choice.clone() {
                        agent = agent.with_tool_choice(tool_choice);
                    }
                    cursor.model_turn = 0;
                    if should_checkpoint(&agent, operations, &config) {
                        return checkpoint(
                            &ctx,
                            agent,
                            generation,
                            &cursor,
                            tool_outcomes,
                            &config,
                            snapshot.as_ref(),
                            &decorate,
                        )
                        .await;
                    }
                    continue;
                }
                // Retained after every awaited event of the run, so histories
                // recorded before this key existed still replay.
                let detailed = DurableResponse::new(response.clone(), tool_outcomes)
                    .fit_within(KV_VALUE_LIMIT);
                ctx.set_kv_value_typed(RUN_RESULT_KEY, &detailed);
                set_status(&ctx, serde_json::json!({"phase":"completed"}), &decorate);
                return Ok(response);
            }
        }
    }
}

/// Position of the active prompt inside an execution.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Cursor {
    pub prompt_index: u64,
    pub model_turn: usize,
}

/// Result of advancing an [`AgentRun`] by one durable operation.
pub(crate) enum Step {
    /// A model call or a tool batch finished.
    Continue,
    /// The prompt answered.
    Done(Box<PromptResponse>),
}

/// Runs the model and tool operations of one [`AgentRun`] as Duroxide
/// activities. The single-run orchestration and the session orchestration
/// share it.
pub(crate) struct Engine<'a> {
    pub ctx: &'a OrchestrationContext,
    pub config: &'a DurableAgentConfig,
    pub names: &'a RuntimeNames,
}

impl Engine<'_> {
    /// Advance the run by one operation. `decorate` adds caller fields to
    /// every custom status the operation writes.
    pub(crate) async fn advance(
        &self,
        agent: &mut AgentRun,
        cursor: &mut Cursor,
        outcomes: &mut Vec<ToolOutcome>,
        decorate: &(dyn Fn(&mut serde_json::Value) + Sync),
    ) -> Result<Step, String> {
        let ctx = self.ctx;
        let config = self.config;
        match agent.next_step().map_err(|error| error.to_string())? {
            AgentRunStep::CallModel {
                prompt,
                history,
                turn,
            } => {
                cursor.model_turn = turn;
                set_status(
                    ctx,
                    serde_json::json!({"phase":"model","turn":turn}),
                    decorate,
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
                                &self.names.completion_activity,
                                &request,
                                config.completion_retry.clone(),
                            )
                            .await?;
                        driver::apply_model_turn(agent, turn)?;
                    }
                    CompletionMode::Streaming => {
                        let transcript = ctx
                            .schedule_activity_with_retry_typed(
                                &self.names.streaming_completion_activity,
                                &request,
                                config.completion_retry.clone(),
                            )
                            .await?;
                        apply_streamed_turn(agent, &request, transcript)?;
                    }
                }
                Ok(Step::Continue)
            }
            AgentRunStep::CallTools { calls } => {
                let mut resolved = Vec::with_capacity(calls.len());
                for (call_index, pending) in calls.into_iter().enumerate() {
                    resolved.push(
                        self.resolve_approval(pending, cursor.position(call_index), decorate)
                            .await?,
                    );
                }
                set_status(
                    ctx,
                    serde_json::json!({"phase":"tools","count":resolved.len()}),
                    decorate,
                );
                let futures: Vec<ToolFuture<'_>> = resolved
                    .into_iter()
                    .enumerate()
                    .map(|(call_index, resolved)| {
                        Box::pin(self.execute_tool_call(resolved, cursor.position(call_index)))
                            as ToolFuture<'_>
                    })
                    .collect();
                let results = ctx
                    .join(futures)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;
                let mut contents = Vec::with_capacity(results.len());
                for (content, outcome) in results {
                    contents.push(content);
                    outcomes.push(outcome);
                }
                agent.tool_results(contents).map_err(|e| e.to_string())?;
                Ok(Step::Continue)
            }
            AgentRunStep::Done(response) => Ok(Step::Done(Box::new(response))),
        }
    }

    async fn resolve_approval(
        &self,
        pending: PendingToolCall,
        position: CallPosition,
        decorate: &(dyn Fn(&mut serde_json::Value) + Sync),
    ) -> Result<ResolvedCall, String> {
        let ctx = self.ctx;
        let config = self.config;
        if pending.preresolved_result.is_some() {
            return Ok(ResolvedCall {
                pending,
                denied: false,
            });
        }
        let call = &pending.tool_call;
        let entry = config
            .tools
            .get(&call.function.name)
            .ok_or_else(|| format!("tool `{}` is not in catalog", call.function.name))?;
        if !entry.requires_approval {
            return Ok(ResolvedCall {
                pending,
                denied: false,
            });
        }
        if !config.approval.enabled {
            return Err(format!(
                "tool `{}` requires approval, but approvals are disabled",
                call.function.name
            ));
        }

        let request = match config.contract {
            InvocationContract::Legacy => driver::approval_request(
                call,
                position.prompt_index,
                position.turn,
                position.call_index,
            )?,
            InvocationContract::Logical => driver::logical_approval_request(
                call,
                &self.logical_key(position),
                entry.policy.version(),
            )?,
        };
        let mut status = serde_json::json!({"phase":"approval","request":request});
        decorate(&mut status);
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
                ApprovalDecision::Approve { .. } => Ok(ResolvedCall {
                    pending,
                    denied: false,
                }),
                ApprovalDecision::Deny { reason, .. } => Ok(ResolvedCall {
                    pending: driver::deny_tool(pending, reason),
                    denied: true,
                }),
            };
        }
    }

    async fn execute_tool_call(
        &self,
        resolved: ResolvedCall,
        position: CallPosition,
    ) -> Result<(UserContent, ToolOutcome), String> {
        let ctx = self.ctx;
        let config = self.config;
        let ResolvedCall { pending, denied } = resolved;
        if let Some(result) = pending.preresolved_result {
            let outcome = if denied {
                ToolOutcome::denied(position, &pending.tool_call)
            } else {
                ToolOutcome::preresolved(position, &pending.tool_call)
            };
            return Ok((result, outcome));
        }

        let call = pending.tool_call;
        let entry = config
            .tools
            .get(&call.function.name)
            .ok_or_else(|| format!("tool `{}` is not in catalog", call.function.name))?;
        let arguments =
            serde_json::to_string(&call.function.arguments).map_err(|e| e.to_string())?;
        check_route_policy(entry, &call.function.name)?;
        let (content, outcome) = match &entry.route {
            ToolRoute::RigTool => {
                let name = call.function.name.to_string();
                let execution_id = format!("{}:{}", ctx.instance_id(), ctx.execution_id());
                let payload = |attempt: u32| {
                    let invocation = match config.contract {
                        InvocationContract::Legacy => ToolInvocation::legacy(
                            execution_id.clone(),
                            position.prompt_index,
                            position.turn,
                            position.call_index,
                        ),
                        InvocationContract::Logical => ToolInvocation {
                            execution_id: execution_id.clone(),
                            prompt_index: position.prompt_index,
                            turn: position.turn,
                            call_index: position.call_index,
                            logical_key: Some(self.logical_key(position)),
                            attempt: Some(AttemptMetadata {
                                backend_execution_id: execution_id.clone(),
                                activity_attempt: Some(attempt),
                            }),
                        },
                    };
                    let policy = match config.contract {
                        InvocationContract::Legacy => None,
                        InvocationContract::Logical => Some(entry.policy.clone()),
                    };
                    serde_json::to_string(&ToolActivityInput {
                        name: name.clone(),
                        arguments: arguments.clone(),
                        invocation,
                        policy,
                    })
                    .map_err(|e| e.to_string())
                };
                let raw = schedule_with_retry(
                    ctx,
                    self.names.tool_activity(config.contract),
                    payload,
                    entry.retry.clone(),
                    entry.tag.as_deref(),
                )
                .await?;
                let output =
                    serde_json::from_str::<ToolActivityOutput>(&raw).map_err(|e| e.to_string())?;
                let outcome = ToolOutcome::from_activity(position, &call, &output);
                (output.content, outcome)
            }
            ToolRoute::Activity { activity_name } => {
                let raw = schedule_with_retry(
                    ctx,
                    activity_name,
                    |_attempt| Ok(arguments.clone()),
                    entry.retry.clone(),
                    entry.tag.as_deref(),
                )
                .await?;
                let content = serde_json::from_str(&raw)
                    .map(ToolResultContent::json)
                    .unwrap_or_else(|_| ToolResultContent::text(raw));
                (vec![content], ToolOutcome::routed(position, &call))
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
                (vec![content], ToolOutcome::routed(position, &call))
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
                (
                    vec![ToolResultContent::text(response.output)],
                    ToolOutcome::routed(position, &call),
                )
            }
        };

        Ok((driver::tool_result(&call, content), outcome))
    }

    fn logical_key(&self, position: CallPosition) -> LogicalCallKey {
        LogicalCallKey {
            logical_execution_id: self.ctx.instance_id(),
            submission_id: LogicalCallKey::submission_for_prompt(position.prompt_index),
            model_turn: position.turn,
            call_index: position.call_index,
        }
    }
}

impl Cursor {
    fn position(&self, call_index: usize) -> CallPosition {
        CallPosition {
            prompt_index: self.prompt_index,
            turn: self.model_turn,
            call_index,
        }
    }
}

struct ResolvedCall {
    pending: PendingToolCall,
    /// A human denied the call; `pending` carries the denial result.
    denied: bool,
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
    // A provider may emit tool calls this run cannot execute: a name that is
    // not registered, or one `tool_choice` does not authorize. Fail the run
    // rather than silently dropping the call.
    for content in &transcript.response.choice {
        if let rig::completion::AssistantContent::ToolCall(call) = content {
            let name = call.function.name.as_str();
            if !executable.contains(name) {
                return Err(format!(
                    "provider streamed a call to tool `{name}`, which is not registered"
                ));
            }
            if !allowed.contains(name) {
                return Err(format!(
                    "provider streamed a call to tool `{name}`, which `tool_choice` does not authorize"
                ));
            }
        }
    }
    let response = &transcript.response;
    agent
        .record_streamed_completion_call(
            response.usage,
            ResponseIdentity {
                message_id: response.message_id.clone(),
                response_id: response.response_id.clone(),
                provider_request_id: response.provider_request_id.clone(),
            },
            response.finish_reason(),
            response.raw.clone(),
        )
        .map_err(|error| error.to_string())?;
    let turn = StreamedTurn {
        message_id: response.message_id.clone(),
        choice: response.choice.clone(),
        executable_tool_names: executable,
        allowed_tool_names: allowed,
        finish_reason: response.finish_reason(),
    };
    agent.streamed_turn(turn).map_err(|error| error.to_string())
}

pub(crate) fn should_checkpoint(
    agent: &AgentRun,
    operations: u32,
    config: &DurableAgentConfig,
) -> bool {
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

#[allow(clippy::too_many_arguments)]
async fn checkpoint(
    ctx: &OrchestrationContext,
    agent: AgentRun,
    generation: u32,
    cursor: &Cursor,
    tool_outcomes: Vec<ToolOutcome>,
    config: &DurableAgentConfig,
    snapshot: Option<&ConfigSnapshot>,
    decorate: &(dyn Fn(&mut serde_json::Value) + Sync),
) -> Result<PromptResponse, String> {
    let generation = generation
        .checked_add(1)
        .ok_or("checkpoint generation overflow")?;
    set_status(
        ctx,
        serde_json::json!({"phase":"checkpoint","generation":generation}),
        decorate,
    );
    let input = AgentInput::resume(
        agent,
        generation,
        0,
        cursor.model_turn,
        cursor.prompt_index,
        snapshot.cloned(),
        tool_outcomes,
    );
    let payload = serde_json::to_string(&input).map_err(|error| error.to_string())?;
    let raw = match &config.checkpoint.target_version {
        Some(version) => ctx.continue_as_new_versioned(version, payload).await?,
        None => ctx.continue_as_new(payload).await?,
    };
    serde_json::from_str(&raw).map_err(|error| error.to_string())
}

pub(crate) fn set_status(
    ctx: &OrchestrationContext,
    mut status: serde_json::Value,
    decorate: &(dyn Fn(&mut serde_json::Value) + Sync),
) {
    decorate(&mut status);
    ctx.set_custom_status(status.to_string());
}

fn last_steering_id_from_status(status: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(status)
        .ok()?
        .get("last_steering_id")?
        .as_str()
        .map(str::to_owned)
}

/// Routes other than Rig tools receive raw arguments, so they cannot use a
/// logical key or an invocation guard.
pub(crate) fn check_route_policy(entry: &ToolEntry, name: &str) -> Result<(), String> {
    if matches!(entry.route, ToolRoute::RigTool) {
        return Ok(());
    }
    match entry.policy.safety() {
        ReplaySafety::ApplicationManaged | ReplaySafety::ReadOnly => Ok(()),
        ReplaySafety::Idempotent => Err(format!(
            "tool `{name}` declares idempotent replay safety, but its route cannot deliver a \
             logical call key"
        )),
        ReplaySafety::InterruptOnUncertain => Err(format!(
            "tool `{name}` declares interrupt-on-uncertain replay safety, but its route cannot \
             use an invocation guard"
        )),
    }
}

async fn schedule_with_retry(
    ctx: &OrchestrationContext,
    name: &str,
    payload: impl Fn(u32) -> Result<String, String>,
    policy: RetryPolicy,
    tag: Option<&str>,
) -> Result<String, String> {
    let mut last_error = String::new();
    for attempt in 1..=policy.max_attempts {
        let input = payload(attempt)?;
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
