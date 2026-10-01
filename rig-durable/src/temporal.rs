//! Temporal-backed durable Rig agents.
//!
//! The workflow owns Rig's deterministic [`AgentRun`] state machine. Model and
//! tool I/O runs in Temporal activities registered from [`TemporalAgent`].

use std::{collections::VecDeque, sync::Arc, time::Duration};

use rig::{
    DynModel,
    agent::{AgentRun, AgentRunStep, PromptResponse},
    completion::{CompletionRequest, Message, ToolDefinition},
    message::{ToolChoice, UserContent},
    operation::Completion,
    tool::{Tool, ToolSet},
};
use serde::{Deserialize, Serialize};
use temporalio_common::RetryPolicy;
use temporalio_sdk::{
    ActivityOptions, ApplicationFailure, ContinueAsNewOptions, SyncWorkflowContext, WorkerOptions,
    WorkflowContext, WorkflowContextView, WorkflowRegistrationError, WorkflowResult,
    activities::{ActivityContext, ActivityError, activities},
    workflows::{join_all, workflow, workflow_methods},
};

use crate::{
    activities,
    activity_types::{ToolActivityInput, ToolActivityOutput, ToolInvocation},
    approval::{ApprovalDecision, ApprovalRequest},
    driver::{self, CompletionOptions},
};

const DEFAULT_ACTIVITY_TIMEOUT_SECS: u64 = 60;
const DEFAULT_SESSION_HISTORY_MAX_BYTES: usize = 1_000_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalAgentConfig {
    pub preamble: Option<String>,
    pub tools: Vec<TemporalTool>,
    pub max_turns: usize,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub tool_choice: Option<ToolChoice>,
    pub additional_params: Option<serde_json::Value>,
    pub activity_timeout_secs: u64,
    pub activity_max_attempts: u32,
    pub session_history_max_bytes: usize,
}

impl Default for TemporalAgentConfig {
    fn default() -> Self {
        Self {
            preamble: None,
            tools: Vec::new(),
            max_turns: 8,
            temperature: None,
            max_tokens: None,
            tool_choice: Some(ToolChoice::Auto),
            additional_params: None,
            activity_timeout_secs: DEFAULT_ACTIVITY_TIMEOUT_SECS,
            activity_max_attempts: 3,
            session_history_max_bytes: DEFAULT_SESSION_HISTORY_MAX_BYTES,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalTool {
    pub definition: ToolDefinition,
    pub requires_approval: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalAgentInput {
    pub prompt: Message,
    #[serde(default)]
    pub history: Vec<Message>,
    pub config: TemporalAgentConfig,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum TemporalAgentStatus {
    #[default]
    Starting,
    Idle,
    Model {
        turn: usize,
    },
    Tools {
        count: usize,
    },
    Approval {
        request: ApprovalRequest,
    },
    Completed,
    Closed,
}

#[derive(Default)]
struct TemporalRuntimeState {
    status: TemporalAgentStatus,
    decisions: Vec<ApprovalDecision>,
    steering: VecDeque<Message>,
    next_prompt_index: u64,
}

trait TemporalWorkflowState {
    fn runtime(&self) -> &TemporalRuntimeState;
    fn runtime_mut(&mut self) -> &mut TemporalRuntimeState;
    fn records_history(&self) -> bool {
        false
    }
    fn record_history(&mut self, _history: Vec<Message>) {}
}

#[derive(Default)]
#[workflow]
pub struct TemporalAgentWorkflow {
    runtime: TemporalRuntimeState,
}

impl TemporalWorkflowState for TemporalAgentWorkflow {
    fn runtime(&self) -> &TemporalRuntimeState {
        &self.runtime
    }

    fn runtime_mut(&mut self) -> &mut TemporalRuntimeState {
        &mut self.runtime
    }
}

#[workflow_methods]
impl TemporalAgentWorkflow {
    #[run(name = "RigTemporalAgentV1")]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: TemporalAgentInput,
    ) -> WorkflowResult<PromptResponse> {
        run_agent(ctx, input).await
    }

    #[signal]
    pub fn approval(&mut self, _ctx: &mut SyncWorkflowContext<Self>, decision: ApprovalDecision) {
        self.runtime.decisions.push(decision);
    }

    /// Queue a message as the next agent turn after the active model/tool turn finishes.
    #[signal]
    pub fn steer(&mut self, _ctx: &mut SyncWorkflowContext<Self>, message: Message) {
        self.runtime.steering.push_back(message);
    }

    #[query]
    pub fn status(&self, _ctx: &WorkflowContextView) -> TemporalAgentStatus {
        self.runtime.status.clone()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalAgentSessionInput {
    #[serde(default)]
    pub history: Vec<Message>,
    pub config: TemporalAgentConfig,
    #[serde(default)]
    pub queued_steering: Vec<Message>,
    #[serde(default)]
    pub next_prompt_index: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TemporalAgentSessionResult {
    pub history: Vec<Message>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TemporalAgentSessionSnapshot {
    pub status: TemporalAgentStatus,
    pub history: Vec<Message>,
    pub queued_steering: usize,
    pub next_prompt_index: u64,
    pub busy: bool,
    pub closed: bool,
}

#[workflow]
pub struct TemporalAgentSessionWorkflow {
    runtime: TemporalRuntimeState,
    config: TemporalAgentConfig,
    history: Vec<Message>,
    busy: bool,
    closed: bool,
}

impl TemporalWorkflowState for TemporalAgentSessionWorkflow {
    fn runtime(&self) -> &TemporalRuntimeState {
        &self.runtime
    }

    fn runtime_mut(&mut self) -> &mut TemporalRuntimeState {
        &mut self.runtime
    }

    fn records_history(&self) -> bool {
        true
    }

    fn record_history(&mut self, history: Vec<Message>) {
        self.history = history;
    }
}

#[workflow_methods]
impl TemporalAgentSessionWorkflow {
    #[init]
    fn init(_ctx: &WorkflowContextView, input: TemporalAgentSessionInput) -> Self {
        Self {
            runtime: TemporalRuntimeState {
                status: TemporalAgentStatus::Idle,
                steering: input.queued_steering.into(),
                next_prompt_index: input.next_prompt_index,
                ..TemporalRuntimeState::default()
            },
            config: input.config,
            history: input.history,
            busy: false,
            closed: false,
        }
    }

    #[run(name = "RigTemporalAgentSessionV1")]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
    ) -> WorkflowResult<TemporalAgentSessionResult> {
        loop {
            let wait_ctx = ctx.clone();
            ctx.wait_condition(move |workflow| {
                !workflow.busy
                    && (workflow.closed
                        || !workflow.runtime.steering.is_empty()
                        || (wait_ctx.continue_as_new_suggested()
                            && wait_ctx.all_handlers_finished()))
            })
            .await?;

            let steering = ctx.state_mut(|workflow| {
                if workflow.busy {
                    return None;
                }
                let prompt = workflow.runtime.steering.pop_front()?;
                workflow.busy = true;
                Some((prompt, workflow.config.clone(), workflow.history.clone()))
            });
            if let Some((prompt, config, history)) = steering {
                if session_payload_too_large(&config, &history, &prompt) {
                    ctx.state_mut(close_session);
                    return Err(workflow_error(
                        "agent session history exceeds configured limit",
                    ));
                }
                let result = run_agent(
                    ctx,
                    TemporalAgentInput {
                        prompt,
                        history,
                        config,
                    },
                )
                .await;
                ctx.state_mut(|workflow| {
                    workflow.busy = false;
                    if result.is_err() {
                        close_session(workflow);
                    } else if workflow.closed {
                        workflow.runtime.status = TemporalAgentStatus::Closed;
                    } else {
                        workflow.runtime.status = TemporalAgentStatus::Idle;
                    }
                });
                result?;
                continue;
            }

            let should_close = ctx.state(|workflow| workflow.closed);
            if should_close {
                break;
            }
            if ctx.continue_as_new_suggested() && ctx.all_handlers_finished() {
                let input = ctx.state(|workflow| TemporalAgentSessionInput {
                    history: workflow.history.clone(),
                    config: workflow.config.clone(),
                    queued_steering: workflow.runtime.steering.iter().cloned().collect(),
                    next_prompt_index: workflow.runtime.next_prompt_index,
                });
                return match ctx.continue_as_new(input, ContinueAsNewOptions::default()) {
                    Err(error) => Err(error),
                    Ok(never) => match never {},
                };
            }
        }
        let wait_ctx = ctx.clone();
        ctx.wait_condition(move |_| wait_ctx.all_handlers_finished())
            .await?;
        ctx.state_mut(|workflow| workflow.runtime.status = TemporalAgentStatus::Closed);
        Ok(TemporalAgentSessionResult {
            history: ctx.state(|workflow| workflow.history.clone()),
        })
    }

    #[update_validator(prompt)]
    fn validate_prompt(
        &self,
        _ctx: &WorkflowContextView,
        _prompt: &Message,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.closed {
            Err("agent session is closed".into())
        } else if self.busy {
            Err("agent session is processing another prompt".into())
        } else if session_payload_too_large(&self.config, &self.history, _prompt) {
            Err("agent session history exceeds configured limit".into())
        } else {
            Ok(())
        }
    }

    /// Run one prompt and any steering messages queued while it is active.
    #[update]
    pub async fn prompt(
        ctx: &mut WorkflowContext<Self>,
        prompt: Message,
    ) -> Result<PromptResponse, Box<dyn std::error::Error + Send + Sync>> {
        let start: Result<_, Box<dyn std::error::Error + Send + Sync>> =
            ctx.state_mut(|workflow| {
                if workflow.closed {
                    return Err("agent session is closed".into());
                }
                if workflow.busy {
                    return Err("agent session is processing another prompt".into());
                }
                if session_payload_too_large(&workflow.config, &workflow.history, &prompt) {
                    return Err("agent session history exceeds configured limit".into());
                }
                workflow.busy = true;
                Ok((workflow.config.clone(), workflow.history.clone()))
            });
        let (config, history): (TemporalAgentConfig, Vec<Message>) = start?;
        let result = run_agent(
            ctx,
            TemporalAgentInput {
                prompt,
                history,
                config,
            },
        )
        .await;
        ctx.state_mut(|workflow| {
            workflow.busy = false;
            if result.is_err() {
                close_session(workflow);
            } else {
                workflow.runtime.status = if workflow.closed {
                    TemporalAgentStatus::Closed
                } else {
                    TemporalAgentStatus::Idle
                };
            }
        });
        result.map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
    }

    #[signal]
    pub fn approval(&mut self, _ctx: &mut SyncWorkflowContext<Self>, decision: ApprovalDecision) {
        self.runtime.decisions.push(decision);
    }

    #[signal]
    pub fn steer(&mut self, _ctx: &mut SyncWorkflowContext<Self>, message: Message) {
        if !self.closed {
            self.runtime.steering.push_back(message);
        }
    }

    #[signal]
    pub fn close(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.closed = true;
        if !self.busy {
            self.runtime.status = TemporalAgentStatus::Closed;
        }
    }

    #[query]
    pub fn status(&self, _ctx: &WorkflowContextView) -> TemporalAgentStatus {
        self.runtime.status.clone()
    }

    #[query]
    pub fn snapshot(&self, _ctx: &WorkflowContextView) -> TemporalAgentSessionSnapshot {
        TemporalAgentSessionSnapshot {
            status: self.runtime.status.clone(),
            history: self.history.clone(),
            queued_steering: self.runtime.steering.len(),
            next_prompt_index: self.runtime.next_prompt_index,
            busy: self.busy,
            closed: self.closed,
        }
    }
}

fn close_session(workflow: &mut TemporalAgentSessionWorkflow) {
    workflow.closed = true;
    workflow.runtime.status = TemporalAgentStatus::Closed;
}

fn session_payload_too_large(
    config: &TemporalAgentConfig,
    history: &[Message],
    prompt: &Message,
) -> bool {
    serde_json::to_vec(&(history, prompt))
        .map(|payload| payload.len() > config.session_history_max_bytes)
        .unwrap_or(true)
}

fn history_payload_too_large(config: &TemporalAgentConfig, history: &[Message]) -> bool {
    serde_json::to_vec(history)
        .map(|payload| payload.len() > config.session_history_max_bytes)
        .unwrap_or(true)
}

struct TemporalActivities {
    model: DynModel<Completion>,
    tools: Arc<ToolSet>,
}

#[activities]
impl TemporalActivities {
    #[activity(name = "RigTemporalModelCompletionV1")]
    async fn complete(
        self: Arc<Self>,
        _ctx: ActivityContext,
        request: CompletionRequest,
    ) -> Result<rig::agent::ModelTurn, ActivityError> {
        activities::completion::complete(&self.model, request)
            .await
            .map_err(activity_error)
    }

    #[activity(name = "RigTemporalToolExecutionV1")]
    async fn execute_tool(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ToolActivityInput,
    ) -> Result<ToolActivityOutput, ActivityError> {
        activities::tool::execute(&self.tools, input)
            .await
            .map_err(activity_error)
    }
}

fn activity_error(message: String) -> ActivityError {
    ApplicationFailure::new(message).into()
}

/// A worker-side Temporal agent definition.
pub struct TemporalAgent {
    model: DynModel<Completion>,
    tools: Arc<ToolSet>,
    config: TemporalAgentConfig,
}

impl TemporalAgent {
    pub fn new(model: impl Into<DynModel<Completion>>) -> Self {
        Self {
            model: model.into(),
            tools: Arc::new(ToolSet::default()),
            config: TemporalAgentConfig::default(),
        }
    }

    pub fn preamble(mut self, preamble: impl Into<String>) -> Self {
        self.config.preamble = Some(preamble.into());
        self
    }

    pub fn max_turns(mut self, max_turns: usize) -> Self {
        self.config.max_turns = max_turns;
        self
    }

    pub fn temperature(mut self, temperature: f64) -> Self {
        self.config.temperature = Some(temperature);
        self
    }

    pub fn max_tokens(mut self, max_tokens: u64) -> Self {
        self.config.max_tokens = Some(max_tokens);
        self
    }

    pub fn tool_choice(mut self, tool_choice: ToolChoice) -> Self {
        self.config.tool_choice = Some(tool_choice);
        self
    }

    pub fn activity_timeout(mut self, timeout: Duration) -> Self {
        self.config.activity_timeout_secs = timeout.as_secs().max(1);
        self
    }

    pub fn activity_max_attempts(mut self, attempts: u32) -> Self {
        self.config.activity_max_attempts = attempts;
        self
    }

    pub fn session_history_max_bytes(mut self, bytes: usize) -> Self {
        self.config.session_history_max_bytes = bytes.max(1);
        self
    }

    pub fn tool<T>(self, tool: T) -> Self
    where
        T: Tool + 'static,
    {
        self.tool_with_approval(tool, false)
    }

    pub fn approval_tool<T>(self, tool: T) -> Self
    where
        T: Tool + 'static,
    {
        self.tool_with_approval(tool, true)
    }

    fn tool_with_approval<T>(mut self, tool: T, requires_approval: bool) -> Self
    where
        T: Tool + 'static,
    {
        let tools =
            Arc::get_mut(&mut self.tools).expect("tools are unique while building an agent");
        let name = tools.add_tool(tool);
        let definition = self
            .tools
            .tool_definitions()
            .into_iter()
            .find(|definition| definition.name == name)
            .expect("newly added tool has a definition");
        if let Some(existing) = self
            .config
            .tools
            .iter_mut()
            .find(|existing| existing.definition.name == name)
        {
            *existing = TemporalTool {
                definition,
                requires_approval,
            };
        } else {
            self.config.tools.push(TemporalTool {
                definition,
                requires_approval,
            });
        }
        self
    }

    pub fn input(&self, prompt: impl Into<Message>) -> TemporalAgentInput {
        TemporalAgentInput {
            prompt: prompt.into(),
            history: Vec::new(),
            config: self.config.clone(),
        }
    }

    pub fn input_with_history(
        &self,
        prompt: impl Into<Message>,
        history: Vec<Message>,
    ) -> TemporalAgentInput {
        TemporalAgentInput {
            prompt: prompt.into(),
            history,
            config: self.config.clone(),
        }
    }

    pub fn session_input(&self, history: Vec<Message>) -> TemporalAgentSessionInput {
        TemporalAgentSessionInput {
            history,
            config: self.config.clone(),
            queued_steering: Vec::new(),
            next_prompt_index: 0,
        }
    }

    /// Register the workflow and this agent's model and tools on a worker.
    ///
    /// Registration consumes the agent because the worker retains its model and tool set.
    pub fn register(self, options: &mut WorkerOptions) -> Result<(), WorkflowRegistrationError> {
        options.register_workflow::<TemporalAgentWorkflow>()?;
        options.register_workflow::<TemporalAgentSessionWorkflow>()?;
        options.register_activities(TemporalActivities {
            model: self.model,
            tools: self.tools,
        });
        Ok(())
    }
}

async fn run_agent<W>(
    ctx: &mut WorkflowContext<W>,
    input: TemporalAgentInput,
) -> WorkflowResult<PromptResponse>
where
    W: TemporalWorkflowState,
{
    let config = input.config;
    let mut agent = AgentRun::new(input.prompt)
        .with_history(input.history)
        .max_turns(config.max_turns);
    if let Some(tool_choice) = config.tool_choice.clone() {
        agent = agent.with_tool_choice(tool_choice);
    }
    let mut prompt_index = allocate_prompt_index(ctx)?;
    let mut model_turn = 0;

    loop {
        match agent.next_step().map_err(workflow_error)? {
            AgentRunStep::CallModel {
                prompt,
                history,
                turn,
            } => {
                model_turn = turn;
                ctx.state_mut(|workflow| {
                    workflow.runtime_mut().status = TemporalAgentStatus::Model { turn }
                });
                let request = driver::completion_request(
                    prompt,
                    history,
                    CompletionOptions {
                        preamble: config.preamble.clone(),
                        tools: config
                            .tools
                            .iter()
                            .map(|tool| tool.definition.clone())
                            .collect(),
                        temperature: config.temperature,
                        max_tokens: config.max_tokens,
                        tool_choice: config.tool_choice.clone(),
                        additional_params: config.additional_params.clone(),
                    },
                );
                let turn = ctx
                    .execute_activity(
                        TemporalActivities::complete,
                        request,
                        activity_options(&config),
                    )
                    .await?;
                driver::apply_model_turn(&mut agent, turn).map_err(workflow_error)?;
            }
            AgentRunStep::CallTools { calls } => {
                ctx.state_mut(|workflow| {
                    workflow.runtime_mut().status =
                        TemporalAgentStatus::Tools { count: calls.len() }
                });
                let mut resolved = Vec::with_capacity(calls.len());
                for (index, mut pending) in calls.into_iter().enumerate() {
                    if pending.preresolved_result.is_none() {
                        let call = &pending.tool_call;
                        let tool = config
                            .tools
                            .iter()
                            .find(|tool| tool.definition.name == call.function.name)
                            .ok_or_else(|| {
                                workflow_error(format!(
                                    "tool `{}` is not registered",
                                    call.function.name
                                ))
                            })?;
                        if tool.requires_approval {
                            pending = await_approval(ctx, pending, prompt_index, model_turn, index)
                                .await?;
                        }
                    }
                    resolved.push(pending);
                }
                ctx.state_mut(|workflow| {
                    workflow.runtime_mut().status = TemporalAgentStatus::Tools {
                        count: resolved.len(),
                    }
                });
                let results = join_all(resolved.into_iter().enumerate().map(
                    |(call_index, pending)| {
                        execute_tool(ctx, pending, &config, prompt_index, model_turn, call_index)
                    },
                ))
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
                agent.tool_results(results).map_err(workflow_error)?;
            }
            AgentRunStep::Done(response) => {
                let history = agent.full_history();
                if ctx.state(TemporalWorkflowState::records_history)
                    && history_payload_too_large(&config, &history)
                {
                    return Err(workflow_error(
                        "agent session history exceeds configured limit",
                    ));
                }
                ctx.state_mut(|workflow| workflow.record_history(history.clone()));
                let steering =
                    ctx.state_mut(|workflow| workflow.runtime_mut().steering.pop_front());
                let Some(prompt) = steering else {
                    ctx.state_mut(|workflow| {
                        workflow.runtime_mut().status = TemporalAgentStatus::Completed;
                    });
                    return Ok(response);
                };
                prompt_index = allocate_prompt_index(ctx)?;
                agent = AgentRun::new(prompt)
                    .with_history(history)
                    .max_turns(config.max_turns);
                if let Some(tool_choice) = config.tool_choice.clone() {
                    agent = agent.with_tool_choice(tool_choice);
                }
                model_turn = 0;
            }
        }
    }
}

fn allocate_prompt_index<W>(
    ctx: &WorkflowContext<W>,
) -> Result<u64, temporalio_sdk::WorkflowTermination>
where
    W: TemporalWorkflowState,
{
    ctx.state_mut(|workflow| {
        let runtime = workflow.runtime_mut();
        let index = runtime.next_prompt_index;
        runtime.next_prompt_index = index
            .checked_add(1)
            .ok_or_else(|| workflow_error("prompt index overflow"))?;
        Ok(index)
    })
}

async fn await_approval<W>(
    ctx: &mut WorkflowContext<W>,
    mut pending: rig::agent::PendingToolCall,
    prompt_index: u64,
    turn: usize,
    call_index: usize,
) -> Result<rig::agent::PendingToolCall, temporalio_sdk::WorkflowTermination>
where
    W: TemporalWorkflowState,
{
    let call = &pending.tool_call;
    let request =
        driver::approval_request(call, prompt_index, turn, call_index).map_err(workflow_error)?;
    ctx.state_mut(|workflow| {
        let runtime = workflow.runtime_mut();
        runtime
            .decisions
            .retain(|decision| decision.approval_id() == request.approval_id);
        runtime.status = TemporalAgentStatus::Approval {
            request: request.clone(),
        }
    });
    ctx.wait_condition(|workflow| {
        workflow
            .runtime()
            .decisions
            .iter()
            .any(|decision| decision.approval_id() == request.approval_id)
    })
    .await?;
    let decision = ctx.state_mut(|workflow| {
        let runtime = workflow.runtime_mut();
        let index = runtime
            .decisions
            .iter()
            .position(|decision| decision.approval_id() == request.approval_id)
            .expect("wait condition found an approval decision");
        let decision = runtime.decisions.remove(index);
        runtime.decisions.clear();
        decision
    });
    if let ApprovalDecision::Deny { reason, .. } = decision {
        pending = driver::deny_tool(pending, reason);
    }
    Ok(pending)
}

async fn execute_tool<W>(
    ctx: &WorkflowContext<W>,
    pending: rig::agent::PendingToolCall,
    config: &TemporalAgentConfig,
    prompt_index: u64,
    turn: usize,
    call_index: usize,
) -> Result<UserContent, temporalio_sdk::WorkflowTermination> {
    if let Some(result) = pending.preresolved_result {
        return Ok(result);
    }
    let call = pending.tool_call;
    let output = ctx
        .execute_activity(
            TemporalActivities::execute_tool,
            ToolActivityInput {
                name: call.function.name.to_string(),
                arguments: serde_json::to_string(&call.function.arguments)
                    .map_err(workflow_error)?,
                invocation: ToolInvocation {
                    execution_id: format!("{}:{}", ctx.workflow_id(), ctx.run_id()),
                    prompt_index,
                    turn,
                    call_index,
                },
            },
            activity_options(config),
        )
        .await?;
    Ok(driver::tool_result(&call, output.content))
}

fn activity_options(config: &TemporalAgentConfig) -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(config.activity_timeout_secs))
        .retry_policy(
            RetryPolicy::builder()
                .maximum_attempts(config.activity_max_attempts)
                .build(),
        )
        .build()
}

fn workflow_error(error: impl ToString) -> temporalio_sdk::WorkflowTermination {
    ApplicationFailure::new(error.to_string()).into()
}
