//! Temporal-backed durable Rig agents.
//!
//! The workflow owns Rig's deterministic [`AgentRun`] state machine. Model and
//! tool I/O runs in Temporal activities registered from [`TemporalAgent`].
//! Enable the `temporal` Cargo feature. Use `default-features = false` if the
//! application does not use Duroxide or SQLite.
//!
//! # Run a worker
//!
//! This example requires a Temporal server, its client configuration, and
//! `OPENAI_API_KEY`. Register one agent definition per task queue. Create inputs
//! before registration, which moves the model and tools into the worker.
//!
//! ```no_run
//! use rig::providers::openai::{self, OpenAI};
//! use rig_durable::temporal::TemporalAgent;
//! use temporalio_client::{
//!     Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
//! };
//! use temporalio_sdk::{Runtime, Worker, WorkerOptions};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let runtime = Runtime::from_current_tokio(Default::default())?;
//!     let (connection_options, client_options) =
//!         ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
//!     let client = Client::new(Connection::connect(connection_options).await?, client_options)?;
//!     let model = OpenAI::from_env()?.completion(openai::GPT_4O_MINI);
//!     let agent = TemporalAgent::new(model).preamble("Answer briefly.");
//!     let mut options = WorkerOptions::new("rig-assistant").build();
//!     agent.register(&mut options)?;
//!     let mut worker = Worker::new(&runtime, client, options)?;
//!     worker.run().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Start a run from a client
//!
//! Build an input with [`TemporalAgent::input`] using the configuration for the
//! registered worker. Use the same namespace and task queue as that worker.
//! The result is Rig's [`PromptResponse`]; query `tool_outcomes` for the ordered
//! tool dispositions. This client function requires a worker running separately.
//!
//! ```no_run
//! use rig_durable::temporal::{TemporalAgentInput, TemporalAgentWorkflow};
//! use temporalio_client::{Client, WorkflowGetResultOptions, WorkflowStartOptions};
//!
//! async fn prompt(client: &Client, input: TemporalAgentInput)
//!     -> Result<(), Box<dyn std::error::Error>>
//! {
//!     let handle = client.start_workflow(
//!         TemporalAgentWorkflow::run,
//!         input,
//!         WorkflowStartOptions::new("rig-assistant", "report-42").build(),
//!     ).await?;
//!     let response = handle.get_result(WorkflowGetResultOptions::default()).await?;
//!     println!("{}", response.output);
//!     Ok(())
//! }
//! ```
//!
//! Activities are delivered at least once. [`ToolPolicy`] declares whether a
//! tool may repeat an uncertain effect; retry limits alone cannot prevent
//! duplicate writes. New inputs use [`InvocationContract::Logical`]. Logical
//! keys include the namespace and first execution run ID, so they survive
//! continue-as-new without colliding with later starts that reuse a workflow ID.
//! Keep worker implementations compatible with their recorded configuration.

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
    activities::{self, tool::ToolExecutor},
    activity_types::{InvocationContract, ToolActivityInput, ToolActivityOutput, ToolInvocation},
    approval::{ApprovalDecision, ApprovalRequest},
    compaction::{
        Compaction, CompactionConfig, CompactionOutput, CompactionRecord, CompactionRequest,
        ContextState,
    },
    driver::{self, CompletionOptions},
    guard::InvocationGuardStore,
    identity::{AttemptMetadata, LogicalCallKey},
    outcome::{CallPosition, DurableResponse, ToolOutcome},
    policy::ToolPolicy,
    submission::{
        Admission, DEFAULT_LEDGER_MAX_BYTES, SessionAvailability, Submission, SubmissionError,
        SubmissionLedger, SubmissionState, SubmitInput,
    },
};

const DEFAULT_ACTIVITY_TIMEOUT_SECS: u64 = 60;
const DEFAULT_SESSION_HISTORY_MAX_BYTES: usize = 1_000_000;
/// Number of per-submission results a session workflow keeps in its state.
pub const SESSION_RESULT_RETENTION: usize = 16;

/// Request configuration. It travels in the workflow input, so a running
/// workflow keeps the configuration it started with across continue-as-new.
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
    /// Tool activity contract. Inputs recorded before this field existed
    /// deserialize as [`InvocationContract::Legacy`]; new inputs built by
    /// [`TemporalAgent`] use [`InvocationContract::Logical`].
    #[serde(default)]
    pub contract: InvocationContract,
    /// Completed-prompt compaction for session workflows. `None` disables it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CompactionConfig>,
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
            contract: InvocationContract::Logical,
            compaction: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalTool {
    pub definition: ToolDefinition,
    pub requires_approval: bool,
    /// Replay policy the tool activity enforces under the logical contract.
    #[serde(default)]
    pub policy: ToolPolicy,
}

/// Errors from [`TemporalAgent::register`].
#[derive(Debug, thiserror::Error)]
pub enum TemporalAgentError {
    #[error(transparent)]
    Registration(#[from] WorkflowRegistrationError),
    #[error(
        "tool `{0}` declares a replay policy, but the agent uses the legacy invocation contract"
    )]
    InvocationContractRequired(String),
    #[error(
        "tool `{0}` never repeats an uncertain effect and requires an invocation guard store; \
         supply one with `invocation_guard`"
    )]
    GuardStoreRequired(String),
}

/// Serializable prompt, Rig history, and recorded worker configuration.
/// Create it with [`TemporalAgent::input`] or [`TemporalAgent::input_with_history`].
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
    /// A session is summarizing its older context.
    Compacting,
    Completed,
    Closed,
}

#[derive(Default)]
struct TemporalRuntimeState {
    status: TemporalAgentStatus,
    decisions: Vec<ApprovalDecision>,
    steering: VecDeque<Message>,
    next_prompt_index: u64,
    /// Dispositions of the current or latest run, in dispatch order.
    outcomes: Vec<ToolOutcome>,
}

trait TemporalWorkflowState {
    fn runtime(&self) -> &TemporalRuntimeState;
    fn runtime_mut(&mut self) -> &mut TemporalRuntimeState;
    fn allocate_prompt_index(&mut self) -> Result<u64, String> {
        let runtime = self.runtime_mut();
        let index = runtime.next_prompt_index;
        runtime.next_prompt_index = index.checked_add(1).ok_or("prompt index overflow")?;
        Ok(index)
    }
    fn records_history(&self) -> bool {
        false
    }
    fn record_history(&mut self, _history: Vec<Message>) -> Result<(), String> {
        Ok(())
    }
}

/// Single-run workflow. Start [`Self::run`] with a [`TemporalAgentInput`].
/// Query [`Self::status`] for progress and send [`Self::approval`] with the
/// exact request ID when a registered approval tool pauses execution.
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
        run_agent(ctx, input)
            .await
            .map(|detailed| detailed.response)
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

    /// Dispositions of every tool call this run made, in dispatch order.
    /// The workflow result stays Rig's `PromptResponse`.
    #[query]
    pub fn tool_outcomes(&self, _ctx: &WorkflowContextView) -> Vec<ToolOutcome> {
        self.runtime.outcomes.clone()
    }
}

/// Session startup or continuation state.
/// Use [`TemporalAgent::session_input`] for a new session rather than constructing
/// continuation fields by hand.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalAgentSessionInput {
    /// Audit transcript. Compaction never removes messages from it.
    #[serde(default)]
    pub history: Vec<Message>,
    pub config: TemporalAgentConfig,
    #[serde(default)]
    pub queued_steering: Vec<Message>,
    #[serde(default)]
    pub next_prompt_index: u64,
    /// Summary that stands in for a prefix of `history`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CompactionRecord>,
    /// Submission receipts carried across continue-as-new. `None` starts a
    /// fresh ledger whose next prompt index is `next_prompt_index`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ledger: Option<SubmissionLedger>,
    /// Admitted submissions that have not run yet.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued: Vec<SubmitInput>,
    /// Recent per-submission results, oldest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<TemporalRetainedResult>,
    /// Last compaction attempt, including a nonfatal error, across continuation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compacted_at: Option<(usize, Option<String>)>,
}

/// The detailed result of one admitted submission.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalRetainedResult {
    pub submission_id: String,
    pub response: DurableResponse,
}

/// Reply of the `submit` update: the receipt and, when the session retained
/// it, the detailed response for that submission.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalSubmission {
    pub submission: Submission,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<DurableResponse>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TemporalAgentSessionResult {
    pub history: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CompactionRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ledger: Option<SubmissionLedger>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TemporalAgentSessionSnapshot {
    pub status: TemporalAgentStatus,
    pub history: Vec<Message>,
    pub queued_steering: usize,
    pub next_prompt_index: u64,
    pub busy: bool,
    pub closed: bool,
    /// Transcript messages the active context replaces with a summary.
    #[serde(default)]
    pub compaction_cutoff: usize,
    #[serde(default)]
    pub queued_submissions: usize,
    #[serde(default)]
    pub ledger_receipts: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_error: Option<String>,
}

/// Long-lived conversation with deduplicated submissions and full audit history.
///
/// Start the workflow with [`TemporalAgent::session_input`]. The `submit` update
/// returns after the prompt answers, unlike Duroxide's admission-only `submit`.
/// Retry the same request ID, message, and mode to get the original receipt.
/// The response can be absent after result retention expires; the receipt stays.
///
/// ```no_run
/// use rig_durable::{SubmitInput, temporal::{TemporalAgentSessionInput, TemporalAgentSessionWorkflow}};
/// use temporalio_client::{
///     Client, WorkflowExecuteUpdateOptions, WorkflowSignalOptions, WorkflowStartOptions,
/// };
///
/// async fn converse(client: &Client, input: TemporalAgentSessionInput)
///     -> Result<(), Box<dyn std::error::Error>>
/// {
///     let handle = client.start_workflow(
///         TemporalAgentSessionWorkflow::run,
///         input,
///         WorkflowStartOptions::new("rig-assistant", "support-42").build(),
///     ).await?;
///     let answer = handle.execute_update(
///         TemporalAgentSessionWorkflow::submit,
///         SubmitInput::new("request-1", "Explain the installation steps."),
///         WorkflowExecuteUpdateOptions::default(),
///     ).await?;
///     if let Some(response) = answer.response {
///         println!("{}", response.output());
///     }
///     handle.signal(
///         TemporalAgentSessionWorkflow::close, (), WorkflowSignalOptions::default(),
///     ).await?;
///     Ok(())
/// }
/// ```
///
/// Closing drains accepted work; failure cancels it. Compaction runs between
/// prompts, including queued submissions and steering. The history byte limit
/// applies to the full transcript, not just the compacted model context.
#[workflow]
pub struct TemporalAgentSessionWorkflow {
    runtime: TemporalRuntimeState,
    config: TemporalAgentConfig,
    context: ContextState,
    /// Transcript and active-context lengths when the current run started.
    run_base: (usize, usize),
    ledger: SubmissionLedger,
    /// Prompt index the ledger assigned to the submission about to run.
    preassigned_prompt_index: Option<u64>,
    queued: VecDeque<SubmitInput>,
    results: VecDeque<TemporalRetainedResult>,
    /// Transcript length when compaction last ran, with the error if it
    /// failed. Compaction runs again once the transcript grows.
    compacted_at: Option<(usize, Option<String>)>,
    init_error: Option<String>,
    busy: bool,
    /// A compaction activity is in flight. Submissions see the session as
    /// busy; the `prompt` update waits for it.
    compacting: bool,
    closed: bool,
}

impl TemporalWorkflowState for TemporalAgentSessionWorkflow {
    fn runtime(&self) -> &TemporalRuntimeState {
        &self.runtime
    }

    fn runtime_mut(&mut self) -> &mut TemporalRuntimeState {
        &mut self.runtime
    }

    fn allocate_prompt_index(&mut self) -> Result<u64, String> {
        if let Some(index) = self.preassigned_prompt_index.take() {
            return Ok(index);
        }
        self.ledger
            .allocate_prompt_index()
            .map_err(|error| error.to_string())
    }

    fn records_history(&self) -> bool {
        true
    }

    /// `history` is the active context the run started from plus the
    /// messages it added. Only the added messages reach the transcript.
    fn record_history(&mut self, mut history: Vec<Message>) -> Result<(), String> {
        let (transcript_base, active_base) = self.run_base;
        let added = history.split_off(active_base.min(history.len()));
        let mut transcript = self.context.transcript[..transcript_base].to_vec();
        transcript.extend(added);
        if history_payload_too_large(&self.config, &transcript) {
            return Err("agent session history exceeds configured limit".into());
        }
        self.context.transcript = transcript;
        Ok(())
    }
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

impl TemporalAgentSessionWorkflow {
    fn availability(&self) -> SessionAvailability {
        SessionAvailability {
            busy: self.busy || self.compacting || !self.queued.is_empty(),
            closed: self.closed,
        }
    }

    /// Reserve the session for one run and return the run inputs.
    fn begin_run(&mut self) -> (TemporalAgentConfig, Vec<Message>) {
        let active = self.context.active_context();
        self.run_base = (self.context.transcript.len(), active.len());
        self.busy = true;
        (self.config.clone(), active)
    }

    fn end_run(&mut self, failed: bool) {
        self.busy = false;
        self.preassigned_prompt_index = None;
        if failed {
            self.ledger.cancel_pending();
            self.queued.clear();
            self.runtime.steering.clear();
            close_session(self);
        } else {
            self.runtime.status = if self.closed {
                TemporalAgentStatus::Closed
            } else {
                TemporalAgentStatus::Idle
            };
        }
    }

    fn retain_result(&mut self, submission_id: String, response: DurableResponse) {
        self.results.push_back(TemporalRetainedResult {
            submission_id,
            response,
        });
        while self.results.len() > SESSION_RESULT_RETENTION {
            self.results.pop_front();
        }
    }

    /// Compaction is due after a completed prompt: when the transcript has
    /// grown since compaction last ran.
    fn compaction_due(&self) -> bool {
        self.config.compaction.is_some()
            && !self.context.transcript.is_empty()
            && self
                .compacted_at
                .as_ref()
                .is_none_or(|(len, _)| *len != self.context.transcript.len())
    }

    fn compaction_error(&self) -> Option<String> {
        self.compacted_at
            .as_ref()
            .and_then(|(_, error)| error.clone())
    }

    fn pending_work(&self) -> bool {
        self.closed
            || !self.runtime.steering.is_empty()
            || !self.queued.is_empty()
            || self.compaction_due()
    }

    fn continue_as_new_input(&self) -> TemporalAgentSessionInput {
        TemporalAgentSessionInput {
            history: self.context.transcript.clone(),
            config: self.config.clone(),
            queued_steering: self.runtime.steering.iter().cloned().collect(),
            next_prompt_index: self.ledger.next_prompt_index,
            compaction: self.context.compaction.clone(),
            ledger: Some(self.ledger.clone()),
            queued: self.queued.iter().cloned().collect(),
            results: self.results.iter().cloned().collect(),
            compacted_at: self.compacted_at.clone(),
        }
    }
}

#[workflow_methods]
impl TemporalAgentSessionWorkflow {
    #[init]
    fn init(ctx: &WorkflowContextView, input: TemporalAgentSessionInput) -> Self {
        let mut init_error = None;
        let context = match ContextState::new(input.history).with_compaction(input.compaction) {
            Ok(context) => context,
            Err(error) => {
                init_error = Some(error.to_string());
                ContextState::default()
            }
        };
        let ledger = input.ledger.unwrap_or_else(|| {
            SubmissionLedger::new(execution_chain_id(ctx), DEFAULT_LEDGER_MAX_BYTES)
                .with_next_prompt_index(input.next_prompt_index)
        });
        if !ledger.is_supported() {
            init_error.get_or_insert_with(|| {
                format!(
                    "unsupported submission ledger format version {}",
                    ledger.format_version
                )
            });
        }
        Self {
            runtime: TemporalRuntimeState {
                status: TemporalAgentStatus::Idle,
                steering: input.queued_steering.into(),
                next_prompt_index: ledger.next_prompt_index,
                ..TemporalRuntimeState::default()
            },
            config: input.config,
            context,
            run_base: (0, 0),
            ledger,
            preassigned_prompt_index: None,
            queued: input.queued.into(),
            results: input.results.into(),
            compacted_at: input.compacted_at,
            init_error,
            busy: false,
            compacting: false,
            closed: false,
        }
    }

    #[run(name = "RigTemporalAgentSessionV1")]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
    ) -> WorkflowResult<TemporalAgentSessionResult> {
        if let Some(error) = ctx.state(|workflow| workflow.init_error.clone()) {
            ctx.state_mut(close_session);
            return Err(workflow_error(error));
        }
        loop {
            let wait_ctx = ctx.clone();
            ctx.wait_condition(move |workflow| {
                !workflow.busy
                    && !workflow.compacting
                    && (workflow.pending_work()
                        || (wait_ctx.continue_as_new_suggested()
                            && wait_ctx.all_handlers_finished()))
            })
            .await?;

            if compact(ctx).await? {
                continue;
            }
            if run_queued_submission(ctx).await? {
                continue;
            }
            if run_steering(ctx).await? {
                continue;
            }

            let should_close = ctx.state(|workflow| workflow.closed);
            if should_close {
                break;
            }
            if ctx.continue_as_new_suggested() && ctx.all_handlers_finished() {
                let input = ctx.state(TemporalAgentSessionWorkflow::continue_as_new_input);
                return match ctx.continue_as_new(input, ContinueAsNewOptions::default()) {
                    Err(error) => Err(error),
                    Ok(never) => match never {},
                };
            }
        }
        let wait_ctx = ctx.clone();
        ctx.wait_condition(move |_| wait_ctx.all_handlers_finished())
            .await?;
        ctx.state_mut(|workflow| {
            workflow.ledger.cancel_pending();
            workflow.runtime.status = TemporalAgentStatus::Closed;
        });
        Ok(ctx.state(|workflow| TemporalAgentSessionResult {
            history: workflow.context.transcript.clone(),
            compaction: workflow.context.compaction.clone(),
            ledger: Some(workflow.ledger.clone()),
        }))
    }

    #[update_validator(prompt)]
    fn validate_prompt(
        &self,
        _ctx: &WorkflowContextView,
        prompt: &Message,
    ) -> Result<(), BoxedError> {
        self.check_prompt(prompt)
    }

    /// Run one prompt and any steering messages queued while it is active.
    #[update]
    pub async fn prompt(
        ctx: &mut WorkflowContext<Self>,
        prompt: Message,
    ) -> Result<PromptResponse, BoxedError> {
        ctx.wait_condition(|workflow| {
            workflow.closed || (!workflow.compacting && !workflow.compaction_due())
        })
        .await
        .map_err(|error| Box::new(error) as BoxedError)?;
        let start: Result<_, BoxedError> = ctx.state_mut(|workflow| {
            workflow.check_prompt(&prompt)?;
            Ok(workflow.begin_run())
        });
        let (config, history) = start?;
        let result = run_session_agent(
            ctx,
            TemporalAgentInput {
                prompt,
                history,
                config,
            },
        )
        .await;
        ctx.state_mut(|workflow| workflow.end_run(result.is_err()));
        result
            .map(|detailed| detailed.response)
            .map_err(|error| Box::new(error) as BoxedError)
    }

    #[update_validator(submit)]
    fn validate_submit(
        &self,
        _ctx: &WorkflowContextView,
        input: &SubmitInput,
    ) -> Result<(), BoxedError> {
        if self.init_error.is_some() {
            return Err(SubmissionError::Closed.to_string().into());
        }
        self.check_submission_payload(input)
            .map_err(|error| submission_error(&error))?;
        let mut ledger = self.ledger.clone();
        ledger
            .admit(input, self.availability())
            .map(drop)
            .map_err(|error| submission_error(&error))
    }

    /// Admit a prompt with a client request ID and wait for its answer.
    ///
    /// A retried request receives its original receipt. The update fails
    /// without a history event when the ledger rejects the request; the
    /// message starts with the `SubmissionError` reason tag.
    #[update]
    pub async fn submit(
        ctx: &mut WorkflowContext<Self>,
        input: SubmitInput,
    ) -> Result<TemporalSubmission, BoxedError> {
        let request_id = input.request_id.clone();
        let admitted: Result<Submission, SubmissionError> = ctx.state_mut(|workflow| {
            workflow.check_submission_payload(&input)?;
            let availability = workflow.availability();
            match workflow.ledger.admit(&input, availability)? {
                Admission::Admitted(submission) => {
                    workflow.queued.push_back(input);
                    Ok(submission)
                }
                Admission::Existing(submission) => Ok(submission),
            }
        });
        let submission = admitted.map_err(|error| submission_error(&error))?;
        let wait_id = request_id.clone();
        ctx.wait_condition(move |workflow| {
            workflow
                .ledger
                .receipt(&wait_id)
                .is_none_or(|receipt| receipt.state.is_terminal())
        })
        .await
        .map_err(|error| Box::new(error) as BoxedError)?;
        Ok(ctx.state(|workflow| {
            let submission = workflow
                .ledger
                .receipt(&request_id)
                .cloned()
                .unwrap_or(submission);
            let response = workflow
                .results
                .iter()
                .find(|result| result.submission_id == submission.submission_id)
                .map(|result| result.response.clone());
            TemporalSubmission {
                submission,
                response,
            }
        }))
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

    /// Stop admitting prompts. Admitted submissions and queued steering still
    /// run before the workflow completes.
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
            history: self.context.transcript.clone(),
            queued_steering: self.runtime.steering.len(),
            next_prompt_index: self.ledger.next_prompt_index,
            busy: self.busy,
            closed: self.closed,
            compaction_cutoff: self.context.applied_cutoff(),
            queued_submissions: self.queued.len(),
            ledger_receipts: self.ledger.receipts.len(),
            compaction_error: self.compaction_error(),
        }
    }

    /// Receipt of one client request, if the ledger has it.
    #[query]
    pub fn receipt(&self, _ctx: &WorkflowContextView, request_id: String) -> Option<Submission> {
        self.ledger.receipt(&request_id).cloned()
    }

    #[query]
    pub fn ledger(&self, _ctx: &WorkflowContextView) -> SubmissionLedger {
        self.ledger.clone()
    }

    /// Dispositions of the current or latest run, in dispatch order.
    #[query]
    pub fn tool_outcomes(&self, _ctx: &WorkflowContextView) -> Vec<ToolOutcome> {
        self.runtime.outcomes.clone()
    }

    /// Detailed response of one submission, while the session retains it.
    #[query]
    pub fn result(
        &self,
        _ctx: &WorkflowContextView,
        submission_id: String,
    ) -> Option<DurableResponse> {
        self.results
            .iter()
            .find(|result| result.submission_id == submission_id)
            .map(|result| result.response.clone())
    }
}

impl TemporalAgentSessionWorkflow {
    fn check_submission_payload(&self, input: &SubmitInput) -> Result<(), SubmissionError> {
        if self.ledger.receipt(&input.request_id).is_none()
            && session_payload_too_large(&self.config, &self.context.transcript, &input.message)
        {
            return Err(SubmissionError::Invalid {
                message: "agent session history exceeds configured limit".into(),
            });
        }
        Ok(())
    }

    fn check_prompt(&self, prompt: &Message) -> Result<(), BoxedError> {
        if self.closed || self.init_error.is_some() {
            Err("agent session is closed".into())
        } else if self.busy || !self.queued.is_empty() {
            Err("agent session is processing another prompt".into())
        } else if session_payload_too_large(&self.config, &self.context.transcript, prompt) {
            Err("agent session history exceeds configured limit".into())
        } else {
            Ok(())
        }
    }
}

/// Run the oldest admitted submission. Returns `false` when none is queued.
async fn run_queued_submission(
    ctx: &mut WorkflowContext<TemporalAgentSessionWorkflow>,
) -> WorkflowResult<bool> {
    let next = ctx.state_mut(|workflow| {
        if workflow.busy {
            return None;
        }
        let input = workflow.queued.pop_front()?;
        let receipt = workflow.ledger.receipt(&input.request_id).cloned()?;
        workflow.preassigned_prompt_index = Some(receipt.prompt_index);
        workflow
            .ledger
            .set_state(&input.request_id, SubmissionState::Running);
        let (config, history) = workflow.begin_run();
        Some((input, receipt, config, history))
    });
    let Some((input, receipt, config, history)) = next else {
        return Ok(false);
    };
    if ctx.state(|workflow| {
        session_payload_too_large(&config, &workflow.context.transcript, &input.message)
    }) {
        let error = "agent session history exceeds configured limit";
        ctx.state_mut(|workflow| {
            workflow.ledger.set_state(
                &input.request_id,
                SubmissionState::Failed {
                    error: error.to_string(),
                },
            );
            workflow.ledger.cancel_pending();
            workflow.end_run(true);
        });
        let wait_ctx = ctx.clone();
        ctx.wait_condition(move |_| wait_ctx.all_handlers_finished())
            .await?;
        return Err(workflow_error(error));
    }
    let result = run_session_agent(
        ctx,
        TemporalAgentInput {
            prompt: input.message,
            history,
            config,
        },
    )
    .await;
    let failed = result.is_err();
    let state = match &result {
        Ok(_) => SubmissionState::Answered,
        Err(error) => SubmissionState::Failed {
            error: error.to_string(),
        },
    };
    ctx.state_mut(|workflow| {
        workflow.ledger.set_state(&input.request_id, state);
        if let Ok(response) = &result {
            workflow.retain_result(receipt.submission_id.clone(), response.clone());
        } else {
            workflow.ledger.cancel_pending();
        }
        workflow.end_run(failed);
    });
    if let Err(error) = result {
        let wait_ctx = ctx.clone();
        ctx.wait_condition(move |_| wait_ctx.all_handlers_finished())
            .await?;
        return Err(error);
    }
    Ok(true)
}

/// Run the oldest queued steering message as a prompt. Returns `false` when
/// none is queued.
async fn run_steering(
    ctx: &mut WorkflowContext<TemporalAgentSessionWorkflow>,
) -> WorkflowResult<bool> {
    let steering = ctx.state_mut(|workflow| {
        if workflow.busy {
            return None;
        }
        let prompt = workflow.runtime.steering.pop_front()?;
        let (config, history) = workflow.begin_run();
        Some((prompt, config, history))
    });
    let Some((prompt, config, history)) = steering else {
        return Ok(false);
    };
    if ctx
        .state(|workflow| session_payload_too_large(&config, &workflow.context.transcript, &prompt))
    {
        ctx.state_mut(|workflow| workflow.end_run(true));
        let wait_ctx = ctx.clone();
        ctx.wait_condition(move |_| wait_ctx.all_handlers_finished())
            .await?;
        return Err(workflow_error(
            "agent session history exceeds configured limit",
        ));
    }
    let result = run_session_agent(
        ctx,
        TemporalAgentInput {
            prompt,
            history,
            config,
        },
    )
    .await;
    ctx.state_mut(|workflow| workflow.end_run(result.is_err()));
    if result.is_err() {
        let wait_ctx = ctx.clone();
        ctx.wait_condition(move |_| wait_ctx.all_handlers_finished())
            .await?;
    }
    result?;
    Ok(true)
}

/// Run one compaction round over the transcript. A round that fails, or that
/// demotes nothing new, leaves the context unchanged and does not fail the
/// session. Returns `false` when no compaction is due.
async fn compact(ctx: &mut WorkflowContext<TemporalAgentSessionWorkflow>) -> WorkflowResult<bool> {
    let planned = ctx.state_mut(|workflow| {
        if workflow.compacting || !workflow.compaction_due() {
            return None;
        }
        let version = workflow.config.compaction.as_ref()?.version.clone();
        workflow.compacting = true;
        workflow.runtime.status = TemporalAgentStatus::Compacting;
        let request = workflow
            .context
            .request(workflow.ledger.logical_session_id.clone(), &version);
        Some((request, workflow.config.clone(), version))
    });
    let Some((request, config, version)) = planned else {
        return Ok(false);
    };
    let transcript_len = request.transcript.len();
    let result = ctx
        .execute_activity(
            TemporalActivities::compact,
            request,
            activity_options(&config),
        )
        .await;
    ctx.state_mut(|workflow| {
        let error = match result {
            Ok(output) => workflow
                .context
                .apply(output, &version)
                .err()
                .map(|error| error.to_string()),
            Err(error) => Some(error.to_string()),
        };
        workflow.compacted_at = Some((transcript_len, error));
        workflow.compacting = false;
        workflow.runtime.status = if workflow.closed {
            TemporalAgentStatus::Closed
        } else {
            TemporalAgentStatus::Idle
        };
    });
    Ok(true)
}

fn submission_error(error: &SubmissionError) -> BoxedError {
    format!("{}: {error}", error.reason()).into()
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
    tools: ToolExecutor,
    compaction: Option<Compaction>,
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

    /// Legacy contract: payloads are byte-identical to registrations made
    /// before tool policies existed.
    #[activity(name = "RigTemporalToolExecutionV1")]
    async fn execute_tool(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ToolActivityInput,
    ) -> Result<ToolActivityOutput, ActivityError> {
        self.tools.execute(input).await.map_err(activity_error)
    }

    /// Logical contract: the input carries the logical call key and policy;
    /// the activity adds the physical attempt Temporal reports.
    #[activity(name = "RigTemporalToolExecutionV2")]
    async fn execute_logical_tool(
        self: Arc<Self>,
        ctx: ActivityContext,
        mut input: ToolActivityInput,
    ) -> Result<ToolActivityOutput, ActivityError> {
        let attempt = input
            .invocation
            .attempt
            .get_or_insert_with(Default::default);
        attempt.activity_attempt = Some(ctx.info().attempt);
        self.tools.execute(input).await.map_err(activity_error)
    }

    /// Run the registered policy and compactor over a session transcript.
    #[activity(name = "RigTemporalContextCompactionV1")]
    async fn compact(
        self: Arc<Self>,
        _ctx: ActivityContext,
        request: CompactionRequest,
    ) -> Result<CompactionOutput, ActivityError> {
        activities::compaction::compact(self.compaction.as_ref(), request)
            .await
            .map_err(activity_error)
    }
}

fn activity_error(message: String) -> ActivityError {
    ApplicationFailure::new(message).into()
}

/// A worker-side Rig model, tool set, and serializable workflow configuration.
///
/// See the [module examples](self) for worker and client setup. Configure tool
/// policies before calling [`Self::register`]; registration rejects guarded
/// tools unless [`Self::invocation_guard`] supplies a shared store.
pub struct TemporalAgent {
    model: DynModel<Completion>,
    tools: Arc<ToolSet>,
    guard: Option<Arc<dyn InvocationGuardStore>>,
    config: TemporalAgentConfig,
    compaction: Option<Compaction>,
}

impl TemporalAgent {
    /// Use a Rig completion model with the logical invocation contract.
    pub fn new(model: impl Into<DynModel<Completion>>) -> Self {
        Self {
            model: model.into(),
            tools: Arc::new(ToolSet::default()),
            guard: None,
            config: TemporalAgentConfig::default(),
            compaction: None,
        }
    }

    /// Select the tool activity contract for inputs this agent builds.
    pub fn invocation_contract(mut self, contract: InvocationContract) -> Self {
        self.config.contract = contract;
        self
    }

    /// Store that tools with [`crate::ReplaySafety::InterruptOnUncertain`]
    /// claim before they run. Share one store across the workers that may
    /// pick up the same activity.
    pub fn invocation_guard(mut self, guard: Arc<dyn InvocationGuardStore>) -> Self {
        self.guard = Some(guard);
        self
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

    /// Limit the serialized full session transcript, including compacted messages.
    /// An oversized input is rejected; an oversized completed turn fails the session.
    pub fn session_history_max_bytes(mut self, bytes: usize) -> Self {
        self.config.session_history_max_bytes = bytes.max(1);
        self
    }

    /// Compact session context after completed prompts with a Rig memory
    /// policy and compactor. The pair runs in an activity on this worker.
    /// Applies to session workflows only.
    pub fn compaction(mut self, compaction: Compaction) -> Self {
        self.config.compaction = Some(compaction.config());
        self.compaction = Some(compaction);
        self
    }

    pub fn tool<T>(self, tool: T) -> Self
    where
        T: Tool + 'static,
    {
        self.tool_with_options(tool, false, ToolPolicy::default())
    }

    /// Pause for an approval decision before executing this Rig tool.
    /// Read the request from the workflow status query and send its exact ID
    /// through the approval signal. Approval does not change replay safety.
    pub fn approval_tool<T>(self, tool: T) -> Self
    where
        T: Tool + 'static,
    {
        self.tool_with_options(tool, true, ToolPolicy::default())
    }

    /// Register a tool with a replay policy. Requires the logical contract.
    pub fn tool_with_policy<T>(self, tool: T, policy: ToolPolicy) -> Self
    where
        T: Tool + 'static,
    {
        self.tool_with_options(tool, false, policy)
    }

    /// Register an approval-gated tool with a replay policy.
    pub fn approval_tool_with_policy<T>(self, tool: T, policy: ToolPolicy) -> Self
    where
        T: Tool + 'static,
    {
        self.tool_with_options(tool, true, policy)
    }

    fn tool_with_options<T>(mut self, tool: T, requires_approval: bool, policy: ToolPolicy) -> Self
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
        let tool = TemporalTool {
            definition,
            requires_approval,
            policy,
        };
        if let Some(existing) = self
            .config
            .tools
            .iter_mut()
            .find(|existing| existing.definition.name == name)
        {
            *existing = tool;
        } else {
            self.config.tools.push(tool);
        }
        self
    }

    /// Capture the configuration for one independent prompt with no prior history.
    pub fn input(&self, prompt: impl Into<Message>) -> TemporalAgentInput {
        TemporalAgentInput {
            prompt: prompt.into(),
            history: Vec::new(),
            config: self.config.clone(),
        }
    }

    /// Capture a prompt and canonical Rig history for an independent run.
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

    /// Capture the configuration and optional Rig history for a new session.
    pub fn session_input(&self, history: Vec<Message>) -> TemporalAgentSessionInput {
        TemporalAgentSessionInput {
            history,
            config: self.config.clone(),
            queued_steering: Vec::new(),
            next_prompt_index: 0,
            compaction: None,
            ledger: None,
            queued: Vec::new(),
            results: Vec::new(),
            compacted_at: None,
        }
    }

    /// Register the workflow and this agent's model and tools on a worker.
    ///
    /// Registration consumes the agent because the worker retains its model and tool set.
    /// It fails closed when a tool policy needs a contract or guard store the agent lacks.
    pub fn register(self, options: &mut WorkerOptions) -> Result<(), TemporalAgentError> {
        for tool in &self.config.tools {
            let name = &tool.definition.name;
            if self.config.contract == InvocationContract::Legacy
                && tool.policy != ToolPolicy::default()
            {
                return Err(TemporalAgentError::InvocationContractRequired(name.clone()));
            }
            if tool.policy.safety().requires_guard() && self.guard.is_none() {
                return Err(TemporalAgentError::GuardStoreRequired(name.clone()));
            }
        }
        options.register_workflow::<TemporalAgentWorkflow>()?;
        options.register_workflow::<TemporalAgentSessionWorkflow>()?;
        let policies = self
            .config
            .tools
            .iter()
            .map(|tool| (tool.definition.name.clone(), tool.policy.clone()));
        options.register_activities(TemporalActivities {
            model: self.model,
            tools: ToolExecutor::new(self.tools)
                .with_guard(self.guard)
                .with_registered_policies(policies),
            compaction: self.compaction,
        });
        Ok(())
    }
}

/// Keep the session reserved while compaction and accepted steering finish.
async fn run_session_agent(
    ctx: &mut WorkflowContext<TemporalAgentSessionWorkflow>,
    mut input: TemporalAgentInput,
) -> WorkflowResult<DurableResponse> {
    let mut outcomes = Vec::new();
    loop {
        let mut response = run_agent(ctx, input).await?;
        outcomes.append(&mut response.tool_outcomes);
        ctx.state_mut(|workflow| workflow.runtime.outcomes = outcomes.clone());
        compact(ctx).await?;
        let next = ctx.state_mut(|workflow| {
            let prompt = workflow.runtime.steering.pop_front()?;
            let (config, history) = workflow.begin_run();
            Some(TemporalAgentInput {
                prompt,
                history,
                config,
            })
        });
        let Some(next) = next else {
            response.tool_outcomes = outcomes;
            return Ok(response);
        };
        if ctx.state(|workflow| {
            session_payload_too_large(&next.config, &workflow.context.transcript, &next.prompt)
        }) {
            return Err(workflow_error(
                "agent session history exceeds configured limit",
            ));
        }
        input = next;
    }
}

async fn run_agent<W>(
    ctx: &mut WorkflowContext<W>,
    input: TemporalAgentInput,
) -> WorkflowResult<DurableResponse>
where
    W: TemporalWorkflowState,
{
    ctx.state_mut(|workflow| workflow.runtime_mut().outcomes.clear());
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
                    let mut denied = false;
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
                            let version = tool.policy.version().to_string();
                            (pending, denied) = await_approval(
                                ctx,
                                pending,
                                config.contract,
                                &version,
                                prompt_index,
                                model_turn,
                                index,
                            )
                            .await?;
                        }
                    }
                    resolved.push(ResolvedCall { pending, denied });
                }
                ctx.state_mut(|workflow| {
                    workflow.runtime_mut().status = TemporalAgentStatus::Tools {
                        count: resolved.len(),
                    }
                });
                let results = join_all(resolved.into_iter().enumerate().map(
                    |(call_index, resolved)| {
                        execute_tool(
                            ctx,
                            resolved,
                            &config,
                            CallPosition {
                                prompt_index,
                                turn: model_turn,
                                call_index,
                            },
                        )
                    },
                ))
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
                let (results, outcomes): (Vec<_>, Vec<_>) = results.into_iter().unzip();
                ctx.state_mut(|workflow| workflow.runtime_mut().outcomes.extend(outcomes));
                agent.tool_results(results).map_err(workflow_error)?;
            }
            AgentRunStep::Done(response) => {
                let history = agent.full_history();
                ctx.state_mut(|workflow| workflow.record_history(history.clone()))
                    .map_err(workflow_error)?;
                let steering = ctx.state_mut(|workflow| {
                    if workflow.records_history() {
                        None
                    } else {
                        workflow.runtime_mut().steering.pop_front()
                    }
                });
                let Some(prompt) = steering else {
                    let outcomes = ctx.state_mut(|workflow| {
                        let runtime = workflow.runtime_mut();
                        runtime.status = TemporalAgentStatus::Completed;
                        runtime.outcomes.clone()
                    });
                    return Ok(DurableResponse::new(response, outcomes));
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
    ctx.state_mut(|workflow| workflow.allocate_prompt_index().map_err(workflow_error))
}

struct ResolvedCall {
    pending: rig::agent::PendingToolCall,
    /// A human denied the call; `pending` carries the denial result.
    denied: bool,
}

fn execution_chain_id(ctx: &WorkflowContextView) -> String {
    let namespace = ctx.namespace();
    format!(
        "temporal:{}:{namespace}:{}",
        namespace.len(),
        ctx.first_execution_run_id()
    )
}

/// Identity of one logical tool call, scoped to a namespace and execution chain.
fn logical_key<W>(
    ctx: &WorkflowContext<W>,
    prompt_index: u64,
    turn: usize,
    call_index: usize,
) -> LogicalCallKey {
    LogicalCallKey {
        logical_execution_id: execution_chain_id(&ctx.info()),
        submission_id: LogicalCallKey::submission_for_prompt(prompt_index),
        model_turn: turn,
        call_index,
    }
}

#[allow(clippy::too_many_arguments)]
async fn await_approval<W>(
    ctx: &mut WorkflowContext<W>,
    mut pending: rig::agent::PendingToolCall,
    contract: InvocationContract,
    implementation_version: &str,
    prompt_index: u64,
    turn: usize,
    call_index: usize,
) -> Result<(rig::agent::PendingToolCall, bool), temporalio_sdk::WorkflowTermination>
where
    W: TemporalWorkflowState,
{
    let call = &pending.tool_call;
    let request = match contract {
        InvocationContract::Legacy => {
            driver::approval_request(call, prompt_index, turn, call_index)
        }
        InvocationContract::Logical => driver::logical_approval_request(
            call,
            &logical_key(ctx, prompt_index, turn, call_index),
            implementation_version,
        ),
    }
    .map_err(workflow_error)?;
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
        return Ok((pending, true));
    }
    Ok((pending, false))
}

async fn execute_tool<W>(
    ctx: &WorkflowContext<W>,
    resolved: ResolvedCall,
    config: &TemporalAgentConfig,
    position: CallPosition,
) -> Result<(UserContent, ToolOutcome), temporalio_sdk::WorkflowTermination> {
    let ResolvedCall { pending, denied } = resolved;
    let CallPosition {
        prompt_index,
        turn,
        call_index,
    } = position;
    if let Some(result) = pending.preresolved_result {
        let outcome = if denied {
            ToolOutcome::denied(position, &pending.tool_call)
        } else {
            ToolOutcome::preresolved(position, &pending.tool_call)
        };
        return Ok((result, outcome));
    }
    let call = pending.tool_call;
    let name = call.function.name.to_string();
    let arguments = serde_json::to_string(&call.function.arguments).map_err(workflow_error)?;
    let execution_id = format!("{}:{}", ctx.workflow_id(), ctx.run_id());
    let output = match config.contract {
        InvocationContract::Legacy => {
            ctx.execute_activity(
                TemporalActivities::execute_tool,
                ToolActivityInput {
                    name,
                    arguments,
                    invocation: ToolInvocation::legacy(
                        execution_id,
                        prompt_index,
                        turn,
                        call_index,
                    ),
                    policy: None,
                },
                activity_options(config),
            )
            .await?
        }
        InvocationContract::Logical => {
            let policy = config
                .tools
                .iter()
                .find(|tool| tool.definition.name == name)
                .map(|tool| tool.policy.clone())
                .ok_or_else(|| workflow_error(format!("tool `{name}` is not registered")))?;
            ctx.execute_activity(
                TemporalActivities::execute_logical_tool,
                ToolActivityInput {
                    name,
                    arguments,
                    invocation: ToolInvocation {
                        execution_id: execution_id.clone(),
                        prompt_index,
                        turn,
                        call_index,
                        logical_key: Some(logical_key(ctx, prompt_index, turn, call_index)),
                        attempt: Some(AttemptMetadata {
                            backend_execution_id: execution_id,
                            activity_attempt: None,
                        }),
                    },
                    policy: Some(policy),
                },
                activity_options(config),
            )
            .await?
        }
    };
    let outcome = ToolOutcome::from_activity(position, &call, &output);
    Ok((driver::tool_result(&call, output.content), outcome))
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
