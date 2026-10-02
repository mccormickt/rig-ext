//! Long-lived Duroxide session: a persistent inbox of submissions with
//! lifetime deduplication, ordered follow-ups, and completed-prompt
//! compaction.
//!
//! Clients enqueue [`SessionCommand`]s on [`SESSION_INBOX_QUEUE`]. The
//! orchestration admits them through the [`SubmissionLedger`] at operation
//! boundaries and publishes the ledger, rejections, and per-prompt results
//! through Duroxide's per-instance KV store, which survives
//! continue-as-new.

use std::{collections::VecDeque, time::Duration};

use duroxide::{Either2, OrchestrationContext};
use rig::agent::AgentRun;
use serde::{Deserialize, Serialize};

use crate::{
    activity_types::InvocationContract,
    compaction::{CompactionError, ContextState},
    config::{CheckpointPolicy, ConfigSnapshot, DurableAgentConfig},
    names::RuntimeNames,
    orchestration::{Cursor, Engine, KV_VALUE_LIMIT, Step, set_status},
    outcome::{DurableResponse, ToolOutcome},
    submission::{
        Admission, SessionAvailability, SubmissionError, SubmissionLedger, SubmissionState,
        SubmitInput,
    },
};

pub const SESSION_FORMAT_VERSION: u32 = 1;
/// Duroxide queue that carries [`SessionCommand`]s.
pub const SESSION_INBOX_QUEUE: &str = "RigSessionInboxV1";
/// KV key of the [`SubmissionLedger`].
pub const SESSION_LEDGER_KEY: &str = "rig_durable.session.ledger.v1";
/// KV key of the recent [`SessionRejection`]s.
pub const SESSION_REJECTIONS_KEY: &str = "rig_durable.session.rejections.v1";
/// KV key prefix of per-prompt [`DurableResponse`]s.
pub const SESSION_RESULT_KEY_PREFIX: &str = "rig_durable.session.result.v1.";
/// Number of per-prompt results a session retains in KV.
pub const SESSION_RESULT_RETENTION: usize = 32;
/// Number of rejections a session retains in KV.
pub const SESSION_REJECTION_RETENTION: usize = 64;

pub fn session_result_key(submission_id: &str) -> String {
    format!("{SESSION_RESULT_KEY_PREFIX}{submission_id}")
}

/// Orchestration input of a session. It is also the continuation payload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionInput {
    pub format_version: u32,
    pub context: ContextState,
    pub ledger: SubmissionLedger,
    /// Admitted submissions that have not run yet, in admission order.
    #[serde(default)]
    pub queued: Vec<SubmitInput>,
    /// Submission IDs whose results are retained in KV, oldest first.
    #[serde(default)]
    pub retained_results: Vec<String>,
    /// Recent rejections, oldest first. The orchestration never reads KV
    /// back, because a replayed turn sees the latest store, not the store
    /// at the time of the original turn.
    #[serde(default)]
    pub rejections: Vec<SessionRejection>,
    #[serde(default)]
    pub generation: u32,
    #[serde(default)]
    pub closed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<ConfigSnapshot>,
}

impl SessionInput {
    pub fn new(logical_session_id: impl Into<String>, ledger_max_bytes: usize) -> Self {
        Self {
            format_version: SESSION_FORMAT_VERSION,
            context: ContextState::default(),
            ledger: SubmissionLedger::new(logical_session_id, ledger_max_bytes),
            queued: Vec::new(),
            retained_results: Vec::new(),
            rejections: Vec::new(),
            generation: 0,
            closed: false,
            snapshot: None,
        }
    }

    pub fn with_history(mut self, history: Vec<rig::completion::Message>) -> Self {
        self.context = ContextState::new(history);
        self
    }

    pub fn with_snapshot(mut self, snapshot: Option<ConfigSnapshot>) -> Self {
        self.snapshot = snapshot;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum SessionCommand {
    Submit {
        /// Identifies this delivery so the client can find its rejection.
        command_id: String,
        input: SubmitInput,
    },
    Close,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRejection {
    pub command_id: String,
    pub request_id: String,
    pub error: SubmissionError,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SessionRejections {
    pub entries: VecDeque<SessionRejection>,
}

/// Output of a closed session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionResult {
    pub format_version: u32,
    /// Full audit transcript and the applied compaction record.
    pub context: ContextState,
    pub ledger: SubmissionLedger,
}

struct SessionState {
    context: ContextState,
    ledger: SubmissionLedger,
    queued: VecDeque<SubmitInput>,
    retained_results: Vec<String>,
    rejections: SessionRejections,
    closed: bool,
    running: Option<String>,
    /// Error of the latest compaction attempt, if it failed.
    compaction_error: Option<String>,
    ledger_dirty: bool,
    rejections_dirty: bool,
}

impl SessionState {
    fn availability(&self) -> SessionAvailability {
        SessionAvailability {
            busy: self.running.is_some(),
            closed: self.closed,
        }
    }

    fn handle(&mut self, raw: &str) {
        let Ok(command) = serde_json::from_str::<SessionCommand>(raw) else {
            return;
        };
        match command {
            SessionCommand::Submit { command_id, input } => {
                let request_id = input.request_id.clone();
                match self.ledger.admit(&input, self.availability()) {
                    Ok(Admission::Admitted(_)) => {
                        self.queued.push_back(input);
                        self.ledger_dirty = true;
                    }
                    Ok(Admission::Existing(_)) => {}
                    Err(error) => {
                        self.rejections.entries.push_back(SessionRejection {
                            command_id,
                            request_id,
                            error,
                        });
                        while self.rejections.entries.len() > SESSION_REJECTION_RETENTION {
                            self.rejections.entries.pop_front();
                        }
                        self.rejections_dirty = true;
                    }
                }
            }
            SessionCommand::Close => {
                self.closed = true;
            }
        }
    }

    /// Dequeue every command already in the inbox without blocking.
    async fn drain(&mut self, ctx: &OrchestrationContext) {
        while let Either2::First(raw) = ctx
            .select2(
                ctx.dequeue_event(SESSION_INBOX_QUEUE),
                ctx.schedule_timer(Duration::ZERO),
            )
            .await
        {
            self.handle(&raw);
        }
        self.flush(ctx);
    }

    fn flush(&mut self, ctx: &OrchestrationContext) {
        if self.ledger_dirty {
            ctx.set_kv_value_typed(SESSION_LEDGER_KEY, &self.ledger);
            self.ledger_dirty = false;
        }
        if self.rejections_dirty {
            ctx.set_kv_value_typed(SESSION_REJECTIONS_KEY, &self.rejections);
            self.rejections_dirty = false;
        }
    }

    fn set_state(&mut self, request_id: &str, state: SubmissionState) {
        self.ledger.set_state(request_id, state);
        self.ledger_dirty = true;
    }

    fn retain_result(
        &mut self,
        ctx: &OrchestrationContext,
        submission_id: &str,
        result: &DurableResponse,
    ) {
        ctx.set_kv_value_typed(session_result_key(submission_id), result);
        self.retained_results.push(submission_id.to_owned());
        while self.retained_results.len() > SESSION_RESULT_RETENTION {
            let evicted = self.retained_results.remove(0);
            ctx.clear_kv_value(session_result_key(&evicted));
        }
    }

    fn status(&self, phase: &str) -> serde_json::Value {
        let mut status = serde_json::json!({"phase": phase});
        self.decorate(&mut status);
        status
    }

    fn decorate(&self, status: &mut serde_json::Value) {
        status["session"] = serde_json::json!({
            "busy": self.running.is_some(),
            "closed": self.closed,
            "running": self.running,
            "queued": self.queued.len(),
            "transcript_len": self.context.transcript.len(),
            "compaction_cutoff": self.context.applied_cutoff(),
            "compaction_error": self.compaction_error,
        });
    }
}

pub(crate) async fn run_session(
    ctx: OrchestrationContext,
    input: SessionInput,
    config: DurableAgentConfig,
    names: RuntimeNames,
) -> Result<SessionResult, String> {
    if input.format_version != SESSION_FORMAT_VERSION {
        return Err(format!(
            "unsupported session format version {}",
            input.format_version
        ));
    }
    if !input.ledger.is_supported() {
        return Err(format!(
            "unsupported submission ledger format version {}",
            input.ledger.format_version
        ));
    }
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
    input
        .context
        .validate_active_context()
        .map_err(|error| format!("session context is not canonical: {error}"))?;
    let generation = input.generation;
    let mut state = SessionState {
        context: input.context,
        ledger: input.ledger,
        queued: input.queued.into(),
        retained_results: input.retained_results,
        rejections: SessionRejections {
            entries: input.rejections.into(),
        },
        closed: input.closed,
        running: None,
        compaction_error: None,
        ledger_dirty: generation == 0,
        rejections_dirty: false,
    };
    state.flush(&ctx);
    let engine = Engine {
        ctx: &ctx,
        config: &config,
        names: &names,
    };
    let mut operations: u32 = 0;

    loop {
        ctx.set_custom_status(state.status("idle").to_string());
        if state.queued.is_empty() && !state.closed {
            let raw = ctx.dequeue_event(SESSION_INBOX_QUEUE).await;
            state.handle(&raw);
        }
        state.drain(&ctx).await;
        let Some(input) = state.queued.pop_front() else {
            if state.closed {
                break;
            }
            continue;
        };
        operations = run_prompt(&engine, &mut state, input, operations).await?;
        if let CheckpointPolicy::Every(threshold) = config.checkpoint.policy
            && operations >= threshold.get()
        {
            return checkpoint(&ctx, state, generation, &config, snapshot).await;
        }
    }

    state.ledger.cancel_pending();
    state.ledger_dirty = true;
    state.flush(&ctx);
    ctx.set_custom_status(state.status("closed").to_string());
    Ok(SessionResult {
        format_version: SESSION_FORMAT_VERSION,
        context: state.context,
        ledger: state.ledger,
    })
}

async fn run_prompt(
    engine: &Engine<'_>,
    state: &mut SessionState,
    input: SubmitInput,
    mut operations: u32,
) -> Result<u32, String> {
    let ctx = engine.ctx;
    let config = engine.config;
    let receipt = state
        .ledger
        .receipt(&input.request_id)
        .cloned()
        .ok_or_else(|| format!("queued request `{}` has no receipt", input.request_id))?;
    state.running = Some(receipt.submission_id.clone());
    state.set_state(&input.request_id, SubmissionState::Running);
    state.flush(ctx);

    let history = state.context.active_context();
    let base_len = history.len();
    let mut agent = AgentRun::new(input.message)
        .with_history(history)
        .max_turns(config.max_turns);
    if let Some(tool_choice) = config.completion.tool_choice.clone() {
        agent = agent.with_tool_choice(tool_choice);
    }
    let mut cursor = Cursor {
        prompt_index: receipt.prompt_index,
        model_turn: 0,
    };
    let mut outcomes: Vec<ToolOutcome> = Vec::new();
    let response = loop {
        let snapshot = state.status("");
        let decorate = move |status: &mut serde_json::Value| {
            status["session"] = snapshot["session"].clone();
        };
        match engine
            .advance(&mut agent, &mut cursor, &mut outcomes, &decorate)
            .await
        {
            Ok(Step::Continue) => {
                operations = operations.saturating_add(1);
                state.drain(ctx).await;
            }
            Ok(Step::Done(response)) => break *response,
            Err(error) => {
                state.set_state(
                    &input.request_id,
                    SubmissionState::Failed {
                        error: error.clone(),
                    },
                );
                state.closed = true;
                state.ledger.cancel_pending();
                state.running = None;
                state.flush(ctx);
                ctx.set_custom_status(state.status("failed").to_string());
                return Err(error);
            }
        }
    };

    let mut full = agent.full_history();
    let added = full.split_off(base_len);
    state.context.append(added);
    let detailed = DurableResponse::new(response, outcomes).fit_within(KV_VALUE_LIMIT);
    state.retain_result(ctx, &receipt.submission_id, &detailed);
    state.set_state(&input.request_id, SubmissionState::Answered);
    // Publish the answer before any summary runs.
    state.flush(ctx);
    if let Some(policy) = &config.compaction
        && let Some(request) = state.context.plan(policy)
    {
        set_status(ctx, state.status("compacting"), &|_| {});
        let summary = ctx
            .schedule_activity_with_retry_typed(
                &engine.names.compaction_activity,
                &request,
                config.completion_retry.clone(),
            )
            .await;
        // A summary that fails, or that does not advance the cutoff, leaves
        // the current context in place; the next prompt plans again.
        state.compaction_error = match summary {
            Ok(output) => match state.context.apply(output) {
                Ok(()) | Err(CompactionError::Stale { .. }) => None,
                Err(error) => Some(error.to_string()),
            },
            Err(error) => Some(error),
        };
    }
    // Commands that arrived while this prompt ran see the session as busy.
    state.drain(ctx).await;
    state.running = None;
    state.flush(ctx);
    Ok(operations)
}

async fn checkpoint(
    ctx: &OrchestrationContext,
    state: SessionState,
    generation: u32,
    config: &DurableAgentConfig,
    snapshot: Option<ConfigSnapshot>,
) -> Result<SessionResult, String> {
    let generation = generation
        .checked_add(1)
        .ok_or("checkpoint generation overflow")?;
    ctx.set_custom_status(state.status("checkpoint").to_string());
    let input = SessionInput {
        format_version: SESSION_FORMAT_VERSION,
        context: state.context,
        ledger: state.ledger,
        queued: state.queued.into(),
        retained_results: state.retained_results,
        rejections: state.rejections.entries.into(),
        generation,
        closed: state.closed,
        snapshot,
    };
    let payload = serde_json::to_string(&input).map_err(|error| error.to_string())?;
    let raw = match &config.checkpoint.target_version {
        Some(version) => ctx.continue_as_new_versioned(version, payload).await?,
        None => ctx.continue_as_new(payload).await?,
    };
    serde_json::from_str(&raw).map_err(|error| error.to_string())
}
