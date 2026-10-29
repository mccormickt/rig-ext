//! Rig runner planning over committed effects.
//!
//! A run starts from one prompt seed. Rig's runner resumes that seed and replays
//! committed answers until it needs external work. Handlers never perform I/O.
//! Backends commit admissions, execute work with their own scheduler, and commit
//! settlements before feeding answers back. Workflow replay still issues backend
//! commands: a new workflow activation rebuilds this state from its input.
//!
//! Records are scoped to this prompt, not a process or session. Backend byte
//! limits apply to the complete serialized run. Prompt boundaries discard this
//! replay window while the backend retains its audit transcript and receipts.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use rig::{
    AgentBuilder, AgentRun,
    agent::{
        AgentHook, CompletionCallAction, CompletionCallEvent, DispatchAction, DispatchEvent,
        HookContext, ModelTurnAction, ModelTurnFinished, PromptResponse,
    },
    completion::{CompletionRequest, CompletionResponse, Message, ToolDefinition},
    effect::{
        Answered, EffectId, EffectKind, FamilyDescriptor, HandlerDescriptor, HandlerKey, family,
    },
    error::{ErrorKind, ErrorReport},
    handlers::Handlers,
    message::{AssistantContent, ToolCall, ToolChoice, UserContent},
    serve::{Dispatch, DispatchContext, Recorder, Reply, Serve},
    tool::{RegisteredTool, ToolOutput, ToolResult, server::ToolServer},
};
use rig_cassette::{
    agent::AgentReplayExt,
    effect_log::{EffectLog, EffectLogRecorder, EffectLogReplayer, ReplayRefusals},
};
use serde::{Deserialize, Serialize};

/// Request configuration checked by Rig's replay stamp before planning.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionOptions {
    pub preamble: Option<String>,
    pub tools: Vec<ToolDefinition>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub tool_choice: Option<ToolChoice>,
    pub additional_params: Option<serde_json::Value>,
}

/// One backend operation. Tool calls retain their original batch positions.
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Effect {
    Model {
        request: CompletionRequest,
        turn: usize,
    },
    Tools {
        calls: Vec<PendingToolCall>,
    },
    Done(PromptResponse),
}

/// A runner dispatch admitted by a backend, with optional policy feedback.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingToolCall {
    pub tool_call: ToolCall,
    pub call_index: usize,
    pub dispatch_id: EffectId,
    pub preresolved_result: Option<UserContent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Pending {
    id: EffectId,
    key: HandlerKey,
    kind: EffectKind,
}

/// Serializable replay window for one prompt. It contains no futures or handles.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Run {
    seed: AgentRun,
    max_turns: usize,
    log: EffectLog,
    options: Option<CompletionOptions>,
    pending: Vec<Pending>,
    history: Vec<Message>,
    done: bool,
    #[serde(skip)]
    preresolved: Vec<crate::outcome::ToolOutcome>,
}

impl Run {
    pub fn new(prompt: impl Into<Message>) -> Self {
        let seed = AgentRun::new(prompt).max_turns(16);
        let history = seed.full_history();
        Self {
            seed,
            max_turns: 16,
            log: EffectLog::default(),
            options: None,
            pending: Vec::new(),
            history,
            done: false,
            preresolved: Vec::new(),
        }
    }

    pub fn with_history(mut self, history: Vec<Message>) -> Self {
        self.seed = self.seed.with_history(history);
        self.history = self.seed.full_history();
        self
    }

    pub fn max_turns(mut self, max_turns: usize) -> Self {
        self.seed = self.seed.max_turns(max_turns);
        self.max_turns = max_turns;
        self
    }

    pub fn full_history(&self) -> Vec<Message> {
        self.history.clone()
    }
    pub fn is_done(&self) -> bool {
        self.done
    }
    pub fn log(&self) -> &EffectLog {
        &self.log
    }

    /// Add dispositions for calls that Rig answered without handler dispatch.
    /// Call once at the completed prompt boundary, before retaining its result.
    pub fn complete_outcomes(
        &self,
        prompt_index: u64,
        outcomes: &mut Vec<crate::outcome::ToolOutcome>,
    ) {
        outcomes.extend(self.preresolved.iter().cloned().map(|mut outcome| {
            outcome.prompt_index = prompt_index;
            outcome
        }));
        outcomes.sort_by_key(|outcome| (outcome.prompt_index, outcome.turn, outcome.call_index));
    }

    /// Plan without executing an effect. Local polling cannot schedule workflow
    /// commands or touch SQL. Backends retain their native concurrency rules.
    pub fn plan(&mut self, options: CompletionOptions) -> Result<Effect, String> {
        if let Some(saved) = &self.options
            && serde_json::to_value(saved).map_err(|error| error.to_string())?
                != serde_json::to_value(&options).map_err(|error| error.to_string())?
        {
            return Err("Rig replay configuration changed".into());
        }
        let trace = Arc::new(Mutex::new(Trace::default()));
        let remaining = Arc::new(Mutex::new(
            self.log
                .records
                .iter()
                .map(|record| (record.id, record.key.clone()))
                .collect::<BTreeMap<_, _>>(),
        ));
        if lock(&remaining).len() != self.log.records.len() {
            return Err("Rig replay contains duplicate dispatch IDs".into());
        }
        let refusals = ReplayRefusals::default();
        let handler = |descriptor: HandlerDescriptor| -> Result<Handler, String> {
            let replay = if self
                .log
                .records
                .iter()
                .any(|record| record.key == descriptor.key)
            {
                Some(
                    EffectLogReplayer::for_key_by_id(&self.log, &descriptor.key)
                        .map_err(|error| error.to_string())?
                        .reporting_refusals(refusals.clone()),
                )
            } else {
                None
            };
            Ok(Handler {
                descriptor,
                replay,
                trace: trace.clone(),
                remaining: remaining.clone(),
            })
        };
        let handlers = Handlers::default();
        handlers
            .register(
                "durable:model",
                handler(HandlerDescriptor {
                    key: "durable:model".into(),
                    family: FamilyDescriptor::Completion {
                        model: "durable/model".into(),
                        capabilities: Default::default(),
                    },
                    layers: vec![],
                })?,
            )
            .map_err(|error| error.to_string())?;
        let mut tools = ToolServer::new().owner("durable");
        for definition in &options.tools {
            let mut descriptor = HandlerDescriptor::tool(
                definition.name.as_str(),
                &definition.description,
                definition.parameters.clone(),
            );
            descriptor.key = format!("durable:tool:{}", definition.name).into();
            tools = tools.registered_tool(
                RegisteredTool::from_handler(handler(descriptor)?)
                    .map_err(|error| error.to_string())?,
            );
        }
        let recorder = EffectLogRecorder::new();
        let mut builder = AgentBuilder::over_handlers(handlers, "durable", "durable:model".into())
            .tool_server_handle(tools.run())
            .default_max_turns(self.max_turns)
            .record_to(recorder.clone())
            .add_hook(TraceHook(trace.clone(), options.tools.clone()));
        if let Some(preamble) = &options.preamble {
            builder = builder.preamble(preamble);
        }
        if let Some(temperature) = options.temperature {
            builder = builder.temperature(temperature);
        }
        if let Some(max_tokens) = options.max_tokens {
            builder = builder.max_tokens(max_tokens);
        }
        if let Some(choice) = &options.tool_choice {
            builder = builder.tool_choice(choice.clone());
        }
        if let Some(params) = &options.additional_params {
            builder = builder.additional_params(params.clone());
        }
        let agent = builder.build();
        if !self.log.records.is_empty() {
            agent
                .check_replayable(&self.log)
                .map_err(|error| error.to_string())?;
        }
        let mut future = Box::pin(
            agent
                .resume(self.seed.clone())
                .without_memory()
                .tool_concurrency(usize::MAX)
                .run(),
        );
        let wake = Arc::new(PlannerWake(AtomicBool::new(false)));
        let waker = Waker::from(wake.clone());
        let mut context = Context::from_waker(&waker);
        // Each recorded dispatch and each emitted tool can wake this local
        // runner. There are no timers or external wake sources in the planner.
        let effects = self.log.records.iter().fold(0usize, |count, record| {
            count
                .saturating_add(1)
                .saturating_add(match &record.outcome {
                    Ok(Answered::Completion(response)) => response.choice.len(),
                    _ => 0,
                })
        });
        let mut polls = effects.saturating_mul(4).saturating_add(32);
        let outcome = loop {
            if polls == 0 {
                return Err("Rig replay exceeded its local poll budget".into());
            }
            polls -= 1;
            wake.0.store(false, Ordering::SeqCst);
            match future.as_mut().poll(&mut context) {
                Poll::Ready(result) => break Some(result.map_err(|error| error.to_string())?),
                Poll::Pending if !wake.0.swap(false, Ordering::SeqCst) => break None,
                Poll::Pending => {}
            }
        };
        if let Some(error) = refusals.take() {
            return Err(error.to_string());
        }
        if !lock(&remaining).is_empty() {
            return Err("Rig replay left unreachable recorded effects".into());
        }
        let mut trace = lock(&trace);
        self.options = Some(options);
        self.log.header = agent.stamp(recorder.log()).header;
        self.pending = std::mem::take(&mut trace.pending);
        self.preresolved = std::mem::take(&mut trace.preresolved);
        self.history = trace.history.clone();
        if let Some(response) = outcome {
            self.history = self
                .seed
                .input_chat_history()
                .iter()
                .map(|message| (**message).clone())
                .collect();
            self.history.extend(response.messages.iter().cloned());
            self.done = true;
            return Ok(Effect::Done(response));
        }
        if self.pending.is_empty() {
            return Err("Rig planner suspended without an effect".into());
        }
        match &self.pending[0].kind {
            EffectKind::Completion { request, .. } if self.pending.len() == 1 => {
                Ok(Effect::Model {
                    request: (**request).clone(),
                    turn: trace.turn,
                })
            }
            EffectKind::ToolCall { .. } => {
                let mut calls = Vec::new();
                // Include resolved siblings when locating a dispatch in the model's
                // batch. Dispatch ids are scoped to this seed and increase in order.
                let all = &trace.tools;
                for pending in &self.pending {
                    let index = all
                        .iter()
                        .position(|id| *id == pending.id)
                        .ok_or("Rig tool dispatch is absent from its batch")?;
                    let (call_index, tool_call) = trace
                        .content
                        .iter()
                        .filter_map(|item| match item {
                            AssistantContent::ToolCall(call) => Some(call),
                            _ => None,
                        })
                        .enumerate()
                        .filter(|(_, call)| call.function.invalid_arguments.is_none())
                        .nth(index)
                        .map(|(index, call)| (index, call.clone()))
                        .ok_or("Rig tool dispatch has no model call")?;
                    calls.push(PendingToolCall {
                        tool_call,
                        call_index,
                        dispatch_id: pending.id,
                        preresolved_result: None,
                    });
                }
                if let Some(response) =
                    self.log
                        .records
                        .iter()
                        .rev()
                        .find_map(|record| match &record.outcome {
                            Ok(Answered::Completion(response)) => Some(response),
                            _ => None,
                        })
                {
                    self.history.push(Message::Assistant(
                        response.head().with_content(trace.content.clone()),
                    ));
                }
                Ok(Effect::Tools { calls })
            }
            _ => Err("Rig planner emitted an unsupported effect batch".into()),
        }
    }

    /// Record one committed answer. Missing earlier answers do not block later
    /// settlements. A backend must persist this mutation before delivery/replay.
    pub fn settle(
        &mut self,
        id: EffectId,
        answer: Result<Answered, ErrorReport>,
    ) -> Result<(), String> {
        let index = self
            .pending
            .iter()
            .position(|pending| pending.id == id)
            .ok_or("settlement has no pending Rig dispatch")?;
        let pending = self.pending.remove(index);
        let recorder = EffectLogRecorder::new();
        recorder.begin(id, pending.key, pending.kind, DispatchContext::default());
        recorder.resolve(id, answer);
        self.log.records.extend(recorder.take().records);
        self.log.records.sort_by_key(|record| record.id);
        Ok(())
    }

    /// Record a completed model request and update the pure replay plan. Persist
    /// the run before executing any newly planned work.
    pub fn model_response(&mut self, response: CompletionResponse) -> Result<(), String> {
        let pending = self.pending.first().ok_or("missing model dispatch")?;
        if !matches!(pending.kind, EffectKind::Completion { .. }) {
            return Err("expected a model dispatch".into());
        }
        self.settle(pending.id, Ok(Answered::Completion(Arc::new(response))))?;
        self.refresh()
    }

    /// Record results in the pending plan's call order, then refresh the plan.
    /// For independent sparse settlements, use [`Self::settle`] instead.
    pub fn tool_results(&mut self, results: Vec<UserContent>) -> Result<(), String> {
        if results.len() != self.pending.len() {
            return Err("tool settlement count mismatch".into());
        }
        let ids: Vec<_> = self.pending.iter().map(|pending| pending.id).collect();
        for (id, content) in ids.into_iter().zip(results) {
            let UserContent::ToolResult(result) = content else {
                return Err("expected a tool result".into());
            };
            let output = ToolOutput::content(result.content).map_err(|error| error.to_string())?;
            self.settle(
                id,
                Ok(Answered::ToolResult {
                    result: ToolResult::success(output),
                }),
            )?;
        }
        self.refresh()
    }

    fn refresh(&mut self) -> Result<(), String> {
        let options = self.options.clone().ok_or("run has not been planned")?;
        self.plan(options).map(|_| ())
    }
}

#[derive(Default)]
struct Trace {
    turn: usize,
    history: Vec<Message>,
    content: Vec<AssistantContent>,
    tools: Vec<EffectId>,
    pending: Vec<Pending>,
    preresolved: Vec<crate::outcome::ToolOutcome>,
}

struct TraceHook(Arc<Mutex<Trace>>, Vec<ToolDefinition>);
impl AgentHook for TraceHook {
    async fn on_dispatch(&self, _: &HookContext, event: DispatchEvent<'_>) -> DispatchAction {
        let mut kind = event.kind.clone();
        if let EffectKind::Completion { request, .. } = &mut kind {
            for tool in &mut Arc::make_mut(request).tools {
                if let Some(definition) = self
                    .1
                    .iter()
                    .find(|definition| definition.name == tool.name)
                {
                    Arc::make_mut(tool).annotations = definition.annotations.clone();
                }
            }
            DispatchAction::patch(kind)
        } else {
            DispatchAction::proceed()
        }
    }
    async fn on_completion_call(
        &self,
        _: &HookContext,
        event: CompletionCallEvent<'_>,
    ) -> CompletionCallAction {
        let mut trace = lock(&self.0);
        trace.turn = event.turn;
        trace.history = event
            .history
            .iter()
            .map(|message| (**message).clone())
            .collect();
        trace.history.push(event.prompt.clone());
        trace.tools.clear();
        CompletionCallAction::continue_run()
    }
    async fn on_model_turn_finished(
        &self,
        _: &HookContext,
        event: ModelTurnFinished<'_>,
    ) -> ModelTurnAction {
        let mut trace = lock(&self.0);
        trace.content = event.content.clone();
        for (call_index, call) in event
            .content
            .iter()
            .filter_map(|item| match item {
                AssistantContent::ToolCall(call) => Some(call),
                _ => None,
            })
            .enumerate()
        {
            if call.function.invalid_arguments.is_some() {
                trace
                    .preresolved
                    .push(crate::outcome::ToolOutcome::preresolved(
                        crate::outcome::CallPosition {
                            prompt_index: 0,
                            turn: event.turn,
                            call_index,
                        },
                        call,
                    ));
            }
        }
        ModelTurnAction::continue_run()
    }
}

struct Handler {
    descriptor: HandlerDescriptor,
    replay: Option<EffectLogReplayer>,
    trace: Arc<Mutex<Trace>>,
    remaining: Arc<Mutex<BTreeMap<EffectId, HandlerKey>>>,
}

impl Serve for Handler {
    type Family = family::Dynamic;
    fn descriptor(&self) -> HandlerDescriptor {
        self.descriptor.clone()
    }
    async fn serve(&self, kind: EffectKind, dispatch: Dispatch) -> Reply {
        if matches!(kind, EffectKind::ToolCall { .. }) {
            lock(&self.trace).tools.push(dispatch.id());
        }
        let saved = lock(&self.remaining).get(&dispatch.id()).cloned();
        if let Some(key) = saved {
            if key != self.descriptor.key {
                return Reply::Outcome(Err(ErrorReport::new(
                    ErrorKind::Divergence,
                    "recorded handler changed",
                )));
            }
            if let Some(replay) = &self.replay {
                lock(&self.remaining).remove(&dispatch.id());
                return replay.serve(kind, dispatch).await;
            }
            return Reply::Outcome(Err(ErrorReport::new(
                ErrorKind::Divergence,
                "recorded handler is missing",
            )));
        }
        lock(&self.trace).pending.push(Pending {
            id: dispatch.id(),
            key: self.descriptor.key.clone(),
            kind,
        });
        std::future::pending().await
    }
}

struct PlannerWake(AtomicBool);
impl Wake for PlannerWake {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
