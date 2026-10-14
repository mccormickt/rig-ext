//! Durable agent sessions in Cloudflare and celld SQLite Durable Objects.
//!
//! The application owns the Durable Object class. Keep one [`Engine`] in
//! that class, admit work with [`Engine::submit`], and delegate alarms to
//! [`Engine::alarm`]. SQL commits precede every external effect. Status and
//! results read committed state, never an in-flight model or tool response.

#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod child;
#[cfg(feature = "codemode")]
mod script;
mod storage;
pub use child::{
    CHILD_PROTOCOL_MAX_BYTES, ChildDefinition, ChildHandle, ChildReply, ChildRequest,
    ChildTransport, NamespaceTransport, child_id,
};
pub use storage::SqlDatabase;

use std::{
    cell::Cell,
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use child::ChildInvocation;

use futures::{StreamExt, stream::FuturesUnordered};
use rig::{
    DynModel,
    agent::{AgentRun, PendingToolCall},
    completion::Message,
    operation::Completion,
    tool::{Tool, ToolSet},
};
use serde::{Deserialize, Serialize};

use crate::{
    ApprovalDecision, ApprovalRequest, AttemptMetadata, Compaction, CompactionRecord,
    CompletionMode, CompletionSettings, ContextState, DurableAgentConfig, DurableResponse,
    DurableToolResult, InterruptionReason, InvocationContract, LogicalCallKey, RetryPolicy,
    Submission, SubmissionLedger, SubmissionState, SubmitInput, ToolActivityInput,
    ToolActivityOutput, ToolExecutor, ToolInvocation, ToolOptions, ToolOutcome,
    activities::completion,
    config::ConfigSnapshot,
    driver::{self, Effect},
    outcome::CallPosition,
    submission::{Admission, DEFAULT_LEDGER_MAX_BYTES, SessionAvailability},
    tools::{ToolEntry, ToolRoute},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Durable Object storage failed: {0}")]
    Storage(String),
    #[error("durable registration is incompatible with retained state: {0}")]
    Incompatible(String),
    #[error("uncertain child protocol reply: {0}")]
    Protocol(String),
    #[error("invalid durable session: {0}")]
    Invalid(String),
    #[error("session payload has {bytes} bytes; configured limit is {max_bytes} bytes")]
    Limit { bytes: usize, max_bytes: usize },
    #[error("durable JSON exceeds the safe persistence depth of {max_depth}")]
    Depth { max_depth: usize },
    #[error(transparent)]
    Submission(#[from] crate::SubmissionError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl From<worker::Error> for Error {
    fn from(error: worker::Error) -> Self {
        Self::Storage(error.to_string())
    }
}

/// Alarm and clock operations stay outside SQL transactions. The engine
/// persists retry deadlines before it waits. Implementations must use an
/// absolute millisecond Unix timestamp for alarms and `now_ms`.
#[allow(async_fn_in_trait)]
pub trait Wake {
    fn now_ms(&self) -> u64;
    async fn arm(&self, deadline_ms: u64) -> Result<(), Error>;
    async fn delay(&self, milliseconds: u64);
}

impl Wake for rig_celld::CellStorage {
    fn now_ms(&self) -> u64 {
        worker::Date::now().as_millis()
    }
    async fn arm(&self, deadline_ms: u64) -> Result<(), Error> {
        if deadline_ms > 8_640_000_000_000_000 {
            return Err(Error::Invalid(
                "alarm deadline exceeds JavaScript Date range".into(),
            ));
        }
        let deadline =
            worker::js_sys::Date::new(&worker::wasm_bindgen::JsValue::from_f64(deadline_ms as f64));
        self.state()
            .storage()
            .set_alarm(worker::ScheduledTime::new(deadline))
            .await
            .map_err(Error::from)
    }
    async fn delay(&self, milliseconds: u64) {
        worker::Delay::from(std::time::Duration::from_millis(milliseconds)).await;
    }
}

/// Committed session state. Approval requests bind final arguments and tool versions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Status {
    pub closed: bool,
    pub busy: bool,
    pub phase: String,
    pub approvals: Vec<ApprovalRequest>,
    pub receipts: Vec<Submission>,
    pub compaction: Option<CompactionRecord>,
    pub compaction_error: Option<String>,
    pub next_deadline_ms: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct Session {
    id: String,
    config: ConfigSnapshot,
    #[serde(default = "default_implementation_version")]
    implementation_version: String,
    #[serde(default)]
    child_id: Option<String>,
    closed: bool,
    queued: VecDeque<SubmitInput>,
    active: Option<Run>,
    compaction: Option<CompactionRecord>,
    compact_due: bool,
    compaction_error: Option<String>,
}

fn default_implementation_version() -> String {
    "1".into()
}

#[derive(Serialize, Deserialize)]
struct Run {
    request_id: String,
    prompt_index: u64,
    agent: AgentRun,
    /// Number of active-context messages already appended or inherited.
    recorded: usize,
    turn: usize,
    phase: Phase,
    outcomes: Vec<ToolOutcome>,
    attempt: u32,
    deadline_ms: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
enum Phase {
    Advance,
    /// AgentRun remains before next_step, so recovery rebuilds the request.
    Model,
    Tools {
        keys: Vec<String>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
struct Call {
    pending: PendingToolCall,
    input: ToolActivityInput,
    digest: String,
    #[serde(default)]
    child: Option<ChildInvocation>,
    approval: Option<ApprovalRequest>,
    decision: Option<ApprovalDecision>,
    started: bool,
    output: Option<ToolActivityOutput>,
    failure: Option<String>,
    attempt: u32,
    deadline_ms: Option<u64>,
}

/// Builder for a single session's model and tools. The durable configuration
/// is captured on first open and checked against registrations on every drive.
pub struct Builder {
    model: DynModel<Completion>,
    tools: ToolSet,
    config: DurableAgentConfig,
    implementation_version: String,
    children: BTreeMap<String, ChildHandle>,
    #[cfg(feature = "codemode")]
    code_mode: Option<script::Registration>,
    compaction: Option<Compaction>,
    max_bytes: usize,
}

impl Builder {
    pub fn new(model: impl Into<DynModel<Completion>>) -> Self {
        Self {
            model: model.into(),
            tools: ToolSet::default(),
            config: DurableAgentConfig {
                contract: InvocationContract::Logical,
                ..Default::default()
            },
            implementation_version: "1".into(),
            children: BTreeMap::new(),
            #[cfg(feature = "codemode")]
            code_mode: None,
            compaction: None,
            max_bytes: 8 * 1024 * 1024,
        }
    }
    pub fn preamble(mut self, value: impl Into<String>) -> Self {
        self.config.preamble = Some(value.into());
        self
    }
    pub fn max_turns(mut self, value: usize) -> Self {
        self.config.max_turns = value;
        self
    }
    pub fn completion(mut self, value: CompletionSettings) -> Self {
        self.config.completion = value;
        self
    }
    pub fn completion_mode(mut self, value: CompletionMode) -> Self {
        self.config.completion_mode = value;
        self
    }
    pub fn completion_retry(mut self, value: RetryPolicy) -> Self {
        self.config.completion_retry = value;
        self
    }
    pub fn implementation_version(mut self, version: impl Into<String>) -> Self {
        self.implementation_version = version.into();
        self
    }
    /// Capture the child contract without the model, credentials, or transport.
    pub fn child_definition(&self) -> Result<ChildDefinition, Error> {
        #[cfg(feature = "codemode")]
        let (config, _) = self.prepare_script()?;
        #[cfg(not(feature = "codemode"))]
        let config = &self.config;
        let definition = ChildDefinition {
            implementation_version: self.implementation_version.clone(),
            config: config.snapshot(),
        };
        definition.validate()?;
        Ok(definition)
    }
    /// Expose a child agent as a tool. It receives the complete argument JSON
    /// as its prompt, owns retries, and does not accept approvals or steering.
    pub fn sub_agent(
        mut self,
        definition: rig::completion::ToolDefinition,
        child: ChildHandle,
    ) -> Result<Self, Error> {
        child.definition.validate()?;
        let name = definition.name.to_string();
        if self.config.tools.get(&name).is_some() {
            return Err(Error::Invalid(format!(
                "tool `{name}` is already registered"
            )));
        }
        self.config.tools.insert(ToolEntry {
            definition,
            route: ToolRoute::DurableObject {
                definition_digest: child.definition.digest()?,
            },
            retry: RetryPolicy::new(1),
            tag: None,
            requires_approval: false,
            policy: crate::ToolPolicy::idempotent()
                .implementation_version(&child.definition.implementation_version),
        });
        self.children.insert(name, child);
        Ok(self)
    }
    /// Cap each checkpoint and the full transcript. Individual SQL records
    /// also have a 1 MiB cap, below the platform's 2 MB row limit.
    pub fn session_history_max_bytes(mut self, value: usize) -> Self {
        self.max_bytes = value.max(1);
        self
    }
    pub fn compaction(mut self, value: Compaction) -> Self {
        self.config.compaction = Some(value.config());
        self.compaction = Some(value);
        self
    }
    pub fn tool<T: Tool + 'static>(self, tool: T) -> Self {
        self.tool_with(tool, ToolOptions::default())
    }
    pub fn tool_with<T: Tool + 'static>(mut self, tool: T, options: ToolOptions) -> Self {
        let definition = rig::tool::tool_definition(&tool);
        self.tools.add_tool(tool);
        self.config.tools.insert(ToolEntry {
            definition,
            route: ToolRoute::RigTool,
            retry: options.retry,
            tag: options.tag,
            requires_approval: options.requires_approval,
            policy: options.policy,
        });
        self
    }
    /// Use the Durable Object ID as `session_id`. Keep exactly one engine per
    /// object; concurrent handlers share its in-memory drive lock.
    pub fn build<D: SqlDatabase, W: Wake>(
        self,
        db: D,
        wake: W,
        session_id: impl Into<String>,
    ) -> Result<Engine<D, W>, Error> {
        for (name, child) in &self.children {
            let registered = self
                .config
                .tools
                .get(name)
                .ok_or_else(|| Error::Invalid("child registration is missing".into()))?;
            if registered.route
                != (ToolRoute::DurableObject {
                    definition_digest: child.definition.digest()?,
                })
                || self.tools.contains(name)
            {
                return Err(Error::Invalid(format!(
                    "child tool `{name}` has conflicting registrations"
                )));
            }
        }
        for entry in self.config.tools.0.values() {
            if entry.tag.is_some() || entry.retry.timeout.is_some() {
                return Err(Error::Invalid(
                    "Durable Objects do not support tool worker tags or timeouts".into(),
                ));
            }
        }
        if self.config.completion_retry.timeout.is_some() {
            return Err(Error::Invalid(
                "Durable Objects do not support completion timeouts".into(),
            ));
        }
        #[cfg(feature = "codemode")]
        let (config, script) = self.prepare_script()?;
        #[cfg(not(feature = "codemode"))]
        let config = self.config;
        storage::initialize(&db)?;
        let id = session_id.into();
        db.transaction(|| {
            if let Some(session) = storage::read::<Session>(&db, "session")? {
                if session.id != id {
                    return Err(Error::Invalid(
                        "session ID does not match stored session".into(),
                    ));
                }
            } else {
                let session = Session {
                    id: id.clone(),
                    config: config.snapshot(),
                    implementation_version: self.implementation_version.clone(),
                    child_id: None,
                    closed: false,
                    queued: VecDeque::new(),
                    active: None,
                    compaction: None,
                    compact_due: false,
                    compaction_error: None,
                };
                storage::write(&db, "session", &session, self.max_bytes)?;
                let ledger = SubmissionLedger::new(id, DEFAULT_LEDGER_MAX_BYTES);
                db.exec(
                    "INSERT INTO rig_submissions (id, data) VALUES (1, ?)",
                    &[storage::encode(&ledger, storage::ROW_BYTES)?],
                )?;
            }
            Ok(())
        })?;
        #[cfg_attr(
            target_family = "wasm",
            expect(
                clippy::arc_with_non_send_sync,
                reason = "ToolExecutor uses Arc across targets; Workers execute local tools without threads"
            )
        )]
        let executor = ToolExecutor::new(Arc::new(self.tools)).with_registered_policies({
            #[cfg(feature = "codemode")]
            {
                script
                    .as_ref()
                    .map(|runtime| {
                        runtime
                            .definition
                            .tools
                            .iter()
                            .map(|entry| (entry.definition.name.to_string(), entry.policy.clone()))
                            .collect()
                    })
                    .unwrap_or_else(|| config.tools.policies())
            }
            #[cfg(not(feature = "codemode"))]
            {
                config.tools.policies()
            }
        });
        Ok(Engine {
            executor,
            db,
            wake,
            model: self.model,
            config,
            implementation_version: self.implementation_version,
            children: self.children,
            #[cfg(feature = "codemode")]
            script,
            compaction: self.compaction,
            max_bytes: self.max_bytes,
            driving: Cell::new(false),
        })
    }
}

/// One local driver, with no cached durable state. Every mutation reloads
/// within a synchronous transaction. A failed write drops the candidate;
/// the next entry point reloads SQL, including after an uncertain commit.
pub struct Engine<D, W> {
    db: D,
    wake: W,
    model: DynModel<Completion>,
    executor: ToolExecutor,
    config: DurableAgentConfig,
    implementation_version: String,
    children: BTreeMap<String, ChildHandle>,
    #[cfg(feature = "codemode")]
    script: Option<script::Runtime>,
    compaction: Option<Compaction>,
    max_bytes: usize,
    driving: Cell<bool>,
}

struct DriveLock<'a>(&'a Cell<bool>);

const STORAGE_FAILURE: &str = "tool result exceeds the durable storage limit; its effect may have happened and must not be repeated";

fn call_capacity_error(error: Error) -> Error {
    match error {
        error @ (Error::Limit { .. } | Error::Depth { .. }) => Error::Storage(format!(
            "tool intent cannot retain a bounded terminal output within the fixed {}-byte tool-call row limit: {error}; reduce or migrate the retained intent; increasing the session-history limit does not change this cap",
            storage::ROW_BYTES
        )),
        error => error,
    }
}

fn storage_failure_output(
    name: &str,
    interruption: Option<InterruptionReason>,
) -> ToolActivityOutput {
    use rig::tool::{ToolExecutionError, ToolResult, portable::ToolResultContext};
    let result = match interruption {
        Some(reason) => DurableToolResult::interrupted(reason, STORAGE_FAILURE),
        None => {
            DurableToolResult::from_rig(
                ToolResult::failed(
                    ToolExecutionError::other(STORAGE_FAILURE).with_retryable(false),
                ),
                &ToolResultContext::default(),
                &crate::policy::MetadataRetention::none(),
            )
            .0
        }
    };
    ToolActivityOutput::from_result(name, result)
}

fn bounded_call_output(
    call: &Call,
    output: ToolActivityOutput,
) -> Result<ToolActivityOutput, Error> {
    let mut finished = call.clone();
    finished.output = Some(output.clone());
    match storage::encode(&finished, storage::ROW_BYTES) {
        Err(Error::Limit { .. } | Error::Depth { .. }) => {
            let interruption = output
                .result
                .as_ref()
                .and_then(|result| result.interruption.as_ref())
                .map(|i| i.reason);
            let output = storage_failure_output(&call.input.name, interruption);
            finished.output = Some(output.clone());
            storage::encode(&finished, storage::ROW_BYTES).map_err(call_capacity_error)?;
            Ok(output)
        }
        result => {
            result?;
            Ok(output)
        }
    }
}

impl Drop for DriveLock<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

impl<D: SqlDatabase, W: Wake> Engine<D, W> {
    fn check_call_capacity(&self, call: &Call) -> Result<(), Error> {
        let needs_reserve = call.child.is_some();
        #[cfg(feature = "codemode")]
        let needs_reserve =
            needs_reserve || (self.script.is_some() && call.input.name == "codemode");
        if !needs_reserve || call.output.is_some() {
            return Ok(());
        }
        let mut finished = call.clone();
        finished.started = false;
        finished.attempt = u32::MAX;
        finished.deadline_ms = Some(u64::MAX);
        if let Some(attempt) = &mut finished.input.invocation.attempt {
            attempt.activity_attempt = Some(u32::MAX);
        }
        for interruption in [None, Some(InterruptionReason::ToolUnavailable)] {
            finished.output = Some(storage_failure_output(&call.input.name, interruption));
            storage::encode(&finished, storage::ROW_BYTES).map_err(call_capacity_error)?;
        }
        Ok(())
    }

    fn load(&self) -> Result<Session, Error> {
        storage::read(&self.db, "session")?
            .ok_or_else(|| Error::Storage("session checkpoint is missing".into()))
    }
    fn ledger(&self) -> Result<SubmissionLedger, Error> {
        let rows = self
            .db
            .exec("SELECT data FROM rig_submissions WHERE id = 1", &[])?;
        let row = rows
            .first()
            .ok_or_else(|| Error::Storage("submission ledger is missing".into()))?;
        Ok(serde_json::from_str(storage::text(row)?)?)
    }
    fn save_ledger(&self, ledger: &SubmissionLedger) -> Result<(), Error> {
        self.db.exec(
            "UPDATE rig_submissions SET data = ? WHERE id = 1",
            &[storage::encode(ledger, storage::ROW_BYTES)?],
        )?;
        Ok(())
    }
    fn update<T>(&self, f: impl FnOnce(&mut Session) -> Result<T, Error>) -> Result<T, Error> {
        self.db.transaction(|| {
            let mut session = self.load()?;
            let result = f(&mut session)?;
            storage::write(&self.db, "session", &session, self.max_bytes)?;
            Ok(result)
        })
    }
    pub fn transcript(&self) -> Result<Vec<Message>, Error> {
        self.db
            .exec("SELECT data FROM rig_entries ORDER BY id", &[])?
            .iter()
            .map(|row| Ok(serde_json::from_str(storage::text(row)?)?))
            .collect()
    }
    fn context(&self, session: &Session) -> Result<ContextState, Error> {
        ContextState::new(self.transcript()?)
            .with_compaction(session.compaction.clone())
            .map_err(|e| Error::Invalid(e.to_string()))
    }
    fn append(&self, messages: &[Message]) -> Result<(), Error> {
        let mut transcript = self.transcript()?;
        transcript.extend_from_slice(messages);
        storage::encode(&transcript, self.max_bytes)?;
        for message in messages {
            self.db.exec(
                "INSERT INTO rig_entries (data) VALUES (?)",
                &[storage::encode(message, storage::ROW_BYTES)?],
            )?;
        }
        Ok(())
    }
    fn record(&self, run: &mut Run) -> Result<(), Error> {
        let history = run.agent.full_history();
        self.append(
            history
                .get(run.recorded..)
                .ok_or_else(|| Error::Invalid("run history moved backwards".into()))?,
        )?;
        run.recorded = history.len();
        Ok(())
    }
    fn call(&self, key: &str) -> Result<Call, Error> {
        let rows = self.db.exec(
            "SELECT data FROM rig_tool_calls WHERE key = ?",
            &[key.into()],
        )?;
        let row = rows
            .first()
            .ok_or_else(|| Error::Storage("tool intent is missing".into()))?;
        Ok(serde_json::from_str(storage::text(row)?)?)
    }
    fn save_call(&self, key: &str, call: &Call) -> Result<(), Error> {
        self.db.exec("INSERT INTO rig_tool_calls (key, data) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET data = excluded.data",
            &[key.into(), storage::encode(call, storage::ROW_BYTES)?])?;
        Ok(())
    }
    /// Invoke only after the application authenticates and routes the request.
    /// This reads committed state; child alarms drive the actual model/tools.
    pub async fn handle_child_request(&self, request: ChildRequest) -> Result<ChildReply, Error> {
        let expected = request.expected();
        let live = ChildDefinition {
            implementation_version: self.implementation_version.clone(),
            config: self.config.snapshot(),
        };
        if let Err(error) = expected.validate() {
            return Ok(ChildReply::Rejected(error.to_string()));
        }
        let session = self.load()?;
        let retained = ChildDefinition {
            config: session.config.clone(),
            implementation_version: session.implementation_version.clone(),
        };
        if expected.digest()? != retained.digest()? {
            return Ok(ChildReply::Rejected(
                "child configuration or implementation version mismatch".into(),
            ));
        }
        if session
            .child_id
            .as_deref()
            .is_some_and(|id| id != request.child_id())
            || (session.child_id.is_none() && !self.ledger()?.receipts.is_empty())
        {
            return Ok(ChildReply::Rejected("child identity mismatch".into()));
        }
        // Validate retained request conflicts before checking live registration.
        let request_id = match &request {
            ChildRequest::Submit {
                child_id, input, ..
            } => {
                if input.request_id != *child_id {
                    return Ok(ChildReply::Rejected(
                        "child request ID must equal child identity".into(),
                    ));
                }
                let mut ledger = self.ledger()?;
                if ledger.receipt(&input.request_id).is_some()
                    && let Err(error) = ledger.admit(
                        input,
                        SessionAvailability {
                            busy: true,
                            closed: session.closed,
                        },
                    )
                {
                    return Ok(ChildReply::Rejected(error.to_string()));
                }
                &input.request_id
            }
            ChildRequest::Poll {
                child_id,
                request_id,
                ..
            } => {
                if request_id != child_id || session.child_id.as_deref() != Some(child_id) {
                    return Ok(ChildReply::Rejected("child identity mismatch".into()));
                }
                request_id
            }
        };
        if let Some(result) = self.result(request_id)? {
            return ChildReply::completed(result);
        }
        if let Some(receipt) = self.ledger()?.receipt(request_id) {
            match &receipt.state {
                SubmissionState::Failed { error } => return Ok(ChildReply::Failed(error.clone())),
                SubmissionState::Cancelled => {
                    return Ok(ChildReply::Failed("child submission was cancelled".into()));
                }
                SubmissionState::Answered => {
                    return Ok(ChildReply::Failed(
                        "committed child result is missing".into(),
                    ));
                }
                _ => {}
            }
        }
        if expected.digest()? != live.digest()? {
            return Err(Error::Incompatible(
                "child configuration or implementation version mismatch".into(),
            ));
        }
        let request_id = match request {
            ChildRequest::Submit {
                child_id, input, ..
            } => {
                let request_id = input.request_id.clone();
                // Admission and identity binding share one transaction. Alarm
                // acknowledgement can be lost without losing the durable intent.
                self.wake
                    .arm(self.wake.now_ms().saturating_add(1000))
                    .await?;
                let admission = self.update(|session| {
                    let mut ledger = self.ledger()?;
                    if session.child_id.as_deref().is_some_and(|id| id != child_id)
                        || (session.child_id.is_none() && !ledger.receipts.is_empty())
                    {
                        return Err(Error::Invalid("child identity mismatch".into()));
                    }
                    let admission = ledger.admit(
                        &input,
                        SessionAvailability {
                            busy: session.active.is_some()
                                || !session.queued.is_empty()
                                || session.compact_due,
                            closed: session.closed,
                        },
                    )?;
                    if matches!(admission, Admission::Admitted(_)) {
                        storage::encode(&input.message, storage::ROW_BYTES)?;
                        session.queued.push_back(input);
                        session.child_id = Some(child_id);
                        self.save_ledger(&ledger)?;
                    }
                    Ok(())
                });
                self.wake
                    .arm(self.wake.now_ms().saturating_add(1000))
                    .await?;
                match admission {
                    Err(Error::Submission(error)) => {
                        return Ok(ChildReply::Rejected(error.to_string()));
                    }
                    Err(Error::Invalid(error)) => return Ok(ChildReply::Rejected(error)),
                    Err(error @ Error::Limit { .. }) => {
                        return Ok(ChildReply::Rejected(error.to_string()));
                    }
                    result => result?,
                }
                request_id
            }
            ChildRequest::Poll { request_id, .. } => request_id,
        };
        if let Some(result) = self.result(&request_id)? {
            return ChildReply::completed(result);
        }
        Ok(
            match self
                .ledger()?
                .receipt(&request_id)
                .map(|receipt| &receipt.state)
            {
                Some(SubmissionState::Failed { error }) => ChildReply::Failed(error.clone()),
                Some(SubmissionState::Cancelled) => {
                    ChildReply::Failed("child submission was cancelled".into())
                }
                Some(SubmissionState::Answered) => {
                    ChildReply::Failed("committed child result is missing".into())
                }
                Some(_) => ChildReply::Pending,
                None => ChildReply::Rejected("child submission is not admitted".into()),
            },
        )
    }
    /// Admit and arm recovery before returning. Drive immediately with `wait`
    /// or let the alarm process the queue. Duplicate IDs return their receipt.
    pub async fn submit(&self, input: SubmitInput) -> Result<Submission, Error> {
        // Arm before admission: eviction after a successful SQL commit must
        // not strand queued work. An unused alarm is harmless.
        self.wake
            .arm(self.wake.now_ms().saturating_add(1000))
            .await?;
        let result = self.update(|session| {
            let mut ledger = self.ledger()?;
            let admission = ledger.admit(
                &input,
                SessionAvailability {
                    busy: session.active.is_some()
                        || !session.queued.is_empty()
                        || session.compact_due,
                    closed: session.closed,
                },
            )?;
            if matches!(admission, Admission::Admitted(_)) {
                storage::encode(&input.message, storage::ROW_BYTES)?;
                session.queued.push_back(input);
                self.save_ledger(&ledger)?;
            }
            Ok(admission.into_submission())
        });
        // The first alarm can fire while its storage acknowledgement awaits.
        // Re-arm after the commit, including an uncertain write failure.
        self.wake
            .arm(self.wake.now_ms().saturating_add(1000))
            .await?;
        result
    }
    pub fn result(&self, request_id: &str) -> Result<Option<DurableResponse>, Error> {
        storage::read(&self.db, &format!("result:{request_id}"))
    }
    /// Drive until idle, a durable retry deadline, or an approval barrier.
    /// `None` means the request is not answered yet; inspect `status`.
    pub async fn wait(&self, request_id: &str) -> Result<Option<DurableResponse>, Error> {
        self.drive().await?;
        self.result(request_id)
    }
    pub fn close(&self) -> Result<(), Error> {
        self.update(|session| {
            session.closed = true;
            Ok(())
        })
    }
    pub async fn approve(&self, approval_id: impl Into<String>) -> Result<(), Error> {
        self.decide(ApprovalDecision::Approve {
            approval_id: approval_id.into(),
        })
        .await
    }
    pub async fn deny(
        &self,
        approval_id: impl Into<String>,
        reason: Option<String>,
    ) -> Result<(), Error> {
        self.decide(ApprovalDecision::Deny {
            approval_id: approval_id.into(),
            reason,
        })
        .await
    }
    async fn decide(&self, decision: ApprovalDecision) -> Result<(), Error> {
        self.wake
            .arm(self.wake.now_ms().saturating_add(1000))
            .await?;
        let result = self.update(|session| {
            if let Some(Run {
                phase: Phase::Tools { keys },
                ..
            }) = &session.active
            {
                for key in keys {
                    let mut call = self.call(key)?;
                    if call
                        .approval
                        .as_ref()
                        .is_some_and(|a| a.approval_id == decision.approval_id())
                    {
                        if call.decision.is_none() {
                            call.decision = Some(decision);
                            self.save_call(key, &call)?;
                        }
                        return Ok(());
                    }
                }
            }
            Err(Error::Invalid("approval ID is not pending".into()))
        });
        self.wake
            .arm(self.wake.now_ms().saturating_add(1000))
            .await?;
        result
    }
    pub fn status(&self) -> Result<Status, Error> {
        let session = self.load()?;
        let mut approvals = Vec::new();
        let mut deadline = session.active.as_ref().and_then(|run| run.deadline_ms);
        let phase = match session.active.as_ref().map(|run| &run.phase) {
            Some(Phase::Model) => "model",
            Some(Phase::Advance) => "advance",
            Some(Phase::Tools { keys }) => {
                for key in keys {
                    let call = self.call(key)?;
                    #[cfg(feature = "codemode")]
                    if call.input.name == "codemode"
                        && let Some(value) = self.script_deadline(&call)?
                    {
                        deadline = Some(deadline.map_or(value, |d| d.min(value)));
                    }
                    if call.decision.is_none() {
                        approvals.extend(call.approval);
                    }
                    if let Some(value) = call.deadline_ms {
                        deadline = Some(deadline.map_or(value, |d| d.min(value)));
                    }
                }
                "tools"
            }
            None if session.compact_due => "compaction",
            None => "idle",
        };
        Ok(Status {
            closed: session.closed,
            busy: session.active.is_some() || !session.queued.is_empty() || session.compact_due,
            phase: phase.into(),
            approvals,
            receipts: self.ledger()?.receipts.into_values().collect(),
            compaction: session.compaction,
            compaction_error: session.compaction_error,
            next_deadline_ms: deadline,
        })
    }
    /// Earliest stored retry deadline or a recovery heartbeat, whichever is earlier.
    pub fn alarm_deadline(&self) -> Result<Option<u64>, Error> {
        let status = self.status()?;
        if !status.busy {
            return Ok(None);
        }
        let now = self.wake.now_ms();
        Ok(Some(
            status
                .next_deadline_ms
                .unwrap_or(u64::MAX)
                .min(now.saturating_add(30_000))
                .max(now.saturating_add(1)),
        ))
    }
    async fn rearm(&self) -> Result<(), Error> {
        match self.alarm_deadline()? {
            Some(deadline) => self.wake.arm(deadline).await,
            // Do not delete an alarm after an idle read: a submission may
            // arm it while the storage operation awaits. One idle wake is harmless.
            None => Ok(()),
        }
    }
    /// Catch driver errors and explicitly arm recovery. Platform alarm retry
    /// counts are not the retry policy. Alarm writes and registration
    /// incompatibilities are returned without terminating admitted work.
    pub async fn alarm(&self) -> Result<(), Error> {
        if self.driving.get() {
            return self
                .wake
                .arm(self.wake.now_ms().saturating_add(30_000))
                .await;
        }
        if let Err(error) = self.drive().await {
            self.wake
                .arm(self.wake.now_ms().saturating_add(2000))
                .await?;
            if matches!(error, Error::Incompatible(_)) {
                return Err(error);
            }
        }
        Ok(())
    }
    pub async fn drive(&self) -> Result<(), Error> {
        if self.driving.replace(true) {
            return Ok(());
        }
        let _lock = DriveLock(&self.driving);
        if !self.status()?.busy {
            return Ok(());
        }
        self.wake
            .arm(self.wake.now_ms().saturating_add(30_000))
            .await?;
        let result = self.drive_loop().await;
        if let Err(error @ (Error::Limit { .. } | Error::Invalid(_))) = &result {
            self.fail(error.to_string())?;
        }
        self.rearm().await?;
        result
    }
    async fn drive_loop(&self) -> Result<(), Error> {
        let started = self.wake.now_ms();
        loop {
            if self.wake.now_ms().saturating_sub(started) >= 60_000 {
                break;
            }
            let session = self.load()?;
            if session.implementation_version != self.implementation_version {
                return Err(Error::Incompatible(
                    "agent implementation version mismatch".into(),
                ));
            }
            let config = self
                .config
                .resolve(&session.config)
                .map_err(|e| Error::Incompatible(e.to_string()))?;
            if session.active.is_none() {
                if session.compact_due {
                    let context = self.context(&session)?;
                    let version = config
                        .compaction
                        .as_ref()
                        .ok_or_else(|| {
                            Error::Invalid("compaction configuration is missing".into())
                        })?
                        .version
                        .clone();
                    let output = crate::activities::compaction::compact(
                        self.compaction.as_ref(),
                        context.request(&session.id, &version),
                    )
                    .await;
                    self.update(|state| {
                        let mut context = self.context(state)?;
                        state.compaction_error = match output {
                            Ok(output) => {
                                context.apply(output, &version).err().map(|e| e.to_string())
                            }
                            Err(error) => Some(error),
                        };
                        state.compaction = context.compaction;
                        state.compact_due = false;
                        Ok(())
                    })?;
                    continue;
                }
                if session.queued.is_empty() {
                    break;
                }
                self.update(|state| {
                    let input = state
                        .queued
                        .pop_front()
                        .ok_or_else(|| Error::Invalid("queue is empty".into()))?;
                    let mut ledger = self.ledger()?;
                    let receipt = ledger
                        .set_state(&input.request_id, SubmissionState::Running)
                        .ok_or_else(|| Error::Invalid("receipt is missing".into()))?;
                    let history = self.context(state)?.active_context();
                    let recorded = history.len();
                    let mut agent = AgentRun::new(input.message)
                        .with_history(history)
                        .max_turns(config.max_turns);
                    if let Some(choice) = config.completion.tool_choice.clone() {
                        agent = agent.with_tool_choice(choice);
                    }
                    state.active = Some(Run {
                        request_id: input.request_id,
                        prompt_index: receipt.prompt_index,
                        agent,
                        recorded,
                        turn: 0,
                        phase: Phase::Advance,
                        outcomes: Vec::new(),
                        attempt: 1,
                        deadline_ms: None,
                    });
                    self.save_ledger(&ledger)
                })?;
                continue;
            }
            let run = session
                .active
                .as_ref()
                .ok_or_else(|| Error::Invalid("active run is missing".into()))?;
            if let Some(deadline) = run.deadline_ms
                && deadline > self.wake.now_ms()
            {
                let wait = deadline.saturating_sub(self.wake.now_ms());
                if wait > 1000 {
                    break;
                }
                self.wake.delay(wait).await;
                continue;
            }
            match &run.phase {
                Phase::Advance => self.advance(&session, &config)?,
                Phase::Model => self.model(&session, &config).await?,
                Phase::Tools { keys } => {
                    if !self.tools(keys, &config).await? {
                        let status = self.status()?;
                        if status.approvals.is_empty()
                            && let Some(deadline) = status.next_deadline_ms
                            && deadline.saturating_sub(self.wake.now_ms()) <= 1000
                        {
                            self.wake
                                .delay(deadline.saturating_sub(self.wake.now_ms()))
                                .await;
                            continue;
                        }
                        break;
                    }
                }
            }
        }
        Ok(())
    }
    fn fail(&self, message: String) -> Result<(), Error> {
        self.update(|state| {
            let mut ledger = self.ledger()?;
            if let Some(run) = state.active.take() {
                ledger.set_state(&run.request_id, SubmissionState::Failed { error: message });
            }
            ledger.cancel_pending();
            state.queued.clear();
            state.closed = true;
            state.compact_due = false;
            self.save_ledger(&ledger)
        })
    }
    fn advance(&self, session: &Session, config: &DurableAgentConfig) -> Result<(), Error> {
        let run = session
            .active
            .as_ref()
            .ok_or_else(|| Error::Invalid("active run is missing".into()))?;
        let mut agent = run.agent.clone();
        let effect = match driver::next_effect(&mut agent, config.into()) {
            Ok(effect) => effect,
            Err(error) => return self.fail(error),
        };
        self.update(|state| {
            let run = state
                .active
                .as_mut()
                .ok_or_else(|| Error::Invalid("active run is missing".into()))?;
            match effect {
                Effect::Model { turn, .. } => {
                    run.turn = turn;
                    run.phase = Phase::Model;
                    run.attempt = 1;
                    run.deadline_ms = None;
                }
                Effect::Tools { calls } => {
                    run.agent = agent;
                    let mut keys = Vec::new();
                    for (index, pending) in calls.into_iter().enumerate() {
                        let call = &pending.tool_call;
                        let entry = config
                            .tools
                            .get(&call.function.name)
                            .ok_or_else(|| Error::Invalid("tool is not registered".into()))?;
                        let key = LogicalCallKey {
                            logical_execution_id: state.id.clone(),
                            submission_id: LogicalCallKey::submission_for_prompt(run.prompt_index),
                            model_turn: run.turn,
                            call_index: index,
                        };
                        let approval =
                            if entry.requires_approval && pending.preresolved_result.is_none() {
                                Some(
                                    driver::logical_approval_request(
                                        call,
                                        &key,
                                        entry.policy.version(),
                                    )
                                    .map_err(Error::Invalid)?,
                                )
                            } else {
                                None
                            };
                        let input = ToolActivityInput {
                            name: call.function.name.to_string(),
                            arguments: serde_json::to_string(&call.function.arguments)?,
                            invocation: ToolInvocation {
                                execution_id: state.id.clone(),
                                prompt_index: run.prompt_index,
                                turn: run.turn,
                                call_index: index,
                                logical_key: Some(key.clone()),
                                attempt: Some(AttemptMetadata {
                                    backend_execution_id: state.id.clone(),
                                    activity_attempt: Some(1),
                                }),
                            },
                            policy: Some(entry.policy.clone()),
                        };
                        let digest = crate::identity::arguments_digest(
                            &serde_json::Value::Object(call.function.arguments.clone()),
                        )?;
                        let child =
                            self.children
                                .get(&entry.definition.name.to_string())
                                .map(|child| {
                                    ChildInvocation::new(
                                        &key,
                                        None,
                                        child.definition.clone(),
                                        serde_json::Value::Object(call.function.arguments.clone())
                                            .to_string(),
                                    )
                                });
                        let call = Call {
                            pending,
                            input,
                            digest,
                            child,
                            approval,
                            decision: None,
                            started: false,
                            output: None,
                            failure: None,
                            attempt: 1,
                            deadline_ms: None,
                        };
                        self.check_call_capacity(&call)?;
                        self.save_call(&key.canonical(), &call)?;
                        keys.push(key.canonical());
                    }
                    run.phase = Phase::Tools { keys };
                    self.record(run)?;
                }
                Effect::Done(response) => {
                    run.agent = agent;
                    self.record(run)?;
                    storage::write(
                        &self.db,
                        &format!("result:{}", run.request_id),
                        &DurableResponse::new(response, run.outcomes.clone()),
                        self.max_bytes,
                    )?;
                    let mut ledger = self.ledger()?;
                    ledger.set_state(&run.request_id, SubmissionState::Answered);
                    self.save_ledger(&ledger)?;
                    state.active = None;
                    state.compact_due = config.compaction.is_some();
                }
            }
            Ok(())
        })
    }
    async fn model(&self, session: &Session, config: &DurableAgentConfig) -> Result<(), Error> {
        let run = session
            .active
            .as_ref()
            .ok_or_else(|| Error::Invalid("active run is missing".into()))?;
        let mut agent = run.agent.clone();
        let Effect::Model { request, .. } =
            driver::next_effect(&mut agent, config.into()).map_err(Error::Invalid)?
        else {
            return Err(Error::Invalid(
                "model checkpoint does not request a model".into(),
            ));
        };
        let result = match config.completion_mode {
            CompletionMode::Blocking => match completion::complete(&self.model, request).await {
                Ok(turn) => {
                    driver::apply_model_turn(&mut agent, turn).map_err(Error::Invalid)?;
                    Ok(())
                }
                Err(error) => Err(error),
            },
            CompletionMode::Streaming => {
                match completion::stream(&self.model, request.clone()).await {
                    Ok(turn) => {
                        crate::streaming::apply(&mut agent, &request, turn)
                            .map_err(Error::Invalid)?;
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            }
        };
        match result {
            Ok(()) => self.update(|state| {
                let run = state
                    .active
                    .as_mut()
                    .ok_or_else(|| Error::Invalid("active run is missing".into()))?;
                run.agent = agent;
                run.phase = Phase::Advance;
                run.deadline_ms = None;
                self.record(run)
            }),
            Err(error) if run.attempt >= config.completion_retry.max_attempts => self.fail(error),
            Err(_) => self.update(|state| {
                let run = state
                    .active
                    .as_mut()
                    .ok_or_else(|| Error::Invalid("active run is missing".into()))?;
                run.deadline_ms = Some(
                    self.wake.now_ms().saturating_add(
                        config
                            .completion_retry
                            .delay_for_attempt(run.attempt)
                            .as_millis() as u64,
                    ),
                );
                run.attempt += 1;
                Ok(())
            }),
        }
    }
    async fn tools(&self, keys: &[String], config: &DurableAgentConfig) -> Result<bool, Error> {
        let calls = keys
            .iter()
            .map(|key| self.call(key))
            .collect::<Result<Vec<_>, _>>()?;
        for call in &calls {
            #[cfg(feature = "codemode")]
            self.check_script_registration(call)?;
            self.check_call_capacity(call)?;
        }
        if let Some(error) = calls.iter().find_map(|call| call.failure.clone()) {
            self.fail(error)?;
            return Ok(true);
        }
        if calls
            .iter()
            .any(|call| call.approval.is_some() && call.decision.is_none())
        {
            return Ok(false);
        }
        let mut pending = FuturesUnordered::new();
        let mut deferred = Vec::new();
        let execute = |key, mut call: Call| async move {
            call.started = true;
            call.deadline_ms = None;
            if let Some(attempt) = &mut call.input.invocation.attempt {
                attempt.activity_attempt = Some(call.attempt);
            }
            self.db.transaction(|| self.save_call(key, &call))?;
            #[cfg(feature = "codemode")]
            if self.script.is_some() && call.input.name == "codemode" {
                let result = self.script(&call).await?;
                return Ok((key, Ok(result), call));
            }
            let result = if let Some(child) = &mut call.child {
                self.poll_child(&call.input.name, child).await
            } else {
                self.executor
                    .execute_committed(call.input.clone())
                    .await
                    .map(Some)
            };
            Ok::<_, Error>((key, result, call))
        };
        for (key, mut call) in keys.iter().zip(calls) {
            if call.output.is_some() || call.pending.preresolved_result.is_some() {
                continue;
            }
            if let Some(ApprovalDecision::Deny { reason, .. }) = &call.decision {
                call.pending = driver::deny_tool(call.pending, reason.clone());
                self.db.transaction(|| self.save_call(key, &call))?;
                continue;
            }
            let policy = call
                .input
                .policy
                .as_ref()
                .ok_or_else(|| Error::Invalid("tool intent has no policy".into()))?;
            let registered = self
                .config
                .tools
                .get(&call.input.name)
                .ok_or_else(|| Error::Incompatible("tool registration is missing".into()))?;
            if policy.version() != registered.policy.version() {
                return Err(Error::Incompatible(
                    "tool implementation version mismatch".into(),
                ));
            }
            let digest =
                crate::identity::arguments_digest(&serde_json::from_str(&call.input.arguments)?)?;
            if digest != call.digest {
                return Err(Error::Invalid(
                    "tool intent argument digest mismatch".into(),
                ));
            }
            if call.started
                && (!policy.safety().permits_repeat()
                    || !registered.policy.safety().permits_repeat()
                    || policy.safety() != registered.policy.safety())
            {
                call.output = Some(ToolActivityOutput::from_result(
                    &call.input.name,
                    DurableToolResult::interrupted(
                        InterruptionReason::ClaimHeld,
                        "tool was interrupted and may have partially run",
                    ),
                ));
                self.db.transaction(|| self.save_call(key, &call))?;
                continue;
            }
            if call
                .deadline_ms
                .is_some_and(|deadline| deadline > self.wake.now_ms())
            {
                deferred.push((key.as_str(), call));
                continue;
            }
            pending.push(execute(key.as_str(), call));
        }
        let mut failure = None;
        while !pending.is_empty() {
            let deadline = deferred
                .iter()
                .filter_map(|(_, call)| call.deadline_ms)
                .min();
            let observation = if let Some(deadline) = deadline {
                match futures::future::select(
                    pending.next(),
                    Box::pin(self.wake.delay(deadline.saturating_sub(self.wake.now_ms()))),
                )
                .await
                {
                    futures::future::Either::Left((observation, _)) => observation,
                    futures::future::Either::Right(((), _)) => {
                        let mut index = 0;
                        while index < deferred.len() {
                            if deferred[index]
                                .1
                                .deadline_ms
                                .is_some_and(|d| d <= self.wake.now_ms())
                            {
                                let (key, call) = deferred.remove(index);
                                pending.push(execute(key, call));
                            } else {
                                index += 1;
                            }
                        }
                        continue;
                    }
                }
            } else {
                pending.next().await
            };
            let Some(observation) = observation else {
                break;
            };
            let (key, result, mut call) = observation?;
            match result {
                Ok(Some(output)) => {
                    call.output = Some(if call.child.is_some() {
                        bounded_call_output(&call, output)?
                    } else {
                        output
                    });
                }
                Ok(None) => {
                    let deadline = self.wake.now_ms().saturating_add(2000);
                    #[cfg(feature = "codemode")]
                    let deadline = if self.script.is_some() && call.input.name == "codemode" {
                        self.script_deadline(&call)?.unwrap_or(deadline)
                    } else {
                        deadline
                    };
                    call.deadline_ms = Some(deadline);
                }
                Err(error) => {
                    let entry = config
                        .tools
                        .get(&call.input.name)
                        .ok_or_else(|| Error::Invalid("tool registration is missing".into()))?;
                    if call.attempt >= entry.retry.max_attempts {
                        call.failure = Some(crate::submission::bounded_error(error.clone()));
                        failure = Some(error);
                    } else {
                        call.deadline_ms = Some(self.wake.now_ms().saturating_add(
                            entry.retry.delay_for_attempt(call.attempt).as_millis() as u64,
                        ));
                        call.attempt += 1;
                    }
                }
            }
            self.db.transaction(|| self.save_call(key, &call))?;
            if call.output.is_none() && call.failure.is_none() && call.deadline_ms.is_some() {
                deferred.push((key, call));
            }
        }
        if let Some(error) = failure {
            self.fail(error)?;
            return Ok(true);
        }
        let calls = keys
            .iter()
            .map(|key| self.call(key))
            .collect::<Result<Vec<_>, _>>()?;
        if calls
            .iter()
            .any(|call| call.output.is_none() && call.pending.preresolved_result.is_none())
        {
            return Ok(false);
        }
        self.update(|state| {
            let run = state
                .active
                .as_mut()
                .ok_or_else(|| Error::Invalid("active run is missing".into()))?;
            let mut contents = Vec::new();
            for (index, call) in calls.into_iter().enumerate() {
                let position = CallPosition {
                    prompt_index: run.prompt_index,
                    turn: run.turn,
                    call_index: index,
                };
                let tool = &call.pending.tool_call;
                if let Some(content) = call.pending.preresolved_result {
                    contents.push(content);
                    run.outcomes.push(
                        if matches!(call.decision, Some(ApprovalDecision::Deny { .. })) {
                            ToolOutcome::denied(position, tool)
                        } else {
                            ToolOutcome::preresolved(position, tool)
                        },
                    );
                } else {
                    let output = call
                        .output
                        .ok_or_else(|| Error::Invalid("tool result is missing".into()))?;
                    run.outcomes
                        .push(ToolOutcome::from_activity(position, tool, &output));
                    contents.push(driver::tool_result(tool, output.content));
                }
            }
            run.agent
                .tool_results(contents)
                .map_err(|e| Error::Invalid(e.to_string()))?;
            run.phase = Phase::Advance;
            self.record(run)
        })?;
        Ok(true)
    }
    async fn poll_child(
        &self,
        name: &str,
        child: &mut ChildInvocation,
    ) -> Result<Option<ToolActivityOutput>, String> {
        let handle = self
            .children
            .get(name)
            .ok_or("child registration is missing")?;
        if child
            .request
            .expected()
            .digest()
            .map_err(|e| e.to_string())?
            != handle.definition.digest().map_err(|e| e.to_string())?
        {
            return Err("child definition mismatch".into());
        }
        let reply = match handle.transport.request(&child.outbound()).await {
            Ok(reply) => reply,
            Err(_) => return Ok(None),
        };
        child.acknowledged = true;
        use rig::tool::{ToolExecutionError, ToolOutput, ToolResult, portable::ToolResultContext};
        let result = match reply {
            ChildReply::Pending => return Ok(None),
            ChildReply::Completed(response) => {
                ToolResult::success(ToolOutput::text(response.output()))
            }
            ChildReply::Failed(error) | ChildReply::Rejected(error) => {
                ToolResult::failed(ToolExecutionError::other(error).with_retryable(false))
            }
        };
        let (result, _) = DurableToolResult::from_rig(
            result,
            &ToolResultContext::default(),
            &crate::policy::MetadataRetention::none(),
        );
        Ok(Some(ToolActivityOutput::from_result(name, result)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests;
