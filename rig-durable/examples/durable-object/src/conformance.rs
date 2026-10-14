//! Local fault controls and independent SQL counters for real child engines.

use futures::future::LocalBoxFuture;
use rig::{completion::ToolDefinition, message::ToolName};
use rig_codemode::{
    Limits,
    replay::{ReplayError, ReplayFuture, ReplayInput, ReplayPlanner},
};
use rig_durable::{
    LogicalCallKey,
    durable_object::{ChildHandle, ChildReply, ChildRequest, ChildTransport, NamespaceTransport},
};
use serde::{Serialize, de::DeserializeOwned};

use super::*;

fn initialize(storage: &CellStorage) -> Result<()> {
    storage.sql().exec(
        "CREATE TABLE IF NOT EXISTS fixture_control (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        None,
    )?;
    storage.sql().exec(
        "CREATE TABLE IF NOT EXISTS fixture_effects (sequence INTEGER PRIMARY KEY, logical_id TEXT NOT NULL, branch TEXT NOT NULL)",
        None,
    )?;
    storage.sql().exec(
        "CREATE TABLE IF NOT EXISTS fixture_requests (sequence INTEGER PRIMARY KEY, child_id TEXT NOT NULL, operation TEXT NOT NULL, body TEXT NOT NULL)",
        None,
    )?;
    Ok(())
}

fn read<T: DeserializeOwned>(storage: &CellStorage, key: &str) -> Result<Option<T>> {
    #[derive(Deserialize)]
    struct Row {
        value: String,
    }
    let rows = storage
        .sql()
        .exec(
            "SELECT value FROM fixture_control WHERE key = ?",
            vec![key.into()],
        )?
        .to_array::<Row>()?;
    rows.first()
        .map(|row| serde_json::from_str(&row.value).map_err(worker_error))
        .transpose()
}

fn write(storage: &CellStorage, key: &str, value: &impl Serialize) -> Result<()> {
    storage.sql().exec(
        "INSERT INTO fixture_control (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        vec![key.into(), serde_json::to_string(value).map_err(worker_error)?.into()],
    )?;
    Ok(())
}

fn increment(storage: &CellStorage, key: &str) -> Result<()> {
    write(storage, key, &(read::<u64>(storage, key)?.unwrap_or(0) + 1))
}

fn committed_result(
    engine: &Engine<CellStorage, CellStorage>,
    request: &Request,
) -> Result<Response> {
    let id = request
        .url()?
        .query_pairs()
        .find(|(key, _)| key == "request_id")
        .map(|(_, value)| value.into_owned())
        .ok_or_else(|| worker_error("request_id required"))?;
    Response::from_json(&engine.result(&id).map_err(worker_error)?)
}

#[derive(Clone, Deserialize, Serialize)]
struct Configuration {
    source: String,
    #[serde(default)]
    ordinary_child_batch: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    second_source: Option<String>,
    #[serde(default = "source_bytes")]
    source_bytes: usize,
    #[serde(default)]
    lose_ack: bool,
    #[serde(default)]
    pause_ack: bool,
    #[serde(default)]
    corrupt_reply: Option<String>,
    #[serde(default)]
    pause_after: Option<u32>,
    #[serde(default)]
    pause_second_result: bool,
    #[serde(default)]
    fail_after: Option<u32>,
    #[serde(default)]
    drift: bool,
    #[serde(default)]
    catalog_drift: bool,
    #[serde(default)]
    policy_drift: bool,
    #[serde(default = "concurrency")]
    max_in_flight: u32,
    #[serde(default = "history_bytes")]
    max_history_bytes: usize,
}

fn concurrency() -> u32 {
    2
}

fn history_bytes() -> usize {
    8 * 1024 * 1024
}

fn source_bytes() -> usize {
    Limits::default().source_bytes
}

/// Stateless completion transport. Only the model is mocked, not Worker I/O.
#[derive(Clone)]
struct FixtureModel {
    source: Option<String>,
    second_source: Option<String>,
    ordinary_child_batch: Option<Vec<serde_json::Value>>,
    storage: CellStorage,
}

impl Transport<MockScript> for FixtureModel {
    fn send(&self, request: CompletionRequest, _: Exchange) -> Opening<MockFrame> {
        let has_result = matches!(request.chat_history.last(), Some(Message::User { content })
            if content.iter().any(|part| matches!(part, UserContent::ToolResult(_))));
        let prompt = request
            .chat_history
            .iter()
            .find_map(|message| match message {
                Message::User { content } => content.iter().find_map(|part| match part {
                    UserContent::Text(text) => Some(text.text.as_str()),
                    _ => None,
                }),
                _ => None,
            })
            .unwrap_or_default();
        let args: serde_json::Value = serde_json::from_str(prompt).unwrap_or_default();
        let gate = self.source.is_none()
            && !has_result
            && args
                .get("gate")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
        let child_model = self.source.is_none() && !has_result;
        let final_text = if self.source.is_some() {
            "Script complete".to_owned()
        } else if let Some(size) = args.get("result_bytes").and_then(serde_json::Value::as_u64) {
            "x".repeat(size as usize)
        } else {
            args.get("branch")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned()
        };
        let event = if has_result {
            MockStreamEvent::text(final_text)
        } else if let Some(source) = &self.source {
            MockStreamEvent::tool_call("script", "codemode", serde_json::json!({"code": source}))
        } else {
            MockStreamEvent::tool_call("effect", "observe", args)
        };
        let mut events = vec![Ok(MockFrame::Event(event))];
        if !has_result && let Some(batch) = &self.ordinary_child_batch {
            events = batch
                .iter()
                .enumerate()
                .map(|(index, args)| {
                    Ok(MockFrame::Event(MockStreamEvent::tool_call(
                        &format!("ordinary-{index}"),
                        "child",
                        args.clone(),
                    )))
                })
                .collect();
        }
        if !has_result && let Some(source) = &self.second_source {
            events.push(Ok(MockFrame::Event(MockStreamEvent::tool_call(
                "script-second",
                "codemode",
                serde_json::json!({"code": source}),
            ))));
        }
        events.push(Ok(MockFrame::Event(
            MockStreamEvent::final_response_with_default_usage(),
        )));
        let storage = self.storage.clone();
        Opening::new(async move {
            let provider_error =
                |e: worker::Error| rig::error::ProviderError::Provider(e.to_string());
            increment(&storage, "model_entries").map_err(provider_error)?;
            if child_model {
                write(&storage, "model_started", &true).map_err(provider_error)?;
            }
            while gate
                && !read::<bool>(&storage, "released")
                    .map_err(provider_error)?
                    .unwrap_or(false)
            {
                Delay::from(std::time::Duration::from_millis(25)).await;
            }
            Ok(Opened::new(futures::stream::iter(events)))
        })
    }
}

#[derive(Deserialize)]
struct EffectArgs {
    branch: String,
    #[serde(default)]
    gate: bool,
    #[serde(default)]
    pause_after_effect: bool,
    #[serde(default)]
    result_bytes: Option<usize>,
    #[serde(default)]
    result_depth: Option<usize>,
    #[serde(default)]
    payload: Option<serde_json::Value>,
}

struct Observe {
    storage: CellStorage,
}

impl Tool for Observe {
    const NAME: &'static str = "observe";
    type Args = EffectArgs;
    type Output = rig::tool::ToolOutput;
    type Error = worker::Error;
    fn description(&self) -> String {
        "Record one independently observed effect".into()
    }
    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({"type":"object","properties":{"branch":{"type":"string"},"gate":{"type":"boolean"},"pause_after_effect":{"type":"boolean"},"result_bytes":{"type":"integer","minimum":0,"maximum":1200000},"result_depth":{"type":"integer","minimum":0,"maximum":128},"payload":{}},"required":["branch"],"additionalProperties":false})
    }
    async fn call(
        &self,
        context: &mut ToolContext,
        args: EffectArgs,
    ) -> Result<rig::tool::ToolOutput> {
        let _ = &args.payload;
        let invocation = context.require::<ToolInvocation>().map_err(worker_error)?;
        let key = invocation
            .logical_key
            .ok_or_else(|| worker_error("logical identity missing"))?;
        write(&self.storage, "started", &true)?;
        while args.gate && !read::<bool>(&self.storage, "released")?.unwrap_or(false) {
            Delay::from(std::time::Duration::from_millis(25)).await;
        }
        self.storage.sql().exec(
            "INSERT INTO fixture_effects (logical_id, branch) VALUES (?, ?)",
            vec![key.canonical().into(), args.branch.clone().into()],
        )?;
        write(&self.storage, "effect_committed", &true)?;
        while args.pause_after_effect
            && !read::<bool>(&self.storage, "effect_released")?.unwrap_or(false)
        {
            Delay::from(std::time::Duration::from_millis(25)).await;
        }
        if let Some(depth) = args.result_depth {
            let mut value = serde_json::json!(args.branch);
            for _ in 0..depth {
                value = serde_json::json!([value]);
            }
            return Ok(rig::tool::ToolOutput::json(value));
        }
        Ok(rig::tool::ToolOutput::text(
            args.result_bytes
                .map_or(args.branch, |size| "x".repeat(size)),
        ))
    }
}

fn child_builder(storage: CellStorage, drift: bool) -> Builder {
    Builder::new(Model::new(
        MockScript::default(),
        FixtureModel {
            source: None,
            second_source: None,
            ordinary_child_batch: None,
            storage: storage.clone(),
        },
    ))
    .implementation_version(if drift { "child-v2" } else { "child-v1" })
    .tool_with(
        Observe { storage },
        ToolOptions::default().policy(ToolPolicy::interrupt_on_uncertain()),
    )
}

#[derive(Deserialize, Serialize)]
struct EffectRow {
    logical_id: String,
    branch: String,
}
#[derive(Deserialize, Serialize)]
struct RequestRow {
    child_id: String,
    operation: String,
    body: String,
}

fn evidence(storage: &CellStorage) -> Result<serde_json::Value> {
    let effects = storage
        .sql()
        .exec(
            "SELECT logical_id, branch FROM fixture_effects ORDER BY sequence",
            None,
        )?
        .to_array::<EffectRow>()?;
    let requests = storage
        .sql()
        .exec(
            "SELECT child_id, operation, body FROM fixture_requests ORDER BY sequence",
            None,
        )?
        .to_array::<RequestRow>()?;
    #[derive(Deserialize)]
    struct StoredEffect {
        data: String,
    }
    let schema = storage
        .sql()
        .exec("SELECT name AS data FROM sqlite_master WHERE type = 'table' AND name = 'rig_script_effects'", None)?
        .to_array::<StoredEffect>()?;
    let rows = if schema.is_empty() {
        Vec::new()
    } else {
        storage
            .sql()
            .exec("SELECT data FROM rig_script_effects ORDER BY key", None)?
            .to_array::<StoredEffect>()?
    };
    let effect_row_bytes: Vec<_> = rows.iter().map(|row| row.data.len()).collect();
    let envelopes = rows
        .into_iter()
        .map(|row| serde_json::from_str::<serde_json::Value>(&row.data).map_err(worker_error))
        .collect::<Result<Vec<_>>>()?;
    let call_schema = storage
        .sql()
        .exec("SELECT name AS data FROM sqlite_master WHERE type = 'table' AND name = 'rig_tool_calls'", None)?
        .to_array::<StoredEffect>()?;
    let calls = if call_schema.is_empty() {
        Vec::new()
    } else {
        storage
            .sql()
            .exec("SELECT data FROM rig_tool_calls ORDER BY key", None)?
            .to_array::<StoredEffect>()?
    };
    let outer_intents = calls.into_iter().map(|row| {
        let mut value: serde_json::Value = serde_json::from_str(&row.data).map_err(worker_error)?;
        let completed = !value["output"].is_null();
        let output = value["output"].clone();
        value["output"] = serde_json::Value::Null;
        Ok(serde_json::json!({"bytes": row.data.len(), "empty_bytes": serde_json::to_vec(&value).map_err(worker_error)?.len(),
            "started": value["started"], "completed": completed, "output": output}))
    }).collect::<Result<Vec<_>>>()?;
    Ok(
        serde_json::json!({"effects": effects, "requests": requests, "effect_envelopes": envelopes, "effect_row_bytes": effect_row_bytes, "outer_intents": outer_intents,
        "started": read::<bool>(storage, "started")?.unwrap_or(false),
        "model_started": read::<bool>(storage, "model_started")?.unwrap_or(false),
        "model_entries": read::<u64>(storage, "model_entries")?.unwrap_or(0),
        "planner_entries": read::<u64>(storage, "planner_entries")?.unwrap_or(0),
        "drive_failures": read::<u64>(storage, "drive_failures")?.unwrap_or(0),
        "drive_error": read::<String>(storage, "drive_error")?,
        "corrupted_reply": read::<String>(storage, "corrupted_reply")?,
        "reply_observed": read::<bool>(storage, "reply_observed")?.unwrap_or(false),
        "paused": read::<bool>(storage, "paused")?.unwrap_or(false),
        "effect_committed": read::<bool>(storage, "effect_committed")?.unwrap_or(false),
        "ack_dropped": read::<bool>(storage, "ack_dropped")?.unwrap_or(false)}),
    )
}

fn record_request(storage: &CellStorage, request: &ChildRequest) -> Result<()> {
    storage.sql().exec(
        "INSERT INTO fixture_requests (child_id, operation, body) VALUES (?, ?, ?)",
        vec![
            request.child_id().into(),
            (match request {
                ChildRequest::Submit { .. } => "submit",
                ChildRequest::Poll { .. } => "poll",
            })
            .into(),
            serde_json::to_string(request).map_err(worker_error)?.into(),
        ],
    )?;
    Ok(())
}

#[durable_object(alarm)]
pub struct ChildSession {
    storage: CellStorage,
    engine: std::cell::OnceCell<Engine<CellStorage, CellStorage>>,
}

impl ChildSession {
    fn engine(&self) -> Result<&Engine<CellStorage, CellStorage>> {
        initialize(&self.storage)?;
        if self.engine.get().is_none() {
            let builder = child_builder(
                self.storage.clone(),
                read::<bool>(&self.storage, "drift")?.unwrap_or(false),
            );
            let engine = builder
                .build(
                    self.storage.clone(),
                    self.storage.clone(),
                    self.storage.state().id().to_string(),
                )
                .map_err(worker_error)?;
            let _ = self.engine.set(engine);
        }
        self.engine
            .get()
            .ok_or_else(|| worker_error("child engine missing"))
    }
}

impl DurableObject for ChildSession {
    fn new(state: State, _: Env) -> Self {
        Self {
            storage: state.into(),
            engine: std::cell::OnceCell::new(),
        }
    }
    async fn fetch(&self, mut request: Request) -> Result<Response> {
        initialize(&self.storage)?;
        match (
            request.method(),
            request.path().rsplit('/').next().unwrap_or_default(),
        ) {
            (Method::Post, "protocol") => {
                let body = request.json::<ChildRequest>().await?;
                record_request(&self.storage, &body)?;
                let operation = match &body {
                    ChildRequest::Submit { .. } => "submit-",
                    ChildRequest::Poll { .. } => "poll-",
                };
                let reply = self
                    .engine()?
                    .handle_child_request(body)
                    .await
                    .map_err(worker_error)?;
                let fault = request
                    .url()?
                    .query_pairs()
                    .find(|(key, _)| key == "fault")
                    .map(|(_, value)| value.into_owned());
                if let Some(fault) = fault.filter(|value| value.starts_with(operation))
                    && matches!(reply, ChildReply::Pending)
                    && read::<String>(&self.storage, "corrupted_reply")?.is_none()
                {
                    write(&self.storage, "corrupted_reply", &fault)?;
                    return Response::ok(if fault.ends_with("oversized") {
                        " ".repeat(rig_durable::durable_object::CHILD_PROTOCOL_MAX_BYTES + 1)
                    } else {
                        "{".into()
                    });
                }
                Response::from_json(&reply)
            }
            (Method::Post, "release") => {
                write(&self.storage, "released", &true)?;
                Response::ok("released")
            }
            (Method::Post, "release-effect") => {
                write(&self.storage, "effect_released", &true)?;
                Response::ok("released")
            }
            (Method::Post, "drift") => {
                write(&self.storage, "drift", &true)?;
                Response::ok("drift stored; restart runtime")
            }
            (Method::Get, "evidence") => Response::from_json(&evidence(&self.storage)?),
            (Method::Get, "status") => {
                Response::from_json(&self.engine()?.status().map_err(worker_error)?)
            }
            (Method::Get, "result") => committed_result(self.engine()?, &request),
            _ => Response::error("unknown child operation", 404),
        }
    }
    async fn alarm(&self) -> Result<Response> {
        self.engine()?.alarm().await.map_err(worker_error)?;
        Response::ok("child alarm handled")
    }
}

struct FaultTransport {
    inner: NamespaceTransport,
    storage: CellStorage,
    lose_ack: bool,
    pause_ack: bool,
    corrupt_reply: Option<String>,
}

impl ChildTransport for FaultTransport {
    fn request<'a>(
        &'a self,
        request: &'a ChildRequest,
    ) -> LocalBoxFuture<'a, std::result::Result<ChildReply, rig_durable::durable_object::Error>>
    {
        Box::pin(async move {
            record_request(&self.storage, request)?;
            let operation = match request {
                ChildRequest::Submit { .. } => "submit-",
                ChildRequest::Poll { .. } => "poll-",
            };
            let target = self
                .corrupt_reply
                .as_deref()
                .is_some_and(|fault| fault.starts_with(operation));
            while target
                && read::<bool>(&self.storage, "reply_observed")?.unwrap_or(false)
                && !read::<bool>(&self.storage, "reply_released")?.unwrap_or(false)
            {
                Delay::from(std::time::Duration::from_millis(25)).await;
            }
            let result = self.inner.request(request).await;
            if target {
                write(&self.storage, "reply_observed", &true)?;
            }
            let reply = result?;
            if self.lose_ack
                && matches!(request, ChildRequest::Submit { .. })
                && !read::<bool>(&self.storage, "ack_dropped")?.unwrap_or(false)
            {
                write(&self.storage, "ack_dropped", &true)?;
                while self.pause_ack
                    && !read::<bool>(&self.storage, "ack_released")?.unwrap_or(false)
                {
                    Delay::from(std::time::Duration::from_millis(25)).await;
                }
                return Err(rig_durable::durable_object::Error::Storage(
                    "injected lost committed child acknowledgement".into(),
                ));
            }
            Ok(reply)
        })
    }
}

struct FaultPlanner {
    inner: Box<dyn ReplayPlanner>,
    identity: String,
    storage: CellStorage,
    config: Configuration,
}

impl ReplayPlanner for FaultPlanner {
    fn runtime_identity(&self) -> &str {
        &self.identity
    }
    fn plan<'a>(&'a self, input: ReplayInput<'a>) -> ReplayFuture<'a> {
        Box::pin(async move {
            increment(&self.storage, "planner_entries")
                .map_err(|e| ReplayError::Runner(e.to_string()))?;
            if self.config.pause_after.is_some_and(|ordinal| {
                input
                    .history
                    .iter()
                    .any(|item| item.call.ordinal == ordinal)
            }) {
                write(&self.storage, "paused", &true)
                    .map_err(|e| ReplayError::Runner(e.to_string()))?;
                while !read::<bool>(&self.storage, "parent_released")
                    .map_err(|e| ReplayError::Runner(e.to_string()))?
                    .unwrap_or(false)
                {
                    Delay::from(std::time::Duration::from_millis(25)).await;
                }
            }
            if self.config.fail_after.is_some_and(|ordinal| {
                input
                    .history
                    .iter()
                    .any(|item| item.call.ordinal == ordinal)
            }) {
                return Err(ReplayError::Runner(
                    "injected planner failure after committed delivery".into(),
                ));
            }
            let step = self.inner.plan(input).await?;
            if self.config.pause_second_result
                && self.config.second_source.as_deref() == Some(input.code)
            {
                write(&self.storage, "paused", &true)
                    .map_err(|e| ReplayError::Runner(e.to_string()))?;
                while !read::<bool>(&self.storage, "parent_released")
                    .map_err(|e| ReplayError::Runner(e.to_string()))?
                    .unwrap_or(false)
                {
                    Delay::from(std::time::Duration::from_millis(25)).await;
                }
            }
            Ok(step)
        })
    }
}

#[durable_object(alarm)]
pub struct ScriptSession {
    storage: CellStorage,
    env: Env,
    boot_ms: u64,
    engine: std::cell::OnceCell<Engine<CellStorage, CellStorage>>,
}

impl ScriptSession {
    fn parent_key(&self, prompt: u64) -> LogicalCallKey {
        LogicalCallKey {
            logical_execution_id: self.storage.state().id().to_string(),
            submission_id: format!("prompt-{prompt}"),
            model_turn: 1,
            call_index: 0,
        }
    }
    fn engine(&self) -> Result<&Engine<CellStorage, CellStorage>> {
        initialize(&self.storage)?;
        if self.engine.get().is_none() {
            let config = read::<Configuration>(&self.storage, "config")?
                .ok_or_else(|| worker_error("configure before submit"))?;
            let inner: Box<dyn ReplayPlanner> = Box::new(super::worker_planner(&self.env)?);
            let identity = format!(
                "{}{}",
                inner.runtime_identity(),
                if config.drift { "/drift" } else { "" }
            );
            let planner = FaultPlanner {
                inner,
                identity,
                storage: self.storage.clone(),
                config: config.clone(),
            };
            let definition = child_builder(self.storage.clone(), false)
                .child_definition()
                .map_err(worker_error)?;
            let transport = FaultTransport {
                inner: NamespaceTransport::new(
                    self.env.durable_object("CHILDREN")?,
                    format!(
                        "https://child.invalid/protocol?fault={}",
                        config.corrupt_reply.as_deref().unwrap_or_default()
                    ),
                ),
                storage: self.storage.clone(),
                lose_ack: config.lose_ack,
                pause_ack: config.pause_ack,
                corrupt_reply: config.corrupt_reply.clone(),
            };
            let child = ToolDefinition {
                name: ToolName::new("child").map_err(worker_error)?,
                description: if config.catalog_drift {
                    "Changed child catalog"
                } else {
                    "Run a durable child"
                }
                .into(),
                parameters: serde_json::json!({"type":"object","properties":{"branch":{"type":"string"},"gate":{"type":"boolean"},"pause_after_effect":{"type":"boolean"},"result_bytes":{"type":"integer"},"payload":{}},"required":["branch"],"additionalProperties":false}),
            };
            let builder = Builder::new(Model::new(
                MockScript::default(),
                FixtureModel {
                    source: Some(config.source),
                    second_source: config.second_source,
                    ordinary_child_batch: config.ordinary_child_batch.clone(),
                    storage: self.storage.clone(),
                },
            ))
            .sub_agent(child, ChildHandle::new(definition, transport))
            .map_err(worker_error)?
            .tool_with(
                Observe {
                    storage: self.storage.clone(),
                },
                ToolOptions::default().policy(
                    ToolPolicy::interrupt_on_uncertain().implementation_version(
                        if config.policy_drift {
                            "effect-v2"
                        } else {
                            "effect-v1"
                        },
                    ),
                ),
            )
            .session_history_max_bytes(config.max_history_bytes);
            let builder = if config.ordinary_child_batch.is_some() {
                builder
            } else {
                builder.code_mode(
                    Limits {
                        max_calls: 8,
                        max_in_flight: config.max_in_flight,
                        source_bytes: config.source_bytes,
                        ..Limits::default()
                    },
                    planner,
                )
            };
            let engine = builder
                .build(
                    self.storage.clone(),
                    self.storage.clone(),
                    self.storage.state().id().to_string(),
                )
                .map_err(worker_error)?;
            let _ = self.engine.set(engine);
        }
        self.engine
            .get()
            .ok_or_else(|| worker_error("parent engine missing"))
    }
    async fn child_route(&self, request: &Request, operation: &str) -> Result<Response> {
        let url = request.url()?;
        let ordinal = url
            .query_pairs()
            .find(|(key, _)| key == "ordinal")
            .map(|(_, v)| v.parse::<u64>())
            .transpose()
            .map_err(worker_error)?;
        let prompt = url
            .query_pairs()
            .find(|(key, _)| key == "prompt")
            .map(|(_, v)| v.parse::<u64>())
            .transpose()
            .map_err(worker_error)?
            .unwrap_or(0);
        let mut key = self.parent_key(prompt);
        key.call_index = url
            .query_pairs()
            .find(|(k, _)| k == "call_index")
            .map(|(_, v)| v.parse())
            .transpose()
            .map_err(worker_error)?
            .unwrap_or(0);
        let id = rig_durable::durable_object::child_id(&key, ordinal);
        let mut init = RequestInit::new();
        init.with_method(request.method());
        self.env
            .durable_object("CHILDREN")?
            .id_from_name(&id)?
            .get_stub()?
            .fetch_with_request(Request::new_with_init(
                &format!("https://child.invalid/{operation}?request_id={id}"),
                &init,
            )?)
            .await
    }
}

impl DurableObject for ScriptSession {
    fn new(state: State, env: Env) -> Self {
        Self {
            storage: state.into(),
            env,
            boot_ms: Date::now().as_millis(),
            engine: std::cell::OnceCell::new(),
        }
    }
    async fn fetch(&self, mut request: Request) -> Result<Response> {
        initialize(&self.storage)?;
        let path = request.path();
        let operation = path.rsplit('/').next().unwrap_or_default();
        match (request.method(), operation) {
            (Method::Post, "configure") => {
                write(
                    &self.storage,
                    "config",
                    &request.json::<Configuration>().await?,
                )?;
                Response::ok("configured; restart to replace an initialized engine")
            }
            (Method::Post, "release-parent") => {
                write(&self.storage, "parent_released", &true)?;
                Response::ok("released")
            }
            (Method::Post, "release-ack") => {
                write(&self.storage, "ack_released", &true)?;
                Response::ok("released")
            }
            (Method::Post, "release-reply") => {
                write(&self.storage, "reply_released", &true)?;
                Response::ok("released")
            }
            (Method::Post, "release-effect") => {
                write(&self.storage, "effect_released", &true)?;
                Response::ok("released")
            }
            (Method::Get, "child-evidence") => self.child_route(&request, "evidence").await,
            (Method::Get, "child-status") => self.child_route(&request, "status").await,
            (Method::Get, "child-result") => self.child_route(&request, "result").await,
            (Method::Post, "child-release") => self.child_route(&request, "release").await,
            (Method::Post, "child-release-effect") => {
                self.child_route(&request, "release-effect").await
            }
            (Method::Get, "evidence") => {
                let mut value = evidence(&self.storage)?;
                value["boot_ms"] = self.boot_ms.into();
                Response::from_json(&value)
            }
            (Method::Get, "state") => {
                let prompt = request
                    .url()?
                    .query_pairs()
                    .find(|(key, _)| key == "prompt")
                    .map(|(_, v)| v.parse::<u64>())
                    .transpose()
                    .map_err(worker_error)?
                    .unwrap_or(0);
                let key = self.parent_key(prompt);
                Response::from_json(
                    &serde_json::json!({"key": key, "scheduler": self.engine()?.script_state(&key).map_err(worker_error)?}),
                )
            }
            (Method::Get, "status") => {
                Response::from_json(&self.engine()?.status().map_err(worker_error)?)
            }
            (Method::Get, "result") => committed_result(self.engine()?, &request),
            (Method::Get, "transcript") => {
                Response::from_json(&self.engine()?.transcript().map_err(worker_error)?)
            }
            (Method::Post, "drive") => {
                let error = self.engine()?.drive().await.err().map(|e| e.to_string());
                if let Some(error) = &error {
                    write(&self.storage, "drive_error", error)?;
                    increment(&self.storage, "drive_failures")?;
                }
                Response::from_json(&error)
            }
            (Method::Post, "submit") => Response::from_json(
                &self
                    .engine()?
                    .submit(request.json::<SubmitInput>().await?)
                    .await
                    .map_err(worker_error)?,
            ),
            (Method::Post, "close") => {
                self.engine()?.close().map_err(worker_error)?;
                Response::ok("closed")
            }
            _ => Response::error("unknown fixture operation", 404),
        }
    }
    async fn alarm(&self) -> Result<Response> {
        if let Err(error) = self.engine()?.alarm().await {
            write(&self.storage, "drive_error", &error.to_string())?;
            increment(&self.storage, "drive_failures")?;
            return Err(worker_error(error));
        }
        Response::ok("parent alarm handled")
    }
}
