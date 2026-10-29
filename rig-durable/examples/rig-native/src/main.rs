//! A single-run, sequential Rig runner with durable effect replay.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use rig_agent::{
    AgentBuilder, AgentRun,
    handlers::Handlers,
    tool::{RegisteredTool, server::ToolServer},
};
use rig_cassette::{
    agent::AgentReplayExt,
    effect_log::{
        Checkpoint, EffectLog, EffectLogRecorder, EffectLogReplayer, LogHeader, ReplayRefusals,
    },
};
use rig_core::{
    completion::{AssistantContent, CompletionResponse, Message, Usage},
    effect::{
        Answered, EffectId, EffectKind, FamilyDescriptor, HandlerDescriptor, HandlerKey, family,
    },
    error::{ErrorKind, ErrorReport},
    message::{Origin, ToolName, UserContent},
    serve::{Dispatch, DispatchContext, Recorder, Reply, Serve},
    streaming::{Item, StreamEvent},
    tool::{ToolOutput, ToolResult, ToolResultContext},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::json;

const MODEL: &str = "prototype:model:v1";
const TOOL: &str = "prototype:charge:v1";
const PREAMBLE: &str = "Use charge once, then report its exact feedback.";

struct Store(Mutex<Connection>);

impl Store {
    fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path)?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS effects (id INTEGER PRIMARY KEY, record TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS intents (
                 id INTEGER PRIMARY KEY, request TEXT NOT NULL, state TEXT NOT NULL, answer TEXT
             );
             CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, count INTEGER NOT NULL);",
        )?;
        Ok(Self(Mutex::new(db)))
    }

    fn with<T>(&self, f: impl FnOnce(&mut Connection) -> Result<T>) -> Result<T> {
        let mut db = self
            .0
            .lock()
            .map_err(|_| anyhow!("database mutex poisoned"))?;
        f(&mut db)
    }

    fn restore(&self) -> Result<(Checkpoint<AgentRun>, EffectLog)> {
        self.with(|db| {
            let initial = EffectLog::default()
                .checkpoint(
                    0,
                    AgentRun::new("Charge invoice-7 for 37 units.").max_turns(4),
                )
                .0;
            db.execute(
                "INSERT OR IGNORE INTO meta VALUES ('checkpoint', ?1)",
                [serde_json::to_string(&initial)?],
            )?;
            let saved: String =
                db.query_row("SELECT value FROM meta WHERE key='checkpoint'", [], |row| {
                    row.get(0)
                })?;
            let checkpoint = serde_json::from_str(&saved)?;
            let header: Option<String> = db
                .query_row("SELECT value FROM meta WHERE key='header'", [], |row| {
                    row.get(0)
                })
                .optional()?;
            let mut statement = db.prepare("SELECT record FROM effects ORDER BY id")?;
            let records = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .map(|row| Ok(serde_json::from_str(&row?)?))
                .collect::<Result<Vec<_>>>()?;
            let log = EffectLog {
                header: header
                    .map(|value| serde_json::from_str(&value))
                    .transpose()?
                    .unwrap_or_default(),
                records,
            };
            Ok((checkpoint, log))
        })
    }

    fn save(&self, log: &EffectLog, seed: AgentRun) -> Result<()> {
        self.with(|db| {
            let tx = db.transaction()?;
            let (mut checkpoint, _) = log.checkpoint(0, seed);
            for record in &log.records {
                let encoded = serde_json::to_string(record)?;
                let existing: Option<String> = tx
                    .query_row(
                        "SELECT record FROM effects WHERE id=?1",
                        [record.id.as_u64()],
                        |row| row.get(0),
                    )
                    .optional()?;
                if let Some(existing) = existing {
                    ensure!(
                        serde_json::from_str::<serde_json::Value>(&existing)?
                            == serde_json::from_str::<serde_json::Value>(&encoded)?,
                        "recorded effect changed at {}",
                        record.id
                    );
                } else {
                    tx.execute(
                        "INSERT INTO effects VALUES (?1, ?2)",
                        params![record.id.as_u64(), encoded],
                    )?;
                }
            }
            let first: Option<u64> =
                tx.query_row("SELECT min(id) FROM effects", [], |row| row.get(0))?;
            checkpoint.next = first.map(EffectId::from_raw);
            for (key, value) in [
                ("checkpoint", serde_json::to_string(&checkpoint)?),
                ("header", serde_json::to_string(&log.header)?),
            ] {
                tx.execute(
                    "INSERT OR REPLACE INTO meta VALUES (?1, ?2)",
                    params![key, value],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
    }
}

#[derive(Clone)]
struct Host {
    store: Arc<Store>,
    stopped: Arc<Mutex<Option<String>>>,
    fault: Arc<str>,
}

impl Host {
    fn stop(&self, message: impl ToString) -> ErrorReport {
        let message = message.to_string();
        if let Ok(mut stopped) = self.stopped.lock() {
            stopped.get_or_insert_with(|| message.clone());
        }
        ErrorReport::new(ErrorKind::Cancelled, message)
    }

    fn check(&self) -> Result<()> {
        if let Some(reason) = self
            .stopped
            .lock()
            .map_err(|_| anyhow!("stop mutex poisoned"))?
            .as_ref()
        {
            bail!("{reason}");
        }
        Ok(())
    }

    fn crash(&self, point: &str) {
        if self.fault.as_ref() == point {
            eprintln!("injected process exit: {point}");
            std::process::exit(75);
        }
    }

    fn model(&self, kind: &EffectKind) -> Result<Answered> {
        let EffectKind::Completion {
            request,
            stream: false,
        } = kind
        else {
            bail!("the prototype supports unary completions only");
        };
        self.store.with(|db| {
            db.execute("INSERT INTO counters VALUES ('model', 1) ON CONFLICT(name) DO UPDATE SET count=count+1", [])?;
            Ok(())
        })?;
        let feedback = request
            .chat_history
            .iter()
            .find_map(|message| match message.as_ref() {
                Message::User { content } => content.iter().find_map(|item| match item {
                    UserContent::ToolResult(result) => Some(result),
                    _ => None,
                }),
                _ => None,
            });
        let choice = match feedback {
            Some(feedback) => vec![AssistantContent::text(format!(
                "Tool feedback: {}",
                serde_json::to_string(feedback)?
            ))],
            None => vec![AssistantContent::tool_call(
                "provider-call-1",
                ToolName::new("charge")?,
                json!({"invoice":"invoice-7","amount":37}),
            )],
        };
        Ok(Answered::Completion(Arc::new(CompletionResponse::new(
            choice,
            Usage::default(),
            Origin::new("prototype", "prototype", "scripted"),
            json!({}),
        ))))
    }

    fn tool(&self, id: EffectId, kind: &EffectKind) -> Result<Answered> {
        let EffectKind::ToolCall { name, args } = kind else {
            bail!("expected a tool call")
        };
        ensure!(
            name == "charge"
                && serde_json::from_str::<serde_json::Value>(args)?
                    == json!({"invoice":"invoice-7","amount":37}),
            "unexpected charge arguments"
        );
        let request = serde_json::to_string(kind)?;
        let ready = self.store.with(|db| {
            let tx = db.transaction()?;
            tx.execute(
                "INSERT OR IGNORE INTO intents (id, request, state) VALUES (?1, ?2, 'awaiting')",
                params![id.as_u64(), request],
            )?;
            let (retained, state, answer): (String, String, Option<String>) = tx.query_row(
                "SELECT request, state, answer FROM intents WHERE id=?1",
                [id.as_u64()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            ensure!(retained == request, "intent request changed at {id}");
            let ready = match state.as_str() {
                "awaiting" => Err(anyhow!("approval required for effect {id}")),
                "claimed" => Err(anyhow!("uncertain tool effect {id}; execution refused")),
                "settled" => Ok(Some(serde_json::from_str(
                    &answer.context("settled intent has no answer")?,
                )?)),
                "denied" => Ok(Some(Answered::ToolResult {
                    result: ToolResult::skipped("denied by reviewer"),
                })),
                "approved" => {
                    tx.execute(
                        "UPDATE intents SET state='claimed' WHERE id=?1",
                        [id.as_u64()],
                    )?;
                    Ok(None)
                }
                _ => Err(anyhow!("unknown intent state: {state}")),
            };
            tx.commit()?;
            ready
        })?;
        if let Some(answer) = ready {
            return Ok(answer);
        }
        self.crash("after-claim");
        // A separate commit represents an external service that cannot join the journal transaction.
        self.store.with(|db| {
            db.execute("INSERT INTO counters VALUES ('charge', 1) ON CONFLICT(name) DO UPDATE SET count=count+1", [])?;
            Ok(())
        })?;
        self.crash("after-effect");
        let answer = Answered::ToolResult {
            result: ToolResult::success(ToolOutput::text("receipt:invoice-7:37")),
        };
        self.store.with(|db| {
            ensure!(
                db.execute(
                    "UPDATE intents SET state='settled', answer=?2 WHERE id=?1 AND state='claimed'",
                    params![id.as_u64(), serde_json::to_string(&answer)?]
                )? == 1,
                "claim settlement refused"
            );
            Ok(())
        })?;
        self.crash("after-settle");
        Ok(answer)
    }
}

#[derive(Clone)]
struct DurableRecorder {
    inner: EffectLogRecorder,
    identity: Arc<Mutex<LogHeader>>,
    seed: AgentRun,
    host: Host,
}

impl Recorder for DurableRecorder {
    fn handlers(&self, handlers: Vec<HandlerDescriptor>) {
        self.inner.handlers(handlers);
    }
    fn begin(&self, id: EffectId, key: HandlerKey, kind: EffectKind, origin: DispatchContext) {
        self.inner.begin(id, key, kind, origin);
    }
    fn discard(&self, id: EffectId) {
        self.inner.discard(id);
    }
    fn patch(&self, id: EffectId, kind: EffectKind) {
        self.inner.patch(id, kind);
    }
    fn keep_events(&self) -> bool {
        false
    }
    fn event(&self, id: EffectId, item: &Item<StreamEvent>) {
        self.inner.event(id, item);
    }
    fn origin(&self, id: EffectId, origin: &Origin) {
        self.inner.origin(id, origin);
    }
    fn tool_output(&self, id: EffectId, output: ToolResultContext) {
        self.inner.tool_output(id, output);
    }
    fn resolve(&self, id: EffectId, outcome: Result<Answered, ErrorReport>) {
        // Control exits do not answer the effect. Recovery must revisit its intent.
        if self.host.check().is_err() {
            return;
        }
        self.inner.resolve(id, outcome);
        let saved =
            (|| -> Result<()> {
                let mut log = self.inner.take();
                let identity = self
                    .identity
                    .lock()
                    .map_err(|_| anyhow!("identity mutex poisoned"))?;
                log.header.run_spec = identity.run_spec;
                log.header.hooks = identity.hooks.clone();
                log.header.required = identity.required.clone();
                self.host.store.save(&log, self.seed.clone())?;
                if log.records.iter().any(|record| {
                    record.id == id && matches!(record.kind, EffectKind::ToolCall { .. })
                }) {
                    self.host.crash("after-record");
                }
                Ok(())
            })();
        if let Err(error) = saved {
            self.host.stop(error);
        }
    }
}

struct Bridge {
    descriptor: HandlerDescriptor,
    replay: Option<EffectLogReplayer>,
    remaining: Arc<Mutex<BTreeMap<EffectId, HandlerKey>>>,
    refusals: ReplayRefusals,
    host: Host,
}

impl Serve for Bridge {
    type Family = family::Dynamic;
    fn descriptor(&self) -> HandlerDescriptor {
        self.descriptor.clone()
    }

    async fn serve(&self, kind: EffectKind, dispatch: Dispatch) -> Reply {
        let result = async {
            self.host.check().map_err(|error| self.host.stop(error))?;
            let replay = {
                let mut remaining = self
                    .remaining
                    .lock()
                    .map_err(|_| self.host.stop("replay mutex poisoned"))?;
                if let Some(key) = remaining.remove(&dispatch.id()) {
                    if key != self.descriptor.key {
                        return Err(self.host.stop("recorded handler changed"));
                    }
                    true
                } else {
                    if !remaining.is_empty() {
                        return Err(self
                            .host
                            .stop("new work before the replay prefix was consumed"));
                    }
                    false
                }
            };
            if replay {
                let replayer = self
                    .replay
                    .as_ref()
                    .ok_or_else(|| self.host.stop("missing replayer"))?;
                let answer = replayer.serve(kind, dispatch).await.into_outcome().await;
                if let Some(error) = self.refusals.take() {
                    return Err(self.host.stop(error));
                }
                return answer;
            }
            match &kind {
                EffectKind::Completion { .. } => self.host.model(&kind),
                EffectKind::ToolCall { .. } => self.host.tool(dispatch.id(), &kind),
                _ => Err(anyhow!("unsupported effect")),
            }
            .map_err(|error| self.host.stop(error))
        }
        .await;
        Reply::Outcome(result)
    }
}

async fn run(store: Arc<Store>, fault: &str, preamble: &str) -> Result<()> {
    let (checkpoint, log) = store.restore()?;
    let log = EffectLog::from_checkpoint(&checkpoint, log)?;
    let host = Host {
        store,
        stopped: Arc::new(Mutex::new(None)),
        fault: fault.into(),
    };
    let remaining = Arc::new(Mutex::new(
        log.records
            .iter()
            .map(|record| (record.id, record.key.clone()))
            .collect(),
    ));
    let refusals = ReplayRefusals::default();
    let bridge = |descriptor: HandlerDescriptor| -> Result<Bridge> {
        let replay = if log
            .records
            .iter()
            .any(|record| record.key == descriptor.key)
        {
            Some(
                EffectLogReplayer::for_key_by_id(&log, &descriptor.key)?
                    .reporting_refusals(refusals.clone()),
            )
        } else {
            None
        };
        Ok(Bridge {
            descriptor,
            replay,
            remaining: remaining.clone(),
            refusals: refusals.clone(),
            host: host.clone(),
        })
    };
    let handlers = Handlers::default();
    handlers.register(
        MODEL,
        bridge(HandlerDescriptor {
            key: MODEL.into(),
            family: FamilyDescriptor::Completion {
                model: "prototype/scripted".into(),
                capabilities: Default::default(),
            },
            layers: vec![],
        })?,
    )?;
    let mut descriptor = HandlerDescriptor::tool(
        "charge",
        "Charge an invoice",
        json!({"type":"object","properties":{"invoice":{"type":"string"},"amount":{"type":"integer"}},"required":["invoice","amount"]}),
    );
    descriptor.key = TOOL.into();
    let tools = ToolServer::new()
        .owner("prototype")
        .registered_tool(RegisteredTool::from_handler(bridge(descriptor)?)?)
        .run();
    let recorder = DurableRecorder {
        inner: EffectLogRecorder::new(),
        identity: Arc::new(Mutex::new(LogHeader::default())),
        seed: checkpoint.state.clone(),
        host: host.clone(),
    };
    let agent = AgentBuilder::over_handlers(handlers, "prototype", MODEL.into())
        .tool_server_handle(tools)
        .preamble(preamble)
        .default_max_turns(4)
        .record_to(recorder.clone())
        .build();
    if !log.records.is_empty() {
        agent.check_replayable(&log)?;
    }
    *recorder
        .identity
        .lock()
        .map_err(|_| anyhow!("identity mutex poisoned"))? =
        agent.stamp(recorder.inner.log()).header;
    let result = agent
        .resume(checkpoint.state)
        .without_memory()
        .tool_concurrency(1)
        .await;
    host.check()?;
    ensure!(
        remaining
            .lock()
            .map_err(|_| anyhow!("replay mutex poisoned"))?
            .is_empty(),
        "run ended with unconsumed records"
    );
    println!("{}", result?.output());
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let path = args
        .get(1)
        .context("usage: rig-native-prototype DATABASE run|approve|deny [FAULT] [PREAMBLE]")?;
    let store = Arc::new(Store::open(Path::new(path))?);
    match args.get(2).map(String::as_str) {
        Some("run") => {
            run(
                store,
                args.get(3).map(String::as_str).unwrap_or("none"),
                args.get(4).map(String::as_str).unwrap_or(PREAMBLE),
            )
            .await
        }
        Some(command @ ("approve" | "deny")) => store.with(|db| {
            let state = if command == "approve" {
                "approved"
            } else {
                "denied"
            };
            ensure!(
                db.execute(
                    "UPDATE intents SET state=?1 WHERE state='awaiting'",
                    [state]
                )? == 1,
                "expected one pending approval"
            );
            println!("decision saved: {state}");
            Ok(())
        }),
        _ => bail!("expected run, approve, or deny"),
    }
}
