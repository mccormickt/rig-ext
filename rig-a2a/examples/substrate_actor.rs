//! Disposable A2A actor with deterministic or local Ollama inference.
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use a2a::{AgentCapabilities, AgentCard, AgentInterface, TRANSPORT_PROTOCOL_JSONRPC};
use a2a_server::{
    DefaultRequestHandler, StaticAgentCard, agent_card, jsonrpc::jsonrpc_router,
    task_store::InMemoryTaskStore,
};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path, Request, State},
    http::StatusCode,
    middleware::{Next, from_fn},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rig_a2a::server::RigExecutor;
use rig_agent::agent::AgentBuilder;
use rig_core::{
    completion::CompletionRequest,
    driver::{Exchange, Model, Opened, Opening, Transport},
    message::{Message, UserContent},
    providers::ollama::OllamaConfig,
    test_utils::{MockCompletionModel, MockFrame, MockScript, MockTurn},
    tool::PortableTool,
};
use serde_json::{Value, json};

#[derive(Clone)]
struct FixtureModel(Arc<Memory>);

impl Transport<MockScript> for FixtureModel {
    fn send(&self, request: CompletionRequest, _: Exchange) -> Opening<MockFrame> {
        let memory = self.0.clone();
        Opening::new(async move {
            memory.completions.fetch_add(1, Ordering::SeqCst);
            let mut prompts = Vec::new();
            let mut last_result = None;
            for message in &request.chat_history {
                if let Message::User { content } = message {
                    for part in content {
                        match part {
                            UserContent::Text(text) => {
                                prompts.push(text.text.clone());
                                last_result = None;
                            }
                            UserContent::ToolResult(result) => {
                                last_result =
                                    Some(serde_json::to_value(&result.content).map_err(|e| {
                                        rig_core::error::ProviderError::Provider(e.to_string())
                                    })?);
                            }
                            _ => {}
                        }
                    }
                }
            }
            let turn = if let Some(result) = last_result {
                MockTurn::text(json!({"prompts": prompts, "tool_result": result}).to_string())
            } else {
                let prompt = prompts.last().ok_or_else(|| {
                    rig_core::error::ProviderError::Provider("missing prompt".into())
                })?;
                let mut args: serde_json::Map<String, Value> = serde_json::from_str(prompt)
                    .map_err(|e| rig_core::error::ProviderError::Provider(e.to_string()))?;
                args.insert("prompts".into(), json!(prompts));
                MockTurn::tool_call("record-call", "record", Value::Object(args))
            };
            let response = MockCompletionModel::from_turns([turn])
                .call(request)
                .await?;
            Ok(Opened::new(futures::stream::iter([Ok(
                MockFrame::Response(Box::new(response)),
            )])))
        })
    }
}

#[derive(Default)]
struct Memory {
    nonce: Mutex<Option<String>>,
    completions: AtomicU64,
    invocations: AtomicU64,
    commits: AtomicU64,
    readiness_failures: AtomicU64,
    readiness_delay: Mutex<(u64, Option<std::time::Instant>)>,
    held: AtomicBool,
    release: AtomicBool,
}

#[derive(Clone)]
struct Record {
    memory: Arc<Memory>,
    journal: PathBuf,
    file_lock: Arc<tokio::sync::Mutex<()>>,
}

impl PortableTool for Record {
    const NAME: &'static str = "record";
    type Args = Value;
    type Output = Value;
    type Error = std::io::Error;

    fn description(&self) -> String {
        "Append a fixture operation to the actor file journal".into()
    }
    fn parameters(&self) -> Value {
        json!({"type":"object", "properties":{"operation":{"type":"string"},"value":{"type":"string"},"hold":{"type":"boolean"},"prompts":{"type":"array"}},"required":["operation","value","prompts"]})
    }

    async fn call(&self, args: Value) -> Result<Value, Self::Error> {
        self.memory.invocations.fetch_add(1, Ordering::SeqCst);
        if args["hold"] == true {
            self.memory.release.store(false, Ordering::SeqCst);
            self.memory.held.store(true, Ordering::SeqCst);
            while !self.memory.release.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
        let _lock = self.file_lock.lock().await;
        let nonce = {
            let mut nonce = self.memory.nonce.lock().unwrap_or_else(|e| e.into_inner());
            nonce.get_or_insert_with(a2a::new_task_id).clone()
        };
        let existing = match tokio::fs::read_to_string(&self.journal).await {
            Ok(contents) => contents,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(e),
        };
        let operation = args["operation"]
            .as_str()
            .ok_or_else(|| std::io::Error::other("operation required"))?;
        for line in existing.lines() {
            let previous: Value = serde_json::from_str(line)?;
            if previous["operation"] == operation {
                if previous["value"] != args["value"] {
                    return Err(std::io::Error::other(
                        "operation reused with different value",
                    ));
                }
                self.memory.held.store(false, Ordering::SeqCst);
                return Ok(json!({"operation":operation,"duplicate":true,"nonce":nonce}));
            }
        }
        let row = json!({"operation":operation,"value":args["value"],"prompts":args["prompts"]});
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.journal)
            .await?;
        file.write_all(format!("{row}\n").as_bytes()).await?;
        file.sync_all().await?;
        drop(file);
        let commits = self.memory.commits.fetch_add(1, Ordering::SeqCst) + 1;
        self.memory.held.store(false, Ordering::SeqCst);
        eprintln!(
            "{}",
            json!({"event":"actor_effect","operation":operation,"nonce":nonce,"commits":commits})
        );
        Ok(json!({"operation":operation,"duplicate":false,"nonce":nonce,"commits":commits}))
    }
}

#[derive(Clone)]
struct App {
    executor: RigExecutor,
    record: Record,
}

async fn inspect(State(app): State<App>) -> Result<Json<Value>, StatusCode> {
    let journal = match tokio::fs::read_to_string(&app.record.journal).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };
    Ok(Json(json!({
        "active":app.executor.active(),
        "nonce":*app.record.memory.nonce.lock().unwrap_or_else(|e| e.into_inner()),
        "completions":app.record.memory.completions.load(Ordering::SeqCst),
        "invocations":app.record.memory.invocations.load(Ordering::SeqCst),
        "commits":app.record.memory.commits.load(Ordering::SeqCst),
        "held":app.record.memory.held.load(Ordering::SeqCst),
        "journal":journal,
        "pid":std::process::id(),
        "build":option_env!("RIG_POC_BUILD").unwrap_or("local")
    })))
}

async fn probe_egress(url: String) -> Json<Value> {
    let result = reqwest::Client::new()
        .get(url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await;
    Json(match result {
        Ok(response) => json!({"status":response.status().as_u16()}),
        Err(error) => json!({"error":error.to_string()}),
    })
}

async fn probe_dns(Path(transport): Path<String>) -> Result<Json<Value>, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let operation = async {
        let resolv = tokio::fs::read_to_string("/etc/resolv.conf").await?;
        let server = resolv
            .lines()
            .find_map(|line| line.strip_prefix("nameserver "))
            .ok_or_else(|| std::io::Error::other("no nameserver"))?
            .trim();
        let address = format!("{server}:53");
        let mut query = vec![0x52, 0x49, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
        for label in "ollama.rig-local.svc.cluster.local".split('.') {
            query.push(label.len() as u8);
            query.extend_from_slice(label.as_bytes());
        }
        query.extend_from_slice(&[0, 0, 1, 0, 1]);
        let reply = if transport == "tcp" {
            let mut socket = tokio::net::TcpStream::connect(&address).await?;
            socket.write_u16(query.len() as u16).await?;
            socket.write_all(&query).await?;
            let length = socket.read_u16().await?;
            let mut reply = vec![0; usize::from(length)];
            socket.read_exact(&mut reply).await?;
            reply
        } else if transport == "udp" {
            let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
            socket.connect(&address).await?;
            socket.send(&query).await?;
            let mut reply = vec![0; 4096];
            let length = socket.recv(&mut reply).await?;
            reply.truncate(length);
            reply
        } else {
            return Err(std::io::Error::other("transport must be udp or tcp"));
        };
        Ok(Json(json!({"server":server,"query":query,"reply":reply})))
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), operation)
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
}

async fn reject_tenant(request: Request, next: Next) -> Response {
    let (parts, body) = request.into_parts();
    let Ok(bytes) = to_bytes(body, 1024 * 1024).await else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    if let Ok(value) = serde_json::from_slice::<Value>(&bytes)
        && value
            .get("params")
            .and_then(|p| p.get("tenant"))
            .is_some_and(|t| !t.is_null())
    {
        return Json(json!({
            "jsonrpc": "2.0", "id": value.get("id"),
            "error": {"code": -32602, "message": "this actor does not support tenants"}
        }))
        .into_response();
    }
    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("rig_a2a::server=info,rig::completions=trace,rig::streaming=trace")
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .init();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    tokio::fs::create_dir_all(&data).await?;
    let record = Record {
        memory: Arc::default(),
        journal: data.join("operations.jsonl"),
        file_lock: Arc::default(),
    };
    let inference = std::env::var("INFERENCE").unwrap_or_else(|_| "fixture".into());
    let builder = match inference.as_str() {
        "fixture" => AgentBuilder::new(Model::new(
            MockScript::default(),
            FixtureModel(record.memory.clone()),
        )),
        "ollama" => {
            let endpoint = std::env::var("OLLAMA_URL")?;
            let model = std::env::var("OLLAMA_MODEL")?;
            eprintln!(
                "{}",
                json!({"event":"inference_config","mode":inference,"endpoint":endpoint,"model":model})
            );
            AgentBuilder::new(OllamaConfig::new().with_base_url(endpoint).client().native_completion(model))
                .temperature(0.0)
                .additional_params(json!({"think":false,"options":{"seed":42,"num_ctx":8192,"num_predict":512}}))
                .preamble("Use the record tool exactly once for each user request. Copy operation and value exactly. Set prompts to an empty array; it is not evidence of history. After the tool result, answer briefly. When asked for a value from a prior turn, use the actual conversation history.")
        }
        _ => anyhow::bail!("unknown INFERENCE mode: {inference}"),
    };
    let agent = builder.tool(record.clone()).build();
    let executor = RigExecutor::new(agent);
    let handler = Arc::new(DefaultRequestHandler::new(
        executor.clone(),
        InMemoryTaskStore::new(),
    ));
    let public = std::env::var("PUBLIC_URL").unwrap_or_else(|_| "http://127.0.0.1:18081".into());
    let card = AgentCard {
        name: "rig-substrate-poc".into(),
        description: format!("Real Rig loop with {inference} inference"),
        version: "0.1.0".into(),
        supported_interfaces: vec![AgentInterface::new(
            format!("{public}/jsonrpc"),
            TRANSPORT_PROTOCOL_JSONRPC,
        )],
        capabilities: AgentCapabilities {
            streaming: Some(true),
            push_notifications: Some(false),
            extensions: None,
            extended_agent_card: Some(false),
        },
        default_input_modes: vec!["text/plain".into()],
        default_output_modes: vec!["text/plain".into()],
        skills: vec![],
        provider: None,
        documentation_url: None,
        icon_url: None,
        security_schemes: None,
        security_requirements: None,
        signatures: None,
    };
    let app = App { executor, record };
    let router = Router::new()
        .route(
            "/readyz",
            get(|State(app): State<App>| async move {
                let remaining = app
                    .record
                    .memory
                    .readiness_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .unwrap_or(0);
                let mut delay = app
                    .record
                    .memory
                    .readiness_delay
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let elapsed = delay
                    .1
                    .get_or_insert_with(std::time::Instant::now)
                    .elapsed();
                let ready = remaining == 0 && elapsed.as_millis() >= u128::from(delay.0);
                eprintln!(
                    "{}",
                    json!({"event":"readiness","remaining":remaining,
                    "delay_ms":delay.0,"elapsed_ms":elapsed.as_millis(),"ready":ready})
                );
                if ready {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .route(
            "/readiness/{failures}",
            post(
                |State(app): State<App>, Path(failures): Path<u64>| async move {
                    app.record
                        .memory
                        .readiness_failures
                        .store(failures, Ordering::SeqCst);
                    StatusCode::OK
                },
            ),
        )
        .route(
            "/readiness-delay/{milliseconds}",
            post(
                |State(app): State<App>, Path(milliseconds): Path<u64>| async move {
                    *app.record
                        .memory
                        .readiness_delay
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()) = (milliseconds, None);
                    StatusCode::OK
                },
            ),
        )
        .route("/inspect", get(inspect))
        .route(
            "/history/{context}",
            get(
                |State(app): State<App>, Path(context): Path<String>| async move {
                    Json(app.executor.history(None, &context).await)
                },
            ),
        )
        .route(
            "/release",
            post(|State(app): State<App>| async move {
                app.record.memory.release.store(true, Ordering::SeqCst);
                "released"
            }),
        )
        .route(
            "/egress",
            get(|| async {
                let url = std::env::var("EGRESS_URL")
                    .unwrap_or_else(|_| "http://fixture-sink.rig-local.svc:8080/probe".into());
                probe_egress(url).await
            }),
        )
        .route(
            "/egress/wrong-host",
            get(|| probe_egress("http://ollama.rig-local.svc:15024/probe".into())),
        )
        .route(
            "/egress/wrong-port",
            get(|| probe_egress("http://ollama.rig-local.svc.cluster.local:15027/probe".into())),
        )
        .route("/dns/{transport}", get(probe_dns))
        .route(
            "/egress/link-local",
            get(|| {
                probe_egress("http://link-local.rig-local.svc.cluster.local:15024/probe".into())
            }),
        )
        .with_state(app)
        .merge(agent_card::agent_card_router(Arc::new(
            StaticAgentCard::new(card),
        )))
        .nest(
            "/jsonrpc",
            jsonrpc_router(handler).layer(from_fn(reject_tenant)),
        );
    let bind = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:80".into());
    axum::serve(tokio::net::TcpListener::bind(bind).await?, router).await?;
    Ok(())
}
