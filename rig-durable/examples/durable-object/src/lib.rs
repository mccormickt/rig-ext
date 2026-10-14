use rig::{
    completion::{CompletionRequest, Message},
    driver::{Exchange, Model, Opened, Opening, Transport},
    message::UserContent,
    tool::{Tool, ToolContext},
};
use rig_celld::CellStorage;
use rig_core::test_utils::{MockFrame, MockScript, MockStreamEvent};
use rig_durable::{
    ApprovalDecision, SubmitInput, ToolInvocation, ToolOptions, ToolPolicy,
    durable_object::{Builder, Engine},
};
use serde::{Deserialize, Serialize};
use worker::*;

#[cfg(all(feature = "conformance", target_family = "wasm"))]
mod conformance;
#[cfg(all(feature = "conformance", target_family = "wasm"))]
pub use conformance::{ChildSession, ScriptSession};

#[cfg(all(feature = "conformance", target_family = "wasm"))]
fn worker_planner(env: &Env) -> Result<rig_codemode::worker_replay::WorkerReplayPlanner> {
    use rig_codemode::worker_replay::{ReplayBudget, WorkerReplayPlanner};
    WorkerReplayPlanner::from_env(env, "LOADER", ReplayBudget::default())
}

/// Deterministic model: ask for an addition, then answer after its result.
/// It has no process-local script cursor, so recovery can reissue any request.
#[derive(Clone)]
struct Calculator;

impl Transport<MockScript> for Calculator {
    fn send(&self, request: CompletionRequest, _exchange: Exchange) -> Opening<MockFrame> {
        let has_result = matches!(request.chat_history.last(), Some(Message::User { content })
            if content.iter().any(|part| matches!(part, UserContent::ToolResult(_))));
        let event = if has_result {
            MockStreamEvent::text("The tool round is complete.")
        } else {
            MockStreamEvent::tool_call(
                "addition",
                "add",
                serde_json::json!({"left": 20, "right": 22}),
            )
        };
        Opening::ready(Opened::new(futures::stream::iter([
            Ok(MockFrame::Event(event)),
            Ok(MockFrame::Event(
                MockStreamEvent::final_response_with_default_usage(),
            )),
        ])))
    }
}

struct Add {
    #[cfg(target_family = "wasm")]
    storage: CellStorage,
}

#[derive(Deserialize, Serialize)]
struct ObservedEffect {
    logical_id: String,
    left_value: i64,
    right_value: i64,
    result: String,
}

#[derive(Deserialize)]
struct AddArgs {
    left: i64,
    right: i64,
}

impl Tool for Add {
    const NAME: &'static str = "add";
    type Args = AddArgs;
    type Output = i128;
    type Error = worker::Error;
    fn description(&self) -> String {
        "Add two integers".into()
    }
    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({"type":"object","properties":{"left":{"type":"integer"},"right":{"type":"integer"}},"required":["left","right"]})
    }
    async fn call(
        &self,
        context: &mut ToolContext,
        args: AddArgs,
    ) -> std::result::Result<i128, Self::Error> {
        let invocation = context.require::<ToolInvocation>().map_err(worker_error)?;
        let key = invocation
            .logical_key
            .ok_or_else(|| Error::RustError("logical tool identity is required".into()))?;
        let result = i128::from(args.left) + i128::from(args.right);
        #[cfg(target_family = "wasm")]
        self.storage.sql().exec(
            "INSERT INTO example_effects (logical_id, left_value, right_value, result) VALUES (?, ?, ?, ?)",
            vec![key.canonical().into(), args.left.into(), args.right.into(), result.to_string().into()],
        )?;
        #[cfg(not(target_family = "wasm"))]
        let _ = key;
        Ok(result)
    }
}

#[durable_object(alarm)]
pub struct AgentSession {
    storage: CellStorage,
    engine: std::cell::OnceCell<Engine<CellStorage, CellStorage>>,
}

impl AgentSession {
    fn engine(&self) -> Result<&Engine<CellStorage, CellStorage>> {
        if self.engine.get().is_none() {
            self.storage.sql().exec(
                "CREATE TABLE IF NOT EXISTS example_effects (sequence INTEGER PRIMARY KEY, logical_id TEXT NOT NULL, left_value INTEGER NOT NULL, right_value INTEGER NOT NULL, result TEXT NOT NULL)",
                None,
            )?;
            let engine = Builder::new(Model::new(MockScript::default(), Calculator))
                .tool_with(
                    Add {
                        #[cfg(target_family = "wasm")]
                        storage: self.storage.clone(),
                    },
                    ToolOptions::default()
                        .policy(ToolPolicy::read_only())
                        .require_approval(),
                )
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
            .ok_or_else(|| Error::RustError("engine is not initialized".into()))
    }
}

impl DurableObject for AgentSession {
    fn new(state: State, _env: Env) -> Self {
        Self {
            storage: state.into(),
            engine: std::cell::OnceCell::new(),
        }
    }

    async fn fetch(&self, mut request: Request) -> Result<Response> {
        let engine = self.engine()?;
        let path = request.path();
        let operation = path.rsplit('/').next().unwrap_or_default();
        match (request.method(), operation) {
            (Method::Post, "submit") => {
                match engine.submit(request.json::<SubmitInput>().await?).await {
                    Ok(receipt) => Response::from_json(&receipt),
                    Err(rig_durable::durable_object::Error::Submission(error)) => {
                        Response::error(error.to_string(), 409)
                    }
                    Err(error) => Err(worker_error(error)),
                }
            }
            (Method::Post, "approval") => {
                match request.json::<ApprovalDecision>().await? {
                    ApprovalDecision::Approve { approval_id } => engine.approve(approval_id).await,
                    ApprovalDecision::Deny {
                        approval_id,
                        reason,
                    } => engine.deny(approval_id, reason).await,
                }
                .map_err(worker_error)?;
                Response::ok("decision committed")
            }
            (Method::Get, "status") => Response::from_json(&engine.status().map_err(worker_error)?),
            (Method::Get, "effects") => Response::from_json(
                &self.storage.sql().exec(
                    "SELECT logical_id, left_value, right_value, result FROM example_effects ORDER BY sequence",
                    None,
                )?.to_array::<ObservedEffect>()?,
            ),
            (Method::Get, "transcript") => {
                Response::from_json(&engine.transcript().map_err(worker_error)?)
            }
            (Method::Get, "result") => {
                let id = request
                    .url()?
                    .query_pairs()
                    .find(|(key, _)| key == "request_id")
                    .map(|(_, value)| value.into_owned())
                    .ok_or_else(|| Error::RustError("request_id is required".into()))?;
                Response::from_json(&engine.wait(&id).await.map_err(worker_error)?)
            }
            (Method::Post, "close") => {
                engine.close().map_err(worker_error)?;
                Response::ok("closed")
            }
            _ => Response::error("unknown operation", 404),
        }
    }

    async fn alarm(&self) -> Result<Response> {
        self.engine()?.alarm().await.map_err(worker_error)?;
        Response::ok("alarm handled")
    }
}

fn worker_error(error: impl std::fmt::Display) -> Error {
    Error::RustError(error.to_string())
}

#[event(fetch)]
pub async fn fetch(request: Request, env: Env, _ctx: Context) -> Result<Response> {
    let path = request.path();
    #[cfg(all(feature = "conformance", target_family = "wasm"))]
    if path.starts_with("/fixture/") {
        let session = path
            .split('/')
            .nth(2)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| worker_error("use /fixture/<session>/<operation>"))?;
        return env
            .durable_object("SCRIPTS")?
            .id_from_name(session)?
            .get_stub()?
            .fetch_with_request(request)
            .await;
    }
    let session = path
        .split('/')
        .nth(1)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::RustError("use /<session>/<operation>".into()))?;
    env.durable_object("AGENTS")?
        .id_from_name(session)?
        .get_stub()?
        .fetch_with_request(request)
        .await
}
