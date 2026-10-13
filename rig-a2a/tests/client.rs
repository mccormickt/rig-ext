//! Loopback integration tests driving [`rig_a2a::A2AClient`] over real HTTP on
//! 127.0.0.1 against a stub A2A server built from the upstream `a2a-server`
//! crate (`DefaultRequestHandler` + a scripted executor).
//!
//! Validates well-known card discovery, transport negotiation, A2A completion
//! and streaming, host-side conversation threading, Rig sub-agent composition,
//! and full roundtrips over JSON-RPC and HTTP+JSON.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use a2a::{
    A2AError as A2AProtocolError, AgentCard, AgentInterface, Artifact, Message, Part, Role,
    StreamResponse, TRANSPORT_PROTOCOL_HTTP_JSON, TRANSPORT_PROTOCOL_JSONRPC, Task, TaskState,
    TaskStatus, new_artifact_id,
};
use a2a_server::task_store::InMemoryTaskStore;
use a2a_server::{
    AgentExecutor, DefaultRequestHandler, ExecutorContext, StaticAgentCard, agent_card,
    jsonrpc::jsonrpc_router, rest::rest_router,
};
use futures::StreamExt;
use futures::stream::BoxStream;
use rig_a2a::{A2AClient, A2AConversationExt, SendMessageResponse};
use rig_agent::agent::AgentBuilder;
use rig_agent::test_utils::{MockCompletionModel, MockTurn};
use rig_core::completion::{CompletionRequest, ToolDefinition};
use rig_core::message::{Message as RigMessage, ToolResultContent, UserContent};

/// What the stub executor replies to a `message/send`.
#[derive(Clone)]
enum StubReply {
    CompletedText(String),
    InputRequired {
        prompt: String,
    },
    /// A task ending in a non-completed terminal (or auth-gated) state.
    Ended {
        state: TaskState,
        reason: String,
    },
    /// A stream of status updates, each carrying a state and a status message.
    /// Many A2A agents deliver their answer this way rather than as artifacts.
    StatusStream(Vec<(TaskState, String)>),
    /// A task that pauses for input and *then* emits an artifact, so the last
    /// event on the stream reports no task state.
    PauseThenArtifact {
        prompt: String,
        artifact: String,
    },
}

impl StubReply {
    fn failed(reason: &str) -> Self {
        Self::Ended {
            state: TaskState::Failed,
            reason: reason.to_string(),
        }
    }
}

/// Inbound messages recorded by the stub executor, shared with the test body.
struct RecordedRequest {
    message: Message,
    tenant: Option<String>,
}

type Recorded = Arc<Mutex<Vec<RecordedRequest>>>;

#[derive(Clone, Copy)]
enum StubBinding {
    JsonRpc,
    Rest,
}

/// A scripted sequence of replies. Each `message/send` pops the next one; the
/// last reply repeats once the script is exhausted, so tests only script the
/// turns they care about.
struct StubExecutor {
    replies: Mutex<VecDeque<StubReply>>,
    recorded: Recorded,
}

impl StubExecutor {
    fn next_reply(&self) -> StubReply {
        let mut replies = self.replies.lock().unwrap();
        if replies.len() > 1 {
            replies.pop_front().expect("non-empty script")
        } else {
            replies
                .front()
                .cloned()
                .expect("stub script must not be empty")
        }
    }
}

impl AgentExecutor for StubExecutor {
    fn execute(
        &self,
        ctx: ExecutorContext,
    ) -> BoxStream<'static, Result<StreamResponse, A2AProtocolError>> {
        if let Some(message) = &ctx.message {
            self.recorded.lock().unwrap().push(RecordedRequest {
                message: message.clone(),
                tenant: ctx.tenant.clone(),
            });
        }
        let (state, status_text, artifact_text) = match self.next_reply() {
            StubReply::CompletedText(text) => (TaskState::Completed, None, Some(text)),
            StubReply::InputRequired { prompt } => (TaskState::InputRequired, Some(prompt), None),
            StubReply::Ended { state, reason } => (state, Some(reason), None),
            StubReply::StatusStream(steps) => {
                let events = steps
                    .into_iter()
                    .map(|(state, text)| {
                        Ok(StreamResponse::StatusUpdate(status_update(
                            &ctx.task_id,
                            &ctx.context_id,
                            state,
                            Some(&text),
                        )))
                    })
                    .collect::<Vec<_>>();
                return futures::stream::iter(events).boxed();
            }
            StubReply::PauseThenArtifact { prompt, artifact } => {
                let events = vec![
                    Ok(StreamResponse::StatusUpdate(status_update(
                        &ctx.task_id,
                        &ctx.context_id,
                        TaskState::InputRequired,
                        Some(&prompt),
                    ))),
                    Ok(StreamResponse::ArtifactUpdate(
                        a2a::TaskArtifactUpdateEvent {
                            task_id: ctx.task_id.clone(),
                            context_id: ctx.context_id.clone(),
                            artifact: Artifact {
                                artifact_id: new_artifact_id(),
                                name: None,
                                description: None,
                                parts: vec![Part::text(artifact)],
                                metadata: None,
                                extensions: None,
                            },
                            append: None,
                            last_chunk: Some(true),
                            metadata: None,
                        },
                    )),
                ];
                return futures::stream::iter(events).boxed();
            }
        };
        let task = Task {
            id: ctx.task_id,
            context_id: ctx.context_id,
            status: TaskStatus {
                state,
                message: status_text.map(|text| Message::new(Role::Agent, vec![Part::text(text)])),
                timestamp: None,
            },
            artifacts: artifact_text.map(|text| {
                vec![Artifact {
                    artifact_id: new_artifact_id(),
                    name: Some("reply".to_string()),
                    description: None,
                    parts: vec![Part::text(text)],
                    metadata: None,
                    extensions: None,
                }]
            }),
            history: None,
            metadata: None,
        };
        futures::stream::once(async move { Ok(StreamResponse::Task(task)) }).boxed()
    }

    fn cancel(
        &self,
        ctx: ExecutorContext,
    ) -> BoxStream<'static, Result<StreamResponse, A2AProtocolError>> {
        let task_id = ctx.task_id;
        futures::stream::once(async move { Err(A2AProtocolError::task_not_cancelable(&task_id)) })
            .boxed()
    }
}

fn status_update(
    task_id: &str,
    context_id: &str,
    state: TaskState,
    text: Option<&str>,
) -> a2a::TaskStatusUpdateEvent {
    a2a::TaskStatusUpdateEvent {
        task_id: task_id.to_string(),
        context_id: context_id.to_string(),
        status: TaskStatus {
            state,
            message: text.map(|text| Message::new(Role::Agent, vec![Part::text(text.to_string())])),
            timestamp: None,
        },
        metadata: None,
    }
}

fn card_with_skills(base_url: &str, name: &str, skill_ids: &[&str]) -> AgentCard {
    AgentCard {
        name: name.to_string(),
        description: format!("Stub agent {name}."),
        version: "0.1.0".to_string(),
        supported_interfaces: vec![AgentInterface::new(
            format!("{base_url}/jsonrpc"),
            TRANSPORT_PROTOCOL_JSONRPC,
        )],
        capabilities: a2a::AgentCapabilities {
            streaming: Some(false),
            push_notifications: Some(false),
            extensions: None,
            extended_agent_card: Some(false),
        },
        default_input_modes: vec!["text/plain".to_string()],
        default_output_modes: vec!["text/plain".to_string()],
        skills: skill_ids
            .iter()
            .map(|id| a2a::AgentSkill {
                id: (*id).to_string(),
                name: (*id).to_string(),
                description: format!("{id} skill."),
                tags: vec![],
                examples: None,
                input_modes: Some(vec!["text/plain".to_string()]),
                output_modes: Some(vec!["text/plain".to_string()]),
                security_requirements: None,
            })
            .collect(),
        provider: None,
        documentation_url: None,
        icon_url: None,
        security_schemes: None,
        security_requirements: None,
        signatures: None,
    }
}

/// Boot a stub A2A server on a random loopback port: well-known card route
/// plus the upstream JSON-RPC handler backed by [`StubExecutor`].
async fn serve_stub(
    name: &str,
    skill_ids: &[&str],
    reply: StubReply,
) -> (SocketAddr, Recorded, tokio::task::JoinHandle<()>) {
    serve_script(name, skill_ids, [reply], StubBinding::JsonRpc).await
}

async fn serve_stub_with_binding(
    name: &str,
    skill_ids: &[&str],
    reply: StubReply,
    binding: StubBinding,
) -> (SocketAddr, Recorded, tokio::task::JoinHandle<()>) {
    serve_script(name, skill_ids, [reply], binding).await
}

/// Boot a stub that answers successive requests with successive replies.
async fn serve_script(
    name: &str,
    skill_ids: &[&str],
    replies: impl IntoIterator<Item = StubReply>,
    binding: StubBinding,
) -> (SocketAddr, Recorded, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let base_url = format!("http://{addr}");
    let mut card = card_with_skills(&base_url, name, skill_ids);
    card.supported_interfaces = vec![match binding {
        StubBinding::JsonRpc => {
            AgentInterface::new(format!("{base_url}/jsonrpc"), TRANSPORT_PROTOCOL_JSONRPC)
        }
        StubBinding::Rest => AgentInterface::new(&base_url, TRANSPORT_PROTOCOL_HTTP_JSON),
    }];
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let replies: VecDeque<StubReply> = replies.into_iter().collect();
    assert!(!replies.is_empty(), "stub script must not be empty");
    let executor = StubExecutor {
        replies: Mutex::new(replies),
        recorded: recorded.clone(),
    };
    let handler = Arc::new(DefaultRequestHandler::new(
        executor,
        InMemoryTaskStore::new(),
    ));
    let protocol_router = match binding {
        StubBinding::JsonRpc => axum::Router::new().nest("/jsonrpc", jsonrpc_router(handler)),
        StubBinding::Rest => rest_router(handler),
    };
    let router = axum::Router::new()
        .merge(agent_card::agent_card_router(Arc::new(
            StaticAgentCard::new(card),
        )))
        .merge(protocol_router);

    let server_task = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("stub server");
    });
    (addr, recorded, server_task)
}

#[tokio::test]
async fn from_url_builds_an_agent_named_and_described_from_the_card() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, _recorded, server) = serve_stub(
        "multi-skill",
        &["alpha", "beta"],
        StubReply::CompletedText("pong".into()),
    )
    .await;

    let client = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch agent card");
    assert_eq!(client.card().name, "multi-skill");
    assert_eq!(client.agent_name(), "multi-skill");

    let agent = client.agent().build();
    assert_eq!(agent.name(), Some("multi-skill"));
    let description = agent.description().expect("agent description");
    assert!(description.contains("alpha"), "{description}");
    assert!(description.contains("beta"), "{description}");

    server.abort();
}

#[tokio::test]
async fn explicit_agent_name_overrides_the_card() {
    let card = card_with_skills("http://127.0.0.1:1", "Rig Agent", &["chat"]);
    let client = A2AClient::builder()
        .card(card)
        .agent_name("billing-desk")
        .build()
        .await
        .expect("client from hand-supplied card");

    assert_eq!(client.agent_name(), "billing-desk");
    assert_eq!(client.agent().build().name(), Some("billing-desk"));
}

#[tokio::test]
async fn from_url_uses_rest_interface() {
    let (addr, recorded, server) = serve_stub_with_binding(
        "rest-agent",
        &["chat"],
        StubReply::CompletedText("rest reply".into()),
        StubBinding::Rest,
    )
    .await;

    let client = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should select REST");
    assert_eq!(
        client.interface().protocol_binding,
        TRANSPORT_PROTOCOL_HTTP_JSON
    );
    let outcome = client
        .message("hello")
        .send()
        .await
        .expect("REST request should succeed");
    assert!(
        matches!(outcome, SendMessageResponse::Task(task) if task.status.state == TaskState::Completed)
    );
    assert_eq!(recorded.lock().unwrap().len(), 1);

    server.abort();
}

#[tokio::test]
async fn card_without_skills_still_yields_an_agent() {
    let card = card_with_skills("http://127.0.0.1:1", "Bare Agent", &[]);
    let client = A2AClient::from_agent_card(card)
        .await
        .expect("client from hand-supplied card");

    let agent = client.agent().build();
    assert_eq!(agent.name(), Some("bare-agent"));
    let description = agent.description().expect("agent description");
    assert!(
        description.contains("Stub agent Bare Agent."),
        "{description}"
    );
    assert!(
        !description.contains("Skills:"),
        "a card with no skills must not render an empty section: {description}"
    );
}

/// All tool-result texts the scripted model received across its turns.
fn tool_result_texts(model: &MockCompletionModel) -> Vec<String> {
    model
        .requests()
        .iter()
        .flat_map(|request| request.chat_history.iter().cloned())
        .filter_map(|message| match message {
            RigMessage::User { content } => Some(content),
            _ => None,
        })
        .flat_map(|content| content.into_iter())
        .filter_map(|content| match content {
            UserContent::ToolResult(result) => Some(result.content),
            _ => None,
        })
        .flat_map(|content| content.into_iter())
        .filter_map(|content| match content {
            ToolResultContent::Text(text) => Some(text.text),
            _ => None,
        })
        .collect()
}

/// Full roundtrip through Rig's standard sub-agent tool conversion.
async fn agent_calls_remote_agent(binding: StubBinding) {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) = serve_script(
        "greeter",
        &["greet"],
        [StubReply::CompletedText("hello from remote".into())],
        binding,
    )
    .await;

    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");
    let agent_name = remote.agent_name().to_string();
    let remote_tool = remote.agent().build().into_tool().expect("named agent");

    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "tool_call_1",
            &agent_name,
            serde_json::json!({"prompt": "greet me"}),
        ),
        MockTurn::text("done"),
    ]);
    let agent = AgentBuilder::new(model.clone())
        .name("local")
        .dynamic_tool(remote_tool)
        .build();

    let out = agent
        .prompt("use the remote agent")
        .max_turns(3)
        .await
        .expect("agent run should succeed");
    assert_eq!(out.to_string(), "done");

    let messages = recorded.lock().unwrap();
    assert_eq!(messages.len(), 1, "stub should have seen one message");
    assert!(matches!(
        &messages[0].message.parts[0].content,
        a2a::PartContent::Text(t) if t == "greet me"
    ));
    // The unbound sub-agent is stateless.
    assert_eq!(messages[0].message.context_id, None);
    drop(messages);

    // The remote reply reaches the model verbatim — no state marker on a
    // completed task.
    let results = tool_result_texts(&model);
    assert!(
        results.iter().any(|text| text == "hello from remote"),
        "model should see the remote reply as a tool result: {results:?}"
    );

    server.abort();
}

#[tokio::test]
async fn sub_agent_tool_calls_remote_agent_end_to_end() {
    agent_calls_remote_agent(StubBinding::JsonRpc).await;
}

#[tokio::test]
async fn sub_agent_tool_calls_remote_agent_over_rest() {
    agent_calls_remote_agent(StubBinding::Rest).await;
}

#[tokio::test]
async fn request_context_threads_to_remote_and_response() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) =
        serve_stub("threaded", &["chat"], StubReply::CompletedText("ok".into())).await;

    let client = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    // Turn 1 sends no ids; the server mints the contextId. Turn 2 echoes it,
    // per the A2A spec's client threading model. The direct API stays explicit.
    let task = match client
        .message("turn 1")
        .send()
        .await
        .expect("send should succeed")
    {
        SendMessageResponse::Task(task) => task,
        other => panic!("expected Task outcome, got {other:?}"),
    };
    assert_eq!(task.status.state, TaskState::Completed);
    let minted = task.context_id.clone();
    assert!(!minted.trim().is_empty(), "server must mint a contextId");

    let task = match client
        .message("turn 2")
        .context(&minted)
        .send()
        .await
        .expect("send should succeed")
    {
        SendMessageResponse::Task(task) => task,
        other => panic!("expected Task outcome, got {other:?}"),
    };
    assert_eq!(task.context_id, minted);

    let messages = recorded.lock().unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].message.context_id, None);
    assert_eq!(
        messages[1].message.context_id.as_deref(),
        Some(minted.as_str())
    );

    server.abort();
}

// ---------------------------------------------------------------------------
// A2AModel: the remote agent as a Rig completion model
// ---------------------------------------------------------------------------

fn text_request(prompt: &str) -> CompletionRequest {
    CompletionRequest::new(prompt)
}

/// Every text part of a recorded inbound message, in order.
fn recorded_texts(recorded: &Recorded, turn: usize) -> Vec<String> {
    recorded.lock().unwrap()[turn]
        .message
        .parts
        .iter()
        .filter_map(|part| match &part.content {
            a2a::PartContent::Text(text) => Some(text.clone()),
            _ => None,
        })
        .collect()
}

fn document(id: &str, text: &str) -> rig_core::completion::Document {
    rig_core::completion::Document {
        id: id.to_string(),
        text: text.to_string(),
        additional_props: Default::default(),
    }
}

/// A threaded remote keeps everything sent under its `contextId`, so standing
/// context goes out once — but a document retrieved for a later turn is new to
/// that remote and must still be delivered.
#[tokio::test]
async fn threaded_turns_send_standing_context_once() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) =
        serve_stub("standing", &["chat"], StubReply::CompletedText("ok".into())).await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");
    let model = remote.model_for_conversation("user-42");

    let mut turn = text_request("turn 1");
    turn.chat_history
        .insert(0, rig_core::message::Message::system("You are terse."));
    turn.documents = vec![document("handbook", "the standing handbook")];
    model.completion(turn).await.expect("turn 1 should succeed");

    // Turn 2 repeats the same preamble and document, as Rig rebuilds them for
    // every request, and additionally retrieves one new document.
    let mut turn = text_request("turn 2");
    turn.chat_history.insert(0, RigMessage::user("turn 1"));
    turn.chat_history
        .insert(0, rig_core::message::Message::system("You are terse."));
    turn.documents = vec![
        document("handbook", "the standing handbook"),
        document("retrieved", "freshly retrieved passage"),
    ];
    model.completion(turn).await.expect("turn 2 should succeed");

    let first = recorded_texts(&recorded, 0);
    assert!(
        first.iter().any(|text| text.contains("You are terse."))
            && first.iter().any(|text| text.contains("standing handbook")),
        "the opening turn carries the whole request: {first:?}"
    );

    let second = recorded_texts(&recorded, 1);
    assert!(
        !second.iter().any(|text| text.contains("You are terse.")),
        "the remote already holds the preamble under this contextId: {second:?}"
    );
    assert!(
        !second.iter().any(|text| text.contains("standing handbook")),
        "a document the remote already holds must not be replayed: {second:?}"
    );
    assert!(
        second
            .iter()
            .any(|text| text.contains("freshly retrieved passage")),
        "a newly retrieved document is new to the remote: {second:?}"
    );
    assert!(
        second.iter().any(|text| text.contains("turn 2")),
        "the newest prompt always goes out: {second:?}"
    );
    assert!(!second.iter().any(|text| text.contains("turn 1")));

    server.abort();
}

#[tokio::test]
async fn threaded_requests_keep_only_the_original_newest_multipart_message() {
    use rig_core::message::{AssistantMessage, Message as RigMessage, StopReason, UserContent};

    let (addr, recorded, server) = serve_stub(
        "boundaries",
        &["chat"],
        StubReply::CompletedText("ok".into()),
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}")).await.unwrap();
    let params = serde_json::json!({"a2a": {"context_id": "ctx-restored"}});
    let newest = RigMessage::User {
        content: vec![
            UserContent::text("new part one"),
            UserContent::text("new part two"),
        ],
    };
    for gap in [
        None,
        Some(RigMessage::assistant("")),
        Some(RigMessage::Assistant(
            AssistantMessage::new(vec![rig_core::message::AssistantContent::text(
                "failed answer",
            )])
            .with_stop(StopReason::Error("failed".into())),
        )),
    ] {
        let mut history = vec![RigMessage::user("old prompt")];
        history.extend(gap);
        for agent_backed in [false, true] {
            for streaming in [false, true] {
                if agent_backed {
                    let agent = remote.agent().additional_params(params.clone()).build();
                    let run = agent.prompt(newest.clone()).history(history.clone());
                    if streaming {
                        let mut stream = run.stream();
                        while let Some(item) = stream.next().await {
                            item.unwrap();
                        }
                    } else {
                        assert_eq!(run.await.unwrap().to_string(), "ok");
                    }
                } else {
                    let mut request = CompletionRequest::new(newest.clone());
                    request.chat_history = history.clone();
                    request.chat_history.push(newest.clone());
                    request.additional_params = Some(params.clone());
                    if streaming {
                        let mut stream = remote.model().stream(request).unwrap();
                        while let Some(item) = stream.next().await {
                            item.unwrap();
                        }
                    } else {
                        assert_eq!(
                            remote.model().completion(request).await.unwrap().text(),
                            "ok"
                        );
                    }
                }
                let turn = recorded.lock().unwrap().len() - 1;
                assert_eq!(
                    recorded_texts(&recorded, turn),
                    ["new part one\nnew part two"]
                );
            }
        }
    }
    assert_eq!(recorded.lock().unwrap().len(), 12);
    server.abort();
}

/// Without a conversation there is no server-side state to rely on, so every
/// request carries its own context.
#[tokio::test]
async fn unthreaded_requests_always_carry_their_context() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) = serve_stub(
        "stateless",
        &["chat"],
        StubReply::CompletedText("ok".into()),
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");
    let model = remote.model();

    for prompt in ["one", "two"] {
        let mut turn = text_request(prompt);
        turn.documents = vec![document("handbook", "the standing handbook")];
        model.completion(turn).await.expect("completion succeeds");
    }

    for turn in 0..2 {
        let texts = recorded_texts(&recorded, turn);
        assert!(
            texts.iter().any(|text| text.contains("standing handbook")),
            "turn {turn} must carry its own context: {texts:?}"
        );
    }

    server.abort();
}

#[tokio::test]
async fn explicit_context_does_not_read_or_replace_a_bound_conversation() {
    let (addr, recorded, server) = serve_stub(
        "explicit-bound",
        &["chat"],
        StubReply::CompletedText("ok".into()),
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");
    let model = remote.model_for_conversation("bound");
    for (index, context) in [None, Some("ctx-restored"), None].into_iter().enumerate() {
        let mut request = text_request(&format!("turn {index}"));
        request
            .chat_history
            .insert(0, RigMessage::system("You are terse."));
        request.documents = vec![document("handbook", "the standing handbook")];
        if let Some(context) = context {
            request.additional_params = Some(serde_json::json!({
                "a2a": { "context_id": context }
            }));
        }
        model
            .completion(request)
            .await
            .expect("completion should succeed");
    }
    let explicit = recorded_texts(&recorded, 1);
    assert!(explicit.iter().any(|text| text.contains("You are terse.")));
    assert!(
        explicit
            .iter()
            .any(|text| text.contains("standing handbook"))
    );
    assert_eq!(recorded_texts(&recorded, 2), ["turn 2"]);
    let messages = recorded.lock().unwrap();
    assert_eq!(
        messages[1].message.context_id.as_deref(),
        Some("ctx-restored")
    );
    assert!(messages[2].message.context_id.is_some());
    assert_ne!(
        messages[2].message.context_id.as_deref(),
        Some("ctx-restored")
    );
    server.abort();
}

/// A caller who owns the ids threads them per request, with no conversation
/// binding and nothing tracked on the client.
#[tokio::test]
async fn a_caller_can_thread_explicitly_through_additional_params() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) =
        serve_stub("explicit", &["chat"], StubReply::CompletedText("ok".into())).await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");
    // No conversation binding: this model tracks nothing.
    let model = remote.model();

    model
        .completion(text_request("turn 1"))
        .await
        .expect("turn 1 should succeed");
    let minted = recorded.lock().unwrap()[0]
        .message
        .context_id
        .clone()
        .or_else(|| Some("ctx-from-response".to_string()));
    assert!(minted.is_some());

    // Thread turn 2 by hand, the way a caller restoring ids from their own
    // storage would.
    let mut turn = text_request("turn 2");
    turn.additional_params = Some(serde_json::json!({
        "a2a": { "context_id": "ctx-restored" }
    }));
    model.completion(turn).await.expect("turn 2 should succeed");

    let messages = recorded.lock().unwrap();
    assert_eq!(
        messages[1].message.context_id.as_deref(),
        Some("ctx-restored"),
        "the caller's contextId must reach the remote"
    );

    server.abort();
}

#[tokio::test]
async fn a_malformed_threading_directive_fails_before_any_network_call() {
    let card = card_with_skills("http://127.0.0.1:1", "unreachable", &["chat"]);
    let client = A2AClient::from_agent_card(card)
        .await
        .expect("client from hand-supplied card");

    let mut turn = text_request("hi");
    turn.additional_params = Some(serde_json::json!({"a2a": {"contextId": "typo"}}));
    let error = client
        .model()
        .completion(turn)
        .await
        .expect_err("a typo must not silently open a new conversation");
    assert!(
        error.to_string().contains("additional_params.a2a"),
        "{error}"
    );
}

/// A completed task whose result was an action rather than text is a
/// legitimate empty turn, the way Rig's Anthropic provider treats an empty
/// `end_turn`.
#[tokio::test]
async fn a_completed_task_with_no_output_is_an_empty_turn() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, _recorded, server) =
        serve_stub("silent", &["chat"], StubReply::CompletedText(String::new())).await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    let response = remote
        .model()
        .completion(text_request("file the ticket"))
        .await
        .expect("a silent completion is not a failure");
    assert_eq!(response.text(), "");

    server.abort();
}

/// An unfinished task that answers with nothing has neither produced a result
/// nor completed, so there is nothing for the caller to act on.
#[tokio::test]
async fn an_unfinished_task_with_no_output_fails_the_completion() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, _recorded, server) = serve_stub(
        "mute",
        &["chat"],
        StubReply::InputRequired {
            prompt: String::new(),
        },
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    let error = remote
        .model()
        .completion(text_request("what next?"))
        .await
        .expect_err("a content-free unfinished task is not an answer");
    let text = error.to_string();
    assert!(text.contains("no content"), "{text}");
    assert!(text.contains("input-required"), "{text}");

    server.abort();
}

#[tokio::test]
async fn model_completes_against_the_remote_agent() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) = serve_stub(
        "modelled",
        &["chat"],
        StubReply::CompletedText("remote answer".into()),
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    let response = remote
        .model()
        .completion(text_request("what is 2 + 2?"))
        .await
        .expect("completion should succeed");

    assert_eq!(response.provider(), "a2a");
    let text = match response.choice.first() {
        Some(rig_core::completion::AssistantContent::Text(text)) => text.text.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert_eq!(text, "remote answer");
    // A2A has no token accounting; zero usage is Rig's documented sentinel.
    assert_eq!(response.usage, rig_core::completion::Usage::default());

    let messages = recorded.lock().unwrap();
    assert_eq!(messages.len(), 1);
    assert!(matches!(
        &messages[0].message.parts[0].content,
        a2a::PartContent::Text(t) if t == "what is 2 + 2?"
    ));

    server.abort();
}

#[tokio::test]
async fn model_rejects_requests_a2a_cannot_carry() {
    let card = card_with_skills("http://127.0.0.1:1", "unreachable", &["chat"]);
    let client = A2AClient::from_agent_card(card)
        .await
        .expect("client from hand-supplied card");
    let model = client.model();

    let mut with_tools = text_request("hi");
    with_tools.tools.push(ToolDefinition {
        name: rig_core::message::ToolName::new("add").expect("non-empty tool name"),
        description: "adds".to_string(),
        parameters: serde_json::json!({}),
    });
    let error = model
        .completion(with_tools)
        .await
        .expect_err("a remote A2A agent can never call a Rig tool");
    assert!(error.to_string().contains("tools"), "{error}");

    let mut with_schema = text_request("hi");
    with_schema.output_schema = Some(schemars::json_schema!({"type": "object"}));
    let error = model
        .completion(with_schema)
        .await
        .expect_err("A2A cannot constrain remote output");
    assert!(error.to_string().contains("output_schema"), "{error}");
}

#[tokio::test]
async fn agent_backed_by_a2a_threads_a_conversation() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) = serve_script(
        "modelled",
        &["chat"],
        [
            StubReply::CompletedText("first".into()),
            StubReply::CompletedText("second".into()),
        ],
        StubBinding::JsonRpc,
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    let agent = remote.agent_for_conversation("user-42").build();
    assert_eq!(agent.name(), Some("modelled"));

    assert_eq!(
        agent.prompt("turn 1").await.expect("turn 1").to_string(),
        "first"
    );
    assert_eq!(
        agent
            .prompt("turn 2")
            .history([RigMessage::user("turn 1")])
            .await
            .expect("turn 2")
            .to_string(),
        "second"
    );

    assert_eq!(recorded_texts(&recorded, 1), ["turn 2"]);
    let messages = recorded.lock().unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].message.context_id, None);
    assert!(
        messages[1].message.context_id.is_some(),
        "a conversation-bound agent must continue its remote context"
    );

    server.abort();
}

/// One agent, many conversations: each run names both the Rig memory
/// conversation and its remote A2A thread.
#[tokio::test]
async fn one_agent_threads_each_conversation_it_is_asked_for() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) =
        serve_stub("multi", &["chat"], StubReply::CompletedText("ok".into())).await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    let agent = remote.agent().build();

    agent
        .prompt("alice turn 1")
        .a2a_conversation("alice")
        .await
        .expect("alice turn 1");
    agent
        .prompt("bob turn 1")
        .a2a_conversation("bob")
        .await
        .expect("bob turn 1");
    agent
        .prompt("alice turn 2")
        .history([RigMessage::user("alice turn 1")])
        .a2a_conversation("alice")
        .await
        .expect("alice turn 2");

    assert_eq!(recorded_texts(&recorded, 2), ["alice turn 2"]);
    let messages = recorded.lock().unwrap();
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0].message.context_id, None, "alice opens a thread");
    assert_eq!(
        messages[1].message.context_id, None,
        "bob opens a separate thread rather than joining alice's"
    );
    let alice = messages[2]
        .message
        .context_id
        .as_deref()
        .expect("alice's second turn continues her thread");
    assert!(!alice.is_empty());

    server.abort();
}

/// A run that names no conversation stays single-turn: the hook must not
/// invent a key that would merge unrelated exchanges into one remote thread.
#[tokio::test]
async fn an_unnamed_run_does_not_join_a_thread() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) =
        serve_stub("anon", &["chat"], StubReply::CompletedText("ok".into())).await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    let agent = remote.agent().build();
    agent.prompt("one").await.expect("turn 1");
    agent.prompt("two").await.expect("turn 2");

    let messages = recorded.lock().unwrap();
    assert_eq!(messages.len(), 2);
    assert!(
        messages.iter().all(|m| m.message.context_id.is_none()),
        "an unnamed run must not be threaded"
    );

    server.abort();
}

#[tokio::test]
async fn agent_backed_by_a2a_streams() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, _recorded, server) = serve_stub(
        "streamer",
        &["chat"],
        StubReply::CompletedText("streamed answer".into()),
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    let mut stream = remote
        .model()
        .stream(text_request("stream me"))
        .expect("stream should open");

    let mut text = String::new();
    while let Some(chunk) = stream.next().await {
        if let rig_core::streaming::Item::Event(rig_core::streaming::StreamEvent::Text {
            text: delta,
            ..
        }) = chunk.expect("stream item")
        {
            text.push_str(&delta);
        }
    }
    assert!(
        text.contains("streamed answer"),
        "expected the remote artifact text, got {text:?}"
    );

    server.abort();
}

#[tokio::test]
async fn model_stream_surfaces_remote_failure() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, _recorded, server) =
        serve_stub("streamer", &["chat"], StubReply::failed("quota exceeded")).await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    let mut stream = remote
        .model()
        .stream(text_request("stream me"))
        .expect("stream should open");

    let mut error = None;
    while let Some(chunk) = stream.next().await {
        if let Err(err) = chunk {
            error = Some(err);
            break;
        }
    }
    let error = error.expect("a failed remote task must end the stream with an error");
    assert!(error.to_string().contains("quota exceeded"), "{error}");

    server.abort();
}

/// An A2A agent may deliver its whole answer through `TaskStatusUpdateEvent`
/// status messages rather than artifacts. The non-streaming surface projects
/// those (`task.status.message` is part of the task body), so the streaming
/// surface must too, or the same server answers one way and stays silent the
/// other.
#[tokio::test]
async fn model_stream_yields_status_message_text() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, _recorded, server) = serve_stub(
        "statuser",
        &["chat"],
        StubReply::StatusStream(vec![
            (TaskState::Working, "partial answer ".to_string()),
            (TaskState::Completed, "final answer".to_string()),
        ]),
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    let mut stream = remote
        .model()
        .stream(text_request("stream me"))
        .expect("stream should open");
    let mut text = String::new();
    while let Some(chunk) = stream.next().await {
        if let rig_core::streaming::Item::Event(rig_core::streaming::StreamEvent::Text {
            text: delta,
            ..
        }) = chunk.expect("stream item")
        {
            text.push_str(&delta);
        }
    }
    assert_eq!(text, "final answer");

    // The unary surface already projected this text; the two must agree.
    let response = remote
        .model()
        .completion(text_request("send me"))
        .await
        .expect("completion should succeed");
    assert!(
        matches!(
            response.choice.first(),
            Some(rig_core::completion::AssistantContent::Text(text)) if text.text == "final answer"
        ),
        "{:?}",
        response.choice
    );

    server.abort();
}

/// Wire fixtures deliberately bypass the server reducer so malformed EOF and
/// repeated snapshots reach the client unchanged. Both real HTTP transports run.
async fn serve_wire(
    binding: StubBinding,
    events: Vec<StreamResponse>,
    streaming: bool,
) -> (
    A2AClient,
    Arc<Mutex<Vec<serde_json::Value>>>,
    tokio::task::JoinHandle<()>,
) {
    serve_wire_script(binding, vec![events], streaming).await
}

async fn serve_wire_script(
    binding: StubBinding,
    script: Vec<Vec<StreamResponse>>,
    streaming: bool,
) -> (
    A2AClient,
    Arc<Mutex<Vec<serde_json::Value>>>,
    tokio::task::JoinHandle<()>,
) {
    use axum::response::IntoResponse;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let saved = recorded.clone();
    let app = axum::Router::new().fallback(axum::routing::post(
        move |axum::Json(request): axum::Json<serde_json::Value>| {
            let events = {
                let mut requests = saved.lock().unwrap();
                let events = script[requests.len().min(script.len() - 1)].clone();
                requests.push(request.clone());
                events
            };
            async move {
                let wrap = |event| match binding {
                    StubBinding::JsonRpc => {
                        serde_json::json!({"jsonrpc": "2.0", "id": request["id"], "result": event})
                    }
                    StubBinding::Rest => event,
                };
                if streaming {
                    let body = events
                        .into_iter()
                        .map(|event| {
                            format!("data: {}\n\n", wrap(serde_json::to_value(event).unwrap()))
                        })
                        .collect::<String>();
                    ([("content-type", "text/event-stream")], body).into_response()
                } else {
                    axum::Json(wrap(serde_json::to_value(&events[0]).unwrap())).into_response()
                }
            }
        },
    ));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut card = card_with_skills(&base, "wire", &[]);
    card.supported_interfaces = vec![AgentInterface::new(
        &base,
        match binding {
            StubBinding::JsonRpc => TRANSPORT_PROTOCOL_JSONRPC,
            StubBinding::Rest => TRANSPORT_PROTOCOL_HTTP_JSON,
        },
    )];
    (
        A2AClient::from_agent_card(card).await.unwrap(),
        recorded,
        server,
    )
}

fn artifact_event(id: &str, text: &str, append: Option<bool>) -> StreamResponse {
    StreamResponse::ArtifactUpdate(a2a::TaskArtifactUpdateEvent {
        task_id: "task-1".into(),
        context_id: "ctx-1".into(),
        artifact: Artifact {
            artifact_id: id.into(),
            name: None,
            description: None,
            parts: vec![Part::text(text)],
            metadata: None,
            extensions: None,
        },
        append,
        last_chunk: Some(true),
        metadata: None,
    })
}

#[tokio::test]
async fn streaming_reduces_replacements_appends_and_snapshots_without_duplicate_text() {
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        for snapshot in [false, true] {
            let mut events = vec![
                StreamResponse::StatusUpdate(status_update(
                    "task-1",
                    "ctx-1",
                    TaskState::Working,
                    Some("progress is not the answer"),
                )),
                artifact_event("a", "obsolete", None),
                artifact_event("b", "second", None),
                artifact_event("a", "replaced", Some(false)),
                artifact_event("a", " + appended", Some(true)),
                StreamResponse::StatusUpdate(status_update(
                    "task-1",
                    "ctx-1",
                    TaskState::Completed,
                    Some("done"),
                )),
            ];
            if snapshot {
                events.push(StreamResponse::Task(Task {
                    id: "task-1".into(),
                    context_id: "ctx-1".into(),
                    status: TaskStatus {
                        state: TaskState::Completed,
                        message: Some(Message::new(Role::Agent, vec![Part::text("done")])),
                        timestamp: None,
                    },
                    artifacts: Some(
                        [("a", "replaced + appended"), ("b", "second")]
                            .into_iter()
                            .map(|(id, text)| {
                                let StreamResponse::ArtifactUpdate(event) =
                                    artifact_event(id, text, None)
                                else {
                                    unreachable!()
                                };
                                event.artifact
                            })
                            .collect(),
                    ),
                    history: None,
                    metadata: None,
                }));
            }
            let (client, _, server) = serve_wire(binding, events.clone(), true).await;
            let response = client
                .model()
                .stream(text_request("go"))
                .unwrap()
                .finish()
                .await
                .unwrap();
            assert_eq!(response.text(), "replaced + appended\nsecond\ndone");
            let rig_a2a::A2AReply::Events(raw) =
                response.extras::<rig_a2a::A2A>().unwrap().unwrap()
            else {
                panic!("expected events")
            };
            assert_eq!(raw.len(), events.len());
            assert_eq!(raw.first(), events.first());
            assert_eq!(raw.last(), events.last());
            let StreamResponse::ArtifactUpdate(replacement) = &raw[3] else {
                panic!("expected replacement")
            };
            assert!(!replacement.append.unwrap_or(false));
            assert_eq!(replacement.artifact.parts, vec![Part::text("replaced")]);
            server.abort();
        }
    }
}

#[tokio::test]
async fn incomplete_and_invalid_streams_never_report_success() {
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        for events in [
            vec![],
            vec![StreamResponse::StatusUpdate(status_update(
                "task-1",
                "ctx-1",
                TaskState::Working,
                Some("not done"),
            ))],
            vec![artifact_event(
                "a",
                "lastChunk is not task completion",
                None,
            )],
            vec![
                artifact_event("missing", "orphan append", Some(true)),
                StreamResponse::StatusUpdate(status_update(
                    "task-1",
                    "ctx-1",
                    TaskState::Completed,
                    None,
                )),
            ],
            vec![
                artifact_event("a", "wrong task", None),
                StreamResponse::StatusUpdate(status_update(
                    "task-2",
                    "ctx-1",
                    TaskState::Completed,
                    None,
                )),
            ],
            vec![
                artifact_event("a", &"x".repeat(600_000), None),
                artifact_event("a", &"y".repeat(600_000), Some(true)),
                StreamResponse::StatusUpdate(status_update(
                    "task-1",
                    "ctx-1",
                    TaskState::Completed,
                    None,
                )),
            ],
        ] {
            let (client, _, server) = serve_wire(binding, events, true).await;
            assert!(
                client
                    .model()
                    .stream(text_request("go"))
                    .unwrap()
                    .finish()
                    .await
                    .is_err()
            );
            server.abort();
        }
    }
}

#[tokio::test]
async fn typed_options_compose_with_conversations_and_survive_serialization() {
    use rig_a2a::{A2A, A2AOptions, A2AReply};
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        let reply = Message::new(Role::Agent, vec![Part::text("ok")]);
        let (client, recorded, server) =
            serve_wire(binding, vec![StreamResponse::Message(reply.clone())], false).await;
        let options = A2AOptions::default()
            .context("remote-context")
            .reference_tasks(["related"])
            .metadata(
                serde_json::json!({"trace": "abc"})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .configuration(a2a::SendMessageConfiguration {
                accepted_output_modes: Some(vec!["text/plain".into()]),
                history_length: Some(0),
                task_push_notification_config: None,
                return_immediately: Some(false),
            });
        let request = CompletionRequest::new("direct").provider_option(options.clone());
        let request = serde_json::from_value(serde_json::to_value(request).unwrap()).unwrap();
        let response = client.model().completion(request).await.unwrap();
        assert!(
            matches!(response.extras::<A2A>().unwrap().unwrap(), A2AReply::Response(response) if *response == SendMessageResponse::Message(reply))
        );
        client
            .agent()
            .provider_option(options.clone())
            .additional_params(serde_json::json!({"metadata": {"extra": true}}))
            .a2a_conversation("local-key")
            .build()
            .prompt("agent")
            .a2a_conversation("run-key")
            .await
            .unwrap();
        client
            .message("client")
            .options(options)
            .send()
            .await
            .unwrap();
        let requests = recorded.lock().unwrap();
        for (index, raw) in requests.iter().enumerate() {
            let body = match binding {
                StubBinding::JsonRpc => &raw["params"],
                StubBinding::Rest => raw,
            };
            assert_eq!(body["message"]["contextId"], "remote-context");
            assert_eq!(
                body["message"]["referenceTaskIds"],
                serde_json::json!(["related"])
            );
            assert_eq!(body["metadata"]["trace"], "abc");
            assert_eq!(body["configuration"]["historyLength"], 0);
            assert_eq!(
                body["configuration"]["acceptedOutputModes"],
                serde_json::json!(["text/plain"])
            );
            if index == 1 {
                assert_eq!(body["metadata"]["extra"], true);
            }
        }
        assert_eq!(requests.len(), 3);
        server.abort();
    }
}

#[tokio::test]
async fn task_only_options_resume_a_paused_task_without_replaying_history() {
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        let (addr, recorded, server) = serve_script(
            "pauser",
            &[],
            [
                StubReply::InputRequired {
                    prompt: "which file?".into(),
                },
                StubReply::CompletedText("done".into()),
            ],
            binding,
        )
        .await;
        let client = A2AClient::from_url(format!("http://{addr}")).await.unwrap();
        let SendMessageResponse::Task(task) = client.message("start").send().await.unwrap() else {
            panic!("expected task")
        };
        client
            .agent()
            .build()
            .prompt("README.md")
            .history([RigMessage::user("must not repeat")])
            .provider_option(rig_a2a::A2AOptions::default().task(&task.id))
            .await
            .unwrap();
        assert_eq!(recorded_texts(&recorded, 1), ["README.md"]);
        assert_eq!(
            recorded.lock().unwrap()[1].message.task_id.as_deref(),
            Some(task.id.as_str())
        );
        server.abort();
    }
}

#[tokio::test]
async fn routing_away_from_a2a_keeps_the_selected_models_history() {
    let card = card_with_skills("http://127.0.0.1:1", "unreachable", &[]);
    let client = A2AClient::from_agent_card(card).await.unwrap();
    let mock = MockCompletionModel::from_turns([MockTurn::text("local answer")]);
    let agent = client.agent().model_route("local", mock.clone()).build();
    let response = agent
        .prompt("new question")
        .merge_additional_params(
            serde_json::json!({"a2a": {"context_id": "remote-context"}})
                .as_object()
                .unwrap()
                .clone(),
        )
        .history([
            RigMessage::user("old question"),
            RigMessage::assistant("old answer"),
        ])
        .using_model("local")
        .await
        .unwrap();
    assert_eq!(response.to_string(), "local answer");
    let requests = mock.requests();
    let history = &requests[0].chat_history;
    assert!(history.iter().any(|message| matches!(message, RigMessage::User { content } if content.iter().any(|part| matches!(part, UserContent::Text(text) if text.text.contains("old question"))))));
    assert!(history.iter().any(|message| matches!(message, RigMessage::Assistant(message) if message.content.iter().any(|part| matches!(part, rig_core::message::AssistantContent::Text(text) if text.text == "old answer")))));
}

#[tokio::test]
async fn a_registered_remote_route_keeps_the_original_message_boundary() {
    use rig_a2a::A2AModelRouteExt;
    let (addr, recorded, server) = serve_stub(
        "routed",
        &[],
        StubReply::CompletedText("remote answer".into()),
    )
    .await;
    let client = A2AClient::from_url(format!("http://{addr}")).await.unwrap();
    let agent = AgentBuilder::new(MockCompletionModel::from_turns([MockTurn::text("local")]))
        .a2a_model_route("remote", client.model())
        .build();
    for streaming in [false, true] {
        let run = agent
            .prompt("only this prompt")
            .history([
                RigMessage::user("old user message"),
                RigMessage::assistant(""),
            ])
            .provider_option(rig_a2a::A2AOptions::default().context("ctx-1"))
            .using_model("remote");
        if streaming {
            let mut stream = run.stream();
            while let Some(item) = stream.next().await {
                item.unwrap();
            }
        } else {
            assert_eq!(run.await.unwrap().to_string(), "remote answer");
        }
    }
    assert_eq!(recorded_texts(&recorded, 0), ["only this prompt"]);
    assert_eq!(recorded_texts(&recorded, 1), ["only this prompt"]);
    server.abort();
}

#[tokio::test]
async fn image_replies_are_canonical_rig_content_on_both_completion_surfaces() {
    use rig_core::message::{AssistantContent, DocumentSourceKind};
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        for streaming in [false, true] {
            let message = Message::new(
                Role::Agent,
                vec![
                    Part {
                        content: a2a::PartContent::Raw(vec![1, 2, 3]),
                        media_type: Some("image/png".into()),
                        filename: None,
                        metadata: None,
                    },
                    Part {
                        content: a2a::PartContent::Url("https://example.com/image.webp".into()),
                        media_type: Some("image/webp".into()),
                        filename: None,
                        metadata: None,
                    },
                ],
            );
            let (client, _, server) =
                serve_wire(binding, vec![StreamResponse::Message(message)], streaming).await;
            let response = if streaming {
                client
                    .model()
                    .stream(text_request("draw"))
                    .unwrap()
                    .finish()
                    .await
                    .unwrap()
            } else {
                client
                    .model()
                    .completion(text_request("draw"))
                    .await
                    .unwrap()
            };
            assert_eq!(response.choice.len(), 2);
            let AssistantContent::Image(first) = &response.choice[0] else {
                panic!("expected image")
            };
            let AssistantContent::Image(second) = &response.choice[1] else {
                panic!("expected image")
            };
            assert_eq!(first.data, DocumentSourceKind::Base64("AQID".into()));
            assert_eq!(
                second.data,
                DocumentSourceKind::Url("https://example.com/image.webp".into())
            );
            assert_eq!(
                first.media_type,
                Some(rig_core::message::ImageMediaType::PNG)
            );
            assert_eq!(
                second.media_type,
                Some(rig_core::message::ImageMediaType::WEBP)
            );
            server.abort();
        }
    }
}

#[tokio::test]
async fn completion_refuses_background_work_but_direct_requests_preserve_it() {
    let task = Task {
        id: "task-1".into(),
        context_id: "ctx-1".into(),
        status: TaskStatus {
            state: TaskState::Working,
            message: Some(Message::new(Role::Agent, vec![Part::text("still working")])),
            timestamp: None,
        },
        artifacts: None,
        history: None,
        metadata: None,
    };
    let (client, recorded, server) = serve_wire(
        StubBinding::JsonRpc,
        vec![StreamResponse::Task(task)],
        false,
    )
    .await;
    let options = rig_a2a::A2AOptions::default().configuration(a2a::SendMessageConfiguration {
        accepted_output_modes: None,
        history_length: None,
        task_push_notification_config: None,
        return_immediately: Some(true),
    });
    assert!(
        client
            .model()
            .completion(text_request("go").provider_option(options.clone()))
            .await
            .unwrap_err()
            .to_string()
            .contains("return_immediately")
    );
    assert!(recorded.lock().unwrap().is_empty());
    assert!(
        client
            .model()
            .completion(text_request("go"))
            .await
            .unwrap_err()
            .to_string()
            .contains("working")
    );
    let SendMessageResponse::Task(task) =
        client.message("go").options(options).send().await.unwrap()
    else {
        panic!("expected task")
    };
    assert_eq!(task.status.state, TaskState::Working);
    server.abort();
}

#[tokio::test]
async fn multimodal_input_and_direct_events_keep_bytes_mime_types_and_order() {
    use rig_core::message::{
        Audio, AudioMediaType, Document as MediaDocument, DocumentMediaType,
        DocumentSourceKind as Source, Image, ImageMediaType, Video, VideoMediaType,
    };
    let prompt = RigMessage::User {
        content: vec![
            UserContent::text("inspect these"),
            UserContent::Image(Image {
                data: Source::Url("https://example.com/image.png".into()),
                media_type: Some(ImageMediaType::PNG),
                ..Default::default()
            }),
            UserContent::Audio(Audio {
                data: Source::Base64("AQIDBA==".into()),
                media_type: Some(AudioMediaType::WAV),
            }),
            UserContent::Video(Video {
                data: Source::Raw(vec![5, 6, 7]),
                media_type: Some(VideoMediaType::MP4),
                ..Default::default()
            }),
            UserContent::Document(MediaDocument {
                data: Source::String("plain document".into()),
                media_type: Some(DocumentMediaType::TXT),
                ..Default::default()
            }),
            UserContent::text("end"),
        ],
    };
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        let (addr, recorded, server) =
            serve_stub_with_binding("media", &[], StubReply::CompletedText("ok".into()), binding)
                .await;
        let client = A2AClient::from_url(format!("http://{addr}")).await.unwrap();
        let agent = client.agent().build();
        for streaming in [false, true] {
            let runner = agent.prompt(prompt.clone()).a2a_conversation("media");
            if streaming {
                let mut stream = runner.stream();
                while let Some(item) = stream.next().await {
                    item.unwrap();
                }
            } else {
                runner.await.unwrap();
            }
        }
        {
            let requests = recorded.lock().unwrap();
            for request in requests.iter() {
                let parts = &request.message.parts;
                assert_eq!(parts.len(), 6);
                assert_eq!(
                    parts[0].content,
                    a2a::PartContent::Text("inspect these".into())
                );
                assert_eq!(
                    parts[1].content,
                    a2a::PartContent::Url("https://example.com/image.png".into())
                );
                assert_eq!(parts[1].media_type.as_deref(), Some("image/png"));
                assert_eq!(parts[2].content, a2a::PartContent::Raw(vec![1, 2, 3, 4]));
                assert_eq!(parts[2].media_type.as_deref(), Some("audio/wav"));
                assert_eq!(parts[3].content, a2a::PartContent::Raw(vec![5, 6, 7]));
                assert_eq!(parts[3].media_type.as_deref(), Some("video/mp4"));
                assert_eq!(
                    parts[4].content,
                    a2a::PartContent::Raw(b"plain document".to_vec())
                );
                assert_eq!(parts[4].media_type.as_deref(), Some("text/plain"));
                assert_eq!(parts[5].content, a2a::PartContent::Text("end".into()));
            }
        }
        let events = vec![StreamResponse::Message(Message::new(
            Role::Agent,
            vec![Part {
                content: a2a::PartContent::Raw(vec![0, 1, 255]),
                media_type: Some("application/octet-stream".into()),
                filename: Some("result.bin".into()),
                metadata: None,
            }],
        ))];
        let (wire_client, recorded, wire_server) = serve_wire(binding, events.clone(), true).await;
        let mut message = Message::new(Role::User, vec![Part::text("go")]);
        message.message_id = "stable-id".into();
        let actual = wire_client
            .request(message)
            .part(Part::text("more"))
            .stream()
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            actual.into_iter().collect::<Result<Vec<_>, _>>().unwrap(),
            events
        );
        let raw = recorded.lock().unwrap();
        let body = match binding {
            StubBinding::JsonRpc => &raw[0]["params"],
            StubBinding::Rest => &raw[0],
        };
        assert_eq!(body["message"]["messageId"], "stable-id");
        assert_eq!(body["message"]["parts"].as_array().unwrap().len(), 2);
        server.abort();
        wire_server.abort();
    }
}

#[tokio::test]
async fn a_restored_agent_run_can_resume_remote_work_with_a_fresh_client() {
    use rig_agent::agent::{AgentRun, AgentRunStep, ModelTurn, RetryRequest};
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        let (addr, recorded, server) = serve_script(
            "durable",
            &[],
            [
                StubReply::InputRequired {
                    prompt: "which file?".into(),
                },
                StubReply::CompletedText("updated".into()),
            ],
            binding,
        )
        .await;
        let client = A2AClient::from_url(format!("http://{addr}")).await.unwrap();
        let mut run = AgentRun::new("update a file").max_turns(2);
        let AgentRunStep::CallModel { prompt, .. } = run.next_step().unwrap() else {
            panic!("expected a pending model call")
        };
        let pending = serde_json::to_vec(&run).unwrap();
        let error = client
            .agent()
            .build()
            .resume(serde_json::from_slice(&pending).unwrap())
            .provider_option(rig_a2a::A2AOptions::default().task("does-not-reissue"))
            .run()
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("model response is pending"),
            "{error}"
        );
        assert!(
            recorded.lock().unwrap().is_empty(),
            "AwaitingModel must not resend the pending call"
        );

        let response = client
            .model()
            .completion(CompletionRequest::new(prompt).accept_unknown_finish_reasons(true))
            .await
            .unwrap();
        let rig_a2a::A2AReply::Response(reply) =
            response.extras::<rig_a2a::A2A>().unwrap().unwrap()
        else {
            panic!("expected unary reply")
        };
        let SendMessageResponse::Task(task) = *reply else {
            panic!("expected task")
        };
        // A host persists the pending run and the response to its issued call.
        // The A2A completion advertises no local tools.
        let turn =
            ModelTurn::from_response_parts(&response, Default::default(), Default::default());
        let saved_turn = serde_json::to_vec(&turn).unwrap();
        let mut run: AgentRun = serde_json::from_slice(&pending).unwrap();
        run.model_response(serde_json::from_slice(&saved_turn).unwrap())
            .unwrap();
        assert_eq!(run.turn(), 1);
        assert_eq!(run.completion_calls().len(), 1);

        // Resuming an accepted turn finalizes it without another HTTP request.
        let accepted = serde_json::to_vec(&run).unwrap();
        assert_eq!(
            client
                .agent()
                .build()
                .resume(serde_json::from_slice(&accepted).unwrap())
                .run()
                .await
                .unwrap()
                .output(),
            "which file?"
        );
        assert_eq!(recorded.lock().unwrap().len(), 1);

        run.retry_model_turn(RetryRequest::Feedback("README.md".into()))
            .unwrap();
        let saved_run = serde_json::to_vec(&run).unwrap();
        let saved_options = serde_json::to_vec(
            &rig_a2a::A2AOptions::default()
                .context(&task.context_id)
                .task(&task.id),
        )
        .unwrap();
        drop(client);
        let client = A2AClient::from_url(format!("http://{addr}")).await.unwrap();
        let restored: AgentRun = serde_json::from_slice(&saved_run).unwrap();
        let options: rig_a2a::A2AOptions = serde_json::from_slice(&saved_options).unwrap();
        assert_eq!(
            client
                .agent()
                .build()
                .resume(restored)
                .provider_option(options)
                .run()
                .await
                .unwrap()
                .to_string(),
            "updated"
        );
        assert_eq!(recorded_texts(&recorded, 1), ["README.md"]);
        let requests = recorded.lock().unwrap();
        assert_eq!(
            requests[1].message.task_id.as_deref(),
            Some(task.id.as_str())
        );
        assert_eq!(
            requests[1].message.context_id.as_deref(),
            Some(task.context_id.as_str())
        );
        server.abort();
    }
}

/// An artifact update names its task but reports no task state, so it says
/// nothing about resumability. Letting it clear the paused task's id would
/// strand the conversation: the next turn would open a new task and the remote
/// would never receive the input it asked for.
#[tokio::test]
async fn artifact_after_a_pause_keeps_the_resumable_task_id() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) = serve_script(
        "pauser",
        &["ask"],
        [
            StubReply::PauseThenArtifact {
                prompt: "which file?".to_string(),
                artifact: "draft".to_string(),
            },
            StubReply::CompletedText("done".into()),
        ],
        StubBinding::JsonRpc,
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");
    let model = remote.model_for_conversation("user-42");

    let mut stream = model
        .stream(text_request("turn 1"))
        .expect("stream should open");
    while let Some(chunk) = stream.next().await {
        chunk.expect("stream item");
    }

    model
        .completion(text_request("README.md"))
        .await
        .expect("turn 2 should succeed");

    let messages = recorded.lock().unwrap();
    assert_eq!(messages.len(), 2);
    assert!(
        messages[1].message.task_id.is_some(),
        "the trailing artifact must not drop the paused task's id"
    );
    assert!(messages[1].message.context_id.is_some());

    server.abort();
}

/// Acceptance is caller-owned, while both unary and streamed pauses retain
/// their distinct reason and the identifiers needed to resume.
#[tokio::test]
async fn input_required_uses_the_callers_finish_policy_and_resumes() {
    use rig_core::completion::FinishReason;
    use rig_core::message::StopReason;

    let (addr, recorded, server) = serve_script(
        "pauser",
        &["ask"],
        [
            StubReply::InputRequired {
                prompt: "which file?".into(),
            },
            StubReply::InputRequired {
                prompt: "which file?".into(),
            },
            StubReply::InputRequired {
                prompt: "which file?".into(),
            },
            StubReply::CompletedText("done".into()),
            StubReply::failed("quota exceeded"),
        ],
        StubBinding::JsonRpc,
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}")).await.unwrap();
    let model = remote.model_for_conversation("policy");
    let rejected = model.completion(text_request("start")).await.unwrap();
    assert_eq!(
        rejected.finish_reason(),
        Some(FinishReason::Other("input_required".into()))
    );
    assert!(matches!(rejected.stop(), StopReason::Error(_)));

    let accepted = model
        .stream(text_request("continue").accept_unknown_finish_reasons(true))
        .unwrap()
        .finish()
        .await
        .unwrap();
    assert_eq!(accepted.finish_reason(), rejected.finish_reason());
    assert_eq!(accepted.stop(), StopReason::Stop);

    let agent = remote.agent_for_conversation("policy").build();
    let reply = agent
        .prompt("continue")
        .accept_unknown_finish_reasons(true)
        .await
        .unwrap();
    assert_eq!(reply.output(), "which file?");
    assert_eq!(agent.prompt("README.md").await.unwrap().output(), "done");
    assert!(
        agent
            .prompt("again")
            .accept_unknown_finish_reasons(true)
            .await
            .is_err()
    );
    let messages = recorded.lock().unwrap();
    assert_eq!(messages.len(), 5);
    assert!(messages[1].message.context_id.is_some());
    for turn in 1..4 {
        assert_eq!(
            messages[turn].message.context_id,
            messages[1].message.context_id
        );
        assert!(
            messages[turn].message.task_id.is_some(),
            "paused task resumes"
        );
    }
    assert!(
        messages[4].message.task_id.is_none(),
        "completion clears task"
    );
    drop(messages);
    server.abort();
}

/// The leading system message is standing context. It goes out once, not a
/// second time as rendered history.
#[tokio::test]
async fn system_instructions_are_sent_once() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) =
        serve_stub("preamble", &["chat"], StubReply::CompletedText("ok".into())).await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    for model in [remote.model(), remote.model_for_conversation("user-42")] {
        let mut turn = text_request("hello");
        turn.chat_history
            .insert(0, rig_core::message::Message::system("You are terse."));
        model.completion(turn).await.expect("completion succeeds");
    }

    for turn in 0..2 {
        let texts = recorded_texts(&recorded, turn);
        let copies = texts
            .iter()
            .filter(|text| text.contains("You are terse."))
            .count();
        assert_eq!(copies, 1, "turn {turn}: {texts:?}");
        assert!(texts.iter().any(|text| text.contains("hello")), "{texts:?}");
    }

    server.abort();
}

/// Opening a stream sends nothing until it is polled. A stream dropped before
/// that never reached the remote, so its standing context must go out again.
#[tokio::test]
async fn an_unpolled_stream_does_not_mark_context_delivered() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) =
        serve_stub("lazy", &["chat"], StubReply::CompletedText("ok".into())).await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");
    let model = remote.model_for_conversation("user-42");

    model
        .completion(text_request("turn 1"))
        .await
        .expect("turn 1 should succeed");

    let mut turn = text_request("turn 2");
    turn.documents = vec![document("retrieved", "freshly retrieved passage")];
    drop(model.stream(turn.clone()).expect("stream should open"));
    model.completion(turn).await.expect("turn 2 should succeed");

    assert_eq!(recorded.lock().unwrap().len(), 2);
    let second = recorded_texts(&recorded, 1);
    assert!(
        second
            .iter()
            .any(|text| text.contains("freshly retrieved passage")),
        "the dropped stream never delivered the document: {second:?}"
    );

    server.abort();
}

/// A streamed terminal failure still carries the remote's identifiers. The
/// conversation keeps its context, and the failed task is not resumed.
#[tokio::test]
async fn a_streamed_failure_after_a_pause_keeps_context_and_drops_the_task() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) = serve_script(
        "failer",
        &["ask"],
        [
            StubReply::InputRequired {
                prompt: "which file?".to_string(),
            },
            StubReply::failed("quota exceeded"),
            StubReply::CompletedText("done".into()),
        ],
        StubBinding::JsonRpc,
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");
    let model = remote.model_for_conversation("user-42");

    model
        .completion(text_request("turn 1"))
        .await
        .expect("turn 1 pauses for input");

    let mut stream = model
        .stream(text_request("README.md"))
        .expect("stream should open");
    let mut failed = false;
    while let Some(chunk) = stream.next().await {
        if chunk.is_err() {
            failed = true;
            break;
        }
    }
    assert!(failed, "the failed task must end the stream with an error");

    model
        .completion(text_request("turn 3"))
        .await
        .expect("turn 3 should succeed");

    let messages = recorded.lock().unwrap();
    assert_eq!(messages.len(), 3);
    assert!(
        messages[1].message.task_id.is_some(),
        "turn 2 resumes the paused task"
    );
    assert!(messages[1].message.context_id.is_some());
    assert_eq!(
        messages[2].message.context_id, messages[1].message.context_id,
        "the conversation keeps its context"
    );
    assert!(
        messages[2].message.task_id.is_none(),
        "the failed task must not be resumed"
    );
    drop(messages);

    server.abort();
}

#[tokio::test]
async fn a2a_backed_agent_becomes_a_sub_agent_tool() {
    let _ = tracing_subscriber::fmt::try_init();

    let (addr, recorded, server) = serve_stub(
        "researcher",
        &["research"],
        StubReply::CompletedText("remote findings".into()),
    )
    .await;
    let remote = A2AClient::from_url(format!("http://{addr}"))
        .await
        .expect("client should fetch card");

    // Bind the conversation on the ordinary Rig builder before converting the
    // A2A-backed agent through Rig's standard sub-agent bridge.
    let sub_agent = remote.agent().a2a_conversation("user-42").build();

    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "call_1",
            "researcher",
            serde_json::json!({"prompt": "look it up"}),
        ),
        MockTurn::tool_call(
            "call_2",
            "researcher",
            serde_json::json!({"prompt": "check again"}),
        ),
        MockTurn::text("relayed"),
    ]);
    let orchestrator = AgentBuilder::new(model.clone())
        .dynamic_tool(sub_agent.into_tool().expect("named agent"))
        .build();

    let out = orchestrator
        .prompt("delegate")
        .max_turns(4)
        .await
        .expect("orchestrator run");
    assert_eq!(out.to_string(), "relayed");

    let results = tool_result_texts(&model);
    assert!(
        results.iter().any(|text| text == "remote findings"),
        "the sub-agent's remote answer should reach the orchestrator: {results:?}"
    );
    let requests = recorded.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].message.context_id, None);
    assert!(
        requests[1].message.context_id.is_some(),
        "the bound sub-agent must continue its remote conversation"
    );

    server.abort();
}

#[tokio::test]
async fn client_selects_first_supported_interface_in_card_order() {
    let mut card = card_with_skills("http://127.0.0.1:1", "ordered", &["chat"]);
    let mut unsupported_version =
        AgentInterface::new("http://127.0.0.1:2/jsonrpc", TRANSPORT_PROTOCOL_JSONRPC);
    unsupported_version.protocol_version = "2.0".to_string();
    let mut unsupported_minor =
        AgentInterface::new("http://127.0.0.1:5/jsonrpc", TRANSPORT_PROTOCOL_JSONRPC);
    unsupported_minor.protocol_version = "1.1".to_string();
    let mut rest = AgentInterface::new("http://127.0.0.1:3", TRANSPORT_PROTOCOL_HTTP_JSON);
    rest.protocol_version = "1.0.1".to_string();
    rest.tenant = Some("tenant-a".to_string());
    card.supported_interfaces = vec![
        unsupported_version,
        unsupported_minor,
        rest,
        AgentInterface::new("http://127.0.0.1:4/jsonrpc", TRANSPORT_PROTOCOL_JSONRPC),
    ];

    let client = A2AClient::from_agent_card(card)
        .await
        .expect("card has a supported interface");
    assert_eq!(
        client.interface().protocol_binding,
        TRANSPORT_PROTOCOL_HTTP_JSON
    );
    assert_eq!(client.interface().tenant.as_deref(), Some("tenant-a"));
}

#[tokio::test]
async fn selected_interface_must_be_an_absolute_http_url() {
    let mut card = card_with_skills("http://127.0.0.1:1", "invalid", &["chat"]);
    card.supported_interfaces[0].url = "not a URL".to_string();

    let err = match A2AClient::from_agent_card(card).await {
        Ok(_) => panic!("invalid interface URL must be rejected"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        rig_a2a::A2AError::AgentCard(rig_a2a::error::AgentCardError::InvalidInterfaceUrl { .. })
    ));
}

#[tokio::test]
async fn selected_interface_tenant_reaches_direct_and_model_requests() {
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        let (addr, recorded, server) = serve_stub_with_binding(
            "tenant",
            &["chat"],
            StubReply::CompletedText("ok".into()),
            binding,
        )
        .await;
        let mut card = card_with_skills(&format!("http://{addr}"), "tenant", &["chat"]);
        card.supported_interfaces[0] = match binding {
            StubBinding::JsonRpc => {
                AgentInterface::new(format!("http://{addr}/jsonrpc"), TRANSPORT_PROTOCOL_JSONRPC)
            }
            StubBinding::Rest => {
                AgentInterface::new(format!("http://{addr}"), TRANSPORT_PROTOCOL_HTTP_JSON)
            }
        };
        card.supported_interfaces[0].tenant = Some("tenant-a".to_string());

        let client = A2AClient::from_agent_card(card)
            .await
            .expect("client should use supplied card");
        client
            .message("direct")
            .send()
            .await
            .expect("direct request should succeed");
        client
            .model()
            .completion(text_request("model"))
            .await
            .expect("model request should succeed");
        let mut events = client.message("direct stream").stream().await.unwrap();
        while let Some(event) = events.next().await {
            event.unwrap();
        }
        client
            .model()
            .stream(text_request("model stream"))
            .unwrap()
            .finish()
            .await
            .unwrap();

        {
            let requests = recorded.lock().unwrap();
            assert_eq!(requests.len(), 4);
            assert!(
                requests
                    .iter()
                    .all(|request| request.tenant.as_deref() == Some("tenant-a"))
            );
        }

        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn empty_request_context_is_rejected_before_any_network_call() {
    // A card pointing at a closed port: if validation didn't reject first,
    // the send would fail with a connection error instead.
    let card = card_with_skills("http://127.0.0.1:1", "unreachable", &["chat"]);
    let client = A2AClient::from_agent_card(card)
        .await
        .expect("client from hand-supplied card");

    let err = client
        .message("hello")
        .context("   ")
        .send()
        .await
        .expect_err("blank context id must be rejected");
    assert!(
        matches!(err, rig_a2a::A2AError::InvalidContextId),
        "expected InvalidContextId, got {err:?}"
    );

    let err = client
        .message("hello")
        .task("")
        .send()
        .await
        .expect_err("blank task id must be rejected");
    assert!(
        matches!(err, rig_a2a::A2AError::InvalidTaskId),
        "expected InvalidTaskId, got {err:?}"
    );
}

#[tokio::test]
async fn cross_origin_card_interfaces_are_rejected_unless_allowed() {
    let _ = tracing_subscriber::fmt::try_init();

    // The served card advertises an interface on a *different* origin than
    // the one it is fetched from.
    let card = card_with_skills("http://192.0.2.1:9", "hostile", &["chat"]);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/.well-known/agent-card.json",
        axum::routing::get(move || {
            let card = card.clone();
            async move { axum::Json(card) }
        }),
    );
    let server_task =
        tokio::spawn(async move { axum::serve(listener, app).await.expect("test server") });

    let err = match A2AClient::from_url(format!("http://{addr}")).await {
        Ok(_) => panic!("cross-origin interface must be rejected"),
        Err(err) => err,
    };
    assert!(
        matches!(
            err,
            rig_a2a::A2AError::AgentCard(
                rig_a2a::error::AgentCardError::CrossOriginInterface { .. }
            )
        ),
        "expected CrossOriginInterface, got {err:?}"
    );

    // Trusted deployments can opt in to cross-origin interfaces.
    A2AClient::builder()
        .url(format!("http://{addr}"))
        .allow_cross_origin_interfaces(true)
        .build()
        .await
        .expect("cross-origin card should be accepted when explicitly allowed");

    server_task.abort();
}

#[tokio::test]
async fn cross_origin_agent_card_redirect_is_not_followed() {
    let redirected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let redirected_for_route = redirected.clone();
    let target_app = axum::Router::new().route(
        "/card",
        axum::routing::get(move || {
            let redirected = redirected_for_route.clone();
            async move {
                redirected.store(true, std::sync::atomic::Ordering::SeqCst);
                axum::http::StatusCode::OK
            }
        }),
    );
    let target_task =
        tokio::spawn(async move { axum::serve(target_listener, target_app).await.unwrap() });

    let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_addr = source_listener.local_addr().unwrap();
    let location = format!("http://{target_addr}/card");
    let source_app = axum::Router::new().route(
        "/.well-known/agent-card.json",
        axum::routing::get(move || {
            let location = location.clone();
            async move {
                (
                    axum::http::StatusCode::TEMPORARY_REDIRECT,
                    [(axum::http::header::LOCATION, location)],
                )
            }
        }),
    );
    let source_task =
        tokio::spawn(async move { axum::serve(source_listener, source_app).await.unwrap() });

    if A2AClient::from_url(format!("http://{source_addr}"))
        .await
        .is_ok()
    {
        panic!("agent-card redirect must not be followed");
    }
    assert!(!redirected.load(std::sync::atomic::Ordering::SeqCst));

    source_task.abort();
    target_task.abort();
}

#[tokio::test]
async fn cross_origin_protocol_redirect_is_not_followed() {
    let redirected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let redirected_for_route = redirected.clone();
    let target_app = axum::Router::new().route(
        "/messages",
        axum::routing::post(move || {
            let redirected = redirected_for_route.clone();
            async move {
                redirected.store(true, std::sync::atomic::Ordering::SeqCst);
                axum::http::StatusCode::OK
            }
        }),
    );
    let target_task =
        tokio::spawn(async move { axum::serve(target_listener, target_app).await.unwrap() });

    let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_addr = source_listener.local_addr().unwrap();
    let card = card_with_skills(&format!("http://{source_addr}"), "redirect", &["chat"]);
    let location = format!("http://{target_addr}/messages");
    let source_app = axum::Router::new()
        .route(
            "/.well-known/agent-card.json",
            axum::routing::get(move || {
                let card = card.clone();
                async move { axum::Json(card) }
            }),
        )
        .route(
            "/jsonrpc",
            axum::routing::post(move || {
                let location = location.clone();
                async move {
                    (
                        axum::http::StatusCode::TEMPORARY_REDIRECT,
                        [(axum::http::header::LOCATION, location)],
                    )
                }
            }),
        );
    let source_task =
        tokio::spawn(async move { axum::serve(source_listener, source_app).await.unwrap() });

    let mut default_headers = reqwest::header::HeaderMap::new();
    default_headers.insert(
        "x-api-key",
        reqwest::header::HeaderValue::from_static("must-not-leak"),
    );
    let client = A2AClient::builder()
        .url(format!("http://{source_addr}"))
        .http_client_builder(
            reqwest::Client::builder()
                .default_headers(default_headers)
                .redirect(reqwest::redirect::Policy::limited(10)),
        )
        .build()
        .await
        .expect("client should fetch agent card");
    client
        .message("secret")
        .send()
        .await
        .expect_err("protocol redirect must not be followed");
    assert!(!redirected.load(std::sync::atomic::Ordering::SeqCst));

    source_task.abort();
    target_task.abort();
}

#[tokio::test]
async fn configured_http_timeout_bounds_agent_card_fetch() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/.well-known/agent-card.json",
        axum::routing::get(|| async { std::future::pending::<axum::http::StatusCode>().await }),
    );
    let server_task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        A2AClient::builder()
            .url(format!("http://{addr}"))
            .http_client_builder(reqwest::Client::builder())
            .timeout(Duration::from_millis(100))
            .build(),
    )
    .await
    .expect("A2A timeout was not applied");
    let err = match result {
        Ok(_) => panic!("hanging card request must time out"),
        Err(err) => err,
    };
    assert!(matches!(err, rig_a2a::A2AError::Http(ref err) if err.is_timeout()));

    server_task.abort();
}

#[tokio::test]
async fn oversized_agent_card_is_rejected() {
    let _ = tracing_subscriber::fmt::try_init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/.well-known/agent-card.json",
        axum::routing::get(|| async { "x".repeat(2 * 1024 * 1024) }),
    );
    let server_task =
        tokio::spawn(async move { axum::serve(listener, app).await.expect("test server") });

    let err = match A2AClient::from_url(format!("http://{addr}")).await {
        Ok(_) => panic!("oversized card must be rejected"),
        Err(err) => err,
    };
    assert!(
        matches!(
            err,
            rig_a2a::A2AError::AgentCard(rig_a2a::error::AgentCardError::ResponseTooLarge { .. })
        ),
        "expected ResponseTooLarge, got {err:?}"
    );

    server_task.abort();
}

/// Application hooks run after the adapter was added to the builder.
#[tokio::test]
async fn oracle_later_hooks_control_the_sent_multipart_prompt() {
    use rig_agent::agent::{AgentHook, DispatchAction, DispatchEvent, HookContext};
    use rig_core::effect::EffectKind;
    struct Redact;
    impl AgentHook for Redact {
        async fn on_dispatch(&self, _: &HookContext, event: DispatchEvent<'_>) -> DispatchAction {
            let EffectKind::Completion { request, stream } = event.kind else {
                return DispatchAction::Proceed;
            };
            let mut request = request.clone();
            if let Some(RigMessage::User { content }) = request.chat_history.last_mut() {
                for part in content {
                    if let UserContent::Text(text) = part {
                        text.text = text.text.replace("private-token", "[redacted]");
                    }
                }
            }
            DispatchAction::Patch(EffectKind::Completion {
                request,
                stream: *stream,
            })
        }
    }
    use rig_a2a::{A2AModelRouteExt, A2AOptions};
    use rig_core::message::{DocumentSourceKind, Image, ImageMediaType};
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        let (addr, recorded, server) = serve_stub_with_binding(
            "redaction",
            &[],
            StubReply::CompletedText("safe".into()),
            binding,
        )
        .await;
        let client = A2AClient::from_url(format!("http://{addr}")).await.unwrap();
        for routed in [false, true] {
            let agent = if routed {
                AgentBuilder::new(MockCompletionModel::from_turns([MockTurn::text("local")]))
                    .a2a_model_route("remote", client.model())
            } else {
                client.agent()
            }
            .add_hook(Redact)
            .build();
            for explicit in [false, true] {
                for streaming in [false, true] {
                    let prompt = RigMessage::User {
                        content: vec![
                            UserContent::text("private-token"),
                            UserContent::Image(Image {
                                data: DocumentSourceKind::Url(
                                    "https://example.com/safe.png".into(),
                                ),
                                media_type: Some(ImageMediaType::PNG),
                                ..Default::default()
                            }),
                        ],
                    };
                    let mut run = agent
                        .prompt(prompt)
                        .history([RigMessage::user("do not resend"), RigMessage::assistant("")])
                        .a2a_conversation("redaction");
                    if routed {
                        run = run.using_model("remote");
                    }
                    if explicit {
                        run = run.provider_option(A2AOptions::default().context("ctx-explicit"));
                    }
                    if streaming {
                        let mut events = run.stream();
                        while let Some(event) = events.next().await {
                            event.unwrap();
                        }
                    } else {
                        assert_eq!(run.await.unwrap().output(), "safe");
                    }
                    let requests = recorded.lock().unwrap();
                    let message = &requests.last().unwrap().message;
                    let text = message
                        .parts
                        .iter()
                        .filter_map(|part| part.as_text())
                        .collect::<String>();
                    assert!(
                        !text.contains("private-token"),
                        "server received unredacted text"
                    );
                    assert!(text.contains("[redacted]"));
                    if message.context_id.is_some() {
                        assert!(!text.contains("do not resend"));
                    }
                    assert!(message.parts.iter().any(|part| matches!(&part.content, a2a::PartContent::Url(url) if url.ends_with("safe.png"))));
                }
            }
        }
        println!(
            "redaction: server inspected 8 requests; both routes, both thread modes, unary and stream"
        );
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn oracle_unary_and_stream_accept_the_same_media_and_escaped_text() {
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        for (mime, bytes, text) in [
            ("image/png", 800_000, "image".to_owned()),
            ("application/octet-stream", 800_000, "attachment".to_owned()),
            ("application/octet-stream", 0, "\\".repeat(600_000)),
        ] {
            let parts = vec![
                Part::text(&text),
                Part {
                    content: a2a::PartContent::Raw(vec![42; bytes]),
                    media_type: Some(mime.into()),
                    filename: None,
                    metadata: None,
                },
            ];
            for streaming in [false, true] {
                let (client, _, server) = serve_wire(
                    binding,
                    vec![StreamResponse::Message(Message::new(
                        Role::Agent,
                        parts.clone(),
                    ))],
                    streaming,
                )
                .await;
                let response = if streaming {
                    client
                        .model()
                        .stream(text_request("go"))
                        .unwrap()
                        .finish()
                        .await
                        .unwrap()
                } else {
                    client.model().completion(text_request("go")).await.unwrap()
                };
                assert_eq!(response.text().len(), text.len());
                assert!(response.text() == text);
                assert_eq!(
                    response.choice.len(),
                    if mime == "image/png" { 2 } else { 1 }
                );
                println!(
                    "payload accepted: stream={streaming}, mime={mime}, binary={bytes}, text={}",
                    text.len()
                );
                server.abort();
                let _ = server.await;
            }
        }
    }
}

#[tokio::test]
async fn oversized_replies_update_conversation_before_rejecting_content() {
    let task = |state, oversized: bool| {
        StreamResponse::Task(Task {
            id: "task-1".into(),
            context_id: "ctx-1".into(),
            status: TaskStatus {
                state,
                message: Some(Message::new(Role::Agent, vec![Part::text("answer")])),
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: oversized.then(|| {
                std::collections::HashMap::from([(
                    "large".into(),
                    serde_json::json!("x".repeat(16 * 1024 * 1024)),
                )])
            }),
        })
    };
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        for streaming in [false, true] {
            for (seed_paused, state) in [
                (true, TaskState::Completed),
                (false, TaskState::InputRequired),
            ] {
                let mut script = Vec::new();
                if seed_paused {
                    script.push(vec![task(TaskState::InputRequired, false)]);
                }
                script.push(vec![task(state.clone(), true)]);
                script.push(vec![task(TaskState::Completed, false)]);
                let (client, recorded, server) =
                    serve_wire_script(binding, script, streaming).await;
                let model = client.model_for_conversation("size-limit");
                if seed_paused {
                    let request = text_request("start");
                    if streaming {
                        model.stream(request).unwrap().finish().await.unwrap();
                    } else {
                        model.completion(request).await.unwrap();
                    }
                }
                let mut request = text_request("continue");
                request.documents = vec![document("delivered", "standing context")];
                let result = if streaming {
                    model.stream(request.clone()).unwrap().finish().await
                } else {
                    model.completion(request.clone()).await
                };
                let Err(error) = result else {
                    panic!("oversized reply must fail")
                };
                assert!(
                    error.to_string().contains("retained protocol response"),
                    "{error}"
                );
                if streaming {
                    model.stream(request).unwrap().finish().await.unwrap();
                } else {
                    model.completion(request).await.unwrap();
                }
                let requests = recorded.lock().unwrap().clone();
                let last = requests.last().unwrap();
                let body = match binding {
                    StubBinding::JsonRpc => &last["params"],
                    StubBinding::Rest => last,
                };
                assert_eq!(body["message"]["contextId"], "ctx-1");
                if state == TaskState::InputRequired {
                    assert_eq!(body["message"]["taskId"], "task-1");
                } else {
                    assert!(
                        body["message"]["taskId"].is_null(),
                        "completed task must not be resumed"
                    );
                }
                assert!(
                    !body["message"]["parts"]
                        .to_string()
                        .contains("standing context")
                );
                println!(
                    "oversize bookkeeping: stream={streaming} seed_paused={seed_paused} state={state:?}; next request has correct IDs and no repeated context"
                );
                server.abort();
                let _ = server.await;
            }
        }
    }
}

#[tokio::test]
async fn stream_identity_errors_do_not_replace_valid_conversation() {
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        for (invalid, expected) in [
            (
                StreamResponse::StatusUpdate(status_update(
                    "wrong-task",
                    "ctx-1",
                    TaskState::Completed,
                    None,
                )),
                "changed task or context identity",
            ),
            (
                StreamResponse::StatusUpdate(status_update(
                    "task-1",
                    "wrong-context",
                    TaskState::Completed,
                    None,
                )),
                "changed task or context identity",
            ),
            (
                StreamResponse::Message(Message::new(
                    Role::Agent,
                    vec![Part::text("wrong reply kind")],
                )),
                "mixes a message reply",
            ),
        ] {
            let (client, recorded, server) = serve_wire_script(
                binding,
                vec![
                    vec![
                        StreamResponse::StatusUpdate(status_update(
                            "task-1",
                            "ctx-1",
                            TaskState::InputRequired,
                            Some("which file?"),
                        )),
                        invalid,
                    ],
                    vec![StreamResponse::StatusUpdate(status_update(
                        "task-1",
                        "ctx-1",
                        TaskState::Completed,
                        Some("done"),
                    ))],
                ],
                true,
            )
            .await;
            let model = client.model_for_conversation("identity");
            let error = model
                .stream(text_request("start"))
                .unwrap()
                .finish()
                .await
                .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
            assert_eq!(
                model
                    .stream(text_request("README.md"))
                    .unwrap()
                    .finish()
                    .await
                    .unwrap()
                    .text(),
                "done"
            );
            {
                let requests = recorded.lock().unwrap();
                let body = match binding {
                    StubBinding::JsonRpc => &requests[1]["params"],
                    StubBinding::Rest => &requests[1],
                };
                assert_eq!(body["message"]["contextId"], "ctx-1");
                assert_eq!(body["message"]["taskId"], "task-1");
            }
            server.abort();
            let _ = server.await;
        }
    }
}

#[tokio::test]
async fn completion_limits_cover_text_boundary_and_protocol_metadata() {
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        for streaming in [false, true] {
            for (text_size, metadata_size, expected_error) in [
                (1024 * 1024, 0, None),
                (1024 * 1024 + 1, 0, Some("1048576")),
                (2, 17 * 1024 * 1024, Some("retained protocol response")),
            ] {
                let mut message =
                    Message::new(Role::Agent, vec![Part::text("x".repeat(text_size))]);
                if metadata_size > 0 {
                    message.metadata = Some(std::collections::HashMap::from([(
                        "detail".into(),
                        serde_json::json!("m".repeat(metadata_size)),
                    )]));
                }
                let (client, _, server) =
                    serve_wire(binding, vec![StreamResponse::Message(message)], streaming).await;
                let result = if streaming {
                    client
                        .model()
                        .stream(text_request("go"))
                        .unwrap()
                        .finish()
                        .await
                } else {
                    client.model().completion(text_request("go")).await
                };
                if let Some(expected) = expected_error {
                    let Err(error) = result else {
                        panic!("over-limit response must fail");
                    };
                    assert!(error.to_string().contains(expected), "{error}");
                } else {
                    assert_eq!(result.unwrap().text().len(), text_size);
                }
                println!(
                    "boundary: stream={streaming} text={text_size} metadata={metadata_size} rejected={}",
                    expected_error.is_some()
                );
                server.abort();
                let _ = server.await;
            }
        }
    }
}

#[tokio::test]
async fn oracle_repeated_replacements_have_a_cumulative_response_limit() {
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        let mut events = vec![artifact_event("a", &"x".repeat(600_000), None); 30];
        events.push(StreamResponse::StatusUpdate(status_update(
            "task-1",
            "ctx-1",
            TaskState::Completed,
            None,
        )));
        let (client, _, server) = serve_wire(binding, events, true).await;
        let result = client
            .model()
            .stream(text_request("go"))
            .unwrap()
            .finish()
            .await;
        let Err(error) = result else {
            panic!(
                "18 MB of retained events must fail even when the final artifact is only 600 KB"
            );
        };
        assert!(
            error.to_string().contains("retained protocol response"),
            "{error}"
        );
        println!("repeated replacements rejected: {error}");
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn oracle_failed_stream_does_not_consume_context_before_retry() {
    use axum::response::IntoResponse;
    for binding in [StubBinding::JsonRpc, StubBinding::Rest] {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let saved = recorded.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new().fallback(axum::routing::post(
            move |axum::Json(request): axum::Json<serde_json::Value>| {
                let saved = saved.clone();
                async move {
                    let count = {
                        let mut requests = saved.lock().unwrap();
                        requests.push(request.clone());
                        requests.len()
                    };
                    if count == 2 {
                        return (
                            [("content-type", "text/event-stream")],
                            "data: not-json\n\n",
                        )
                            .into_response();
                    }
                    let mut message = Message::new(Role::Agent, vec![Part::text("ok")]);
                    message.context_id = Some("ctx-retry".into());
                    let reply =
                        serde_json::to_value(SendMessageResponse::Message(message)).unwrap();
                    axum::Json(match binding {
                        StubBinding::JsonRpc => {
                            serde_json::json!({"jsonrpc":"2.0", "id":request["id"], "result":reply})
                        }
                        StubBinding::Rest => reply,
                    })
                    .into_response()
                }
            },
        ));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut card = card_with_skills(&base, "retry", &[]);
        card.supported_interfaces = vec![AgentInterface::new(
            &base,
            match binding {
                StubBinding::JsonRpc => TRANSPORT_PROTOCOL_JSONRPC,
                StubBinding::Rest => TRANSPORT_PROTOCOL_HTTP_JSON,
            },
        )];
        let client = A2AClient::from_agent_card(card).await.unwrap();
        let model = client.model_for_conversation("retry");
        model.completion(text_request("start")).await.unwrap();
        let mut request = text_request("continue");
        request.documents = vec![document("new", "must survive retry")];
        assert!(
            model
                .stream(request.clone())
                .unwrap()
                .finish()
                .await
                .is_err()
        );
        model.completion(request).await.unwrap();
        let requests = recorded.lock().unwrap().clone();
        for (index, request) in requests.iter().enumerate().skip(1) {
            let body = match binding {
                StubBinding::JsonRpc => &request["params"],
                StubBinding::Rest => request,
            };
            assert!(
                body["message"]["parts"]
                    .to_string()
                    .contains("must survive retry"),
                "attempt {index} lost context"
            );
        }
        println!("retry: server received fresh document on failed attempt and retry");
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn card_fetch_failure_surfaces_status_and_body_snippet() {
    let _ = tracing_subscriber::fmt::try_init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/.well-known/agent-card.json",
        axum::routing::get(|| async {
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                r#"{"error":"tenant suspended"}"#,
            )
        }),
    );
    let server_task =
        tokio::spawn(async move { axum::serve(listener, app).await.expect("test server") });

    let err = match A2AClient::from_url(format!("http://{addr}")).await {
        Ok(_) => panic!("5xx card fetch must fail"),
        Err(err) => err,
    };
    let rendered = err.to_string();
    assert!(
        rendered.contains("503"),
        "expected status in error: {rendered}"
    );
    assert!(
        rendered.contains("tenant suspended"),
        "expected body snippet in error: {rendered}"
    );

    server_task.abort();
}
