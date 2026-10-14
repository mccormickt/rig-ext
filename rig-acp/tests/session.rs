#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use futures::StreamExt;
use parking_lot::Mutex;
use rig_acp::{
    Client, Error, Event, Permission, Turn,
    protocol::{self, Agent, Channel, ConnectionTo, Responder},
    schema::*,
};
use rig_agent::{
    agent::{AgentBuilder, AgentHook, DispatchAction, DispatchEvent, HookContext},
    test_utils::{MockCompletionModel, MockTurn},
};
use rig_core::{
    completion::CompletionRequest,
    effect::EffectKind,
    message::{Message, UserContent},
    streaming::{Item, StreamEvent},
};
use std::{collections::HashMap, sync::Arc, time::Duration};

#[derive(Default)]
struct State {
    initialize: Option<InitializeRequest>,
    prompts: Vec<PromptRequest>,
    remembered: HashMap<SessionId, String>,
    effects: HashMap<SessionId, usize>,
    decisions: Vec<(SessionId, RequestPermissionOutcome)>,
    cancels: usize,
    gates: HashMap<SessionId, Responder<PromptResponse>>,
}

struct Peer {
    state: Arc<Mutex<State>>,
    task: tokio::task::JoinHandle<protocol::Result<()>>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn update(cx: &ConnectionTo<protocol::Client>, id: &SessionId, text: &str) -> protocol::Result<()> {
    cx.send_notification(SessionNotification::new(
        id.clone(),
        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(
            text,
        )))),
    ))
}

async fn peer() -> (Client, Peer) {
    let state = Arc::new(Mutex::new(State::default()));
    let init = state.clone();
    let sessions = state.clone();
    let prompts = state.clone();
    let cancels = state.clone();
    let (client, server) = Channel::duplex();
    let task = tokio::spawn(async move {
        Agent.builder()
            .on_receive_request(async move |request: InitializeRequest, responder: Responder<InitializeResponse>, _| {
                init.lock().initialize = Some(request);
                responder.respond(InitializeResponse::new(protocol::schema::ProtocolVersion::V1))
            }, protocol::on_receive_request!())
            .on_receive_request(async move |_: NewSessionRequest, responder: Responder<NewSessionResponse>, _| {
                let mut state = sessions.lock();
                let id = SessionId::new(format!("sdk-{}", state.remembered.len() + 1));
                state.remembered.insert(id.clone(), "empty".into());
                responder.respond(NewSessionResponse::new(id))
            }, protocol::on_receive_request!())
            .on_receive_notification(async move |request: CancelNotification, _| {
                let mut state = cancels.lock();
                state.cancels += 1;
                if let Some(responder) = state.gates.remove(&request.session_id) {
                    responder.respond(PromptResponse::new(StopReason::Cancelled))?;
                }
                Ok(())
            }, protocol::on_receive_notification!())
            .on_receive_request(async move |request: PromptRequest, responder: Responder<PromptResponse>, cx: ConnectionTo<protocol::Client>| {
                let text = request.prompt.iter().filter_map(|b| if let ContentBlock::Text(t) = b { Some(t.text.as_str()) } else { None }).collect::<Vec<_>>().join("\n");
                let id = request.session_id.clone();
                prompts.lock().prompts.push(request);
                if text == "permission" || text == "two-permissions" {
                    let shared = prompts.clone();
                    update(&cx, &id, "before")?;
                    return cx.clone().spawn(async move {
                        let permission: RequestPermissionRequest = serde_json::from_value(serde_json::json!({
                            "sessionId": id, "toolCall": {"toolCallId":"same-name", "title":"effect"},
                            "options": [{"optionId":"deny", "name":"Deny", "kind":"reject_once"}, {"optionId":"allow", "name":"Allow", "kind":"allow_once"}]
                        })).unwrap();
                        let pending = cx.send_request(permission.clone());
                        let second = (text == "two-permissions").then(|| cx.send_request(permission));
                        update(&cx, &id, "during")?;
                        let decision = pending.block_task().await?.outcome;
                        let allowed = matches!(&decision, RequestPermissionOutcome::Selected(s) if s.option_id.0.as_ref() == "allow");
                        {
                            let mut state = shared.lock();
                            state.decisions.push((id.clone(), decision.clone()));
                            if allowed { *state.effects.entry(id.clone()).or_default() += 1; }
                        }
                        update(&cx, &id, if allowed { "approved" } else { "denied" })?;
                        if let Some(second) = second {
                            let outcome = second.block_task().await?.outcome;
                            update(&cx, &id, if outcome == selected("allow") { ";second-approved" } else { ";second-denied" })?;
                        }
                        responder.respond(PromptResponse::new(if decision == RequestPermissionOutcome::Cancelled { StopReason::Cancelled } else { StopReason::EndTurn }))
                    });
                }
                if text == "wait" {
                    prompts.lock().gates.insert(id.clone(), responder);
                    return update(&cx, &id, "gated");
                }
                if text == "release" {
                    let mut state = prompts.lock();
                    for (id, responder) in std::mem::take(&mut state.gates) {
                        *state.effects.entry(id).or_default() += 1;
                        responder.respond(PromptResponse::new(StopReason::EndTurn))?;
                    }
                }
                if text == "error" { return responder.respond_with_error(protocol::Error::internal_error().data("controlled error")); }
                if text == "flood" {
                    for _ in 0..130 { update(&cx, &id, &"x".repeat(65536))?; }
                } else if text == "large-final" {
                    return responder.respond(PromptResponse::new(StopReason::EndTurn).meta(serde_json::Map::from_iter([("large".into(), "x".repeat(8 * 1024 * 1024).into())])));
                } else {
                    let reply = {
                        let mut state = prompts.lock();
                        if let Some(value) = text.strip_prefix("remember ") { state.remembered.insert(id.clone(), value.to_string()); }
                        if text == "recall" { state.remembered[&id].clone() } else { format!("echo:{text}") }
                    };
                    cx.send_notification(SessionNotification::new(id.clone(), SessionUpdate::AgentThoughtChunk(ContentChunk::new(ContentBlock::Text(TextContent::new("hidden-thought"))))))?;
                    update(&cx, &id, &reply[..2])?;
                    update(&cx, &id, &reply[2..])?;
                }
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            }, protocol::on_receive_request!())
            .connect_to(server).await
    });
    let client = Client::connect(client).await.unwrap();
    (client, Peer { state, task })
}

async fn next(turn: &mut Turn) -> Result<Event, Error> {
    tokio::time::timeout(Duration::from_secs(5), turn.next())
        .await
        .expect("event deadline")
        .expect("event")
}

async fn permission(turn: &mut Turn) -> Permission {
    assert!(matches!(next(turn).await.unwrap(), Event::Update(_)));
    let Event::Permission(permission) = next(turn).await.unwrap() else {
        panic!("permission required")
    };
    // The ordered handler must be free even while no permission answer exists.
    assert!(matches!(next(turn).await.unwrap(), Event::Update(_)));
    permission
}

async fn finish(mut turn: Turn) -> (String, StopReason) {
    let mut text = String::new();
    let mut stop = None;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), turn.next())
        .await
        .unwrap()
    {
        match event.unwrap() {
            Event::Update(n) => {
                if let SessionUpdate::AgentMessageChunk(c) = n.update
                    && let ContentBlock::Text(t) = c.content
                {
                    text.push_str(&t.text);
                }
            }
            Event::Completed(r) => {
                assert!(stop.is_none());
                stop = Some(r.stop_reason);
            }
            Event::Permission(_) => panic!("unhandled permission"),
        }
    }
    (text, stop.expect("final response"))
}

fn selected(id: &str) -> RequestPermissionOutcome {
    RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(id.to_owned()))
}

#[tokio::test]
async fn official_peer_sessions_capabilities_and_order() {
    let (client, peer) = peer().await;
    let init = serde_json::to_value(peer.state.lock().initialize.as_ref().unwrap()).unwrap();
    assert_eq!(init["protocolVersion"], 1);
    assert_eq!(init["clientCapabilities"]["fs"]["readTextFile"], false);
    assert_eq!(init["clientCapabilities"]["terminal"], false);
    let a = client.new_session("/tmp").await.unwrap();
    let b = client.new_session("/tmp").await.unwrap();
    finish(a.prompt_text("remember amber-73").unwrap()).await;
    assert_eq!(
        finish(a.prompt_text("recall").unwrap()).await,
        ("amber-73".into(), StopReason::EndTurn)
    );
    assert_eq!(finish(b.prompt_text("recall").unwrap()).await.0, "empty");
    let image: ContentBlock = serde_json::from_value(
        serde_json::json!({"type":"image","data":"AA==","mimeType":"image/png"}),
    )
    .unwrap();
    let before = peer.state.lock().prompts.len();
    assert!(matches!(a.prompt(vec![image]), Err(Error::Unsupported(_))));
    assert_eq!(peer.state.lock().prompts.len(), before);
    let mut stream = a.prompt_text("λ tail").unwrap();
    let mut sequence = Vec::new();
    while let Some(event) = stream.next().await {
        sequence.push(match event.unwrap() {
            Event::Update(n) => serde_json::to_value(n.update).unwrap(),
            Event::Completed(r) => serde_json::to_value(r).unwrap(),
            _ => panic!("unexpected permission"),
        });
    }
    assert_eq!(sequence.len(), 4);
    assert_eq!(sequence[0]["sessionUpdate"], "agent_thought_chunk");
    assert_eq!(sequence[1]["content"]["text"], "ec");
    assert_eq!(sequence[2]["content"]["text"], "ho:λ tail");
    assert_eq!(sequence[3]["stopReason"], "end_turn");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permissions_cannot_cross_sessions_and_cancel_wins() {
    let (client, peer) = peer().await;
    let a = client.new_session("/tmp").await.unwrap();
    let b = client.new_session("/tmp").await.unwrap();
    let mut ta = a.prompt_text("permission").unwrap();
    let mut tb = b.prompt_text("permission").unwrap();
    let pa = permission(&mut ta).await;
    let pb = permission(&mut tb).await;
    assert!(matches!(a.clone().prompt_text("overlap"), Err(Error::Busy)));
    pb.respond(selected("deny")).unwrap();
    pa.respond(selected("allow")).unwrap();
    assert_eq!(finish(tb).await.0, "denied");
    assert_eq!(finish(ta).await.0, "approved");
    assert_eq!(peer.state.lock().effects.get(a.id()), Some(&1));
    assert_eq!(peer.state.lock().effects.get(b.id()), None);
    for action in ["drop", "invalid", "cancel-pending", "cancel-before"] {
        let mut turn = a.prompt_text("permission").unwrap();
        if action == "cancel-before" {
            a.cancel().unwrap();
        } else {
            let permission = permission(&mut turn).await;
            match action {
                "drop" => drop(permission),
                "invalid" => assert!(matches!(
                    permission.respond(selected("forged")),
                    Err(Error::InvalidPermission)
                )),
                _ => {
                    a.cancel().unwrap();
                    let _ = permission.respond(selected("allow"));
                }
            }
        }
        assert_eq!(finish(turn).await.1, StopReason::Cancelled);
    }
    assert_eq!(peer.state.lock().effects.get(a.id()), Some(&1));
    assert_eq!(peer.state.lock().decisions.len(), 6);
    let mut turn = a.prompt_text("two-permissions").unwrap();
    next(&mut turn).await.unwrap();
    let Event::Permission(first) = next(&mut turn).await.unwrap() else {
        panic!("first permission")
    };
    let Event::Permission(second) = next(&mut turn).await.unwrap() else {
        panic!("second permission")
    };
    next(&mut turn).await.unwrap();
    second.respond(selected("allow")).unwrap();
    first.respond(selected("deny")).unwrap();
    assert_eq!(finish(turn).await.0, "denied;second-approved");
    for _ in 0..200 {
        let mut turn = a.prompt_text("permission").unwrap();
        let pending = permission(&mut turn).await;
        let (cancelled, wait) = tokio::sync::oneshot::channel();
        let session = a.clone();
        let cancelling = tokio::spawn(async move {
            session.cancel().unwrap();
            cancelled.send(()).unwrap();
        });
        let approving = tokio::spawn(async move {
            wait.await.unwrap();
            let _ = pending.respond(selected("allow"));
        });
        cancelling.await.unwrap();
        approving.await.unwrap();
        assert_eq!(finish(turn).await.1, StopReason::Cancelled);
    }
    assert_eq!(peer.state.lock().effects.get(a.id()), Some(&1));
}

#[tokio::test]
async fn cancellation_prevents_effect_drop_does_not_and_next_turn_works() {
    let (client, peer) = peer().await;
    let a = client.new_session("/tmp").await.unwrap();
    let control = client.new_session("/tmp").await.unwrap();
    let mut turn = a.prompt_text("wait").unwrap();
    next(&mut turn).await.unwrap();
    a.cancel().unwrap();
    assert_eq!(finish(turn).await.1, StopReason::Cancelled);
    finish(control.prompt_text("release").unwrap()).await;
    assert_eq!(peer.state.lock().effects.get(a.id()), None);
    assert!(peer.state.lock().gates.is_empty());
    let mut turn = a.prompt_text("wait").unwrap();
    next(&mut turn).await.unwrap();
    drop(turn);
    finish(control.prompt_text("release").unwrap()).await;
    assert_eq!(peer.state.lock().effects.get(a.id()), Some(&1));
    assert_eq!(peer.state.lock().cancels, 1);
    assert_eq!(finish(a.prompt_text("next").unwrap()).await.0, "echo:next");
}

struct Redact;
impl AgentHook for Redact {
    async fn on_dispatch(&self, _: &HookContext, event: DispatchEvent<'_>) -> DispatchAction {
        let EffectKind::Completion { request, stream } = event.kind else {
            return DispatchAction::Proceed;
        };
        let mut request = request.clone();
        if let Some(Message::User { content }) = request.chat_history.last_mut() {
            for item in content {
                if let UserContent::Text(text) = item {
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

#[tokio::test]
async fn rig_hooks_routes_streams_and_real_subagent_invocations() {
    let (client, peer) = peer().await;
    let session = client.new_session("/tmp").await.unwrap();
    let model = session.model();
    for routed in [false, true] {
        let builder = if routed {
            AgentBuilder::new(MockCompletionModel::from_turns([MockTurn::text(
                "BAD FALLBACK",
            )]))
            .model_route_handler("remote", model.handler("remote"))
        } else {
            model.agent("local-key")
        };
        let agent = builder.add_hook(Redact).build();
        let label = if routed { "remote" } else { "default" };
        let result = agent
            .prompt("private-token")
            .using_model(label)
            .await
            .unwrap();
        assert_eq!(result.to_string(), "echo:[redacted]");
        let mut stream = agent
            .prompt("private-token-stream")
            .using_model(label)
            .stream();
        let mut text = String::new();
        while let Some(item) = stream.next().await {
            if let rig_agent::agent::MultiTurnStreamItem::StreamAssistantItem(Item::Event(
                StreamEvent::Text { text: chunk, .. },
            )) = item.unwrap()
            {
                text.push_str(&chunk);
            }
        }
        assert_eq!(text, "echo:[redacted]-stream");
    }
    assert_eq!(peer.state.lock().prompts.len(), 4);
    for prompt in &peer.state.lock().prompts {
        let encoded = serde_json::to_string(prompt).unwrap();
        assert!(!encoded.contains("private-token"));
        assert_eq!(prompt.prompt.len(), 1);
    }
    let child = model.agent("child-key").build().into_tool().unwrap();
    let parent_model = MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "call-a",
            "acp_delegate",
            serde_json::json!({"prompt":"remember child-29"}),
        ),
        MockTurn::tool_call(
            "call-b",
            "acp_delegate",
            serde_json::json!({"prompt":"recall"}),
        ),
        MockTurn::text("done"),
    ]);
    let parent = AgentBuilder::new(parent_model.clone())
        .dynamic_tool(child)
        .build();
    assert_eq!(
        parent
            .prompt("delegate")
            .max_turns(4)
            .await
            .unwrap()
            .to_string(),
        "done"
    );
    assert_eq!(peer.state.lock().prompts.len(), 6);
    let results: Vec<_> = parent_model
        .requests()
        .into_iter()
        .flat_map(|request| request.chat_history)
        .filter_map(|message| {
            if let Message::User { content } = message {
                Some(content)
            } else {
                None
            }
        })
        .flatten()
        .filter_map(|content| {
            if let UserContent::ToolResult(result) = content {
                Some(result.content)
            } else {
                None
            }
        })
        .flatten()
        .filter_map(|content| {
            if let rig_core::message::ToolResultContent::Text(text) = content {
                Some(text.text)
            } else {
                None
            }
        })
        .collect();
    assert!(
        results.iter().any(|text| text == "child-29"),
        "actual recall result: {results:?}"
    );
    let calls = serde_json::to_string(&parent_model.requests()).unwrap();
    assert!(!calls.contains("sdk-1"));
    assert!(!calls.contains("hidden-thought"));
    let result = model
        .agent("policy-default")
        .build()
        .prompt("permission")
        .await;
    assert!(
        result.is_err(),
        "default policy cancels rather than fabricates success"
    );
    assert!(peer.state.lock().effects.is_empty());
    let before = peer.state.lock().prompts.len();
    let mut request = CompletionRequest::new("not dispatched");
    request.tool_choice = Some(rig_core::message::ToolChoice::Required);
    assert!(model.completion(request).await.is_err());
    assert_eq!(peer.state.lock().prompts.len(), before);
    let wrong_route = model
        .agent("wrong-route")
        .build()
        .prompt("not dispatched")
        .using_model("absent")
        .await;
    assert!(wrong_route.is_err());
    assert_eq!(peer.state.lock().prompts.len(), before);
    let response = model
        .completion(CompletionRequest::new("raw"))
        .await
        .unwrap();
    assert!(response.raw.to_string().contains("hidden-thought"));
    assert!(
        !serde_json::to_string(&response.choice)
            .unwrap()
            .contains("hidden-thought")
    );
}

#[tokio::test]
async fn cancellation_interrupts_a_pending_rig_permission_policy() {
    let (client, peer) = peer().await;
    let session = client.new_session("/tmp").await.unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let signal = started.clone();
    let model = session.model().permission_policy(move |_| {
        let signal = signal.clone();
        async move {
            signal.notify_one();
            futures::future::pending().await
        }
    });
    let run = tokio::spawn(async move { model.agent("policy").build().prompt("permission").await });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    session.cancel().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(peer.state.lock().effects.is_empty());
    assert_eq!(
        finish(session.prompt_text("next").unwrap()).await.0,
        "echo:next"
    );
}

#[tokio::test]
async fn errors_limits_and_transport_loss_do_not_become_success() {
    let (client, peer) = peer().await;
    let session = client.new_session("/tmp").await.unwrap();
    let mut error = session.prompt_text("error").unwrap();
    assert!(matches!(next(&mut error).await, Err(Error::Protocol(_))));
    assert!(error.next().await.is_none());
    for prompt in ["flood", "large-final"] {
        let mut turn = session.prompt_text(prompt).unwrap();
        let mut limits = 0;
        while let Some(event) = tokio::time::timeout(Duration::from_secs(5), turn.next())
            .await
            .unwrap()
        {
            match event {
                Err(Error::EventLimit) => limits += 1,
                Ok(Event::Update(_)) => {}
                other => panic!("unexpected event after budget: {other:?}"),
            }
        }
        assert_eq!(limits, 1);
    }
    assert_eq!(
        finish(session.prompt_text("next").unwrap()).await.0,
        "echo:next"
    );
    let mut waiting = session.prompt_text("wait").unwrap();
    next(&mut waiting).await.unwrap();
    let before = peer.state.lock().prompts.len();
    peer.task.abort();
    assert!(next(&mut waiting).await.is_err());
    assert_eq!(peer.state.lock().prompts.len(), before);
}

#[tokio::test]
async fn dropped_rig_stream_does_not_cancel_work_and_denies_held_permission() {
    let (client, peer) = peer().await;
    let session = client.new_session("/tmp").await.unwrap();
    let control = client.new_session("/tmp").await.unwrap();
    let mut stream = session
        .model()
        .stream(CompletionRequest::new("wait"))
        .unwrap();
    while !matches!(
        stream.next().await.unwrap().unwrap(),
        Item::Event(StreamEvent::Text { .. })
    ) {}
    drop(stream);
    finish(control.prompt_text("release").unwrap()).await;
    assert_eq!(peer.state.lock().effects.get(session.id()), Some(&1));
    assert_eq!(peer.state.lock().cancels, 0);
    let started = Arc::new(tokio::sync::Notify::new());
    let signal = started.clone();
    let model = session.model().permission_policy(move |_| {
        let signal = signal.clone();
        async move {
            signal.notify_one();
            futures::future::pending().await
        }
    });
    let mut stream = model.stream(CompletionRequest::new("permission")).unwrap();
    let polling = tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            item.unwrap();
        }
    });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    polling.abort();
    let _ = polling.await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match session.prompt_text("next") {
                Ok(turn) => {
                    assert_eq!(finish(turn).await.0, "echo:next");
                    break;
                }
                Err(Error::Busy) => tokio::task::yield_now().await,
                other => panic!("unexpected admission: {:?}", other.err()),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(peer.state.lock().effects.get(session.id()), Some(&1));
    assert_eq!(
        peer.state.lock().decisions.last().unwrap().1,
        RequestPermissionOutcome::Cancelled
    );
    assert_eq!(peer.state.lock().cancels, 0);
}
