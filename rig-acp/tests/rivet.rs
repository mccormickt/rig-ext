#![cfg(feature = "rivet")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response, Sse, sse::Event as SseEvent},
    routing::post,
};
use parking_lot::Mutex;
use rig_acp::{Client, Event, rivet::RivetTransport, schema::*};
use serde_json::{Value, json};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::Notify;

struct HttpPeer {
    mode: &'static str,
    frames: Mutex<Vec<Value>>,
    notify: Notify,
    prompt: Notify,
    release_post: Notify,
    admit: Notify,
    cancel_received: Notify,
    deliver_cancel: Notify,
    order: Mutex<Vec<&'static str>>,
    prompt_id: Mutex<Option<Value>>,
    counts: Mutex<(usize, usize, usize)>, // prompts, effects, DELETEs
}

impl HttpPeer {
    fn push(&self, value: Value) {
        self.frames.lock().push(value);
        self.notify.notify_one();
    }
    fn output(&self) {
        self.push(json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"remote", "update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"first λ"}}}}));
        self.push(json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"remote", "update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":" then second"}}}}));
        self.push(json!({"jsonrpc":"2.0","id":self.prompt_id.lock().clone().unwrap(),"result":{"stopReason":"end_turn"}}));
    }
}

async fn post_rpc(
    State(state): State<Arc<HttpPeer>>,
    headers: HeaderMap,
    Json(value): Json<Value>,
) -> Response {
    if headers.get("authorization").and_then(|h| h.to_str().ok())
        != Some("Bearer host-only-sentinel")
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let method = value["method"].as_str().unwrap_or("");
    let response = match method {
        "initialize" => json!({"protocolVersion":1,"agentCapabilities":{}}),
        "session/new" => json!({"sessionId":"remote"}),
        "session/prompt" => {
            assert!(!value.to_string().contains("host-only-sentinel"));
            if state.mode.starts_with("cancel-") {
                state.counts.lock().0 += 1;
                let next = value["params"]["prompt"][0]["text"] == "next";
                if !next {
                    state.prompt.notify_one();
                    if state.mode == "cancel-early" {
                        state.push(json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"remote","update":{"sessionUpdate":"available_commands_update","availableCommands":[]}}}));
                        state.admit.notified().await;
                    }
                }
                state
                    .order
                    .lock()
                    .push(if next { "next" } else { "prompt" });
                if state.mode != "cancel-silent" || next {
                    state.push(json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"remote","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"admitted"}}}}));
                }
                if !next {
                    state.release_post.notified().await;
                }
                let cancelled = !next && state.order.lock().contains(&"cancel");
                if !cancelled {
                    state.counts.lock().1 += 1;
                }
                let response = json!({"jsonrpc":"2.0","id":value["id"],"result":{"stopReason":if cancelled { "cancelled" } else { "end_turn" }}});
                state.push(response.clone());
                return Json(response).into_response();
            }
            {
                let mut counts = state.counts.lock();
                counts.0 += 1;
                counts.1 += 1;
            }
            *state.prompt_id.lock() = Some(value["id"].clone());
            state.prompt.notify_one();
            if state.mode != "post-first" {
                state.output();
            }
            if state.mode == "sse-first" {
                state.release_post.notified().await;
            }
            return Json(
                json!({"jsonrpc":"2.0","id":value["id"],"result":{"stopReason":"end_turn"}}),
            )
            .into_response();
        }
        "session/cancel" => {
            state.cancel_received.notify_one();
            if state.mode.starts_with("cancel-") {
                state.deliver_cancel.notified().await;
                let mut order = state.order.lock();
                let event = if order.contains(&"prompt") {
                    "cancel"
                } else {
                    "cancel-before-prompt"
                };
                order.push(event);
            }
            return StatusCode::ACCEPTED.into_response();
        }
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    let response = json!({"jsonrpc":"2.0","id":value["id"],"result":response});
    state.push(response.clone());
    Json(response).into_response()
}

async fn get_events(State(state): State<Arc<HttpPeer>>, headers: HeaderMap) -> Response {
    if headers.get("authorization").and_then(|h| h.to_str().ok())
        != Some("Bearer host-only-sentinel")
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    assert_eq!(headers["last-event-id"], "0");
    Sse::new(async_stream::stream! {
        let mut index = 0;
        loop {
            let value = state.frames.lock().get(index).cloned();
            if let Some(value) = value {
                if index >= 2 && state.mode == "disconnect" { break; }
                let id = index + 1 + usize::from(index >= 2 && state.mode == "gap");
                index += 1;
                let data = if index == 3 && state.mode == "malformed" { "{broken".to_string() } else { value.to_string() };
                yield Ok::<_, Infallible>(SseEvent::default().event("message").id(id.to_string()).data(data));
            } else { state.notify.notified().await; }
        }
    }).into_response()
}

async fn start(mode: &'static str) -> (Client, Arc<HttpPeer>, tokio::task::JoinHandle<()>) {
    let state = Arc::new(HttpPeer {
        mode,
        frames: Mutex::new(Vec::new()),
        notify: Notify::new(),
        prompt: Notify::new(),
        release_post: Notify::new(),
        admit: Notify::new(),
        cancel_received: Notify::new(),
        deliver_cancel: Notify::new(),
        order: Mutex::new(Vec::new()),
        prompt_id: Mutex::new(None),
        counts: Mutex::new((0, 0, 0)),
    });
    let router = Router::new()
        .route(
            "/v1/acp/test",
            post(post_rpc).get(get_events).delete(
                |State(state): State<Arc<HttpPeer>>| async move {
                    state.counts.lock().2 += 1;
                    StatusCode::NO_CONTENT
                },
            ),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url: reqwest::Url = format!(
        "http://{}/v1/acp/test?agent=fixture",
        listener.local_addr().unwrap()
    )
    .parse()
    .unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    assert!(
        Client::connect(RivetTransport::new(reqwest::Client::new(), url.clone()))
            .await
            .is_err()
    );
    let mut headers = reqwest::header::HeaderMap::new();
    let mut auth = reqwest::header::HeaderValue::from_static("Bearer host-only-sentinel");
    auth.set_sensitive(true);
    headers.insert(reqwest::header::AUTHORIZATION, auth);
    let http = reqwest::Client::builder()
        .default_headers(headers)
        .retry(reqwest::retry::never())
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let client = Client::connect(RivetTransport::new(http, url))
        .await
        .unwrap();
    (client, state, server)
}

#[tokio::test]
async fn post_and_sse_duplicates_have_one_ordered_result_in_either_arrival_order() {
    for mode in ["post-first", "sse-first"] {
        let (client, peer, server) = start(mode).await;
        let session = client.new_session("/tmp").await.unwrap();
        let mut turn = session.prompt_text("execute once").unwrap();
        peer.prompt.notified().await;
        if mode == "post-first" {
            assert!(
                tokio::time::timeout(Duration::from_millis(50), turn.next())
                    .await
                    .is_err(),
                "POST completion must not bypass SSE updates"
            );
            peer.output();
        }
        let mut text = Vec::new();
        let mut completed = 0;
        while let Some(event) = tokio::time::timeout(Duration::from_secs(5), turn.next())
            .await
            .unwrap()
        {
            match event.unwrap() {
                Event::Update(n) => {
                    if let SessionUpdate::AgentMessageChunk(c) = n.update
                        && let ContentBlock::Text(t) = c.content
                    {
                        text.push(t.text);
                    }
                }
                Event::Completed(r) => {
                    assert_eq!(r.stop_reason, StopReason::EndTurn);
                    completed += 1;
                }
                _ => panic!("unexpected permission"),
            }
        }
        peer.release_post.notify_one();
        assert_eq!(text, ["first λ", " then second"]);
        assert_eq!(completed, 1);
        drop(turn);
        drop(session);
        drop(client);
        assert_eq!(*peer.counts.lock(), (1, 1, 0));
        server.abort();
    }
}

#[tokio::test]
async fn gap_malformed_and_disconnect_fail_without_replay_or_duplicate_effect() {
    for mode in ["gap", "malformed", "disconnect"] {
        let (client, peer, server) = start(mode).await;
        let session = client.new_session("/tmp").await.unwrap();
        let mut turn = session.prompt_text("execute once").unwrap();
        let mut failed = false;
        while let Some(event) = tokio::time::timeout(Duration::from_secs(5), turn.next())
            .await
            .unwrap()
        {
            assert!(!matches!(event, Ok(Event::Completed(_))));
            failed |= event.is_err();
        }
        assert!(failed);
        assert_eq!(*peer.counts.lock(), (1, 1, 0));
        server.abort();
    }
}

#[tokio::test]
async fn cancellation_waits_for_admission_and_fences_the_next_turn() {
    for mode in ["cancel-early", "cancel-late"] {
        let (client, peer, server) = start(mode).await;
        let session = client.new_session("/tmp").await.unwrap();
        let mut turn = session.prompt_text("effect").unwrap();
        if mode == "cancel-early" {
            session.cancel().unwrap();
            peer.prompt.notified().await;
            let Event::Update(update) = tokio::time::timeout(Duration::from_secs(5), turn.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
            else {
                panic!("metadata");
            };
            assert!(matches!(
                update.update,
                SessionUpdate::AvailableCommandsUpdate(_)
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(50), peer.cancel_received.notified())
                    .await
                    .is_err()
            );
            peer.admit.notify_one();
        } else {
            assert!(matches!(
                turn.next().await.unwrap().unwrap(),
                Event::Update(_)
            ));
            session.cancel().unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), peer.cancel_received.notified())
            .await
            .unwrap();
        if mode == "cancel-late" {
            // Natural completion overtakes a cancellation that has not reached stdin.
            peer.release_post.notify_one();
            assert!(
                tokio::time::timeout(Duration::from_millis(50), turn.next())
                    .await
                    .is_err()
            );
            assert!(matches!(
                session.prompt_text("next"),
                Err(rig_acp::Error::Busy)
            ));
        }
        peer.deliver_cancel.notify_one();
        if mode == "cancel-early" {
            assert!(matches!(
                turn.next().await.unwrap().unwrap(),
                Event::Update(_)
            ));
            peer.release_post.notify_one();
        }
        let Event::Completed(response) = tokio::time::timeout(Duration::from_secs(5), turn.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        else {
            panic!("terminal response");
        };
        assert_eq!(
            response.stop_reason,
            if mode == "cancel-early" {
                StopReason::Cancelled
            } else {
                StopReason::EndTurn
            }
        );
        let mut next = session.prompt_text("next").unwrap();
        while let Some(event) = tokio::time::timeout(Duration::from_secs(5), next.next())
            .await
            .unwrap()
        {
            event.unwrap();
        }
        assert_eq!(*peer.order.lock(), ["prompt", "cancel", "next"]);
        assert_eq!(
            *peer.counts.lock(),
            (2, if mode == "cancel-early" { 1 } else { 2 }, 0)
        );
        server.abort();
    }
    let (client, peer, server) = start("cancel-silent").await;
    let session = client.new_session("/tmp").await.unwrap();
    let mut turn = session.prompt_text("silent effect").unwrap();
    session.cancel().unwrap();
    peer.prompt.notified().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), peer.cancel_received.notified())
            .await
            .is_err()
    );
    // Without output the adapter cannot establish admission. Natural completion
    // discards deferred cancellation; it must not reach the following turn.
    peer.release_post.notify_one();
    let Event::Completed(response) = tokio::time::timeout(Duration::from_secs(5), turn.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    else {
        panic!("completion");
    };
    assert_eq!(response.stop_reason, StopReason::EndTurn);
    let mut next = session.prompt_text("next").unwrap();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), next.next())
        .await
        .unwrap()
    {
        event.unwrap();
    }
    assert_eq!(*peer.order.lock(), ["prompt", "next"]);
    assert_eq!(*peer.counts.lock(), (2, 2, 0));
    server.abort();
}
