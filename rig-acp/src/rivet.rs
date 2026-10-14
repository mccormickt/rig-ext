//! Rivet Sandbox Agent 0.5.3 POST/SSE transport.
//!
//! Use a fresh, dedicated `/v1/acp/{server_id}?agent=...` URL. The first POST
//! creates the server process. This adapter never sends DELETE, reconnects,
//! retries a prompt, or claims lossless replay. The host owns server cleanup.

use agent_client_protocol::{
    Channel, Client, ConnectTo, Error, RawJsonRpcMessage, RawJsonRpcParams, Result, TransportFrame,
};
use eventsource_stream::Eventsource;
use futures::{StreamExt, stream::FuturesUnordered};
use std::collections::HashMap;

struct PendingPrompt {
    id: crate::schema::RequestId,
    admitted: bool,
    cancel: Option<TransportFrame>,
}

fn session(params: &Option<RawJsonRpcParams>) -> Option<&str> {
    match params {
        Some(RawJsonRpcParams::Object(params)) => params.get("sessionId")?.as_str(),
        _ => None,
    }
}

/// Host-owned HTTP configuration. Credentials belong in the reqwest client,
/// not in Rig prompts, tool arguments, or the endpoint query.
pub struct RivetTransport {
    http: reqwest::Client,
    endpoint: reqwest::Url,
}

impl RivetTransport {
    pub fn new(http: reqwest::Client, endpoint: reqwest::Url) -> Self {
        Self { http, endpoint }
    }

    async fn post(&self, frame: TransportFrame) -> Result<()> {
        if !matches!(frame, TransportFrame::Single(_)) {
            return Err(failure("Rivet requires a single JSON-RPC object"));
        }
        let request = matches!(
            &frame,
            TransportFrame::Single(RawJsonRpcMessage::Request(_))
        );
        let response = self
            .http
            .post(self.endpoint.clone())
            .header("content-type", "application/json")
            .body(frame.to_json()?)
            .send()
            .await
            .map_err(|_| failure("Rivet POST failed; delivery may be ambiguous"))?;
        let expected = if request { 200 } else { 202 };
        if response.status().as_u16() != expected {
            return Err(failure(
                "unexpected Rivet POST status; delivery may be ambiguous",
            ));
        }
        // SSE is the sole ordered inbound path. POST carries duplicate responses.
        let mut body = response.bytes_stream();
        let mut size = 0usize;
        while let Some(chunk) = body.next().await {
            size = size.saturating_add(chunk.map_err(|_| failure("Rivet POST body failed"))?.len());
            if size > 8 * 1024 * 1024 {
                return Err(failure("Rivet POST body exceeds 8 MiB"));
            }
        }
        Ok(())
    }

    async fn run(self, mut channel: Channel) -> Result<()> {
        let first = channel
            .rx
            .next()
            .await
            .ok_or_else(|| failure("missing initialize"))?;
        match &first {
            TransportFrame::Single(RawJsonRpcMessage::Request(request))
                if request.method.as_ref() == "initialize" => {}
            _ => return Err(failure("Rivet bootstrap must be initialize")),
        }
        self.post(first).await?;
        let response = self
            .http
            .get(self.endpoint.clone())
            .header("accept", "text/event-stream")
            .header("last-event-id", "0")
            .send()
            .await
            .map_err(|_| failure("Rivet SSE connect failed"))?;
        if response.status().as_u16() != 200
            || !response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("text/event-stream"))
        {
            return Err(failure("Rivet SSE endpoint rejected the connection"));
        }
        let events = response.bytes_stream().eventsource();
        futures::pin_mut!(events);
        let mut posts = FuturesUnordered::new();
        let mut sequence = 0u64;
        let mut prompts: HashMap<String, PendingPrompt> = HashMap::new();
        loop {
            tokio::select! {
                frame = channel.rx.next() => match frame {
                    Some(frame) => {
                        match &frame {
                            TransportFrame::Single(RawJsonRpcMessage::Request(request)) if request.method.as_ref() == "session/prompt" => {
                                let session = session(&request.params).ok_or_else(|| failure("prompt lacks sessionId"))?;
                                prompts.insert(session.to_owned(), PendingPrompt { id: request.id.clone(), admitted: false, cancel: None });
                            }
                            TransportFrame::Single(RawJsonRpcMessage::Notification(notification)) if notification.method.as_ref() == "session/cancel" => {
                                if let Some(prompt) = session(&notification.params).and_then(|id| prompts.get_mut(id)) {
                                    if prompt.admitted {
                                        // HTTP 202 acknowledges the stdin write. Fence all later
                                        // frames until it arrives so cancellation cannot hit a new turn.
                                        self.post(frame).await?;
                                    } else {
                                        // A pending POST has no admission acknowledgment. Wait for
                                        // session output; a terminal response makes this cancel moot.
                                        prompt.cancel = Some(frame);
                                    }
                                }
                                continue;
                            }
                            _ => {}
                        }
                        if posts.len() >= 128 { return Err(failure("too many pending Rivet POSTs")); }
                        posts.push(self.post(frame));
                    }
                    None => return Ok(()),
                },
                result = posts.next(), if !posts.is_empty() => {
                    if let Some(result) = result { result?; }
                }
                event = events.next() => {
                    let event = event.ok_or_else(|| failure("Rivet SSE disconnected; delivery may be ambiguous"))?
                        .map_err(|_| failure("Rivet SSE failed; delivery may be ambiguous"))?;
                    if event.event != "message" { continue; }
                    let id: u64 = event.id.parse().map_err(|_| failure("Rivet SSE lacks a numeric event ID"))?;
                    if id != sequence + 1 { return Err(failure("Rivet SSE sequence gap or duplicate; no automatic replay")); }
                    sequence = id;
                    if event.data.len() > 8 * 1024 * 1024 { return Err(failure("Rivet SSE event exceeds 8 MiB")); }
                    let frame = TransportFrame::parse_json(&event.data);
                    if !matches!(frame, TransportFrame::Single(_)) { return Err(failure("invalid Rivet JSON-RPC envelope")); }
                    let admitted = match &frame {
                        TransportFrame::Single(RawJsonRpcMessage::Notification(n)) if n.method.as_ref() == "session/update" => {
                            // Session metadata can arrive without prompt execution.
                            let activity = match &n.params {
                                Some(RawJsonRpcParams::Object(p)) => p.get("update")
                                    .and_then(|u| u.get("sessionUpdate")).and_then(|u| u.as_str()),
                                _ => None,
                            };
                            match activity {
                                Some("agent_message_chunk" | "agent_thought_chunk" | "tool_call" | "tool_call_update" | "plan") => session(&n.params),
                                _ => None,
                            }
                        }
                        TransportFrame::Single(RawJsonRpcMessage::Request(r)) if r.method.as_ref() == "session/request_permission" => session(&r.params),
                        TransportFrame::Single(RawJsonRpcMessage::Response(r)) => {
                            let (agent_client_protocol::RawJsonRpcResponse::Result { id, .. } | agent_client_protocol::RawJsonRpcResponse::Error { id, .. }) = r;
                            prompts.retain(|_, prompt| &prompt.id != id);
                            None
                        }
                        _ => None,
                    };
                    if let Some(prompt) = admitted.and_then(|id| prompts.get_mut(id)) {
                        prompt.admitted = true;
                        if let Some(cancel) = prompt.cancel.take() {
                            self.post(cancel).await?;
                        }
                    }
                    channel.tx.unbounded_send(frame).map_err(|_| failure("ACP receiver closed"))?;
                }
            }
        }
    }
}

impl ConnectTo<Client> for RivetTransport {
    async fn connect_to(self, client: impl ConnectTo<agent_client_protocol::Agent>) -> Result<()> {
        let (sdk, transport) = Channel::duplex();
        futures::try_join!(client.connect_to(sdk), self.run(transport))?;
        Ok(())
    }
}

fn failure(message: &str) -> Error {
    Error::internal_error().data(message)
}
