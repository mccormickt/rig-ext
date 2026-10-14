use std::{collections::HashMap, path::PathBuf, sync::Arc};

use agent_client_protocol::{Agent, ConnectTo, ConnectionTo, Responder};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot, watch};

use crate::schema::*;

/// A direct client or projection error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Protocol(#[from] agent_client_protocol::Error),
    #[error("ACP connection closed")]
    Closed,
    #[error("a prompt is already active in this session")]
    Busy,
    #[error("permission option was not offered by the agent")]
    InvalidPermission,
    #[error("ACP turn exceeded the 8 MiB protocol event budget")]
    EventLimit,
    #[error("unsupported ACP projection: {0}")]
    Unsupported(&'static str),
}

/// Original protocol events, separate from any Rig text projection.
#[derive(Debug)]
pub enum Event {
    Update(SessionNotification),
    Permission(Permission),
    Completed(PromptResponse),
}

/// A pending agent-to-client permission request. Drop denies authorization.
#[derive(Debug)]
pub struct Permission {
    request: RequestPermissionRequest,
    reply: oneshot::Sender<RequestPermissionResponse>,
    cancelled: watch::Receiver<bool>,
}

impl Permission {
    pub fn request(&self) -> &RequestPermissionRequest {
        &self.request
    }

    pub(crate) async fn cancelled(&mut self) {
        if !*self.cancelled.borrow() {
            let _ = self.cancelled.changed().await;
        }
    }

    /// Select an offered option or cancel the permission request.
    pub fn respond(self, outcome: RequestPermissionOutcome) -> Result<(), Error> {
        if let RequestPermissionOutcome::Selected(selected) = &outcome
            && !self
                .request
                .options
                .iter()
                .any(|o| o.option_id == selected.option_id)
        {
            return Err(Error::InvalidPermission);
        }
        self.reply
            .send(RequestPermissionResponse::new(outcome))
            .map_err(|_| Error::Closed)
    }
}

struct Active {
    events: mpsc::UnboundedSender<Result<Event, Error>>,
    cancelled: watch::Sender<bool>,
    bytes: usize,
    failed: bool,
}

impl Active {
    fn reserve(
        &mut self,
        value: &impl serde::Serialize,
        cancel: Option<(&ConnectionTo<Agent>, &SessionId)>,
    ) -> bool {
        if self.failed {
            return false;
        }
        struct Budget<'a>(&'a mut usize);
        impl std::io::Write for Budget<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                *self.0 = self.0.saturating_add(bytes.len());
                if *self.0 > 8 * 1024 * 1024 {
                    return Err(std::io::Error::other("event budget exceeded"));
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        if serde_json::to_writer(Budget(&mut self.bytes), value).is_err() {
            self.failed = true;
            let _ = self.events.send(Err(Error::EventLimit));
            self.cancelled.send_replace(true);
            if let Some((cx, id)) = cancel {
                let _ = cx.send_notification(CancelNotification::new(id.clone()));
            }
            false
        } else {
            true
        }
    }
}

type ActiveTurns = Arc<Mutex<HashMap<SessionId, Active>>>;

struct Inner {
    connection: ConnectionTo<Agent>,
    active: ActiveTurns,
    _task: AbortOnDrop,
    initialized: InitializeResponse,
}

struct AbortOnDrop(tokio::task::AbortHandle);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A live connection. Clones share the same connection, not a new agent process.
#[derive(Clone)]
pub struct Client(Arc<Inner>);

impl Client {
    /// Connect and negotiate ACP v1 on the current Tokio runtime.
    /// No client filesystem or terminal capabilities are advertised.
    pub async fn connect(
        transport: impl ConnectTo<agent_client_protocol::Client>,
    ) -> Result<Self, Error> {
        let active: ActiveTurns = Arc::default();
        let updates = active.clone();
        let permissions = active.clone();
        let cleanup = active.clone();
        let (ready, wait) = oneshot::channel();
        let task = tokio::spawn(async move {
            let result = agent_client_protocol::Client.builder()
                .on_receive_notification(
                    async move |notification: SessionNotification, cx: ConnectionTo<Agent>| {
                        let mut turns = updates.lock();
                        if let Some(turn) = turns.get_mut(&notification.session_id)
                            && turn.reserve(&notification, Some((&cx, &notification.session_id))) {
                                let _ = turn.events.send(Ok(Event::Update(notification)));
                            }
                        Ok(())
                    },
                    agent_client_protocol::on_receive_notification!(),
                )
                .on_receive_request(
                    async move |request: RequestPermissionRequest,
                                responder: Responder<RequestPermissionResponse>,
                                cx: ConnectionTo<Agent>| {
                        let pending = {
                            let mut turns = permissions.lock();
                            turns.get_mut(&request.session_id).and_then(|turn| {
                                if !turn.reserve(&request, Some((&cx, &request.session_id))) || *turn.cancelled.borrow() {
                                    return None;
                                }
                                let (reply, answer) = oneshot::channel();
                                let _ = turn.events.send(Ok(Event::Permission(Permission { request: request.clone(), reply, cancelled: turn.cancelled.subscribe() })));
                                Some((answer, turn.cancelled.subscribe()))
                            })
                        };
                        if let Some((answer, mut cancelled)) = pending {
                            let active = permissions.clone();
                            // Waiting outside dispatch keeps callbacks and updates serviceable.
                            cx.spawn(async move {
                                let mut response = tokio::select! {
                                    biased;
                                    _ = cancelled.changed() => RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled),
                                    response = answer => response.unwrap_or_else(|_| RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled)),
                                };
                                // Order authorization against cancellation and prompt settlement.
                                let _turns = active.lock();
                                if *cancelled.borrow() || cancelled.has_changed().is_err() {
                                    response = RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled);
                                }
                                responder.respond(response)
                            })
                        } else {
                            responder.respond(RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled))
                        }
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .connect_with(transport, async move |cx: ConnectionTo<Agent>| {
                    let initialized = cx.send_request(InitializeRequest::new(
                        agent_client_protocol::schema::ProtocolVersion::V1,
                    )).block_task().await?;
                    if initialized.protocol_version != agent_client_protocol::schema::ProtocolVersion::V1 {
                        return Err(agent_client_protocol::Error::invalid_params().data("ACP v1 is required"));
                    }
                    let _ = ready.send((cx.clone(), initialized));
                    cx.incoming_closed().await;
                    Ok(())
                }).await;
            for (_, turn) in cleanup.lock().drain() {
                let error = result
                    .as_ref()
                    .err()
                    .cloned()
                    .map_or(Error::Closed, Error::Protocol);
                let _ = turn.events.send(Err(error));
            }
            result
        });
        let guard = AbortOnDrop(task.abort_handle());
        match wait.await {
            Ok((connection, initialized)) => Ok(Self(Arc::new(Inner {
                connection,
                active,
                _task: guard,
                initialized,
            }))),
            Err(_) => match task.await {
                Ok(Err(error)) => Err(error.into()),
                _ => Err(Error::Closed),
            },
        }
    }

    pub fn initialized(&self) -> &InitializeResponse {
        &self.0.initialized
    }

    /// Open a remote session. `cwd` is an absolute path on the agent host.
    pub async fn new_session(&self, cwd: impl Into<PathBuf>) -> Result<Session, Error> {
        let response = self
            .0
            .connection
            .send_request(NewSessionRequest::new(cwd.into()))
            .block_task()
            .await?;
        Ok(Session {
            client: self.clone(),
            opened: response,
        })
    }
}

/// An explicit remote session binding. Clones share its single active turn.
#[derive(Clone)]
pub struct Session {
    client: Client,
    opened: NewSessionResponse,
}

impl Session {
    pub fn id(&self) -> &SessionId {
        &self.opened.session_id
    }

    pub fn opened(&self) -> &NewSessionResponse {
        &self.opened
    }

    /// Submit content without blocking event or permission dispatch.
    /// Concurrent prompts in the same session fail with `Busy`.
    pub fn prompt(&self, content: Vec<ContentBlock>) -> Result<Turn, Error> {
        let capabilities = &self
            .client
            .initialized()
            .agent_capabilities
            .prompt_capabilities;
        for block in &content {
            let supported = match block {
                ContentBlock::Text(_) | ContentBlock::ResourceLink(_) => true,
                ContentBlock::Image(_) => capabilities.image,
                ContentBlock::Audio(_) => capabilities.audio,
                ContentBlock::Resource(_) => capabilities.embedded_context,
                _ => false,
            };
            if !supported {
                return Err(Error::Unsupported(
                    "agent did not advertise this prompt content capability",
                ));
            }
        }
        let (events, receiver) = mpsc::unbounded_channel();
        let (cancelled, _) = watch::channel(false);
        let mut turns = self.client.0.active.lock();
        if self.client.0.connection.is_incoming_closed() {
            return Err(Error::Closed);
        }
        if turns.contains_key(self.id()) {
            return Err(Error::Busy);
        }
        turns.insert(
            self.id().clone(),
            Active {
                events,
                cancelled,
                bytes: 0,
                failed: false,
            },
        );
        let active = self.client.0.active.clone();
        let id = self.id().clone();
        let sent = self
            .client
            .0
            .connection
            .prepare_request(PromptRequest::new(id.clone(), content))
            .on_receiving_result(async move |result| {
                if let Some(mut turn) = active.lock().remove(&id) {
                    let fits = match &result {
                        Ok(response) => turn.reserve(response, None),
                        Err(error) => turn.reserve(error, None),
                    };
                    if fits {
                        let _ = turn
                            .events
                            .send(result.map(Event::Completed).map_err(Error::Protocol));
                    }
                }
                Ok(())
            });
        if let Err(error) = sent {
            turns.remove(self.id());
            return Err(error.into());
        }
        Ok(Turn {
            receiver,
            session: self.clone(),
        })
    }

    pub fn prompt_text(&self, text: impl Into<String>) -> Result<Turn, Error> {
        self.prompt(vec![ContentBlock::Text(TextContent::new(text))])
    }

    /// Request cancellation, then continue to read the prompt's final response.
    /// This does not destroy the server, process, or sandbox.
    pub fn cancel(&self) -> Result<(), Error> {
        let turns = self.client.0.active.lock();
        if let Some(turn) = turns.get(self.id()) {
            turn.cancelled.send_replace(true);
            self.client
                .0
                .connection
                .send_notification(CancelNotification::new(self.id().clone()))?;
        }
        Ok(())
    }
}

/// Ordered events for one prompt. Dropping this receiver does not cancel work.
pub struct Turn {
    receiver: mpsc::UnboundedReceiver<Result<Event, Error>>,
    session: Session,
}

impl Turn {
    pub async fn next(&mut self) -> Option<Result<Event, Error>> {
        self.receiver.recv().await
    }

    pub fn session(&self) -> &Session {
        &self.session
    }
}
