// Rig's model wire requires ProviderError and EncodeError.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use futures::future::BoxFuture;
use rig_agent::agent::{AgentBuilder, NoToolConfig};
use rig_core::{
    completion::{CompletionRequest, CompletionResponse, FinishReason, Usage},
    driver::{Exchange, Model, Opened, Opening, Transport},
    error::{EncodeError, ProviderError},
    message::{Message, ToolChoice, UserContent},
    operation::{Block, Completion, Finish},
    serve::{Dispatch, Reply, Serve, adapters::ModelAdapter},
    streaming::Streamed,
    wire::{Decoder, Descriptor, Flow, Mode, Out, Wire, WireEvent},
};

use crate::{Error, Event, Session, schema::*};

type Policy = Arc<
    dyn Fn(RequestPermissionRequest) -> BoxFuture<'static, RequestPermissionOutcome> + Send + Sync,
>;

/// A text projection bound to exactly one live ACP session.
///
/// Use one model per remote conversation. Rig memory keys do not select remote
/// sessions. Only the newest user message is sent; the remote retains history.
#[derive(Clone)]
pub struct SessionModel {
    session: Session,
    policy: Policy,
}

impl std::fmt::Debug for SessionModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionModel").finish_non_exhaustive()
    }
}

impl rig_core::completion::ReplayTarget for SessionModel {
    fn api(&self) -> rig_core::message::Api {
        "acp".to_string().into()
    }
    fn provider(&self) -> &str {
        "acp"
    }
    fn model(&self) -> &str {
        "remote-session"
    }
    fn accepts(&self, _: &str) -> rig_core::completion::Accepts {
        rig_core::completion::Accepts {
            tools: false,
            ..rig_core::completion::Accepts::ALL
        }
    }
    fn takes_documents(&self) -> bool {
        true
    }
    fn map_options(
        &self,
        _: &CompletionRequest,
        fields: rig_core::completion::options::OptionFields<'_>,
    ) -> rig_core::completion::options::OptionMap {
        use rig_core::completion::options::{Mapping, OptionFields, OptionMap};
        let OptionFields {
            reasoning,
            cache,
            service_tier,
            verbosity,
            parallel_tool_calls,
            top_p,
            seed,
            stop,
        } = fields;
        let unsupported =
            || Mapping::unsupported("the remote ACP agent owns its generation settings");
        OptionMap {
            reasoning: Mapping::of(reasoning, |_| unsupported()),
            cache: Mapping::of(cache, |_| unsupported()),
            service_tier: Mapping::of(service_tier, |_| unsupported()),
            verbosity: Mapping::of(verbosity, |_| unsupported()),
            parallel_tool_calls: Mapping::of(parallel_tool_calls, |_| unsupported()),
            top_p: Mapping::of(top_p, |_| unsupported()),
            seed: Mapping::of(seed, |_| unsupported()),
            stop: Mapping::of_stop(stop, |_| unsupported()),
        }
    }
}

impl Session {
    pub fn model(&self) -> SessionModel {
        SessionModel {
            session: self.clone(),
            policy: Arc::new(|_| Box::pin(async { RequestPermissionOutcome::Cancelled })),
        }
    }
}

impl SessionModel {
    /// Set an asynchronous permission policy. The default cancels each request.
    pub fn permission_policy<F, Fut>(mut self, policy: F) -> Self
    where
        F: Fn(RequestPermissionRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = RequestPermissionOutcome> + Send + 'static,
    {
        self.policy = Arc::new(move |request| Box::pin(policy(request)));
        self
    }

    fn driver(&self) -> Model<Self, AcpTransport> {
        Model::new(self.clone(), AcpTransport)
    }

    /// Build a Rig agent with an explicit local memory key. This key does not
    /// change the bound remote session, including when used as a sub-agent.
    pub fn agent(&self, conversation: impl Into<String>) -> AgentBuilder<NoToolConfig> {
        AgentBuilder::new(self.driver())
            .name("acp_delegate")
            .description("Delegate a text task to the bound remote ACP agent.")
            .conversation(conversation.into())
            .model_route_handler("default", self.handler("default"))
    }

    /// Register this handler with Rig's `model_route_handler` for named routes.
    pub fn handler(&self, label: impl Into<rig_agent::agent::ModelRef>) -> AcpHandler {
        AcpHandler {
            adapter: ModelAdapter::new(label, self.driver()),
        }
    }

    pub async fn completion(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse, ProviderError> {
        self.driver().call(Self::prepare(request)?).await
    }

    pub fn stream(
        &self,
        request: CompletionRequest,
    ) -> Result<Streamed<Completion>, ProviderError> {
        self.driver().stream(Self::prepare(request)?)
    }

    fn prepare(mut request: CompletionRequest) -> Result<CompletionRequest, ProviderError> {
        request.validate_message_content()?;
        if !request.tools.is_empty()
            || matches!(
                request.tool_choice,
                Some(ToolChoice::Required | ToolChoice::Specific { .. })
            )
        {
            return Err(provider(Error::Unsupported(
                "the remote agent owns its tools",
            )));
        }
        if request.output_schema.is_some() {
            return Err(provider(Error::Unsupported("remote schema enforcement")));
        }
        if !request.documents.is_empty() {
            return Err(provider(Error::Unsupported(
                "documents; include context in the text prompt",
            )));
        }
        if let Some(Message::User { content }) = request.chat_history.last()
            && content
                .iter()
                .any(|item| !matches!(item, UserContent::Text(_)))
        {
            return Err(provider(Error::Unsupported(
                "only text input is supported by the Rig projection",
            )));
        }
        // Select the message after dispatch hooks and before replay adaptation.
        let newest = request
            .chat_history
            .pop()
            .ok_or_else(|| provider(Error::Unsupported("empty history")))?;
        request
            .chat_history
            .retain(|message| matches!(message, Message::System { .. }));
        request.chat_history.push(newest);
        Ok(request)
    }
}

#[doc(hidden)]
pub struct AcpHandler {
    adapter: ModelAdapter<Completion>,
}

impl Serve for AcpHandler {
    type Family = rig_core::effect::family::Completion;
    fn descriptor(&self) -> rig_core::effect::HandlerDescriptor {
        self.adapter.descriptor()
    }
    async fn serve(&self, kind: rig_core::effect::EffectKind, dispatch: Dispatch) -> Reply {
        let kind = match kind {
            rig_core::effect::EffectKind::Completion { request, stream } => {
                match SessionModel::prepare(request) {
                    Ok(request) => rig_core::effect::EffectKind::Completion { request, stream },
                    Err(error) => return Reply::Outcome(Err(error.into())),
                }
            }
            other => other,
        };
        self.adapter.serve(kind, dispatch).await
    }
}

#[doc(hidden)]
pub struct Payload {
    model: SessionModel,
    content: Vec<ContentBlock>,
}

/// Protocol records retained in Rig response extras; not model-visible text.
#[derive(Clone, serde::Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Frame {
    Update(SessionNotification),
    Permission(RequestPermissionRequest),
    Decision(RequestPermissionResponse),
    Completed(PromptResponse),
}

#[doc(hidden)]
#[derive(Default)]
pub struct Document(Vec<serde_json::Value>);
impl rig_core::wire::document::Serves<Completion> for Document {}
impl rig_core::wire::document::Reassemble<Frame> for Document {
    fn absorb(&mut self, frame: &Frame) {
        if let Ok(value) = serde_json::to_value(frame) {
            self.0.push(value);
        }
    }
    fn finish(self) -> serde_json::Value {
        self.0.into()
    }
}

#[derive(Clone)]
struct AcpTransport;
impl Transport<SessionModel> for AcpTransport {
    fn send(&self, payload: Payload, _: Exchange) -> Opening<Frame> {
        Opening::new(async move {
            let mut turn = payload
                .model
                .session
                .prompt(payload.content)
                .map_err(provider)?;
            Ok(Opened::new(async_stream::try_stream! {
                let mut completed = false;
                while let Some(event) = turn.next().await {
                    match event.map_err(provider)? {
                        Event::Update(update) => yield Frame::Update(update),
                        Event::Permission(mut permission) => {
                            let request = permission.request().clone();
                            yield Frame::Permission(request.clone());
                            let answer = tokio::select! {
                                biased;
                                _ = permission.cancelled() => None,
                                outcome = (payload.model.policy)(request) => Some(outcome),
                            };
                            let outcome = if let Some(outcome) = answer {
                                permission.respond(outcome.clone()).map_err(provider)?;
                                outcome
                            } else {
                                drop(permission);
                                RequestPermissionOutcome::Cancelled
                            };
                            yield Frame::Decision(RequestPermissionResponse::new(outcome));
                        }
                        Event::Completed(response) => {
                            completed = true;
                            yield Frame::Completed(response);
                        }
                    }
                }
                if !completed { Err(ProviderError::Truncated)?; }
            }))
        })
    }
}

impl Wire for SessionModel {
    type Op = Completion;
    type Payload = Payload;
    type Frame = Frame;
    type Decoder<'id> = AcpDecoder<'id>;
    type Reassembler = Document;
    fn describe(&self) -> Descriptor<'_> {
        Descriptor::new("acp").model("remote-session").replay(self)
    }
    fn encode(&self, request: CompletionRequest, _: Mode) -> Result<Payload, EncodeError> {
        let request = Self::prepare(request).map_err(EncodeError::request)?;
        if !request.documents.is_empty() {
            return Err(EncodeError::request(Error::Unsupported(
                "documents; include context in the text prompt",
            )));
        }
        let Some(Message::User { content }) = request.chat_history.last() else {
            return Err(EncodeError::request(Error::Unsupported(
                "prompt must end in a user message",
            )));
        };
        let mut prompt = Vec::new();
        for message in &request.chat_history {
            if let Message::System { content } = message {
                prompt.push(ContentBlock::Text(TextContent::new(content.clone())));
            }
        }
        let content = content
            .iter()
            .map(|item| match item {
                UserContent::Text(text) => {
                    Ok(ContentBlock::Text(TextContent::new(text.text.clone())))
                }
                _ => Err(EncodeError::request(Error::Unsupported(
                    "only text input is supported by the Rig projection",
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        prompt.extend(content);
        Ok(Payload {
            model: self.clone(),
            content: prompt,
        })
    }
    fn decoder<'id>(&self) -> AcpDecoder<'id> {
        AcpDecoder(std::marker::PhantomData)
    }
}

#[doc(hidden)]
pub struct AcpDecoder<'id>(std::marker::PhantomData<&'id ()>);
impl<'id> Decoder<'id, Completion, Frame> for AcpDecoder<'id> {
    type Event = Frame;
    fn classify(&self, frame: Frame) -> WireEvent<Frame> {
        WireEvent::Known(frame)
    }
    fn decode(
        &mut self,
        event: Frame,
        mut out: Out<'id, Completion>,
    ) -> Result<Flow, ProviderError> {
        match event {
            Frame::Update(SessionNotification {
                update: SessionUpdate::AgentMessageChunk(chunk),
                ..
            }) => {
                if let ContentBlock::Text(text) = chunk.content {
                    out.run(Block::Text, &text.text)?;
                }
            }
            Frame::Completed(response) => {
                let reason = match response.stop_reason {
                    StopReason::EndTurn => FinishReason::Stop,
                    StopReason::MaxTokens => FinishReason::Length,
                    other => {
                        return Err(ProviderError::Provider(format!(
                            "ACP prompt stopped: {other:?}"
                        )));
                    }
                };
                return Ok(out.end(Finish {
                    usage: Usage::default(),
                    reason: Some(reason),
                    response_id: None,
                    model: Some("remote-session".into()),
                    error: None,
                }));
            }
            _ => {}
        }
        Ok(Flow::More)
    }
    fn eof(&mut self, _: Out<'id, Completion>) -> Result<Flow, ProviderError> {
        Err(ProviderError::Truncated)
    }
}

fn provider(error: Error) -> ProviderError {
    ProviderError::Provider(error.to_string())
}
