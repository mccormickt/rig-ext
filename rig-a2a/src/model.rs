//! A remote A2A agent as a Rig completion model.
//!
//! [`A2AModel::agent`] lets a remote agent back a Rig [`Agent`], so `prompt`, `chat`,
//! streaming, hooks, and conversation memory all work against it, and
//! [`Agent::into_tool`] projects it as a sub-agent.
//!
//! # What A2A cannot carry
//!
//! A [`CompletionRequest`] describes a chat-completion API. A2A is a different
//! contract: it exchanges messages with an agent that owns its own instructions
//! and hides its own tools — the specification's phrasing is that agents
//! collaborate "without needing to share their internal thoughts, plans, or
//! tool implementations". Several request fields therefore have no A2A
//! equivalent, and this model refuses rather than ignoring the ones whose
//! absence would silently change the result:
//!
//! | Request field | Behavior |
//! | --- | --- |
//! | `tools` | **Error.** A remote agent never emits a tool call, so registering local tools on an A2A-backed agent would advertise tools that can never be invoked. |
//! | `output_schema` | **Error.** A2A has no remote schema-enforcement contract. Parse and validate response text locally when needed. |
//! | `tool_choice` | **Error** when it demands a tool; ignored when it merely permits one. |
//! | `preamble` | Sent as a leading text part. The remote already has its own instructions and is free to disregard it. |
//! | `documents` | Sent as one text part each, rendered by [`Document`]'s `Display`. |
//! | `additional_params` | Reads [`THREAD_PARAMS_KEY`], `configuration`, `metadata`, and `reference_task_ids`, merged over [`A2AOptions`](crate::A2AOptions). Other fields are ignored. |
//! | `temperature`, `max_tokens`, `model` | Ignored (logged at debug). Sampling belongs to the remote agent. |
//!
//! `usage` reports every counter as absent (`None`): A2A has no token
//! accounting.
//!
//! A paused task reports `FinishReason::Other("input_required")` and retains
//! the IDs needed to resume it. Rig fails closed on unknown finish reasons;
//! callers expecting clarification turns must explicitly set
//! `accept_unknown_finish_reasons(true)` on their A2A request, run or builder.
//! This adapter does not override that policy, and failed remote tasks still
//! produce provider errors even when it is enabled.
//!
//! # Conversation threading
//!
//! Three ways to thread, in increasing precedence:
//!
//! 1. **Bind the model.** [`A2AClient::model_for_conversation`] fixes one
//!    conversation for the model's lifetime.
//! 2. **Name it on a builder or run.** [`A2AConversationExt`] sets both Rig's
//!    memory key and the A2A conversation request parameter. Binding the
//!    builder also works before [`Agent::into_tool`].
//! 3. **Supply the ids.** Set [`A2AOptions`](crate::A2AOptions) context/task IDs
//!    to resume from your own storage. A task ID alone is valid. Raw identifiers
//!    under [`THREAD_PARAMS_KEY`] override typed identifiers.
//!
//! The first two look the conversation up in the client's store; the third
//! bypasses it. A model that sees none of them is single-turn.
//!
//! Threaded models assume the remote retains context and send only the newest
//! prompt; an unthreaded model renders the whole chat
//! history into the outbound message so a single-shot call still carries its
//! context.
//!
//! The preamble and the context documents get the same treatment. Rig rebuilds
//! both on every turn — a stateless provider sees only the current request —
//! but a threaded remote keeps everything sent under one `contextId`, so a
//! later turn carries only the chunks that remote has not already been given.
//! Documents are tracked individually, so per-turn retrieval still delivers
//! each newly retrieved document exactly once.
//!
//! [`Agent`]: rig_agent::agent::Agent
//! [`Agent::into_tool`]: rig_agent::agent::Agent::into_tool
//! [`Document`]: rig_core::completion::Document
//! [`A2AClient::agent`]: crate::A2AClient::agent
//! [`A2AClient::model_for_conversation`]: crate::A2AClient::model_for_conversation
//! [`A2AConversationExt`]: crate::A2AConversationExt

use std::sync::Arc;

use a2a::{Message as A2AMessage, Part, Role, SendMessageRequest, SendMessageResponse};
use a2a_client::{A2AClient as InnerClient, Transport};
use futures::StreamExt;
use rig_agent::agent::{AgentBuilder, NoToolConfig};
use rig_core::completion::{
    AssistantContent, CompletionRequest, CompletionResponse, FinishReason, Usage,
};
use rig_core::driver::{Exchange, Model, Opened, Opening};
use rig_core::error::{EncodeError, ProviderError};
use rig_core::message::{Message as RigMessage, ToolChoice, UserContent};
use rig_core::operation::{Block, Completion, Finish};
use rig_core::serve::{Dispatch, Reply, Serve, adapters::ModelAdapter};
use rig_core::streaming::Streamed;
use rig_core::wire::{Decoder, Descriptor, Flow, Mode, Out, Wire, WireEvent};

use crate::error::A2AError;
use crate::options::MessageOptions;
use crate::parts::{
    DEFAULT_TEXT_LIMIT, message_body_limited, state_label, status_text_limited, task_body_limited,
};
use crate::thread::{A2AThreadInfo, ConversationId, Thread, ThreadStore};

/// Provider name reported on responses and streams from this model.
pub const PROVIDER: &str = "a2a";

/// A remote A2A agent with direct completion calls and a Rig agent builder.
///
/// Build one with [`A2AClient::model`](crate::A2AClient::model) or
/// [`A2AClient::model_for_conversation`](crate::A2AClient::model_for_conversation).
#[derive(Clone)]
pub struct A2AModel {
    inner: Arc<InnerClient<Box<dyn Transport>>>,
    tenant: Option<String>,
    threads: ThreadStore,
    conversation: Option<ConversationId>,
    agent_name: String,
}

impl std::fmt::Debug for A2AModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("A2AModel")
            .field("agent_name", &self.agent_name)
            .field("conversation", &self.conversation)
            .finish_non_exhaustive()
    }
}

/// Register an A2A route with request preparation after application hooks.
pub trait A2AModelRouteExt: Sized {
    /// Add a remote model selected with Rig's `using_model(label)` or model
    /// selection hooks. Local tools and output schemas remain unsupported on
    /// requests routed to A2A; use a sub-agent tool for local tool orchestration.
    fn a2a_model_route(self, label: impl Into<rig_agent::agent::ModelRef>, model: A2AModel)
    -> Self;
}

impl<T> A2AModelRouteExt for AgentBuilder<T> {
    fn a2a_model_route(
        self,
        label: impl Into<rig_agent::agent::ModelRef>,
        model: A2AModel,
    ) -> Self {
        let label = label.into();
        self.model_route_handler(label.clone(), model.handler(label))
    }
}

impl A2AModel {
    pub(crate) fn new(
        inner: Arc<InnerClient<Box<dyn Transport>>>,
        tenant: Option<String>,
        threads: ThreadStore,
        conversation: Option<ConversationId>,
        agent_name: String,
    ) -> Self {
        Self {
            inner,
            tenant,
            threads,
            conversation,
            agent_name,
        }
    }

    /// The remote agent's card name.
    pub fn agent_name(&self) -> &str {
        &self.agent_name
    }

    /// The conversation this model threads into, if any.
    pub fn conversation(&self) -> Option<&ConversationId> {
        self.conversation.as_ref()
    }

    /// Bind (or rebind) this model to a conversation.
    ///
    /// Conversation state lives in the shared store on the originating
    /// [`A2AClient`](crate::A2AClient), so models using the same id continue the
    /// same remote conversation.
    pub fn with_conversation(mut self, id: impl Into<ConversationId>) -> Self {
        self.conversation = Some(id.into());
        self
    }

    fn driver(&self) -> Model<Self, A2ATransport> {
        Model::new(self.clone(), A2ATransport)
    }

    fn handler(&self, label: impl Into<rig_agent::agent::ModelRef>) -> A2AHandler {
        A2AHandler {
            model: self.clone(),
            adapter: ModelAdapter::new(label, self.driver()),
        }
    }

    /// Build a Rig agent with A2A request preparation before history adaptation.
    ///
    /// Use this builder or [`A2AClient::agent`](crate::A2AClient::agent), not a
    /// raw erased driver, to preserve the newest prompt's message boundary.
    pub fn agent(&self) -> AgentBuilder<NoToolConfig> {
        // Rig registers the default model first. Its bus supports replacing
        // a handler with one of the same family before serving any requests.
        AgentBuilder::new(self.driver())
            .name(crate::parts::agent_name(&self.agent_name))
            .model_route_handler("default", self.handler("default"))
    }

    /// Complete one request against the remote agent.
    pub async fn completion(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse, ProviderError> {
        self.driver().call(self.prepare_request(request)?).await
    }

    /// Open a streaming completion against the remote agent.
    pub fn stream(
        &self,
        request: CompletionRequest,
    ) -> Result<Streamed<Completion>, ProviderError> {
        self.driver().stream(self.prepare_request(request)?)
    }

    fn thread(
        &self,
        request: &CompletionRequest,
    ) -> Result<(Option<ConversationId>, Thread), A2AError> {
        let merged = rig_core::completion::options::param(self, request, THREAD_PARAMS_KEY)
            .map(|params| serde_json::json!({ THREAD_PARAMS_KEY: params }));
        let params = thread_params(merged.as_ref())?;
        if params.context_id.is_some() || params.task_id.is_some() {
            return Ok((
                None,
                Thread {
                    context_id: params.context_id,
                    task_id: params.task_id,
                    ..Thread::default()
                },
            ));
        }
        // A conversation named on the run outranks the model's own binding, so
        // one agent serves many conversations instead of one agent per
        // conversation. `A2AConversationExt` puts the run's id here.
        let conversation = params
            .conversation
            .or_else(|| {
                request
                    .provider_options
                    .get::<crate::conversation::Conversation>()
                    .and_then(|sections| sections.get("*"))
                    .and_then(|fields| fields.get("conversation"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .map(ConversationId::new)
            .or_else(|| self.conversation.clone());

        if conversation
            .as_ref()
            .is_some_and(|id| id.as_str().trim().is_empty())
        {
            return Err(A2AError::InvalidThreadParams {
                detail: "`conversation` must be a non-empty string",
            });
        }
        let thread = conversation
            .as_ref()
            .map(|id| self.threads.get(id))
            .unwrap_or_default();
        Ok((conversation, thread))
    }

    fn prepare_request(
        &self,
        mut request: CompletionRequest,
    ) -> Result<CompletionRequest, ProviderError> {
        request.validate_message_content()?;
        let (_, thread) = self.thread(&request)?;
        if (thread.context_id.is_some() || thread.task_id.is_some())
            && request.chat_history.len() > 1
        {
            // The remote holds the older turns. Select the original newest
            // message before Rig merges user messages across failed turns.
            let newest = request.chat_history.remove(request.chat_history.len() - 1);
            let standing = usize::from(matches!(
                request.chat_history.first(),
                Some(RigMessage::System { .. })
            ));
            request.chat_history.truncate(standing);
            request.chat_history.push(newest);
        }
        Ok(request)
    }

    /// Build the outbound A2A request for a Rig completion request.
    fn build_request(&self, request: CompletionRequest) -> Result<Outbound, A2AError> {
        reject_unsupported(&request)?;
        log_ignored(&request);
        let options = MessageOptions {
            a2a: None,
            configuration: rig_core::completion::options::param(self, &request, "configuration")
                .map(serde_json::from_value)
                .transpose()?,
            metadata: rig_core::completion::options::param(self, &request, "metadata")
                .map(serde_json::from_value)
                .transpose()?,
            reference_task_ids: rig_core::completion::options::param(
                self,
                &request,
                "reference_task_ids",
            )
            .map(serde_json::from_value)
            .transpose()?,
        };
        if options
            .configuration
            .as_ref()
            .is_some_and(|c| c.return_immediately == Some(true))
        {
            return Err(A2AError::Unsupported {
                what: "return_immediately",
                detail: "a Rig completion needs a finished or interrupted task; use the direct client API for background tasks",
            });
        }
        let (conversation, thread) = self.thread(&request)?;
        // A threaded remote already holds everything sent under this
        // `contextId`; replaying it would duplicate it on the remote side. An
        // unthreaded call has no server-side memory, so the whole request is
        // rendered into this one message.
        let threaded = thread.context_id.is_some() || thread.task_id.is_some();

        let mut parts = Vec::new();
        let mut context_fingerprints = Vec::new();

        // The preamble and the context documents are *standing* context: Rig
        // rebuilds them on every turn for a stateless provider, but a thread
        // needs each chunk only once. Documents are fingerprinted one by one
        // rather than as a block, so a turn that retrieves one new document
        // sends that document alone.
        let mut push_standing_context = |text: String| {
            if text.trim().is_empty() {
                return;
            }
            let fingerprint = fingerprint(&text);
            if threaded && thread.sent_context.contains(&fingerprint) {
                return;
            }
            context_fingerprints.push(fingerprint);
            parts.push(Part::text(text));
        };

        // The leading system message is the preamble. It is standing context,
        // so it is removed from the history that is rendered below.
        let mut history = request.chat_history.into_iter().collect::<Vec<_>>();
        if let Some(RigMessage::System { content }) = history.first() {
            push_standing_context(content.clone());
            history.remove(0);
        }
        for document in &request.documents {
            push_standing_context(document.to_string());
        }

        let messages: &[RigMessage] = if threaded {
            history.last().map(std::slice::from_ref).unwrap_or_default()
        } else {
            &history
        };
        for message in messages {
            push_message_text(&mut parts, message);
        }

        if parts.is_empty() {
            return Err(A2AError::EmptyRequest);
        }

        let mut message = A2AMessage::new(Role::User, parts);
        message.context_id = thread.context_id;
        message.task_id = thread.task_id;
        message.reference_task_ids = options.reference_task_ids;
        Ok(Outbound {
            request: SendMessageRequest {
                message,
                configuration: options.configuration,
                metadata: options
                    .metadata
                    .map(|metadata| metadata.into_iter().collect()),
                tenant: self.tenant.clone(),
            },
            context_fingerprints,
            conversation,
        })
    }

    /// Record the identifiers a response carried against the conversation this
    /// request threaded into, if any.
    fn record(&self, conversation: Option<&ConversationId>, info: &A2AThreadInfo) {
        if let Some(conversation) = conversation {
            self.threads.record(conversation, info);
        }
    }

    /// Record the standing context a delivered request carried, so later turns
    /// in the same conversation do not repeat it.
    fn record_sent_context(&self, conversation: Option<&ConversationId>, fingerprints: &[u64]) {
        if let Some(conversation) = conversation {
            self.threads.record_sent_context(conversation, fingerprints);
        }
    }
}

/// An outbound request together with the standing context it carries.
///
/// The fingerprints are committed to the conversation only once the remote has
/// answered, so a request that never landed offers its context again.
struct Outbound {
    request: SendMessageRequest,
    context_fingerprints: Vec<u64>,
    /// Conversation this request threads into, after per-run overrides.
    conversation: Option<ConversationId>,
}

/// Key under which a caller threads A2A identifiers onto one request through
/// [`CompletionRequest::additional_params`].
///
/// ```no_run
/// # use serde_json::json;
/// # async fn run(agent: rig_agent::agent::Agent, context_id: String) -> anyhow::Result<()> {
/// let reply = agent
///     .prompt("turn 2")
///     .merge_additional_params(
///         json!({ "a2a": { "context_id": context_id } })
///             .as_object()
///             .cloned()
///             .unwrap_or_default(),
///     )
///     .await?;
/// # let _ = reply;
/// # Ok(()) }
/// ```
pub const THREAD_PARAMS_KEY: &str = "a2a";

/// A2A threading a caller attached to one request.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ThreadParams {
    /// Conversation whose tracked identifiers this request continues.
    conversation: Option<String>,
    pub(crate) context_id: Option<String>,
    pub(crate) task_id: Option<String>,
}

/// Read the identifiers a caller threaded onto this request.
///
/// A caller who owns the conversation — because they persisted the ids, or
/// resumed one this process never opened — supplies them here instead of
/// binding the model to a [`ConversationId`]. Malformed directives fail the
/// request rather than being dropped: a typo that silently opened a fresh
/// remote conversation would read as the remote losing its memory.
pub(crate) fn thread_params(
    additional_params: Option<&serde_json::Value>,
) -> Result<ThreadParams, A2AError> {
    let Some(params) = additional_params.and_then(|params| params.get(THREAD_PARAMS_KEY)) else {
        return Ok(ThreadParams::default());
    };
    let Some(fields) = params.as_object() else {
        return Err(A2AError::InvalidThreadParams {
            detail: "must be an object with optional `context_id` and `task_id` string fields",
        });
    };
    if fields
        .keys()
        .any(|key| !matches!(key.as_str(), "conversation" | "context_id" | "task_id"))
    {
        return Err(A2AError::InvalidThreadParams {
            detail: "accepts only the `conversation`, `context_id` and `task_id` fields",
        });
    }

    let conversation = id_field(
        fields,
        "conversation",
        A2AError::InvalidThreadParams {
            detail: "`conversation` must be a non-empty string",
        },
    )?;
    let context_id = id_field(fields, "context_id", A2AError::InvalidContextId)?;
    let task_id = id_field(fields, "task_id", A2AError::InvalidTaskId)?;
    // The two are different claims — "look up what you tracked" versus "use
    // these ids" — and honoring one would silently ignore the other.
    if conversation.is_some() && (context_id.is_some() || task_id.is_some()) {
        return Err(A2AError::InvalidThreadParams {
            detail: "sets `conversation` together with explicit ids; supply one or the other",
        });
    }
    Ok(ThreadParams {
        conversation,
        context_id,
        task_id,
    })
}

fn id_field(
    fields: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    invalid: A2AError,
) -> Result<Option<String>, A2AError> {
    match fields.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(id)) if !id.trim().is_empty() => Ok(Some(id.clone())),
        Some(_) => Err(invalid),
    }
}

/// Identify a chunk of standing context by content.
///
/// Content rather than position or document id: Rig merges static context with
/// whatever passive or active retrieval produced this turn, and only the text
/// itself says whether the remote has seen it.
fn fingerprint(text: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

#[doc(hidden)]
pub struct A2APayload {
    outbound: Outbound,
    client: Arc<InnerClient<Box<dyn Transport>>>,
    threads: ThreadStore,
    mode: Mode,
}

#[doc(hidden)]
pub enum A2AFrame {
    Unary(SendMessageResponse, Option<ConversationId>, Vec<u64>),
    Stream(a2a::StreamResponse, Option<ConversationId>),
}

/// The remote reply as received: a unary response or an ordered array of events.
#[doc(hidden)]
#[derive(Default)]
pub struct A2ADocument {
    unary: Option<serde_json::Value>,
    events: Vec<serde_json::Value>,
}

impl rig_core::wire::document::Serves<Completion> for A2ADocument {}

impl rig_core::wire::document::Reassemble<A2AFrame> for A2ADocument {
    fn absorb(&mut self, frame: &A2AFrame) {
        match frame {
            A2AFrame::Unary(response, ..) => {
                self.unary = serde_json::to_value(response).ok();
            }
            A2AFrame::Stream(event, ..) => {
                if let Ok(event) = serde_json::to_value(event) {
                    self.events.push(event);
                }
            }
        }
    }

    fn finish(self) -> serde_json::Value {
        self.unary.unwrap_or(serde_json::Value::Array(self.events))
    }
}

#[derive(Clone, Copy)]
struct A2ATransport;

impl rig_core::driver::Transport<A2AModel> for A2ATransport {
    fn send(&self, payload: A2APayload, _exchange: Exchange) -> Opening<A2AFrame> {
        Opening::new(async move {
            let conversation = payload.outbound.conversation.clone();
            match payload.mode {
                Mode::Unary => {
                    let response = payload
                        .client
                        .send_message(&payload.outbound.request)
                        .await
                        .map_err(|e| ProviderError::from(A2AError::Protocol(e)))?;
                    let frame = A2AFrame::Unary(
                        response,
                        conversation,
                        payload.outbound.context_fingerprints,
                    );
                    Ok(Opened::new(futures::stream::once(async { Ok(frame) })))
                }
                Mode::Streaming => {
                    let upstream = payload
                        .client
                        .send_streaming_message(&payload.outbound.request)
                        .await
                        .map_err(|e| ProviderError::from(A2AError::Protocol(e)))?;
                    // The remote accepted the request, so it holds the
                    // standing context this request carried.
                    if let Some(conversation) = &conversation {
                        payload.threads.record_sent_context(
                            conversation,
                            &payload.outbound.context_fingerprints,
                        );
                    }
                    Ok(Opened::new(upstream.map(move |event| {
                        event
                            .map(|event| A2AFrame::Stream(event, conversation.clone()))
                            .map_err(|e| ProviderError::from(A2AError::Protocol(e)))
                    })))
                }
            }
        })
    }
}

impl Wire for A2AModel {
    type Op = Completion;
    type Payload = A2APayload;
    type Frame = A2AFrame;
    type Decoder<'id> = A2ADecoder<'id>;
    type Reassembler = A2ADocument;

    fn describe(&self) -> Descriptor<'_> {
        Descriptor::new(PROVIDER)
            .model(self.agent_name.as_str())
            .replay(self)
    }
    fn encode(&self, request: CompletionRequest, mode: Mode) -> Result<A2APayload, EncodeError> {
        let outbound = self.build_request(request).map_err(EncodeError::request)?;
        Ok(A2APayload {
            outbound,
            client: self.inner.clone(),
            threads: self.threads.clone(),
            mode,
        })
    }
    fn decoder<'id>(&self) -> A2ADecoder<'id> {
        A2ADecoder {
            model: self.clone(),
            last: None,
            unary: false,
            brand: std::marker::PhantomData,
        }
    }
}

impl rig_core::completion::ReplayTarget for A2AModel {
    fn api(&self) -> rig_core::message::Api {
        rig_core::message::Api::from(PROVIDER.to_owned())
    }
    fn provider(&self) -> &str {
        PROVIDER
    }
    fn model(&self) -> &str {
        &self.agent_name
    }
    fn accepts(&self, _model: &str) -> rig_core::completion::Accepts {
        rig_core::completion::Accepts::TEXT
    }
    fn takes_documents(&self) -> bool {
        true
    }
    fn map_options(
        &self,
        _request: &CompletionRequest,
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
        let unsupported = |_| Mapping::unsupported("an A2A agent owns its generation settings");
        OptionMap {
            reasoning: Mapping::of(reasoning, unsupported),
            cache: Mapping::of(cache, |_| Mapping::unsupported("A2A has no cache setting")),
            service_tier: Mapping::of(service_tier, |_| {
                Mapping::unsupported("A2A has no service tier")
            }),
            verbosity: Mapping::of(verbosity, |_| {
                Mapping::unsupported("an A2A agent owns its generation settings")
            }),
            parallel_tool_calls: Mapping::of(parallel_tool_calls, |_| {
                Mapping::unsupported("an A2A agent owns its tools")
            }),
            top_p: Mapping::of(top_p, |_| {
                Mapping::unsupported("an A2A agent owns its generation settings")
            }),
            seed: Mapping::of(seed, |_| {
                Mapping::unsupported("an A2A agent owns its generation settings")
            }),
            stop: Mapping::of_stop(stop, |_| {
                Mapping::unsupported("an A2A agent owns its generation settings")
            }),
        }
    }
}

/// Select history after dispatch hooks, before the model's replay adaptation.
struct A2AHandler {
    model: A2AModel,
    adapter: ModelAdapter<Completion>,
}

impl Serve for A2AHandler {
    type Family = rig_core::effect::family::Completion;

    fn descriptor(&self) -> rig_core::effect::HandlerDescriptor {
        self.adapter.descriptor()
    }

    async fn serve(&self, kind: rig_core::effect::EffectKind, dispatch: Dispatch) -> Reply {
        use rig_core::effect::EffectKind;
        let kind = match kind {
            EffectKind::Completion { request, stream } => {
                match self.model.prepare_request(request) {
                    Ok(request) => EffectKind::Completion { request, stream },
                    Err(error) => return Reply::Outcome(Err(error.into())),
                }
            }
            other => other,
        };
        self.adapter.serve(kind, dispatch).await
    }
}

#[doc(hidden)]
pub struct A2ADecoder<'id> {
    model: A2AModel,
    last: Option<A2AThreadInfo>,
    unary: bool,
    brand: std::marker::PhantomData<&'id ()>,
}

impl<'id> A2ADecoder<'id> {
    fn emit(&mut self, text: String, out: &mut Out<'id, Completion>) -> Result<(), ProviderError> {
        out.run(Block::Text, &text)?;
        Ok(())
    }
    fn finish(&mut self, out: Out<'id, Completion>, message_id: Option<String>) -> Flow {
        out.end(Finish {
            usage: Usage::default(),
            reason: Some(self.last.as_ref().map_or(FinishReason::Stop, finish_reason)),
            response_id: self
                .last
                .as_ref()
                .and_then(|i| i.task_id.clone())
                .or(message_id),
            model: Some(self.model.agent_name.clone()),
            error: None,
        })
    }
}

impl<'id> Decoder<'id, Completion, A2AFrame> for A2ADecoder<'id> {
    type Event = A2AFrame;
    fn classify(&self, frame: A2AFrame) -> WireEvent<A2AFrame> {
        WireEvent::Known(frame)
    }
    fn decode(
        &mut self,
        event: A2AFrame,
        mut out: Out<'id, Completion>,
    ) -> Result<Flow, ProviderError> {
        match event {
            A2AFrame::Unary(response, conversation, fingerprints) => {
                self.unary = true;
                let info = A2AThreadInfo::from_response(&response);
                self.model.record(conversation.as_ref(), &info);
                self.model
                    .record_sent_context(conversation.as_ref(), &fingerprints);
                self.last = Some(info);
                let (text, id) = match &response {
                    SendMessageResponse::Task(task) => {
                        if let Some(error) = task_failure(&response) {
                            return Err(error);
                        }
                        (
                            task_text(
                                &task.status.state,
                                task_body_limited(task, DEFAULT_TEXT_LIMIT)?,
                            )?,
                            None,
                        )
                    }
                    SendMessageResponse::Message(message) => {
                        let body = message_body_limited(message, DEFAULT_TEXT_LIMIT)?;
                        if body.is_empty() {
                            return Err(ProviderError::Response(
                                "remote A2A agent replied with a message carrying no text".into(),
                            ));
                        }
                        (body, Some(message.message_id.clone()))
                    }
                };
                self.emit(text, &mut out)?;
                Ok(self.finish(out, id))
            }
            A2AFrame::Stream(event, conversation) => {
                // Record the remote's identifiers before the event can fail,
                // so a failed or oversized event still updates the
                // conversation.
                let info = stream_info(&event, self.last.as_ref());
                self.model.record(conversation.as_ref(), &info);
                self.last = Some(info);
                let text = stream_text(event)?;
                if !text.is_empty() {
                    self.emit(text, &mut out)?;
                }
                Ok(Flow::More)
            }
        }
    }
    fn eof(&mut self, out: Out<'id, Completion>) -> Result<Flow, ProviderError> {
        if self.unary {
            return Err(ProviderError::Truncated);
        }
        Ok(self.finish(out, None))
    }
}

/// The conversation identifiers an A2A stream event carries.
///
/// An artifact update reports no task state, so it keeps the state and
/// resumability of the last event. Clearing a paused task's id here would
/// strand the conversation.
fn stream_info(event: &a2a::StreamResponse, last: Option<&A2AThreadInfo>) -> A2AThreadInfo {
    match event {
        a2a::StreamResponse::Task(task) => {
            A2AThreadInfo::for_task(&task.context_id, &task.id, &task.status.state)
        }
        a2a::StreamResponse::Message(message) => A2AThreadInfo {
            context_id: message.context_id.clone(),
            task_id: message.task_id.clone(),
            state: None,
            resumable: false,
        },
        a2a::StreamResponse::StatusUpdate(update) => {
            A2AThreadInfo::for_task(&update.context_id, &update.task_id, &update.status.state)
        }
        a2a::StreamResponse::ArtifactUpdate(update) => A2AThreadInfo {
            context_id: Some(update.context_id.clone()),
            task_id: Some(update.task_id.clone()),
            state: last.and_then(|i| i.state.clone()),
            resumable: last.is_some_and(|i| i.resumable),
        },
    }
}

/// The text an A2A stream event carries.
///
/// Text arrives as artifact deltas and status messages; the terminal record is
/// emitted when the upstream stream ends, carrying the finish reason implied by
/// the last observed task state. A failure state ends the stream with an error
/// rather than a terminal record, matching the non-streaming surface.
fn stream_text(event: a2a::StreamResponse) -> Result<String, ProviderError> {
    match event {
        a2a::StreamResponse::Task(task) => {
            if let Some(error) = failure_for(&task.status.state, || {
                status_text_limited(&task, DEFAULT_TEXT_LIMIT)
            }) {
                return Err(error);
            }
            Ok(task_body_limited(&task, DEFAULT_TEXT_LIMIT)?)
        }
        a2a::StreamResponse::Message(message) => {
            Ok(message_body_limited(&message, DEFAULT_TEXT_LIMIT)?)
        }
        a2a::StreamResponse::StatusUpdate(update) => {
            // A status update's message is where many A2A agents put their
            // answer, so it both explains a failure and streams as text.
            let text = update.status.message.as_ref().map_or_else(
                || Ok(String::new()),
                |m| {
                    crate::parts::parts_to_text_limited(
                        &m.parts,
                        DEFAULT_TEXT_LIMIT,
                        "stream status",
                    )
                },
            )?;
            if let Some(error) = failure_for(&update.status.state, || Ok(text.clone())) {
                return Err(error);
            }
            Ok(text)
        }
        a2a::StreamResponse::ArtifactUpdate(update) => Ok(crate::parts::parts_to_text_limited(
            &update.artifact.parts,
            DEFAULT_TEXT_LIMIT,
            "stream artifact",
        )?),
    }
}

/// Refuse request fields whose absence would silently change the result.
fn reject_unsupported(request: &CompletionRequest) -> Result<(), A2AError> {
    if !request.tools.is_empty() {
        return Err(A2AError::Unsupported {
            what: "tools",
            detail: "an A2A agent runs its own tools and never returns a tool call, so tools registered on this agent could never be invoked",
        });
    }
    if request.output_schema.is_some() {
        return Err(A2AError::Unsupported {
            what: "output_schema",
            detail: "A2A has no remote schema-enforcement contract; parse and validate the response locally",
        });
    }
    if matches!(
        request.tool_choice,
        Some(ToolChoice::Required) | Some(ToolChoice::Specific { .. })
    ) {
        return Err(A2AError::Unsupported {
            what: "tool_choice",
            detail: "an A2A agent cannot be made to call a Rig tool",
        });
    }
    Ok(())
}

/// Note the sampling knobs the remote agent owns instead of the caller.
///
/// Protocol fields are read through Rig's provider-options merge.
fn log_ignored(request: &CompletionRequest) {
    let ignored_params = request.additional_params.as_ref().is_some_and(|params| {
        params.as_object().is_none_or(|fields| {
            fields.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    THREAD_PARAMS_KEY | "configuration" | "metadata" | "reference_task_ids"
                )
            })
        })
    });
    if request.temperature.is_some() || request.max_tokens.is_some() || ignored_params {
        tracing::debug!(
            target: "rig_a2a",
            "sampling parameters are ignored: an A2A agent owns its own decoding settings"
        );
    }
}

/// Append a Rig message's text to the outbound parts.
///
/// Prefixes assistant turns so a rendered history reads as a dialogue rather
/// than one undifferentiated block. Non-text content has no A2A projection and
/// is skipped.
fn push_message_text(parts: &mut Vec<Part>, message: &RigMessage) {
    let (prefix, text) = match message {
        RigMessage::User { content } => ("", user_text(content)),
        RigMessage::Assistant(message) => ("assistant: ", assistant_text(&message.content)),
        RigMessage::System { content, .. } => ("", content.clone()),
    };
    if text.trim().is_empty() {
        return;
    }
    parts.push(Part::text(format!("{prefix}{text}")));
}

fn user_text(content: &[UserContent]) -> String {
    content
        .iter()
        .filter_map(|item| match item {
            UserContent::Text(text) => Some(text.text.clone()),
            UserContent::ToolResult(result) => Some(
                result
                    .content
                    .iter()
                    .filter_map(|content| match content {
                        rig_core::message::ToolResultContent::Text(text) => Some(text.text.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            other => {
                log_dropped_content(content_label(other));
                None
            }
        })
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn assistant_text(content: &[AssistantContent]) -> String {
    content
        .iter()
        .filter_map(|item| match item {
            AssistantContent::Text(text) => Some(text.text.clone()),
            AssistantContent::ToolCall(_) => None,
            _ => {
                log_dropped_content("assistant");
                None
            }
        })
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn content_label(content: &UserContent) -> &'static str {
    match content {
        UserContent::Image(_) => "image",
        UserContent::Audio(_) => "audio",
        UserContent::Document(_) => "document",
        _ => "user",
    }
}

/// Non-text history has no A2A projection. Say so rather than dropping it
/// silently, mirroring the binary-part log in [`crate::parts`].
fn log_dropped_content(kind: &'static str) {
    tracing::debug!(
        target: "rig_a2a",
        kind,
        "skipping non-text message content while projecting to an A2A message"
    );
}

/// A task's assistant text, or the error a content-free reply deserves.
///
/// Rig's providers normalize a *legitimate* empty turn to empty text — the
/// Anthropic provider does this for a documented empty `end_turn` — and report
/// any other content-free response as a [`ProviderError::Response`].
/// A2A's legitimate empty turn is a completed task with no artifacts and no
/// closing status message: a task whose result was an action rather than text.
/// Any other state answering with nothing has neither produced a result nor
/// finished, so the caller has nothing to act on and is told so.
///
fn task_text(state: &a2a::TaskState, body: String) -> Result<String, ProviderError> {
    if !body.is_empty() {
        return Ok(body);
    }
    if matches!(state, a2a::TaskState::Completed) {
        tracing::debug!(
            target: "rig_a2a",
            "remote A2A task completed with no artifacts and no status message; reporting an empty assistant turn"
        );
        return Ok(body);
    }
    Err(ProviderError::Response(format!(
        "remote A2A agent returned no content and its task is {}",
        state_label(state)
    )))
}

/// A2A has no stop-reason concept; a paused task is the one case that is not a
/// plain stop, and it maps to "the model wants more input".
fn finish_reason(info: &A2AThreadInfo) -> FinishReason {
    match info.state {
        Some(a2a::TaskState::InputRequired) => FinishReason::Other("input_required".to_string()),
        _ => FinishReason::Stop,
    }
}

fn task_failure(response: &SendMessageResponse) -> Option<ProviderError> {
    let SendMessageResponse::Task(task) = response else {
        return None;
    };
    failure_for(&task.status.state, || {
        status_text_limited(task, DEFAULT_TEXT_LIMIT)
    })
}

/// Map a task state the caller cannot act on to a completion error.
///
/// A completion model has one provider-error channel; the state name is kept
/// in the message so callers can still distinguish task states.
fn failure_for(
    state: &a2a::TaskState,
    status: impl FnOnce() -> Result<String, A2AError>,
) -> Option<ProviderError> {
    use a2a::TaskState::{AuthRequired, Canceled, Failed, Rejected};
    if !matches!(state, Failed | Rejected | Canceled | AuthRequired) {
        return None;
    }
    Some(match status() {
        Ok(text) => {
            let text = if text.is_empty() {
                "no status message".to_string()
            } else {
                text
            };
            ProviderError::Provider(format!("remote A2A agent {}: {text}", state_label(state)))
        }
        Err(error) => ProviderError::from(error),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> CompletionRequest {
        CompletionRequest::new("hello")
    }

    #[test]
    fn tools_are_rejected_rather_than_ignored() {
        let mut req = request();
        req.tools.push(rig_core::completion::ToolDefinition {
            name: rig_core::message::ToolName::new("add").expect("non-empty tool name"),
            description: "adds".to_string(),
            parameters: serde_json::json!({}),
        });
        let error = reject_unsupported(&req).expect_err("tools must be refused");
        assert!(error.to_string().contains("tools"), "{error}");
    }

    #[test]
    fn output_schema_is_rejected() {
        let mut req = request();
        req.output_schema = Some(schemars::json_schema!({"type": "object"}));
        let error = reject_unsupported(&req).expect_err("output_schema must be refused");
        assert!(error.to_string().contains("output_schema"), "{error}");
    }

    #[test]
    fn demanding_tool_choice_is_rejected_but_permissive_is_not() {
        let mut req = request();
        req.tool_choice = Some(ToolChoice::Required);
        assert!(reject_unsupported(&req).is_err());

        req.tool_choice = Some(ToolChoice::Auto);
        assert!(reject_unsupported(&req).is_ok());

        req.tool_choice = Some(ToolChoice::None);
        assert!(reject_unsupported(&req).is_ok());
    }

    #[test]
    fn thread_params_are_read_off_additional_params() {
        let params = serde_json::json!({"a2a": {"context_id": "ctx-1", "task_id": "task-1"}});
        assert_eq!(
            thread_params(Some(&params)).expect("well-formed directive"),
            ThreadParams {
                conversation: None,
                context_id: Some("ctx-1".to_string()),
                task_id: Some("task-1".to_string()),
            }
        );

        let params = serde_json::json!({"a2a": {"conversation": "user-42"}});
        assert_eq!(
            thread_params(Some(&params)).expect("well-formed directive"),
            ThreadParams {
                conversation: Some("user-42".to_string()),
                ..ThreadParams::default()
            }
        );

        assert_eq!(
            thread_params(None).expect("absent directive"),
            ThreadParams::default()
        );
        let unrelated = serde_json::json!({"temperature": 0.5});
        assert_eq!(
            thread_params(Some(&unrelated)).expect("unrelated params"),
            ThreadParams::default()
        );
    }

    #[test]
    fn malformed_thread_params_fail_rather_than_opening_a_new_conversation() {
        for params in [
            serde_json::json!({"a2a": "ctx-1"}),
            serde_json::json!({"a2a": {"contextId": "ctx-1"}}),
            serde_json::json!({"a2a": {"context_id": ""}}),
            serde_json::json!({"a2a": {"context_id": 7}}),
            // Two different claims; honoring one would ignore the other.
            serde_json::json!({"a2a": {"conversation": "c", "context_id": "ctx-1"}}),
        ] {
            assert!(
                thread_params(Some(&params)).is_err(),
                "must be rejected: {params}"
            );
        }
    }

    #[test]
    fn sampling_parameters_do_not_fail_the_request() {
        let mut req = request();
        req.temperature = Some(0.9);
        req.max_tokens = Some(128);
        assert!(reject_unsupported(&req).is_ok());
    }

    #[test]
    fn history_renders_with_assistant_turns_prefixed() {
        let mut parts = Vec::new();
        push_message_text(&mut parts, &RigMessage::user("hi"));
        push_message_text(&mut parts, &RigMessage::assistant("hello there"));
        push_message_text(&mut parts, &RigMessage::user("and again"));

        let rendered: Vec<String> = parts
            .iter()
            .filter_map(|part| match &part.content {
                a2a::PartContent::Text(text) => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(rendered, ["hi", "assistant: hello there", "and again"]);
    }

    #[test]
    fn empty_messages_are_skipped() {
        let mut parts = Vec::new();
        push_message_text(&mut parts, &RigMessage::user("   "));
        assert!(parts.is_empty());
    }

    #[test]
    fn input_required_reports_a_distinct_finish_reason() {
        let info = A2AThreadInfo {
            context_id: None,
            task_id: None,
            state: Some(a2a::TaskState::InputRequired),
            resumable: true,
        };
        assert_eq!(
            finish_reason(&info),
            FinishReason::Other("input_required".to_string())
        );

        let info = A2AThreadInfo {
            state: Some(a2a::TaskState::Completed),
            ..info
        };
        assert_eq!(finish_reason(&info), FinishReason::Stop);
    }

    #[test]
    fn failure_states_become_provider_errors_naming_the_state() {
        for state in [
            a2a::TaskState::Failed,
            a2a::TaskState::Rejected,
            a2a::TaskState::Canceled,
            a2a::TaskState::AuthRequired,
        ] {
            let error = failure_for(&state, || Ok("quota exceeded".to_string()))
                .expect("state must fail the completion");
            let text = error.to_string();
            assert!(text.contains(state_label(&state)), "{text}");
            assert!(text.contains("quota exceeded"), "{text}");
        }
        assert!(failure_for(&a2a::TaskState::Completed, || Ok(String::new())).is_none());
        assert!(failure_for(&a2a::TaskState::InputRequired, || Ok(String::new())).is_none());
    }
}
