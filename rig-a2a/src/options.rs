//! Typed Rig provider options and lossless A2A replies.

use a2a::{SendMessageConfiguration, SendMessageResponse, StreamResponse};
use rig_core::completion::{ExtensionOptions, ProviderExtension, ReplyExtras};
use rig_core::message::Api;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Provider extension used by `CompletionResponse::extras::<A2A>()`.
pub struct A2A;

impl ProviderExtension for A2A {
    const PROVIDER: &'static str = crate::model::PROVIDER;
    type Options = A2AOptions;
    type Extras = A2AReply;
}

/// Options for `AgentBuilder::provider_option`, `AgentRunner::provider_option`,
/// or `CompletionRequest::provider_option`.
///
/// Rig replaces an agent's provider entry when a run supplies one. Build a
/// complete value for each run; conversation selection is independent of it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct A2AOptions {
    #[serde(rename = "*")]
    pub(crate) fields: MessageOptions,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct MessageOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a2a: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<SendMessageConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference_task_ids: Option<Vec<String>>,
}

impl A2AOptions {
    /// Continue a remote context. This takes precedence over a local conversation
    /// key. Echo a server-issued ID unless the server accepts client IDs.
    pub fn context(mut self, id: impl Into<String>) -> Self {
        self.fields
            .a2a
            .get_or_insert_with(Map::new)
            .insert("context_id".into(), Value::String(id.into()));
        self
    }

    /// Resume a nonterminal task. The server infers its context when omitted.
    pub fn task(mut self, id: impl Into<String>) -> Self {
        self.fields
            .a2a
            .get_or_insert_with(Map::new)
            .insert("task_id".into(), Value::String(id.into()));
        self
    }

    /// Set output media types, history length, and push notification settings.
    /// `return_immediately: true` is for the direct client API, not completions.
    pub fn configuration(mut self, configuration: SendMessageConfiguration) -> Self {
        self.fields.configuration = Some(configuration);
        self
    }

    /// Set operation metadata. This does not configure transport credentials.
    pub fn metadata(mut self, metadata: Map<String, Value>) -> Self {
        self.fields.metadata = Some(metadata);
        self
    }

    /// Reference related tasks without attempting to resume them.
    pub fn reference_tasks(mut self, ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.fields.reference_task_ids = Some(ids.into_iter().map(Into::into).collect());
        self
    }
}

impl ExtensionOptions for A2AOptions {
    type Ext = A2A;
}

/// The complete protocol reply, including binary artifacts, metadata and IDs.
/// Streaming replies retain the ordered protocol events, not a text projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum A2AReply {
    Response(Box<SendMessageResponse>),
    Events(Vec<StreamResponse>),
}

impl ReplyExtras for A2AReply {
    fn from_reply(_api: &Api, raw: &Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(raw.clone())
    }
}
