//! Keep Rig memory and the remote A2A thread on the same conversation key.
//!
//! ```no_run
//! use rig_a2a::A2AConversationExt;
//!
//! # async fn run(client: rig_a2a::A2AClient) -> anyhow::Result<()> {
//! let agent = client.agent().build();
//! agent
//!     .prompt("what did we decide?")
//!     .a2a_conversation("user-42")
//!     .await?;
//! # Ok(()) }
//! ```

use rig_agent::agent::{AgentBuilder, AgentRunner};
use rig_core::completion::{ExtensionOptions, ProviderExtension};
use serde::Serialize;

/// Configure both Rig conversation memory and A2A remote threading.
///
/// Rig does not pass its memory conversation ID to completion models. This
/// extension sets that ID normally and also sends it to
/// [`A2AModel`](crate::A2AModel) through a separate provider-options entry.
/// It does not replace other provider options or raw additional parameters.
pub trait A2AConversationExt: Sized {
    /// Continue `id` in both Rig memory and the remote A2A agent.
    fn a2a_conversation(self, id: impl Into<String>) -> Self;
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Conversation {
    #[serde(rename = "*")]
    fields: Fields,
}

#[derive(Debug, Clone, Serialize)]
struct Fields {
    conversation: String,
}

impl Conversation {
    fn new(id: String) -> Self {
        Self {
            fields: Fields { conversation: id },
        }
    }
}

impl ProviderExtension for Conversation {
    const PROVIDER: &'static str = "rig-a2a-conversation";
    type Options = Self;
    type Extras = crate::A2AReply;
}

impl ExtensionOptions for Conversation {
    type Ext = Self;
}

impl A2AConversationExt for AgentBuilder {
    fn a2a_conversation(self, id: impl Into<String>) -> Self {
        let id = id.into();
        self.conversation(id.clone())
            .provider_option(Conversation::new(id))
    }
}

impl<O> A2AConversationExt for AgentRunner<O> {
    fn a2a_conversation(self, id: impl Into<String>) -> Self {
        let id = id.into();
        self.conversation(id.clone())
            .provider_option(Conversation::new(id))
    }
}
