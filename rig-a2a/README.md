# rig-a2a

`rig-a2a` lets [Rig](https://github.com/0xPlaygrounds/rig) use a remote agent
that implements the Agent2Agent (A2A) protocol.

It discovers the remote agent card and exposes the remote as an `A2AModel` or
an ordinary Rig `Agent`. The model supports unary and streaming requests over
JSON-RPC or HTTP+JSON. Server-issued A2A context and task identifiers stay
host-side.

## Installation

```toml
[dependencies]
rig-a2a = "0.1"
rig-agent = "0.42"
rig-core = "0.42"
```

The default feature uses Rustls. To use native TLS instead:

```toml
rig-a2a = { version = "0.1", default-features = false, features = ["native-tls"] }
```

`rig-a2a` currently supports native targets only.

## Use a remote agent as a Rig model

```rust,no_run
use rig_a2a::{A2AClient, A2AConversationExt};
use rig_agent::completion::Prompt;

# async fn run() -> anyhow::Result<()> {
let client = A2AClient::from_url("http://localhost:8080").await?;
let agent = client.agent().build();

let response = agent
    .prompt("Continue our migration plan.")
    .a2a_conversation("project-42")
    .await?;

println!("{response}");
# Ok(())
# }
```

Use `a2a_conversation` instead of Rig's ordinary `conversation` setter for an
A2A-backed model. It sets both Rig's conversation-memory key and the remote A2A
thread key. It works on an agent builder or on an individual run.
`model_for_conversation` and `agent_for_conversation` are also available when a
model or agent should bind its remote thread for its whole lifetime.

## Compose it as a sub-agent tool

```rust,no_run
use rig_a2a::{A2AClient, A2AConversationExt};
use rig_agent::client::AgentClientExt;
use rig_core::{client::ProviderClient, providers::openai};

# async fn run() -> anyhow::Result<()> {
let remote = A2AClient::from_url("http://localhost:8080").await?;
let openai = openai::Client::from_env()?;

let remote_tool = remote
    .agent()
    .a2a_conversation("project-42")
    .build()
    .into_tool();
let agent = openai
    .agent(openai::GPT_4O_MINI)
    .dynamic_tool(remote_tool)
    .build();
# let _ = agent;
# Ok(())
# }
```

No A2A-specific tool adapter is needed. `A2AClient::agent` returns a normal Rig
agent, and Rig's `Agent::into_tool` performs the standard sub-agent conversion.
Use `a2a_conversation` on the agent builder before conversion when repeated
calls must continue one remote A2A thread.

Rig 0.42 does not expose a run's conversation ID to completion hooks and does
not copy it from an outer agent into a sub-agent run. Therefore, a converted
sub-agent cannot infer the outer conversation automatically. Bind the
conversation when constructing the sub-agent, or create one orchestrator tool
set per application conversation.

## Protocol boundaries

An A2A agent owns its instructions and tools. Requests that require local tool
calls or a Rig output schema therefore fail instead of silently dropping those
requirements. Sampling parameters are ignored because decoding belongs to the
remote agent. A2A does not report token usage, so responses use zero-valued Rig
usage metrics.

See the examples for direct requests, model-backed agents, streaming, and
sub-agent composition.
