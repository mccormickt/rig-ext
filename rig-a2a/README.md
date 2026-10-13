# rig-a2a

`rig-a2a` lets [Rig](https://github.com/0xPlaygrounds/rig) use a remote agent
that implements the Agent2Agent (A2A) protocol.

It discovers the remote agent card and exposes the remote as an `A2AModel` or
an ordinary Rig `Agent`. The model supports unary and streaming requests over
JSON-RPC or HTTP+JSON. Server-issued A2A context and task identifiers stay
host-side.

## Dependency

This crate is unpublished. From a sibling crate in this workspace:

```toml
[dependencies]
rig-a2a = { path = "../rig-a2a" }
rig-agent = { workspace = true }
rig-core = { workspace = true }
```

The default feature uses Rustls. To use native TLS instead:

```toml
rig-a2a = { path = "../rig-a2a", default-features = false, features = ["native-tls"] }
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

Build agents with `client.agent()`, `client.agent_for_conversation(...)`, or
`model.agent()`. These builders install A2A request preparation before Rig
adapts history. `A2AModel` does not convert to `DynModel`: a raw erased driver
cannot retain the original newest-message boundary. Direct `model.completion`
and `model.stream` calls prepare requests at the same boundary.

## Compose it as a sub-agent tool

```rust,no_run
use rig_a2a::{A2AClient, A2AConversationExt};
use rig_agent::agent::AgentBuilder;
use rig_core::providers::openai::{self, OpenAI};

# async fn run() -> anyhow::Result<()> {
let remote = A2AClient::from_url("http://localhost:8080").await?;
let openai = OpenAI::from_env()?;

let remote_tool = remote
    .agent()
    .a2a_conversation("project-42")
    .build()
    .into_tool()?;
let agent = AgentBuilder::new(openai.completion(openai::GPT_4O_MINI))
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

Rig does not expose a run's conversation ID to completion hooks and does
not copy it from an outer agent into a sub-agent run. Therefore, a converted
sub-agent cannot infer the outer conversation automatically. Bind the
conversation when constructing the sub-agent, or create one orchestrator tool
set per application conversation.

## Protocol boundaries

An A2A agent owns its instructions and tools. Requests that require local tool
calls or a Rig output schema therefore fail instead of silently dropping those
requirements. Sampling parameters are ignored because decoding belongs to the
remote agent. A2A does not report token usage, so response usage counters are
absent (`None`). Typed generation options are unsupported and follow Rig's
unsupported-option policy.

### Paused tasks and finish-reason policy

An `input-required` task retains its context and task IDs for the next turn
and reports `FinishReason::Other("input_required")`, distinct from completion.
Rig defaults to treating unknown finish reasons as errors. No acceptance
policy is forced by this adapter. For a remote that intentionally asks for
more input, opt in on that A2A run with
`.accept_unknown_finish_reasons(true)` (or on its agent builder or direct
`CompletionRequest`). This allows the clarification to be returned normally;
the next run with the same `a2a_conversation` resumes the paused task. Keep this
policy scoped to that remote, not an orchestrator's unrelated models. Remote
failed, rejected, canceled and auth-required tasks still produce provider
errors even when acceptance is enabled.

See the examples for direct requests, model-backed agents, streaming, and
sub-agent composition.

## Examples and checks

- [`a2a_agent`](examples/a2a_agent.rs): model-backed agents and sub-agent tools.
- [`a2a_client`](examples/a2a_client.rs): direct requests and streaming.

```sh
cargo test --locked -p rig-a2a
cargo clippy --locked -p rig-a2a --all-targets --all-features -- -D warnings
```
