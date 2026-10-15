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

## Experimental server adapter

The optional `server` feature exposes `server::RigExecutor::new(agent)` for
`a2a-server-lf` 0.5.2. It runs an ordinary Rig `Agent`, retains canonical Rig
history per tenant/context, and serializes turns in each context. The host must
supply authentication, task ownership checks, admission control, and an A2A
task store. Only text inputs are supported. A run has an eight-model-call limit.

Cancellation is cooperative at Rig effect boundaries. An in-flight tool can
finish and commit its effect before cancellation returns. Tools must await
their cleanup and must not leave detached work. Neither task state `Canceled`
nor a disconnected stream means execution has stopped. History and task state
are process-local; this is not a durable scheduler or an exactly-once service.

See the [Substrate prototype and test instructions](../scripts/substrate/README.md).
The example uses deterministic inference with real Rig tool dispatch and file
I/O. Its cluster proof is blocked on the tested orb's Linux 6.1 kernel; it is
not a verified Substrate snapshot/restore integration.

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
`model.agent()`. The selected model handler prepares history **after all Rig
dispatch hooks and before replay adaptation**. Hook changes, including
redaction, reach the remote. Routing to a local model does not remove its
history. `A2AModel` does not convert to `DynModel`: that conversion cannot
install the request-preparation handler. Use
`A2AModelRouteExt::a2a_model_route(label, model)` on a normal Rig builder to
register a remote handler, then select it with
`.using_model(label)` or a Rig model-selection hook.

Direct `model.completion(request)` and `model.stream(request)` calls prepare
the original message boundary themselves. They also work in a custom driver
of Rig's `AgentRun`.

## Typed requests and protocol replies

`A2AOptions` uses Rig's `ProviderExtension` / `ExtensionOptions` contract. It
works on completion requests, agent builders, and runners. It supports remote
context/task IDs, `SendMessageConfiguration`, operation metadata, and related
task references. `a2a_conversation` uses a separate entry, so it does not erase
these options or raw additional parameters.

```rust,no_run
use rig_a2a::{A2A, A2AClient, A2AOptions, A2AReply};
use rig_core::completion::CompletionRequest;

# async fn run(client: A2AClient, task_id: String) -> anyhow::Result<()> {
let response = client.model().completion(
    CompletionRequest::new("Use README.md.")
        .provider_option(A2AOptions::default().task(task_id)),
).await?;
if let Some(Ok(A2AReply::Response(reply))) = response.extras::<A2A>() {
    println!("{reply:?}"); // Full protocol task/message, not just its text.
}
# Ok(()) }
```

A task ID alone is valid: the server infers its context. Explicit IDs override
the local conversation key. Raw `additional_params.a2a` remains supported and
overrides typed values. Rig merges raw `configuration`, `metadata`, and
`reference_task_ids` over typed fields. Other raw keys are ignored. Rig replaces
the complete provider entry when a run supplies `A2AOptions`; build a complete
options value rather than expecting field-level inheritance.

For full protocol control, `client.request(Message)` preserves the message ID,
parts, metadata, and extension identifiers. `client.message(text).part(part)`
is the short form. Both accept `.options(options)`, `.send()`, and `.stream()`.
Direct `.context()` / `.task()` setters override IDs from options.

## Streaming and media boundaries

The Rig completion stream **buffers the task until EOF**. A2A artifacts can be
replaced by ID; Rig text deltas cannot retract text. The adapter applies
replacement, append, and task-snapshot semantics before emitting the final
text. Status messages replace prior status messages, not append to the answer.
An empty stream, a stream ending in `working`/`submitted`, or an orphan artifact
append fails. `lastChunk` completes an artifact, not the task.

Use `client.message(...).stream().await?` for **live protocol events**. It
preserves progress, replacements, metadata, and binary parts. Completed Rig
streams also expose ordered events through `response.extras::<A2A>()` as
`A2AReply::Events`. These are SDK-decoded events; transport defaults can be
normalized (for example, absent and false `append`). Dropping a stream does
not cancel the remote task.

Both completion modes limit the final text projection to **1 MiB** and the
serialized protocol content retained for reply extras to **16 MiB**. For a
stream, the latter budget counts all events, including overwritten artifacts,
binary data, and metadata. JSON escaping and base64 expansion count only toward
the protocol budget, not the text limit. The budget is checked once per incoming
event, before retaining it; accumulated artifacts are not repeatedly serialized.
These are adapter retention limits, not HTTP body limits: the upstream SDK
decodes each response/event first. The direct streaming API does not accumulate
events; consumers must set their own retention policy.

Rig image, audio, video, and document input maps to A2A URL or byte parts,
preserving MIME types and part order. URLs are forwarded, not fetched. Rig's
replay adapter cannot transfer provider-local file IDs and replaces unsupported
sources with its normal placeholders. Responses contain projected text followed
by canonical Rig images when the part declares a supported image MIME type.
JSON data and other URLs render as text. Other binary artifacts remain in typed
reply extras; use the direct API for binary-only messages without a supported
image type. No audio/video generation API is implied. Use protocol replies when
exact part order, artifact boundaries, or extension metadata matters.

## AgentRunner, AgentRun, and persistence

`agent.prompt(...)` returns Rig's `AgentRunner`: hooks, memory, request options,
model selection, and standard sub-agent tools remain Rig's responsibility.
`a2a_conversation` works on all runner output types. It does not enable remote
schema enforcement; a typed run that requires `output_schema` still fails.
Applications can parse and validate the completed text locally instead.

`AgentRun` is the serializable state machine, not a transport or persistence
backend. Use `agent.resume(restored_run).provider_option(saved_options)` to
resume a ready checkpoint through the normal runner, or supply model/tool IO
from your own driver. A run in `AwaitingModel` cannot be resumed directly:
feed the issued call's recovered response through `model_response` first.
Setting provider options does not reissue a pending call. Persist the response
and pending run together; if the response was lost, the host must reconcile the
remote task before deciding whether another request is safe.

An accepted-turn checkpoint resumes without repeating that model call. A
checkpoint prepared with `retry_model_turn(Feedback(...))` sends the follow-up
on the next turn. The example records the first response before saving this
started-run checkpoint; it is not a demonstration of automatic crash recovery.
Persist A2A context/task IDs and endpoint/tenant identity separately from the
run. `A2AOptions` is serializable. The offline example demonstrates restoring a
run with a fresh client and explicit IDs, and checks the saved tenant identity.
Restoring a run is not the same as reconnecting to a running remote task:
use task polling/subscription for running work, and a new message to answer an
`input-required` task. Never put credentials in the saved envelope.

| Identifier | Owner and purpose |
| --- | --- |
| `a2a_conversation("tenant:user:chat")` | Application key for Rig memory and the process-local remote-ID lookup. Scope it to the caller when sharing clients. |
| `A2AOptions::context(id)` | Remote context ID. Bypasses the local lookup. |
| `A2AOptions::task(id)` | Remote task ID. Can be used without a context ID. |
| Endpoint and interface tenant | Remote service identity. Save and validate it with the run and options. |

The automatic conversation cache is process-local (1,024 conversations and
64 context fingerprints per conversation). Eviction opens a new remote context.
Serialize sends for the same conversation in your application; this cache does
not provide a per-conversation queue or exactly-once delivery. Threaded requests
send only new context and the newest prompt, so use them with agents that retain
context. A2A permits remote context memory but does not require it. Rig memory
and remote context are separate stores; clearing one does not clear the other.

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

The direct client preserves all task states. `inner()` exposes polling, task
listing, cancellation, subscriptions, push configuration, and extended cards.
Use it for background tasks (`returnImmediately`) and out-of-band authorization;
the completion adapter refuses `returnImmediately: true` and unfinished unary
tasks rather than reporting them as successful completions.

Authentication and `A2A-Extensions` are transport configuration, not operation
metadata. Configure HTTP headers/TLS through `http_client_builder`. The caller
must implement any extension it declares. This crate does not acquire OAuth
tokens, implement arbitrary extensions, or validate signed agent cards. It
supports native JSON-RPC and HTTP+JSON, not gRPC, SLIMRPC, or legacy A2A 0.3 wire
formats. Interface selection requires the supported Major.Minor version
(`1.0`); patch versions do not affect selection. A newer minor version does
not imply support for `1.0` semantics.

JSON-RPC forwards the selected interface tenant. The released
`a2a-client-lf` 0.2.8 REST transport omits tenant from polling, task listing,
subscriptions, extended-card retrieval, and push-configuration reads, listing,
and deletion. Do not use those REST operations
on tenant-bearing interfaces; select a JSON-RPC interface for tenant-scoped
task management. Message sends carry tenant on both transports. This is an
upstream transport limit, not an authentication guarantee.

## Released API baseline and source review

Reviewed against released packages on 2026-10-13:

| Component | Release | Integration decision |
| --- | --- | --- |
| Rig | [0.44.0](https://github.com/0xPlaygrounds/rig/releases/tag/v0.44.0) | Keep the released `Wire` + `Transport` driver; the old `CompletionModel` trait is no longer the extension point. |
| A2A specification | [1.0.1](https://github.com/a2aproject/A2A/releases/tag/v1.0.1) | Wire version is `1.0`; patch releases do not change negotiation. |
| LF core/client/server | [0.4.1](https://docs.rs/a2a-lf/0.4.1/a2a/), [0.2.8](https://docs.rs/a2a-client-lf/0.2.8/a2a_client/), [0.5.2](https://docs.rs/a2a-server-lf/0.5.2/a2a_server/) | Upgrade together to keep one set of protocol types and obtain streaming/error, tenant, task-state, and artifact-reduction fixes. |

The authoritative Rig contracts are the version-pinned docs for
[AgentRunner](https://docs.rs/rig-agent/0.44.0/rig_agent/agent/runner/struct.AgentRunner.html),
[AgentRun](https://docs.rs/rig-agent/0.44.0/rig_agent/run/struct.AgentRun.html),
[Wire](https://docs.rs/rig-core/0.44.0/rig_core/wire/trait.Wire.html), and
[ReplayTarget](https://docs.rs/rig-core/0.44.0/rig_core/completion/trait.ReplayTarget.html).
Old `rig` library URLs can show stale `CompletionModel` documentation. Upstream
`main` also changes the serialized run format and turn-policy types; this crate
does not depend on unreleased APIs or promise cross-version run compatibility.

A2A 1.0 adds task listing, interface/version negotiation, flattened content
parts, and updated OAuth/security descriptions. Its
[tagged specification](https://github.com/a2aproject/A2A/blob/v1.0.1/docs/specification.md)
and [protobuf contract](https://github.com/a2aproject/A2A/blob/v1.0.1/specification/a2a.proto)
define task-only continuation, interrupted blocking calls, and artifact update
semantics. The documentation site's latest-version label can lag GitHub releases.

Keep the [LF Rust SDK](https://github.com/a2aproject/a2a-rs). The unrelated
[`a2a-rs` 0.10.0](https://docs.rs/crate/a2a-rs/0.10.0) adds opinionated auth,
persistence, resilience, and ConnectRPC behavior; those are not reasons to
replace the current transport adapter. [`a2a-types` 0.2.0](https://docs.rs/a2a-types/0.2.0/)
is a types-first alternative, not a complete client. The older
[`a2a-sdk` 0.7.0](https://docs.rs/crate/a2a-sdk/0.7.0) does not establish current
A2A 1.0 client parity. These alternatives were researched, not interoperability-tested.

## Comparison with other agent integrations

Reviewed on 2026-10-13. These comparisons validate API design choices, not
cross-framework interoperability or equivalent durability guarantees.

| Integration and source | Relevant contract | Decision for this crate |
| --- | --- | --- |
| [Google ADK A2A](https://adk.dev/a2a/intro/index.md), [v2.12.0 remote proxy](https://github.com/google/adk-python/blob/v2.12.0/src/google/adk/a2a/agent/_remote_a2a_agent.py) | `RemoteA2aAgent` behaves as a local agent; session state stays local to each side. Events retain A2A metadata and remote IDs. | Keep the ordinary Rig agent/sub-agent interface, with separate local keys, remote IDs, and typed protocol replies. This is the direct A2A comparison. |
| [Rivet v2.3.27 AI actor example](https://github.com/rivet-dev/rivet/blob/v2.3.27/examples/ai-agent/src/index.ts), [actor lifecycle](https://rivet.dev/actors/docs/lifecycle/) | Actors own persistent state; the AI example sends both text deltas and accumulated content with a message ID. | Keep persistence with the host. Distinguish live protocol events from completed projections; do not treat artifact replacement as append-only text. The repository tag is not a `rivetkit` package version. |
| [Cloudflare Agents chat example](https://developers.cloudflare.com/agents/examples/chat-agent/) | `AIChatAgent` from `@cloudflare/ai-chat` uses Durable Objects/SQLite for messages and reconnectable streams. | Make run restoration and remote task continuation explicit. An HTTP client and its local cache cannot promise actor durability or automatic reconnect. |
| [Vercel AI SDK subagents](https://ai-sdk.dev/docs/agents/subagents), [middleware](https://ai-sdk.dev/docs/ai-sdk-core/middleware), [stream resume](https://ai-sdk.dev/docs/ai-sdk-ui/chatbot-resume-streams) | Sub-agents are ordinary tools; middleware transforms parameters before model calls; reconnect needs application storage for chat/stream IDs. | Reuse Rig `into_tool()`, hooks, and typed provider options. Prepare the final request after middleware, and keep persistence outside the adapter. |

ADK's A2A conversion remains experimental; its artifact converter is not a
proof of arbitrary A2A artifact parity. Rivet, Cloudflare, and Vercel are used
here as framework design comparisons, not evidence of native A2A support.
Unversioned documentation can change independently of package releases.

## Examples and checks

- [`a2a_agent`](examples/a2a_agent.rs): offline agents, routing, streams, sub-agent
  tools, typed replies, and serialized-run recovery. No credentials required.
- [`a2a_client`](examples/a2a_client.rs): a live remote and OpenAI orchestrator;
  requires `A2A_AGENT_URL` and `OPENAI_API_KEY`.

```sh
cargo test --locked -p rig-a2a
cargo test --locked -p rig-a2a --test client oracle_ -- --nocapture
cargo clippy --locked -p rig-a2a --all-targets --all-features -- -D warnings
cargo run --locked -p rig-a2a --example a2a_agent
```
