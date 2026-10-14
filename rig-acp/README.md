# rig-acp

Native ACP v1 client delegation for Rig 0.44.0. **The remote agent owns its
model/tool loop.** This is not a set of remote tools for a locally owned Rig loop.
The crate is a prototype and is not published.

## Scope and API

```rust,no_run
use rig_acp::{Client, Event, protocol::{AcpAgent, AcpAgentConfig}, schema::*};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::connect(AcpAgent::new(
    AcpAgentConfig::new("hermes").arg("acp"),
)).await?;
let session = client.new_session("/absolute/path/on/agent/host").await?;
let mut turn = session.prompt_text("Inspect the tests; do not change files.")?;
while let Some(event) = turn.next().await {
    match event? {
        Event::Update(update) => println!("{:?}", update.update),
        Event::Permission(permission) => {
            // Replace this with an application decision. Never select an
            // arbitrary first option: the agent chooses the option order.
            permission.respond(RequestPermissionOutcome::Cancelled)?;
        }
        Event::Completed(response) => println!("{:?}", response.stop_reason),
    }
}
// The same Session continues the remote conversation.
let agent = session.model().agent("host-memory-key").build();
let answer = agent.prompt("Summarize the result.").await?;
let sub_agent = agent.into_tool()?;
# let _ = (answer, sub_agent);
# Ok(()) }
```

- `Client::initialized()` exposes the negotiated version and capabilities.
  Filesystem, terminal, and terminal-auth capabilities are not advertised.
  There are no host filesystem/terminal callback implementations.
- `Session::prompt` accepts official content blocks and checks the negotiated
  image/audio/embedded-context capabilities. Text and resource links are baseline
  ACP inputs. This does not prove that each agent implements its advertised media.
- `Turn::next` returns ordered updates, pending permissions, and one final
  response. Official schema types preserve protocol content separately from text.
  Unmodeled wire fields may be omitted by the official schema. Updates outside an
  active prompt are not retained by this bounded API.
- `Permission::respond` accepts only an offered option ID or `Cancelled`.
  Dropping a permission sends `Cancelled`. Callbacks run outside ordered
  dispatch, so another update or permission can arrive while a prompt is pending.
- Cloned sessions share admission: only one prompt per session. Other sessions
  can run concurrently. Do not use one bound session for unrelated users.
- `Session::cancel()` sends `session/cancel`, denies outstanding permissions,
  and does **not** end the local turn immediately. Read until the final response
  or a connection error. Peer acknowledgment is not proof that real external work
  stopped; the demo checks its own independent effect counter.
- Dropping only `Turn` does not cancel work if a client/session owner remains.
  Dropping the last owner closes the connection. The official process connector
  kills its owned child/process group on drop; remote HTTP processes remain
  host-owned. No server or sandbox DELETE is sent by this crate.
- A turn has an 8 MiB cumulative serialized protocol-event budget. Overflow emits
  an error, cancels pending work once, and remains busy until remote settlement.
  This bounds retained application events, **not total process memory**: SDK and
  SSE framing can allocate a complete incoming frame before this check. There is
  no claim of hostile-peer memory containment or transport backpressure.
- No ACP-level automatic prompt retries, reconnect, load/resume, persistent identity map,
  durable replay, or exactly-once guarantee. On transport loss, prompt delivery
  can be ambiguous. A deliberate resend can repeat an effect.
- The host supplies deadlines. A timeout that drops a future is not remote
  cancellation. Retain the session, call `cancel`, and wait for settlement.

## Rig integration is explicitly session-bound

`Session::model()` uses Rig's `Wire`, `Transport`, and completion `Serve` handler.
Its `agent(local_memory_key)` installs request preparation **after dispatch
hooks and before replay adaptation**. For a named route, use:

```rust,ignore
builder.model_route_handler("remote", session.model().handler("remote"))
```

The newest user message is sent. Remote history is not replayed. System/preamble
text is sent as ordinary ACP text on each call; ACP does not give it system-role
authority. Rig's local memory key never changes the remote session. Bind the child
before `into_tool()`; the parent's conversation key is not inherited.

The projection accepts text, rejects local tool declarations, required tool
choices, output schemas, documents, and non-text newest input. The remote owns
generation settings: typed provider options are rejected; temperature/max-token
fields and unrelated raw parameters are not forwarded. Transport credentials and
remote session routing are host-owned, not model-visible tool arguments.

Only `AgentMessageChunk` text becomes Rig text deltas. Thoughts, plans, tool
events, and permission records remain in the protocol view. Direct completion
responses retain the structured record array in `CompletionResponse.raw`.
`EndTurn` maps to normal stop, `MaxTokens` to length; cancellation and other stop
reasons fail rather than look like a successful completion. Token usage is absent.
The default model permission policy cancels; use `permission_policy` for an async
application decision. Explicit cancellation interrupts a pending policy future.

## Transport choices and released-library research

Checked on 2026-10-14 with Cargo registry metadata and released source:

| Component | Pin / contract |
| --- | --- |
| Official Rust ACP core | `=3.3.0`, schema resolved to `1.11.0` in Cargo.lock; Rust 1.88 / edition 2024 |
| Official Rust HTTP | `=3.3.0`, opt-in `client` feature; Tokio/reqwest |
| Rig | `0.44.0`; no upstream runner changes |
| Rivet Sandbox Agent | released `0.5.3`; separate POST/SSE contract |
| Hermes | source `952322ca3cf23435af4f95c2b91216d9e3658b84`; reports 0.21.5; Python ACP extra `0.9.0`, Python 3.14 |

ACP crate version 3.3.0 is **not protocol v3**. This crate negotiates stable v1.
The core SDK is runtime-neutral and uses `Send` connection futures. This wrapper
uses Tokio tasks, channels, and `Send + Sync` Rig interfaces. Older non-Send
`ClientSideConnection` examples do not describe this release.

- `process` (default): reuses official `AcpAgent` stdio framing and process
  lifecycle. In-process official `Channel` peers also work without this feature.
- `http`: re-exports official `rig_acp::http::HttpClient`. Pass a configured
  transport to `Client::connect`. This feature does not enable `process` or
  `rivet`. Its standard HTTP server contract is not Rivet's contract.
- `rivet`: `RivetTransport::new(reqwest_client, endpoint)` uses a **fresh,
  dedicated server ID** at `/v1/acp/{id}?agent=...`. Set bearer headers in the
  host-owned reqwest client. The first initialize POST creates the agent process.
  Existing/shared server attachment is not supported.

The official HTTP transport expects an `Acp-Connection-Id` initialization header
and separate connection/session streams. Rivet uses URL identity and one
process-wide SSE stream. Rivet responds to requests on **both POST and SSE**;
client responses/notifications return HTTP 202. Therefore, this adapter uses SSE
as its sole ordered inbound source and drains duplicate POST bodies without
publishing them. POSTs run concurrently so a pending prompt cannot block a
permission reply. Up to 128 ordinary POSTs can be in flight. Observable numeric SSE gaps,
duplicates, invalid frames, and disconnection fail the connection; no automatic
replay/reconnect occurs. Bootstrap reads retained SSE from ID 0 only for a fresh
server. Rivet's bounded in-memory replay is not durable or lossless.

Rivet does not acknowledge prompt admission before completion. Cancellation is
therefore deferred until agent message/thought, tool, plan, or permission activity.
Commands, metadata, mode, configuration, and usage updates do not open that gate.
This assumes a fresh, dedicated session with no load/replay or independent agent
turns; ACP session updates do not carry prompt IDs. If the prompt completes first,
the deferred cancellation is discarded. **A non-completing peer with no recognized
turn activity cannot be cancelled through this adapter.** The host must use its
deadline and separate process lifecycle controls. Once cancellation is sent,
the adapter waits for HTTP 202 (stdin-write acknowledgment) before forwarding
further frames. This fences subsequent turns but can block all sessions on that
connection if the acknowledgment stalls. Permission responses remain concurrent
with a pending prompt; a cancellation notification needs no permission response.
HTTP 202 proves only delivery to stdin, not stopped execution.

Configure the supplied reqwest client with `retry(reqwest::retry::never())` and
`redirect(reqwest::redirect::Policy::none())` when prompt reissuance is forbidden.
The adapter cannot inspect or override a caller's HTTP retry policy. The demo and
fault tests disable both; no exactly-once guarantee follows from this setting.

Authoritative sources:

- [Official SDK release and examples](https://github.com/agentclientprotocol/rust-sdk/tree/199532c7433f1aec207dd0cada299901a5621ece/src/agent-client-protocol)
- [SDK dispatch and callback ordering](https://github.com/agentclientprotocol/rust-sdk/blob/199532c7433f1aec207dd0cada299901a5621ece/src/agent-client-protocol/src/concepts/ordering.rs)
- [Official HTTP contract](https://github.com/agentclientprotocol/rust-sdk/blob/199532c7433f1aec207dd0cada299901a5621ece/md/http-transport.md)
- [Rivet POST, SSE replay, and response broadcast](https://github.com/rivet-dev/agents/blob/v0.5.3/sandbox-agent/server/packages/acp-http-adapter/src/process.rs)
- [Rivet released-server integration fixture](https://github.com/rivet-dev/agents/blob/v0.5.3/sandbox-agent/sdks/acp-http-client/tests/smoke.test.ts)
- [Hermes ACP adapter](https://github.com/NousResearch/hermes-agent/tree/952322ca3cf23435af4f95c2b91216d9e3658b84/acp_adapter)
- [Hermes local-model probe used as the test setup reference](https://github.com/NousResearch/hermes-agent/blob/952322ca3cf23435af4f95c2b91216d9e3658b84/evals/acp_empty_session_wire.py)

Agent-side serving is a separate future capability: a facade would need capability
negotiation, session ownership, client-tool authorization, and callback scheduling.
Do not await a callback-dependent Rig turn inside the ordered ACP handler.
The prototype does not implement this facade, A2A task semantics, Kubernetes,
Substrate lifecycle, or sandbox provisioning.

## Repeat the tests and demonstration

Requirements: native Rust, Node.js, Python 3, and loopback networking. Run from
the repository root. No provider credential is needed for the deterministic demo.

```sh
cargo test --locked -p rig-acp --all-features -- --nocapture
cargo run --locked -p rig-acp --example acp_demo
```

The example uses the real official process transport and an independent Node
JSON-RPC fixture. It asserts two turns on one session, isolation, live ordered
chunks, approval and denial, cancelled work, and a later turn. Expected effect
receipt: `effects=1, cancels=1`: one approved effect, zero denied/cancelled effects.
It also exercises a Rig agent and a parent model's actual sub-agent tool call.
The parent model is scripted; it verifies that the real child result arrives.

Test matrix and independent controls:

| Layer / test file | Assertions and failure controls |
| --- | --- |
| Official-SDK peer / `tests/session.rs` | Capabilities, exact event order, isolation, busy admission, reversed permission replies across sessions and within one session, invalid option, drop/missing policy, cancellation before/during permission, prevented gated effect versus drop-then-release effect, next turn, late redaction on default/named routes, actual sub-agent result, invalid route with zero remote prompts, raw events distinct from text, remote errors, output/final-metadata overflow, transport loss |
| Real stdio process / `tests/process.rs` | Bad version, malformed wire plus EOF, effect before lost response, no retry through direct and Rig APIs, deliberate resend increments independent effect receipts, child exit after last-owner drop and abandoned initialization |
| Loopback HTTP fault peer / `tests/rivet.rs` | Auth on GET/POST, no auth in prompt, POST-first/SSE-first duplicates, exact chunks before one completion, sequence gap/malformed/EOF fail, one received prompt/effect, no DELETE; delayed prompt admission and cancellation delivery, next-turn fence and effect counters |
| Released Rivet server / `scripts/rivet_demo.sh` | Same protocol and Rig demo with process-side request/session/PID receipts, permission POST while prompt POST is pending, HTTP 401 control, server survives session cancellation and client drop |
| Real Hermes / `scripts/hermes_smoke.py` | Rust public API → Rig agent → ACP stdio → unmodified Hermes → local model HTTP fixture; exact user histories `[A1]`, `[A1, A2]`, `[B1]` with distinct input markers |

For Rivet in an Amp orb, download the released native binary (not a container):

```sh
mkdir -p /tmp/rig-acp-tools
curl -fsSL https://releases.rivet.dev/sandbox-agent/0.5.3/binaries/sandbox-agent-x86_64-unknown-linux-musl \
  -o /tmp/rig-acp-tools/sandbox-agent
chmod +x /tmp/rig-acp-tools/sandbox-agent
SANDBOX_AGENT_BIN=/tmp/rig-acp-tools/sandbox-agent bash rig-acp/scripts/rivet_demo.sh
```

The script uses an authenticated, loopback-only supervised service, fresh data,
and a synthetic local token. It does not create a portal. It prints the binary
SHA-256. It stops the service/process group and removes its disposable data.
**Observed limitation:** released Rivet 0.5.3 did not complete the explicit server
DELETE in this fixture run. The script bounds this separate check to five seconds
and reports it; supervisor cleanup is used. This is not session cancellation.

For Hermes, install the pinned independent peer once:

```sh
git clone https://github.com/NousResearch/hermes-agent.git /tmp/hermes-acp-smoke
git -C /tmp/hermes-acp-smoke checkout --detach 952322ca3cf23435af4f95c2b91216d9e3658b84
uv sync --project /tmp/hermes-acp-smoke --locked --python 3.14 --extra acp
/tmp/hermes-acp-smoke/.venv/bin/hermes acp --check
cargo build --locked -p rig-acp --example acp_peer
python3 rig-acp/scripts/hermes_smoke.py /tmp/hermes-acp-smoke
```

Hermes enables Python SDK unstable extensions, but this smoke test uses only v1
initialize/new/prompt and Rig text output. The harness isolates HOME/environment,
disables configured MCP, tools, memory, and automatic model-generated titles,
and uses a loopback model fixture with a dummy credential. It checks actual model
requests, not only final text. Live model inference, Hermes tool permissions,
Hermes cancellation, media handling, and all unstable methods remain unproven.
No real provider credentials are used.

To capture provenance, sanitized synthetic receipts, commands, timestamps, exit
codes, source hashes, and optional external demos:

```sh
SANDBOX_AGENT_BIN=/tmp/rig-acp-tools/sandbox-agent \
HERMES_CHECKOUT=/tmp/hermes-acp-smoke bash rig-acp/scripts/verify.sh
```

Missing optional executables are reported as `BLOCKED`, not passing. There are no
container-image digests because these checks use native binaries. This is a new
feature, so no unchanged-code baseline is called a failed ACP implementation.
Fault injection and positive/negative controls provide the comparison instead.
The official HTTP client re-export is feature-checked; this suite does not claim
a separate live official-HTTP-server interoperability run. WASM is not supported.

The reported [Hermes/Substrate deployment](https://github.com/agent-substrate/substrate/pull/487#issuecomment-5097213171)
does not identify a reproducible ACP bridge/image/version. The
[OpenClaw `AcpRuntime` plugin](https://github.com/agent-substrate/always-on-agent/blob/c380889b73ee78b4bb71fc17079227ab886d9e82/extensions/substrate/acp-runtime.ts)
uses `/v1/chat/completions`, not ACP JSON-RPC. Neither is a claimed test layer here.
Its [wrong-route fallback incident](https://github.com/agent-substrate/always-on-agent/commit/3cb37731ae744e6cfdd145ab67df3212dbaa3f22)
is why these tests require peer receipts and an invalid-route control.
