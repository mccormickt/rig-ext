# Stateless MCP tools

`rig_celld::mcp` provides stateless MCP `2026-07-28` tools for Rig, on native Rust and
`wasm32-unknown-unknown` Workers/celld. Uses official `rmcp` protocol types,
not its Tokio client service or `rig-rmcp`.

## JSON-only profile

This is a **restricted profile, not a complete Streamable HTTP client**.
MCP Streamable HTTP requires clients to accept both JSON and SSE. This client
sends `Accept: application/json` and rejects other response media types before
reading their bodies. Use it only with endpoints configured for JSON responses.

- Supports paginated `tools/list` and `tools/call`.
- Sends version, client identity, and empty capabilities on every request.
- Mirrors method, tool name, and valid `x-mcp-header` parameter annotations.
  Invalid annotated tool definitions are excluded from discovery.
- Requires modern `resultType: complete` results. No initialization, sessions,
  legacy fallback, GET streams, SSE, subscriptions, MRTR, or background tasks.
- Does not retry, follow redirects, refresh credentials, or negotiate versions.
  Protocol-version errors are returned to the host without repeating the call.
- Supports host-configured bearer credentials. OAuth discovery and token refresh
  remain application responsibilities. Never let model input select endpoints.

See the [transport specification](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http).

## Use

```toml
rig-celld = { path = "../rig-ext/rig-celld", features = ["mcp-http"] }
```

The `mcp-http` feature includes `mcp` and selects reqwest on native targets and
global Workers fetch on Wasm. Enable only `mcp` to use the portable client,
protocol types, and Rig tool adapter with an application-supplied `Transport`.
Neither feature enables storage, Worker Loader, code mode, or durable workflows.

```rust,no_run
# #[cfg(feature = "mcp-http")]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use rig_celld::mcp::{Client, HttpTransport, model::Implementation};

let transport = HttpTransport::new("https://mcp.example.com/tools")?;
// For authenticated endpoints, call transport.bearer_auth(host_token)?;
let client = Client::new(transport, Implementation::new("my-agent", "1.0"));
let tools = client.list_tools().await?;
for tool in tools {
    // Apply the host's allowlist before registration.
    let dynamic = tool.into_dynamic_tool();
    // Register with Rig AgentBuilder::dynamic_tool or DynamicToolDispatcher.
    let _ = dynamic;
}
# Ok(())
# }
```

Definitions and compiled schemas stay fixed for each `McpTool`. Use
`client.tool(retained_definition)` to bind a host-approved snapshot without
rediscovery. The client does not cache `tools/list` responses or subscribe to
catalog changes. Refresh and version the snapshot at an application boundary.
Input and output schemas compile offline. Local references such as `#/$defs`
work; external HTTP and file references fail even if another dependency enables
JSON Schema resolver features.

## Cargo examples

These native Rust examples use a configured MCP endpoint. They do not start a
fixture, need Node, or use the Amp CLI. Set `MCP_ENDPOINT` to a compatible
JSON-only MCP `2026-07-28` URL. For authenticated endpoints, supply
`MCP_BEARER_TOKEN` through your environment or secret manager; neither example
uses a built-in credential. Do not put credentials in the endpoint URL.

Discover tools without calling any of them:

```sh
cargo run --locked -p rig-celld --example mcp_call --features mcp-http -- "$MCP_ENDPOINT"
```

Call one discovered tool with explicit JSON arguments. Replace `lookup` and its
arguments with a tool from your server:

```sh
cargo run --locked -p rig-celld --example mcp_call --features mcp-http -- \
  "$MCP_ENDPOINT" lookup '{"region":"west"}'
```

The [direct example](examples/mcp/call.rs) prints the host-facing MCP result,
including metadata. It exits unsuccessfully for transport/protocol/schema errors
or a tool result with `isError: true`. Calls can have external effects; select
tools and arguments deliberately. There are no automatic retries or approvals.

Run a local JavaScript file through a Rust code-mode host:

```sh
cargo run --locked -p rig-celld --example mcp_code_mode --features mcp-http -- \
  "$MCP_ENDPOINT" rig-celld/examples/mcp/stock.js lookup
```

The [code-mode example](examples/mcp/code_mode.rs) registers only the tool names
listed after the script path. Missing names fail before script execution. The
sample [stock script](examples/mcp/stock.js) requires a `lookup({region})` tool
that returns `{answer: number}`; use your own script for other servers. Only
emitted or returned payloads reach the transcript, without MCP metadata. A script
error, timeout, or other incomplete execution returns a nonzero exit status.
Neither example uses a model provider. Both accept `--help` without an endpoint.

The [Worker example](examples/mcp/worker/src/demo.rs) is also Rust, but runs in
celld or workerd after `worker-build`, not through `cargo run`. Its code-mode
backend requires Worker Loader; plain MCP HTTP does not.

## Code mode and durable hosts

`McpTool::into_dynamic_tool()` works with `rig-codemode::DynamicToolDispatcher`
without enabling code mode's native-only `mcp` feature. The tool validates
inputs and successful `structuredContent` itself, including on direct calls.
Do not wrap it in the `rig-rmcp`-specific `OutputSchemaValidator`.

Build code-mode catalog entries from each MCP definition **before** conversion:

```rust,ignore
let definition = tool.definition();
let mut entry = rig_codemode::CatalogEntry::new(
    definition.name.to_string(),
    definition.description.as_deref().unwrap_or(""),
    serde_json::Value::Object(definition.input_schema.as_ref().clone()),
);
if let Some(schema) = &definition.output_schema {
    entry = entry.with_output_schema(serde_json::Value::Object(schema.as_ref().clone()));
}
```

Successful structured output remains JSON. `isError` becomes a failed Rig call
with its permitted content; `.raw()` does not bypass validation and makes a new
call. Response and content-block metadata stay out of model/script output.
The direct host API `McpTool::call` returns typed MCP results, including metadata.
Nested content reaches the transcript only when a script emits or returns it.

`rig-durable` builders currently accept typed `Tool` implementations, not runtime
`DynamicTool` values. Use an application-owned typed wrapper that forwards to the
dynamic tool with the supplied `ToolContext`; this works across its backends.
Register an explicit replay policy. Retain and version the MCP definition and
output schema with the workflow configuration; no automatic durable adapter or
configuration fingerprint is supplied here.

For a server that implements `Idempotency-Key`, pass `CallOptions` directly or
insert it into the Rig inbound `ToolContext`. The host must derive a stable key
for **each logical call**, not each run or entire script. A durable wrapper can
derive it from `ToolInvocation`. The header alone does not deduplicate effects.
Tool annotations are not trusted replay-safety declarations. A timeout,
disconnect, rejected response, or cancellation can leave a write's outcome
uncertain; the client must not be used as an automatic retry layer.

## Bounds and runtime limits

Defaults are 1 MiB per request body and response body, 4 MiB across discovery
responses, 16 discovery pages, 256 tools, and 30 seconds per HTTP exchange.
`Client::with_limits` sets different ceilings. The deadline includes connection,
headers, and body consumption, not synchronous schema compilation/validation.
Serde's JSON recursion limit remains enabled. Missing/mismatched IDs, malformed
envelopes, and unsupported result types fail without retry.

Non-success HTTP responses return `Error::Http(status)` unless a valid JSON-RPC
error matches the request ID; that returns only `Error::Rpc(code)`. Unsupported
media types are rejected without reading their bodies. JSON response reads stay
bounded: byte limits, timeouts, and transport failures take priority over status.
Successful HTTP responses still require valid JSON-RPC envelopes and IDs.

The body bound limits collected bytes, not total process/Worker memory or
transport buffers. Workers checks JS chunk sizes before copying into Rust.
Dropping HTTP work requests abort and reader cancellation; this is not proof
that the server stopped an external effect. Custom transports must enforce
deadlines, byte bounds, and single-attempt behavior themselves.

## Validation

```sh
cargo test --locked -p rig-celld --features mcp-http
cargo check --locked -p rig-celld --target wasm32-unknown-unknown --features mcp
cargo check --locked -p rig-celld --target wasm32-unknown-unknown --features mcp-http
cargo test --locked -p rig-worker-tests --test mcp -- --ignored --nocapture
```

The [Worker fixture](examples/mcp/worker) runs the packaged Rust client against a
loopback MCP server on celld and local workerd. It checks actual HTTP requests,
code-mode dispatch, schema validation, metadata isolation, SSE rejection,
oversized and stalled bodies, mismatched IDs, HTTP errors, uncertain disconnects,
and caller cancellation before headers and during body reads.
Server counters independently check no retries or followed redirects.
It uses no model provider or production MCP server and proves neither production
Cloudflare enforcement nor full MCP conformance. Do not expose or deploy it.

## Cross-runtime test harness

Install the pinned external tools with `scripts/workers/install.sh`. The tests
need Node, worker-build, celld, and the Miniflare npm dependencies. They run on
Unix and do not need the Amp CLI. From the repository root, run:

```sh
MCP_EVIDENCE_DIR="$PWD/.amp/in/artifacts/mcp-proof" \
  cargo test --locked -p rig-worker-tests --test mcp -- --ignored --nocapture
```

This builds the native example and the Worker bundle, starts a Rust loopback
HTTP fixture and two Worker runtimes, runs 22 fault checks on each runtime, and checks the same script
on native QuickJS, local workerd, and celld. It repeats successful runs three
times, checks invalid output schemas, and reruns after both Worker restarts.
The optional evidence directory receives raw client/server JSON and build hashes.
Port 9899 must be free for the fixture; Worker ports are selected at runtime.
The test owns the fixture and child process groups and stops them on completion
or assertion failure. It does not create public listeners or portals.

- [Native conformance example](examples/mcp/http_tools.rs): paginated discovery, input validation,
  `DynamicTool` registration, and `CodeMode::execute` over real HTTP.
- [Worker example](examples/mcp/worker/src/demo.rs): the same script through
  `WorkerReplayPlanner` and a `LOADER` binding. The small host keeps its journal
  in memory; it does **not** implement durable recovery.
- [Script](examples/mcp/stock.js): read west=37 and east=12, then emit only the total
  49. These are controlled fixture values, not production inventory.
- [Rust integration test](../scripts/workers/tests/mcp.rs): compare reports with independent server
  records; verify that invalid arguments make no HTTP request, bad output stops
  the script, private metadata stays out, and prefix replay adds no HTTP calls.
- [Rust HTTP fixture](../scripts/workers/tests/support/mcp_fixture.rs): sends
  malformed and stalled responses and records requests, effects, and cancellation.

The [Node adapter](../scripts/workers/miniflare.mjs) only configures Miniflare's
workerd modules and bindings. Rust controls test requests, assertions, process
startup, and restarts. celld starts directly through its CLI, not through
Miniflare. Plain MCP HTTP calls do not require Worker Loader; the Worker
**code-mode** example does. The example bearer value is a public test constant.
Do not use it as a production credential.
