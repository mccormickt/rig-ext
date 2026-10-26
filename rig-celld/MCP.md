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
```
