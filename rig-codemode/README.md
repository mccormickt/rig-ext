# rig-codemode

`rig-codemode` lets a model write one JavaScript async function body that calls approved Rig tools, combines their results, and emits selected output. Intermediate tool results stay out of the model transcript unless the script emits them. The host keeps a record of every call.

The crate owns script execution, tool discovery, and bounded results. The host owns tool authority through a `HostDispatcher`. Tools own their external I/O.

## Features

| Cargo features | Contents | Build requirement |
| --- | --- | --- |
| none | `Catalog`, `Limits`, `ExecutionReport`, `HostDispatcher`, `DynamicToolDispatcher`, `ScriptPolicy`, `analyze`. `CodeMode::builder().build()` returns `BuildError::NoBackend`. | Rust 1.95+ (the `rig-core` 0.43 MSRV) |
| `quickjs` | Native QuickJS backend through `rquickjs` 0.14, one worker thread per script | A C compiler for QuickJS; native targets only |
| `mcp` | `CatalogEntry::from_mcp_definition` and `mcp::OutputSchemaValidator` for `rig-rmcp` tools | `rig-rmcp` 0.43, `jsonschema` 0.58 |

Enabling `mcp` does not change dispatch or permissions for non-MCP tools. The builder never selects a backend by feature precedence: with several backends compiled, call `CodeModeBuilder::backend`.

## Threat model of the `quickjs` backend

The script runs in a fresh QuickJS runtime on a dedicated thread inside the host process. The guest has no `fetch`, `require`, `process`, timers, `std`/`os`, module loader, or filesystem. Its heap is bounded by `Limits::memory_bytes`, its stack by a fixed guest limit, and its CPU time by an interrupt handler that reads a cancellation flag and the deadline. The host bounds every bridge message, the output buffer, and the number of pending calls.

This is **not** a fault boundary. A QuickJS memory-safety bug would run with the host's privileges. Use this backend when scripts come from a model you run for a single trust domain, or put the host process behind an OS sandbox. Do not describe it as equivalent to a WASM or process sandbox. The confinement tests in `tests/acceptance.rs` establish the tested boundary; they do not prove absence of engine vulnerabilities. Keep `rquickjs` updated.

## Backend selection

Four shortlisted engines were measured with one shared fixture set (fresh runtime per script, trivial eval, infinite CPU loop, allocation loop, deep recursion, confinement probes) on x86_64 Linux, release profile, rustc 1.99. Measurements are from this workspace, not published rankings.

| Option | Versions | Binary | Cold init + trivial eval | Repeat | CPU loop stop | Heap bound | Result |
| --- | --- | --- | --- | --- | --- | --- | --- |
| B. Native QuickJS (`rquickjs`) | rquickjs 0.14 (QuickJS-NG) | 2.0 MB | 0.6 ms | 0.26 ms | interrupt handler, within 1 ms of the deadline | `set_memory_limit`: `out of memory` on alloc loop, 3e6 sort, 100 MB `JSON.parse` | **Selected** |
| A. QuickJS WASM in Wasmtime | wasmtime 49, `quickjs-wasi` 3.6.2 artifact (637 KB, 12 imports, 450 exports) | 19.6 MB | 1.5 s cold Cranelift compile; 1.7 ms precompiled deserialize; 0.5–0.7 ms per fresh instance | 0.5 ms | fuel trap | WASM linear memory | Viable follow-on; needs a JSValue-handle ABI adapter for `host_call`/promise glue |
| C. Boa | boa_engine 0.22 | 16.5 MB | 1.1 ms | 0.44 ms | iteration limit | none: `s = s + s` ×40 requested 34 GB and aborted the process | Rejected |
| D. V8 (`deno_core`) | — | — | — | — | — | — | Not measured; V8 is not already in these applications |

B satisfies the limits the design requires (heap, stack, CPU, confinement) with the smallest integration surface and no artifact pinning. Its weaker fault boundary is documented above rather than hidden. A is the path to a stronger boundary and shares the bridge protocol; nothing in the public API names an engine type.

Two `rquickjs` facts shaped the implementation. `set_memory_limit` is only effective with the default allocator, so the `rust-alloc` feature is not enabled. `rquickjs-sys` compiles QuickJS with assertions on, and `JS_FreeRuntime` asserts that no JS values are alive, so the worker drops every guest value before the runtime.

## Use

The crate's rustdocs contain checked examples for direct execution, Rig 0.43 agent registration, typed context, policies, limits, reports, discovery, and MCP integration. Build them with:

```sh
cargo doc -p rig-codemode --no-deps --features quickjs,mcp --open
```

```rust,ignore
use std::sync::Arc;
use rig_codemode::{Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest, Limits, ScriptReview};

let dispatcher = DynamicToolDispatcher::new(tools)?
    .with_call_policy(|invocation| { /* deny or rewrite arguments */ Ok(()) })
    .with_result_policy(|_invocation, result| { /* redact */ result });
let catalog = Catalog::new(dispatcher.definitions().iter().map(CatalogEntry::from_definition))?;
let codemode = CodeMode::builder(catalog, Arc::new(dispatcher))
    .limits(Limits { max_calls: 16, ..Limits::default() })
    .script_policy(|review: ScriptReview<'_>| Ok(review.grant_referenced()))
    .build()?;

// Explicit host mode: run a script directly.
let report = codemode.execute(ExecutionRequest::new(code)).await?;
println!("{}", report.render_text());

// Or expose it to a Rig agent as one tool with `{ "code": string }` input.
let tool: rig_core::tool::DynamicTool = codemode.tool();
```

`codemode.description()` renders usage rules plus TypeScript declarations for inline catalog entries within a byte budget. Entries marked `Presentation::Deferred`, or beyond the budget, stay callable and discoverable from scripts.

## Script policy

Per-call policy lives in the dispatcher (`CallPolicy`, `ResultPolicy`). `DynamicToolDispatcher::new` compiles input schemas once and returns `InputSchemaError` for an invalid schema. Every call validates its effective arguments after call-policy rewrites, before the callback runs. This validation is part of core, without the `mcp` feature. A `ScriptPolicy` adds one review of the whole script before it runs, so a host can approve a script, attach credentials, or refuse it without seeing each nested call.

```rust,ignore
.script_policy(|mut review: ScriptReview<'_>| {
    if review.analysis.dynamic_tool_access {
        return Err(ToolExecutionError::refused("scripts must name their tools statically"));
    }
    if review.analysis.tools.contains("crm_lookup") {
        review.context.insert(CrmToken(token.clone()))?;   // reaches every child call
    }
    Ok(ScriptGrant::only(["crm_lookup"], review.context))
})
```

The review carries `analyze(code)`: tool names written literally as `tools.name`, `tools["name"]`, `tools?.name`, or `globalThis.tools.name`; a `dynamic_tool_access` flag for computed access, `eval`/`Function`, or value use of the table; and `uses_discovery` for `searchTools`/`describeTool`. Comments and strings are skipped; code inside template `${...}` holes is scanned. Analysis stops at 64 KiB of source or 64 nested templates and marks the result dynamic. The analysis is advisory, with both false negatives and false positives, including regular-expression literals. `is_static()` means only that the scanner found no dynamic access; it does not prove completeness. Never approve a denylist from these names and then use `grant_catalog()`.

The returned `ScriptGrant` is enforced on every call: a call outside `GrantedTools::Only(names)` is refused before dispatch, recorded as `Refused` with `permission_denied`, and the script sees the same `denied` error a dispatcher refusal produces. Use an explicit host allowlist or `grant_referenced()`; the latter can refuse valid calls missed by analysis. `grant_catalog()` keeps the whole catalog callable and requires independent authorization.

A policy `Err` refuses the script. Nothing runs; `execute` returns `ExecutionError::Refused` and `codemode.tool()` returns that error as the tool result. Synchronous closures implement `ScriptPolicy`; implement the trait directly to await an operator approval. Policy review and approval waits are outside the execution wall-time deadline; the host must set a separate approval timeout if needed. Without a policy, every script may call the whole catalog with the request's context.

## Script API

| Name | Behavior |
| --- | --- |
| `await tools["name"](args)` | One dispatch. Resolves to the tool's JSON value (including a JSON string), its literal text, or `{ content: [...] }` for mixed content. Rejects with `CodeModeToolError` on failure, refusal, skip, host rejection, or size error. |
| `await tools["name"].raw(args)` | A **separate** dispatch. Resolves to `{ status, name, content, error? }` and never throws for tool outcomes. `error` has `kind`, `message`, `retryable`, `code`. |
| `text(value)` | Appends a string, or the JSON of a value, plus a newline to the bounded output. Strings are cut at a character boundary at the limit; structured values that do not fit throw a `RangeError` and are never cut. |
| `searchTools(query, { limit })` | BM25 over the catalog snapshot; default 10, at most 50 results of `{ name, namespace?, description }`. Queries are limited to 4096 bytes and 32 tokens; repeated terms count once in scoring. Native scans check cancellation and the execution deadline. |
| `describeTool(name)` | `{ name, namespace?, description, inputSchema, outputSchema? }` or `null`. Names are limited to 128 bytes. |
| return value | Shown to the model as `Return value:` when JSON-serializable. |

`CodeModeToolError` fields: `message` (the tool's model-visible output only), `kind` (Rig `ToolErrorKind` in snake_case), `status` (`error`, `denied`, `skipped`, `rejected`, `size`), `tool`.

Both discovery helpers serialize borrowed catalog data through the message byte and nesting limits. Oversized queries or responses throw `RangeError`. Catalog search indexes are built once at host construction. The host stops admitting tool calls at the execution deadline and joins the interrupted worker before returning a report.

`tools` is a frozen null-prototype object. Names route exactly: `a-b`, `a_b`, and `__proto__` are three different tools, and `tools.constructor` is `undefined`. There is no `fetch`, `require`, `process`, `setTimeout`, `std`, `os`, or module loading; `import()` rejects. Each execution gets a fresh runtime, so globals and prototype changes never cross scripts.

## Limits

Host `Limits` are ceilings. `ExecutionRequest::with_limits(LimitOverrides)` can lower `wall_time`, `max_calls`, `memory_bytes`, and `output_bytes`; raising any of them is an error before the script runs.

| Resource | Default |
| --- | --- |
| Source | 64 KiB |
| Guest heap | 64 MiB |
| Wall time, including queued and in-flight calls | 30 s |
| Tool calls | 64 total, 8 in flight |
| One argument or result message | 1 MiB |
| Emitted output | 64 KiB, then `[output truncated at the host limit]` |

Host result serialization stops at `message_bytes` or 64 JSON nesting levels, without cloning mixed content. Oversized structured results produce a size error. Exception name, message, and stack share a UTF-8-safe byte budget: the smallest of 8 KiB, `message_bytes`, and `output_bytes`.

When the script returns, throws, times out, or the caller drops the `execute` future: no further calls are admitted, in-flight dispatch futures are dropped, queued calls never start, late replies are discarded, and the runtime is destroyed. The report records each call as `Succeeded`, `Failed`, `Refused`, `Skipped`, `Rejected`, `NotStarted`, or `CancellationRequested`. A dropped future is not proof that an external effect stopped.

Shutdown retains completed tool outcomes even when the host has not yet joined their tasks; their delivery is `Discarded`. `ScriptDelivery::Delivered` means a reply was sent to the worker (or a rejection was created there), not that the promise settled or the script read it. `Oversized` means a size error was sent instead; it also does not prove consumption.

`ExecutionReport::into_tool_result` maps statuses to Rig results: completed scripts return text; `TimedOut` is a non-retryable `timeout` error; `Cancelled` is `cancelled`; script errors and stalls are non-retryable `other` errors. The error's model output keeps the partial text and the call summary.

## MCP

```rust,ignore
let definitions = client.list_all_tools().await?;
let mcp_tools = rig_rmcp::tools_from_server(definitions, client.peer());
let catalog = Catalog::new(mcp_tools.iter().map(McpTool::definition).map(CatalogEntry::from_mcp_definition))?;
let dispatcher = OutputSchemaValidator::new(
    DynamicToolDispatcher::new(mcp_tools.into_iter().map(Into::into))?,
    &catalog,
)?;
```

Build catalog entries from `McpTool::definition()`; the Rig `ToolDefinition` has no output schema. `OutputSchemaValidator` reads the `McpStructuredContent` that `rig-rmcp` publishes to the result context and validates it against the declared schema. A missing or mismatching `structuredContent` becomes a failed, non-retryable call; the tool is not called again. `isError` results reject in normal mode; `.raw` mode keeps the permitted content blocks and status. Response `_meta` and the raw `CallToolResult` never reach scripts.

## Examples

```sh
cargo run -p rig-codemode --example explicit_host --features quickjs
cargo run -p rig-codemode --example mcp_tools --features quickjs,mcp   # in-process fake MCP server
```

## Tests

```sh
cargo test -p rig-codemode                          # no backend: contracts and catalog
cargo test -p rig-codemode --features quickjs       # acceptance suite with mock tools
cargo test -p rig-codemode --features quickjs,mcp   # plus the fake rmcp server
```

## Not in this release

- **Agent-hook adapter (design stage 3).** `rig-agent` 0.43 keeps `dispatch_tool_call` crate-private, so nested calls cannot reuse an agent runner's hooks. `codemode.tool()` works as an ordinary agent tool, but nested calls pass only through the `HostDispatcher` you supply. Keep per-call policy in the dispatcher and script-level policy in a `ScriptPolicy`.
- Small-value store, image helpers, and raw provider source grammar.
- A WASM-hosted guest (option A) or a process sandbox (option E).
