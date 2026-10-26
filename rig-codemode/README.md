# rig-codemode

`rig-codemode` lets a model write one JavaScript async function body that calls approved Rig tools, combines their results, and emits selected output. Intermediate tool results stay out of the model transcript unless the script emits them. The host keeps a record of every call.

The crate owns script execution, tool discovery, and bounded results. The host owns tool authority through a `HostDispatcher`. Tools own their external I/O.

## Features

| Cargo features | Contents | Build requirement |
| --- | --- | --- |
| none | `Catalog`, `Limits`, `ExecutionReport`, `HostDispatcher`, `DynamicToolDispatcher`, `ScriptPolicy`, `analyze`. `CodeMode::builder().build()` returns `BuildError::NoBackend`. | Stable Rust |
| `quickjs` | Native QuickJS backend through `rquickjs`, one worker thread per script | A C compiler for QuickJS; native targets only |
| `mcp` | `CatalogEntry::from_mcp_definition` and `mcp::OutputSchemaValidator` for `rig-rmcp` tools | Native targets only; MCP tools with declared output schemas |

Enabling `mcp` does not change dispatch or permissions for non-MCP tools. The builder never selects a backend by feature precedence: with several backends compiled, call `CodeModeBuilder::backend`.

## Rust Workers and external executors

Use `rig-codemode` with `default-features = false` for contracts and direct host dispatch on `wasm32-unknown-unknown`. `HostDispatcher`, `ScriptPolicy`, and their callbacks use Rig's portable bounds: local state and non-`Send` futures are allowed on this target, while native targets retain `Send`/`Sync`. Threaded Wasm is not supported by Rig.

This is not a Workers script backend. The `quickjs` feature requires a native target. `CodeModeBuilder::backend` selects a compiled engine; it does not accept an external executor. On Wasm, `build()` returns `BuildError::NoBackend`. The native bridge uses a worker thread, Tokio task scheduling, timers, and a join handle; it is not compiled on Wasm.

An application-owned executor can use `ExecutionRequest`, `Limits::restrict`, `ExecutionReport::new(status)`, `ScriptDiagnostic::new(message)`, and `ExecutionReport::into_tool_result`. Use `CallRecord::new(ordinal, name, status, delivery)` for child calls and set its public `error_kind` when needed; it defaults to `None`. Delivery is explicit and independent of tool success. Fill the report's public fields with trusted execution results; `ScriptOutput::default()` provides an empty output. The constructors do not sanitize guest data or enforce limits. The application must enforce the limits it supports and keep host details out of diagnostics. QuickJS heap and cancellation guarantees do not apply to celld or other external executors.

## Threat model of the `quickjs` backend

The script runs in a fresh QuickJS runtime on a dedicated thread inside the host process. The guest has no `fetch`, `require`, `process`, timers, `std`/`os`, module loader, or filesystem. Its heap is bounded by `Limits::memory_bytes`, its stack by a fixed guest limit, and its CPU time by an interrupt handler that reads a cancellation flag and the deadline. The host bounds every bridge message, the output buffer, and the number of pending calls.

This is **not** a fault boundary. A QuickJS memory-safety bug would run with the host's privileges. Use this backend when scripts come from a model you run for a single trust domain, or put the host process behind an OS sandbox. Do not describe it as equivalent to a WASM or process sandbox. The confinement tests in `tests/acceptance.rs` establish the tested boundary; they do not prove absence of engine vulnerabilities. Keep `rquickjs` updated.

`set_memory_limit` requires QuickJS's default allocator, so the `rust-alloc`
feature is disabled. The worker drops every guest value before destroying the
runtime, as required by QuickJS.

## Use

The crate's rustdocs contain checked examples for direct execution, Rig agent registration, typed context, policies, limits, reports, discovery, and MCP integration. Build them with:

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

## Sub-agents

Rig agents are tools. Give each agent a name and description with `AgentBuilder`,
then register `agent.into_tool()?` in `DynamicToolDispatcher`. Scripts call the
agent with `await tools.research({ prompt: "Find supporting facts." })` and get
its text answer. `Promise.all` can run independent agents in parallel; a later
call can use their answers. The dispatcher applies the same argument validation
and call/result policies as for other tools. The agent tool inherits dispatch
context; the coordinator's agent hooks do not run for nested calls.

The API-key-free [`sub_agents`](examples/sub_agents.rs) example uses two Rig
agents, not tool callbacks that imitate agents:

```sh
cargo run -p rig-codemode --example sub_agents --features quickjs
```

This is ordinary execution, not durable execution. Use `rig-durable`'s
`codemode` feature and `DurableAgentBuilder::code_mode` when each nested effect
must survive recovery.

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

Every execution excludes `Date`, `Math.random`, weak references, and finalizers.
There is no nondeterministic guest mode. Get time or random values through
explicit host tools so a durable host can record their results. These guest
restrictions alone do not make live execution durable; recovery still requires
a durable host and its delivery journal.

## Durable script steps

With `quickjs`, `replay::step(code, catalog, limits, admitted, history)` reconstructs a
fresh VM and returns either `ScriptStep::Calls` or `ScriptStep::Done`. It does
not have a dispatcher and cannot run tools. A durable host must authorize and
persist new calls before execution. `admitted` retains accepted calls, including
queued and pending calls. Persist each `SettledCall` in delivery order and supply
that journal to the next step. `Calls` includes existing unresolved admissions;
the host must retain their operations, not dispatch them again.
Input schemas are checked before calls are returned;
ordinary result projection, raw envelopes, and message limits still apply.
Replay checks the name, ordinal, arguments, and raw flag of recorded calls
before it returns new calls. The durable backend must also persist the source
and version the catalog, limits, and engine together.

Results settle in **recorded delivery order**, which can differ from invocation
order. A later call can win `Promise.race` and start a follow-up while an earlier
call remains pending. `Promise.all` still returns values in input order. A host
must record its observed delivery order before replay, not race tools again on
recovery. Local schema errors settle deterministically before external results.
`Date`, `Math.random`, weak references, and finalizers are unavailable.
The wall-time limit covers each reconstruction step, not external tool waits.
If a script terminates with new unresolved calls, the step rejects those calls
without execution. Calls admitted at an earlier wait are not rolled back if
the script later fails. A terminal step may leave admitted race losers pending;
the host must complete them and replay with their results for the final report.
Late results are marked `Discarded`, not cancelled.

See the [`rig-durable` example](../rig-durable/examples/dynamic_workflow.rs)
and its shared Duroxide/Temporal scheduler for separate durable nested calls.

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

For stateless JSON-only MCP `2026-07-28` endpoints on native or Workers targets,
use [`rig-celld::mcp`](../rig-celld/MCP.md). Its tools work with
`DynamicToolDispatcher` and validate their own structured results; no `mcp`
feature in `rig-codemode` or `OutputSchemaValidator` is needed. SSE and legacy
sessions are not supported by that client.

The `mcp` feature below integrates the native-only `rig-rmcp` client:

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

The Wasm regression test runs direct dispatch and script review with `Rc` state and local timer futures. It checks refusal, effective-argument validation, redaction, grants, and external report construction, not guest execution. Install `wasm-bindgen-cli` at the version of `wasm-bindgen` in `Cargo.lock`, then run with Node.js:

```sh
rustup target add wasm32-unknown-unknown
cargo check --locked -p rig-codemode --target wasm32-unknown-unknown
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  cargo test --locked -p rig-codemode --target wasm32-unknown-unknown --test wasm_dispatch
```

## Boundaries

- Nested calls pass through the supplied `HostDispatcher`, not an agent runner's hooks. Keep per-call policy in the dispatcher and script-level policy in a `ScriptPolicy`.
- Scripts have no small-value store, image helpers, or raw provider source grammar.
- QuickJS is the only script backend. It is native-only and runs in the host process.
