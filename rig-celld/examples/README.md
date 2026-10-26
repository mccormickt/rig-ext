# rig-celld examples

Runnable examples for `rig-celld`. Native MCP examples run with Cargo against a
configured endpoint; Worker applications need a Workers runtime. Read each
guide before running tools with external effects or deploying an application.

| Example | Crate used | Description |
|---|---|---|
| [`mcp_call`](mcp/call.rs) | `rig-celld` | Native Rust tool discovery and direct calls. See the [Cargo commands](../MCP.md#cargo-examples). |
| [`mcp_code_mode`](mcp/code_mode.rs) | `rig-celld`, `rig-codemode` | Native Rust host for a local script and an explicit MCP tool allowlist. See the [Cargo commands](../MCP.md#cargo-examples). |
| [`celld-agent`](celld-agent/README.md) | `rig-celld` | A Rust Durable Object on celld. It keeps chat history and semantic memory in its private SQLite database. |
| [`celld-conformance`](celld-conformance/README.md) | `rig-celld` | A live celld test fixture. It checks CRUD, concurrency, rollback, garbage collection, and reopen behavior. |

The two Durable Object applications target `wasm32-unknown-unknown` and need
`worker-build` and celld. See the [`rig-celld` README](../README.md#examples) for
build steps and constraints. Scripts under `scripts/workers` run cross-runtime
tests; they are not required for the native MCP examples.

The MCP examples use Rust and Cargo. Non-Rust files serve these runtime and
test requirements:

- `mcp/stock.js` is guest input for the JavaScript code-mode engine, not a host
  launcher. The Rust `mcp_code_mode` example loads and executes it.
- `mcp/worker/fixture.mjs` is an independent Node HTTP test server. It sends
  malformed responses, stalls body reads, and records connection closure.
- `mcp/e2e.py` and `scripts/workers/mcp.sh` build, start, and restart external
  celld/workerd processes and compare their results with the native Rust example.
  Cargo alone does not supply the Worker runtime or its bindings.
