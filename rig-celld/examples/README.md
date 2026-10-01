# rig-celld examples

Runnable examples for `rig-celld`. Each example is a full application. Read its README before you deploy it.

| Example | Crate used | Description |
|---|---|---|
| [`celld-agent`](celld-agent/README.md) | `rig-celld` | A Rust Durable Object on celld. It keeps chat history and semantic memory in its private SQLite database. |
| [`celld-conformance`](celld-conformance/README.md) | `rig-celld` | A live celld test fixture. It checks CRUD, concurrency, rollback, garbage collection, and reopen behavior. |

Both examples target `wasm32-unknown-unknown` and need `worker-build` and celld. See the [`rig-celld` README](../README.md#examples) for build steps and constraints.
