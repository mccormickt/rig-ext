# Rig integrations for celld

This workspace is a feasibility implementation for running [Rig](https://github.com/0xPlaygrounds/rig) agents in [celld](https://celld.dev/docs/) cells through [workers-rs](https://github.com/cloudflare/workers-rs).

It contains:

- [`rig-celld`](rig-celld/README.md), a Rig vector-store adapter for celld's built-in `sqlite-vec` extension.
- [`celld-agent`](examples/celld-agent/README.md), a Rust Durable Object that keeps chat history and semantic memories in its private SQLite database.
- [`celld-conformance`](examples/celld-conformance/README.md), a deterministic live celld test fixture for the reusable backend contract.

## Architecture

```text
HTTP request
    │
    ▼
workers-rs router
    │ /agents/{name}/messages
    ▼
AgentCell Durable Object
    ├── Rig completion and embedding models ──► provider HTTP API
    ├── exact chat history ───────────────────► SQLite
    └── semantic turn memory ─────────────────► sqlite-vec
```

The Durable Object name is the agent partition key. Each agent has isolated storage, execution, alarms, and WebSocket connections. This matches celld's cell model and avoids a shared lock or shared conversation table.

## Feasibility results

| Capability | Feasibility | Result and constraint |
| --- | --- | --- |
| Rig agents in Rust Workers | High | Rig 0.42 and workers-rs compile to `wasm32-unknown-unknown`. `worker-build` produces the JavaScript shim and WASM that celld loads. |
| Provider-backed completion and embeddings | High | Rig's existing provider clients use the browser Fetch path in WASM. A celld-specific `CompletionModel` is not necessary for OpenAI, Anthropic, and similar HTTP APIs. |
| Per-agent vector memory | High on celld | celld statically embeds `sqlite-vec` 0.1.9. The adapter has been exercised in `celld dev` with BLOB insertion, `vec0` cosine search, a bound `k`, and auxiliary text columns. |
| Encrypted Turso memory | Blocked | The prototype's multi-chunk `pwrite` uses separate Durable Object SQL calls and `sync` is a no-op. Celld 0.4.0 reproduced a durable torn write, so this backend is not exposed as a Cargo feature. |
| Cloudflare deployment of this vector store | Not portable | Cloudflare Durable Object SQLite does not expose `sqlite-vec` or `vec0`. Use another Rig index, such as a Vectorize adapter, when Cloudflare is the deployment target. |
| Durable Workflows | Medium | celld implements Workflows partially, but workers-rs cannot author a `WorkflowEntrypoint` today. A small JavaScript/TypeScript Workflow shell can call a Rust Worker or Durable Object through a binding. |
| celld's `CELLD_AI_URL` adapter | Experimental | It provides one buffered `env.AI.run(model, input)` endpoint. It does not provide provider-specific request semantics or true response streaming. A Rig adapter would also need structural JavaScript interop instead of relying on the workers-rs `Ai` class check. |

### workers-rs version constraint

This workspace pins `worker` to 0.8.3. Rig 0.42 uses reqwest 0.13, which uses `wasm-streams` 0.5. `worker` 0.8.4 and 0.8.5 use `wasm-streams` 0.6. Linking both versions into one Worker produces duplicate WASM symbols, even though `cargo check --target wasm32-unknown-unknown` succeeds. `worker-build` is the required compatibility check.

Remove the pin when Rig/reqwest and workers-rs use the same `wasm-streams` ABI.

## Durable workflow design

The practical first design keeps state in the Rust cell and uses a thin Workflow shell only for long waits, retries, and step visibility:

```text
TypeScript WorkflowEntrypoint
    │ step.do("plan", ...)
    │ step.sleep(...)
    │ step.waitForEvent(...)
    ▼
Rust service binding
    ▼
AgentCell operation endpoint
    ├── operation ID / idempotency record
    ├── Rig agent or tool call
    └── durable result
```

Every provider call and side-effecting tool call needs a stable operation ID. This is required because celld can replay `run()` from the start and can run a step callback again after a crash. Code outside `step.do` must also be safe to repeat. Keep large transcripts and tool results in the cell or object storage and return only references from Workflow steps.

If portability to Cloudflare Workflows is not required, a Durable Object state machine plus alarms is simpler and can stay fully in Rust.

## Storage migration boundary

An existing Durable Object must not switch between native sqlite-vec and encrypted Turso in place. Each backend and incompatible vector schema needs a new Durable Object class and Wrangler migration tag. Applications copy logical documents through `(id, generation)` cursors, verify the destination, and then change routing. Physical vec0 row IDs and Turso file chunks are not migration cursors.

## Next integrations

1. **Hybrid memory:** combine SQLite FTS5 and `sqlite-vec`, then fuse lexical and semantic ranks. This improves recall for identifiers, exact tool output, and natural-language references.
2. **Memory lifecycle:** add extraction, correction/preference records, summarization, retention, and deletion. Exact chat history should not grow without a bound.
3. **Durable tools and approval:** store tool requests in an inbox, send them over hibernatable WebSockets, and resume after a person approves or rejects them.
4. **Scheduled agents:** use alarms for reminders, polling, and maintenance. Use Workflows when the process needs named steps, long waits, or external events.
5. **Semantic cache:** key expensive model/tool results by embeddings and invalidate them by model, prompt, tenant, and data version.
6. **Shared knowledge:** keep private memory in each agent cell. Put cross-agent knowledge in a separate shared vector service because one cell cannot query another cell's SQLite database directly.
7. **Agent gateways:** expose cells through A2A or an MCP-facing gateway. Review WASM support before using companion protocol crates; many server transports assume a native Tokio runtime.
8. **Streaming:** use hibernatable WebSockets for client streams and inbox notifications. Keep persisted events separate from transient token chunks.
9. **Evaluation and audit:** persist model/tool metadata, retrieval scores, operation IDs, and prompt versions so retries and memory quality can be inspected.

## Sources

- [celld documentation](https://celld.dev/docs/)
- [celld Cloudflare compatibility](https://celld.dev/docs/cloudflare-compat/)
- [celld WebAssembly and workers-rs example](https://github.com/denoland/celld/blob/main/docs/wasm.md)
- [Cloudflare Durable Object SQLite extensions](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Cloudflare Workflows Workers API](https://developers.cloudflare.com/workflows/build/workers-api/)
- [workers-rs Workflow support issue](https://github.com/cloudflare/workers-rs/issues/663)
