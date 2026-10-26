# rig-celld

`rig-celld` contains reusable storage contracts, stateless MCP tools, and Rig adapters for workers-rs applications on celld. Durable Object classes, routes, authorization, conversation state, and provider configuration stay in application crates.

## Features

| Cargo features | Contents | Native | WASM runtime requirement |
| --- | --- | --- | --- |
| none | Memory models, logical cursors, backend contract, and capability types | Supported | No Workers binding |
| `dynamic-workers` | Fresh Worker Loader, JS/WASM modules, entrypoint Fetcher, bounded response reads | Compile only | Worker Loader binding on celld or Cloudflare/workerd |
| `storage` | `CellStorage`, SQL access, and synchronous transactions | Compiles for tests | Cloudflare or celld SQLite Durable Objects |
| `sqlite-vec` | Includes `storage`; `SqliteVecIndex<M>`, Rig `VectorStoreIndex`, and Rig `InsertDocuments` | Compiles for tests | celld 0.6.0 or later with the Wrangler `sqlite_vec` compatibility flag |
| `mcp` | Stateless JSON-only MCP client, protocol types, schema validation, and Rig tools | Supported | Application-supplied transport |
| `mcp-http` | Includes `mcp`; bounded HTTP transport | reqwest | Global Workers fetch; no storage or Loader binding |

## MCP

[`rig_celld::mcp`](MCP.md) supports stateless MCP `2026-07-28` endpoints configured
for JSON responses. It does not support SSE or legacy sessions and is not a
complete Streamable HTTP client. Both MCP features are independent of storage,
dynamic Workers, code mode, and durable workflows. The [MCP guide](MCP.md)
starts with [Cargo examples](MCP.md#cargo-examples) for direct tool calls and
code mode. Worker examples, limits, and cross-runtime tests are documented there
separately.

## Dynamic Workers

`dynamic-workers` enables only workers-rs, not `storage` or `sqlite-vec`. It does not depend on codemode or durable workflows. workers-rs stays pinned to `=0.8.3`; the adapter uses its JS/WASM reexports and reflection because that version has no typed Loader API.

Configure a Loader binding in Wrangler:

```json
{"worker_loaders": [{"binding": "LOADER"}]}
```

Use an explicit network policy and per-invocation limits:

```rust,ignore
use rig_celld::{OutboundPolicy, WorkerCode, WorkerLimits, WorkerLoader, WorkerModule};

let loader = WorkerLoader::from_env(&env, "LOADER")?;
let mut loaded = loader.load(WorkerCode {
    main_module: "guest.js".into(),
    modules: [(
        "guest.js".into(),
        WorkerModule::JavaScript(
            "export default {fetch() {return new Response('hello')}}".into(),
        ),
    )].into(),
    compatibility_date: "2026-07-01".into(),
    compatibility_flags: vec![],
    outbound: OutboundPolicy::Deny,
    limits: WorkerLimits { cpu_ms: 100, subrequests: 0 },
})?;
let result = loaded.fetch_bounded(None, request, 4096).await;
loaded.dispose();
let response = result?;
```

Each `load()` creates a fresh anonymous Worker. Compilation can be lazy, so load errors can reach the caller on its first fetch. `WorkerModule::Wasm(bytes)` supplies a compiled WASM module, which guest JS can import by its module path. Compatibility settings are passed to the runtime. The guest environment is explicitly empty: the adapter passes no credentials or storage bindings.

`OutboundPolicy::Deny` sets `globalOutbound: null`. `Inherit` explicitly selects the parent's outbound access; `Service(fetcher)` routes global fetch through a service binding. celld 0.6.2 does not support service-routed `connect()` or WebSockets: its service protocol cannot carry bidirectional tunnels. Select `Deny` for isolated replay. This adapter does not transfer Rust callbacks or provide typed guest RPC APIs. On celld 0.6.2, a service binding has a different JS constructor name from workers-rs' `Fetcher`; obtain that binding through reflection when `Env::service()` rejects it.

`LoadedWorker::entrypoint(None)` returns the default `worker::Fetcher`; `Some(name)` selects a named entrypoint. If you use the Fetcher directly, keep the loaded owner alive until all response bodies finish or are abandoned. `fetch_bounded()` does this for you and returns status, headers, and body bytes. Non-2xx status is not a transport error. It checks each JS chunk before copying it into Rust and rejects a body over the requested bound. On overflow, body error, or a dropped read future, it requests stream cancellation and releases the reader lock on a best-effort basis. Cancellation failures do not replace the primary error, and cancellation completion is not awaited.

The byte bound limits the collected body, not JS/runtime buffers, request bytes, or elapsed time. Bound request generation and supply an application deadline separately. Runtime CPU/subrequest settings apply per invocation; they do not provide an aggregate budget or wall-time deadline.

| Runtime tested | CPU setting | Subrequest setting | Loaded-worker cleanup |
| --- | --- | --- | --- |
| celld 0.6.2 | Finite CPU probe rejected at 1 ms and completed at 1000 ms | Global fetch rejected at zero; second fetch rejected at one and allowed at two | Stub `dispose()` requests asynchronous release; no shutdown acknowledgment |
| local workerd 1.20260730.1 via Miniflare 4.20260730.0 | Probe completes despite 1 ms setting | Zero/one settings do not reject the tested global fetch calls | No stub disposer; application references are dropped for runtime GC |

`dispose()` is idempotent and nonthrowing, with a `Drop` fallback. It catches property access and invocation failures on the **Loader stub**, never calls `dispose` on an entrypoint, and never claims cancellation or deterministic release. celld entrypoint properties can become RPC methods. On workerd, GC timing is a runtime concern. Explicit cleanup while the request context is live is preferred; a WASM trap does not run Rust destructors.

The [Loader conformance fixture](examples/loader-conformance/README.md) runs a real bundled Rust Worker on both local runtimes. Local tests do not prove Cloudflare production limit enforcement. Cached `get()`, facets, bindings in guest `env`, and tail APIs are outside this adapter's scope.

## Configure celld

Use celld 0.6.0 or later for nested `transactionSync()` calls. The examples are tested with celld 0.6.1.

Enable `sqlite_vec` and use a SQLite-backed Durable Object:

```json
{
  "compatibility_flags": ["sqlite_vec"],
  "durable_objects": {
    "bindings": [{ "name": "AGENTS", "class_name": "AgentCell" }]
  },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["AgentCell"] }]
}
```

Keep a `CellStorage` in that object and construct the index from it:

```rust,ignore
#[durable_object(fetch)]
pub struct AgentCell {
    storage: CellStorage,
}

impl DurableObject for AgentCell {
    fn new(state: State, _env: Env) -> Self {
        Self { storage: state.into() }
    }
    // ...
}

let model = client.embedding(openai::TEXT_EMBEDDING_3_SMALL, Some(512));
let index = SqliteVecIndex::new(self.storage.clone(), model)?;

index.upsert_text("turn-42", &memory, &memory_text).await?;

let agent = AgentBuilder::new(client.completion(openai::GPT_4O_MINI))
    .dynamic_context(5, index)
    .build();
```

`SqliteVecIndex::named` selects another SQL table prefix and the JSON field that Rig's generic `InsertDocuments` implementation reads as the document ID.

The index accepts any Rig embedding `Model` or `DynModel<Embedding>` through the `EmbedText` trait. Implement `EmbedText` directly for other embedding backends.

## Transactions

`CellStorage::transaction_sync` exposes `ctx.storage.transactionSync()`. The closure runs synchronously. If it returns an error, the runtime rolls back all writes of the closure, and the method returns that error. Return recoverable failures as `Err`. A panic traps the Wasm instance: the runtime rolls back the database writes, but Rust destructors do not run and in-memory state is not restored.

Schema initialization, each upsert, and each `insert_documents` batch run in their own `transactionSync()` call. `delete` and `collect_garbage` are each one SQL statement, which is atomic without a transaction. In a call that is already open, celld runs the inner call as a nested savepoint. An inner failure discards only the inner writes, and the outer transaction can continue. Thus an application can commit its own rows and index writes together. Embed the text before the transaction, because the closure cannot await:

```rust,ignore
let embedding = model.embed_text(&memory_text).await?;
let sql = self.storage.sql();
self.storage.transaction_sync(|| {
    let turn = insert_turn(&sql, &prompt, &answer)?;
    index.upsert_embedding(turn.to_string(), &memory, &embedding)
        .map_err(|error| Error::RustError(error.to_string()))?;
    append_messages(&sql, turn, &new_messages)
})?;
```

celld commits a SQL statement outside a transaction by itself. One transaction gives one commit and one replicated LTX segment for the group. celld holds the response until that commit is durable, so the handler does not need `storage.sync()`.

## Storage contract

- Vectors have a fixed width from `EmbedText::ndims()`, which reads the model's `Capabilities::ndims` for Rig models and are stored as little-endian `f32` BLOBs.
- The supported width is 1 through 8192 dimensions, matching celld's embedded sqlite-vec 0.1.9.
- Search uses cosine distance. Rig receives `1 - distance` as its similarity score.
- Equal scores are sorted by logical document ID in ascending order.
- A search embeds its query before it reads the index. Thus it sees writes that complete while it waits for the embedding provider. It sends no embedding request when the catalog is empty.
- Zero vectors and non-finite values are rejected for inserts and queries.
- Search filters are rejected. A fixed vec0 schema cannot safely translate Rig's arbitrary JSON filter algebra.
- A regular SQLite catalog stores each JSON document and its active logical generation. Vec0 rows contain the logical ID, generation, and embedding index.
- Inserting an existing ID is an upsert. All embeddings supplied for that document replace its prior generation.
- One upsert is one `transactionSync()` call. The call inserts the new vec0 rows with one fixed statement per vector, so celld's compiled-statement cache keeps one entry for it. A catalog `INSERT ... ON CONFLICT DO UPDATE` then makes the generation visible, and SQLite triggers remove older vector generations. A failure rolls back all of these writes.
- `InsertDocuments::insert_documents` writes the complete batch in one `transactionSync()` call. An invalid document rolls back the batch.
- Index writes do not leave orphan vector rows. Reads join through the active catalog, so they cannot observe rows that other writers left. `collect_garbage` removes such rows and is idempotent.
- List and migration cursors contain `(id, generation)`. They never expose vec0 row IDs.
- The schema table records layout version and vector width. Reopening with another width fails instead of silently changing an existing index.

## Migration

Do not change a Durable Object's storage backend or vector width in place.

1. Add a new Durable Object class and Wrangler migration tag for the target schema.
2. List the source through its logical `(id, generation)` cursor.
3. Upsert each logical document into the target class with its target embedding model.
4. Verify counts and retrieval before routing traffic to the target class.
5. Keep the source class available until rollback is no longer required.

`SqliteVecIndex` rejects an incompatible table layout as `SchemaMigrationRequired`; it does not alter or reinterpret it. Schema initialization runs in one transaction, so a rejected open does not leave new tables behind.

## Conformance

[`examples/celld-conformance`](examples/celld-conformance/README.md) runs CRUD, duplicate-ID and multi-embedding upsert, score normalization, stable ties, logical pagination, forced catalog-switch rollback, upsert rollback, nested commit and rollback, inner rollback inside a committed transaction, batch rollback, search during a concurrent write, orphan isolation and collection, restart/reopen, and concurrent requests against a live celld Durable Object. The fixture uses the same test sequence that a future backend must satisfy.

## Examples

| Example | Description |
|---|---|
| [`celld-agent`](examples/celld-agent/README.md) | A Rust Durable Object that keeps chat history and semantic memory in its private SQLite database. |
| [`celld-conformance`](examples/celld-conformance/README.md) | A live celld test fixture for the `rig-celld` storage contract. |

Both examples target `wasm32-unknown-unknown` and need `worker-build`, celld, and `esbuild` on `PATH`. celld uses esbuild to bundle the `worker-build` output:

```sh
rustup target add wasm32-unknown-unknown
cargo install worker-build
curl -fsSL https://celld.dev/install.sh | sh
npm install --global esbuild
```

The examples are architecture references. Add authentication, history compaction, and request idempotency before you use them in production. See the [workspace constraints](../README.md#constraints) for the Workers ABI requirement.

## Portability

The `storage` feature works on Cloudflare and celld without a compatibility flag. The `sqlite-vec` adapter is specific to celld. Cloudflare Durable Object SQLite does not allow the `sqlite-vec` extension or the `vec0` virtual table. The remaining Rig agent code can use another `VectorStoreIndex` on Cloudflare.
