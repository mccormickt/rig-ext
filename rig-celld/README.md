# rig-celld

`rig-celld` contains reusable storage contracts and Rig adapters for workers-rs applications on celld. Durable Object classes, routes, authorization, conversation state, and provider configuration stay in application crates.

## Features

| Cargo features | Contents | Native | WASM runtime requirement |
| --- | --- | --- | --- |
| none | Memory models, logical cursors, backend contract, and capability types | Supported | No Workers binding |
| `sqlite-vec` | `SqliteVecIndex<M>`, Rig `VectorStoreIndex`, Rig `InsertDocuments`, and Durable Object SQL storage | Compiles for tests | celld with the Wrangler `sqlite_vec` compatibility flag |
| `turso-encrypted` | Not published | Not supported | Blocked on atomic multi-chunk file writes and a real durability barrier |

The encrypted Turso backend remains outside the feature graph. The tested `DurableIo` prototype performs one host SQL operation per 4 KiB chunk and implements `sync` as a no-op. A persistent torn write was reproduced with celld 0.4.0. Publishing an experimental adapter before those two contracts are fixed would give applications a false durability guarantee.

## Configure celld

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

Construct the index inside that object:

```rust,ignore
let sql = self.state.storage().sql();
let model = openai_client.embedding_model_with_ndims(
    openai::TEXT_EMBEDDING_3_SMALL,
    512,
);
let index = SqliteVecIndex::new(sql, model)?;

index.upsert_text("turn-42", &memory, &memory_text).await?;

let agent = openai_client
    .agent(openai::GPT_4O_MINI)
    .dynamic_context(5, index)
    .build();
```

`SqliteVecIndex::named` selects another SQL table prefix and the JSON field that Rig's generic `InsertDocuments` implementation reads as the document ID.

## Storage contract

- Vectors have a fixed width from `EmbeddingModel::ndims()` and are stored as little-endian `f32` BLOBs.
- The supported width is 1 through 8192 dimensions, matching celld's embedded sqlite-vec 0.1.9.
- Search uses cosine distance. Rig receives `1 - distance` as its similarity score.
- Equal scores are sorted by logical document ID in ascending order.
- Zero vectors and non-finite values are rejected for inserts and queries.
- Search filters are rejected. A fixed vec0 schema cannot safely translate Rig's arbitrary JSON filter algebra.
- A regular SQLite catalog stores each JSON document and its active logical generation. Vec0 rows contain the logical ID, generation, and embedding index.
- Inserting an existing ID is an upsert. All embeddings supplied for that document replace its prior generation.
- The visibility switch is one catalog `INSERT ... ON CONFLICT DO UPDATE` host-SQL operation. SQLite triggers remove older vector generations in the same statement.
- A fault before the catalog switch can leave a new vector generation as an orphan. Reads join through the active catalog and cannot observe it. `collect_garbage` removes such rows and is idempotent.
- List and migration cursors contain `(id, generation)`. They never expose vec0 row IDs.
- The schema table records layout version and vector width. Reopening with another width fails instead of silently changing an existing index.

## Migration

Do not change a Durable Object's storage backend or vector width in place.

1. Add a new Durable Object class and Wrangler migration tag for the target schema.
2. List the source through its logical `(id, generation)` cursor.
3. Upsert each logical document into the target class with its target embedding model.
4. Verify counts and retrieval before routing traffic to the target class.
5. Keep the source class available until rollback is no longer required.

The append-only sqlite-vec prototype does not have generation columns. `SqliteVecIndex` rejects that table as `SchemaMigrationRequired`; it does not alter or reinterpret it. Native sqlite-vec and encrypted Turso must also use separate Durable Object classes and schema versions.

## Conformance

[`examples/celld-conformance`](examples/celld-conformance/README.md) runs CRUD, duplicate-ID and multi-embedding upsert, score normalization, stable ties, logical pagination, forced catalog-switch rollback, orphan isolation and collection, restart/reopen, and concurrent requests against a live celld Durable Object. The fixture uses the same test sequence that a future backend must satisfy.

## Examples

| Example | Description |
|---|---|
| [`celld-agent`](examples/celld-agent/README.md) | A Rust Durable Object that keeps chat history and semantic memory in its private SQLite database. |
| [`celld-conformance`](examples/celld-conformance/README.md) | A live celld test fixture for the `rig-celld` storage contract. |

Both examples target `wasm32-unknown-unknown` and need `worker-build` and celld:

```sh
rustup target add wasm32-unknown-unknown
cargo install worker-build
```

The examples are architecture references. Add authentication, history compaction, and request idempotency before you use them in production. The workspace pins `worker` to 0.8.3; see the [workspace constraints](../README.md#constraints).

## Portability

This implementation is specific to celld. Cloudflare Durable Object SQLite does not allow the `sqlite-vec` extension or the `vec0` virtual table. The remaining Rig agent code can use another `VectorStoreIndex` on Cloudflare.
