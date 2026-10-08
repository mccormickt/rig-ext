# rig-ext

Extensions and integrations for the [Rig](https://github.com/0xPlaygrounds/rig) agent framework.

## Crates

| Crate | Description |
|---|---|
| [`rig-durable`](rig-durable/README.md) | Durable Rig agents on Duroxide, Temporal, or SQLite Durable Objects (Cloudflare and celld). |
| [`rig-a2a`](rig-a2a/README.md) | Uses remote Agent2Agent (A2A) services as Rig completion models and agents. |
| [`rig-celld`](rig-celld/README.md) | Durable Object SQL storage and a celld `sqlite-vec` vector-store adapter. |
| [`rig-codemode`](rig-codemode/README.md) | Runs model-written JavaScript that composes approved Rig tools in a bounded QuickJS sandbox. Optional `mcp` feature for `rig-rmcp` tools. |

## Build

You need Rust with the 2024 edition (1.85 or later). Check the native crates:

```sh
cargo check --workspace
```

Each crate README lists its examples and any extra build requirements.

## Constraints

- The workspace pins `worker` to 0.8.3. Rig 0.43 uses `wasm-streams` 0.5. workers-rs 0.8.4 and later use `wasm-streams` 0.6. Both versions in one Worker make duplicate WASM symbols.
- `rig-a2a` supports native targets only.
- `rig-durable` supports WASM with default features disabled. Its `duroxide` and `temporal` backends are native-only; use `durable-object` on Workers.
- `rig-celld/storage` supports Cloudflare and celld SQLite Durable Objects. `sqlite-vec` requires celld 0.6.0 or later. See the [`rig-celld` README](rig-celld/README.md#configure-celld).
