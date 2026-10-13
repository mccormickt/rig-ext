# rig-ext

Extensions and integrations for the [Rig](https://github.com/0xPlaygrounds/rig) agent framework.

## Crates

| Crate | Description |
|---|---|
| [`rig-durable`](rig-durable/README.md) | Durable Rig agents on Duroxide, Temporal, or SQLite Durable Objects (Cloudflare and celld). |
| [`rig-a2a`](rig-a2a/README.md) | Uses remote Agent2Agent (A2A) services as Rig completion models and agents. |
| [`rig-celld`](rig-celld/README.md) | Durable Object SQL storage and a celld `sqlite-vec` vector-store adapter. |
| [`rig-codemode`](rig-codemode/README.md) | Runs model-written JavaScript that composes approved Rig tools in a bounded QuickJS sandbox. Optional `mcp` feature for `rig-rmcp` tools. |
| [`rig-typesafeai-tools`](rig-typesafeai-tools/README.md) | Exposes host-configured Jev evaluations as Rig tools for direct or code-mode calls. |

## Build

Use the stable Rust toolchain. All crates use the Rust 2024 edition. Run the
native checks from the workspace root:

```sh
cargo check --locked --workspace
cargo test --locked --workspace --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

The crates are unpublished. Use a local path dependency when integrating them
into another workspace. Dependency versions and features are defined in
[`Cargo.toml`](Cargo.toml) and each crate's manifest.

Each crate README lists its API guide, examples, feature choices, and extra
build requirements. Native tests do not exercise Workers bindings; use the
Wasm checks and celld conformance example for those integrations. Temporal's
live tests require a development server and are ignored by default.

## Constraints

- The workspace pins `worker` because its `wasm-streams` ABI must match Rig and reqwest. Mixing those ABIs in one Worker creates duplicate WASM symbols.
- `rig-a2a` supports native targets only.
- `rig-durable` supports WASM with default features disabled. Its `duroxide` and `temporal` backends are native-only; use `durable-object` on Workers.
- `rig-celld/storage` supports Cloudflare and celld SQLite Durable Objects. `sqlite-vec` requires celld 0.6.0 or later. See the [`rig-celld` README](rig-celld/README.md#configure-celld).
