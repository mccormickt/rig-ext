# rig-ext

Extensions and integrations for the [Rig](https://github.com/0xPlaygrounds/rig) agent framework.

## Crates

| Crate | Description |
|---|---|
| [`rig-durable`](rig-durable/README.md) | Durable execution for Rig agents. Uses Duroxide by default. Temporal support is behind the `temporal` feature. |
| [`rig-a2a`](rig-a2a/README.md) | Uses remote Agent2Agent (A2A) services as Rig completion models and agents. |
| [`rig-celld`](rig-celld/README.md) | Storage contracts and a `sqlite-vec` vector-store adapter for celld Durable Objects. |

## Build

You need Rust with the 2024 edition (1.85 or later). Check the native crates:

```sh
cargo check --workspace
```

Each crate README lists its examples and any extra build requirements.

## Constraints

- The workspace pins `worker` to 0.8.3. Rig 0.43 uses `wasm-streams` 0.5. workers-rs 0.8.4 and later use `wasm-streams` 0.6. Both versions in one Worker make duplicate WASM symbols.
- `rig-a2a` supports native targets only.
- `rig-celld` requires celld 0.6.0 or later. See the [`rig-celld` README](rig-celld/README.md#configure-celld).
