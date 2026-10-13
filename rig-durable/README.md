# rig-durable

Run Rig agents on Duroxide, Temporal, or SQLite Durable Objects. Model responses
and tool results are recorded by the backend. Replay uses those records instead
of repeating completed I/O. Tools with external effects must define their replay
safety and idempotency policy.

## Features

| Cargo features | Entry point | Runtime |
|---|---|---|
| Default: `duroxide`, `sqlite` | `DurableAgent::builder`, `AgentOrchestrator::sqlite` | Embedded Duroxide and SQLite |
| `duroxide`, without defaults | `AgentOrchestrator::builder` | Application-supplied Duroxide provider |
| `temporal`, without defaults | `temporal::TemporalAgent::new` | Temporal server and worker |
| `durable-object`, without defaults | `durable_object::Builder::new` | Cloudflare or celld SQLite Durable Objects |
| `codemode` with a native backend | `.code_mode(...)` | Durable JavaScript composition through QuickJS |
| None | Shared contracts and configuration | Native or Wasm, without a runtime |

Backend features are independent. Enable `temporal` with defaults to use both
native backends. Use only `durable-object` on `wasm32-unknown-unknown`.

## Guides

- Backends: [Duroxide](docs/guide.md#duroxide), [Temporal](docs/guide.md#temporal),
  and [Durable Objects](docs/guide.md#durable-objects).
- Conversations: [sessions](docs/guide.md#sessions),
  [compaction](docs/guide.md#compaction), and [detailed results](docs/guide.md#detailed-results).
- Execution: [streaming](docs/guide.md#streaming-completion),
  [checkpointing](docs/guide.md#checkpointing), [human approval](docs/guide.md#human-approval),
  and [durable code mode](docs/guide.md#durable-code-mode-workflows).
- Contracts: [tool replay safety](docs/guide.md#tool-replay-safety)
  and [architecture](docs/guide.md#architecture).

The Rust API docs contain checked examples for models, tools, sessions,
approvals, and compaction:

```sh
cargo doc --locked -p rig-durable --features temporal,codemode --no-deps --open
```

## Examples and checks

Most [native examples](examples) use controlled models and need no API key.
`durable_agent` uses OpenAI and requires `OPENAI_API_KEY`.
The [Durable Object example](examples/durable-object/README.md) runs on celld
or Wrangler without `sqlite-vec`.

```sh
cargo test --locked -p rig-durable --all-features
cargo clippy --locked -p rig-durable --all-targets --all-features -- -D warnings
cargo check --locked -p rig-durable --no-default-features
cargo check --locked -p rig-durable --no-default-features --features temporal,codemode
cargo check --locked -p rig-durable --no-default-features --features durable-object \
  --target wasm32-unknown-unknown
```

Live Temporal tests are ignored by default and require a development server.
See the [Temporal guide](docs/guide.md#temporal) for server configuration.
