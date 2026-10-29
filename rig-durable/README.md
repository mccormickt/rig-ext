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

## Persistent Rig runner

Duroxide, Temporal, and Durable Objects use `persistent::Run`, backed by Rig 0.45's
`AgentRunner::resume`, custom `Serve` handlers, `EffectLogRecorder`, replay
stamps, and by-ID `EffectLogReplayer`. The runner plans work without I/O.
Backend adapters retain durable admission, independent settlement, approvals,
retry timers, child protocols, and invocation guards. Code-mode scheduling
stays in `SchedulerState`.

Each prompt retains its initial Rig seed and incrementally added effect records.
Sparse recovery can replay a later settled call while an earlier call remains
pending. Replay work has a local poll budget; prompt turn limits and backend
payload limits also apply. SQL and Duroxide checkpoints include the complete
prompt log; Temporal rebuilds it from command results in workflow history.
Prompt boundaries discard the in-memory log.
See [architecture](docs/guide.md#architecture) for storage costs and limits.

Temporal SDK 1.1 rebuilds each run from workflow input and history. The shared
planner requires no `Send` workflow context and never polls backend futures.
SDK-owned command replay remains the execution authority; an external Rig log
cannot replace activity or child commands. Nondeterministic-future detection
stays enabled.

This crate is pre-release. Persisted state and workflow histories are not
compatible across crate or Rig upgrades. Use fresh executions and storage when
the persistence format changes.

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

The isolated [Rig-native replay prototype](examples/rig-native/README.md) uses
Rig 0.45's runner and effect logs with a SQLite approval and intent journal.
Its process-restart tests are separate from the production backend tests below.

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
