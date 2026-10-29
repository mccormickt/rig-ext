# Rig-native durable replay prototype

A local experiment with the published Rig 0.45 runner, recording, and replay
APIs. This is an independent Cargo workspace. It does not change the production
adapters or require a provider API key.

## Run

From the repository root:

```sh
cargo build --locked --manifest-path rig-durable/examples/rig-native/Cargo.toml \
  --target-dir target/rig-native
target/rig-native/debug/rig-native-prototype /tmp/rig-native-demo.db run
# Expected: approval required; the process exits without charging.
target/rig-native/debug/rig-native-prototype /tmp/rig-native-demo.db approve
target/rig-native/debug/rig-native-prototype /tmp/rig-native-demo.db run
target/rig-native/debug/rig-native-prototype /tmp/rig-native-demo.db run
```

Use a new database path for each experiment. Use `deny` instead of `approve`
to return a skipped tool result to the model. A completed run prints the same
answer on every restart without another live model call or tool effect.

```sh
cargo test --locked --manifest-path rig-durable/examples/rig-native/Cargo.toml \
  --target-dir target/rig-native
cargo clippy --locked --manifest-path rig-durable/examples/rig-native/Cargo.toml \
  --target-dir target/rig-native --all-targets -- -D warnings
```

## Execution boundary

- `AgentBuilder::over_handlers` installs host handlers. The tool is registered
  with `RegisteredTool::from_handler` through Rig's `ToolServer`.
- `agent.resume(checkpoint.state)` drives the entire agent loop. There is no
  host-owned `next_step` loop or manual tool-result construction.
- `EffectLogRecorder` records each dispatch. A `Recorder` adapter drains resolved
  records and commits them to SQLite before the runner can dispatch another
  effect. It stores only the newly drained records, not another full log copy.
- `AgentReplayExt::stamp` and `check_replayable` validate the run configuration.
  `Checkpoint<AgentRun>` retains the initial state and expected first log ID;
  `EffectLog::from_checkpoint` checks the log boundary on reopen.
- `EffectLogReplayer::for_key_by_id` answers saved effects and checks their
  requests. The host allows live execution only after all saved records have
  been consumed. A replay refusal never becomes a live request.
- Approval and guard state live outside the effect log. An intent binds the
  effect ID to its exact request. Approval does not make an uncertain effect
  safe to repeat. A saved settlement can answer a call even if its effect-log
  write did not complete.

`Recorder::resolve` cannot return a storage error. The adapter therefore stores
the failure in shared control state; each handler checks it before new work,
and the entry point checks it before reporting success. Approval and uncertainty
also use this control state. They are not saved as terminal effect answers.

The single database represents one logical execution. Its effect IDs are stable
because each restart uses the same initial run, handler keys, and sequential
dispatch order. They are not suitable as global idempotency keys across runs.

## Fault checks

The tests launch a fresh binary process for every run and decision. Fault
injection uses `process::exit(75)`, without Rust destructor cleanup.

| Exit point | Charges before exit | Recovery |
|---|---:|---|
| After claim, before charge | 0 | Refuse the uncertain intent |
| After charge, before settlement | 1 | Refuse; do not charge again |
| After settlement, before recording | 1 | Use the settlement, then continue |
| After recording, before the runner advances | 1 | Replay the recorded result |

To select a fault, pass `after-claim`, `after-effect`, `after-settle`, or
`after-record` after `run` on an approved database. Other tests cover repeated
approval pauses, denial feedback, complete-run replay, changed configuration,
request divergence, and SQLite write failures during recording. Independent
model and charge counters detect repeated I/O.

## Limits

- The model is scripted. The charge is a counter committed in a separate
  transaction to model an external effect. No payment service is contacted.
  Process termination tests do not prove machine power-loss behavior.
- One active runner per database; one prompt, one tool, unary completions,
  and sequential tools only. This is not a public service or a production API.
- The SQL journal implements the same claim-before-effect rule as
  `InvocationGuardStore`, but does not use the production executor.
- Recovery replays from the initial run. It does not bound replay work with
  intermediate runner snapshots. SQLite records retain complete requests;
  Rig's compressed JSONL writer is not used here.
- The recorder uses synchronous SQLite commits. Remote or asynchronous storage
  needs an awaitable commit boundary in a handler or in Rig's driver.
- Rig hooks run again during replay. This prototype does not establish durable
  execution for arbitrary hooks or direct I/O outside registered handlers.
- No Duroxide, Temporal, or Worker runtime is involved. The prototype tests do
  not establish backend recovery or scheduling guarantees.

For backend execution and recovery guarantees, see the
[durable execution guide](../../docs/guide.md#architecture). This example's
sequential SQLite journal does not replace backend scheduling or persistence.
