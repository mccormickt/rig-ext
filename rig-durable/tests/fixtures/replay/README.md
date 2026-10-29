# Current replay recordings

These histories were exported from real local Duroxide and Temporal runtimes
with Rig 0.45.0 and the persistent runner. Models and tools use deterministic
test fixtures. These recordings test current restart and replay behavior, not
compatibility with previous crate versions.

Duroxide recordings cover all three continuation windows, parallel code-mode
children with worker restart, and fast-branch progress with race-loser drain.
Temporal recordings cover retry, approval, session steering, and code-mode
child race/restart. Temporal still uses the typed Rig state machine; these
histories do not prove Temporal runner integration.

Record to a new directory with the existing test export hooks:

```sh
mkdir -p /tmp/rig-replay
export RIG_DURABLE_RECORD_DIR=/tmp/rig-replay
cargo test -p rig-durable --all-features --test checkpointing threshold_one
cargo test -p rig-durable --all-features --test facade code_mode
# Start a local Temporal development server on port 7233 first.
export TEMPORAL_ADDRESS=localhost:7233 TEMPORAL_NAMESPACE=default TEMPORAL_TLS=false
cargo test -p rig-durable --all-features --test temporal temporal_retries_tools_with_stable_invocation_identity -- --ignored
cargo test -p rig-durable --all-features --test temporal temporal_approval_executes_only_approved_tools -- --ignored
cargo test -p rig-durable --all-features --test temporal temporal_session_serializes_updates_and_drains_active_steering_on_close -- --ignored
cargo test -p rig-durable --all-features --test temporal temporal_code_mode_out_of_order_child_race_restart_and_replay -- --ignored
```

Replace these recordings when the implementation changes. Replay needs no
server or live activities:

```sh
cargo test -p rig-durable --all-features --test recorded_replay
cargo test -p rig-durable --all-features --lib replays_recorded
```

Duroxide prefix checks compare newly emitted schedule inputs and child IDs.
Temporal SDK replay checks command/history compatibility, not exact activity
input or completion payload equality. Live tests also check the final results.
