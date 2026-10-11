# Rig 0.44 / persistence generation 2 recordings

These six histories were exported from actual Duroxide and Temporal runtimes
using the current tests with Rig 0.44, not transformed from legacy histories.
The old fixtures under `../legacy` are unchanged.

To record into a new directory (never overwrite legacy fixtures):

```sh
mkdir -p /tmp/rig-generation-2
export RIG_DURABLE_RECORD_DIR=/tmp/rig-generation-2
cargo test -p rig-durable --all-features --test checkpointing threshold_one
# Start: temporal server start-dev --port 7233
TEMPORAL_ADDRESS=localhost:7233 cargo test -p rig-durable --all-features --test temporal temporal_retries_tools_with_stable_invocation_identity -- --ignored
TEMPORAL_ADDRESS=localhost:7233 cargo test -p rig-durable --all-features --test temporal temporal_approval_executes_only_approved_tools -- --ignored
TEMPORAL_ADDRESS=localhost:7233 cargo test -p rig-durable --all-features --test temporal temporal_session_serializes_updates_and_drains_active_steering_on_close -- --ignored
```

Copy the resulting JSON here only when intentionally recording a new baseline.
`recorded_replay.rs` replays all six without a server or activities. Temporal
replay uses namespace `default`, matching the recording: logical approval IDs
include namespace and execution-chain identity. Duroxide replays all three
continue-as-new windows and compares continuation state and final output.
