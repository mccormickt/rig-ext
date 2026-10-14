# Native code-mode scheduler baseline

These histories were captured from actual Duroxide 0.1.30 and Temporal 1.32.0
runtimes at `39b3a291228357cb26621e21ea9986f9407f28a6`, before the scheduler
extraction. Only test recording hooks were added. Production scheduler and
adapter files were unchanged. The models are deterministic Rig mock models;
workflow execution, activities, child workflows, and replay use native backends.

| Recording | Events | Scenario |
| --- | ---: | --- |
| `duroxide-codemode-children.json` | 34 | Parallel child agents, barrier, interrupted activity, worker restart, stable logical identity, dependent review child |
| `duroxide-codemode-race.json` | 37 | Schema rejection, fast branches with slow call pending, concurrency limit 2, provisional report, race-loser drain |
| `temporal-codemode-race.json` | 116 | Blocked child on independent queue, fast follow-up, parent worker replacement, ordered all/race results, blocked race loser, structured refusal |

Capture commands on the baseline, with the recording hooks from `facade.rs`
and `temporal.rs` applied:

```sh
mkdir -p /tmp/codemode-baseline
RIG_DURABLE_RECORD_DIR=/tmp/codemode-baseline cargo test -p rig-durable --features codemode,temporal --test facade code_mode
# Start a local Temporal development server on port 7233.
RIG_DURABLE_RECORD_DIR=/tmp/codemode-baseline TEMPORAL_ADDRESS=localhost:7233 cargo test -p rig-durable --features codemode,temporal --test temporal temporal_code_mode_out_of_order_child_race_restart_and_replay -- --ignored
```

Do not overwrite these baseline files when recording changed code.

Replay requires no server, models, or tool execution:

```sh
cargo test -p rig-durable --features codemode,temporal --lib replays_baseline
cargo test -p rig-durable --features codemode,temporal --test recorded_replay temporal_replays_baseline
```

The Duroxide test removes the terminal event before full replay and compares the
complete report. It also replays each pre-schedule prefix and compares the new
schedule event, including exact wire input and child instance ID, plus requested
child version. Full Duroxide replay alone restores child IDs from history rather
than checking newly emitted IDs. Temporal uses the SDK workflow replayer.

Temporal SDK replay checks command-history compatibility, not exact input or
completion payload equality. The native adapter payload construction remains
unchanged. The baseline Temporal recording releases its loser before the
provisional `Done`; provisional-report draining is covered by the Duroxide
recording and the shared state test. The live Temporal test also holds the loser
until a completed planner activity records a provisional report, then checks its
discarded delivery in the final report. That live check is not baseline replay.

A scratch mutation that replaced the extra admission `Plan` with `Execute`
failed both replay checks: Duroxide reported a custom-status command mismatch;
Temporal reported child-workflow/activity command nondeterminism. The mutation
was removed, then both checks were rerun. This tests the proof harness, not an
exhaustive set of historical workflows. Existing native names and wire types
are retained; fixtures cover only the scenarios above.
