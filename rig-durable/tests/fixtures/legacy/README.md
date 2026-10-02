# Recorded Legacy histories

These histories were recorded by the unchanged runtime at baseline
`1bd458dc474f907aa5f59b70d37b1f7d417f5133`. They are not synthetic payloads.
`record.patch` adds only history exports to that revision's existing tests.

To regenerate, apply that patch in a separate checkout of the baseline, create
`/tmp/legacy-histories`, start a Temporal dev server, then run:

```sh
TEMPORAL_ADDRESS=localhost:7233 cargo test -p rig-durable --no-default-features --features temporal --test temporal -- --include-ignored --test-threads=1
cargo test -p rig-durable --test checkpointing threshold_one
```

Copy the JSON files from `/tmp/legacy-histories` here. The Temporal fixtures
cover a retried tool, approval, and active steering. The three Duroxide fixtures
cover each execution window of one continue-as-new chain. Replay tests need no
server and register no activities. They fail if replay schedules incompatible
commands or changes the retained state.
