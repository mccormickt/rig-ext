# Replay contracts

- Keep journal checks, input schema validation, result projection, and report
  construction in portable Rust. Runtime adapters never execute host tools.
- Use `replay::ReplayPlanner` for durable planning. Keep `replay::step` and
  ordinary `CodeMode::execute` cancellation behavior compatible.
- Persist the planner's immutable runtime identity with the source, catalog,
  and limits. Do not replay a history with a different engine or artifact.
- Checked prefixes are temporary transport state, not a durable journal.
  Compare each complete event batch before supplying its one selected reply.
  Drain guest jobs after each reply. Reconstruct output; do not append repeated
  prefix output. Retain already-admitted race losers until the host drains them.
- Bound full request and response prefixes, total executed work, and
  host-observed exchange time across fresh executions. Bound response bodies
  before parsing. JavaScript wire integers must round-trip exactly.
- Keep native threads and channels out of WASM contracts. Use Rig's
  `WasmBoxedFuture` and compatibility bounds for local WASM futures.

# Runtime adapters

- Share Rust guest bindings through `rig-codemode-core`; do not duplicate helper
  semantics or add a JavaScript prelude.
- Build replay assets in Cargo's OUT_DIR from `runtime/guest` with pinned tools.
  Do not check in generated JS/WASM or download npm tools from build scripts.
  Keep required redistribution notices and lock files with the build sources.
- Native and WASM QuickJS builds are not history-compatible.
  Fingerprints must pin the engine, exact runtime assets, adapter policy, and
  aggregate replay budgets. Persist script limits separately.
- Give each exchange a fresh interpreter and no guest I/O capabilities.
  Block outbound access. Enforce heap, stack, job, interrupt, message, and output
  ceilings. Use integer work budgets; Workers clocks can freeze during CPU work.
- Schema failures remain catchable. Do not dispatch invalid calls, but admit
  valid parallel calls before settling local schema failures. Cover sparse
  history, asymmetric races, discarded replies, and new unawaited calls.
- Cleanup releases runtime resources; it does not prove cancellation of an
  external effect. Cleanup errors must not replace the primary outcome.
