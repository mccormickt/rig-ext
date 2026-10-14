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
