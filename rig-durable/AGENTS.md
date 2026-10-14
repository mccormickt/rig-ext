# Durable execution

- Keep scheduling decisions in `codemode::SchedulerState`. Backend adapters
  own futures, timers, and persistence. Do not add a second scheduler.
- Preserve native schedule-command order and the `StepInput` wire contract.
  Reserve free slots before delivering one retained ready result. Ready results
  occupy slots. Deliver retained ready FIFO before observing new completions.
  Poll simultaneous completions in the scheduler's active admission order,
  including after retry or reopen.
- A DO drive must wait on the earliest stored retry or child-poll deadline as
  well as local effect futures. Retain slow futures while fast branches advance.
- Commit admissions before effects, observations before delivery, and delivery
  before replay. Never repeat an uncertain non-repeatable effect. A terminal
  script report or planner failure must drain queued and admitted work first.
- Reserve checkpoint space for bounded settlements and terminal control before
  accepting planner calls or effect results. Oversized results become committed
  nonretryable failures; interrupted results retain their uncertainty. Do not
  rewrite delivered history to fit a cap. Older checkpoints without drain
  headroom require an operator to increase the checkpoint cap.
  Reserve bounded terminal-output space in each containing script or child Call
  before batch admission and before resuming retained effects. Validate the
  replacement output too. A retained intent without space is nonterminally
  blocked by the fixed tool-call row cap; a larger session-history cap cannot
  enlarge that row.
- DO persistence retains serde's recursion guard. Bound script arguments,
  settled result envelopes, and terminal reports to 64 JSON containers before
  acceptance; fixed observation/delivery/report wrappers have 32 containers of
  headroom. All DO encodes reject depth above 96, leaving another 32 below the
  decoder's 128-container limit. Depth limits use the same bounded failure/drain
  paths as byte limits. Unsafe retained checkpoints stay nonterminal without
  new effects and need operator migration, not a disabled recursion guard.
- Persist source, runtime identity, catalog, limits, policies, child definitions,
  scheduler state, effects, and deadlines. Do not persist futures or DO stubs.
  Reject runtime, configuration, or implementation drift before new effects.
  Registration incompatibility is nonterminal: preserve receipts, active work,
  and checkpoints, rearm recovery, and return `Error::Incompatible` from alarms.
  Restore a matching registration to resume; do not cancel admitted children.
- Child DOs use ordinary namespaces. Derive child identities from the full
  parent call key and nested ordinal, not the ordinal alone. Retry the same
  committed request after uncertain acknowledgement. Poll committed receipts
  and results; do not restart a completed child to retrieve its result.
  Malformed or oversized transport replies are uncertain `Error::Protocol`,
  not terminal failures. Only decoded terminal replies establish termination.
  Validate retained identity/configuration/payload conflicts before checking
  live registration. A matching terminal result remains readable during drift.
- Applications own routes and authorization. Child protocol handlers validate
  expected configuration and identity before admission. Nested approvals and
  steering are not supported. Closing a session is not cancellation.
- SQL schema upgrades must be additive, versioned, and atomic. Keep version-2
  checkpoints readable and test rollback and reopen through real SQLite.
- Keep native QuickJS dependencies target-specific. The WASM DO feature path
  must not enable Duroxide or Temporal. Worker planners pin engine/assets with
  `ReplayPlanner::runtime_identity`; keep old artifacts while histories are live.
- Validate DO changes with `cargo test -p rig-durable --no-default-features
  --features durable-object,codemode --lib durable_object` and a WASM library
  check. Native SQLite/QuickJS tests do not prove Worker runtime behavior.
  Run the packaged Rust Worker conformance fixture on celld and local workerd
  for protocol, crash, and restart changes. Local workerd does not prove
  production Cloudflare CPU enforcement.
