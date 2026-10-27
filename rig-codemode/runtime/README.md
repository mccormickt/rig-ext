# Worker replay runtime

Enable `rig-codemode/worker-replay` without `quickjs` for a Workers build.
The `WorkerReplayPlanner` type is available on `wasm32-unknown-unknown`.
It uses the independent `rig-celld/dynamic-workers` Loader capability.

```rust,ignore
use rig_codemode::replay::{ReplayInput, ReplayPlanner};
use rig_codemode::worker_replay::{ReplayBudget, WorkerReplayPlanner};

let planner = WorkerReplayPlanner::from_env(&env, "LOADER", ReplayBudget::default())?;
let identity = planner.runtime_identity();
// Persist identity with source, catalog, and Limits before admitting effects.
let next = planner.plan(ReplayInput {
    code: &code,
    catalog: &catalog,
    limits,
    admitted: &admitted,
    history: &history,
}).await?;
```

Declare a Worker Loader binding named `LOADER`. The application executes admitted
tool calls and persists their results. The planner never executes tools. It has
no dispatcher, RPC callbacks, service bindings, secrets, or inherited environment.
The guest has no Workers APIs. Outbound access is explicitly denied.
This backend does not implement ordinary `CodeMode::execute` and does not change
that method's native cancellation behavior.

## Rust/WASM adapter

The Worker runtime shares the native Rust `Session` bindings, catalog, output
writer, return conversion, and guest prelude. Rust owns the replay exchange, prefix
checks, integer work counters, and transport accounting. JavaScript only loads
the WASM module, reads a bounded HTTP body, and returns the response. The engine
is still QuickJS C compiled through `rquickjs`; this is not a Rust JavaScript
engine. The shared `prelude.js` remains JavaScript.

The standalone artifact crate contains the small unsafe job-step FFI call;
`rig-codemode` retains `forbid(unsafe_code)`. It preserves pending job exceptions
instead of using the binding's boolean job API, which clears them. A stalled
root promise with unhandled rejections is a script error; a stalled root with no
unhandled rejection remains `stalled`. The runtime counts rejections without
retaining guest handles. Handled failures do not prevent normal completion.

Guest diagnostic and discovery strings use checked UTF-8 conversion. Invalid or
oversized diagnostic fields use a bounded `error` fallback before Rust copying.

The WASM module has no WASI, clock, or network imports. A constant clock supplies
the engine's hidden clock hook; guest clocks and random values remain unavailable. Heap limits apply
to QuickJS allocations, not all Rust or Worker allocations. Non-polling engine
built-ins still require platform CPU isolation. Local tests do not prove
Cloudflare production enforcement or hard cancellation.

## Exchange contract

Each exchange creates a fresh anonymous Worker and QuickJS interpreter. The
shared `prelude.js` supplies the frozen tool table, `text`, `searchTools`, and
`describeTool`. Date, WeakRef, and FinalizationRegistry are absent; Math.random
throws. Engine JSON APIs and Rust-owned state keep trusted protocol operations
separate from guest globals. Guest serialization can still execute `toJSON`.

The guest emits an ordered Call/Rejected batch followed by Idle or Done. It
compares the complete expected batch before injecting one selected reply, then
drains the job queue. Reentrant argument serialization can emit ordinal 1 before
ordinal 0; arrival order is preserved. The portable Rust checker validates
schemas, projections, history, and prefixes. Only new events advance host state.
Output snapshots are reconstructed, not appended across exchanges.

`Done.returned` is null or a JSON string on the private wire, not a nested
transport object. This preserves the native return-depth ceiling. Strings cross
the VM boundary with explicit lengths so embedded NUL bytes are not truncated.

## Limits and identity

The guest enforces `Limits` heap, call, message, and output ceilings. The WASM
stack ceiling is fixed at **64 KiB**. Each exchange permits at most 10,000
interrupt callbacks and 100,000 jobs; aggregate work charges both and Rust
discovery checks, including re-executed prefixes. These counters stop CPU loops
and job storms without depending on a clock that can freeze during Worker CPU execution.

`ReplayBudget` defaults to 64 MiB aggregate request/response bytes, 1,000,000 work
units, and 30 seconds. Elapsed time is also bounded by `Limits.wall_time`. The
host measures startup, transport, body read, and decode. A post-decode timer
checkpoint advances a frozen Workers clock; the deadline race covers that
checkpoint too. A hung response or body cannot wait without a deadline.

Each request has an independent 16 MiB transport cap. Rust already debits its
encoded size before the exchange; the remaining byte allowance applies to the
response, not to the request a second time. Response bytes are bounded before
Rust parsing, and actual raw body length replaces guest usage. The Rust runtime
charges escaped event strings and repeated boundary/output snapshots before
retaining them. Transport exhaustion is fatal even if guest code catches a
bridge error. Return byte length is checked before Rust copying and JSON parsing.
Final serialization has a second size check. Numeric budgets must
fit JavaScript's safe integer range; the host timer must fit its signed 32-bit
millisecond range. Transport depth is 128; discovery projection depth is 64.

The immutable planner identity includes JS/WASM SHA-256 fingerprints, engine
version, adapter version, compatibility date, CPU/subrequest profile, transport
cap, and all aggregate budget values. Persist it with source, catalog, and
`Limits`. Use a new history when these change. Response-envelope decoding is
separate from guest return conversion:
unrepresentable guest JSON (including unpaired surrogates and depth 128) produces
a `Done` script error, not a protocol error. Depth 127 remains valid.
Native and WASM use QuickJS-NG **0.16.2**, but their diagnostics, stack limits,
and compiled engine behavior can differ.
Differential tests demonstrate selected shared contracts, not interchangeable
histories.

These are not total Worker heap limits: WASM and its JavaScript wrapper also
consume platform memory. Select heap/message/output limits below platform
capacity. Cleanup releases handles and requests Worker disposal. It is not
acknowledged shutdown or cancellation of an external effect. The runtime does
not drain more jobs after interruption. Primary failures survive cleanup errors.

Tested platforms: celld 0.6.2 and workerd 1.20260730.1 through Miniflare
4.20260730.0. Loader probes measured that celld enforces CPU and subrequest
settings, while local workerd does **not** enforce either setting. Both deny
outbound access. QuickJS counters and transport bounds remain required. Workerd
uses GC-only Worker cleanup; celld requests asynchronous release. Cloudflare
production behavior and platform-level memory exhaustion are not tested here.

## Reproducible assets

Cargo uses checked-in `worker.js`, `quickjs.wasm`, `identity.txt`, and
`fingerprint.json`; it never runs npm or downloads runtime artifacts.

Build with Rust 1.99.1, the `wasm32-unknown-unknown` target, clang/lld (tested with
14.0.6), and `wasm-bindgen-cli` 0.2.129. The orb setup installs the build tools.
`crate/Cargo.lock` pins the standalone artifact's Rust dependencies, including
`rquickjs` 0.14.0 and `wasm-bindgen` 0.2.129. The `worker-runtime` Cargo feature
builds the interpreter; `worker-replay` only embeds its assets in the host planner.

```sh
# From rig-codemode/runtime:
npm ci
npm run build
npm audit

# From the repository root:
cargo test --locked -p rig-worker-tests --test replay -- --ignored --nocapture
```

The ignored Unix Rust integration test owns all 11 direct Dynamic Worker conformance
groups, HTTP status checks, and semantic assertions. It starts and cleans up
`miniflare.mjs`, a loopback-only Node adapter for the host Worker and `LOADER`.
An additional test loads the built assets through Node's VM module linker and
checks WASM memory growth for oversized returns and diagnostics against controls.
Rust owns the test assertions.

`build.sh` compiles Rust and runs wasm-bindgen. `build.mjs` bundles the generated
glue and `entry.js`, leaving `./quickjs.wasm` external. The Loader supplies that
file as a compiled WASM module, not a fetchable URL. npm supplies only development
tools: esbuild 0.28.2 and Miniflare 4.20260730.0, with sharp 0.35.5 and undici
7.30.1 overrides. `NOTICES.txt` contains Rust dependency and engine licenses;
review notices when dependency pins change.

Rebuild twice with the same toolchain and compare hashes to check reproducibility.
Cargo's package file list includes the assets, JavaScript source, npm lock, and
notices. The standalone artifact crate is built from the repository. Creating a
publishable `.crate` remains blocked by the workspace's unpublished path-only
dependencies; this runtime does not change publication policy.

## Real Rust/runtime conformance

The standalone fixture in `conformance/` compiles the actual public planner
and Loader adapter. It compares public steps/reports against native execution,
excluding engine-specific diagnostic wording and zeroing report elapsed time.
Its injected Loader controls test hung fetch/body, malformed transport, actual
byte accounting, and a CPU-frozen host clock. It is not an alternate planner.

```sh
# From rig-codemode/runtime/conformance; install worker-build separately:
cargo build --locked --features native --bin native-replay
worker-build --release --no-opt -- --locked --features worker

# Use separate supervised services for these alternatives:
node miniflare.mjs 9890
# Or add ../node_modules/.bin to PATH, then:
celld dev --no-watch --port 9891

# Test the local service that is running:
node differential.mjs http://127.0.0.1:9890
node differential.mjs http://127.0.0.1:9891
```

Restart local conformance services after rebuilding so they load the current assets.

The fixture covers deep promises, asymmetric schema-invalid/valid races, sparse
history, reentrant serialization, late losers, unawaited calls, output/message
bounds, return depth 127 versus 128 and unpaired surrogates, embedded NUL,
deterministic helpers, fresh isolation,
safe integers, aggregate budgets, and CPU/job/heap/stack failures. The direct
Dynamic Worker harness also tests tampered prefixes and escaped-wire transport
exhaustion. Tests run no external tool effects.
