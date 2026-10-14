# Durable Object agent

This Worker uses a deterministic mock model and an approval-gated addition
tool. It needs no API key. The application owns the Durable Object class;
`rig-durable` owns SQL checkpoints, submissions, approvals, and recovery.
The Wrangler migration uses `new_sqlite_classes`. No `sqlite-vec` extension
or compatibility flag is needed.

Install pinned local tooling and build from the repository root:

```sh
scripts/workers/install.sh
scripts/workers/build.sh
```

In an orb, use the declared supervised services and run both runtime checks:

```sh
scripts/workers/conformance.sh
# Native checks, WASM feature isolation, and asset selection checks:
RUST_TEST_THREADS=1 scripts/workers/check.sh
```

This runs celld 0.6.2 on port 9876 and Miniflare 4.20260730.0 with workerd
1.20260730.1 on port 9877. It tests approval, denial, deduplication, conflicting
requests, busy rejection, and close. Then it restarts each service without
deleting SQL state and checks retained results and a pending approval.
The services are not public portals because this example has no authentication.

The default `conformance` feature adds `ScriptSession` and `ChildSession` to
this same Worker. The planner runs the packaged QuickJS-WASM interpreter through
a real Loader. The children use ordinary Durable Object namespaces and real
SQL-backed engines. Only model completions are deterministic mocks. There is
no mock namespace or Loader at these runtime boundaries.

`workflows.py` checks twenty scenarios on each runtime:

- Ordinal 1 wins while ordinal 0 is gated. The committed delivery survives a
  process restart. Close does not finish the parent before the loser drains.
- A real child submit commits, then its acknowledgement is withheld. After
  restart, the retry uses the same identity, payload, receipt, and single effect.
- Malformed and oversized HTTP200 replies after real child admission remain
  uncertain. Submit and Poll preserve their acknowledgement state across
  restart and retry the same operation until the gated child completes.
- Planner failure drains both active and queued children.
- An effect commits before its result is recorded. Restart retains one effect
  and an interrupted durable envelope, rather than running it again.
- Two outer scripts with child ordinal 0 use different child identities.
- A schema-invalid promise chain wins while a valid child drains; the invalid
  call never reaches the child namespace.
- Runtime, catalog, policy-version, and limit drift each block new model,
  planner, or child I/O without closing the session or losing admitted work.
  Restoring the original configuration resumes the same winner and drains
  the same children.
- Selected ambient capabilities are absent from the guest interpreter.
- Fitting results succeed under a small checkpoint cap. Oversized individual
  results and aggregate checkpoints produce bounded tool errors, drain the
  admitted child, and do not repeat completed effects after restart.
- A two-script model batch without terminal-result space in the fixed intent
  row is blocked before any sibling dispatch. The running receipt and cached
  model response survive restart without a model repeat. A nearby fitting
  batch completes both scripts and drains its real gated child. A larger
  session-history cap does not change the fixed intent row cap.

The native command serializes test runners, not the concurrency inside each
test. Keep parallel native failures in the evidence rather than treating a
serial rerun as proof that parallel validation passes.

The gates use persistent app SQL controls. Independent append-only SQL tables
record effects, logical identities, and child protocol requests. The test waits
for the named commit boundary before each restart; a sleep does not select the
failure window. JSON evidence and logs are written to `.amp/in/artifacts/`.
Run a single runtime without rebuilding with:

```sh
python3 rig-durable/examples/durable-object/workflows.py --runtime celld \
  --evidence .amp/in/artifacts/workflows-celld.json
# Use --runtime workerd for the other declared service.
# Add --outer-boundary-only to run only the large-input batch boundary/control.
```

Fault routes are `/fixture/<session>/<operation>`. They expose unauthenticated
local controls, not a production API. Do not expose this fixture to other users.
`--no-default-features` is a compile-only check with the supplied configuration:
Wrangler still names the fixture classes, which that build does not export.
An approval-only runtime needs a matching configuration with only `AgentSession`
and no Loader binding. The default build preserves its existing routes and
approval/deduplication/reopen controls.

Outside an orb, run either server in a terminal from the repository root:

```sh
PATH="$HOME/.local/bin:$PWD/scripts/workers/node_modules/.bin:$PATH" \
  celld dev rig-durable/examples/durable-object --no-watch --port 9876
# Or, in another terminal:
node scripts/workers/miniflare.mjs rig-durable/examples/durable-object 9877
```

Run the API check, stop and restart the selected server without `--clean`,
then run its reopen check. Use a new prefix for each independent run:

```sh
python3 rig-durable/examples/durable-object/conformance.py --session-prefix demo
python3 rig-durable/examples/durable-object/conformance.py --session-prefix demo --verify-reopen
# Add --url http://127.0.0.1:9877 for workerd.
```

Each first path segment selects an object: `/<session>/submit`, `/status`, `/approval`,
`/result?request_id=<id>`, `/transcript`, and `/close`. Mutations use POST;
reads use GET. Submit bodies use the serde format of `SubmitInput`; approval
bodies use `ApprovalDecision`. The conformance script contains examples.

The example does not authenticate requests. Add authentication and access
control before deployment. Keep one engine per object. Initialization is
retried after storage errors; request handlers read only committed state.

Local workerd does not enforce production dynamic-Worker CPU or subrequest
limits. Local conformance does not prove Cloudflare production enforcement.
Production deployment is a separate action and is not part of these commands.
The Loader and Worker planner owners provide separate real runtime fixtures for
outbound denial, body bounds, Unicode boundaries, divergent replay, aggregate
budgets, and infinite CPU/job chains. See their crate READMEs; the workflow
fixture does not replace those tests or claim to prove cleanup or memory safety.

`scripts/workers/assets.py` verifies Cargo's selected runtime asset paths,
fingerprints, engine package pins, and MIT notices. Ordinary Cargo builds use
the checked-in assets without npm regeneration. The app build copies the notices
and asset fingerprint beside its Worker bundle; conformance checks those copies.
This is not registry package validation: `cargo package --no-verify` is blocked by the workspace's path-only
`rig-celld` dependency. Publication and release ordering are outside this example.
