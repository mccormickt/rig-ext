# Rig actor on Agent Substrate: bounded prototype

**The full cluster proof is blocked, not passed.** On 2026-10-14, this orb ran
kind, Kubernetes, Substrate, two gVisor workers, and agentgateway. It registered
the Rust actor image, an ActorTemplate, and two actors. An A2A request sent while
`rig-1` was `SUSPENDED` reached `ResumeActor`, but startup failed before `runsc`
could create the sandbox:

```text
Linux 6.1.158+
overlayfs: unrecognized mount option "lowerdir+=..."
while creating overlay superblock: invalid argument
SUSPENDED → CRASHED; gateway HTTP 500
```

Substrate v0.4.0 requires Linux **6.5 or later** for this mount API. See its
[implementation](https://github.com/agent-substrate/substrate/blob/v0.4.0/internal/imagecache/bundle_linux.go)
and [contract](https://github.com/agent-substrate/substrate/blob/v0.4.0/internal/imagecache/README.md).
Changing the kind node image does not change the host kernel. No KVM, microVM,
unsandboxed fallback, or core patch is used to bypass this failure.

## Protocol and execution boundaries

```text
test client → loopback authenticated edge → real agentgateway substrateIngress
            → ResumeActor → worker atunnel → gVisor → Rust A2A server → Rig
                                                      ↑ blocked before here
```

The separate local component suite executes the same Rust server and ordinary
Rig AgentRunner without Substrate. Its deterministic model reads actual Rig
history, requests a registered file tool, and returns the actual tool result.
Only inference is simulated. The tool writes and fsyncs a file, counts calls
separately from commits, and generates a heap-only nonce on its first call.
Readiness does not create the nonce, so a golden snapshot should contain none.

The actor contains one workspace. Distinct A2A contexts have separate histories
and locks, but share its file journal. Use separate actors for separate owners.
The example rejects non-null A2A `tenant` parameters before task lookup or state
changes. Its task store is not a multitenant authorization boundary.
The fixture's operation-ID deduplication is local to that journal. It is **not**
an external-effect transaction protocol; snapshots do not give exactly-once
external effects. A repeated operation can invoke the tool again without a
second file commit. A process crash during a file write is outside this fixture's
contract.

A2A 1.0 JSON-RPC uses `SendMessage`, `SendStreamingMessage`, `GetTask`,
`SubscribeToTask`, and `CancelTask`, with `A2A-Version: 1.0`. The card advertises
`supportedInterfaces[].protocolVersion = "1.0"`. Missing, 0.3, and unsupported
version headers are rejected. Streaming means A2A task events, not model token
streaming. Resubscribe gives current task state plus later events, not missed
token replay. The gateway's native Substrate policy is not its separate A2A
inspection/card-rewrite policy. No version conversion or signed-card rewrite is
claimed. The fixture card URL is the loopback edge endpoint. The edge selects
the actor from bearer credentials at `/jsonrpc`; `/actors/<name>/jsonrpc` is
also available for explicit selection controls. Set `PUBLIC_URL` for a different
host. Do not use card discovery as the waking second turn.

**This is not an ACP test.** Request-triggered wake is HTTP routing and is not
exclusive to A2A. ACP could use a separate authenticated HTTP/WS-to-stdio host or
a compatible ACP server transport inside an actor. This prototype implements
neither. Hermes is a real ACP stdio peer, but a public reproducible
Hermes/Substrate ACP bridge was not identified. The `always-on-agent`
`acp-runtime.ts` implements OpenClaw's plugin API through OpenAI HTTP/SSE; that
does not establish ACP JSON-RPC interoperability. The standalone `rig-acp`
prototype is separate and is not copied into this checkout.

## Pins and prerequisites

| Component | Version/source |
|---|---|
| Substrate source, manifests, CLI, workers | v0.4.0, `756c2a53741121e728f4cc3066c8a19e575b4919` |
| agentgateway | `cr.agentgateway.dev/agentgateway:v0.0.0-alpha.168b05d6@sha256:5e8384d1fcfdc443a3ad5155ea6a64dc7953bde162df7f5f8e131a5c19ae08a3` |
| gVisor assets | release manifest's 2026-09-02 nightly; amd64 SHA-256 `d547d81401461fd1c679c5c4fa0a6c2b8ef7dc3c22ce23c9e25dcc4c69cfd06f` |
| kind / node | release tool module: kind 0.33.0 / Kubernetes 1.37.0 |
| Go | 1.27.0 |
| Rust / Rig | tested Rust 1.99.0 / Rig 0.44.0; workspace Cargo.lock |
| A2A crates | a2a-lf 0.4.1; a2a-server-lf 0.5.2; a2a-client-lf 0.2.8 |
| actor base | digest-pinned Debian trixie in Dockerfile |

Use a disposable Linux amd64 Docker host with kernel >=6.5, privileged container
support, mount/network namespaces, and writable cgroup v2 delegation. KVM is not
needed for gVisor systrap. Keep at least 8 GiB RAM and 15 GiB free disk available.
These are planning allowances, not measured minimums. Use a local Docker daemon;
the script's kernel preflight does not inspect a remote Docker host.

Install Go 1.27.0, Rust, protoc, kubectl 1.37.x, Docker, Python 3, and binutils.
The upstream tool modules pin kind and ko. `setup.sh` builds Substrate images
from the verified tag and pushes **only to the local registry**. The initially
tried `ghcr.io/agent-substrate/substrate/*:v0.4.0` path returned access denied;
it is not a prerequisite. Source builds use upstream base images; save their
resolved digests because they need not be bit-for-bit reproducible later.

## Deploy only to the disposable cluster

Run from this repository root. These commands write only to `kind-rig-substrate`.
Do not use them with shared clusters. The upstream creation script owns a local
`kind-registry` container; use a fresh Docker environment to avoid name conflicts.

```bash
bash scripts/substrate/setup.sh
export PATH="/tmp/rig-substrate-v0.4.0/bin:$PATH"
kubectl ate --context kind-rig-substrate get actor-template rig-poc -a rig-local -o json
```

Wait for `status.goldenSnapshotStatus` to contain its completed golden tag, with
no `errorMessage`. **Stop on an error.** Do not silently call cold startup a
golden restore. Create the logical actors through the Substrate API:

```bash
kubectl ate --context kind-rig-substrate create actor rig-1 -a rig-local --template rig-poc
kubectl ate --context kind-rig-substrate create actor rig-2 -a rig-local --template rig-poc
```

For the blocked-path diagnostic only, actors were deliberately created after
golden creation failed. Their initial SUSPENDED state contained no snapshot.
The waking request therefore tested an attempted cold start, not memory restore.

`ActorTemplate` is not a Kubernetes resource. `WorkerPool` is. Ordinary gateway
HTTP targets actor port 80. Templates use the v0.4.0 `onCommit: FULL` contract,
not main's newer fidelity fields. Kind's RustFS stores external snapshots.

Generate private disposable credentials; do not print or save them in evidence:

```bash
python3 - <<'PY'
import json, os, secrets
fd = os.open('/tmp/rig-poc-tokens.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
os.write(fd, json.dumps({k: secrets.token_hex(32) for k in ('admin','rig-1','rig-2')}).encode())
os.close(fd)
PY
```

In an orb, use supervised services, **without portals**:

```bash
amp orb service start rig-substrate-forward --command \
  'kubectl --context kind-rig-substrate -n ate-system port-forward --address=127.0.0.1 svc/atenet-router 18080:80 18082:8081'
amp orb service start rig-substrate-edge --command \
  'python3 scripts/substrate/edge.py --tokens /tmp/rig-poc-tokens.json'
```

On a standalone host, run those two commands in foreground terminals instead.
Do not expose these ports publicly. The edge is a small test boundary, not a
production HTTP proxy. Operator access to port-forward, the node, or the cluster
is trusted. It does not isolate untrusted co-tenants inside Kubernetes.

The edge authenticates each actor binding, rejects **all** client actor-selector
headers, generates one trusted selector, and rejects CONNECT. Native gateway
duplicate-header behavior is a separate control: the pinned gateway selects the
first header on ordinary HTTP and on an inner HTTP request after CONNECT. This
is a failed security expectation, consistent with
[issue #2460](https://github.com/agent-substrate/substrate/issues/2460).
CONNECT 200 alone does not mean the actor woke.

## Execute and inspect the tests

Use a new evidence directory for every run; scripts refuse to overwrite one.
Evidence includes UTC times, inputs, expected values, HTTP/protocol responses,
CLI exit codes, source hashes, lock hash, binary hash, actor metadata, and image
digests. The harness omits credential headers and redacts its known test tokens.
Review upstream command logs for other secrets before sharing. Only synthetic
prompts are used.

```bash
cargo test --locked -p rig-a2a --features server
cargo check --locked -p rig-a2a --no-default-features
cargo check --locked -p rig-a2a --no-default-features --features server
cargo build --locked -p rig-a2a --features server --example substrate_actor
python3 scripts/substrate/local.py --evidence .amp/in/artifacts/substrate/local-new

# Run against a newly created SUSPENDED rig-1. This characterizes failure too.
python3 scripts/substrate/cluster_probe.py --tokens /tmp/rig-poc-tokens.json \
  --evidence .amp/in/artifacts/substrate/cluster-new

# On a supported host, with fresh actors and a successful golden snapshot:
python3 scripts/substrate/demo.py --tokens /tmp/rig-poc-tokens.json \
  --evidence .amp/in/artifacts/substrate/restore-new
```

`cluster_probe.py` deliberately sends raw duplicate selectors to the private
gateway. It is diagnostic and reports BLOCKED rather than PASS if startup fails.
Do not run it immediately before a fresh-state restore test without recreating
the disposable actors. The raw known-actor duplicate cases can trigger wake.

`demo.py` is the **unverified two-turn restore procedure**, not the complete
acceptance suite. It closes external admission, waits for forwarded requests and
Rig execution to finish, suspends explicitly, checks `SUSPENDED` and the FULL
external snapshot, reopens admission, and sends the next A2A turn. It does not
manually resume or fetch a card between suspension and that turn. There is no
automatic idle suspender. Admin callers must not admit new work during drain.
It checks the selected actor is RUNNING with a worker after the second turn.
Its `TWO_TURN_CHECKS_PASS` result still requires independent routing evidence:
join the client request ID to the gateway trace, then actor UID, worker
assignment, and the actor's operation-ID/nonce receipt. Trace IDs need not
propagate unchanged through the gateway's ResumeActor RPC. Reject a correct
answer without that chain. The procedure does not test cross-worker movement.

| Case | Executed result / acceptance condition |
|---|---|
| Real kind/control plane/gateway/workers | Ready; pinned images recorded |
| Existing actor selected while SUSPENDED | Resume attempted; actor CRASHED before sandbox startup |
| Wrong/missing credentials, cross-actor selection | Edge denies; actor UID/version/state unchanged |
| Unknown actor / broken route | Gateway 404; no actor creation and no fallback answer |
| Duplicate ordinary/CONNECT selectors | Edge denies; native gateway accepts duplicates and uses first value |
| Local two-turn history / separate contexts | Exact prompts from actual Rig request history |
| Local concurrent turns | Held tool prevents second effect; second request sees first transcript |
| Local SSE disconnect/resubscribe | Held execution continues; reconnect gets terminal result; GetTask agrees |
| Local active/queued cancellation | Active cancellation waits for in-flight effect; queued cancellation has no tool call |
| Local cancellation dispatch | A canceled held turn makes no next model call; uncanceled turns make two calls |
| Local tenant/malformed-input controls | Mismatched-tenant cancel leaves the working task unchanged; array/scalar fixture prompts fail without tool effects |
| Local uncertain response | Read stored task; replay invokes tool but operation ID prevents second file commit |
| Local cold restart | File survives; memory nonce and counter do not. Cannot pass a memory-continuity assertion |
| Cluster retained memory/file/history | BLOCKED; no successful actor turn or snapshot |
| Cross-worker restore | BLOCKED. Require old/new worker Pod UIDs, same actor UID, nonce and exact file bytes |
| Wake/suspend races | BLOCKED. Close admission atomically; count or reject each racing turn; no suspend while cleanup is active |
| Readiness delay/timeout | BLOCKED. Use delayed/failed readiness; verify no early request delivery and explicit timeout |
| Parking capacity and worker autoscaling | NOT VERIFIED. Test parking with small configured capacity separately; no autoscaler is installed |
| Direct-worker bypass | NOT VERIFIED against a running actor; require mTLS rejection plus a valid gateway positive control |
| Default-deny/allowed egress | BLOCKED. Use a live local sink and one narrow allow rule; check independent sink counters, not just timeouts |
| Card/task reads without wake | NOT IMPLEMENTED; native routing can wake for any forwarded HTTP request |
| ACP/WS bridge, permission flow, Origin/backpressure | NOT IMPLEMENTED; no ACP interoperability claim |

The first local run had an incorrect expected error shape for non-text input:
the server returns JSON-RPC `-32602`, not a failed task response. The test was
corrected without changing that behavior. An initial CONNECT probe used an
invalid authority; its 400 result was discarded. The valid probe sends
`CONNECT rig-1:80` then an inner HTTP request. Keep those failed/invalid runs
separate from the final evidence.

The history check was also tested with a deliberate mutation: pass empty Rig
history on each turn. The same local suite failed on the exact second-turn
prompt assertion. The mutation was removed before the final build. This tests
the history assertion; it does not validate the blocked snapshot assertions.

Oracle review found a mismatched-tenant cancellation and a JSON-shape panic.
The added checks reproduced both: `tenant-before` failed its rejection assertion;
`shape-before` timed out and its actor log contains the array-index panic.
Keep these failures as before evidence. The local suite includes both regression
checks. The cancellation hook observes all Rig dispatch families, but the
executed fixture covers completion and tool dispatch only.

A package rerun (`package-reviewed`) reached an old Docker container left after
stopping its supervising CLI service. Its effect-count assertion failed; that
run is invalid for the rebuilt image. Package checks must compare the running
container's image ID with the built image, require empty initial state, and stop
the container explicitly. The review artifacts include the corrected command
script and keep the failed run.

## Sources and limits

- [Hermes ACP source](https://github.com/NousResearch/hermes-agent/tree/952322ca3cf23435af4f95c2b91216d9e3658b84): real stdio peer, not the missing deployed bridge.
- [Hermes actor reference](https://github.com/agent-substrate/substrate/pull/487#issuecomment-5097213171): not a reproducible ACP image/version contract.
- [OpenClaw example](https://github.com/agent-substrate/always-on-agent/tree/c380889b73ee78b4bb71fc17079227ab886d9e82): separate application gateway and older Substrate baseline.
- [Fallback false-positive precedent](https://github.com/agent-substrate/always-on-agent/commit/3cb37731ae744e6cfdd145ab67df3212dbaa3f22): require actor-side effect receipts, not only a good response.
- [Egress omission](https://github.com/agent-substrate/substrate/issues/2487): inference success is not a default-deny egress test.

The next required environment is a disposable kernel >=6.5 Linux host with the
same pinned source and gVisor assets. Complete the blocked matrix there before
claiming E2E compatibility. No shared cluster changes, cloud provisioning,
remote push, PR, or public unauthenticated exposure is part of this example.

## Cleanup

Stop the two supervised services. Delete only the disposable cluster:

```bash
amp orb service stop rig-substrate-edge
amp orb service stop rig-substrate-forward
(cd /tmp/rig-substrate-v0.4.0; ./hack/kind.sh delete cluster --name rig-substrate)
```

Remove `kind-registry` only if this run created it
and no other local cluster uses it. Remove the disposable token file separately.
