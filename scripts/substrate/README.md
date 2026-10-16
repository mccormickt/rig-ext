# Rig actor on Agent Substrate: bounded prototype

## Published nightly comparison: c624bd88 is blocked on stock v0.5

On 2026-10-16, a fresh private Zeus installation tested the published nightly
`ghcr.io/agentgateway/agentgateway:v0.0.0-alpha.c624bd88` at index digest
`sha256:805c58c7ed4f2a111a2b1e2e2adecc0a5292ed17b9b5f70f115da806b7b71b25`
(amd64 `sha256:505c2c6f733b9250b144e83b6c6e8f210dcccd337a83bab0ef5e9c5f511b3ba0`).
Both gateway Pods ran revision `c624bd881364b765b977b176b4c9d3efda5efa03`.
Substrate stayed at stock v0.5.0. No core patch, identity-validation bypass,
Substrate main or Envoy substitution was used. The released setup pin below is
unchanged; this nightly is not a stable release or a replacement live proof.

**Permitted native HTTP egress failed before policy lookup.** The worker received
`403 Forbidden: invalid worker-for-actor SPIFFE identity`. Its minted runtime
certificate has URI `spiffe://substrate-actor.local/ateom-for-actor/rig-local/rig-1`;
the nightly accepts only `worker-for-actor`. The same actor, policy and private
destination first reproduced the release-pinned `name: Required value` failure.
Host and ordinary-Pod probes reached the exact Service destination; actor UDP/TCP
DNS succeeded. The nightly actor request did not increment the sink counter, and
provider calls stayed zero. Absence of the old policy error is not proof of a
working policy lookup: the nightly rejects the CONNECT earlier.

Independent fixture controls found:

- Native duplicate/conflicting selectors returned404 for ordinary HTTP and valid
  CONNECT attempts. Single-selector HTTP and CONNECT+inner A2A each completed a
  real tool effect. Denied requests left actor state unchanged; the other actor
  remained suspended. Protected-edge controls and unknown404/no-creation passed.
- Warm default-cache requests completed after assigned-Pod deletion, on another
  ready Pod, within the default5s TTL (final gateway receipt0.974875s after the
  initial cache-fill request started, with a matched successful lookup receipt).
  Zero-TTL also passed. These are bounded nightly observations. The release-pinned
  failure remains valid and its `cacheTtl: 0s` workaround stays in setup.
- Readiness2s passed. Readiness7s still caused first/retry504 and RESUMING; a
  separate longer operator retry succeeded with no late request effects.
  Permanent failure reached CRASHED only on the operator retry's worker deadline.

The full live-inference/restore matrix was not repeated after the egress blocker.
There is no nightly Ollama, cross-node MEMORY, VOLUMES, ACP or exactly-once claim.
Actor code and Cargo.lock are unchanged from the clean
[9df331e](https://github.com/mccormickt/rig-ext/commit/9df331ec02305250c6652f59148870e45473bea6)
build; its rebuilt actor image is
`sha256:52428d13d07a161ad95afbdb2b4162821f3e3faa5797b694bd412b70ea1db9d8`.
The separate evidence directory is `zeus-nightly-c624bd88`; prior archives remain
unchanged. Initial worker startup hit inotify exhaustion. Scaling only the owned
cluster's optional telemetry Deployments to zero released capacity. Host limits
and shared services were not changed; local RPC/access logs supply the receipts.

To repeat, use a new `RUN_DIR` and evidence directory with the dedicated setup
below. First reproduce permitted egress with the stock pin and independent
destination controls. Then change only the two gateway container images:

```bash
CANDIDATE=ghcr.io/agentgateway/agentgateway:v0.0.0-alpha.c624bd88@sha256:805c58c7ed4f2a111a2b1e2e2adecc0a5292ed17b9b5f70f115da806b7b71b25
kubectl --context kind-rig-ollama-e2e -n ate-system set image \
  deployment/atenet-router deployment/atenet-egress "agentgateway=$CANDIDATE"
kubectl --context kind-rig-ollama-e2e -n ate-system rollout status deployment/atenet-egress
kubectl --context kind-rig-ollama-e2e -n ate-system rollout status deployment/atenet-router
```

Restart only the owned router port-forward. Repeat the same actor `/egress`
request, with sink counts before/after, certificate and worker-error receipts.
If blocked, stop the live matrix. For fixture ingress checks, use fresh `rig-poc`
actors and add `--expect-selector-rejection` to `cluster_probe.py`. This flag does
not alter the release-pinned expectation. Run both cache settings with the matching
gateway configuration. `cache.py` requires a ready spare and checks the actual
gateway receipt inside the TTL; it does not wait for replacement-Pod startup.

## Released v0.5 proof remains separate

**Substrate v0.5.0 with the released native agentgateway pin fails live egress.**
On Zeus, the gateway's `GetActorEgressPolicy` request omits the new required
`name: default` field. The API rejects it with `name: Required value`, and the
gateway returns403 even for the permitted private destination. The matching
v0.5 CLI and atelet send the name and succeed. DNS permission is not HTTP egress
permission: raw UDP/TCP DNS REFUSED negatives and the allowed full-name DNS
positive passed. The actual live A2A task failed with zero provider calls, tool
invocations, effects or fixture fallback. **No v0.5 Ollama restore pass.**

The fresh v0.5 install uses new golden snapshots and two private kind nodes on
one physical host. Deterministic A2A requests restored MEMORY across worker Pods
and across those nodes, with the same heap nonce, file and canonical history.
The cross-node control verified no destination local checkpoint before wake,
completed durable copy, executed fidelity3, matching runtime assets, and
object-store download receipts joined through the restore trace. It is not
physical-host failover. Initial `inspect` starts the actor; only the second
message proves request-triggered wake after suspension.

The warmed default gateway cache still produced503 after worker replacement;
the attempt began within its5s TTL. With `cacheTtl: 0s`, it succeeded. Keep the
workaround. A2A mechanics, drain races and readiness controls remain separate
from live inference. Readiness2s passed; readiness7s timed out on both gateway
attempts and required a separate long operator retry. Permanent failure reached
CRASHED only after that retry's30s worker deadline. Native duplicate HTTP and
CONNECT selectors remain failed security expectations. VOLUMES cold restore
preserved the file but lost heap/history; automatic runtime-mismatch fallback
is not tested. Link-local egress enforcement is not proved because the native
permitted HTTP control fails first. No Envoy, core patch or gateway replacement
was substituted. The installer selects ingress and egress together; a mixed
topology needs a separate test decision.

The v0.5 actor image was built from clean
[f8e680a](https://github.com/mccormickt/rig-ext/commit/f8e680a624e4bd3589b9361d734fbba1804f0617),
digest `sha256:eac522aa78990b5cb522f904ab355ce2b9d1d442c2d7524e1113bf326d33d6eb`.
Final harness checks ran at clean
[a189da3](https://github.com/mccormickt/rig-ext/commit/a189da31b165744ff37e334038e453bfb174ab83)
with that same image. The same-image unavailable-provider check passed with no
fallback or effects. Unit, integration, documentation, feature-isolation and
clippy checks passed. Direct worker TLS without a client certificate failed with
`CERTIFICATE_REQUIRED`; the authenticated gateway still reached the unchanged actor.

## The v0.4 proof remains separate

**Zeus passed real Ollama cross-worker restore on v0.4 on 2026-10-15.** Linux
7.0.0-38-generic passed the pinned overlay mount tests. The live test's initial
`inspect` request started the actor before the first live message. After explicit
drain/suspend and deletion of its worker Pod, the **second A2A turn** through the
authenticated edge and deployed agentgateway woke the verified SUSPENDED gVisor
actor. It restored the same heap nonce, counters, file and conversation on a
different worker Pod UID. No manual resume preceded that waking request. The first
live message is not initial-wake evidence.

The tested runtime commit is
[4da3aea](https://github.com/mccormickt/rig-ext/commit/4da3aea23ce6bdced76e2a86a8dcb944dc0d6864).
Later documentation changes do not change the tested image, build or evidence.

The installed model was `qwen3.5:9b`, digest
`56671c2ab9385f9cfcb404638e32cd62d88e3501d44822208363c010179a3c90`.
Native `/api/chat` used `think:false`, temperature 0, seed 42, context8192 and
prediction limit512. Independent provider receipts contain actual token counts
and tool calls. Tool/file counters and model-to-A2A text checks reject a plausible
fallback answer. No model download or shared Ollama configuration change was needed.

## Boundaries

```text
client → loopback authenticated edge → agentgateway substrateIngress
       → ResumeActor → worker atunnel → gVisor → Rust A2A server → Rig
       → narrow egress allow → receipt proxy → shared local Ollama
```

`INFERENCE=fixture` preserves deterministic protocol controls. Its model reads
actual Rig history, calls the registered file tool, then returns the tool result.
`INFERENCE=ollama` replaces only inference with Rig's native provider. Provider
failure fails the task; it never selects the fixture. The tool writes and fsyncs
a file, counts invocations separately from commits, and creates a heap-only nonce
on its first call. Readiness does not create the nonce; the golden snapshot has none.

One actor contains one workspace. Contexts have separate histories and locks but
share the file journal. Use separate actors for separate owners. Non-null `tenant`
parameters are rejected before task lookup or state changes. The task store is
not a multitenant authorization boundary. Operation-ID deduplication is local to
the journal, not an external-effect transaction. **No external exactly-once claim.**

A2A1.0 JSON-RPC uses `SendMessage`, `SendStreamingMessage`, `GetTask`,
`SubscribeToTask` and `CancelTask`, with `A2A-Version: 1.0`. The card advertises
protocolVersion1.0. Missing,0.3 and unsupported versions are rejected. SSE carries
task events, not model tokens. Resubscribe returns current state and later events,
not missed-token replay. Native Substrate routing is not A2A card rewriting.
`PUBLIC_URL` sets the advertised endpoint. Do not use discovery as the waking turn.

**This is not ACP.** HTTP wake is not A2A-exclusive. A separate authenticated
ACP HTTP/WS-to-stdio host is possible but is not implemented here. Hermes is a real
ACP stdio peer. The older OpenClaw `acp-runtime.ts` is an application plugin using
OpenAI HTTP/SSE, not ACP wire-conformance evidence. The separate `rig-acp` branch
was not copied into this checkout or tested through this cluster.

## Pins and host requirements

| Component | Pin |
|---|---|
| Substrate source, CLI, manifests and workers | v0.5.0, `e8d40c33f96f4b7a4984bfea7334a36ab5b45b7b` |
| agentgateway | `cr.agentgateway.dev/agentgateway:v0.0.0-alpha.168b05d6@sha256:5e8384d1fcfdc443a3ad5155ea6a64dc7953bde162df7f5f8e131a5c19ae08a3` |
| gVisor systrap assets | nightly2026-10-10, defaultVersionv3; amd64 SHA256 `4ae7fab988c3647d2a835d6c3c70c6a99021c562db97827592562b6c51c3e88c` |
| kind / Kubernetes | release tool module0.33.0 /1.37.0 |
| Zeus toolchain | Go1.27.1, Rust1.98.1, Docker29.8.2 |
| Rig / A2A | Rig0.44.0; a2a-lf0.4.1, a2a-server-lf0.5.2, a2a-client-lf0.2.8; Cargo.lock |
| Actor base | digest-pinned Debian trixie in Dockerfile, with CA certificates |

Use Linux amd64 >=6.5, a local Docker daemon with privileged disposable containers,
mount/network namespaces and cgroupv2. KVM is not needed. Install Go, Rust, protoc,
kubectl, Docker, Python3 and binutils. The upstream modules pin kind and ko.
`preflight.sh` exercises the actual `lowerdir+` mount API inside disposable
containers before provisioning. It makes no host mount or permission changes.
Source-built Substrate image digests must be recorded; upstream base tags can move.

## Dedicated deployment

Run from the repository root. Setup uses only cluster `rig-ollama-e2e`, registry
`rig-ollama-registry` on loopback15010 and Kubernetes API on loopback15011. It
refuses existing resources or kubeconfig. It does not use upstream's destructive
creation script, sudo, or host permission changes. Network sysctls apply only
inside the two disposable node containers. Do not run against a shared cluster.
Minor versions before1.0 require reinstall; do not reuse v0.4 snapshots.

```bash
bash scripts/substrate/setup.sh
export PATH="/tmp/rig-substrate-v0.5.0/bin:$PATH"
export KUBECONFIG="$PWD/.amp/in/substrate-v05-run/kubeconfig"
kubectl ate --context kind-rig-ollama-e2e get actor-template rig-poc -a rig-local -o json
```

Wait for `status.goldenSnapshotStatus.goldenTag` with no errorMessage, then fetch
that Tag in its referenced atespace and verify its durable snapshot is COMPLETED.
Stop on failure. ActorTemplate is a Substrate API object; WorkerPool is Kubernetes.
Templates use v0.5 `preferredFidelity: MEMORY`; a captured snapshot alone is not
proof of the executed restore fidelity. RustFS stores external snapshots through
the released objectstore-plugin. Ordinary gateway HTTP targets actor port80.

```bash
kubectl ate --context kind-rig-ollama-e2e create actor rig-1 -a rig-local --template rig-poc
kubectl ate --context kind-rig-ollama-e2e create actor rig-2 -a rig-local --template rig-poc
python3 - <<'PY'
import json, os, secrets
fd = os.open('/tmp/rig-poc-tokens.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
os.write(fd, json.dumps({k: secrets.token_hex(32) for k in ('admin','rig-1','rig-2')}).encode())
os.close(fd)
PY
```

Run each service in its own foreground terminal, without public portals:

```bash
kubectl --context kind-rig-ollama-e2e -n ate-system port-forward --address=127.0.0.1 \
  svc/atenet-router 15021:80 15022:8081 15025:15020
python3 scripts/substrate/edge.py --tokens /tmp/rig-poc-tokens.json --port 15020 --gateway-port 15021
```

The edge is a test boundary, not a production proxy. Operator port-forward, node
and cluster access are trusted. It does not isolate hostile Kubernetes tenants.
It rejects all client actor selectors and CONNECT, authenticates the actor binding,
and emits one trusted selector. Native gateway duplicates are a separate **failed
expectation**: HTTP and valid CONNECT followed by inner HTTP select the first
header ([issue2460](https://github.com/agent-substrate/substrate/issues/2460)).
CONNECT200 alone is not wake evidence. Unknown actors must return404 and stay absent.

The dedicated gateway sets native `cacheTtl: 0s`: its default cache retained a
deleted worker and caused503 in the first cross-worker test. This costs one
control-plane lookup per request. `gateway-config.py` also sets explicit parking
budget5s and max1024. Restart the owned port-forward after gateway rollout.

## Reproduce and inspect

Use a new evidence directory per run. Scripts refuse overwrite. Evidence includes
UTC times, commands, exit codes, inputs, independent expected outcomes, HTTP data,
source/lock/binary hashes, image and actor metadata. Known tokens are redacted.
Review logs for other secrets before sharing; only synthetic prompts are used.
Do not commit evidence, credentials or binaries.

```bash
cargo test --locked -p rig-a2a --features server
cargo check --locked -p rig-a2a --no-default-features
cargo check --locked -p rig-a2a --no-default-features --features server
cargo clippy --locked -p rig-a2a --features server --all-targets -- -D warnings
python3 scripts/substrate/edge_test.py
python3 scripts/substrate/receipt_test.py
python3 scripts/substrate/local.py --evidence .amp/in/artifacts/local-new
# Derive expected identity from the packaged source, not the actor under test.
SOURCE_HASH=$(sha256sum Cargo.lock rig-a2a/Cargo.toml rig-a2a/src/*.rs rig-a2a/examples/substrate_actor.rs | sha256sum | cut -c1-16)
BUILD="$(git rev-parse HEAD)-$SOURCE_HASH"
# Each fresh-state test needs newly created actors, not actors used by another test.
python3 scripts/substrate/cluster_probe.py --tokens /tmp/rig-poc-tokens.json --evidence .amp/in/artifacts/probe-new
python3 scripts/substrate/demo.py --tokens /tmp/rig-poc-tokens.json --evidence .amp/in/artifacts/restore-new
python3 scripts/substrate/cluster.py --tokens /tmp/rig-poc-tokens.json --evidence .amp/in/artifacts/protocol-new \
  --substrate /tmp/rig-substrate-v0.5.0 --build "$BUILD"
python3 scripts/substrate/races.py --tokens /tmp/rig-poc-tokens.json --evidence .amp/in/artifacts/races-new
python3 scripts/substrate/readiness.py --tokens /tmp/rig-poc-tokens.json --evidence .amp/in/artifacts/readiness-new
```

Readiness failure deliberately leaves RESUMING: gateway deadline5s cancels the
30s worker probe. A separate operator `kubectl ate ... resume actor` retry waits
for the worker timeout and crashes the actor; then it can be deleted. That retry
is a negative-test cleanup action, **not** evidence for request-triggered restore.

For parking, use fresh fixture actors, set the owned WorkerPool replicas1 and run
`gateway-config.py --parking-max 1`. Restart port-forward, then run `parking.py`
with the same token/evidence arguments. It checks gauge1, overflow503, release→200,
budget504, exact effect counts and absence of HPA. Restore replicas2 and default
gateway config afterward. This proves parking, not Kubernetes autoscaling.

### Real Ollama

Agree on an inference window with other host users first. Inspect `/api/tags`
and `/api/version`; use the installed pinned model. Do not download models or
change the shared listener. Determine the Docker `kind` bridge gateway, then run:

```bash
export OLLAMA_BRIDGE_IP=$(docker network inspect kind | python3 -c 'import json,sys; print(next(c["Gateway"] for c in json.load(sys.stdin)[0]["IPAM"]["Config"] if ":" not in c["Gateway"]))')
(set -o noclobber; python3 scripts/substrate/receipt_proxy.py --bind "$OLLAMA_BRIDGE_IP" --port 15024 \
  > .amp/in/artifacts/ollama-receipts.jsonl)
# Separate terminal:
bash scripts/substrate/ollama-setup.sh
```

The proxy accepts only non-streaming qwen3.5:9b `/api/chat` and forwards to fixed
127.0.0.1:11434. It records synthetic requests/responses. It binds only the private
bridge, never all interfaces. The Kubernetes Service/EndpointSlice provides a
stable name; no shared Ollama listener change is required.

Wait for rig-ollama's golden snapshot. Delete disposable fixture actors and create
rig-1/rig-2 from rig-ollama. `egress.py --sink http://$OLLAMA_BRIDGE_IP:15024` plus
token/evidence arguments checks both actors' no-policy DNS REFUSED, live sink
counters and a grant for only rig-1 HTTP hostname
`ollama.rig-local.svc.cluster.local`, port15024. The full Service name must match
the DNS policy. **The v0.5 native HTTP positive currently fails403; stop there.**
Do not interpret the remaining unexecuted exclusions as passing controls.

```bash
# Use the source-derived BUILD above and the same image for positive/negative tests.
python3 scripts/substrate/ollama.py --tokens /tmp/rig-poc-tokens.json \
  --evidence .amp/in/artifacts/live-new --build "$BUILD" \
  --receipts .amp/in/artifacts/ollama-receipts.jsonl --replace-worker
python3 scripts/substrate/live-cancel.py --tokens /tmp/rig-poc-tokens.json \
  --evidence .amp/in/artifacts/cancel-new --sink "http://$OLLAMA_BRIDGE_IP:15024"
```

The live check joins client request ID to gateway trace/route/actor UID. Control
plane trace IDs differ: join actor UID, snapshot URI, worker assignment and request
interval instead. Unique operation IDs join provider tool calls to worker effects
and the journal. Each final model response must equal the corresponding A2A artifact;
no exact expected prose is imposed. Independently generated values test history.
Use `package-check.py --image "$IMAGE" --build "$BUILD" --evidence ...` for the
unavailable-provider negative control, exact running image and empty-state check.
It explicitly removes its owned container.

For deterministic restore controls, use fresh rig-poc actors and run `ollama.py`
with `--fixture --replace-worker` (no `--receipts`), then a separate fresh run with
`--fixture --replace-worker --cross-node`. The cross-node run moves the owned
WorkerPool to the control-plane node; restore its selector to the worker node
before another cross-node run. This is explicitly not live inference.
`volumes.py` checks the cold-restore negative on fresh fixture actors.
`cache.py --cache default` and `cache.py --cache 0s` require the matching
`gateway-config.py --cache ...` setting and a restarted owned port-forward.
Only run worker replacement when the other actor is suspended.

Parking tests require `--parking-max 1` and one worker. Before protocol or race
tests, restore `gateway-config.py --parking-max 1024`, restart the owned gateway
port-forward, and restore two workers. Leaving capacity1 in place correctly
rejects the concurrent protocol turn with503; that failed run is retained.

For the direct-worker negative, read `status.workerAssignment.workerPod` and
`metadata.uid` from the running actor. Forward only that owned Pod and send a
valid CONNECT through TLS without a client certificate:

```bash
kubectl --context kind-rig-ollama-e2e -n rig-local port-forward \
  "pod/$WORKER_POD" 15029:8443 --address=127.0.0.1
# Separate terminal; expect a TLS certificate-required alert, not HTTP success.
curl --max-time 10 --proxy-insecure --proxy https://127.0.0.1:15029 \
  --proxytunnel "http://$ACTOR_UID:80/inspect"
```

Pair the denial with an authenticated edge inspection before and after, verify
unchanged actor counters/file state, and stop only this port-forward process.

## v0.4 executed controls and limits

| Case | Zeus result |
|---|---|
| Golden/startup, FULL memory/file/history restore | Passed, including different worker Pod UID and post-golden heap nonce |
| Context isolation/serialization, SSE disconnect/resubscribe | Passed through deployed edge/gateway with deterministic inference |
| Active/queued cancel, cleanup, uncertain response | Passed; separate invocation/effect counters; live canceled tool makes no next model call |
| Wrong/missing auth, actor/task selection, tenant, JSON shape | Passed; unknown actor404, no creation or fallback |
| Duplicate HTTP and valid CONNECT+inner HTTP | Protected edge denies; native gateway failure retained |
| Wake/suspend races | Held cleanup blocks suspension; closed admission rejects work; concurrent waking turns serialize |
| Readiness | Three503 probes before effect; permanent failure→gateway504, canceled probe, RESUMING; separate operator retry→30s failure/CRASHED |
| Parking | Fixed one-worker pool, gauge1, overflow503, release200, timeout504; no HPA |
| Direct worker bypass | TLS client certificate required; valid gateway positive succeeds |
| Egress | Explicit default403, independent sink, exact local host/port allowed for one actor |
| Cold process restart | File survives; nonce/counters do not; cannot pass memory continuity |
| General quiescence | Not proved. Terminal task or closed HTTP is insufficient; active0 is not a general detached-handler barrier |
| Automatic idle suspend, external exactly-once, ACP, autoscaling | Not implemented or claimed |

Suspension is explicit after admission closure and observed executor/tool cleanup.
Admin callers must not start work during drain. The controlled race test is not
a general proof against arbitrary detached upstream handlers.

## Failures remain evidence

Prior-orb archive is labelled separately, not credited as Zeus results. Linux
6.1.158 rejected `lowerdir+` before runsc. Its SUSPENDED→CRASHED/HTTP500 was a
snapshot-less cold-start attempt, not memory restore. Changing a kind image does
not change the host kernel. No microVM, unsandboxed fallback or core patch was used.

Prior controls retain: invalid non-text error-shape expectation (actual -32602),
invalid CONNECT authority (replaced by CONNECT rig-1:80 plus inner HTTP), an empty-
history mutation that failed, tenant-mismatched cancel and array-index panic before
their fixes, and a stale Docker container package run that failed exact effects.
Prior Rust35 unit+56 integration+4 doc tests and local deterministic checks passed.

Zeus retains: an invalid overlay upper-dir probe, missing CA certificates in the
first image, default gateway cache503 after worker deletion, readiness parser error,
the incorrect CRASHED expectation after the shorter gateway deadline, and an early
Docker startup connection reset. CA packaging, disabled native route cache and
bounded package startup retry resolved the corresponding reruns. Native duplicate
selectors remain a failure, not a passing security claim.

## Sources

- [Substrate v0.5 release](https://github.com/agent-substrate/substrate/releases/tag/v0.5.0)
- [Tagged upgrade contract](https://github.com/agent-substrate/substrate/blob/v0.5.0/docs/upgrade.md)
- [Substrate overlay contract](https://github.com/agent-substrate/substrate/blob/v0.5.0/internal/imagecache/README.md)
- [Hermes ACP source](https://github.com/NousResearch/hermes-agent/tree/952322ca3cf23435af4f95c2b91216d9e3658b84): real stdio peer, not a deployed bridge here.
- [Hermes actor reference](https://github.com/agent-substrate/substrate/pull/487#issuecomment-5097213171): not a reproducible ACP image contract.
- [OpenClaw example](https://github.com/agent-substrate/always-on-agent/tree/c380889b73ee78b4bb71fc17079227ab886d9e82): application gateway and older Substrate baseline.
- [Fallback false positive](https://github.com/agent-substrate/always-on-agent/commit/3cb37731ae744e6cfdd145ab67df3212dbaa3f22): require actor effects, not a good answer alone.
- [Egress omission](https://github.com/agent-substrate/substrate/issues/2487): inference success alone does not test default deny.

## Cleanup

Stop only this run's edge, port-forward and receipt proxy. Never stop shared Ollama.
`cleanup.sh` checks recorded container IDs and labels before deleting this run's
cluster and registry. A missing or mismatched identity stops cleanup. Remove the
private token file separately:

```bash
bash scripts/substrate/cleanup.sh
```
