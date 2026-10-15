# Rig actor on Agent Substrate: bounded prototype

**Zeus passed real Ollama cross-worker restore on 2026-10-15.** Linux
7.0.0-38-generic passed the pinned overlay mount tests. A public A2A request
through the authenticated edge and deployed agentgateway woke a SUSPENDED
gVisor actor. After explicit drain/suspend and deletion of its worker Pod, a
second request restored the same heap nonce, counters, file and conversation on
a different worker Pod UID. No manual resume preceded that waking request.

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
| Substrate source, CLI, manifests and workers | v0.4.0, `756c2a53741121e728f4cc3066c8a19e575b4919` |
| agentgateway | `cr.agentgateway.dev/agentgateway:v0.0.0-alpha.168b05d6@sha256:5e8384d1fcfdc443a3ad5155ea6a64dc7953bde162df7f5f8e131a5c19ae08a3` |
| gVisor systrap assets | release manifest nightly; amd64 SHA256 `d547d81401461fd1c679c5c4fa0a6c2b8ef7dc3c22ce23c9e25dcc4c69cfd06f` |
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
inside the disposable node container. Do not run against a shared cluster.

```bash
bash scripts/substrate/setup.sh
export PATH="/tmp/rig-substrate-v0.4.0/bin:$PATH"
export KUBECONFIG="$PWD/.amp/in/substrate-run/kubeconfig"
kubectl ate --context kind-rig-ollama-e2e get actor-template rig-poc -a rig-local -o json
```

Wait for a completed `status.goldenSnapshotStatus.goldenTag` with no errorMessage.
Stop on failure. ActorTemplate is a Substrate API object; WorkerPool is Kubernetes.
Templates use v0.4.0 `onCommit: FULL`, not current-main snapshot APIs. RustFS stores
external snapshots. Ordinary gateway HTTP targets actor port80.

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
  --substrate /tmp/rig-substrate-v0.4.0 --build "$BUILD"
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
export OLLAMA_BRIDGE_IP=$(docker network inspect kind --format '{{(index .IPAM.Config 0).Gateway}}')
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
token/evidence arguments checks both actors' default403, live sink counters and
then grants only rig-1 HTTP hostname `ollama.rig-local.svc`, port15024. It suspends
rig-1 afterward. DNS port53 is outside this egress contract.

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

## Executed controls and limits

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

- [Substrate overlay contract](https://github.com/agent-substrate/substrate/blob/v0.4.0/internal/imagecache/README.md)
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
