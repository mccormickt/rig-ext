#!/usr/bin/env bash
set -euo pipefail
ROOT=$(git rev-parse --show-toplevel)
SUBSTRATE=${SUBSTRATE:-/tmp/rig-substrate-v0.4.0}
export KIND_CLUSTER_NAME=rig-substrate KUBECTL_CONTEXT=kind-rig-substrate
export KO_DOCKER_REPO=localhost:5001 KO_DEFAULTPLATFORMS=linux/amd64
PIN=756c2a53741121e728f4cc3066c8a19e575b4919
python3 - <<'PY'
import platform
version = tuple(map(int, platform.release().split('.')[:2]))
if version < (6, 5):
    raise SystemExit('BLOCKED: Substrate v0.4.0 requires Linux >=6.5 (overlayfs lowerdir+); host is ' + platform.release())
PY
command -v go kubectl docker cargo >/dev/null
docker info >/dev/null
if [[ ! -d "$SUBSTRATE" ]]; then
  git clone --depth 1 --branch v0.4.0 https://github.com/agent-substrate/substrate.git "$SUBSTRATE"
fi
test "$(git -C "$SUBSTRATE" rev-parse HEAD)" = "$PIN"
test -z "$(git -C "$SUBSTRATE" status --porcelain)"
# The upstream script deletes a cluster with this name. Refuse to replace one.
if ! docker inspect rig-substrate-control-plane >/dev/null 2>&1; then
  (cd "$SUBSTRATE"; IP_FAMILY=ipv4 ./hack/create-kind-cluster.sh)
fi
(cd "$SUBSTRATE"
  go build -o bin/ate-setup ./cmd/ate-setup
  go build -o bin/kubectl-ate ./cmd/kubectl-ate
  ./hack/install-ate-kind.sh --deploy-ate-system --atenet-dataplane=agentgateway \
    --credential-provider='{"enabled":false}' --rollout-timeout=10m
  ./hack/run-tool.sh ko build --base-import-paths ./cmd/ateom-gvisor > /tmp/rig-worker-image.txt
)
WORKER_IMAGE=$(tail -1 /tmp/rig-worker-image.txt)
export PATH="$SUBSTRATE/bin:$PATH"
SOURCE_HASH=$(sha256sum Cargo.lock rig-a2a/Cargo.toml rig-a2a/src/*.rs rig-a2a/examples/substrate_actor.rs | sha256sum | cut -c1-16)
RIG_POC_BUILD="$(git rev-parse HEAD)-$SOURCE_HASH" \
  cargo build --locked -p rig-a2a --features server --example substrate_actor
BUILD=$(mktemp -d)
trap 'rm -rf "$BUILD"' EXIT
cp "$ROOT/target/debug/examples/substrate_actor" "$BUILD/"
strip "$BUILD/substrate_actor"
docker build -f "$ROOT/scripts/substrate/Dockerfile" -t localhost:5001/rig-actor:poc "$BUILD"
docker push localhost:5001/rig-actor:poc
ACTOR_IMAGE=$(docker inspect localhost:5001/rig-actor:poc --format '{{index .RepoDigests 0}}')
cat <<EOF | kubectl --context "$KUBECTL_CONTEXT" apply -f -
apiVersion: v1
kind: Namespace
metadata:
  name: rig-local
---
apiVersion: ate.dev/v1alpha1
kind: WorkerPool
metadata:
  name: rig
  namespace: rig-local
  labels:
    workload: rig
spec:
  replicas: 2
  workerImage: $WORKER_IMAGE
  template:
    nodeSelector:
      ate.dev/substrate-version: v0.4.0
    resources:
      requests: {cpu: 250m, memory: 1Gi}
      limits: {cpu: '1', memory: 1Gi}
EOF
kubectl --context "$KUBECTL_CONTEXT" rollout status deployment/rig -n rig-local --timeout=10m
if ! kubectl ate --context "$KUBECTL_CONTEXT" get atespace rig-local >/dev/null 2>&1; then
  kubectl ate --context "$KUBECTL_CONTEXT" create atespace rig-local
fi
cat > /tmp/rig-template.yaml <<EOF
metadata:
  atespace: rig-local
  name: rig-poc
workerSelector:
  matchLabels: {workload: rig}
containers:
- name: rig
  image: $ACTOR_IMAGE
  wakeupProbe:
    httpGet: {path: /readyz, port: 80}
    timeoutSeconds: 30
  volumeMounts:
  - {name: data, mountPath: /data}
resources:
  limits:
  - {name: cpu, quantity: '1'}
  - {name: memory, quantity: 512Mi}
snapshotConfig:
  onCommit: SNAPSHOT_CONTENT_SCOPE_FULL
  storageLocation: gs://ate-snapshots/rig-poc/
sandboxConfig:
  sandboxClass: SANDBOX_CLASS_GVISOR
  configName: gvisor-default
volumes:
- name: data
  durableDir: {}
EOF
kubectl ate --context "$KUBECTL_CONTEXT" create actor-template -f /tmp/rig-template.yaml
echo 'Wait for the golden snapshot, then create rig-1 and rig-2. See README.md.'
