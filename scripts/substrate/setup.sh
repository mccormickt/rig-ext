#!/usr/bin/env bash
set -euo pipefail
if [[ -n ${ATE_CONFIG:-} ]]; then
  echo 'Refusing ATE_CONFIG: the isolated proof must not load another install target.' >&2
  exit 1
fi
ROOT=$(git rev-parse --show-toplevel)
SUBSTRATE=${SUBSTRATE:-$ROOT/.amp/in/substrate-6a67f431}
export KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:-rig-ollama-e2e}
export KUBECTL_CONTEXT=kind-$KIND_CLUSTER_NAME
test "$KIND_CLUSTER_NAME" = rig-ollama-e2e
REGISTRY=${REGISTRY:-rig-ollama-registry}
REGISTRY_PORT=${REGISTRY_PORT:-15010}
RUN_DIR=${RUN_DIR:-$ROOT/.amp/in/substrate-6a67f431-run}
mkdir -p "$RUN_DIR"
export KUBECONFIG=$RUN_DIR/kubeconfig
export ATE_RECORD_DIR=$RUN_DIR/install-records
export KO_DOCKER_REPO=localhost:$REGISTRY_PORT KO_DEFAULTPLATFORMS=linux/amd64
PIN=6a67f431e0f2d407eaaa73fa03756fac8a07ffcb
GATEWAY=cr.agentgateway.dev/agentgateway:v1.7.0@sha256:60216aafb03d37f94e952ff3d51d2f630a65b10ecaebdc0a864882f92610c45b
python3 - <<'PY'
import platform
version = tuple(map(int, platform.release().split('.')[:2]))
if version < (6, 5):
    raise SystemExit('BLOCKED: Substrate requires Linux >=6.5 (overlayfs lowerdir+); host is ' + platform.release())
PY
command -v go kubectl docker cargo >/dev/null
docker info >/dev/null
if [[ ! -d "$SUBSTRATE" ]]; then
  git clone --no-checkout https://github.com/agent-substrate/substrate.git "$SUBSTRATE"
  git -C "$SUBSTRATE" checkout --detach "$PIN"
fi
test "$(git -C "$SUBSTRATE" rev-parse HEAD)" = "$PIN"
test -z "$(git -C "$SUBSTRATE" status --porcelain)"
# Never adopt or replace a cluster, registry, or kubeconfig.
test ! -e "$KUBECONFIG"
for name in "$KIND_CLUSTER_NAME-control-plane" "$KIND_CLUSTER_NAME-worker" "$REGISTRY"; do
  if docker inspect "$name" >/dev/null 2>&1; then
    echo "Refusing existing container: $name" >&2
    exit 1
  fi
done
SUBSTRATE="$SUBSTRATE" bash "$ROOT/scripts/substrate/preflight.sh"
docker run -d --name "$REGISTRY" --label rig-proof="$KIND_CLUSTER_NAME" \
  -p "127.0.0.1:$REGISTRY_PORT:5000" registry:3 > "$RUN_DIR/registry.id"
cat > "$RUN_DIR/kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
featureGates:
  ClusterTrustBundle: true
  ClusterTrustBundleProjection: true
  PodCertificateRequest: true
runtimeConfig:
  "certificates.k8s.io/v1beta1": "true"
networking:
  ipFamily: ipv4
  apiServerAddress: "127.0.0.1"
  apiServerPort: 15011
kubeadmConfigPatches:
- |
  kind: KubeletConfiguration
  serializeImagePulls: false
  maxParallelImagePulls: 4
EOF
(cd "$SUBSTRATE"; ./hack/kind.sh create cluster --name "$KIND_CLUSTER_NAME" --config "$RUN_DIR/kind.yaml")
docker inspect "$KIND_CLUSTER_NAME-control-plane" --format '{{.Id}}' > "$RUN_DIR/node.id"
docker inspect "$KIND_CLUSTER_NAME-worker" --format '{{.Id}}' > "$RUN_DIR/worker-node.id"
docker network connect kind "$REGISTRY"
REGISTRY_IP=$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$REGISTRY")
# These network sysctls apply only inside the dedicated node namespace.
for NODE in "$KIND_CLUSTER_NAME-control-plane" "$KIND_CLUSTER_NAME-worker"; do
  docker exec "$NODE" sysctl net.ipv4.conf.all.proxy_arp=1
  docker exec "$NODE" sysctl -e net.ipv6.conf.all.proxy_ndp=1
  docker exec "$NODE" mkdir -p "/etc/containerd/certs.d/localhost:$REGISTRY_PORT"
  printf '[host."http://%s:5000"]\n' "$REGISTRY_IP" | docker exec -i "$NODE" \
    sh -c "cat > /etc/containerd/certs.d/localhost:$REGISTRY_PORT/hosts.toml"
done
(cd "$SUBSTRATE"
  go build -o bin/ate-setup ./cmd/ate-setup
  go build -o bin/kubectl-ate ./cmd/kubectl-ate
  ./hack/install-ate-kind.sh deploy ate-system --atenet-dataplane=agentgateway \
    --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/rig-local/sa/rig-proof-client \
    --snapshot-location=gs://ate-snapshots/rig-poc/ \
    --credential-provider='{"enabled":false}' --rollout-timeout=10m
  ./hack/run-tool.sh ko build --base-import-paths ./cmd/ateworker-gvisor > "$RUN_DIR/worker-image.txt"
)
kubectl --context "$KUBECTL_CONTEXT" -n ate-system get configmaps -o json > "$RUN_DIR/configmaps-installed.json"
kubectl --context "$KUBECTL_CONTEXT" -n ate-system set image \
  deployment/atenet-router deployment/atenet-egress "agentgateway=$GATEWAY"
for DEPLOYMENT in atenet-router atenet-egress; do
  kubectl --context "$KUBECTL_CONTEXT" -n ate-system rollout status "deployment/$DEPLOYMENT" --timeout=180s
done
kubectl --context "$KUBECTL_CONTEXT" -n ate-system get daemonsets -o json | \
  python3 -c 'import json,sys; d=json.load(sys.stdin); ip=sys.argv[1]
for item in d["items"]:
    for c in item["spec"]["template"]["spec"]["containers"]:
        if c["name"] == "atelet":
            c["args"] = ["--localhost-registry-replacement="+ip+":5000" if a.startswith("--localhost-registry-replacement=") else a for a in c["args"]]
            item["spec"]["template"]["spec"].setdefault("tolerations", []).append({"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"})
            print(json.dumps(item))' "$REGISTRY_IP" | kubectl --context "$KUBECTL_CONTEXT" replace -f -
kubectl --context "$KUBECTL_CONTEXT" -n ate-system rollout status daemonset/atelet --timeout=120s
python3 "$ROOT/scripts/substrate/gateway-config.py"
kubectl --context "$KUBECTL_CONTEXT" -n ate-system get configmaps -o json > "$RUN_DIR/configmaps-configured.json"
WORKER_IMAGE=$(tail -1 "$RUN_DIR/worker-image.txt")
export PATH="$SUBSTRATE/bin:$PATH"
ACTOR_IMAGE=$(REGISTRY="$REGISTRY" REGISTRY_PORT="$REGISTRY_PORT" bash "$ROOT/scripts/substrate/package.sh")
cat <<EOF | kubectl --context "$KUBECTL_CONTEXT" apply -f -
apiVersion: v1
kind: Namespace
metadata:
  name: rig-local
  labels:
    ate.dev/api-access: 'true'
---
apiVersion: ate.dev/v1
kind: WorkerPool
metadata:
  name: rig
  namespace: rig-local
  labels:
    workload: rig
spec:
  replicas: 2
  workerImage: $WORKER_IMAGE
  sandboxClasses:
  - name: gvisor
    configRef: {name: gvisor-default}
  template:
    nodeSelector:
      kubernetes.io/hostname: $KIND_CLUSTER_NAME-worker
    resources:
      requests: {cpu: 250m, memory: 1Gi}
      limits: {cpu: '1', memory: 1Gi}
EOF
kubectl --context "$KUBECTL_CONTEXT" wait --for=create deployment/rig -n rig-local --timeout=120s
kubectl --context "$KUBECTL_CONTEXT" rollout status deployment/rig -n rig-local --timeout=10m
if ! kubectl ate --context "$KUBECTL_CONTEXT" get atespace rig-local >/dev/null 2>&1; then
  kubectl ate --context "$KUBECTL_CONTEXT" create atespace rig-local
fi
cat > "$RUN_DIR/template.yaml" <<EOF
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
    cpuMillicores: 1000
    memoryBytes: 536870912
snapshotConfig:
  preferredFidelity: SNAPSHOT_FIDELITY_MEMORY
volumes:
- name: data
  durableDir: {}
EOF
kubectl ate --context "$KUBECTL_CONTEXT" create actor-template -f "$RUN_DIR/template.yaml"
echo 'Wait for the golden snapshot, then create rig-1 and rig-2. See README.md.'
