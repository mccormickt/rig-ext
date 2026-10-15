#!/usr/bin/env bash
# Delete only the disposable resources whose IDs this setup recorded.
set -euo pipefail
ROOT=$(git rev-parse --show-toplevel)
RUN_DIR=${RUN_DIR:-$ROOT/.amp/in/substrate-v05-run}
NODE_ID=$(cat "$RUN_DIR/node.id")
WORKER_NODE_ID=$(cat "$RUN_DIR/worker-node.id")
REGISTRY_ID=$(cat "$RUN_DIR/registry.id")
test -n "$NODE_ID" && test -n "$WORKER_NODE_ID" && test -n "$REGISTRY_ID"
test "$(docker inspect rig-ollama-e2e-control-plane --format '{{.Id}}')" = "$NODE_ID"
test "$(docker inspect "$NODE_ID" --format '{{index .Config.Labels "io.x-k8s.kind.cluster"}}')" = rig-ollama-e2e
test "$(docker inspect rig-ollama-e2e-worker --format '{{.Id}}')" = "$WORKER_NODE_ID"
test "$(docker inspect "$WORKER_NODE_ID" --format '{{index .Config.Labels "io.x-k8s.kind.cluster"}}')" = rig-ollama-e2e
test "$(docker inspect rig-ollama-registry --format '{{.Id}}')" = "$REGISTRY_ID"
test "$(docker inspect "$REGISTRY_ID" --format '{{index .Config.Labels "rig-proof"}}')" = rig-ollama-e2e
docker rm -fv "$NODE_ID" "$WORKER_NODE_ID" "$REGISTRY_ID"
rm -f "$RUN_DIR/kubeconfig"
