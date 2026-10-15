#!/usr/bin/env bash
# Delete only the disposable resources whose IDs this setup recorded.
set -euo pipefail
ROOT=$(git rev-parse --show-toplevel)
RUN_DIR=${RUN_DIR:-$ROOT/.amp/in/substrate-run}
SUBSTRATE=${SUBSTRATE:-/tmp/rig-substrate-v0.4.0}
NODE_ID=$(cat "$RUN_DIR/node.id")
REGISTRY_ID=$(cat "$RUN_DIR/registry.id")
test -n "$NODE_ID" && test -n "$REGISTRY_ID"
test "$(docker inspect rig-ollama-e2e-control-plane --format '{{.Id}}')" = "$NODE_ID"
test "$(docker inspect "$NODE_ID" --format '{{index .Config.Labels "io.x-k8s.kind.cluster"}}')" = rig-ollama-e2e
test "$(docker inspect rig-ollama-registry --format '{{.Id}}')" = "$REGISTRY_ID"
test "$(docker inspect "$REGISTRY_ID" --format '{{index .Config.Labels "rig-proof"}}')" = rig-ollama-e2e
test "$(git -C "$SUBSTRATE" rev-parse HEAD)" = 756c2a53741121e728f4cc3066c8a19e575b4919
(cd "$SUBSTRATE"; ./hack/kind.sh delete cluster --name rig-ollama-e2e)
docker rm -f "$REGISTRY_ID"
