#!/usr/bin/env bash
set -euo pipefail
ROOT=$(git rev-parse --show-toplevel)
REGISTRY=${REGISTRY:-rig-ollama-registry}
REGISTRY_PORT=${REGISTRY_PORT:-15010}
test "$(docker inspect "$REGISTRY" --format '{{index .Config.Labels "rig-proof"}}')" = "${KIND_CLUSTER_NAME:-rig-ollama-e2e}"
SOURCE_HASH=$(sha256sum Cargo.lock rig-a2a/Cargo.toml rig-a2a/src/*.rs rig-a2a/examples/substrate_actor.rs | sha256sum | cut -c1-16)
RIG_POC_BUILD="$(git rev-parse HEAD)-$SOURCE_HASH" \
  cargo build --locked -p rig-a2a --features server --example substrate_actor >&2
BUILD=$(mktemp -d)
trap 'rm -rf "$BUILD"' EXIT
cp "$ROOT/target/debug/examples/substrate_actor" "$BUILD/"
strip "$BUILD/substrate_actor"
IMAGE=localhost:$REGISTRY_PORT/rig-actor:poc
docker build -f "$ROOT/scripts/substrate/Dockerfile" -t "$IMAGE" "$BUILD" >&2
docker push "$IMAGE" >&2
docker inspect "$IMAGE" --format '{{index .RepoDigests 0}}'
