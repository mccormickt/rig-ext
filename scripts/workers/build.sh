#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
export PATH="$HOME/.cargo/bin:$root/scripts/workers/node_modules/.bin:$PATH"
export ESBUILD_BIN="${ESBUILD_BIN:-$root/scripts/workers/node_modules/.bin/esbuild}"
"$root/rig-codemode/runtime/build.sh"
cd "$root/rig-durable/examples/durable-object"
worker-build --release -- --locked "$@"
cp "$root/rig-codemode/runtime/NOTICES.txt" build/NOTICES.txt
cp "$root/rig-codemode/runtime/build/fingerprint.json" build/worker-replay-fingerprint.json
