#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
export PATH="$HOME/.cargo/bin:$root/scripts/workers/node_modules/.bin:$PATH"
cd "$root/rig-durable/examples/durable-object"
worker-build --release -- --locked "$@"
cp "$root/rig-codemode/runtime/NOTICES.txt" build/NOTICES.txt
cp "$root/rig-codemode/runtime/fingerprint.json" build/worker-replay-fingerprint.json
