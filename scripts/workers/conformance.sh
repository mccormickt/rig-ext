#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"
mkdir -p .amp/in/artifacts
scripts/workers/build.sh
python3 scripts/workers/assets.py
cmp rig-codemode/runtime/NOTICES.txt rig-durable/examples/durable-object/build/NOTICES.txt
cmp rig-codemode/runtime/fingerprint.json rig-durable/examples/durable-object/build/worker-replay-fingerprint.json
amp orb services ensure
# Ensure already-running services read the newly built bundle.
amp orb service restart durable-celld
amp orb service restart durable-workerd
scripts/workers/baseline.sh | tee .amp/in/artifacts/workers-baseline.log
for runtime in celld workerd; do
  python3 rig-durable/examples/durable-object/workflows.py --runtime "$runtime" \
    --evidence ".amp/in/artifacts/workflows-$runtime.json" \
    | tee ".amp/in/artifacts/workflows-$runtime.log"
done
