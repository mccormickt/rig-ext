#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
prefix="baseline-$(date +%s)-$$"
test="$root/rig-durable/examples/durable-object/conformance.py"
for runtime in celld workerd; do
  port=9876
  [[ "$runtime" != workerd ]] || port=9877
  python3 "$test" --url "http://127.0.0.1:$port" --session-prefix "$prefix-$runtime"
  amp orb service restart "durable-$runtime"
  for attempt in {1..100}; do
    if curl -fsS "http://127.0.0.1:$port/health/status" >/dev/null; then break; fi
    sleep 0.1
  done
  python3 "$test" --url "http://127.0.0.1:$port" --session-prefix "$prefix-$runtime" --verify-reopen
done
