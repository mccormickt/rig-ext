#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$root/scripts/workers/node_modules/.bin:$PATH"

# The fixture and both runtimes bind loopback. No portal is created.
(cd "$root" && cargo build --locked -p rig-celld --example http_tools --features mcp-http)
(cd "$root/rig-celld/examples/mcp/worker" && worker-build --release -- --locked)
cp "$root/rig-codemode/runtime/NOTICES.txt" "$root/rig-celld/examples/mcp/worker/build/NOTICES.txt"
cp "$root/rig-codemode/runtime/fingerprint.json" "$root/rig-celld/examples/mcp/worker/build/worker-replay-fingerprint.json"
amp orb service start mcp-fixture --cwd "$root" --port 9899 \
  --command 'node rig-celld/examples/mcp/worker/fixture.mjs'
amp orb service start mcp-workerd --cwd "$root" --port 9897 \
  --command 'node scripts/workers/miniflare.mjs rig-celld/examples/mcp/worker "$PORT"'
amp orb service start mcp-celld --cwd "$root" --port 9896 \
  --command 'env PATH="$HOME/.local/bin:$PWD/scripts/workers/node_modules/.bin:$PATH" celld dev rig-celld/examples/mcp/worker --no-watch --port "$PORT"'
for service in mcp-fixture mcp-workerd mcp-celld; do
  amp orb service restart "$service"
done

for port in 9897 9896; do
  curl --fail --silent --retry 20 --retry-connrefused --retry-delay 1 "http://127.0.0.1:$port/health" >/dev/null
  curl --fail --silent --retry 20 --retry-connrefused --retry-delay 1 http://127.0.0.1:9899/reset >/dev/null
  result=$(curl --fail-with-body --silent --max-time 45 "http://127.0.0.1:$port/check")
  jq -e '.passed and (.checks | length == 22) and (.checks | all(. == true))' <<< "$result" >/dev/null
  echo "$port: $result"
  # Closing transport reads is asynchronous; wait briefly for its observation.
  for attempt in {1..20}; do
    stats=$(curl --fail --silent http://127.0.0.1:9899/stats)
    if jq -e '(.started - .closed | length == 0)' <<< "$stats" >/dev/null; then
      break
    fi
    sleep 0.1
  done
  jq -e '.effects == 20 and .requests.ok == 3 and (.requests | length == 20) and
    ([.requests | to_entries[] | select(.key != "ok") | .value] | all(. == 1)) and
    (.started | length == 6) and (.started - .closed | length == 0)' <<< "$stats" >/dev/null
  echo "$port: $stats"
  if [[ -n "${MCP_EVIDENCE_DIR:-}" ]]; then
    mkdir -p "$MCP_EVIDENCE_DIR"
    printf '%s\n' "$result" > "$MCP_EVIDENCE_DIR/faults-$port.json"
    printf '%s\n' "$stats" > "$MCP_EVIDENCE_DIR/faults-$port-server.json"
  fi
done

args=()
if [[ -n "${MCP_EVIDENCE_DIR:-}" ]]; then
  args+=(--output "$MCP_EVIDENCE_DIR")
fi
python3 "$root/rig-celld/examples/mcp/e2e.py" "${args[@]}"
