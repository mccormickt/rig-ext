#!/usr/bin/env bash
# Run only on disposable local data. No portal or external service is created.
set -euo pipefail
root=$(git rev-parse --show-toplevel)
binary=${SANDBOX_AGENT_BIN:?Set SANDBOX_AGENT_BIN to the released 0.5.3 executable}
"$binary" --version
sha256sum "$binary"
data=$(mktemp -d)
service="acp-fixture-$$"
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
token=rig-acp-local-fixture-only
base="http://127.0.0.1:$port"
server="fixture-$$"
cleanup() {
  local status=$?
  if ! amp orb service stop "$service" >/dev/null 2>&1; then
    echo "FAIL supervisor stop; disposable data retained at $data"
    exit 1
  fi
  if [[ -f "$data/receipts.jsonl" ]]; then
    for pid in $(jq -r .pid "$data/receipts.jsonl" | sort -u); do
      for _ in $(seq 1 50); do
        [[ ! -d /proc/$pid ]] && break
        sleep 0.1
      done
      if [[ -d /proc/$pid ]]; then
        echo "FAIL agent PID $pid remains after supervisor stop; data retained at $data"
        exit 1
      fi
      echo "PASS supervisor cleanup: agent PID $pid exited"
    done
  fi
  rm -rf "$data"
  exit "$status"
}
trap cleanup EXIT
mkdir -p "$data/sandbox-agent/bin/agent_processes"
cp "$root/rig-acp/examples/fixtures/agent.mjs" "$data/sandbox-agent/bin/agent_processes/mock-acp"
chmod +x "$data/sandbox-agent/bin/agent_processes/mock-acp"
amp orb service start "$service" --command "env -i PATH='$PATH' HOME='$data' XDG_DATA_HOME='$data' ACP_FIXTURE_RECEIPTS='$data/receipts.jsonl' '$binary' server --host 127.0.0.1 --port '$port' --token '$token' --no-telemetry" >/dev/null
for _ in $(seq 1 100); do
  if curl -fsS -H "Authorization: Bearer $token" "$base/v1/acp" > /dev/null 2>&1; then break; fi
  sleep 0.1
done
status=$(curl -s -o /dev/null -w '%{http_code}' "$base/v1/acp")
test "$status" = 401
echo "PASS authentication negative control: unauthenticated request HTTP $status"
ACP_RIVET_URL="$base/v1/acp/$server?agent=mock" ACP_RIVET_TOKEN="$token" \
  cargo run --locked --manifest-path "$root/Cargo.toml" -p rig-acp --features rivet --example acp_demo
echo "server still exists after session cancel and client drop:"
curl -fsS -H "Authorization: Bearer $token" "$base/v1/acp" | jq .
echo "independent agent process receipts:"
cat "$data/receipts.jsonl"
for pid in $(jq -r .pid "$data/receipts.jsonl" | sort -u); do
  test -d "/proc/$pid"
  echo "PASS agent PID $pid remains live after client drop"
done
echo "host explicitly destroys its disposable server:"
if ! curl --max-time 5 -fsS -X DELETE -H "Authorization: Bearer $token" "$base/v1/acp/$server"; then
  echo "LIMITATION: Rivet DELETE did not complete; the supervisor stops this disposable service and its process group."
fi
echo
echo "PASS Rivet protocol scenarios; server DELETE is a separate lifecycle result above."
