#!/usr/bin/env bash
# Evidence contains only synthetic test data and public source/tool metadata.
set -euo pipefail
root=$(git rev-parse --show-toplevel)
test "$PWD" = "$root" || { echo "Run from the repository root" >&2; exit 2; }
evidence="$root/.amp/in/artifacts/acp-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$evidence"
export ACP_FIXTURE_RECEIPTS="$evidence/stdio-receipts.jsonl"
run() {
  local label=$1
  shift
  {
    printf 'START %s\nCOMMAND ' "$(date -u +%FT%TZ)"
    printf '%q ' "$@"
    printf '\n'
  } | tee "$evidence/$label.log"
  set +e
  "$@" 2>&1 | tee -a "$evidence/$label.log"
  local status=${PIPESTATUS[0]}
  set -e
  printf 'END %s EXIT %s\n' "$(date -u +%FT%TZ)" "$status" | tee -a "$evidence/$label.log"
  return "$status"
}
{
  date -u +%FT%TZ
  git remote get-url origin
  git rev-parse HEAD origin/main
  git status --short
  rustc --version
  cargo --version
  node --version
  python3 --version
  uname -sm
  echo 'No container images used. New feature; no ACP baseline exists at origin/main.'
  sha256sum Cargo.toml Cargo.lock
  rg --files rig-acp | sort | xargs sha256sum
} > "$evidence/provenance.txt"
run format cargo fmt -p rig-acp --check
run tests cargo test --locked -p rig-acp --all-features -- --nocapture
run clippy cargo clippy --locked -p rig-acp --all-features --all-targets -- -D warnings
run minimal cargo check --locked -p rig-acp --no-default-features --lib
run http cargo check --locked -p rig-acp --no-default-features --features http --lib
run rivet-feature cargo check --locked -p rig-acp --no-default-features --features rivet --lib
run stdio cargo run --locked -p rig-acp --example acp_demo
if [[ -n ${SANDBOX_AGENT_BIN:-} ]]; then
  run rivet bash rig-acp/scripts/rivet_demo.sh
else
  echo 'BLOCKED: set SANDBOX_AGENT_BIN for the released-server demo' | tee "$evidence/rivet.log"
fi
if [[ -n ${HERMES_CHECKOUT:-} ]]; then
  run peer-build cargo build --locked -p rig-acp --example acp_peer
  run hermes python3 rig-acp/scripts/hermes_smoke.py "$HERMES_CHECKOUT"
  git -C "$HERMES_CHECKOUT" status --short >> "$evidence/provenance.txt"
  sha256sum "$HERMES_CHECKOUT/uv.lock" >> "$evidence/provenance.txt"
  "$HERMES_CHECKOUT/.venv/bin/python" --version >> "$evidence/provenance.txt"
else
  echo 'BLOCKED: set HERMES_CHECKOUT for the real-peer/local-model demo' | tee "$evidence/hermes.log"
fi
echo "Evidence: $evidence"
