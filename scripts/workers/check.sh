#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"
export ESBUILD_BIN="${ESBUILD_BIN:-$root/scripts/workers/node_modules/.bin/esbuild}"
mode=${1:-all}
case "$mode" in
  all | native | wasm) ;;
  *)
    echo "Usage: $0 [all|native|wasm]" >&2
    exit 2
    ;;
esac
if [[ "$mode" != native ]]; then
  rig-codemode/runtime/build.sh
  target=wasm32-unknown-unknown
  for package in rig-codemode-core rig-typesafeai-tools celld-agent-example celld-conformance; do
    cargo check --locked --target "$target" -p "$package" --no-default-features
  done
  cargo check --locked --target "$target" -p rig-codemode-core --no-default-features --features worker-runtime
  for features in '' storage sqlite-vec dynamic-workers mcp mcp-http; do
    cargo check --locked --target "$target" -p rig-celld --no-default-features --features "$features"
  done
  cargo check --locked --target "$target" -p rig-celld --all-features
  for features in '' worker-runtime worker-replay wasmtime-replay worker-replay,wasmtime-replay; do
    cargo check --locked --target "$target" -p rig-codemode --no-default-features --features "$features"
  done
  cargo check --locked --target "$target" -p mcp-worker-conformance
  for features in '' durable-object durable-object,codemode codemode-worker-replay; do
    cargo check --locked --target "$target" -p rig-durable --no-default-features --features "$features"
  done
  cargo check --locked --target "$target" -p durable-object-example
  cargo check --locked --target "$target" -p durable-object-example --no-default-features
  dependencies=$(cargo tree --locked --target "$target" -p rig-durable --no-default-features \
    --features codemode-worker-replay --edges normal --prefix none)
  if rg -q '^(duroxide|temporalio|rquickjs|rusqlite|rig-rmcp|rmcp|sqlite-vec)([- ]|$)' <<<"$dependencies"; then
    echo "Workers convenience feature enabled an unrelated backend" >&2
    exit 1
  fi
  cargo check --locked -p rig-codemode --no-default-features --features wasmtime-replay
  dependencies=$(cargo tree --locked -p rig-codemode --no-default-features \
    --features wasmtime-replay --edges normal --prefix none)
  if rg -q '^(rquickjs|worker|rig-celld)([- ]|$)' <<<"$dependencies"; then
    echo "Wasmtime replay enabled native QuickJS or Workers dependencies" >&2
    exit 1
  fi
  python3 scripts/workers/assets.py
fi
if [[ "$mode" != wasm ]]; then
  cargo check --locked --workspace
  cargo test --locked --workspace
  cargo test --locked --workspace --all-features
  cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
  cargo fmt --all -- --check
fi
