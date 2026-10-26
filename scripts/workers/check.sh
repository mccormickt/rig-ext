#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"
target=wasm32-unknown-unknown
for features in '' storage sqlite-vec dynamic-workers mcp mcp-http; do
  cargo check --locked --target "$target" -p rig-celld --no-default-features --features "$features"
done
for features in '' worker-replay; do
  cargo check --locked --target "$target" -p rig-codemode --no-default-features --features "$features"
done
for features in '' durable-object durable-object,codemode codemode-worker-replay; do
  cargo check --locked --target "$target" -p rig-durable --no-default-features --features "$features"
done
cargo check --locked --target "$target" -p durable-object-example
cargo check --locked --target "$target" -p durable-object-example --no-default-features
dependencies=$(cargo tree --locked --target "$target" -p rig-durable --no-default-features \
  --features codemode-worker-replay --edges normal --prefix none)
if rg -q '^(duroxide|temporalio|rquickjs|rusqlite|rig-rmcp|rmcp|sqlite-vec)([- ]|$)' <<< "$dependencies"; then
  echo "Workers convenience feature enabled an unrelated backend" >&2
  exit 1
fi
cargo check --locked --workspace
cargo test --locked --workspace --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
python3 scripts/workers/assets.py
