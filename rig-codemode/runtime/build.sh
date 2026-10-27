#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
if [[ "$(wasm-bindgen --version)" != "wasm-bindgen 0.2.129" ]]; then
  echo "Install wasm-bindgen-cli 0.2.129" >&2
  exit 1
fi
cargo build --manifest-path crate/Cargo.toml --target-dir target --locked --release --target wasm32-unknown-unknown
wasm-bindgen --target bundler --out-dir build --out-name runtime \
  target/wasm32-unknown-unknown/release/rig_codemode_worker_runtime.wasm
node build.mjs
