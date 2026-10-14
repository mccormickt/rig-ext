#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

rustup target add wasm32-unknown-unknown
if [[ "$(worker-build --version 2>/dev/null || true)" != "0.8.3" ]]; then
  cargo install worker-build --version 0.8.3 --locked
fi
if [[ "$(celld --version 2>/dev/null || true)" != "celld 0.6.2" ]]; then
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64)
      target=x86_64-unknown-linux-gnu
      digest=ce0ee3552cc8d55dde3998dc3b1b0c8f547bbcb4bc803d2a94842368b047646c ;;
    Linux-aarch64)
      target=aarch64-unknown-linux-gnu
      digest=4eca87c6797590000a344c1b768364e9696e8817f5fbc16193f459d2edac7cd0 ;;
    Darwin-arm64)
      target=aarch64-apple-darwin
      digest=b81a38299800679e9651fd7c1544540185682dcb59eed1a24c8cc36c6b31e76d ;;
    *) echo "No pinned celld binary for this platform" >&2; exit 1 ;;
  esac
  archive=$(mktemp)
  trap 'rm -f "$archive"' EXIT
  curl --proto '=https' --tlsv1.2 -fsSL \
    "https://github.com/denoland/celld/releases/download/v0.6.2/celld-$target.gz" -o "$archive"
  echo "$digest  $archive" | sha256sum --check
  mkdir -p "$HOME/.local/bin"
  gzip -dc "$archive" > "$HOME/.local/bin/celld"
  chmod +x "$HOME/.local/bin/celld"
fi
npm ci --prefix "$root/scripts/workers"
npm audit --prefix "$root/scripts/workers" --audit-level=high
celld --version
worker-build --version
"$root/scripts/workers/node_modules/.bin/workerd" --version
