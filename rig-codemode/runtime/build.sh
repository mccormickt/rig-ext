#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
export ESBUILD_BIN="${ESBUILD_BIN:-$root/scripts/workers/node_modules/.bin/esbuild}"
messages=$(mktemp)
trap 'rm -f "$messages"' EXIT
cargo build --manifest-path "$root/Cargo.toml" --locked -p rig-codemode \
  --features worker-replay,wasmtime-replay --message-format=json > "$messages"
python3 - "$messages" "$root/rig-codemode/runtime/build" <<'PY'
import json
import shutil
import sys
from pathlib import Path

out = None
for line in Path(sys.argv[1]).read_text().splitlines():
    message = json.loads(line)
    if (message.get("reason") == "build-script-executed"
            and ("/rig-codemode#" in message["package_id"]
                 or "#rig-codemode@" in message["package_id"])):
        out = Path(message["out_dir"])
assert out is not None, "Cargo did not report the rig-codemode asset directory"
destination = Path(sys.argv[2])
destination.mkdir(parents=True, exist_ok=True)
for name in ["index.js", "index_bg.wasm", "quickjs-core.wasm", "identity.txt",
             "core-identity.txt", "fingerprint.json"]:
    shutil.copyfile(out / name, destination / name)
print(f"Runtime test assets: {destination}")
PY
