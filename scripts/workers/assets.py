"""Verify Cargo's selected package files, pinned assets, and license notices."""

import hashlib
import json
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[2]
crate = root / "rig-codemode"
runtime = crate / "runtime"
selected = set(subprocess.check_output(
    ["cargo", "package", "--locked", "--list", "--allow-dirty", "-p", "rig-codemode"],
    cwd=root, text=True,
).splitlines())
required = {
    "worker.js", "quickjs.wasm", "identity.txt", "fingerprint.json", "NOTICES.txt",
    "build.mjs", "package.json", "package-lock.json", "guest.js", "prelude.js", "discovery.js",
}
assert {"runtime/" + name for name in required} <= selected, required - selected
manifest = json.loads((runtime / "fingerprint.json").read_text())
for name in ["worker.js", "quickjs.wasm"]:
    data = (runtime / name).read_bytes()
    assert len(data) == manifest["files"][name]["bytes"], name
    assert hashlib.sha256(data).hexdigest() == manifest["files"][name]["sha256"], name
identity = (runtime / "identity.txt").read_text().strip()
assert identity == (
    f"worker-replay-v1:{manifest['engine']}:"
    f"{manifest['files']['worker.js']['sha256']}:"
    f"{manifest['files']['quickjs.wasm']['sha256']}"
), identity
lock = json.loads((runtime / "package-lock.json").read_text())
for package in ["quickjs-emscripten-core", "@jitl/quickjs-ng-wasmfile-release-sync"]:
    assert lock["packages"]["node_modules/" + package]["version"] == manifest["package"]
    assert lock["packages"]["node_modules/" + package]["license"] == "MIT"
notices = (runtime / "NOTICES.txt").read_text()
for expected in ["Jake Teton-Landis", "Fabrice Bellard", "Charlie Gordon", "Ben Noordhuis",
                 "Saúl Ibarra Corretgé", "Permission is hereby granted", "THE SOFTWARE IS PROVIDED"]:
    assert expected in notices, expected
print(f"PASS Cargo selects {len(required)} runtime files; asset hashes, engine pins, MIT notices")
print("Cargo package selection is checked; this does not publish or verify a registry .crate archive.")
