"""Verify Cargo build inputs, generated assets, and redistribution notices."""

import hashlib
import json
import subprocess
import tomllib
from pathlib import Path

root = Path(__file__).resolve().parents[2]
crate = root / "rig-codemode"
runtime = crate / "runtime"
output = runtime / "build"
selected = set(subprocess.check_output(
    ["cargo", "package", "--locked", "--list", "--allow-dirty", "-p", "rig-codemode"],
    cwd=root, text=True,
).splitlines())
required = {
    "NOTICES.txt", "guest/Cargo.toml.in", "guest/Cargo.lock",
    "guest/src/lib.rs", "guest/src/workers.rs",
}
assert {"runtime/" + name for name in required} <= selected, required - selected
assert "build.rs" in selected
assert not any(name.endswith((".wasm", "prelude.js", "worker.js")) for name in selected)
manifest = json.loads((output / "fingerprint.json").read_text())
for name in ["index.js", "index_bg.wasm"]:
    data = (output / name).read_bytes()
    assert len(data) == manifest["files"][name]["bytes"], name
    assert hashlib.sha256(data).hexdigest() == manifest["files"][name]["sha256"], name
identity = (output / "identity.txt").read_text().strip()
assert identity == (
    f"worker-replay-v1:{manifest['engine']}:"
    f"{manifest['files']['index.js']['sha256']}:"
    f"{manifest['files']['index_bg.wasm']['sha256']}"
), identity
lock = tomllib.loads((runtime / "guest/Cargo.lock").read_text())
packages = {package["name"]: package for package in lock["package"]}
dependencies = tomllib.loads((runtime / "guest/Cargo.toml.in").read_text().replace(
    "@CORE@", '"../../../rig-codemode-core"'
))["dependencies"]
for package in ["rquickjs", "rquickjs-core", "rquickjs-sys"]:
    assert packages[package]["version"] == manifest["rquickjs"]
assert dependencies["rquickjs"]["version"] == "=" + manifest["rquickjs"]
assert dependencies["wasm-bindgen"]["version"] == "=" + packages["wasm-bindgen"]["version"]
assert dependencies["worker"]["version"] == "=0.8.3"
assert not json.loads((runtime / "package.json").read_text()).get("dependencies")
notices = (runtime / "NOTICES.txt").read_text()
for expected in ["Fabrice Bellard", "Charlie Gordon", "Ben Noordhuis",
                 "Saúl Ibarra Corretgé", "rquickjs", "wasm-bindgen", "wasi-libc",
                 "Permission is hereby granted", "THE SOFTWARE IS PROVIDED"]:
    assert expected in notices, expected
print(f"PASS Cargo selects {len(required)} runtime inputs; generated hashes, Rust pins, license notices")
print("Cargo package selection is checked; this does not publish or verify a registry .crate archive.")
