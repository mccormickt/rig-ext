# Loader conformance

This standalone fixture builds a real Rust Worker with workers-rs `=0.8.3` and
`rig-celld/dynamic-workers`, without storage or workflow crates. Its isolated
Cargo workspace keeps the root workspace and lockfile unchanged. Generate its
ignored local lockfile once with `cargo generate-lockfile`.

## Tooling and build

Tested versions: celld 0.6.2, worker-build 0.8.3, Node 26.10.0, esbuild 0.28.2,
Miniflare 4.20260730.0, and workerd 1.20260730.1. The audited npm toolchain uses
overrides `sharp=0.35.5` and `undici=7.30.0`. Use the shared conformance toolchain
when available, or install these versions in a separate tooling directory.
The fixture does not install npm packages or own workspace setup.

From this directory:

```sh
cargo generate-lockfile
worker-build --release --no-opt -- --locked
```

worker-build's legacy build uses its downloaded esbuild 0.28.0. The local runtime
bundles use the audited esbuild 0.28.2. `--no-opt` skips wasm-opt only; the Worker
is still built with the Rust release profile. Both JS and the compiled
`build/index_bg.wasm` must be present.

## Local runtime checks

Set `TOOLING` to the absolute path of the audited npm directory. Put celld 0.6.2
and `$TOOLING/node_modules/.bin` on PATH. In an Amp orb, start supervised services
from this directory (substitute absolute paths in the service commands):

```sh
amp orb service start loader-celld --command 'cd /absolute/rig-ext/rig-celld/examples/loader-conformance && PATH=/absolute/tooling/node_modules/.bin:$PATH celld dev --port 9876 --no-watch'
amp orb service start loader-workerd --command 'cd /absolute/rig-ext/rig-celld/examples/loader-conformance && node miniflare.mjs /absolute/tooling 9877'
python3 conformance.py --runtime celld --url http://127.0.0.1:9876
python3 conformance.py --runtime workerd --url http://127.0.0.1:9877
amp orb service stop loader-celld
amp orb service stop loader-workerd
```

Outside an orb, run the same celld and Node commands in separate terminals.
Restart both runtime processes after rebuilding, since the celld command disables
watching. The local URLs above are CLI test targets, not public preview links.

## Cases and limits

`/run` exercises the real Loader: POST request body, repeated fetch, empty guest
environment despite parent bindings, default/named Fetchers, delayed chunked
responses and metadata, exact/overflow/zero byte bounds, a 1 MiB chunk with a
32-byte bound, invocation/body errors, explicit repeated dispose and rejected
owner reuse, 300 sequential fresh-load/drop cycles, invalid JS/imports/WASM,
JS importing a compiled WASM module, outbound deny and deterministic service
gateway, and zero/one/two subrequest controls. Outbound denial must report the
runtime's explicit permission error; DNS or connectivity errors do not pass.

`/cleanup` is separate fault injection, not a substitute for Loader E2E. The
bundled Rust adapter uses an injected stub and real local streams to check a
throwing disposer, a throwing property getter, and a missing disposer against
overflow, body error, invocation error, and abandonment of a pending body read.
Counters show one cleanup attempt, no entrypoint RPC cleanup, and reader
cancellation/lock release attempts. Cancellation rejects and lock release throws
after releasing; neither replaces the primary result. These observations do not
prove remote cancellation or acknowledged release.

`/cpu` and `/cpu-control` run the same finite 100-million-iteration computation
with 1 ms and 1000 ms CPU settings. The expected result is calculated independently
in Python. celld rejects the low bound and completes the control. Local workerd
completes both and also allows the zero/one subrequest negative controls. The
driver asserts these runtime-specific results instead of treating non-enforcement
as enforcement. No production deployment or production enforcement is tested.

celld stub disposal requests asynchronous release. Local workerd exposes no stub
disposer; reference cleanup is GC-managed. The sequential-load control does not
prove immediate isolate destruction, cancellation, or an awaited shutdown.
