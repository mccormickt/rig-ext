# Architecture

- Keep shared behavior and contracts backend-neutral. Use backend adapters for
  scheduling, persistence, and runtime-specific operations.
- Compose crate capabilities through Cargo features. Enabling one backend must
  not require an unrelated backend.
- Implement shared capabilities across applicable enabled backends, including
  Duroxide and Temporal for native durable workflows. Do not make a capability
  backend-specific only because one backend was implemented first. Document
  actual platform limits and any remaining backend gaps.

# Local Workers validation

- Use `scripts/workers/install.sh` for pinned celld, worker-build, esbuild, and
  Miniflare tooling. Keep `worker = "=0.8.3"`; its WASM ABI must match Rig.
- Build the Durable Object example with `scripts/workers/build.sh` before
  `amp orb services ensure`. Services preserve local SQL state across restarts.
  Run `scripts/workers/baseline.sh` for approval, denial, deduplication, and reopen
  checks on celld and workerd. Do not use `--clean` for restart checks.
- Use `scripts/workers/conformance.sh` for the built Worker fault scenarios.
  It restarts running services to read the new bundle, then selects crash windows
  through committed SQL gates. Preserve independent effect and protocol counters.
  Use `scripts/workers/check.sh` for native/WASM feature isolation and assets.
- Worker-build 0.8.3 emits ES modules with `.js` names and separate `.wasm`
  imports. Miniflare needs explicit `ESModule` and `CompiledWasm` module rules.
- Local workerd does not enforce the tested dynamic Worker CPU or subrequest
  limits. Do not report local tests as proof of Cloudflare production enforcement.
- The local example has no authentication. Do not expose it as a public service
  or deploy it without separate authorization and access controls.
