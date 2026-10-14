# Worker runtime boundaries

- Keep dynamic-worker support independent of storage and workflow crates.
- Use workers-rs `=0.8.3` reexports for JS/WASM interop. Preserve the JS method
  receiver in reflection calls. Constructor-name checks are not portable to
  celld service bindings.
- Verify Loader changes with a bundled Rust Worker on celld and local workerd.
  `examples/loader-conformance` supplies positive and negative controls. Do not
  equate a local workerd pass with CPU/subrequest enforcement or a production
  Cloudflare test.
- Do not probe entrypoints for cleanup methods: an unknown property can become
  RPC. Cleanup must not replace a primary error. Distinguish a celld release
  request from GC-managed workerd cleanup, and neither from cancellation.
- Bound stream data before copying JS chunks into Rust. A byte bound is not a
  wall-time deadline or a bound on runtime buffers. Test error and abandoned
  read paths, not only a successful small body.
