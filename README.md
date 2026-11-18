# rig-ext

`rig-ext` extends [Rig](https://github.com/0xPlaygrounds/rig), the Rust agent framework, with durable workflows, remote agents, storage, and tools. Use the crates separately or together.

## Crates

| Crate | Description |
| --- | --- |
| [`rig-a2a`](rig-a2a/README.md) | Use a remote Agent2Agent (A2A) service as a Rig completion model or agent. |
| [`rig-celld`](rig-celld/README.md) | Connect Rig to Durable Object storage, vector search on celld, Dynamic Workers, and stateless MCP tools. |
| [`rig-codemode`](rig-codemode/README.md) | Let a model combine approved Rig tools in a single JavaScript function. |
| [`rig-codemode-core`](rig-codemode-core/) | Build code-mode runtimes with shared tool bindings, execution limits, and a replay interpreter. |
| [`rig-durable`](rig-durable/README.md) | Run agent workflows that resume after a restart on Duroxide, Temporal, or SQLite Durable Objects. |
| [`rig-typesafeai-tools`](rig-typesafeai-tools/README.md) | Expose a host-configured TypeSafe Jev evaluator as a Rig tool. |

For maintainers: [CI and releases](RELEASE.md).
