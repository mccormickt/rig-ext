# Shared code-mode runtime

Follow the replay contracts and runtime constraints in `../rig-codemode/AGENTS.md`.
Keep contracts, guest bindings, and interpreter behavior here. This crate must
not depend on the facade or invoke its asset build. The facade and WASM guest
both use this crate.
