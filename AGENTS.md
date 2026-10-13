# Architecture

- Keep shared behavior and contracts backend-neutral. Use backend adapters for
  scheduling, persistence, and runtime-specific operations.
- Compose crate capabilities through Cargo features. Enabling one backend must
  not require an unrelated backend.
- Implement shared capabilities across applicable enabled backends, including
  Duroxide and Temporal for native durable workflows. Do not make a capability
  backend-specific only because one backend was implemented first. Document
  actual platform limits and any remaining backend gaps.
