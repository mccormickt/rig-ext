# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/mccormickt/rig-ext/releases/tag/rig-celld-v0.1.0) - 2026-11-18

### Added

- *(durable)* integrate the persistent Rig runner
- *(celld)* add stateless JSON-only MCP tools
- *(celld)* add independent dynamic Worker Loader
- *(celld)* expose SQL storage without sqlite-vec
- *(celld)* target celld 0.6.1 with transactional storage
- add Rig celld integration

### Fixed

- *(celld)* reject MCP headers in content schemas

### Other

- simplify the workspace README and separate release guidance
- *(mcp)* move cross-runtime orchestration to Rust
- *(mcp)* add Cargo-first Rust examples
- *(mcp)* add native and Worker conformance harness
- *(rig-celld)* describe current storage contracts
- *(deps)* [**breaking**] migrate all crates to Rig 0.44
- upgrade rig to 0.43
- add examples README index
