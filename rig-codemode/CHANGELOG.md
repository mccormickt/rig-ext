# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/mccormickt/rig-ext/releases/tag/rig-codemode-v0.1.0) - 2026-11-18

### Added

- *(codemode)* add import-free Wasmtime replay adapter
- *(codemode)* build workers-rs replay assets with Cargo
- *(durable)* integrate the persistent Rig runner
- *(codemode)* build Worker replay runtime in Rust
- *(celld)* add stateless JSON-only MCP tools
- *(codemode)* add bounded Worker replay runtime
- *(codemode)* [**breaking**] add replay-safe script steps
- *(rig-codemode)* add bounded JavaScript code-mode executor with script policy

### Fixed

- *(deps)* patch undici security vulnerabilities
- *(codemode)* support Wasm-local dispatch and external reports

### Other

- simplify the workspace README and separate release guidance
- configure workspace checks and release-plz
- *(codemode)* define guest helpers with Rust bindings
- *(codemode)* extract shared runtime and replay contracts
- *(codemode)* move Worker replay checks to Rust
- *(codemode)* expose portable replay planning
- *(rig-codemode)* separate result projection from scheduling
- *(rig-codemode)* describe workspace setup and runtime limits
- *(codemode)* demonstrate subagent workflows
- *(deps)* [**breaking**] migrate all crates to Rig 0.44
- *(rig-codemode)* add tested Rig integration examples
