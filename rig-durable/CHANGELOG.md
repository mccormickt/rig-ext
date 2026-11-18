# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/mccormickt/rig-ext/releases/tag/rig-durable-v0.1.0) - 2026-11-18

### Added

- *(guard)* add independent bounded SQLite storage
- *(durable)* [**breaking**] enforce scoped logical execution definitions
- *(temporal)* adopt persistent replay with SDK 1.1
- *(durable)* integrate the persistent Rig runner
- *(durable)* prototype Rig-native persistent replay
- *(codemode)* add bounded Worker replay runtime
- *(durable)* support codemode and children in Durable Objects
- *(durable)* support Temporal code mode and child agents
- *(durable)* add shared code-mode scheduling and Duroxide adapter
- *(durable)* [**breaking**] isolate Rig persistence generation 2
- *(durable)* add the SQLite Durable Object engine
- *(rig-durable)* compact sessions with rig-memory policies and Rig compactors
- *(rig-durable)* add submission dedup, completed-prompt compaction, and host-visible dispositions
- *(rig-durable)* add replay-safety policies, logical call identity, and config snapshots
- add Temporal durable agents
- Durable Agent Execution

### Fixed

- *(codemode)* drain admitted calls after host failures
- *(rig-durable)* enforce approval deadline for malformed status
- *(durable)* canonicalize JSON identity hashes
- *(durable)* reject failed compaction summaries
- *(durable)* support the backend-neutral core on WASM
- *(rig-durable)* keep unreleased crate at 0.1.0
- *(rig-durable)* enforce execution and session boundaries

### Other

- simplify the workspace README and separate release guidance
- configure workspace checks and release-plz
- *(durable)* document logical execution safety and migration
- *(workers)* add durable workflow conformance and pinned tooling
- *(durable)* extract serializable codemode scheduler
- *(rig-durable)* unify tool policy and enforce lint boundaries
- *(rig-durable)* separate overview from operational guide
- *(codemode)* demonstrate subagent workflows
- *(deps)* [**breaking**] migrate all crates to Rig 0.44
- *(durable)* add a runnable Durable Object example and backend guide
- *(durable)* share configuration and effect planning across backends
- *(rig-durable)* add checked Rig usage examples
- *(rig-durable)* replay legacy histories and define 0.2 compatibility
- upgrade rig to 0.43
