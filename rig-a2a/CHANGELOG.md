# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/mccormickt/rig-ext/releases/tag/rig-a2a-v0.1.0) - 2026-11-18

### Added

- *(durable)* integrate the persistent Rig runner
- *(a2a)* preserve multimodal input through Rig replay
- *(a2a)* add typed protocol requests and hook-safe model routes
- add A2A-backed Rig agents

### Fixed

- *(a2a)* bound retained replies without losing remote state
- *(a2a)* reduce task streams and project image replies
- *(rig-a2a)* isolate explicit context requests from bound state
- *(a2a)* [**breaking**] preserve newest threaded message boundaries
- *(a2a)* restore context and thread bookkeeping after rig 0.43 upgrade

### Other

- *(a2a)* document integration contracts and exercise run recovery
- *(a2a)* update SDK releases and enforce wire version compatibility
- *(rig-a2a)* describe workspace installation and current contracts
- *(rig-a2a)* inherit workspace dependencies and package metadata
- *(deps)* [**breaking**] migrate all crates to Rig 0.44
- upgrade rig to 0.43
