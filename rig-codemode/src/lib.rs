//! Compose Rig tools with model-written JavaScript in a bounded sandbox.
//!
//! Shared contracts and execution live in `rig-codemode-core`. This crate adds
//! Workers and Wasmtime replay adapters and builds their isolated WASM guests.

#![forbid(unsafe_code)]

pub use rig_codemode_core::*;

#[cfg(all(feature = "wasmtime-replay", not(target_family = "wasm")))]
pub mod wasmtime_replay;
#[cfg(feature = "worker-replay")]
pub mod worker_replay;
