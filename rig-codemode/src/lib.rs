//! Compose Rig tools with model-written JavaScript in a bounded sandbox.
//!
//! Shared contracts and execution live in `rig-codemode-core`. This crate adds
//! the Workers replay adapter and builds its isolated WASM guest.

#![forbid(unsafe_code)]

pub use rig_codemode_core::*;

#[cfg(feature = "worker-replay")]
pub mod worker_replay;
