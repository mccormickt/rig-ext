//! Delegate a whole remote agent session through ACP v1.
//!
//! The remote agent owns its model and tools. This crate owns neither a sandbox
//! nor durable session storage. See the crate README for transport contracts.

mod client;
mod model;
#[cfg(feature = "rivet")]
pub mod rivet;

pub use agent_client_protocol as protocol;
pub use agent_client_protocol::schema::v1 as schema;
#[cfg(feature = "http")]
pub use agent_client_protocol_http as http;
pub use client::{Client, Error, Event, Permission, Session, Turn};
pub use model::SessionModel;
