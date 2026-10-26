//! Stateless MCP 2026-07-28 tools over a bounded JSON-only HTTP profile.
//!
//! Servers must return `application/json`. SSE, sessions, legacy initialization,
//! subscriptions, tasks, and multi-round input requests are not supported.
//! This is not a complete implementation of the Streamable HTTP transport.
//! No request is automatically retried. A lost response can mean an uncertain
//! external effect; a JSON-RPC ID is not an idempotency key.

#![doc = include_str!("../../MCP.md")]

use std::time::Duration;

use rig_core::wasm_compat::{WasmBoxedFuture, WasmCompatSend, WasmCompatSync};

mod client;
mod headers;
mod tool;

#[cfg(feature = "mcp-http")]
mod http;

pub use client::Client;
#[cfg(feature = "mcp-http")]
pub use http::HttpTransport;
pub use rmcp::model;
pub use tool::{CallOptions, McpTool};

pub const PROTOCOL_VERSION: &str = "2026-07-28";

/// Per-request limits and aggregate discovery limits.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub request_bytes: usize,
    pub response_bytes: usize,
    /// Total response bytes across all discovery pages.
    pub catalog_bytes: usize,
    pub max_pages: usize,
    pub max_tools: usize,
    /// Includes connecting, response headers, and body consumption.
    pub timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            request_bytes: 1024 * 1024,
            response_bytes: 1024 * 1024,
            catalog_bytes: 4 * 1024 * 1024,
            max_pages: 16,
            max_tools: 256,
            timeout: Duration::from_secs(30),
        }
    }
}

/// One POST. Header values and the body may contain credentials or private data.
pub struct Request {
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// A response bounded by the transport before collection and JSON parsing.
pub struct Response {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

/// Application-owned endpoint and credentials. Implementations must enforce
/// `limits`, disable redirects and retries, and cancel reads when dropped.
/// Cancellation of local I/O does not establish cancellation of a remote effect.
pub trait Transport: WasmCompatSend + WasmCompatSync + 'static {
    fn post(
        &self,
        request: Request,
        limits: Limits,
    ) -> WasmBoxedFuture<'_, Result<Response, Error>>;
}

/// Errors contain no response bodies, credentials, URLs, or schema instance data.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid MCP configuration: {0}")]
    Configuration(&'static str),
    #[error("MCP limit exceeded: {0}")]
    Limit(&'static str),
    #[error("MCP request timed out; the remote outcome may be uncertain")]
    Timeout,
    #[error("MCP transport failed; the remote outcome may be uncertain")]
    Transport,
    #[error("MCP endpoint returned HTTP {0}")]
    Http(u16),
    #[error("MCP response must use application/json; SSE is not supported")]
    ContentType,
    #[error("invalid MCP response: {0}")]
    Protocol(&'static str),
    #[error("MCP request failed with JSON-RPC code {0}")]
    Rpc(i64),
    #[error("MCP result requires an unsupported capability: {0}")]
    Unsupported(&'static str),
    #[error("invalid MCP tool schema")]
    Schema,
    #[error("invalid MCP x-mcp-header annotation")]
    HeaderAnnotation,
    #[error("MCP tool arguments do not match the input schema")]
    Arguments,
    #[error("MCP structuredContent is missing or does not match the output schema")]
    OutputSchema,
}
