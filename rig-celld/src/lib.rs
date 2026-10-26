//! Rig integrations for Rust Workers running on celld.
//!
//! Common memory contracts do not select a storage backend. Enable `storage`
//! for `CellStorage` on Cloudflare or celld SQLite Durable Objects. Enable
//! `sqlite-vec` together with celld's `sqlite_vec` Wrangler compatibility flag
//! to use `SqliteVecIndex`.
//! Enable independent `dynamic-workers` for fresh Worker Loader invocations.
//! Enable `mcp` for stateless JSON-only MCP tools, or `mcp-http` to also use
//! native HTTP and Workers fetch without storage or Worker Loader bindings.

mod capabilities;
mod memory;
mod vector;

#[cfg(feature = "mcp")]
pub mod mcp;

#[cfg(feature = "dynamic-workers")]
mod dynamic_workers;
#[cfg(feature = "storage")]
mod storage;
#[cfg(feature = "sqlite-vec")]
mod vector_store;

pub use capabilities::{
    BackendCapabilities, BackendKind, BackendMaturity, ScoreKind, SearchKind, TieBreak,
    ZeroVectorPolicy,
};
pub use memory::{
    ListCursor, MemoryBackend, MemoryPage, MemoryRecord, StoredDocument, StoredMemory,
};
pub use vector::VectorError;

#[cfg(feature = "dynamic-workers")]
pub use dynamic_workers::{
    BoundedResponse, LoadedWorker, OutboundPolicy, WorkerCode, WorkerLimits, WorkerLoader,
    WorkerModule,
};
#[cfg(feature = "storage")]
pub use storage::CellStorage;
#[cfg(feature = "sqlite-vec")]
pub use vector_store::{
    EmbedText, MAX_DIMENSIONS, MAX_EMBEDDINGS_PER_DOCUMENT, MAX_GENERATION, MAX_LIST_LIMIT,
    SCHEMA_VERSION, SqliteVecError, SqliteVecIndex,
};
