//! Rig storage integrations for Rust Workers running on celld.
//!
//! Common memory contracts do not select a storage backend. Enable the
//! `sqlite-vec` Cargo feature together with celld's `sqlite_vec` Wrangler
//! compatibility flag to use [`SqliteVecIndex`].

mod capabilities;
mod memory;
mod vector;

#[cfg(feature = "sqlite-vec")]
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

#[cfg(feature = "sqlite-vec")]
pub use storage::CellStorage;
#[cfg(feature = "sqlite-vec")]
pub use vector_store::{
    EmbedText, MAX_DIMENSIONS, MAX_EMBEDDINGS_PER_DOCUMENT, MAX_GENERATION, MAX_LIST_LIMIT,
    SCHEMA_VERSION, SqliteVecError, SqliteVecIndex,
};
