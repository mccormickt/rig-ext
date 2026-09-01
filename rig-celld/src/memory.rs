//! Shared memory records and backend operations.

use std::future::Future;

use rig_core::wasm_compat::{WasmCompatSend, WasmCompatSync};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::BackendCapabilities;

/// A logical memory independent of its storage backend.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MemoryRecord {
    /// Stable application-owned identifier.
    pub id: String,
    /// Text embedded for semantic retrieval.
    pub content: String,
    /// Application-defined metadata stored with the memory.
    #[serde(default)]
    pub metadata: Value,
}

/// A serialized document and its active logical generation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StoredDocument<T> {
    /// Stable application-owned identifier.
    pub id: String,
    /// Active generation assigned by the backend.
    pub generation: i64,
    /// Stored document.
    pub document: T,
}

/// A stored memory and its active logical generation.
pub type StoredMemory = StoredDocument<MemoryRecord>;

/// Logical pagination position used for listing or migration.
///
/// This cursor does not expose a vec0 row ID or another backend-specific key.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ListCursor {
    /// Last logical document ID returned by the preceding page.
    pub id: String,
    /// Generation observed for that document.
    pub generation: i64,
}

/// One page of logical memories.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MemoryPage {
    /// Memories in `(id, generation)` order.
    pub memories: Vec<StoredMemory>,
    /// Position for the next page, or `None` at the end.
    pub next_cursor: Option<ListCursor>,
}

/// CRUD and migration operations shared by celld memory backends.
///
/// Rig semantic retrieval remains available through each backend's
/// `VectorStoreIndex` implementation.
pub trait MemoryBackend: WasmCompatSend + WasmCompatSync {
    /// Backend-specific error.
    type Error;

    /// Report static backend behavior and limits.
    fn capabilities(&self) -> BackendCapabilities;

    /// Create or replace one logical memory.
    fn upsert_memory(
        &self,
        memory: MemoryRecord,
    ) -> impl Future<Output = Result<StoredMemory, Self::Error>> + WasmCompatSend;

    /// Read one logical memory.
    fn get_memory(&self, id: &str) -> Result<Option<StoredMemory>, Self::Error>;

    /// Delete one logical memory. Returns whether it existed.
    fn delete_memory(&self, id: &str) -> Result<bool, Self::Error>;

    /// List memories after a logical cursor.
    fn list_memories(
        &self,
        cursor: Option<&ListCursor>,
        limit: u64,
    ) -> Result<MemoryPage, Self::Error>;

    /// Remove vector generations that are not visible through the catalog.
    ///
    /// Implementations must make this operation idempotent.
    fn collect_garbage(&self) -> Result<(), Self::Error>;
}
