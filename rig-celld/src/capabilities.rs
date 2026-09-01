//! Storage behavior reported by a Rig celld backend.

use serde::{Deserialize, Serialize};

/// Storage implementation used by a backend.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendKind {
    /// celld Durable Object SQLite with its embedded sqlite-vec extension.
    SqliteVec,
    /// Encrypted Turso files stored through Durable Object SQLite.
    EncryptedTurso,
}

/// Readiness level of a backend.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendMaturity {
    /// The documented persistence contract has conformance coverage.
    Stable,
    /// A required persistence or durability invariant is not established.
    Experimental,
}

/// Similarity-search implementation.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SearchKind {
    /// Exact nearest-neighbor search.
    Exact,
}

/// Meaning of scores returned through Rig.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScoreKind {
    /// Cosine similarity in the inclusive range `-1..=1`.
    CosineSimilarity,
}

/// Deterministic ordering used when scores compare equal.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TieBreak {
    /// Sort equal scores by logical document ID in ascending byte order.
    IdAscending,
}

/// Behavior for embeddings whose f32 representation has no non-zero element.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ZeroVectorPolicy {
    /// Reject zero vectors at insertion and query time.
    Reject,
}

/// Static behavior and limits of a storage backend.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackendCapabilities {
    /// Storage implementation.
    pub backend: BackendKind,
    /// Readiness level.
    pub maturity: BackendMaturity,
    /// Search implementation.
    pub search: SearchKind,
    /// Returned score meaning and range.
    pub score: ScoreKind,
    /// Equal-score ordering.
    pub tie_break: TieBreak,
    /// Zero-vector behavior.
    pub zero_vectors: ZeroVectorPolicy,
    /// Largest supported embedding width.
    pub max_dimensions: usize,
    /// Whether arbitrary Rig JSON filters are supported.
    pub supports_filters: bool,
    /// Whether replacing a logical ID is supported.
    pub supports_upsert: bool,
    /// Whether the backend adds application-managed encryption at rest.
    pub encrypted_at_rest: bool,
}
