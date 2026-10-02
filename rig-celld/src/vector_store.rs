// Rig's `VectorStoreError` and `ProviderError` are large error types that this
// module must return.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;

use rig_core::{
    DynModel, Embed,
    driver::{Model, Transport},
    embeddings::Embedding,
    error::ProviderError,
    operation,
    vector_store::{
        InsertDocuments, VectorSearchRequest, VectorStoreError, VectorStoreIndex, request::Filter,
    },
    wasm_compat::{WasmCompatSend, WasmCompatSync},
    wire::Wire,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use worker::{SqlStorage, SqlStorageValue};

use crate::{
    BackendCapabilities, BackendKind, BackendMaturity, CellStorage, ListCursor, MemoryBackend,
    MemoryPage, MemoryRecord, ScoreKind, SearchKind, StoredDocument, StoredMemory, TieBreak,
    VectorError, ZeroVectorPolicy,
    vector::{cosine_score, embedding_f32_le, validate_dimensions},
};

/// The maximum vector width accepted by sqlite-vec 0.1.9, which celld embeds.
pub const MAX_DIMENSIONS: usize = 8_192;

/// Maximum number of embeddings replaced as one logical document.
pub const MAX_EMBEDDINGS_PER_DOCUMENT: usize = 128;

/// Maximum number of logical documents returned by one list operation.
pub const MAX_LIST_LIMIT: u64 = 1_000;

/// Largest generation that the Workers JavaScript SQL binding represents exactly.
pub const MAX_GENERATION: i64 = 9_007_199_254_740_991;

/// Schema version for the generation-catalog sqlite-vec layout.
pub const SCHEMA_VERSION: i64 = 1;

const DEFAULT_TABLE: &str = "rig_vectors";
const DEFAULT_ID_FIELD: &str = "id";

const SQLITE_VEC_CAPABILITIES: BackendCapabilities = BackendCapabilities {
    backend: BackendKind::SqliteVec,
    maturity: BackendMaturity::Stable,
    search: SearchKind::Exact,
    score: ScoreKind::CosineSimilarity,
    tie_break: TieBreak::IdAscending,
    zero_vectors: ZeroVectorPolicy::Reject,
    max_dimensions: MAX_DIMENSIONS,
    supports_filters: false,
    supports_upsert: true,
};

/// Errors produced by [`SqliteVecIndex`].
#[derive(Debug, thiserror::Error)]
pub enum SqliteVecError {
    /// A generated SQL identifier was not safe to interpolate.
    #[error("invalid SQLite identifier: {0}")]
    InvalidIdentifier(String),
    /// The table exists with the append-only schema from the first prototype.
    #[error(
        "SQLite table '{0}' uses an incompatible schema; migrate it explicitly instead of changing it in place"
    )]
    SchemaMigrationRequired(String),
    /// A logical ID must contain non-whitespace text.
    #[error("document ID must not be empty")]
    EmptyId,
    /// Rig's generic insertion API needs a document field to use as its ID.
    #[error("document does not contain the configured ID field '{0}'")]
    MissingId(String),
    /// IDs in the generic insertion API must be strings.
    #[error("document ID field '{0}' must be a string")]
    InvalidId(String),
    /// A logical document must have at least one searchable embedding.
    #[error("a document must contain at least one embedding")]
    EmptyEmbeddings,
    /// One logical document exceeded the supported embedding count.
    #[error(
        "a document contains {actual} embeddings; the supported maximum is {MAX_EMBEDDINGS_PER_DOCUMENT}"
    )]
    TooManyEmbeddings { actual: usize },
    /// A logical generation cannot be incremented safely.
    #[error("document generation is exhausted for ID '{0}'")]
    GenerationExhausted(String),
    /// A logical cursor contains a generation that the backend cannot bind safely.
    #[error("cursor generation must be between 1 and {MAX_GENERATION}, got {0}")]
    InvalidCursorGeneration(i64),
    /// A list request was empty or too large.
    #[error("list limit must be between 1 and {MAX_LIST_LIMIT}, got {0}")]
    InvalidListLimit(u64),
    /// Arbitrary JSON filters cannot be translated to the vec0 schema.
    #[error("sqlite-vec filtering is not supported by this index")]
    FiltersUnsupported,
    /// A non-finite threshold has no defined ordering.
    #[error("search threshold must be finite")]
    InvalidThreshold,
    /// The requested result count cannot be bound through the Workers SQL API.
    #[error("result limit is too large: {0}")]
    ResultLimitTooLarge(u64),
    /// Shared vector validation failed.
    #[error(transparent)]
    Vector(#[from] VectorError),
    /// The embedding provider failed.
    #[error(transparent)]
    Embedding(#[from] ProviderError),
    /// JSON serialization or deserialization failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// The Workers SQL API failed.
    #[error("Workers SQL error: {0}")]
    Worker(String),
}

impl From<worker::Error> for SqliteVecError {
    fn from(error: worker::Error) -> Self {
        Self::Worker(error.to_string())
    }
}

/// An embedding model that [`SqliteVecIndex`] can use.
///
/// Rig models embedding generators as `Model<W, T>` or `DynModel<Embedding>`
/// over `operation::Embedding`. Blanket implementations cover both. Other
/// embedding backends implement this trait directly.
pub trait EmbedText: WasmCompatSend + WasmCompatSync {
    /// The fixed vector width of the model. Zero means unknown.
    fn ndims(&self) -> usize;

    /// Embed `text` into one vector.
    fn embed_text(
        &self,
        text: &str,
    ) -> impl std::future::Future<Output = Result<Embedding, ProviderError>> + WasmCompatSend;
}

impl<W, T> EmbedText for Model<W, T>
where
    W: Wire<Op = operation::Embedding>,
    T: Transport<W>,
{
    fn ndims(&self) -> usize {
        self.wire.describe().capabilities.ndims
    }

    async fn embed_text(&self, text: &str) -> Result<Embedding, ProviderError> {
        Model::embed_text(self, text).await
    }
}

impl EmbedText for DynModel<operation::Embedding> {
    fn ndims(&self) -> usize {
        self.capabilities().ndims
    }

    async fn embed_text(&self, text: &str) -> Result<Embedding, ProviderError> {
        DynModel::embed_text(self, text).await
    }
}

/// A Rig vector index backed by one Durable Object's sqlite-vec database.
///
/// A regular SQLite catalog selects the active generation for each logical
/// document. Each write runs in one `transactionSync()` call, so a new vec0
/// generation, the catalog switch, and the SQLite triggers that remove
/// superseded rows commit together or not at all. Reads join through the
/// catalog, and [`Self::collect_garbage`] removes vec0 rows that the catalog
/// does not select.
#[derive(Clone, Debug)]
pub struct SqliteVecIndex<M> {
    storage: CellStorage,
    sql: SqlStorage,
    model: M,
    tables: TableNames,
    id_field: String,
    dimensions: usize,
}

impl<M> SqliteVecIndex<M>
where
    M: EmbedText,
{
    /// Create an index named `rig_vectors` and read generic document IDs from `id`.
    pub fn new(storage: CellStorage, model: M) -> Result<Self, SqliteVecError> {
        Self::named(storage, model, DEFAULT_TABLE, DEFAULT_ID_FIELD)
    }

    /// Create a named index and select the JSON field used by [`InsertDocuments`].
    ///
    /// `table` must contain only ASCII letters, digits, and underscores, and it
    /// must not start with a digit. Existing append-only prototype tables must
    /// be migrated explicitly.
    pub fn named(
        storage: CellStorage,
        model: M,
        table: impl Into<String>,
        id_field: impl Into<String>,
    ) -> Result<Self, SqliteVecError> {
        let tables = TableNames::new(table.into())?;
        let dimensions = model.ndims();
        validate_dimensions(dimensions, MAX_DIMENSIONS)?;

        let index = Self {
            sql: storage.sql(),
            storage,
            model,
            tables,
            id_field: id_field.into(),
            dimensions,
        };
        index.storage.transaction_sync(|| index.initialize())?;
        Ok(index)
    }

    /// Report the backend's static behavior and limits.
    pub fn capabilities(&self) -> BackendCapabilities {
        SQLITE_VEC_CAPABILITIES
    }

    /// Return the fixed vector width of this index.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Embed `text` and atomically replace the logical document under `id`.
    pub async fn upsert_text<T: Serialize>(
        &self,
        id: impl AsRef<str>,
        document: &T,
        text: &str,
    ) -> Result<i64, SqliteVecError> {
        let embedding = self.model.embed_text(text).await?;
        self.upsert_embedding(id, document, &embedding)
    }

    /// Replace a logical document and its one precomputed embedding.
    pub fn upsert_embedding<T: Serialize>(
        &self,
        id: impl AsRef<str>,
        document: &T,
        embedding: &Embedding,
    ) -> Result<i64, SqliteVecError> {
        self.upsert_embeddings(id, document, std::slice::from_ref(embedding))
    }

    /// Replace a logical document and its complete set of embeddings.
    ///
    /// The new vec0 rows, the catalog switch, and its garbage-collection
    /// triggers commit in one `transactionSync()` call. Inside an open
    /// transaction, this call is a nested savepoint.
    pub fn upsert_embeddings<T: Serialize>(
        &self,
        id: impl AsRef<str>,
        document: &T,
        embeddings: &[Embedding],
    ) -> Result<i64, SqliteVecError> {
        let id = id.as_ref();
        validate_id(id)?;
        if embeddings.is_empty() {
            return Err(SqliteVecError::EmptyEmbeddings);
        }
        if embeddings.len() > MAX_EMBEDDINGS_PER_DOCUMENT {
            return Err(SqliteVecError::TooManyEmbeddings {
                actual: embeddings.len(),
            });
        }

        let vectors = embeddings
            .iter()
            .map(|embedding| embedding_f32_le(&embedding.vec, self.dimensions))
            .collect::<Result<Vec<_>, _>>()?;
        let document = serde_json::to_string(document)?;
        let insert_vector = format!(
            "INSERT INTO {}(embedding, id, generation, embedding_index) VALUES (?, ?, ?, ?)",
            self.tables.vectors
        );

        self.storage.transaction_sync(|| {
            let generation = self.next_generation(id)?;

            // Rows that a write outside this index left at the new generation
            // would become visible with the catalog switch. Remove them first.
            self.sql.exec(
                &format!(
                    "DELETE FROM {} WHERE id = ? AND generation = ?",
                    self.tables.vectors
                ),
                vec![id.into(), generation.into()],
            )?;

            // One fixed statement per vector keeps one entry in celld's
            // compiled-statement cache. The transaction commits them together.
            for (index, vector) in (0_i64..).zip(vectors) {
                self.sql.exec(
                    &insert_vector,
                    vec![
                        SqlStorageValue::Blob(vector),
                        SqlStorageValue::String(id.to_owned()),
                        SqlStorageValue::Integer(generation),
                        SqlStorageValue::Integer(index),
                    ],
                )?;
            }

            // The INSERT or UPDATE trigger of the catalog deletes all older
            // generations of this document.
            self.sql.exec(
                &format!(
                    "INSERT INTO {}(id, generation, document) VALUES (?, ?, ?) \
                     ON CONFLICT(id) DO UPDATE SET \
                        generation = excluded.generation, document = excluded.document",
                    self.tables.documents
                ),
                vec![id.into(), generation.into(), document.into()],
            )?;
            Ok(generation)
        })
    }

    /// Compatibility alias whose behavior is now upsert, not append.
    pub async fn insert_text<T: Serialize>(
        &self,
        id: impl AsRef<str>,
        document: &T,
        text: &str,
    ) -> Result<(), SqliteVecError> {
        self.upsert_text(id, document, text).await.map(|_| ())
    }

    /// Compatibility alias whose behavior is now upsert, not append.
    pub fn insert_embedding<T: Serialize>(
        &self,
        id: impl AsRef<str>,
        document: &T,
        embedding: &Embedding,
    ) -> Result<(), SqliteVecError> {
        self.upsert_embedding(id, document, embedding).map(|_| ())
    }

    /// Read a logical document from the active catalog.
    pub fn get<T: DeserializeOwned>(
        &self,
        id: &str,
    ) -> Result<Option<StoredDocument<T>>, SqliteVecError> {
        validate_id(id)?;
        let rows = self
            .sql
            .exec(
                &format!(
                    "SELECT id, generation, document FROM {} WHERE id = ?",
                    self.tables.documents
                ),
                vec![id.into()],
            )?
            .to_array::<DocumentRow>()?;
        rows.into_iter().next().map(decode_document).transpose()
    }

    /// Delete a logical document and all its vector generations.
    pub fn delete(&self, id: &str) -> Result<bool, SqliteVecError> {
        validate_id(id)?;
        let deleted = self
            .sql
            .exec(
                &format!(
                    "DELETE FROM {} WHERE id = ? RETURNING id",
                    self.tables.documents
                ),
                vec![id.into()],
            )?
            .to_array::<IdRow>()?;
        Ok(!deleted.is_empty())
    }

    /// List logical documents after an `(id, generation)` cursor.
    pub fn list<T: DeserializeOwned>(
        &self,
        cursor: Option<&ListCursor>,
        limit: u64,
    ) -> Result<(Vec<StoredDocument<T>>, Option<ListCursor>), SqliteVecError> {
        if !(1..=MAX_LIST_LIMIT).contains(&limit) {
            return Err(SqliteVecError::InvalidListLimit(limit));
        }
        if let Some(cursor) = cursor
            && !(1..=MAX_GENERATION).contains(&cursor.generation)
        {
            return Err(SqliteVecError::InvalidCursorGeneration(cursor.generation));
        }
        let fetch_limit =
            i64::try_from(limit + 1).map_err(|_| SqliteVecError::InvalidListLimit(limit))?;
        let (query, bindings) = match cursor {
            Some(cursor) => (
                format!(
                    "SELECT id, generation, document FROM {} \
                     WHERE id > ? OR (id = ? AND generation > ?) \
                     ORDER BY id, generation LIMIT ?",
                    self.tables.documents
                ),
                vec![
                    cursor.id.clone().into(),
                    cursor.id.clone().into(),
                    cursor.generation.into(),
                    fetch_limit.into(),
                ],
            ),
            None => (
                format!(
                    "SELECT id, generation, document FROM {} \
                     ORDER BY id, generation LIMIT ?",
                    self.tables.documents
                ),
                vec![fetch_limit.into()],
            ),
        };
        let mut rows = self.sql.exec(&query, bindings)?.to_array::<DocumentRow>()?;
        let has_more = rows.len() > limit as usize;
        if has_more {
            rows.pop();
        }
        let documents = rows
            .into_iter()
            .map(decode_document)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = has_more
            .then(|| documents.last())
            .flatten()
            .map(|document| ListCursor {
                id: document.id.clone(),
                generation: document.generation,
            });
        Ok((documents, next_cursor))
    }

    /// Delete every vec0 row not selected by the logical document catalog.
    ///
    /// Index writes do not leave such rows. They can come from writes outside
    /// this index or from data that an earlier non-transactional layout wrote.
    /// Repeating this operation has no additional effect.
    pub fn collect_garbage(&self) -> Result<(), SqliteVecError> {
        self.sql.exec(
            &format!(
                "DELETE FROM {} WHERE rowid IN (\
                   SELECT vectors.rowid FROM {} AS vectors \
                   LEFT JOIN {} AS documents \
                     ON documents.id = vectors.id \
                    AND documents.generation = vectors.generation \
                   WHERE documents.id IS NULL\
                 )",
                self.tables.vectors, self.tables.vectors, self.tables.documents
            ),
            None,
        )?;
        Ok(())
    }

    fn initialize(&self) -> Result<(), SqliteVecError> {
        self.sql.exec(
            &format!(
                "CREATE VIRTUAL TABLE IF NOT EXISTS {} USING vec0(\
                 embedding float[{}] distance_metric=cosine, \
                 +id text, +generation integer, +embedding_index integer)",
                self.tables.vectors, self.dimensions
            ),
            None,
        )?;
        self.sql.exec(
            &format!(
                "CREATE TABLE IF NOT EXISTS {}(\
                 id TEXT PRIMARY KEY, \
                 generation INTEGER NOT NULL CHECK(generation > 0), \
                 document TEXT NOT NULL)",
                self.tables.documents
            ),
            None,
        )?;
        self.sql.exec(
            &format!(
                "CREATE TABLE IF NOT EXISTS {}(\
                 singleton INTEGER PRIMARY KEY CHECK(singleton = 1), \
                 version INTEGER NOT NULL, dimensions INTEGER NOT NULL)",
                self.tables.schema
            ),
            None,
        )?;

        self.require_columns(
            &self.tables.vectors,
            &["embedding", "id", "generation", "embedding_index"],
        )?;
        self.require_columns(&self.tables.documents, &["id", "generation", "document"])?;
        self.require_columns(&self.tables.schema, &["singleton", "version", "dimensions"])?;
        self.sql.exec(
            &format!(
                "INSERT OR IGNORE INTO {}(singleton, version, dimensions) VALUES (1, ?, ?)",
                self.tables.schema
            ),
            vec![SCHEMA_VERSION.into(), (self.dimensions as i64).into()],
        )?;
        let schema = self
            .sql
            .exec(
                &format!(
                    "SELECT version, dimensions FROM {} WHERE singleton = 1",
                    self.tables.schema
                ),
                None,
            )?
            .one::<SchemaRow>()?;
        if schema.version != SCHEMA_VERSION || schema.dimensions != self.dimensions as i64 {
            return Err(SqliteVecError::SchemaMigrationRequired(
                self.tables.vectors.clone(),
            ));
        }

        self.sql.exec(
            &format!(
                "CREATE TRIGGER IF NOT EXISTS {} AFTER INSERT ON {} BEGIN \
                   DELETE FROM {} WHERE id = NEW.id AND generation <> NEW.generation; \
                 END",
                self.tables.insert_trigger, self.tables.documents, self.tables.vectors
            ),
            None,
        )?;
        self.sql.exec(
            &format!(
                "CREATE TRIGGER IF NOT EXISTS {} AFTER UPDATE OF generation ON {} BEGIN \
                   DELETE FROM {} WHERE id = NEW.id AND generation <> NEW.generation; \
                 END",
                self.tables.update_trigger, self.tables.documents, self.tables.vectors
            ),
            None,
        )?;
        self.sql.exec(
            &format!(
                "CREATE TRIGGER IF NOT EXISTS {} AFTER DELETE ON {} BEGIN \
                   DELETE FROM {} WHERE id = OLD.id; \
                 END",
                self.tables.delete_trigger, self.tables.documents, self.tables.vectors
            ),
            None,
        )?;
        Ok(())
    }

    fn require_columns(&self, table: &str, required: &[&str]) -> Result<(), SqliteVecError> {
        let rows = self
            .sql
            .exec(&format!("PRAGMA table_info({table})"), None)?
            .to_array::<ColumnRow>()?;
        if required
            .iter()
            .all(|required| rows.iter().any(|row| row.name == *required))
        {
            Ok(())
        } else {
            Err(SqliteVecError::SchemaMigrationRequired(table.to_owned()))
        }
    }

    fn next_generation(&self, id: &str) -> Result<i64, SqliteVecError> {
        let rows = self
            .sql
            .exec(
                &format!(
                    "SELECT generation FROM {} WHERE id = ?",
                    self.tables.documents
                ),
                vec![id.into()],
            )?
            .to_array::<GenerationRow>()?;
        rows.into_iter()
            .next()
            .map_or(Some(1), |row| {
                (row.generation < MAX_GENERATION).then_some(row.generation + 1)
            })
            .ok_or_else(|| SqliteVecError::GenerationExhausted(id.to_owned()))
    }

    fn count(&self, query: &str) -> Result<i64, VectorStoreError> {
        self.sql
            .exec(query, None)
            .and_then(|cursor| cursor.one::<CountRow>())
            .map(|row| row.count)
            .map_err(SqliteVecError::from)
            .map_err(VectorStoreError::datastore)
    }

    async fn search(
        &self,
        request: VectorSearchRequest<Filter<Value>>,
    ) -> Result<Vec<SearchResult>, VectorStoreError> {
        if request.filter().is_some() {
            return Err(VectorStoreError::datastore(
                SqliteVecError::FiltersUnsupported,
            ));
        }
        if request
            .threshold()
            .is_some_and(|threshold| !threshold.is_finite())
        {
            return Err(VectorStoreError::datastore(
                SqliteVecError::InvalidThreshold,
            ));
        }
        if request.samples() == 0 {
            return Ok(Vec::new());
        }

        let wanted = usize::try_from(request.samples()).map_err(|_| {
            VectorStoreError::datastore(SqliteVecError::ResultLimitTooLarge(request.samples()))
        })?;
        // An empty catalog has no visible results, so skip the embedding request.
        let has_documents = self.count(&format!(
            "SELECT EXISTS(SELECT 1 FROM {}) AS count",
            self.tables.documents
        ))?;
        if has_documents == 0 {
            return Ok(Vec::new());
        }
        let embedding = self.model.embed_text(request.query()).await?;
        let vector = embedding_f32_le(&embedding.vec, self.dimensions)
            .map_err(SqliteVecError::from)
            .map_err(VectorStoreError::datastore)?;

        // Other requests can write while the embedding request waits. The code
        // below does not await, so the count and every candidate query read the
        // same database state.
        let physical = self.count(&format!(
            "SELECT COUNT(*) AS count FROM {}",
            self.tables.vectors
        ))?;
        if physical <= 0 {
            return Ok(Vec::new());
        }
        let physical = usize::try_from(physical).map_err(|_| {
            VectorStoreError::datastore(SqliteVecError::ResultLimitTooLarge(request.samples()))
        })?;

        let mut candidate_limit = wanted.min(physical).max(1);
        let mut results;
        loop {
            let limit = i64::try_from(candidate_limit).map_err(|_| {
                VectorStoreError::datastore(SqliteVecError::ResultLimitTooLarge(request.samples()))
            })?;
            let rows = self
                .sql
                .exec(
                    &format!(
                        "SELECT vectors.id, vectors.generation, \
                                documents.generation AS active_generation, \
                                documents.document, vectors.distance \
                         FROM {} AS vectors \
                         LEFT JOIN {} AS documents ON documents.id = vectors.id \
                         WHERE vectors.embedding MATCH ? AND k = ? \
                         ORDER BY vectors.distance",
                        self.tables.vectors, self.tables.documents
                    ),
                    vec![SqlStorageValue::Blob(vector.clone()), limit.into()],
                )
                .map_err(SqliteVecError::from)
                .map_err(VectorStoreError::datastore)?
                .to_array::<CandidateRow>()
                .map_err(SqliteVecError::from)
                .map_err(VectorStoreError::datastore)?;

            let last_score = rows
                .last()
                .map(|row| cosine_score(row.distance))
                .transpose()
                .map_err(SqliteVecError::from)
                .map_err(VectorStoreError::datastore)?;
            results = active_results(rows)?;
            results.sort_by(|left, right| {
                right
                    .score
                    .total_cmp(&left.score)
                    .then_with(|| left.id.cmp(&right.id))
            });

            let boundary_is_complete = results
                .get(wanted.saturating_sub(1))
                .zip(last_score)
                .is_some_and(|(last_result, last_candidate)| last_candidate < last_result.score);
            if candidate_limit == physical || boundary_is_complete {
                break;
            }
            candidate_limit = candidate_limit.saturating_mul(2).min(physical);
        }

        results.truncate(wanted);
        if let Some(threshold) = request.threshold() {
            results.retain(|result| result.score >= threshold);
        }
        Ok(results)
    }
}

impl<M> VectorStoreIndex for SqliteVecIndex<M>
where
    M: EmbedText,
{
    type Filter = Filter<Value>;

    async fn top_n<T: DeserializeOwned + WasmCompatSend>(
        &self,
        request: VectorSearchRequest<Self::Filter>,
    ) -> Result<Vec<(f64, String, T)>, VectorStoreError> {
        self.search(request)
            .await?
            .into_iter()
            .map(|result| {
                serde_json::from_str(&result.document)
                    .map(|document| (result.score, result.id, document))
                    .map_err(VectorStoreError::from)
            })
            .collect()
    }

    async fn top_n_ids(
        &self,
        request: VectorSearchRequest<Self::Filter>,
    ) -> Result<Vec<(f64, String)>, VectorStoreError> {
        Ok(self
            .search(request)
            .await?
            .into_iter()
            .map(|result| (result.score, result.id))
            .collect())
    }
}

impl<M> InsertDocuments for SqliteVecIndex<M>
where
    M: EmbedText,
{
    /// Upsert all documents in one `transactionSync()` call.
    ///
    /// An error in any document rolls back the complete batch.
    async fn insert_documents<Doc: Serialize + Embed + WasmCompatSend>(
        &self,
        documents: Vec<(Doc, Vec<Embedding>)>,
    ) -> Result<(), VectorStoreError> {
        self.storage
            .transaction_sync(|| {
                for (document, embeddings) in &documents {
                    let value = serde_json::to_value(document)?;
                    let id = value
                        .get(&self.id_field)
                        .ok_or_else(|| SqliteVecError::MissingId(self.id_field.clone()))?
                        .as_str()
                        .ok_or_else(|| SqliteVecError::InvalidId(self.id_field.clone()))?;
                    self.upsert_embeddings(id, &value, embeddings)?;
                }
                Ok::<_, SqliteVecError>(())
            })
            .map_err(VectorStoreError::datastore)
    }
}

impl<M> MemoryBackend for SqliteVecIndex<M>
where
    M: EmbedText,
{
    type Error = SqliteVecError;

    fn capabilities(&self) -> BackendCapabilities {
        SQLITE_VEC_CAPABILITIES
    }

    async fn upsert_memory(&self, memory: MemoryRecord) -> Result<StoredMemory, Self::Error> {
        let generation = self
            .upsert_text(&memory.id, &memory, &memory.content)
            .await?;
        Ok(StoredDocument {
            id: memory.id.clone(),
            generation,
            document: memory,
        })
    }

    fn get_memory(&self, id: &str) -> Result<Option<StoredMemory>, Self::Error> {
        self.get(id)
    }

    fn delete_memory(&self, id: &str) -> Result<bool, Self::Error> {
        self.delete(id)
    }

    fn list_memories(
        &self,
        cursor: Option<&ListCursor>,
        limit: u64,
    ) -> Result<MemoryPage, Self::Error> {
        let (memories, next_cursor) = self.list(cursor, limit)?;
        Ok(MemoryPage {
            memories,
            next_cursor,
        })
    }

    fn collect_garbage(&self) -> Result<(), Self::Error> {
        SqliteVecIndex::collect_garbage(self)
    }
}

#[derive(Clone, Debug)]
struct TableNames {
    vectors: String,
    documents: String,
    schema: String,
    insert_trigger: String,
    update_trigger: String,
    delete_trigger: String,
}

impl TableNames {
    fn new(vectors: String) -> Result<Self, SqliteVecError> {
        if !valid_identifier(&vectors) {
            return Err(SqliteVecError::InvalidIdentifier(vectors));
        }
        Ok(Self {
            documents: format!("{vectors}_documents"),
            schema: format!("{vectors}_schema"),
            insert_trigger: format!("{vectors}_catalog_insert_gc"),
            update_trigger: format!("{vectors}_catalog_update_gc"),
            delete_trigger: format!("{vectors}_catalog_delete_gc"),
            vectors,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ColumnRow {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GenerationRow {
    generation: i64,
}

#[derive(Debug, Deserialize)]
struct SchemaRow {
    version: i64,
    dimensions: i64,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    count: i64,
}

#[derive(Debug, Deserialize)]
struct IdRow {
    #[serde(rename = "id")]
    _id: String,
}

#[derive(Debug, Deserialize)]
struct DocumentRow {
    id: String,
    generation: i64,
    document: String,
}

#[derive(Debug, Deserialize)]
struct CandidateRow {
    id: String,
    generation: i64,
    active_generation: Option<i64>,
    document: Option<String>,
    distance: f64,
}

#[derive(Debug)]
struct SearchResult {
    score: f64,
    id: String,
    document: String,
}

fn decode_document<T: DeserializeOwned>(
    row: DocumentRow,
) -> Result<StoredDocument<T>, SqliteVecError> {
    Ok(StoredDocument {
        id: row.id,
        generation: row.generation,
        document: serde_json::from_str(&row.document)?,
    })
}

fn active_results(rows: Vec<CandidateRow>) -> Result<Vec<SearchResult>, VectorStoreError> {
    let mut best = BTreeMap::<String, SearchResult>::new();
    for row in rows {
        if row.active_generation != Some(row.generation) {
            continue;
        }
        let Some(document) = row.document else {
            continue;
        };
        let score = cosine_score(row.distance)
            .map_err(SqliteVecError::from)
            .map_err(VectorStoreError::datastore)?;
        match best.get_mut(&row.id) {
            Some(existing) if score > existing.score => {
                existing.score = score;
                existing.document = document;
            }
            Some(_) => {}
            None => {
                best.insert(
                    row.id.clone(),
                    SearchResult {
                        score,
                        id: row.id,
                        document,
                    },
                );
            }
        }
    }
    Ok(best.into_values().collect())
}

fn validate_id(id: &str) -> Result<(), SqliteVecError> {
    if id.trim().is_empty() {
        Err(SqliteVecError::EmptyId)
    } else {
        Ok(())
    }
}

fn valid_identifier(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use rig_core::vector_store::VectorStoreError;

    use super::{CandidateRow, SqliteVecError, TableNames, active_results, valid_identifier};

    #[test]
    fn identifiers_accept_only_unquoted_sql_names() {
        assert!(valid_identifier("rig_vectors_2"));
        assert!(valid_identifier("_private"));
        assert!(!valid_identifier("2vectors"));
        assert!(!valid_identifier("vectors; DROP TABLE state"));
        assert!(!valid_identifier(""));
    }

    #[test]
    fn table_names_share_one_validated_prefix() -> Result<(), SqliteVecError> {
        let names = TableNames::new("memories".to_owned())?;
        assert_eq!(names.documents, "memories_documents");
        assert_eq!(names.update_trigger, "memories_catalog_update_gc");
        Ok(())
    }

    #[test]
    fn active_results_keep_the_best_embedding_per_document() -> Result<(), VectorStoreError> {
        let rows = vec![
            CandidateRow {
                id: "a".to_owned(),
                generation: 2,
                active_generation: Some(2),
                document: Some("{\"id\":\"a\"}".to_owned()),
                distance: 0.4,
            },
            CandidateRow {
                id: "a".to_owned(),
                generation: 2,
                active_generation: Some(2),
                document: Some("{\"id\":\"a\"}".to_owned()),
                distance: 0.1,
            },
            CandidateRow {
                id: "orphan".to_owned(),
                generation: 3,
                active_generation: None,
                document: None,
                distance: 0.0,
            },
        ];
        let results = active_results(rows)?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "a");
        assert!((results[0].score - 0.9).abs() < f64::EPSILON);
        Ok(())
    }
}
