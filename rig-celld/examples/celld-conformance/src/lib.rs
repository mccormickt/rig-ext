use rig_celld::{
    BackendKind, BackendMaturity, CellStorage, EmbedText, MemoryBackend, MemoryRecord, SearchKind,
    SqliteVecIndex, StoredMemory, TieBreak, ZeroVectorPolicy,
};
use rig_core::{
    embeddings::Embedding,
    error::ProviderError,
    vector_store::{InsertDocuments, VectorSearchRequest, VectorStoreIndex, request::Filter},
};
use serde::Deserialize;
use serde_json::{Value, json};
use worker::*;

const TABLE: &str = "conformance_vectors";
const RACE_TABLE: &str = "conformance_race_vectors";
const CONCURRENT_ID: &str = "concurrent";

#[derive(Clone, Debug)]
struct FixtureEmbeddingModel;

impl EmbedText for FixtureEmbeddingModel {
    fn ndims(&self) -> usize {
        2
    }

    async fn embed_text(&self, text: &str) -> std::result::Result<Embedding, ProviderError> {
        if text.contains("slow") {
            // Lets another write run while a search waits for its embedding.
            send::SendFuture::new(Delay::from(std::time::Duration::from_millis(50))).await;
        }
        let vector = if text.contains("alpha") {
            vec![1.0, 0.0]
        } else if text.contains("beta") {
            vec![0.0, 1.0]
        } else if text.contains("diagonal") {
            vec![1.0, 1.0]
        } else {
            return Err(ProviderError::Provider(format!(
                "fixture has no vector for '{text}'"
            )));
        };
        Ok(Embedding {
            document: text.to_owned(),
            vec: vector,
        })
    }
}

#[durable_object(fetch)]
pub struct ConformanceCell {
    storage: CellStorage,
}

impl DurableObject for ConformanceCell {
    fn new(state: State, _env: Env) -> Self {
        Self {
            storage: state.into(),
        }
    }

    async fn fetch(&self, mut request: Request) -> Result<Response> {
        let path = request.path();
        let operation = path.rsplit('/').next().unwrap_or_default();
        match (request.method(), operation) {
            (Method::Post, "run") => self.run_suite().await,
            (Method::Get, "verify-reopen") => self.verify_reopen().await,
            (Method::Post, "reset-concurrency") => self.reset_concurrency(),
            (Method::Post, "upsert") => self.upsert(&mut request).await,
            (Method::Post, "verify-concurrency") => {
                let input = request.json::<ConcurrencyCheck>().await?;
                self.verify_concurrency(input.expected_generation).await
            }
            _ => Response::error("unknown conformance operation\n", 404),
        }
    }
}

impl ConformanceCell {
    fn index(&self) -> Result<SqliteVecIndex<FixtureEmbeddingModel>> {
        SqliteVecIndex::named(self.storage.clone(), FixtureEmbeddingModel, TABLE, "id")
            .map_err(worker_error)
    }

    async fn run_suite(&self) -> Result<Response> {
        let index = self.index()?;
        let sql = self.storage.sql();
        sql.exec(
            "DROP TRIGGER IF EXISTS conformance_vectors_test_abort",
            None,
        )?;
        sql.exec("DELETE FROM conformance_vectors_documents", None)?;
        index.collect_garbage().map_err(worker_error)?;

        let capabilities = index.capabilities();
        ensure(
            capabilities.backend == BackendKind::SqliteVec,
            "backend kind",
        )?;
        ensure(
            capabilities.maturity == BackendMaturity::Stable,
            "backend maturity",
        )?;
        ensure(capabilities.search == SearchKind::Exact, "search kind")?;
        ensure(capabilities.tie_break == TieBreak::IdAscending, "tie break")?;
        ensure(
            capabilities.zero_vectors == ZeroVectorPolicy::Reject,
            "zero-vector policy",
        )?;
        ensure(capabilities.supports_upsert, "upsert capability")?;
        ensure(!capabilities.supports_filters, "filter capability")?;

        let first = upsert(&index, "a", "alpha original").await?;
        ensure(first.generation == 1, "first generation")?;
        upsert(&index, "b", "beta original").await?;
        upsert(&index, "tie-b", "alpha right").await?;
        upsert(&index, "tie-a", "alpha left").await?;

        let replaced = upsert(&index, "b", "beta replaced").await?;
        ensure(replaced.generation == 2, "upsert generation")?;
        let stored = index
            .get_memory("b")
            .map_err(worker_error)?
            .ok_or_else(|| Error::RustError("upserted memory is missing".to_owned()))?;
        ensure(stored == replaced, "upserted memory contents")?;
        ensure(orphan_count(&sql)? == 0, "atomic generation cleanup")?;

        let first_page = index.list_memories(None, 2).map_err(worker_error)?;
        ensure(
            ids(&first_page.memories) == ["a", "b"],
            "first logical list page",
        )?;
        let cursor = first_page
            .next_cursor
            .as_ref()
            .ok_or_else(|| Error::RustError("first list page has no cursor".to_owned()))?;
        ensure(cursor.id == "b" && cursor.generation == 2, "logical cursor")?;
        let second_page = index.list_memories(Some(cursor), 2).map_err(worker_error)?;
        ensure(
            ids(&second_page.memories) == ["tie-a", "tie-b"],
            "second logical list page",
        )?;
        ensure(second_page.next_cursor.is_none(), "final list cursor")?;

        let ranked = search(&index, "alpha", 10).await?;
        ensure(
            ranked
                .iter()
                .map(|(_, id, _)| id.as_str())
                .collect::<Vec<_>>()
                == ["a", "tie-a", "tie-b", "b"],
            "score and stable tie ranking",
        )?;
        ensure(
            ranked[..3]
                .iter()
                .all(|(score, _, _)| (*score - 1.0).abs() < f64::EPSILON),
            "cosine score normalization",
        )?;

        let multi = memory("multi", "diagonal");
        let multi_generation = index
            .upsert_embeddings(
                "multi",
                &multi,
                &[
                    Embedding {
                        document: "alpha".to_owned(),
                        vec: vec![1.0, 0.0],
                    },
                    Embedding {
                        document: "beta".to_owned(),
                        vec: vec![0.0, 1.0],
                    },
                ],
            )
            .map_err(worker_error)?;
        ensure(multi_generation == 1, "multi-embedding generation")?;
        ensure(
            search(&index, "beta", 10)
                .await?
                .iter()
                .any(|(score, id, _)| id == "multi" && (*score - 1.0).abs() < f64::EPSILON),
            "multi-embedding retrieval",
        )?;
        ensure(
            index.delete_memory("multi").map_err(worker_error)?,
            "multi-embedding delete",
        )?;

        ensure(index.delete_memory("b").map_err(worker_error)?, "delete")?;
        ensure(
            !index.delete_memory("b").map_err(worker_error)?,
            "idempotent delete",
        )?;

        self.verify_atomic_catalog_rollback(&index).await?;
        self.verify_nested_transactions(&index)?;
        verify_atomic_batch(&index).await?;
        self.verify_search_after_concurrent_write().await?;

        sql.exec(
            "INSERT INTO conformance_vectors(embedding, id, generation, embedding_index) \
             VALUES (?, ?, ?, ?)",
            vec![
                SqlStorageValue::Blob(vector_blob([1.0, 0.0])),
                "orphan".into(),
                41_i64.into(),
                0_i64.into(),
            ],
        )?;
        ensure(
            index.get_memory("orphan").map_err(worker_error)?.is_none(),
            "orphan get visibility",
        )?;
        ensure(
            !search(&index, "alpha", 10)
                .await?
                .iter()
                .any(|(_, id, _)| id == "orphan"),
            "orphan search visibility",
        )?;
        ensure(
            !index
                .list_memories(None, 10)
                .map_err(worker_error)?
                .memories
                .iter()
                .any(|memory| memory.id == "orphan"),
            "orphan list visibility",
        )?;
        ensure(orphan_count(&sql)? == 1, "orphan fixture")?;
        index.collect_garbage().map_err(worker_error)?;
        ensure(orphan_count(&sql)? == 0, "orphan collection")?;
        index.collect_garbage().map_err(worker_error)?;
        ensure(orphan_count(&sql)? == 0, "idempotent orphan collection")?;

        sql.exec("DROP TABLE IF EXISTS legacy_vectors", None)?;
        sql.exec(
            "CREATE VIRTUAL TABLE legacy_vectors USING vec0(\
             embedding float[2] distance_metric=cosine, +id text, +document text)",
            None,
        )?;
        ensure(
            SqliteVecIndex::named(
                self.storage.clone(),
                FixtureEmbeddingModel,
                "legacy_vectors",
                "id",
            )
            .is_err(),
            "legacy schema requires explicit migration",
        )?;

        Response::from_json(&json!({
            "status": "ok",
            "generation": replaced.generation,
            "ranking": ranked.iter().map(|(_, id, _)| id).collect::<Vec<_>>()
        }))
    }

    async fn verify_atomic_catalog_rollback(
        &self,
        index: &SqliteVecIndex<FixtureEmbeddingModel>,
    ) -> Result<()> {
        let sql = self.storage.sql();
        upsert(index, "atomic", "alpha before switch").await?;
        sql.exec(
            "INSERT INTO conformance_vectors(embedding, id, generation, embedding_index) \
             VALUES (?, ?, ?, ?)",
            vec![
                SqlStorageValue::Blob(vector_blob([0.0, 1.0])),
                "atomic".into(),
                2_i64.into(),
                0_i64.into(),
            ],
        )?;
        let orphan = atomic_orphans(&sql)?;
        ensure(orphan.len() == 1, "failed generation fixture")?;
        sql.exec("DROP TRIGGER conformance_vectors_catalog_update_gc", None)?;
        sql.exec(
            "CREATE TRIGGER conformance_vectors_test_abort \
             AFTER UPDATE OF generation ON conformance_vectors_documents BEGIN \
               DELETE FROM conformance_vectors \
                 WHERE id = NEW.id AND generation <> NEW.generation; \
               SELECT RAISE(ABORT, 'forced catalog switch failure'); \
             END",
            None,
        )?;

        let failed_switch = sql.exec(
            "UPDATE conformance_vectors_documents \
             SET generation = 2, document = ? WHERE id = ?",
            vec![
                serde_json::to_string(&memory("atomic", "beta after switch"))?.into(),
                "atomic".into(),
            ],
        );
        ensure(failed_switch.is_err(), "forced catalog switch failure")?;
        ensure(
            upsert(index, "atomic", "beta after switch").await.is_err(),
            "forced upsert failure",
        )?;

        let visible = index
            .get_memory("atomic")
            .map_err(worker_error)?
            .ok_or_else(|| Error::RustError("atomic memory is missing".to_owned()))?;
        ensure(
            visible.generation == 1 && visible.document.content == "alpha before switch",
            "catalog switch rollback",
        )?;
        ensure(
            search(index, "alpha", 10)
                .await?
                .iter()
                .any(|(_, id, _)| id == "atomic"),
            "old vector rollback",
        )?;
        ensure(
            !search(index, "beta", 10)
                .await?
                .iter()
                .any(|(score, id, _)| id == "atomic" && *score > 0.5),
            "failed generation isolation",
        )?;
        // Only the row inserted above remains. The failed upsert rolled back
        // its leftover cleanup and its vec0 rows with its catalog switch.
        ensure(orphan_count(&sql)? == 1, "failed generation orphan")?;
        ensure(atomic_orphans(&sql)? == orphan, "failed upsert rollback")?;

        sql.exec("DROP TRIGGER conformance_vectors_test_abort", None)?;
        let restored = self.index()?;
        restored.collect_garbage().map_err(worker_error)?;
        ensure(orphan_count(&sql)? == 0, "failed generation collection")?;
        ensure(
            restored.delete_memory("atomic").map_err(worker_error)?,
            "atomic fixture cleanup",
        )
    }

    fn verify_nested_transactions(
        &self,
        index: &SqliteVecIndex<FixtureEmbeddingModel>,
    ) -> Result<()> {
        let alpha = Embedding {
            document: "alpha".to_owned(),
            vec: vec![1.0, 0.0],
        };
        let rolled_back = self.storage.transaction_sync(|| {
            index
                .upsert_embedding("nested", &memory("nested", "alpha nested"), &alpha)
                .map_err(worker_error)?;
            Err::<(), _>(Error::RustError("forced outer rollback".to_owned()))
        });
        ensure(rolled_back.is_err(), "outer transaction failure")?;
        ensure(
            index.get_memory("nested").map_err(worker_error)?.is_none(),
            "outer rollback discards a nested upsert",
        )?;
        ensure(
            orphan_count(&self.storage.sql())? == 0,
            "outer rollback discards nested vectors",
        )?;

        let generations = self.storage.transaction_sync(|| {
            let first = index
                .upsert_embedding("nested", &memory("nested", "alpha first"), &alpha)
                .map_err(worker_error)?;
            let second = index
                .upsert_embedding("nested", &memory("nested", "alpha second"), &alpha)
                .map_err(worker_error)?;
            Ok::<_, Error>((first, second))
        })?;
        ensure(generations == (1, 2), "nested upsert generations")?;
        let stored = index
            .get_memory("nested")
            .map_err(worker_error)?
            .ok_or_else(|| Error::RustError("nested memory is missing".to_owned()))?;
        ensure(
            stored.generation == 2 && stored.document.content == "alpha second",
            "outer commit keeps the last nested upsert",
        )?;
        ensure(
            orphan_count(&self.storage.sql())? == 0,
            "nested upserts replace earlier generations",
        )?;
        ensure(
            index.delete_memory("nested").map_err(worker_error)?,
            "nested fixture cleanup",
        )?;

        let inner_error = self.storage.transaction_sync(|| {
            index
                .upsert_embedding("outer-a", &memory("outer-a", "alpha"), &alpha)
                .map_err(worker_error)?;
            let inner = self.storage.transaction_sync(|| {
                index
                    .upsert_embedding("inner-b", &memory("inner-b", "alpha"), &alpha)
                    .map_err(worker_error)?;
                Err::<(), _>(Error::RustError("forced inner rollback".to_owned()))
            });
            let Err(Error::RustError(message)) = inner else {
                return Err(Error::RustError(
                    "inner transaction did not return its error".to_owned(),
                ));
            };
            index
                .upsert_embedding("outer-c", &memory("outer-c", "alpha"), &alpha)
                .map_err(worker_error)?;
            Ok::<_, Error>(message)
        })?;
        ensure(
            inner_error == "forced inner rollback",
            "inner transaction returns the original error",
        )?;
        ensure(
            index.get_memory("inner-b").map_err(worker_error)?.is_none(),
            "inner rollback discards its writes",
        )?;
        ensure(
            orphan_count(&self.storage.sql())? == 0,
            "inner rollback discards its vectors",
        )?;
        for id in ["outer-a", "outer-c"] {
            ensure(
                index.get_memory(id).map_err(worker_error)?.is_some(),
                "outer transaction commits after an inner rollback",
            )?;
            ensure(
                index.delete_memory(id).map_err(worker_error)?,
                "inner rollback fixture cleanup",
            )?;
        }
        Ok(())
    }

    async fn verify_search_after_concurrent_write(&self) -> Result<()> {
        let index = SqliteVecIndex::named(
            self.storage.clone(),
            FixtureEmbeddingModel,
            RACE_TABLE,
            "id",
        )
        .map_err(worker_error)?;
        self.storage
            .sql()
            .exec("DELETE FROM conformance_race_vectors_documents", None)?;
        index.collect_garbage().map_err(worker_error)?;
        upsert(&index, "race-beta", "beta").await?;

        let (ranked, written) = futures::join!(search(&index, "slow alpha", 10), async {
            for id in ["race-alpha-1", "race-alpha-2", "race-alpha-3"] {
                upsert(&index, id, "alpha").await?;
            }
            Ok::<_, Error>(())
        });
        written?;
        ensure(
            ranked?.len() == 4,
            "search sees writes made while it waits for its embedding",
        )
    }

    async fn verify_reopen(&self) -> Result<Response> {
        let index = self.index()?;
        let memory = index
            .get_memory("a")
            .map_err(worker_error)?
            .ok_or_else(|| Error::RustError("memory 'a' did not survive reopen".to_owned()))?;
        ensure(memory.generation == 1, "reopen generation")?;
        ensure(
            memory.document.content == "alpha original",
            "reopen contents",
        )?;
        let ranked = search(&index, "alpha", 10).await?;
        ensure(
            ranked.iter().any(|(_, id, _)| id == "a"),
            "reopen vector search",
        )?;
        Response::from_json(&json!({"status": "ok", "generation": memory.generation}))
    }

    fn reset_concurrency(&self) -> Result<Response> {
        let index = self.index()?;
        let _deleted = index.delete_memory(CONCURRENT_ID).map_err(worker_error)?;
        Response::from_json(&json!({"status": "ok"}))
    }

    async fn upsert(&self, request: &mut Request) -> Result<Response> {
        let memory = request.json::<MemoryRecord>().await?;
        ensure(memory.id == CONCURRENT_ID, "concurrency fixture ID")?;
        let stored = self
            .index()?
            .upsert_memory(memory)
            .await
            .map_err(worker_error)?;
        Response::from_json(&stored)
    }

    async fn verify_concurrency(&self, expected_generation: i64) -> Result<Response> {
        let index = self.index()?;
        let stored = index
            .get_memory(CONCURRENT_ID)
            .map_err(worker_error)?
            .ok_or_else(|| Error::RustError("concurrent memory is missing".to_owned()))?;
        ensure(
            stored.generation == expected_generation,
            "concurrent generation",
        )?;
        let matches = search(&index, "alpha", 100).await?;
        ensure(
            matches
                .iter()
                .filter(|(_, id, _)| id == CONCURRENT_ID)
                .count()
                == 1,
            "one visible concurrent generation",
        )?;
        Response::from_json(&json!({"status": "ok", "generation": stored.generation}))
    }
}

#[event(fetch)]
async fn fetch(request: Request, env: Env, _context: Context) -> Result<Response> {
    let path = request.path();
    let mut segments = path.strip_prefix('/').unwrap_or_default().split('/');
    let route = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    );
    let (Some("conformance"), Some(cell), Some(_operation), None) = route else {
        return Response::error(
            "use /conformance/{cell}/{run|verify-reopen|reset-concurrency|upsert|verify-concurrency}\n",
            404,
        );
    };
    if cell.is_empty() {
        return Response::error("cell name must not be empty\n", 400);
    }
    env.durable_object("CONFORMANCE")?
        .id_from_name(cell)?
        .get_stub()?
        .fetch_with_request(request)
        .await
}

#[derive(Debug, Deserialize)]
struct CountRow {
    count: i64,
}

#[derive(Debug, Deserialize)]
struct RowIdRow {
    rowid: i64,
}

#[derive(Debug, Deserialize)]
struct ConcurrencyCheck {
    expected_generation: i64,
}

async fn upsert(
    index: &SqliteVecIndex<FixtureEmbeddingModel>,
    id: &str,
    content: &str,
) -> Result<StoredMemory> {
    index
        .upsert_memory(memory(id, content))
        .await
        .map_err(worker_error)
}

fn memory(id: &str, content: &str) -> MemoryRecord {
    MemoryRecord {
        id: id.to_owned(),
        content: content.to_owned(),
        metadata: Value::Null,
    }
}

async fn search(
    index: &SqliteVecIndex<FixtureEmbeddingModel>,
    query: &str,
    samples: u64,
) -> Result<Vec<(f64, String, MemoryRecord)>> {
    index
        .top_n(
            VectorSearchRequest::<Filter<Value>>::builder()
                .query(query)
                .samples(samples)
                .build(),
        )
        .await
        .map_err(worker_error)
}

async fn verify_atomic_batch(index: &SqliteVecIndex<FixtureEmbeddingModel>) -> Result<()> {
    let alpha = || {
        vec![Embedding {
            document: "alpha".to_owned(),
            vec: vec![1.0, 0.0],
        }]
    };
    let failed = index
        .insert_documents(vec![
            (json!({"id": "batch-a", "content": "alpha"}), alpha()),
            (json!({"id": 7, "content": "alpha"}), alpha()),
        ])
        .await;
    ensure(failed.is_err(), "batch with an invalid ID fails")?;
    ensure(
        index.get_memory("batch-a").map_err(worker_error)?.is_none(),
        "failed batch rolls back earlier documents",
    )?;

    index
        .insert_documents(vec![
            (json!({"id": "batch-a", "content": "alpha"}), alpha()),
            (json!({"id": "batch-b", "content": "alpha"}), alpha()),
        ])
        .await
        .map_err(worker_error)?;
    for id in ["batch-a", "batch-b"] {
        ensure(
            index.get_memory(id).map_err(worker_error)?.is_some(),
            "batch commits every document",
        )?;
        ensure(
            index.delete_memory(id).map_err(worker_error)?,
            "batch fixture cleanup",
        )?;
    }
    Ok(())
}

fn ids(memories: &[StoredMemory]) -> Vec<&str> {
    memories.iter().map(|memory| memory.id.as_str()).collect()
}

fn orphan_count(sql: &SqlStorage) -> Result<i64> {
    Ok(sql
        .exec(
            "SELECT COUNT(*) AS count FROM conformance_vectors AS vectors \
             LEFT JOIN conformance_vectors_documents AS documents \
               ON documents.id = vectors.id \
              AND documents.generation = vectors.generation \
             WHERE documents.id IS NULL",
            None,
        )?
        .one::<CountRow>()?
        .count)
}

fn atomic_orphans(sql: &SqlStorage) -> Result<Vec<i64>> {
    Ok(sql
        .exec(
            "SELECT rowid FROM conformance_vectors WHERE id = ? AND generation = ?",
            vec!["atomic".into(), 2_i64.into()],
        )?
        .to_array::<RowIdRow>()?
        .into_iter()
        .map(|row| row.rowid)
        .collect())
}

fn vector_blob(values: [f32; 2]) -> Vec<u8> {
    values.into_iter().flat_map(f32::to_le_bytes).collect()
}

fn ensure(condition: bool, invariant: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::RustError(format!(
            "conformance invariant failed: {invariant}"
        )))
    }
}

fn worker_error(error: impl std::fmt::Display) -> Error {
    Error::RustError(error.to_string())
}
