//! Independent SQLite invocation ledger. No backend-private tables are used.

use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::*;

/// A persistent guard for processes on one host sharing a local database file.
/// SQLite supplies cross-connection/process uniqueness. This is not distributed
/// multi-host storage; do not place the WAL database on a network filesystem.
///
/// Uses WAL and `synchronous=FULL`. Acknowledged commits survive process crashes
/// and host/power loss only when the filesystem and device honor SQLite's sync
/// calls. All users must preserve this setting and the database/WAL together.
/// Rollback to a backup can permit duplicate effects. There is no expiry, row
/// deletion, takeover, or repair API.
///
/// Operations require a Tokio runtime. At most eight operations per store
/// instance can serialize, queue, or execute. Each input and stored claim/result
/// is at most 1 MiB and 64 JSON containers. Saturation fails closed without an
/// internal waiting queue. Each operation can read a claim and result (2 MiB).
/// Caller-owned inputs/results and SQLite's page cache are outside this bound.
/// A canceled wait leaves its blocking task and capacity permit alive until
/// the operation ends. It does not prove cancellation of a write.
pub struct SqliteGuardStore {
    connection: Arc<Mutex<Connection>>,
    capacity: Arc<Semaphore>,
}

fn error(error: impl std::fmt::Display) -> GuardError {
    GuardError::Store(error.to_string())
}

impl SqliteGuardStore {
    /// Open a persistent local file. Initialization is synchronous.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, GuardError> {
        if path.as_ref().as_os_str().is_empty() || path.as_ref() == Path::new(":memory:") {
            return Err(error("a persistent database path is required"));
        }
        let connection = Connection::open(path).map_err(error)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(error)?;
        let journal: String = connection
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .map_err(error)?;
        if journal != "wal" {
            return Err(error("SQLite could not enable WAL"));
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(error)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS rig_invocation_guard_v1 (
                key TEXT PRIMARY KEY NOT NULL,
                token TEXT NOT NULL,
                claim TEXT NOT NULL CHECK(length(CAST(claim AS BLOB)) <= 1048576),
                result TEXT CHECK(result IS NULL OR length(CAST(result AS BLOB)) <= 1048576)
            ) STRICT;",
            )
            .map_err(error)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            capacity: Arc::new(Semaphore::new(8)),
        })
    }

    fn reserve(&self) -> Result<OwnedSemaphorePermit, GuardError> {
        self.capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| error("SQLite guard capacity exhausted"))
    }

    async fn run<T: Send + 'static>(
        &self,
        permit: OwnedSemaphorePermit,
        operation: impl FnOnce(&mut Connection) -> Result<T, GuardError> + Send + 'static,
    ) -> Result<T, GuardError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(error)?;
        let connection = self.connection.clone();
        runtime
            .spawn_blocking(move || {
                let _permit = permit;
                let mut connection = connection.lock().map_err(error)?;
                operation(&mut connection)
            })
            .await
            .map_err(error)?
    }
}

fn read(connection: &Connection, key: &str) -> Result<Option<ClaimRecord>, GuardError> {
    let row: Option<(String, Option<String>)> = connection
        .query_row(
            "SELECT claim, result FROM rig_invocation_guard_v1 WHERE key = ?",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(error)?;
    row.map(|(claim, result)| {
        Ok(ClaimRecord {
            claim: serde_json::from_str(&claim).map_err(error)?,
            state: match result {
                Some(result) => ClaimState::Settled {
                    result: serde_json::from_str(&result).map_err(error)?,
                },
                None => ClaimState::Claimed,
            },
        })
    })
    .transpose()
}

impl InvocationGuardStore for SqliteGuardStore {
    fn claim(&self, claim: ClaimRequest) -> BoxFuture<'_, Result<ClaimOutcome, GuardError>> {
        Box::pin(async move {
            let permit = self.reserve()?;
            let encoded = crate::config::encode_execution(&claim).map_err(error)?;
            let key = claim.key.canonical();
            self.run(permit, move |connection| {
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(error)?;
                let created = transaction.execute(
                    "INSERT INTO rig_invocation_guard_v1 (key, token, claim) VALUES (?, ?, ?) ON CONFLICT(key) DO NOTHING",
                    params![key, claim.claim_token, encoded],
                ).map_err(error)? == 1;
                let outcome = if created {
                    ClaimOutcome::Created
                } else {
                    ClaimOutcome::Existing(Box::new(read(&transaction, &key)?
                        .ok_or_else(|| error("claim conflict without a committed record"))?))
                };
                transaction.commit().map_err(error)?;
                Ok(outcome)
            }).await
        })
    }

    fn settle(
        &self,
        key: &LogicalCallKey,
        claim_token: &str,
        result: DurableToolResult,
    ) -> BoxFuture<'_, Result<SettleOutcome, GuardError>> {
        let identity = self.reserve().and_then(|permit| {
            crate::config::encode_execution(&(key, claim_token)).map_err(error)?;
            Ok((permit, key.canonical(), claim_token.to_owned()))
        });
        Box::pin(async move {
            let (permit, key, token) = identity?;
            let encoded = crate::config::encode_execution(&result).map_err(error)?;
            self.run(permit, move |connection| {
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(error)?;
                let updated = transaction.execute(
                    "UPDATE rig_invocation_guard_v1 SET result = ? WHERE key = ? AND token = ? AND result IS NULL",
                    params![encoded, key, token],
                ).map_err(error)?;
                transaction.commit().map_err(error)?;
                Ok(if updated == 1 { SettleOutcome::Settled } else { SettleOutcome::Rejected })
            }).await
        })
    }

    fn get(&self, key: &LogicalCallKey) -> BoxFuture<'_, Result<Option<ClaimRecord>, GuardError>> {
        let identity = self.reserve().and_then(|permit| {
            crate::config::encode_execution(key).map_err(error)?;
            Ok((permit, key.canonical()))
        });
        Box::pin(async move {
            let (permit, key) = identity?;
            self.run(permit, move |connection| read(connection, &key))
                .await
        })
    }
}
