use std::collections::BTreeMap;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::Error;

/// Synchronous SQL with positional parameters. A transaction must roll back
/// all writes when its closure returns an error. Rows use column names.
pub trait SqlDatabase {
    fn exec(&self, query: &str, params: &[String]) -> Result<Vec<BTreeMap<String, Value>>, Error>;
    fn transaction<T>(&self, f: impl FnOnce() -> Result<T, Error>) -> Result<T, Error>;
}

impl SqlDatabase for rig_celld::CellStorage {
    fn exec(&self, query: &str, params: &[String]) -> Result<Vec<BTreeMap<String, Value>>, Error> {
        self.sql()
            .exec(
                query,
                params
                    .iter()
                    .cloned()
                    .map(worker::SqlStorageValue::String)
                    .collect::<Vec<_>>(),
            )?
            .to_array()
            .map_err(Error::from)
    }

    fn transaction<T>(&self, f: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
        self.transaction_sync(f)
    }
}

pub(super) const CHUNK_BYTES: usize = 512 * 1024;
pub(super) const ROW_BYTES: usize = 1024 * 1024;

pub(super) fn initialize(db: &impl SqlDatabase) -> Result<(), Error> {
    db.transaction(|| {
        db.exec("CREATE TABLE IF NOT EXISTS rig_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)", &[])?;
        let version = db.exec("SELECT value FROM rig_meta WHERE key = 'schema'", &[])?;
        if let Some(row) = version.first() {
            if !matches!(row.get("value").and_then(Value::as_str), Some("2" | "3")) {
                return Err(Error::Invalid("unsupported Durable Object schema version".into()));
            }
        } else {
            db.exec("INSERT INTO rig_meta VALUES ('schema', '3')", &[])?;
        }
        for statement in [
            "CREATE TABLE IF NOT EXISTS rig_entries (id INTEGER PRIMARY KEY, data TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS rig_runs (key TEXT NOT NULL, chunk INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY (key, chunk))",
            "CREATE TABLE IF NOT EXISTS rig_tool_calls (key TEXT PRIMARY KEY, data TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS rig_submissions (id INTEGER PRIMARY KEY CHECK (id = 1), data TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS rig_script_effects (key TEXT PRIMARY KEY, data TEXT NOT NULL)",
        ] {
            db.exec(statement, &[])?;
        }
        db.exec("UPDATE rig_meta SET value = '3' WHERE key = 'schema'", &[])?;
        Ok(())
    })
}

pub(super) fn encode(value: &impl Serialize, cap: usize) -> Result<String, Error> {
    let data = serde_json::to_string(value)?;
    check_depth(&data, 96)?;
    if data.len() > cap {
        return Err(Error::Limit {
            bytes: data.len(),
            max_bytes: cap,
        });
    }
    Ok(data)
}

/// Count containers without recursively decoding attacker-controlled JSON.
/// Strings (including escaped quotes and JSON stored inside strings) are opaque.
pub(super) fn check_depth(data: &str, max_depth: usize) -> Result<(), Error> {
    let (mut depth, mut string, mut escape) = (0usize, false, false);
    for byte in data.bytes() {
        if string {
            if escape {
                escape = false;
            } else if byte == b'\\' {
                escape = true;
            } else if byte == b'"' {
                string = false;
            }
        } else {
            match byte {
                b'"' => string = true,
                b'[' | b'{' => {
                    depth += 1;
                    if depth > max_depth {
                        return Err(Error::Depth { max_depth });
                    }
                }
                b']' | b'}' => depth -= 1,
                _ => {}
            }
        }
    }
    Ok(())
}

pub(super) fn read<T: DeserializeOwned>(
    db: &impl SqlDatabase,
    key: &str,
) -> Result<Option<T>, Error> {
    let rows = db.exec(
        "SELECT data FROM rig_runs WHERE key = ? ORDER BY chunk",
        &[key.into()],
    )?;
    if rows.is_empty() {
        return Ok(None);
    }
    let mut data = String::new();
    for row in rows {
        data.push_str(text(&row)?);
    }
    Ok(Some(serde_json::from_str(&data)?))
}

/// Caller owns the transaction, including any related transcript writes.
pub(super) fn write(
    db: &impl SqlDatabase,
    key: &str,
    value: &impl Serialize,
    cap: usize,
) -> Result<(), Error> {
    let data = encode(value, cap)?;
    db.exec("DELETE FROM rig_runs WHERE key = ?", &[key.into()])?;
    let mut rest = data.as_str();
    let mut index = 0;
    while !rest.is_empty() {
        let mut end = rest.len().min(CHUNK_BYTES);
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        db.exec(
            "INSERT INTO rig_runs (key, chunk, data) VALUES (?, ?, ?)",
            &[key.into(), index.to_string(), rest[..end].into()],
        )?;
        rest = &rest[end..];
        index += 1;
    }
    Ok(())
}

pub(super) fn text(row: &BTreeMap<String, Value>) -> Result<&str, Error> {
    row.get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Storage("missing SQL data column".into()))
}
