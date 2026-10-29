use std::{
    path::Path,
    process::{Command, Output},
};

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension};
use tempfile::TempDir;

fn invoke(db: &Path, args: &[&str]) -> Result<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_rig-native-prototype"))
        .arg(db)
        .args(args)
        .output()?)
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn count(db: &Path, name: &str) -> Result<u64> {
    Ok(Connection::open(db)?
        .query_row("SELECT count FROM counters WHERE name=?1", [name], |row| {
            row.get(0)
        })
        .optional()?
        .unwrap_or(0))
}

fn records(db: &Path) -> Result<u64> {
    Ok(Connection::open(db)?.query_row("SELECT count(*) FROM effects", [], |row| row.get(0))?)
}

fn paused(db: &Path) -> Result<()> {
    let output = invoke(db, &["run"])?;
    ensure!(
        !output.status.success() && text(&output).contains("approval required"),
        "{}",
        text(&output)
    );
    ensure!(count(db, "model")? == 1 && count(db, "charge")? == 0 && records(db)? == 1);
    Ok(())
}

fn decision(db: &Path, decision: &str) -> Result<()> {
    let output = invoke(db, &[decision])?;
    ensure!(output.status.success(), "{}", text(&output));
    Ok(())
}

#[test]
fn approval_and_completed_run_replay_across_processes() -> Result<()> {
    let dir = TempDir::new()?;
    let db = dir.path().join("run.db");
    paused(&db)?;
    paused(&db)?;
    decision(&db, "approve")?;
    let first = invoke(&db, &["run"])?;
    ensure!(
        first.status.success() && text(&first).contains("receipt:invoice-7:37"),
        "{}",
        text(&first)
    );
    ensure!(count(&db, "model")? == 2 && count(&db, "charge")? == 1 && records(&db)? == 3);
    let replay = invoke(&db, &["run"])?;
    ensure!(
        replay.status.success() && replay.stdout == first.stdout,
        "{}",
        text(&replay)
    );
    ensure!(count(&db, "model")? == 2 && count(&db, "charge")? == 1 && records(&db)? == 3);
    Ok(())
}

#[test]
fn denial_is_feedback_without_a_charge() -> Result<()> {
    let dir = TempDir::new()?;
    let db = dir.path().join("run.db");
    paused(&db)?;
    decision(&db, "deny")?;
    for _ in 0..2 {
        let output = invoke(&db, &["run"])?;
        ensure!(
            output.status.success() && text(&output).contains("denied by reviewer"),
            "{}",
            text(&output)
        );
    }
    ensure!(count(&db, "model")? == 2 && count(&db, "charge")? == 0 && records(&db)? == 3);
    Ok(())
}

#[test]
fn crashes_preserve_uncertainty_or_recover_a_settled_result() -> Result<()> {
    for (fault, charges, recovered) in [
        ("after-claim", 0, false),
        ("after-effect", 1, false),
        ("after-settle", 1, true),
        ("after-record", 1, true),
    ] {
        let dir = TempDir::new()?;
        let db = dir.path().join("run.db");
        paused(&db)?;
        decision(&db, "approve")?;
        let crashed = invoke(&db, &["run", fault])?;
        ensure!(
            crashed.status.code() == Some(75),
            "{fault}: {}",
            text(&crashed)
        );
        ensure!(count(&db, "charge")? == charges, "{fault}");
        ensure!(
            records(&db)? == if fault == "after-record" { 2 } else { 1 },
            "{fault}"
        );
        for _ in 0..2 {
            let output = invoke(&db, &["run"])?;
            if recovered {
                ensure!(
                    output.status.success() && text(&output).contains("receipt:invoice-7:37"),
                    "{fault}: {}",
                    text(&output)
                );
            } else {
                ensure!(
                    !output.status.success() && text(&output).contains("uncertain tool effect"),
                    "{fault}: {}",
                    text(&output)
                );
            }
            ensure!(count(&db, "charge")? == charges, "{fault}: repeated charge");
            ensure!(
                count(&db, "model")? == if recovered { 2 } else { 1 },
                "{fault}: repeated model call"
            );
        }
    }
    Ok(())
}

#[test]
fn changed_configuration_is_refused_before_live_work() -> Result<()> {
    let dir = TempDir::new()?;
    let db = dir.path().join("run.db");
    paused(&db)?;
    decision(&db, "approve")?;
    let output = invoke(&db, &["run", "none", "A different preamble"])?;
    ensure!(
        !output.status.success() && text(&output).contains("run spec"),
        "{}",
        text(&output)
    );
    ensure!(count(&db, "model")? == 1 && count(&db, "charge")? == 0);
    let output = invoke(&db, &["run"])?;
    ensure!(output.status.success(), "{}", text(&output));
    Ok(())
}

#[test]
fn request_divergence_never_falls_back_to_live_execution() -> Result<()> {
    let dir = TempDir::new()?;
    let db = dir.path().join("run.db");
    paused(&db)?;
    decision(&db, "approve")?;
    let connection = Connection::open(&db)?;
    let encoded: String =
        connection.query_row("SELECT record FROM effects", [], |row| row.get(0))?;
    let mut record: serde_json::Value = serde_json::from_str(&encoded)?;
    let history = record["kind"]["request"]["chat_history"]
        .as_array_mut()
        .context("recorded completion history")?;
    history[0]["content"] = "Tampered system prompt".into();
    connection.execute(
        "UPDATE effects SET record=?1",
        [serde_json::to_string(&record)?],
    )?;
    let output = invoke(&db, &["run"])?;
    ensure!(
        !output.status.success() && text(&output).contains("replay divergence"),
        "{}",
        text(&output)
    );
    ensure!(count(&db, "model")? == 1 && count(&db, "charge")? == 0 && records(&db)? == 1);
    Ok(())
}

#[test]
fn recorder_write_failure_stops_new_effects_and_can_recover() -> Result<()> {
    for (trigger, charges) in [
        ("BEFORE INSERT ON meta WHEN NEW.key='header'", 0),
        ("BEFORE INSERT ON effects WHEN NEW.id>1", 1),
    ] {
        let dir = TempDir::new()?;
        let db = dir.path().join("run.db");
        paused(&db)?;
        decision(&db, "approve")?;
        Connection::open(&db)?.execute_batch(&format!(
            "CREATE TRIGGER fail_recording {trigger} BEGIN SELECT RAISE(ABORT, 'recording blocked'); END;",
        ))?;
        let output = invoke(&db, &["run"])?;
        ensure!(
            !output.status.success() && text(&output).contains("recording blocked"),
            "{}",
            text(&output)
        );
        ensure!(
            count(&db, "model")? == 1 && count(&db, "charge")? == charges && records(&db)? == 1
        );
        Connection::open(&db)?.execute_batch("DROP TRIGGER fail_recording")?;
        let output = invoke(&db, &["run"])?;
        ensure!(
            output.status.success() && text(&output).contains("receipt:invoice-7:37"),
            "{}",
            text(&output)
        );
        ensure!(count(&db, "model")? == 2 && count(&db, "charge")? == 1 && records(&db)? == 3);
    }
    Ok(())
}
