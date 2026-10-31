#![cfg(all(feature = "guard-sqlite", not(target_family = "wasm")))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{
    io::{BufRead, BufReader, Read, Write},
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

use rig::tool::{ToolOutput, ToolResult, ToolSet};
use rig_durable::{
    AttemptMetadata, DurableToolResult, InvocationGuardStore, LogicalCallKey, ToolActivityInput,
    ToolExecutor, ToolInvocation, ToolPolicy,
    guard::{ClaimOutcome, ClaimRequest, ClaimState, SettleOutcome, SqliteGuardStore, conformance},
};

fn request(token: &str) -> ClaimRequest {
    ClaimRequest {
        key: LogicalCallKey {
            logical_execution_id: "application/store/run".into(),
            submission_id: "prompt-0".into(),
            model_turn: 1,
            call_index: 2,
        },
        tool_name: "payment".into(),
        arguments_digest: rig_durable::identity::arguments_digest(&serde_json::json!({})).unwrap(),
        policy: ToolPolicy::interrupt_on_uncertain(),
        attempt: AttemptMetadata {
            backend_execution_id: token.into(),
            activity_attempt: Some(1),
        },
        claim_token: token.into(),
    }
}

fn result(text: &str) -> DurableToolResult {
    DurableToolResult::from_rig(
        ToolResult::success(ToolOutput::text(text)),
        &Default::default(),
        &Default::default(),
    )
    .0
}

#[tokio::test]
async fn independent_connections_reopen_and_conditional_settlement() {
    let dir = std::env::temp_dir().join(format!("rig-guard-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("guard.db");
    let first = SqliteGuardStore::open(&path).unwrap();
    let second = SqliteGuardStore::open(&path).unwrap();
    conformance::run(Arc::new(SqliteGuardStore::open(&path).unwrap()))
        .await
        .unwrap();
    let claim = request("owner");
    assert!(matches!(
        first.claim(claim.clone()).await.unwrap(),
        ClaimOutcome::Created
    ));
    assert!(matches!(
        second.claim(request("other")).await.unwrap(),
        ClaimOutcome::Existing(_)
    ));
    assert_eq!(
        second
            .settle(&claim.key, "other", result("wrong"))
            .await
            .unwrap(),
        SettleOutcome::Rejected
    );
    assert_eq!(
        first
            .settle(&claim.key, "owner", result("receipt-42"))
            .await
            .unwrap(),
        SettleOutcome::Settled
    );
    drop(first);
    drop(second);
    let reopened = Arc::new(SqliteGuardStore::open(&path).unwrap());
    assert_eq!(
        reopened
            .settle(&claim.key, "owner", result("late"))
            .await
            .unwrap(),
        SettleOutcome::Rejected
    );
    let record = reopened.get(&claim.key).await.unwrap().unwrap();
    assert_eq!(record.claim, claim);
    let ClaimState::Settled { result } = record.state else {
        panic!("settlement missing")
    };
    assert_eq!(
        result.model_content("payment"),
        vec![rig::message::ToolResultContent::text("receipt-42")]
    );
    let recovered = ToolExecutor::new(Arc::new(ToolSet::default()))
        .with_guard(Some(reopened.clone()))
        .execute(ToolActivityInput {
            name: "payment".into(),
            arguments: "{}".into(),
            invocation: ToolInvocation {
                execution_id: "restarted-worker".into(),
                prompt_index: 0,
                turn: claim.key.model_turn,
                call_index: claim.key.call_index,
                logical_key: claim.key,
                attempt: AttemptMetadata {
                    backend_execution_id: "restarted-worker".into(),
                    activity_attempt: Some(2),
                },
            },
            policy: claim.policy,
            runtime_identity: Some("unavailable-runtime".into()),
        })
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(recovered.result).unwrap(),
        serde_json::to_value(result).unwrap()
    );
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}

// The parent starts this same test executable in independent OS processes.
#[tokio::test]
async fn process_claim() {
    let Ok(path) = std::env::var("RIG_GUARD_PROCESS_DB") else {
        return;
    };
    let store = SqliteGuardStore::open(path).unwrap();
    println!("READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
    let outcome = store
        .claim(request(&std::process::id().to_string()))
        .await
        .unwrap();
    println!("OWNER={}", matches!(outcome, ClaimOutcome::Created));
}

#[test]
fn independent_processes_have_one_owner() {
    let dir = std::env::temp_dir().join(format!("rig-guard-process-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("guard.db");
    drop(SqliteGuardStore::open(&path).unwrap());
    let mut children = Vec::new();
    for _ in 0..8 {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "process_claim", "--nocapture"])
            .env("RIG_GUARD_PROCESS_DB", &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(
                stdout.read_line(&mut line).unwrap(),
                0,
                "child exited before opening the database"
            );
            if line.contains("READY") {
                break;
            }
        }
        children.push((child, stdout));
    }
    for (child, _) in &mut children {
        child.stdin.take().unwrap().write_all(b"claim\n").unwrap();
    }
    let mut owners = 0;
    for (mut child, mut stdout) in children {
        let mut output = String::new();
        stdout.read_to_string(&mut output).unwrap();
        assert!(child.wait().unwrap().success(), "{output}");
        owners += usize::from(output.contains("OWNER=true"));
    }
    assert_eq!(owners, 1);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn cancelled_waits_keep_capacity_and_can_commit_after_release() {
    let dir = std::env::temp_dir().join(format!("rig-guard-blocked-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("guard.db");
    let store = SqliteGuardStore::open(&path).unwrap();
    let blocker = rusqlite::Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    for index in 0..8 {
        let mut claim = request(&format!("owner-{index}"));
        claim.key.call_index = index;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), store.claim(claim))
                .await
                .is_err()
        );
    }
    let error = store.claim(request("overflow")).await.unwrap_err();
    assert!(error.to_string().contains("capacity exhausted"));
    blocker.execute_batch("COMMIT").unwrap();
    let observer = SqliteGuardStore::open(&path).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut complete = true;
            for index in 0..8 {
                let mut key = request("observer").key;
                key.call_index = index;
                complete &= observer.get(&key).await.unwrap().is_some();
            }
            if complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        observer.claim(request("replacement")).await.unwrap(),
        ClaimOutcome::Existing(_)
    ));
    let mut oversized = request("oversized");
    oversized.key.logical_execution_id = "x".repeat(1024 * 1024);
    assert!(
        observer
            .claim(oversized)
            .await
            .unwrap_err()
            .to_string()
            .contains("1 MiB")
    );
    drop((store, observer, blocker));
    std::fs::remove_dir_all(dir).unwrap();
}
