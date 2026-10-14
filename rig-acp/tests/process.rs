#![cfg(feature = "process")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use rig_acp::{
    Client, Event,
    protocol::{AcpAgent, AcpAgentConfig},
};
use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn config() -> AcpAgentConfig {
    AcpAgentConfig::new("node").arg(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/fixtures/agent.mjs"
    ))
}

fn receipts() -> PathBuf {
    std::env::temp_dir().join(format!(
        "rig-acp-{}-{}.jsonl",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[tokio::test]
async fn bad_version_malformed_and_ambiguous_delivery_are_not_success_or_retried() {
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            Client::connect(AcpAgent::new(config().env("ACP_BAD_VERSION", "1")))
        )
        .await
        .unwrap()
        .is_err()
    );
    for prompt in ["malformed", "disconnect-after-effect"] {
        let path = receipts();
        for attempt in 1..=2 {
            let client = Client::connect(AcpAgent::new(
                config().env("ACP_FIXTURE_RECEIPTS", path.to_string_lossy()),
            ))
            .await
            .unwrap();
            let session = client.new_session("/tmp").await.unwrap();
            if attempt == 1 {
                let mut turn = session.prompt_text(prompt).unwrap();
                let mut failed = false;
                while let Some(event) = tokio::time::timeout(Duration::from_secs(5), turn.next())
                    .await
                    .unwrap()
                {
                    assert!(!matches!(event, Ok(Event::Completed(_))));
                    failed |= event.is_err();
                }
                assert!(failed);
            } else {
                assert!(
                    tokio::time::timeout(
                        Duration::from_secs(5),
                        session
                            .model()
                            .agent("bound")
                            .build()
                            .prompt(prompt)
                            .into_future()
                    )
                    .await
                    .unwrap()
                    .is_err()
                );
            }
            let log = std::fs::read_to_string(&path).unwrap();
            let rows: Vec<serde_json::Value> = log
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(
                rows.iter().filter(|r| r["event"] == "prompt").count(),
                attempt
            );
            if prompt == "disconnect-after-effect" {
                assert_eq!(
                    rows.iter()
                        .filter(|r| r["event"] == "ambiguous-effect")
                        .count(),
                    attempt
                );
            }
        }
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn last_owner_drop_and_abandoned_connect_reap_the_owned_child() {
    for abandon in [false, true] {
        let path = receipts();
        let mut config = config().env("ACP_FIXTURE_RECEIPTS", path.to_string_lossy());
        if abandon {
            config = config.env("ACP_HOLD_INITIALIZE", "1");
        }
        let connecting = tokio::spawn(Client::connect(AcpAgent::new(config)));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !std::fs::read_to_string(&path)
                .unwrap_or_default()
                .contains("initialize")
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let first = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .to_string();
        let pid = serde_json::from_str::<serde_json::Value>(&first).unwrap()["pid"]
            .as_u64()
            .unwrap();
        assert!(std::path::Path::new(&format!("/proc/{pid}")).exists());
        if abandon {
            connecting.abort();
        } else {
            drop(connecting.await.unwrap().unwrap());
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while std::path::Path::new(&format!("/proc/{pid}")).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("owned process must exit");
        std::fs::remove_file(path).unwrap();
    }
}

use std::future::IntoFuture;
