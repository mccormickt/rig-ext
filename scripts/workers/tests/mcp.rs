#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/mcp_fixture.rs"]
mod mcp_fixture;

use mcp_fixture::Fixture;
use rig_worker_tests::{Service, command, free_port, http, root, run};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

fn save(output: &Option<PathBuf>, name: &str, value: &Value) {
    if let Some(output) = output {
        fs::create_dir_all(output).unwrap();
        fs::write(
            output.join(name),
            format!("{}\n", serde_json::to_string_pretty(value).unwrap()),
        )
        .unwrap();
    }
}

fn get(url: &str) -> Value {
    let response = http().get(url).send().unwrap();
    let status = response.status();
    let text = response.text().unwrap();
    assert!(status.is_success(), "{url}: {status}: {text}");
    serde_json::from_str(&text).unwrap()
}

fn faults(fixture: &Fixture, base: &str, runtime: &str, output: &Option<PathBuf>) {
    fixture.reset();
    let result = get(&format!("{base}/check"));
    assert_eq!(result["passed"], true, "{result}");
    let checks = result["checks"].as_object().unwrap();
    assert_eq!(checks.len(), 22);
    assert!(checks.values().all(|v| v == true), "{result}");
    let deadline = Instant::now() + Duration::from_secs(2);
    let stats = loop {
        let stats = fixture.stats();
        let closed = stats["closed"].as_array().unwrap();
        if stats["started"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| closed.contains(p))
        {
            break stats;
        }
        assert!(
            Instant::now() < deadline,
            "client did not cancel all stalled reads: {stats}"
        );
        thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(stats["effects"], 20);
    let requests = stats["requests"].as_object().unwrap();
    assert_eq!(requests.len(), 20);
    for (path, count) in requests {
        assert_eq!(count, if path == "ok" { 3 } else { 1 }, "{path}");
    }
    assert_eq!(stats["started"].as_array().unwrap().len(), 6);
    save(output, &format!("faults-{runtime}.json"), &result);
    save(output, &format!("faults-{runtime}-server.json"), &stats);
    println!("PASS {runtime}: 22 fault checks and independent effect/cancellation counters");
}

fn example(
    fixture: &Fixture,
    native: &Path,
    base: Option<&str>,
    bad: bool,
    label: &str,
    output: &Option<PathBuf>,
) -> f64 {
    fixture.reset();
    let start = Instant::now();
    let path = if bad { "bad-schema" } else { "ok" };
    let result = if let Some(base) = base {
        get(&format!(
            "{base}/demo{}",
            if bad { "/bad-schema" } else { "" }
        ))
    } else {
        serde_json::from_str(&run(
            command(native).arg(format!("http://127.0.0.1:9899/{path}"))
        ))
        .unwrap()
    };
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    let stats = fixture.stats();
    save(
        output,
        &format!("{label}.json"),
        &json!({"elapsed_ms":elapsed, "result":result, "server":stats}),
    );
    assert_eq!(result["tools"], json!(["lookup", "other"]));
    assert_eq!(result["invalid_input_blocked"], true);
    assert_eq!(
        result["script"],
        fs::read_to_string(root().join("rig-celld/examples/mcp/stock.js")).unwrap()
    );
    let report = &result["report"];
    let records = stats["records"].as_array().unwrap();
    let calls: Vec<_> = records
        .iter()
        .filter(|r| r["method"] == "tools/call")
        .collect();
    let expected_calls = if bad { 1 } else { 2 };
    assert_eq!(stats["effects"], expected_calls);
    assert_eq!(stats["requests"], json!({path: 2 + expected_calls}));
    assert_eq!(
        records
            .iter()
            .map(|r| r["method"].as_str().unwrap())
            .collect::<Vec<_>>(),
        if bad {
            vec!["tools/list", "tools/list", "tools/call"]
        } else {
            vec!["tools/list", "tools/list", "tools/call", "tools/call"]
        }
    );
    assert_eq!(
        records
            .iter()
            .map(|r| r["id"].to_string())
            .collect::<BTreeSet<_>>()
            .len(),
        records.len()
    );
    assert!(records.iter().all(|r| r["protocol"] == "2026-07-28"));
    assert_eq!(
        calls
            .iter()
            .map(|r| r["arguments"].clone())
            .collect::<Vec<_>>(),
        if bad {
            vec![json!({"region":"west"})]
        } else {
            vec![json!({"region":"west"}), json!({"region":"east"})]
        }
    );
    assert!(
        calls
            .iter()
            .all(|r| r["regionHeader"] == r["arguments"]["region"])
    );
    assert!(
        calls
            .iter()
            .all(|r| r["result"]["_meta"]["secret"] == "private-marker")
    );
    assert!(!report.to_string().contains("private-marker"));
    assert!(
        !result["transcript"]
            .as_str()
            .unwrap()
            .contains("\"answer\"")
    );
    assert_eq!(report["calls"].as_array().unwrap().len(), expected_calls);
    if bad {
        assert_eq!(report["status"], "script_error");
        assert_eq!(report["calls"][0]["status"], "failed");
        assert_eq!(report["output"]["text"], "");
    } else {
        assert_eq!(report["status"], "completed");
        assert_eq!(report["output"]["text"], "Available units: 49\n");
        assert_eq!(report["returned"], json!({"total":49}));
        assert_eq!(
            calls
                .iter()
                .map(|r| r["result"]["structuredContent"]["answer"].clone())
                .collect::<Vec<_>>(),
            vec![json!(37), json!(12)]
        );
        assert!(
            report["calls"]
                .as_array()
                .unwrap()
                .iter()
                .all(|c| c["status"] == "succeeded")
        );
    }
    println!(
        "PASS {label}: {}; {expected_calls} HTTP tool calls; {elapsed:.2} ms",
        report["status"]
    );
    elapsed
}

#[test]
#[ignore = "requires Node, worker-build, celld, and scripts/workers npm dependencies"]
fn native_and_worker_mcp_conformance() {
    let output = std::env::var_os("MCP_EVIDENCE_DIR").map(PathBuf::from);
    run(command("cargo").args([
        "build",
        "--locked",
        "-p",
        "rig-celld",
        "--example",
        "http_tools",
        "--features",
        "mcp-http",
    ]));
    let metadata: Value = serde_json::from_str(&run(command("cargo").args([
        "metadata",
        "--locked",
        "--no-deps",
        "--format-version=1",
    ])))
    .unwrap();
    let native = PathBuf::from(metadata["target_directory"].as_str().unwrap())
        .join("debug/examples/http_tools");
    let worker = root().join("rig-celld/examples/mcp/worker");
    run(command("worker-build")
        .current_dir(&worker)
        .args(["--release", "--", "--locked"]));
    for (source, dest) in [
        ("NOTICES.txt", "NOTICES.txt"),
        ("fingerprint.json", "worker-replay-fingerprint.json"),
    ] {
        fs::copy(
            root().join("rig-codemode/runtime").join(source),
            worker.join("build").join(dest),
        )
        .unwrap();
    }
    let fixture = Fixture::start();
    let mut summary = json!({});
    let mut samples = Vec::new();
    for i in 1..=3 {
        samples.push(example(
            &fixture,
            &native,
            None,
            false,
            &format!("native-{i}"),
            &output,
        ));
    }
    example(&fixture, &native, None, true, "native-bad-schema", &output);
    samples.sort_by(f64::total_cmp);
    summary["native"] = json!({"passed":4, "median_ms": samples[1]});
    for runtime in ["workerd", "celld"] {
        let port = free_port();
        let base = format!("http://127.0.0.1:{port}");
        let mut cmd = if runtime == "workerd" {
            let mut cmd = command("node");
            cmd.arg("scripts/workers/miniflare.mjs")
                .arg(&worker)
                .arg(port.to_string());
            cmd
        } else {
            let mut cmd = command("celld");
            cmd.arg("dev")
                .arg(&worker)
                .args(["--no-watch", "--port", &port.to_string()]);
            cmd
        };
        cmd.env("PORT", port.to_string());
        let mut service = Service::start(cmd);
        service.wait_http(&format!("{base}/health"));
        faults(&fixture, &base, runtime, &output);
        let mut samples = Vec::new();
        for i in 1..=3 {
            samples.push(example(
                &fixture,
                &native,
                Some(&base),
                false,
                &format!("{runtime}-{i}"),
                &output,
            ));
        }
        example(
            &fixture,
            &native,
            Some(&base),
            true,
            &format!("{runtime}-bad-schema"),
            &output,
        );
        service.restart();
        service.wait_http(&format!("{base}/health"));
        example(
            &fixture,
            &native,
            Some(&base),
            false,
            &format!("{runtime}-restart"),
            &output,
        );
        samples.sort_by(f64::total_cmp);
        summary[runtime] = json!({"passed":5, "median_ms":samples[1]});
    }
    save(&output, "summary.json", &summary);
    if output.is_some() {
        let mut hashes = json!({});
        for path in [
            "scripts/workers/tests/mcp.rs",
            "scripts/workers/tests/support/mcp_fixture.rs",
            "scripts/workers/src/lib.rs",
            "rig-celld/examples/mcp/stock.js",
            "rig-celld/examples/mcp/http_tools.rs",
            "rig-celld/examples/mcp/worker/src/demo.rs",
            "rig-celld/examples/mcp/worker/wrangler.jsonc",
            "rig-celld/src/mcp/client.rs",
            "rig-celld/src/mcp/tool.rs",
            "rig-celld/src/mcp/http/native.rs",
            "rig-celld/src/mcp/http/workers.rs",
            "Cargo.lock",
            "rig-celld/examples/mcp/worker/build/index_bg.wasm",
            "rig-celld/examples/mcp/worker/build/index.js",
            "rig-celld/examples/mcp/worker/build/worker/shim.mjs",
        ] {
            hashes[path] = json!(format!(
                "{:x}",
                Sha256::digest(fs::read(root().join(path)).unwrap())
            ));
        }
        hashes["native-example"] =
            json!(format!("{:x}", Sha256::digest(fs::read(native).unwrap())));
        save(&output, "sha256.json", &hashes);
    }
    println!("{}", serde_json::to_string_pretty(&summary).unwrap());
}
