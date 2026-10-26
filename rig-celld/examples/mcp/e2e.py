"""Check built native and Worker examples against independent HTTP records.

Start the loopback services and build both examples with scripts/workers/mcp.sh.
Use --output DIR to retain raw evidence. No model or production service is used.
"""

import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import time
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = "http://127.0.0.1:9899"
RUNTIMES = {"native": None, "workerd": 9897, "celld": 9896}


def get(url):
    with urlopen(url, timeout=45) as response:
        return response.read()


def check(runtime, port, bad_schema, output, label):
    get(f"{FIXTURE}/reset")
    path = "bad-schema" if bad_schema else "ok"
    start = time.monotonic()
    if port is None:
        raw = subprocess.check_output(
            [str(ROOT.parent / "target/debug/examples/http_tools"), f"{FIXTURE}/{path}"],
            text=True, timeout=45,
        )
    else:
        raw = get(f"http://127.0.0.1:{port}/demo" + ("/bad-schema" if bad_schema else ""))
    elapsed = round((time.monotonic() - start) * 1000, 2)
    result = json.loads(raw)
    stats = json.loads(get(f"{FIXTURE}/stats"))
    evidence = {"runtime": runtime, "elapsed_ms": elapsed, "result": result, "server": stats}
    if output:
        (output / f"{label}.json").write_text(json.dumps(evidence, indent=2) + "\n")
    assert result["tools"] == ["lookup", "other"], result
    assert result["invalid_input_blocked"] is True, result
    assert result["script"] == (ROOT / "examples/mcp/stock.js").read_text(), result
    report = result["report"]
    records = stats["records"]
    calls = [record for record in records if record["method"] == "tools/call"]
    expected_calls = 1 if bad_schema else 2
    assert stats["effects"] == expected_calls, stats
    assert stats["requests"] == {path: 2 + expected_calls}, stats
    assert [record["method"] for record in records] == ["tools/list", "tools/list"] + ["tools/call"] * expected_calls, records
    assert len({record["id"] for record in records}) == len(records), records
    assert all(record["protocol"] == "2026-07-28" for record in records), records
    assert [call["arguments"] for call in calls] == [{"region": "west"}] + ([] if bad_schema else [{"region": "east"}]), calls
    assert all(call["regionHeader"] == call["arguments"]["region"] for call in calls), calls
    assert all(call["result"]["_meta"]["secret"] == "private-marker" for call in calls), calls
    assert "private-marker" not in json.dumps(report), report
    assert '"answer"' not in result["transcript"], result
    assert len(report["calls"]) == expected_calls, report
    if bad_schema:
        assert report["status"] == "script_error", report
        assert report["calls"][0]["status"] == "failed", report
        assert report["output"]["text"] == "", report
    else:
        assert report["status"] == "completed", report
        assert report["output"]["text"] == "Available units: 49\n", report
        assert report["returned"] == {"total": 49}, report
        assert [call["result"]["structuredContent"]["answer"] for call in calls] == [37, 12], calls
        assert all(call["status"] == "succeeded" for call in report["calls"]), report
    print(f"PASS {label}: {report['status']}; {expected_calls} HTTP tool calls; {elapsed} ms", flush=True)
    return elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--native-only", action="store_true")
    parser.add_argument("--skip-restart", action="store_true")
    args = parser.parse_args()
    if args.output:
        args.output.mkdir(parents=True, exist_ok=True)
    summary = {}
    runtimes = {"native": None} if args.native_only else RUNTIMES
    for runtime, port in runtimes.items():
        samples = [check(runtime, port, False, args.output, f"{runtime}-{index}") for index in range(1, 4)]
        check(runtime, port, True, args.output, f"{runtime}-bad-schema")
        summary[runtime] = {"passed": 4, "median_ms": statistics.median(samples)}
    if not args.skip_restart and not args.native_only:
        for runtime in ["workerd", "celld"]:
            subprocess.run(["amp", "orb", "service", "restart", f"mcp-{runtime}"], check=True)
            for _ in range(50):
                try:
                    get(f"http://127.0.0.1:{RUNTIMES[runtime]}/health")
                    break
                except OSError:
                    time.sleep(0.1)
            check(runtime, RUNTIMES[runtime], False, args.output, f"{runtime}-restart")
            summary[runtime]["passed"] += 1
    if args.output:
        files = [
            ROOT / "examples/mcp/stock.js", ROOT / "examples/mcp/http_tools.rs",
            ROOT / "examples/mcp/worker/fixture.mjs", ROOT / "examples/mcp/e2e.py",
            ROOT / "src/mcp/client.rs", ROOT / "src/mcp/tool.rs",
            ROOT / "src/mcp/http/native.rs", ROOT.parent / "Cargo.lock",
            ROOT.parent / "target/debug/examples/http_tools",
        ]
        if not args.native_only:
            files += [
                ROOT / "examples/mcp/worker/src/demo.rs", ROOT / "examples/mcp/worker/wrangler.jsonc",
                ROOT / "src/mcp/http/workers.rs",
                ROOT / "examples/mcp/worker/build/index_bg.wasm",
                ROOT / "examples/mcp/worker/build/index.js",
                ROOT / "examples/mcp/worker/build/worker/shim.mjs",
            ]
        manifest = {str(path.relative_to(ROOT.parent)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
        (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        (args.output / "sha256.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
