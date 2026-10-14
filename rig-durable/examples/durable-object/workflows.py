"""Real Worker/SQL conformance with forced crash windows, without API keys."""

import argparse
import hashlib
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--runtime", choices=["celld", "workerd"], required=True)
parser.add_argument("--url")
parser.add_argument("--prefix", default=f"workflow-{time.time_ns()}")
parser.add_argument("--evidence")
parser.add_argument("--outer-boundary-only", action="store_true", help="Run the fixed-row batch admission boundary and control")
parser.add_argument("--review-fixes-only", action="store_true", help="Run depth, guest decoder, and ordinary child Call regressions")
parser.add_argument("--review-case", action="append", help="Select individual review regression cases")
args = parser.parse_args()
expected_url = f"http://127.0.0.1:{9876 if args.runtime == 'celld' else 9877}"
base = (args.url or expected_url).rstrip("/")
if base != expected_url:
    parser.error("--url must match the declared local service that this script restarts")
root = Path(__file__).resolve().parents[3]
observations = {"runtime": args.runtime, "prefix": args.prefix, "cases": {}, "checkpoints": {},
                "restarts": [], "sessions": {},
                "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
                "dirty_paths": subprocess.check_output(["git", "diff", "--name-only"], cwd=root, text=True).splitlines(),
                "bundle_sha256": hashlib.sha256((root / "rig-durable/examples/durable-object/build/index_bg.wasm").read_bytes()).hexdigest(),
                "shim_sha256": hashlib.sha256((root / "rig-durable/examples/durable-object/build/worker/shim.mjs").read_bytes()).hexdigest(),
                "notices_sha256": hashlib.sha256((root / "rig-durable/examples/durable-object/build/NOTICES.txt").read_bytes()).hexdigest(),
                "worker_assets": json.loads((root / "rig-durable/examples/durable-object/build/worker-replay-fingerprint.json").read_text()),
                "celld_version": subprocess.check_output([str(Path.home() / ".local/bin/celld"), "--version"], text=True).strip(),
                "workerd_version": json.loads((root / "scripts/workers/node_modules/workerd/package.json").read_text())["version"]}


def api(session, operation, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        f"{base}/fixture/{session}/{operation}", data=data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        text = response.read().decode()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def until(read, predicate, label):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        value = read()
        if predicate(value):
            return value
        time.sleep(0.05)
    raise AssertionError(f"{label}: no progress; last state: {json.dumps(value)[:4000]}")


def save():
    if args.evidence:
        Path(args.evidence).parent.mkdir(parents=True, exist_ok=True)
        with open(args.evidence, "w") as output:
            json.dump(observations, output, indent=2)


def checkpoint(label, session):
    snapshot = {"sql": api(session, "evidence"), "state": api(session, "state"),
                "status": api(session, "status")}
    observations["checkpoints"][label] = snapshot
    save()
    return snapshot


def restart(session):
    before = checkpoint(f"restart-{len(observations['restarts'])}", session)
    subprocess.run(["amp", "orb", "service", "restart", f"durable-{args.runtime}"], check=True, timeout=120)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base}/health/status", timeout=1):
                break
        except (urllib.error.URLError, TimeoutError):
            time.sleep(0.1)
    else:
        raise AssertionError("restarted runtime is not ready")
    after = api(session, "evidence")
    assert after["boot_ms"] != before["sql"]["boot_ms"], (before, after)
    observations["restarts"].append({"session": session, "before": before["sql"]["boot_ms"], "after": after["boot_ms"]})
    save()


def configure(name, source, **controls):
    session = f"{args.prefix}-{name}"
    configuration = {"source": source, **controls}
    api(session, "configure", configuration)
    observations["sessions"][session] = configuration
    save()
    return session, configuration


def submit(session, request_id="one"):
    return api(session, "submit", {
        "request_id": request_id, "message": {"role": "user", "content": [
            {"type": "text", "text": "Run the configured workflow"},
        ]}, "mode": "follow_up",
    })


def finished(session):
    status = until(lambda: api(session, "status"), lambda s: not s["busy"], "parent finish")
    assert status["receipts"], status
    assert status["receipts"][-1]["state"] == "answered", status
    return status


def report(session):
    transcript = api(session, "transcript")
    reports = [part["value"] for message in transcript for block in message.get("content", [])
               if block["type"] == "toolresult" for part in block["content"]
               if part["type"] == "json" and "calls" in part["value"]]
    assert reports, transcript
    return reports[-1]


def canonical(key):
    execution, submission = key["logical_execution_id"], key["submission_id"]
    return (f"v1;{len(execution.encode())}:{execution};{len(submission.encode())}:{submission};"
            f"{key['model_turn']};{key['call_index']}")


def expected_child(key, ordinal):
    data = f"child-v1;{canonical(key)};Some({ordinal})".encode()
    return "rig-child-" + hashlib.sha256(data).hexdigest()


def child(session, ordinal, operation="evidence", prompt=0, body=None):
    return api(session, f"child-{operation}?ordinal={ordinal}&prompt={prompt}", body)


def child_effect(session, ordinal, branch, prompt=0):
    evidence = child(session, ordinal, prompt=prompt)
    assert len(evidence["effects"]) == 1, evidence
    assert evidence["effects"][0]["branch"] == branch, evidence
    status = child(session, ordinal, "status", prompt)
    assert not status["busy"] and len(status["receipts"]) == 1, status
    assert status["receipts"][0]["state"] == "answered", status
    receipt = status["receipts"][0]
    effect_key = {"logical_execution_id": receipt["logical_session_id"],
                  "submission_id": receipt["submission_id"], "model_turn": 1, "call_index": 0}
    assert evidence["effects"][0]["logical_id"] == canonical(effect_key), evidence
    result = child(session, ordinal, "result", prompt)
    assert result["response"]["content"] == [{"type": "text", "text": branch}], result
    assert len(result["tool_outcomes"]) == 1 and result["tool_outcomes"][0]["disposition"] == "success", result
    key = api(session, f"state?prompt={prompt}")["key"]
    identity = expected_child(key, ordinal)
    assert status["receipts"][0]["request_id"] == identity, status
    submissions = [r for r in evidence["requests"] if r["operation"] == "submit"]
    assert submissions and all(r["child_id"] == identity for r in evidence["requests"]), evidence
    assert len({r["body"] for r in submissions}) == 1, evidence
    assert all(json.loads(r["body"])["input"]["request_id"] == identity for r in submissions), evidence
    return evidence


def record(name, session, **data):
    observations["cases"][name] = {
        "parent_sql": api(session, "evidence"),
        "scheduler": api(session, "state"),
        "status": api(session, "status"), **data,
    }
    save()
    print(f"PASS [{args.runtime}] {name}", flush=True)


def outer_boundary():
    slow = "return await tools.child({branch: 'outer-control', gate: true});"
    session, _ = configure("outer-fitting", slow,
                           second_source="/*" + " " * 512_000 + "*/ return 'normal';",
                           source_bytes=1024 * 1024, max_history_bytes=32 * 1024 * 1024)
    submit(session)
    until(lambda: child(session, 0), lambda s: s["model_started"], "outer control child started")
    fitting = until(lambda: api(session, "evidence"),
                    lambda s: len(s["outer_intents"]) == 2 and sum(c["completed"] for c in s["outer_intents"]) == 1,
                    "large fitting script settled while sibling remains live")
    large = max(fitting["outer_intents"], key=lambda c: c["empty_bytes"])
    assert 1_000_000 < large["empty_bytes"] < large["bytes"] <= 1024 * 1024, large
    assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
    child(session, 0, "release", body={})
    finished(session)
    result = api(session, "result?request_id=one")
    assert len(result["tool_outcomes"]) == 2 and all(o["disposition"] == "success" for o in result["tool_outcomes"]), result
    assert report(session)["returned"] == "normal", report(session)
    control = child_effect(session, 0, "outer-control")

    session, _ = configure("outer-too-full", slow,
                           second_source="/*" + " " * 523_700 + "*/ return 'normal';",
                           source_bytes=1024 * 1024, max_history_bytes=32 * 1024 * 1024,
                           pause_second_result=True)
    submit(session)
    def drive_evidence():
        evidence = api(session, "evidence")
        if evidence["model_entries"] and not evidence["paused"]:
            api(session, "drive", {})
        return api(session, "evidence")
    candidate = until(drive_evidence, lambda s: s["drive_failures"] > 0 or s["paused"], "batch admission boundary")
    if candidate["paused"]:
        until(lambda: child(session, 0), lambda s: s["model_started"], "pre-fix sibling admitted before large settlement")
        large = max(candidate["outer_intents"], key=lambda c: c["empty_bytes"])
        assert 0 < 1024 * 1024 - large["empty_bytes"] < 580, large
        checkpoint("outer-near-full-before-settlement", session)
        api(session, "release-parent", {})
    blocked = until(drive_evidence,
                    lambda s: s["drive_failures"] > 0 or not api(session, "status")["busy"],
                    "fixed-row admission blocks")
    status = api(session, "status")
    assert status["busy"] and not status["closed"] and status["receipts"][-1]["state"] == "running", status
    assert "fixed" in blocked["drive_error"].lower() and "storage" in blocked["drive_error"].lower(), blocked
    assert blocked["model_entries"] == 1 and blocked["planner_entries"] == 0, blocked
    assert not blocked["outer_intents"] and not blocked["requests"] and not blocked["effects"], blocked
    assert api(session, "state")["scheduler"] is None
    assert api(session, "result?request_id=one") is None
    assert not child(session, 0)["model_started"] and not child(session, 0)["effects"]
    restart(session)
    after = until(drive_evidence, lambda s: s["drive_failures"] > blocked["drive_failures"], "retained model output blocks after restart")
    assert after["model_entries"] == 1 and after["planner_entries"] == 0, after
    assert not after["outer_intents"] and not after["requests"] and not after["effects"], after
    assert api(session, "status") == status and api(session, "result?request_id=one") is None
    record("outer fixed-row batch admission preserves work without dispatch", session,
           fitting=large, control_child=control, before_restart=blocked)


def failed_run(error_type, error, traceback):
    observations["failure"] = {"type": error_type.__name__, "message": str(error)}
    for session in observations["sessions"]:
        try:
            checkpoint(f"failure-{session}", session)
            state = api(session, "state")["scheduler"]
            for operation in ["release-parent", "release-ack", "release-effect", "release-reply"]:
                api(session, operation, {})
            for call in (state or {}).get("input", {}).get("admitted", []):
                if call["name"] == "child":
                    observations.setdefault("failure_children", {}).setdefault(session, []).append({
                        "ordinal": call["ordinal"], "sql": child(session, call["ordinal"]),
                        "status": child(session, call["ordinal"], "status"),
                        "result": child(session, call["ordinal"], "result"),
                    })
                    child(session, call["ordinal"], "release", body={})
                    child(session, call["ordinal"], "release-effect", body={})
        except Exception as cleanup_error:
            observations.setdefault("cleanup_errors", {})[session] = str(cleanup_error)
    save()
    sys.__excepthook__(error_type, error, traceback)


def review_fixes():
    nest = "function nest(n) { let x = 'leaf'; while(n--) x = [x]; return x; }"
    cases = [
        ("terminal-depth", "return nest(124);", "script_error", 0),
        ("argument-depth", "return await tools.observe({branch:'deep',payload:nest(124)});", "script_error", 0),
        ("local-result-depth", "return await tools.observe({branch:'deep',result_depth:124});", "script_error", 1),
        ("terminal-fitting", "return nest(63);", "completed", 0),
        ("argument-fitting", "await tools.observe({branch:'deep',payload:nest(63)}); return 'fit';", "completed", 1),
        ("result-fitting", "return await tools.observe({branch:'deep',result_depth:40});", "completed", 1),
        ("guest-surrogate", "return '\\ud800';", "script_error", 0),
        ("guest-depth128", "return nest(128);", "script_error", 0),
    ]
    for name, terminal, expected, effects in cases:
        if args.review_case and name not in args.review_case:
            continue
        effects += 1  # The fast race winner establishes a prior admission checkpoint.
        source = nest + " const slow = tools.child({branch:'depth-drain',gate:true}); await Promise.race([slow, tools.observe({branch:'admit'})]); " + terminal
        session, _ = configure(name, source)
        submit(session)
        until(lambda: child(session, 0), lambda s: s["model_started"], "depth sibling admitted")
        before = until(lambda: api(session, "state"), lambda s: s["scheduler"]["draining"], "depth terminal drain")
        assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
        effects_before = api(session, "evidence")["effects"]
        assert len(effects_before) == effects
        if name == "local-result-depth":
            envelope = next(e for e in api(session, "evidence")["effect_envelopes"] if e["call"]["ordinal"] == 2)
            assert envelope["output"]["result"]["disposition"] == "error", envelope
            error = envelope["output"]["result"]["result"]["value"]
            assert error["kind"] == "other" and error["retryable"] is False, envelope
            assert len(json.dumps(envelope).encode()) < 4096, envelope
        restart(session)
        assert api(session, "evidence")["effects"] == effects_before
        assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
        child(session, 0, "release", body={})
        finished(session)
        state = api(session, "state")["scheduler"]
        if name in {"terminal-depth", "argument-depth"}:
            assert "durable storage limit" in state["action"]["Complete"]["Err"], state
            assert api(session, "result?request_id=one")["tool_outcomes"][0]["disposition"] == "error"
            assert not state["active"] and not state["queued"] and not state["ready"], state
            drained = child_effect(session, 0, "depth-drain")
            assert api(session, "evidence")["effects"] == effects_before
            record(name, session, before_restart=before, child=drained)
            continue
        outcome = report(session)
        assert outcome["status"] == expected and state["action"]["Complete"]["Ok"] == outcome, outcome
        assert state["failure"] is None and not state["active"] and not state["queued"] and not state["ready"], state
        assert api(session, "result?request_id=one")["tool_outcomes"][0]["disposition"] == "success"
        if name.startswith("guest-"):
            assert "returned" not in outcome and outcome["diagnostic"]["message"] == "the return value is not valid JSON", outcome
        elif expected == "completed":
            value = outcome["returned"]
            depth = {"terminal-fitting": 63, "result-fitting": 40}.get(name, 0)
            for _ in range(depth):
                assert isinstance(value, list) and len(value) == 1, value
                value = value[0]
            assert value == {"terminal-fitting": "leaf", "argument-fitting": "fit", "result-fitting": "deep"}[name], value
        drained = child_effect(session, 0, "depth-drain")
        assert api(session, "evidence")["effects"] == effects_before
        record(name, session, report=outcome, before_restart=before, child=drained)

    def ordinary(session, index, operation="evidence", body=None):
        return api(session, f"child-{operation}?call_index={index}", body)

    for name, payload_size, result_size in [("ordinary-fitting", 1000, 1000),
                                            ("ordinary-call-overflow", 240_000, 170_000),
                                            ("ordinary-atomic-admission", 525_000, 1000)]:
        if args.review_case and name not in args.review_case:
            continue
        batch = [{"branch":"ordinary-slow", "gate":True},
                 {"branch":"ordinary-large", "payload":"a" * payload_size, "result_bytes":result_size}]
        session, _ = configure(name, "", ordinary_child_batch=batch, max_history_bytes=32 * 1024 * 1024)
        submit(session)
        if name == "ordinary-atomic-admission":
            def blocked():
                api(session, "drive", {})
                return api(session, "evidence")
            before = until(blocked, lambda s: s["drive_failures"] > 0, "ordinary atomic admission blocked")
            assert "fixed" in before["drive_error"] and "storage" in before["drive_error"], before
            assert not before["outer_intents"] and not before["requests"] and not before["effects"], before
            assert before["model_entries"] == 1 and before["planner_entries"] == 0, before
            assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
            assert all(not ordinary(session, i)["requests"] for i in range(2))
            restart(session)
            after = until(blocked, lambda s: s["drive_failures"] > before["drive_failures"], "ordinary blocked after reopen")
            assert not after["outer_intents"] and not after["requests"] and not after["effects"], after
            assert after["model_entries"] == 1 and after["planner_entries"] == 0, after
            assert all(not ordinary(session, i)["requests"] for i in range(2))
            record(name, session, before_restart=before)
            continue
        until(lambda: ordinary(session, 0), lambda s: s["model_started"], "ordinary gated child")
        settled = until(lambda: api(session, "evidence"),
                        lambda s: len(s["outer_intents"]) == 2 and sum(c["completed"] for c in s["outer_intents"]) == 1,
                        "ordinary large child settled")
        large = next(c for c in settled["outer_intents"] if c["completed"])
        child_result = ordinary(session, 1, "result")
        assert len(child_result["response"]["content"][0]["text"]) == result_size
        # ChildReply fits, but the containing Call also retains repeated input/intent data.
        reply_bytes = len(json.dumps({"status":"completed", "result":child_result}, separators=(",", ":")).encode())
        assert reply_bytes < 1024 * 1024, reply_bytes
        # The parent stores the returned text in both content and DurableToolResult.
        successful_call_min_bytes = large["empty_bytes"] - len("null") + 2 * result_size
        if name == "ordinary-call-overflow":
            assert successful_call_min_bytes > 1024 * 1024, (large, successful_call_min_bytes)
            assert large["output"]["result"]["disposition"] == "error", large
            error = large["output"]["result"]["result"]["value"]
            assert error["kind"] == "other" and error["retryable"] is False, large
            assert large["bytes"] <= 1024 * 1024, large
        else:
            assert large["output"]["result"]["disposition"] == "success", large
        assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
        fast_before = ordinary(session, 1)["effects"]
        assert len(fast_before) == 1
        restart(session)
        assert ordinary(session, 1)["effects"] == fast_before
        assert api(session, "status")["busy"]
        ordinary(session, 0, "release", {})
        finished(session)
        children = [ordinary(session, i) for i in range(2)]
        assert all(len(c["effects"]) == 1 for c in children)
        assert children[1]["effects"] == fast_before
        result = api(session, "result?request_id=one")
        assert [o["disposition"] for o in result["tool_outcomes"]] == ["success", "error" if name == "ordinary-call-overflow" else "success"], result
        for index, evidence in enumerate(children):
            key = {**api(session, "state")["key"], "call_index": index}
            identity = "rig-child-" + hashlib.sha256(f"child-v1;{canonical(key)};None".encode()).hexdigest()
            assert evidence["requests"] and all(r["child_id"] == identity for r in evidence["requests"]), evidence
            submissions = [r["body"] for r in evidence["requests"] if r["operation"] == "submit"]
            assert submissions and len(set(submissions)) == 1, evidence
            status = ordinary(session, index, "status")
            assert not status["busy"] and len(status["receipts"]) == 1, status
            assert status["receipts"][0]["request_id"] == identity and status["receipts"][0]["state"] == "answered", status
        record(name, session, settled=settled, child_reply_bytes=reply_bytes,
               successful_call_min_bytes=successful_call_min_bytes, children=children, result=result)


sys.excepthook = failed_run

if args.review_fixes_only:
    review_fixes()
    print(f"PASS [{args.runtime}] {len(observations['cases'])} review regression cases", flush=True)
    sys.exit(0)

if args.outer_boundary_only:
    outer_boundary()
    sys.exit(0)


# Ordinal 0 must remain pending while ordinal 1 is committed as the winner.
race_source = """
return {winner: await Promise.race([
  tools.child({branch: 'slow', gate: true}),
  tools.child({branch: 'fast'})
])};
"""
session, _ = configure("race", race_source, pause_after=1)
submit(session)
until(lambda: child(session, 0), lambda s: s["model_started"], "slow child model started")
before = until(lambda: api(session, "evidence"), lambda s: s["paused"], "committed fast delivery")
prefix = api(session, "state")
assert [item["call"]["ordinal"] for item in prefix["scheduler"]["input"]["history"]] == [1], prefix
assert not child(session, 0)["effects"], child(session, 0)
fast_before = child_effect(session, 1, "fast")
assert api(session, "status")["busy"]
restart(session)
assert api(session, "state")["scheduler"]["input"]["history"] == prefix["scheduler"]["input"]["history"]
assert child(session, 1)["effects"] == fast_before["effects"]
api(session, "release-parent", {})
draining = until(lambda: api(session, "state"), lambda s: s["scheduler"]["draining"], "parent drains blocked loser")
assert api(session, "status")["busy"] and not child(session, 0)["effects"], draining
api(session, "close", {})
assert api(session, "status")["busy"] and api(session, "status")["closed"]
child(session, 0, "release", body={})
finished(session)
outcome = report(session)
assert outcome["status"] == "completed" and outcome["returned"] == {"winner": "fast"}, outcome
assert [(c["ordinal"], c["delivery"]) for c in outcome["calls"]] == [(0, "discarded"), (1, "delivered")], outcome
slow = child_effect(session, 0, "slow")
fast = child_effect(session, 1, "fast")
record("out-of-order race/restart/loser drain", session, before=before, prefix_before=prefix,
       report=outcome, children=[slow, fast])

# The real child endpoint commits admission, then its acknowledgement is held.
session, _ = configure("lost-ack", "return await tools.child({branch: 'once'});", lose_ack=True, pause_ack=True)
submit(session)
until(lambda: api(session, "evidence"), lambda s: s["ack_dropped"], "child committed before parent acknowledgement")
until(lambda: child(session, 0, "result"), lambda s: s is not None, "successful child result before parent crash")
child_effect(session, 0, "once")
before = child(session, 0)
assert len([r for r in before["requests"] if r["operation"] == "submit"]) == 1, before
restart(session)
finished(session)
assert report(session)["status"] == "completed" and report(session)["returned"] == "once", report(session)
after = child_effect(session, 0, "once")
submissions = [r for r in after["requests"] if r["operation"] == "submit"]
assert len(submissions) >= 2, after
assert after["effects"] == before["effects"], (before, after)
record("uncertain child acknowledgement reuses identity", session, child_before=before, child_after=after)

# A malformed or oversized HTTP200 reply is not proof that the child stopped.
for operation in ["submit", "poll"]:
    for kind in ["malformed", "oversized"]:
        fault = f"{operation}-{kind}"
        session, _ = configure(f"corrupt-{fault}", "return await tools.child({branch: 'corrupt', gate: true});",
                               corrupt_reply=fault)
        submit(session)
        until(lambda: child(session, 0), lambda s: s["corrupted_reply"] == fault, "real child committed before corrupt HTTP200")
        until(lambda: api(session, "evidence"), lambda s: s["reply_observed"], "parent decoded corrupt response")
        until(lambda: child(session, 0), lambda s: s["model_started"], "corrupt reply child actually running")
        assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None, api(session, "status")
        before = child(session, 0)
        assert not before["effects"]
        acknowledged = operation == "poll"
        assert api(session, "evidence")["effect_envelopes"][0]["child"]["acknowledged"] == acknowledged
        count = len([r for r in before["requests"] if r["operation"] == operation])
        restart(session)
        assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
        assert api(session, "evidence")["effect_envelopes"][0]["child"]["acknowledged"] == acknowledged
        api(session, "release-reply", {})
        retried = until(lambda: child(session, 0), lambda s: len([
            r for r in s["requests"] if r["operation"] == operation
        ]) > count, "same child operation retried after reopen")
        assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
        until(lambda: api(session, "evidence"), lambda s: s["effect_envelopes"][0]["child"]["acknowledged"], "valid retried response acknowledged")
        if operation == "poll":
            assert len([r for r in retried["requests"] if r["operation"] == "submit"]) == 1, retried
        requests = [r for r in retried["requests"] if r["operation"] == operation]
        assert len({r["body"] for r in requests}) == 1, requests
        assert not retried["effects"]
        checkpoint(f"{fault}-retry-pending", session)
        child(session, 0, "release", body={})
        finished(session)
        outcome = report(session)
        assert outcome["status"] == "completed" and outcome["returned"] == "corrupt", outcome
        assert len(outcome["calls"]) == 1 and outcome["calls"][0]["status"] == "succeeded", outcome
        record(f"corrupt HTTP200 {fault} retries until child terminal", session,
               child_before=before, child_after=child_effect(session, 0, "corrupt"), report=outcome)

# A planner failure must drain the gated active child and the queued third child.
session, _ = configure("planner-failure", """
return await Promise.all([
  tools.child({branch: 'fast'}),
  tools.child({branch: 'active', gate: true}),
  tools.child({branch: 'queued', gate: true})
]);
""", pause_after=0, fail_after=0)
submit(session)
until(lambda: api(session, "evidence"), lambda s: s["paused"], "planner failure checkpoint")
state = checkpoint("before-planner-failure", session)["state"]
assert [c["ordinal"] for c in state["scheduler"]["active"]] == [1], state
assert [c["ordinal"] for c in state["scheduler"]["queued"]] == [2], state
assert not child(session, 2)["requests"], child(session, 2)
until(lambda: child(session, 1), lambda s: s["model_started"], "active child started before planner failure")
assert not child(session, 1)["effects"]
assert api(session, "status")["busy"]
api(session, "release-parent", {})
until(lambda: api(session, "state"), lambda s: bool(s["scheduler"]["failure"]), "planner failure retained")
until(lambda: child(session, 2), lambda s: s["model_started"], "queued child admitted during drain")
assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
assert not child(session, 1)["effects"] and not child(session, 2)["effects"]
child(session, 1, "release", body={})
until(lambda: api(session, "state"), lambda s: any(
    item["call"]["ordinal"] == 1 for item in s["scheduler"]["input"]["history"]
), "active child drained while queued child remains blocked")
assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
assert not child(session, 2)["effects"]
child(session, 2, "release", body={})
finished(session)
state = api(session, "state")["scheduler"]
assert not state["queued"] and not state["active"] and not state["ready"], state
assert "injected planner failure" in state["action"]["Complete"]["Err"], state
assert api(session, "result?request_id=one")["tool_outcomes"][0]["disposition"] == "error"
assert sorted(item["call"]["ordinal"] for item in state["input"]["history"]) == [0, 1, 2], state
children = [child_effect(session, ordinal, branch) for ordinal, branch in enumerate(["fast", "active", "queued"])]
record("planner failure drains active and queued children", session, children=children)

# Commit an effect, hold its result, and restart. A non-repeatable tool must not run again.
session, _ = configure("uncertain-effect", "return await tools.observe({branch: 'uncertain', pause_after_effect: true});")
submit(session)
before = until(lambda: api(session, "evidence"), lambda s: s["effect_committed"], "effect before result commit")
assert len(before["effects"]) == 1, before
restart(session)
finished(session)
outcome = report(session)
assert outcome["status"] == "script_error" and len(outcome["calls"]) == 1, outcome
assert outcome["calls"][0]["status"] == "failed" and outcome["calls"][0]["error_kind"] == "other", outcome
assert "was interrupted" in outcome["diagnostic"]["message"] and "it was not run again" in outcome["diagnostic"]["message"], outcome
envelopes = api(session, "evidence")["effect_envelopes"]
assert len(envelopes) == 1 and envelopes[0]["output"]["result"]["disposition"] == "interrupted", envelopes
assert envelopes[0]["output"]["result"]["interruption"]["reason"] == "claim_held", envelopes
parent = api(session, "state")["key"]
nested = {**parent, "logical_execution_id": "script:" + canonical(parent)}
assert before["effects"][0] == {"logical_id": canonical(nested), "branch": "uncertain"}, before
assert api(session, "evidence")["effects"] == before["effects"]
record("uncertain non-repeatable effect is not repeated", session, before=before, report=outcome)

# Equal child ordinals from two outer script calls must address different objects.
session, _ = configure("identity", "return await tools.child({branch: 'identity'});")
submit(session, "one")
finished(session)
first = child_effect(session, 0, "identity")
submit(session, "two")
finished(session)
second = child_effect(session, 0, "identity", prompt=1)
first_id = first["requests"][0]["child_id"]
second_id = second["requests"][0]["child_id"]
assert first_id != second_id, (first_id, second_id)
record("two scripts separate ordinal-zero identities", session, children=[first, second])

# A local schema rejection must win its finite chain while the valid child drains.
session, _ = configure("schema", """
const invalid = tools.child({branch: 7}).catch(() =>
  Promise.resolve().then(() => Promise.resolve()).then(() => 'invalid'));
const valid = tools.child({branch: 'valid'});
return await Promise.race([invalid, valid]);
""")
submit(session)
finished(session)
outcome = report(session)
assert outcome["returned"] == "invalid", outcome
assert [(c["status"], c["delivery"]) for c in outcome["calls"]] == [("failed", "delivered"), ("succeeded", "discarded")], outcome
assert outcome["calls"][0]["error_kind"] == "invalid_args", outcome
assert not child(session, 0)["requests"] and not child(session, 0)["effects"]
record("schema denial plus admitted valid child drain", session, report=outcome, child=child_effect(session, 1, "valid"))

# A temporary registration mismatch must retain admitted work for restoration.
for name, change in [("runtime", {"drift": True}), ("catalog", {"catalog_drift": True}),
                     ("policy", {"policy_drift": True}), ("limits", {"max_in_flight": 1})]:
    session, config = configure(f"drift-{name}", race_source, pause_after=1)
    submit(session)
    before = until(lambda: api(session, "evidence"), lambda s: s["paused"], "drift checkpoint")
    retained = checkpoint(f"{name}-before-mismatch", session)
    api(session, "configure", {**config, **change})
    restart(session)
    blocked = until(lambda: api(session, "evidence"), lambda s: s["drive_failures"] > before["drive_failures"] or not api(session, "status")["busy"], "incompatible registration blocks recovery")
    status = api(session, "status")
    assert status["busy"] and not status["closed"] and api(session, "result?request_id=one") is None, status
    assert status["receipts"] == retained["status"]["receipts"], status
    assert api(session, "state") == retained["state"]
    assert blocked["drive_error"] and ("implementation version" in blocked["drive_error"] or "mismatch" in blocked["drive_error"]), blocked
    assert api(session, "evidence")["requests"] == before["requests"], (before, api(session, "evidence"))
    assert api(session, "evidence")["planner_entries"] == before["planner_entries"]
    assert api(session, "evidence")["model_entries"] == before["model_entries"]
    assert not child(session, 0)["effects"]
    assert len(child(session, 1)["effects"]) == 1
    checkpoint(f"{name}-retained-under-mismatch", session)
    api(session, "configure", config)
    restart(session)
    assert api(session, "state") == retained["state"]
    api(session, "release-parent", {})
    until(lambda: api(session, "state"), lambda s: s["scheduler"]["draining"], "restored parent drains retained loser")
    assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
    child(session, 0, "release", body={})
    finished(session)
    outcome = report(session)
    assert outcome["status"] == "completed" and outcome["returned"] == {"winner": "fast"}, outcome
    assert [(c["ordinal"], c["delivery"]) for c in outcome["calls"]] == [(0, "discarded"), (1, "delivered")], outcome
    record(f"{name} mismatch retains work and resumes after restore", session, before=before,
           report=outcome, children=[child_effect(session, 0, "slow"), child_effect(session, 1, "fast")])

session, _ = configure("capabilities", "return [typeof fetch, typeof crypto, typeof process, typeof setTimeout, typeof WebAssembly];")
submit(session)
finished(session)
outcome = report(session)
assert outcome["returned"] == ["undefined"] * 5 and not outcome["calls"], outcome
assert not api(session, "evidence")["requests"] and not api(session, "evidence")["effects"]
record("selected ambient globals absent", session, report=outcome)

# A fitting result must still succeed under the same small checkpoint cap.
for name, sizes in [("small-results-control", [1000, 1000]), ("single-large-control", [15_000])]:
    calls = [f"tools.observe({{branch: 'control-{i}', result_bytes: {size}}})"
             for i, size in enumerate(sizes)]
    calls.append("tools.child({branch: 'limit-control', gate: true})")
    session, _ = configure(name, "await Promise.all([" + ",".join(calls) + "]); return 'bounded-control';",
                           max_in_flight=len(calls), max_history_bytes=32 * 1024)
    submit(session)
    until(lambda: api(session, "state"), lambda s: s["scheduler"] is not None and len(s["scheduler"]["input"]["history"]) == len(sizes),
          "fitting results committed under small cap")
    until(lambda: child(session, len(sizes)), lambda s: s["model_started"], "positive control child started")
    assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
    child(session, len(sizes), "release", body={})
    finished(session)
    outcome = report(session)
    assert outcome["status"] == "completed" and outcome["returned"] == "bounded-control", outcome
    assert all(c["status"] == "succeeded" for c in outcome["calls"]), outcome
    assert sorted(row["branch"] for row in api(session, "evidence")["effects"]) == [f"control-{i}" for i in range(len(sizes))]
    record(f"{name} succeeds under small checkpoint cap", session, report=outcome,
           child=child_effect(session, len(sizes), "limit-control"))

# Oversized results must not publish failure before the admitted sibling drains.
for name, sizes, controls in [
    ("oversized-result", [1_200_000], {}),
    ("aggregate-checkpoint", [15_000, 15_000], {"max_history_bytes": 32 * 1024}),
]:
    calls = [f"tools.observe({{branch: 'large-{i}', result_bytes: {size}, pause_after_effect: true}})"
             for i, size in enumerate(sizes)]
    calls.append("tools.child({branch: 'limit-loser', gate: true})")
    session, _ = configure(name, "return await Promise.all([" + ",".join(calls) + "]);",
                           max_in_flight=len(calls), **controls)
    submit(session)
    until(lambda: api(session, "evidence"), lambda s: len(s["effects"]) == len(sizes), "large effects committed before result")
    until(lambda: child(session, len(sizes)), lambda s: s["model_started"], "limit sibling model started")
    assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
    checkpoint(f"{name}-before-results", session)
    api(session, "release-effect", {})
    until(lambda: api(session, "state"), lambda s: s["scheduler"]["draining"] or not api(session, "status")["busy"], "limit rejection begins drain")
    assert api(session, "status")["busy"] and api(session, "result?request_id=one") is None
    assert not child(session, len(sizes))["effects"]
    checkpoint(f"{name}-draining", session)
    child(session, len(sizes), "release", body={})
    finished(session)
    result = api(session, "result?request_id=one")
    assert result["tool_outcomes"][0]["disposition"] == "success", result
    state = api(session, "state")["scheduler"]
    assert not state["active"] and not state["queued"] and not state["ready"], state
    outcome = report(session)
    assert outcome["status"] == "script_error" and state["failure"] is None, outcome
    assert state["action"]["Complete"]["Ok"] == outcome, state
    assert [c["ordinal"] for c in outcome["calls"]] == list(range(len(calls))), outcome
    assert any(c["name"] == "observe" and c["status"] == "failed" and c["error_kind"] == "other"
               for c in outcome["calls"]), outcome
    assert "durable storage limit" in outcome["diagnostic"]["message"], outcome
    stored = api(session, "evidence")
    failed_ordinals = {c["ordinal"] for c in outcome["calls"] if c["name"] == "observe" and c["status"] == "failed"}
    failed = [(e, size) for e, size in zip(stored["effect_envelopes"], stored["effect_row_bytes"])
              if e["call"]["ordinal"] in failed_ordinals]
    assert len(failed) == len(failed_ordinals), stored
    for envelope, size in failed:
        assert size <= min(1024 * 1024, controls.get("max_history_bytes", 8 * 1024 * 1024)), (envelope, size)
        assert envelope["output"]["result"]["disposition"] == "error", envelope
        assert "durable storage limit" in json.dumps(envelope["output"]), envelope
    before = stored["effects"]
    assert sorted(effect["branch"] for effect in before) == [f"large-{i}" for i in range(len(sizes))], before
    loser = child_effect(session, len(sizes), "limit-loser")
    restart(session)
    assert not api(session, "status")["busy"]
    assert api(session, "evidence")["effects"] == before
    assert child(session, len(sizes))["effects"] == loser["effects"]
    record(f"{name} drains without repeating effects", session, child=loser, report=outcome)

outer_boundary()
review_fixes()

print(f"PASS [{args.runtime}] {len(observations['cases'])} real Worker workflow cases", flush=True)
