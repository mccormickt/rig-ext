"""Exercise a local Durable Object through its HTTP API, without API keys."""

import argparse
import json
import time
import urllib.error
import urllib.request


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--url", default="http://127.0.0.1:9876")
parser.add_argument("--session-prefix", default="durable")
parser.add_argument("--verify-reopen", action="store_true")
args = parser.parse_args()


def request(session, path, body=None, error=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        f"{args.url}/{session}/{path}", data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            text = response.read().decode()
    except urllib.error.HTTPError as response:
        text = response.read().decode()
        assert error and error in text, (response.status, text)
        return
    assert error is None, f"expected rejection: {error}"
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def submit(session, request_id, text="Add 20 and 22", **kwargs):
    return request(session, "submit", {
        "request_id": request_id,
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
        "mode": kwargs.pop("mode", "follow_up"),
    }, **kwargs)


def until(session, predicate):
    for _ in range(100):
        status = request(session, "status")
        if predicate(status):
            return status
        time.sleep(0.1)
    raise AssertionError(f"alarm did not make progress: {status}")


def decision(session, approve):
    status = until(session, lambda s: bool(s["approvals"]))
    body = {"decision": "approve" if approve else "deny",
            "approval_id": status["approvals"][0]["approval_id"]}
    if not approve:
        body["reason"] = "test denial"
    request(session, "approval", body)
    until(session, lambda s: not s["busy"])


def result(session, request_id, disposition):
    response = request(session, f"result?request_id={request_id}")
    assert response["response"]["output"] == "The tool round is complete.", response
    assert len(response["tool_outcomes"]) == 1, response
    assert response["tool_outcomes"][0]["disposition"] == disposition, response
    return response


session = f"{args.session_prefix}-conformance"
pending = f"{args.session_prefix}-reopen-pending"
if args.verify_reopen:
    assert request(session, "status")["closed"]
    result(session, "one", "success")
    result(session, "two", "refused")
    assert len(request(session, "transcript")) == 8
    assert submit(session, "one")["state"] == "answered"
    assert request(pending, "status")["approvals"]
    decision(pending, True)
    result(pending, "pending", "success")
    assert len(request(pending, "transcript")) == 4
    print("PASS: results, receipts, transcript, and pending approval survive restart")
else:
    receipt = submit(session, "one")
    assert receipt["prompt_index"] == 0, receipt
    until(session, lambda s: bool(s["approvals"]))
    assert submit(session, "one")["prompt_index"] == 0
    submit(session, "one", "different", error="different message or mode")
    submit(session, "busy", mode="reject_if_busy", error="processing another prompt")
    decision(session, True)
    result(session, "one", "success")
    transcript = request(session, "transcript")
    assert len(transcript) == 4, transcript
    assert "42" in json.dumps(transcript[2]), transcript
    submit(session, "two")
    decision(session, False)
    result(session, "two", "refused")
    request(session, "close", {})
    submit(session, "closed", error="session is closed")
    assert submit(session, "one")["state"] == "answered"
    submit(pending, "pending")
    until(pending, lambda s: bool(s["approvals"]))
    print("PASS: alarm-driven tools, dedupe, conflict, busy, approval, denial, close")
    print("Restart the server without deleting storage, then use --verify-reopen")
