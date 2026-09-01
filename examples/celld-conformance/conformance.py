#!/usr/bin/env python3
"""Run the shared HTTP conformance sequence against a live celld fixture."""

import argparse
import concurrent.futures
import json
import urllib.error
import urllib.request


def request(base_url: str, operation: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode()
    method = "GET" if payload is None else "POST"
    req = urllib.request.Request(
        f"{base_url}/conformance/sqlite/{operation}",
        data=data,
        method=method,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        body = error.read().decode(errors="replace")
        raise RuntimeError(f"{operation} returned HTTP {error.code}: {body}") from error


def run(base_url: str) -> None:
    suite = request(base_url, "run", {})
    if suite.get("status") != "ok":
        raise RuntimeError(f"suite failed: {suite}")

    request(base_url, "reset-concurrency", {})
    count = 16
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(
                request,
                base_url,
                "upsert",
                {"id": "concurrent", "content": f"alpha write {index}", "metadata": {}},
            )
            for index in range(count)
        ]
        generations = sorted(future.result()["generation"] for future in futures)
    expected = list(range(1, count + 1))
    if generations != expected:
        raise RuntimeError(
            f"concurrent generations were {generations}, expected {expected}"
        )
    concurrency = request(
        base_url, "verify-concurrency", {"expected_generation": count}
    )
    print(json.dumps({"suite": suite, "concurrency": concurrency}, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:9876")
    parser.add_argument("--verify-reopen", action="store_true")
    args = parser.parse_args()
    if args.verify_reopen:
        print(json.dumps(request(args.base_url, "verify-reopen"), sort_keys=True))
    else:
        run(args.base_url)


if __name__ == "__main__":
    main()
