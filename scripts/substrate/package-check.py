#!/usr/bin/env python3
"""Check image identity, fresh state, and fail-closed unavailable inference."""

import argparse
import tempfile
import time
from pathlib import Path

from ollama import Live

parser = argparse.ArgumentParser()
parser.add_argument("--image", required=True)
parser.add_argument("--build", required=True)
parser.add_argument("--evidence", type=Path, required=True)
args = parser.parse_args()
args.direct_port = 15028
with tempfile.TemporaryDirectory(dir=".amp/in", prefix="package-") as scratch:
    args.tokens = Path(scratch) / "tokens.json"
    args.tokens.write_text("{}")
    p = Live(args)
    container = None
    try:
        image = p.command(
            "docker", "inspect", args.image, "--format", "{{.Id}}"
        ).strip()
        container = p.command(
            "docker",
            "run",
            "-d",
            "--label",
            "rig-proof=package-check",
            "-p",
            "127.0.0.1:15028:80",
            "-e",
            "INFERENCE=ollama",
            "-e",
            "OLLAMA_URL=http://127.0.0.1:9",
            "-e",
            "OLLAMA_MODEL=qwen3.5:9b",
            args.image,
        ).strip()
        p.check(
            "running exact built image",
            p.command("docker", "inspect", container, "--format", "{{.Image}}").strip(),
            image,
        )
        for _ in range(100):
            try:
                initial = p.inspect()
                break
            except (ConnectionRefusedError, ConnectionResetError):
                time.sleep(0.1)
        else:
            p.command("docker", "logs", container)
            raise TimeoutError("packaged actor did not listen")
        p.check("packaged build", initial["build"], args.build)
        p.check("fresh packaged journal", initial["journal"], "")
        p.check("fresh packaged nonce", initial["nonce"], None)
        task = p.send("Call record with operation must-not-run and value no-fallback.")
        p.check(
            "unavailable inference fails task",
            task["status"]["state"],
            "TASK_STATE_FAILED",
        )
        after = p.inspect()
        p.check(
            "no model fallback or effect",
            (after["completions"], after["invocations"], after["commits"]),
            (0, 0, 0),
        )
        p.check("cleanup completed", after["active"], 0)
        p.check("no journal change", after["journal"], "")
        p.command("docker", "logs", container)
        p.record(
            "result",
            status="PASS",
            scope="exact packaged image and unavailable-provider negative control; not sandbox restore",
        )
    except Exception as error:
        p.record("result", status="FAIL", error=str(error), type=type(error).__name__)
        raise
    finally:
        if container:
            p.command("docker", "rm", "-f", container)
