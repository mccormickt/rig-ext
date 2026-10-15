#!/usr/bin/env python3
"""Set explicit native policy controls in the dedicated proof cluster only."""

import argparse
import json
import os
import re
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--parking-max", type=int, default=1024)
args = parser.parse_args()
if args.parking_max < 0:
    parser.error("parking capacity cannot be negative")
context = os.environ.get("KUBECTL_CONTEXT", "kind-rig-ollama-e2e")
assert context == "kind-rig-ollama-e2e", "refusing another cluster"
assert os.environ.get("KUBECONFIG"), "set the dedicated kubeconfig"
base = ["kubectl", "--context", context, "-n", "ate-system"]
config = json.loads(
    subprocess.check_output(
        [*base, "get", "configmap", "atenet-router-agentgateway-config", "-o", "json"]
    )
)
text = config["data"]["config.yaml"]
text = re.sub(r"(?m)^      (cacheTtl: .*|requestParking: \{.*\})\n", "", text)
anchor = "      connectTargetPort: 8443\n"
assert (
    text.count(anchor) == 1
    and "requestParking:" not in text
    and "cacheTtl:" not in text
)
text = text.replace(
    anchor,
    anchor
    + f"      cacheTtl: 0s\n      requestParking: {{budget: 5s, max: {args.parking_max}, retryInterval: 100ms, retryFactor: 1.1}}\n",
)
config["data"]["config.yaml"] = text
print(text)
subprocess.run(
    [*base, "replace", "-f", "-"], input=json.dumps(config), text=True, check=True
)
subprocess.run([*base, "rollout", "restart", "deployment/atenet-router"], check=True)
subprocess.run(
    [*base, "rollout", "status", "deployment/atenet-router", "--timeout=120s"],
    check=True,
)
