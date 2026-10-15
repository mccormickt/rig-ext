#!/usr/bin/env bash
# Configure only the dedicated cluster; start receipt_proxy.py separately.
set -euo pipefail
: "${KUBECONFIG:?Set the dedicated kubeconfig}"
: "${OLLAMA_BRIDGE_IP:?Set the Docker bridge address used by receipt_proxy.py}"
CONTEXT=${KUBECTL_CONTEXT:-kind-rig-ollama-e2e}
test "$CONTEXT" = kind-rig-ollama-e2e
kubectl --context "$CONTEXT" apply -f - <<EOF
apiVersion: v1
kind: Service
metadata: {name: ollama, namespace: rig-local}
spec:
  ports:
  - {name: http, port: 15024, targetPort: 15024}
  - {name: excluded, port: 15027, targetPort: 15024}
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: ollama
  namespace: rig-local
  labels: {kubernetes.io/service-name: ollama}
addressType: IPv4
ports:
- {name: http, protocol: TCP, port: 15024}
- {name: excluded, protocol: TCP, port: 15024}
endpoints:
- addresses: ["$OLLAMA_BRIDGE_IP"]
  conditions: {ready: true}
EOF
kubectl ate --context "$CONTEXT" get actor-template rig-poc -a rig-local -o json | \
  python3 -c 'import json,sys
d=json.load(sys.stdin); d.pop("status",None)
d["metadata"]={"name":"rig-ollama","atespace":"rig-local"}
d["snapshotConfig"]["storageLocation"]="gs://ate-snapshots/rig-ollama/"
d["containers"][0]["env"]=[{"name":k,"value":v} for k,v in {
  "INFERENCE":"ollama", "OLLAMA_URL":"http://ollama.rig-local.svc.cluster.local:15024",
  "OLLAMA_MODEL":"qwen3.5:9b", "EGRESS_URL":"http://ollama.rig-local.svc.cluster.local:15024/probe",
  "PUBLIC_URL":"http://127.0.0.1:15020"}.items()]
print(json.dumps(d))' | kubectl ate --context "$CONTEXT" create actor-template -f -
echo 'Wait for a successful golden snapshot. Create actors from rig-ollama, then test default-deny before granting egress.'
echo 'Allow only http hostnames: [ollama.rig-local.svc.cluster.local], ports: {numbers: [15024]}.'
