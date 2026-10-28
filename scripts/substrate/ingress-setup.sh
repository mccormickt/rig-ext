#!/usr/bin/env bash
# Issue client credentials only in the dedicated proof cluster.
set -euo pipefail
: "${KUBECONFIG:?Set the dedicated kubeconfig}"
: "${RUN_DIR:?Set the private run directory}"
CONTEXT=${KUBECTL_CONTEXT:-kind-rig-ollama-e2e}
test "$CONTEXT" = kind-rig-ollama-e2e
umask 077
mkdir -p "$RUN_DIR/clients"
for CLIENT in rig-proof-client rig-denied-client; do
  kubectl --context "$CONTEXT" -n rig-local apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata: {name: $CLIENT}
---
apiVersion: v1
kind: Pod
metadata: {name: $CLIENT}
spec:
  serviceAccountName: $CLIENT
  containers:
  - name: client
    image: curlimages/curl@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6
    command: [sleep, infinity]
    volumeMounts:
    - {name: identity, mountPath: /identity, readOnly: true}
  volumes:
  - name: identity
    projected:
      sources:
      - podCertificate:
          signerName: podidentity.podcert.ate.dev/identity
          keyType: ECDSAP256
          credentialBundlePath: client.pem
      - clusterTrustBundle:
          signerName: servicedns.podcert.ate.dev/identity
          labelSelector:
            matchLabels: {podcert.ate.dev/canarying: live}
          path: ca.pem
EOF
  kubectl --context "$CONTEXT" -n rig-local wait --for=condition=Ready "pod/$CLIENT" --timeout=120s
  kubectl --context "$CONTEXT" -n rig-local exec "$CLIENT" -- cat /identity/client.pem > "$RUN_DIR/clients/$CLIENT.pem"
done
kubectl --context "$CONTEXT" -n rig-local exec rig-proof-client -- cat /identity/ca.pem > "$RUN_DIR/clients/ca.pem"
openssl x509 -in "$RUN_DIR/clients/rig-proof-client.pem" -noout -subject -issuer -ext subjectAltName
openssl x509 -in "$RUN_DIR/clients/rig-denied-client.pem" -noout -subject -issuer -ext subjectAltName
