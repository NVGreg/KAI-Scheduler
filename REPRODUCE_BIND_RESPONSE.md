<!-- Copyright 2026 NVIDIA CORPORATION; SPDX-License-Identifier: Apache-2.0 -->

# Reproduce committed Binding response loss on a disposable Kind cluster

Run these commands from this reproduction branch’s repository root. Requirements: Linux amd64, Docker, Go 1.26.8, curl and sha256sum. The CPU-only harness directly calls the real Binder, GPUSharing plugin and reservation service against a single-node Kind cluster, using synthetic GPU metadata. Both comparison modes use the same code, with binding confirmation enabled or disabled.

The fixed test cluster name is deliberately checked by the fixture. The commands refuse an already-existing cluster with that name, use a new dedicated kubeconfig, and never load or modify the default kubeconfig. The injected fault closes an HTTP/1 socket only after the API returned 201 and the same Pod became Running/Ready. It does not measure production frequency.

```bash
(
set -euo pipefail
umask 077
KAI_REPRO_SOURCE=$(pwd)
KAI_REPRO_DIR=$(mktemp -d)
KAI_REPRO_CLUSTER=kai2452-repro-20261022-f4c2a9
KAI_REPRO_IMAGE=kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0

curl -fL https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-amd64 -o "$KAI_REPRO_DIR/kind"
printf '%s  %s\n' aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d "$KAI_REPRO_DIR/kind" | sha256sum -c -
curl -fL https://dl.k8s.io/release/v1.35.8/bin/linux/amd64/kubectl -o "$KAI_REPRO_DIR/kubectl"
printf '%s  %s\n' 874d5e72dbb819f43cff16bcd1e4f8bac5b7f2361fe1e55049b0a6c676fb0cbf "$KAI_REPRO_DIR/kubectl" | sha256sum -c -
chmod +x "$KAI_REPRO_DIR/kind" "$KAI_REPRO_DIR/kubectl"
test -z "$(docker ps -a --filter "name=^/${KAI_REPRO_CLUSTER}-control-plane$" --format '{{.ID}}')"

cat > "$KAI_REPRO_DIR/kind.yaml" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  apiServerAddress: "127.0.0.1"
  apiServerPort: 0
nodes:
  - role: control-plane
YAML

export KIND_EXPERIMENTAL_PROVIDER=docker
"$KAI_REPRO_DIR/kind" create cluster --name "$KAI_REPRO_CLUSTER" --image "$KAI_REPRO_IMAGE" \
  --config "$KAI_REPRO_DIR/kind.yaml" --kubeconfig "$KAI_REPRO_DIR/kubeconfig" --wait 180s
trap '"$KAI_REPRO_DIR/kind" delete cluster --name "$KAI_REPRO_CLUSTER" --kubeconfig "$KAI_REPRO_DIR/kubeconfig"' EXIT
chmod 600 "$KAI_REPRO_DIR/kubeconfig"
docker exec "${KAI_REPRO_CLUSTER}-control-plane" crictl inspecti registry.k8s.io/pause:3.10 \
  > "$KAI_REPRO_DIR/pause-image.json"

"$KAI_REPRO_DIR/kubectl" --kubeconfig "$KAI_REPRO_DIR/kubeconfig" apply \
  -f "$KAI_REPRO_SOURCE/deployments/kai-scheduler/crds/scheduling.run.ai_bindrequests.yaml"
"$KAI_REPRO_DIR/kubectl" --kubeconfig "$KAI_REPRO_DIR/kubeconfig" wait \
  --for=condition=Established crd/bindrequests.scheduling.run.ai --timeout=60s

export KAI_PRIVATE_KUBECONFIG="$KAI_REPRO_DIR/kubeconfig"
export KAI_PRIVATE_NODE="${KAI_REPRO_CLUSTER}-control-plane"
export KAI_PRIVATE_NODE_UID
KAI_PRIVATE_NODE_UID=$("$KAI_REPRO_DIR/kubectl" --kubeconfig "$KAI_REPRO_DIR/kubeconfig" get node "$KAI_PRIVATE_NODE" -o jsonpath='{.metadata.uid}')
export KAI_PRIVATE_RECEIPTS="$KAI_REPRO_DIR/receipts"
mkdir -p "$KAI_PRIVATE_RECEIPTS"
go test ./pkg/binder/controllers -run '^TestPrivateKindBindingResponseLoss$' -count=1 -v -timeout=8m \
  2>&1 | tee "$KAI_PRIVATE_RECEIPTS/test.log"
)
```

Expected: all six comparison cases pass their assertions. Ordinary success uses zero Binder confirmation reads in both modes. With confirmation enabled, a lost Binding response preserves both ConfigMap UIDs, the GPU-group label and reservation UID, marks the request Succeeded and calls PostBind once. With confirmation disabled, the same real committed Binding yields Failed, loses both ConfigMaps and the label, and skips PostBind. Both modes reject the real wrong-UID Binding request. Each case records actual wire attempts, response status, genuine transport errors, separate verification/confirmation counts, manifests and resource identities.

The pinned node image includes `registry.k8s.io/pause:3.10`; the preflight checks that tag. No separate workload image download is needed.

Share only reviewed test logs and per-case results. Keep kubeconfigs, certificates and tokens private. The exit trap deletes the disposable cluster. The official pinned tool/node-image sources are the [Kind v0.33.0 release](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0) and [Kubernetes kubectl downloads](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/).
