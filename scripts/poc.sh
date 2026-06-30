#!/usr/bin/env bash
# Full POC lifecycle manager — Kafka-as-a-Claim on Crossplane + Confluent Cloud.
#
# Usage:
#   bash scripts/poc.sh up    — provision Confluent Cloud + full K8s stack
#   bash scripts/poc.sh down  — destroy everything in reverse order
#
# Prerequisites (must be done before running this script):
#   confluent login                              (Confluent Cloud auth)
#   kind create cluster --name kafka-claim-poc   (or any reachable cluster)
#
# Running 'up' when a state file already exists skips Confluent Cloud and
# re-applies the K8s stack (all Helm installs are idempotent via upgrade --install).

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
STATE_FILE="${SCRIPT_DIR}/.poc-state"

ENV_NAME="${CONFLUENT_ENV_NAME:-kafka-claim-poc}"
CLUSTER_NAME="${CONFLUENT_CLUSTER_NAME:-kafka-claim-poc-cluster}"
SA_NAME="${CONFLUENT_SA_NAME:-crossplane-poc}"
AWS_REGION="${AWS_REGION:-us-east-1}"
K8S_NAMESPACE="${K8S_NAMESPACE:-crossplane-system}"

# ── Command guard ─────────────────────────────────────────────────────────────

COMMAND="${1:-}"
if [[ "$COMMAND" != "up" && "$COMMAND" != "down" ]]; then
  echo "Usage: bash scripts/poc.sh <up|down>"
  exit 1
fi

# ── Prerequisites ─────────────────────────────────────────────────────────────

check_cmd() {
  command -v "$1" &>/dev/null || {
    echo "ERROR: '$1' not found. Install it and retry."
    exit 1
  }
}

check_cmd confluent
check_cmd kubectl
check_cmd helm
check_cmd jq

if ! confluent environment list --output json &>/dev/null; then
  echo "ERROR: Not logged in to Confluent Cloud. Run: confluent login --save"
  exit 1
fi


# ── up ────────────────────────────────────────────────────────────────────────

cmd_up() {
  if ! kubectl cluster-info &>/dev/null; then
    echo "ERROR: kubectl cannot reach a cluster."
    echo "       Run: kind create cluster --name kafka-claim-poc"
    exit 1
  fi

  # ── 1. Confluent Cloud ────────────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 1/7  Confluent Cloud"
  echo "════════════════════════════════════════════════"

  if [[ -f "$STATE_FILE" ]]; then
    echo "==> State file found — skipping Confluent Cloud (already provisioned)."
    echo "    Re-applying K8s stack (idempotent)."
    # shellcheck source=/dev/null
    source "$STATE_FILE"
  else
    echo "==> Creating Confluent Cloud environment: $ENV_NAME"
    ENV_OUTPUT=$(confluent environment create "$ENV_NAME" \
      --governance-package ESSENTIALS \
      --output json)
    ENV_ID=$(echo "$ENV_OUTPUT" | jq -r '.id')
    echo "    environment_id = $ENV_ID"

    echo "==> Creating Kafka cluster: $CLUSTER_NAME (Basic, SINGLE_ZONE, $AWS_REGION)"
    CLUSTER_OUTPUT=$(confluent kafka cluster create "$CLUSTER_NAME" \
      --environment "$ENV_ID" \
      --cloud aws \
      --region "$AWS_REGION" \
      --type basic \
      --output json)
    CLUSTER_ID=$(echo "$CLUSTER_OUTPUT" | jq -r '.id')
    echo "    cluster_id = $CLUSTER_ID"

    echo "==> Waiting for cluster to become UP (may take 2–3 minutes)..."
    while true; do
      STATUS=$(confluent kafka cluster describe "$CLUSTER_ID" \
        --environment "$ENV_ID" \
        --output json | jq -r '.status')
      echo "    status = $STATUS"
      [[ "$STATUS" == "UP" ]] && break
      sleep 15
    done

    echo "==> Creating service account: $SA_NAME"
    SA_OUTPUT=$(confluent iam service-account create "$SA_NAME" \
      --description "Crossplane provider-confluent POC — do not use manually" \
      --output json)
    SA_ID=$(echo "$SA_OUTPUT" | jq -r '.id')
    echo "    service_account_id = $SA_ID"

    echo "==> Assigning EnvironmentAdmin role to service account"
    confluent iam rbac role-binding create \
      --principal "User:${SA_ID}" \
      --role EnvironmentAdmin \
      --environment "$ENV_ID"

    echo "==> Creating Cloud API key for service account"
    KEY_OUTPUT=$(confluent api-key create \
      --resource cloud \
      --service-account "$SA_ID" \
      --description "crossplane-poc-cloud-key" \
      --output json)
    API_KEY=$(echo "$KEY_OUTPUT" | jq -r '.api_key // .key')
    API_SECRET=$(echo "$KEY_OUTPUT" | jq -r '.api_secret // .secret')
    API_KEY_ID="$API_KEY"

    echo "==> Saving state to $STATE_FILE"
    cat > "$STATE_FILE" <<STATE
ENV_ID=${ENV_ID}
CLUSTER_ID=${CLUSTER_ID}
SA_ID=${SA_ID}
API_KEY_ID=${API_KEY_ID}
STATE

    echo "==> Seeding Confluent credentials secret (namespace: $K8S_NAMESPACE)"
    # Use base64 + kubectl apply — --from-literal strips inner quotes in Git Bash on Windows.
    # tr -d '\n' removes line-wrapping added by base64 on some platforms.
    CRED_B64=$(printf '{"cloud_api_key":"%s","cloud_api_secret":"%s"}' \
      "${API_KEY}" "${API_SECRET}" | base64 | tr -d '\n')
    kubectl create namespace "$K8S_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    kubectl apply -f - <<YAML
apiVersion: v1
kind: Secret
metadata:
  name: confluent-credentials
  namespace: ${K8S_NAMESPACE}
type: Opaque
data:
  credentials: ${CRED_B64}
YAML

    echo ""
    echo "  Bootstrap endpoint:"
    confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" --output json \
      | jq -r '"    " + .endpoint'
  fi

  # ── 2. Crossplane + Providers ─────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 2/7  Crossplane + Providers"
  echo "════════════════════════════════════════════════"

  helm repo add crossplane-stable https://charts.crossplane.io/stable 2>/dev/null || true
  helm repo add kedacore           https://kedacore.github.io/charts 2>/dev/null || true
  helm repo add hashicorp          https://helm.releases.hashicorp.com 2>/dev/null || true
  helm repo add external-secrets   https://charts.external-secrets.io 2>/dev/null || true
  helm repo add argo               https://argoproj.github.io/argo-helm 2>/dev/null || true
  helm repo update

  # Pin to latest v2.x — major version is load-bearing: v2 requires Pipeline mode;
  # v1 has inline patches which are removed in v2.
  CROSSPLANE_VERSION=$(helm search repo crossplane-stable/crossplane --versions -o json \
    | jq -r '[.[] | select(.version | test("^2\\."))] | first | .version')
  echo "==> Installing Crossplane $CROSSPLANE_VERSION in namespace $K8S_NAMESPACE..."
  helm upgrade --install crossplane crossplane-stable/crossplane \
    --namespace "$K8S_NAMESPACE" --create-namespace \
    --version "$CROSSPLANE_VERSION" \
    --wait

  echo "==> Applying providers and function..."
  kubectl apply -f "${REPO_DIR}/crossplane/provider/providers.yaml"

  echo "==> Waiting for providers and function to become Healthy (up to 5 min each)..."
  kubectl wait providers.pkg.crossplane.io/provider-confluent \
    --for=condition=Healthy --timeout=300s
  kubectl wait providers.pkg.crossplane.io/provider-kubernetes \
    --for=condition=Healthy --timeout=300s
  kubectl wait functions.pkg.crossplane.io/function-patch-and-transform \
    --for=condition=Healthy --timeout=300s

  echo "==> Applying provider-kubernetes RBAC extension..."
  kubectl apply -f "${REPO_DIR}/crossplane/provider/provider-kubernetes-rbac.yaml"

  echo "==> Applying ProviderConfigs..."
  kubectl apply -f "${REPO_DIR}/crossplane/provider/providerconfigs.yaml"

  # ── 3. KEDA ───────────────────────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 3/7  KEDA"
  echo "════════════════════════════════════════════════"

  echo "==> Installing KEDA in namespace keda..."
  helm upgrade --install keda kedacore/keda \
    --namespace keda --create-namespace \
    --wait

  # ── 4. Vault (dev mode) ───────────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 4/7  Vault (dev mode)"
  echo "════════════════════════════════════════════════"

  echo "==> Installing Vault in namespace vault..."
  helm upgrade --install vault hashicorp/vault \
    --set server.dev.enabled=true \
    --namespace vault --create-namespace \
    --wait

  # ── 5. External Secrets Operator ──────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 5/7  External Secrets Operator"
  echo "════════════════════════════════════════════════"

  echo "==> Installing ESO in namespace external-secrets..."
  helm upgrade --install external-secrets external-secrets/external-secrets \
    --namespace external-secrets --create-namespace \
    --wait

  echo "==> Creating Vault token secret for ESO..."
  kubectl apply -f - <<'YAML'
apiVersion: v1
kind: Secret
metadata:
  name: vault-token
  namespace: external-secrets
type: Opaque
stringData:
  token: root
YAML

  echo "==> Creating ClusterSecretStore (Vault, KV v2)..."
  # Must use external-secrets.io/v1 — v1beta1 is served=false in current ESO release.
  kubectl apply -f - <<'YAML'
apiVersion: external-secrets.io/v1
kind: ClusterSecretStore
metadata:
  name: vault-cluster-store
spec:
  provider:
    vault:
      server: "http://vault.vault.svc.cluster.local:8200"
      path: "secret"
      version: "v2"
      auth:
        tokenSecretRef:
          name: vault-token
          key: token
          namespace: external-secrets
YAML

  # ── 6. XRD + Composition ──────────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 6/7  XRD + Composition"
  echo "════════════════════════════════════════════════"

  echo "==> Applying XRD..."
  kubectl apply -f "${REPO_DIR}/crossplane/composition/xrd.yaml"

  echo "==> Waiting for XRD to become Established..."
  kubectl wait xrd/xkafkatopics.platform.girishn.cloud \
    --for=condition=Established --timeout=60s

  echo "==> Applying Composition..."
  kubectl apply -f "${REPO_DIR}/crossplane/composition/composition.yaml"

  # ── 7. Argo CD ────────────────────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 7/7  Argo CD"
  echo "════════════════════════════════════════════════"

  echo "==> Installing Argo CD in namespace argocd..."
  # Bake argocd-cm settings into the Helm release at install time so pods start
  # with the correct configuration and no post-install patch + restart is needed.
  #
  # resource.exclusions includes both the standard K8s Endpoints entry (Argo CD
  # default, prevents noise from service endpoint churn) and the Crossplane
  # ProviderConfigUsage entry (prevents thousands of spurious OutOfSync alerts).
  helm upgrade --install argocd argo/argo-cd \
    --namespace argocd --create-namespace \
    --wait \
    --values - <<'EOF'
configs:
  cm:
    application.resourceTrackingMethod: annotation
    resource.exclusions: |
      - apiGroups:
        - ""
        kinds:
        - Endpoints
        clusters:
        - "https://kubernetes.default.svc"
      - apiGroups:
        - "*.crossplane.io"
        - "*.upbound.io"
        kinds:
        - ProviderConfigUsage
        clusters:
        - "*"
EOF

  echo "==> Applying Argo CD Application..."
  kubectl apply -f "${REPO_DIR}/gitops/argocd-app.yaml"

  # ── Summary ───────────────────────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " Done — POC stack provisioned"
  echo "════════════════════════════════════════════════"
  echo ""
  echo "  State file : $STATE_FILE"
  echo ""
  echo "  Next — push to git so Argo CD syncs the claim:"
  echo "    git push origin master"
  echo ""
  echo "  Or apply the claim directly (bypasses GitOps):"
  echo "    kubectl apply -f crossplane/claims/"
  echo ""
  echo "  Watch reconciliation:"
  echo "    kubectl get kafkatopiclaim -A"
  echo "    kubectl get managed -o wide"
  echo ""
  echo "  Tear everything down when done:"
  echo "    bash scripts/poc.sh down"
}

# ── down ──────────────────────────────────────────────────────────────────────

cmd_down() {
  if [[ ! -f "$STATE_FILE" ]]; then
    echo "ERROR: State file not found at $STATE_FILE."
    echo "       Nothing to destroy (or 'up' was never run)."
    exit 1
  fi

  # shellcheck source=/dev/null
  source "$STATE_FILE"

  if kubectl cluster-info &>/dev/null; then
    K8S_AVAILABLE=true
  else
    K8S_AVAILABLE=false
    echo ""
    echo "WARN: kubectl cannot reach a cluster — K8s teardown steps will be skipped."
    echo "      Confluent Cloud resources will still be destroyed."
    echo "      If the cluster still exists, fix the context first:"
    echo "        kubectl config use-context kind-kafka-claim-poc"
    echo "      then re-run 'bash scripts/poc.sh down' to clean up the K8s side."
  fi

  echo ""
  echo "==> Will destroy:"
  echo "    Confluent Cloud : env=$ENV_ID  cluster=$CLUSTER_ID  sa=$SA_ID  key=$API_KEY_ID"
  if [[ "$K8S_AVAILABLE" == "true" ]]; then
    echo "    Kubernetes      : Argo CD, Crossplane, KEDA, Vault, ESO + all managed namespaces"
  else
    echo "    Kubernetes      : SKIPPED (cluster unreachable)"
  fi
  echo ""
  read -r -p "Confirm full teardown? [y/N] " CONFIRM
  [[ "$CONFIRM" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }

  if [[ "$K8S_AVAILABLE" == "true" ]]; then
    # ── 1. Stop Argo CD from re-syncing during teardown ───────────────────────
    echo ""
    echo "==> [1/6] Removing Argo CD Application (prevents re-sync during teardown)..."
    kubectl delete application kafka-as-a-claim -n argocd \
      --ignore-not-found 2>/dev/null || true

    # ── 2. Delete claim and wait for full Crossplane cascade ──────────────────
    # --wait blocks until the claim finalizer clears, which requires the XR to be
    # gone, which requires all composed MRs and Objects to be deleted.
    # This is the correct gate: only proceed with provider/Crossplane uninstall
    # once all Object wrappers are confirmed gone — otherwise they get stuck
    # behind finalizers with no controller running to clear them.
    echo ""
    echo "==> [2/6] Deleting KafkaTopicClaim and waiting for full cascade (up to 5 min)..."
    kubectl delete kafkatopiclaims.platform.girishn.cloud orders-events \
      -n team-payments --ignore-not-found --wait --timeout=5m 2>/dev/null || true

    # Belt-and-suspenders: force-delete any stuck Confluent MRs.
    for CRD in kafkatopics serviceaccounts apikeys rolebindings; do
      FULL="${CRD}.confluent.crossplane.io"
      REMAINING=$(kubectl get "$FULL" --all-namespaces --no-headers 2>/dev/null \
        | grep -c '[a-z]' || true)
      if [ "${REMAINING:-0}" -gt 0 ]; then
        echo "    forcing deletion of remaining $FULL..."
        kubectl delete "$FULL" --all --all-namespaces \
          --ignore-not-found 2>/dev/null || true
      fi
    done

    # ── 3. Remove Crossplane composition layer ────────────────────────────────
    echo ""
    echo "==> [3/6] Removing Composition, XRD, ProviderConfigs, ClusterSecretStore..."
    kubectl delete -f "${REPO_DIR}/crossplane/composition/composition.yaml" \
      --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${REPO_DIR}/crossplane/composition/xrd.yaml" \
      --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${REPO_DIR}/crossplane/provider/providerconfigs.yaml" \
      --ignore-not-found 2>/dev/null || true
    kubectl delete clustersecretstore vault-cluster-store \
      --ignore-not-found 2>/dev/null || true
    kubectl delete secret vault-token -n external-secrets \
      --ignore-not-found 2>/dev/null || true

    # ── 4. Uninstall Helm releases ────────────────────────────────────────────
    echo ""
    echo "==> [4/6] Uninstalling Helm releases..."
    helm uninstall external-secrets -n external-secrets 2>/dev/null \
      || echo "    ESO: not installed, skipping"
    helm uninstall vault -n vault 2>/dev/null \
      || echo "    Vault: not installed, skipping"
    helm uninstall keda -n keda 2>/dev/null \
      || echo "    KEDA: not installed, skipping"
    helm uninstall crossplane -n "$K8S_NAMESPACE" 2>/dev/null \
      || echo "    Crossplane: not installed, skipping"
    helm uninstall argocd -n argocd 2>/dev/null \
      || echo "    Argo CD: not installed, skipping"

    # ── 5. Delete namespaces ──────────────────────────────────────────────────
    echo ""
    echo "==> [5/6] Deleting namespaces..."
    for NS in team-payments external-secrets vault keda argocd "$K8S_NAMESPACE"; do
      kubectl delete namespace "$NS" --ignore-not-found 2>/dev/null &
    done
    wait
    echo "    namespace deletions dispatched (finalizers may add a few more seconds)"
  else
    echo ""
    echo "==> [1-5/6] K8s teardown skipped (cluster unreachable)."
  fi

  # ── 6. Destroy Confluent Cloud ────────────────────────────────────────────
  echo ""
  echo "==> [6/6] Destroying Confluent Cloud resources..."

  echo "    deleting Cloud API key: $API_KEY_ID"
  confluent api-key delete "$API_KEY_ID" --force 2>/dev/null \
    || echo "    WARN: API key may already be deleted — continuing"

  echo "    removing EnvironmentAdmin role binding (SA: $SA_ID)"
  confluent iam rbac role-binding delete \
    --principal "User:${SA_ID}" \
    --role EnvironmentAdmin \
    --environment "$ENV_ID" \
    --force 2>/dev/null \
    || echo "    WARN: Role binding may already be removed — continuing"

  echo "    deleting service account: $SA_ID"
  confluent iam service-account delete "$SA_ID" --force 2>/dev/null \
    || echo "    WARN: Service account may already be deleted — continuing"

  echo "    deleting Kafka cluster: $CLUSTER_ID (takes ~1 min)"
  confluent kafka cluster delete "$CLUSTER_ID" \
    --environment "$ENV_ID" \
    --force 2>/dev/null \
    || echo "    WARN: Cluster may already be deleted — continuing"

  echo "    waiting for cluster deletion..."
  while confluent kafka cluster describe "$CLUSTER_ID" \
      --environment "$ENV_ID" --output json &>/dev/null 2>&1; do
    echo "    still deleting cluster..."
    sleep 15
  done
  echo "    cluster gone."

  echo "    deleting environment: $ENV_ID"
  confluent environment delete "$ENV_ID" --force 2>/dev/null \
    || echo "    WARN: Environment may already be deleted — continuing"

  echo "==> Removing state file..."
  rm -f "$STATE_FILE"

  echo ""
  echo "════════════════════════════════════════════════"
  echo " Done — full POC teardown complete"
  echo "════════════════════════════════════════════════"
  echo ""
  echo "  To delete the kind cluster itself:"
  echo "    kind delete cluster --name kafka-claim-poc"
}

# ── dispatch ──────────────────────────────────────────────────────────────────

case "$COMMAND" in
  up)   cmd_up   ;;
  down) cmd_down ;;
esac
