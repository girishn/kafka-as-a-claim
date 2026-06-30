#!/usr/bin/env bash
# Provision or destroy Confluent Cloud POC resources.
#
# Usage:
#   bash scripts/confluent.sh up    — create env, cluster, SA, API key; seed K8s secret
#   bash scripts/confluent.sh down  — tear down all of the above in reverse order

set -euo pipefail

# ── Config ───────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${SCRIPT_DIR}/.confluent-state"

ENV_NAME="${CONFLUENT_ENV_NAME:-kafka-claim-poc}"
CLUSTER_NAME="${CONFLUENT_CLUSTER_NAME:-kafka-claim-poc-cluster}"
SA_NAME="${CONFLUENT_SA_NAME:-crossplane-poc}"
AWS_REGION="${AWS_REGION:-us-east-1}"
K8S_NAMESPACE="${K8S_NAMESPACE:-crossplane-system}"

# ── Usage guard ───────────────────────────────────────────────────────────────

COMMAND="${1:-}"
if [[ "$COMMAND" != "up" && "$COMMAND" != "down" ]]; then
  echo "Usage: bash scripts/confluent.sh <up|down>"
  exit 1
fi

# ── Prerequisites ─────────────────────────────────────────────────────────────

check_cmd() { command -v "$1" &>/dev/null || { echo "ERROR: '$1' not found. Install it and retry."; exit 1; }; }

check_cmd confluent
check_cmd kubectl
check_cmd jq

if ! confluent environment list --output json &>/dev/null; then
  echo "ERROR: Not logged in to Confluent Cloud. Run: confluent login"
  exit 1
fi

if ! kubectl cluster-info &>/dev/null; then
  echo "ERROR: kubectl cannot reach a cluster. Run: kind create cluster --name kafka-claim-poc"
  exit 1
fi

# ── up ────────────────────────────────────────────────────────────────────────

cmd_up() {
  if [[ -f "$STATE_FILE" ]]; then
    echo "ERROR: State file already exists at $STATE_FILE."
    echo "       Run 'bash scripts/confluent.sh down' first, or delete the state file if stale."
    exit 1
  fi

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

  echo "==> Seeding Crossplane ProviderConfig secret into cluster (namespace: $K8S_NAMESPACE)"
  # --from-literal is unreliable on Windows (Git Bash strips inner quotes).
  # Use base64 + kubectl apply to guarantee the JSON is stored verbatim.
  CRED_B64=$(printf '{"cloud_api_key":"%s","cloud_api_secret":"%s"}' "${API_KEY}" "${API_SECRET}" | base64 -w0)
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
  echo "✓ Done. Confluent Cloud resources created and secret seeded."
  echo ""
  echo "  Environment ID : $ENV_ID"
  echo "  Cluster ID     : $CLUSTER_ID"
  echo "  SA ID          : $SA_ID"
  echo "  Secret         : confluent-credentials (namespace: $K8S_NAMESPACE)"
  echo ""
  echo "  Bootstrap endpoint:"
  confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" --output json \
    | jq -r '"  " + .endpoint'
  echo ""
  echo "  Next: apply crossplane/provider/ manifests and proceed with the POC."
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

  echo "==> Will destroy the following Confluent Cloud resources:"
  echo "    environment  : $ENV_ID"
  echo "    cluster      : $CLUSTER_ID"
  echo "    service acct : $SA_ID"
  echo "    api key      : $API_KEY_ID"
  echo ""
  read -r -p "Confirm destroy? [y/N] " CONFIRM
  [[ "$CONFIRM" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }

  echo "==> Deleting Cloud API key: $API_KEY_ID"
  confluent api-key delete "$API_KEY_ID" --force 2>/dev/null || \
    echo "    WARN: API key may already be deleted — continuing."

  echo "==> Removing EnvironmentAdmin role binding from service account: $SA_ID"
  confluent iam rbac role-binding delete \
    --principal "User:${SA_ID}" \
    --role EnvironmentAdmin \
    --environment "$ENV_ID" \
    --force 2>/dev/null || \
    echo "    WARN: Role binding may already be removed — continuing."

  echo "==> Deleting service account: $SA_ID"
  confluent iam service-account delete "$SA_ID" --force 2>/dev/null || \
    echo "    WARN: Service account may already be deleted — continuing."

  echo "==> Deleting Kafka cluster: $CLUSTER_ID (this takes ~1 minute)"
  confluent kafka cluster delete "$CLUSTER_ID" \
    --environment "$ENV_ID" \
    --force 2>/dev/null || \
    echo "    WARN: Cluster may already be deleted — continuing."

  echo "==> Waiting for cluster deletion..."
  while confluent kafka cluster describe "$CLUSTER_ID" \
      --environment "$ENV_ID" --output json &>/dev/null 2>&1; do
    echo "    still deleting..."
    sleep 15
  done
  echo "    cluster gone."

  echo "==> Deleting environment: $ENV_ID"
  confluent environment delete "$ENV_ID" --force 2>/dev/null || \
    echo "    WARN: Environment may already be deleted — continuing."

  echo "==> Removing Crossplane credentials secret from cluster"
  kubectl delete secret confluent-credentials \
    --namespace "$K8S_NAMESPACE" \
    --ignore-not-found

  echo "==> Removing state file"
  rm -f "$STATE_FILE"

  echo ""
  echo "✓ Done. All Confluent Cloud POC resources destroyed."
  echo ""
  echo "  To tear down the kind cluster itself:"
  echo "    kind delete cluster --name kafka-claim-poc"
}

# ── dispatch ──────────────────────────────────────────────────────────────────

case "$COMMAND" in
  up)   cmd_up   ;;
  down) cmd_down ;;
esac
