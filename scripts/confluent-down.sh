#!/usr/bin/env bash
# Tear down all Confluent Cloud resources created by confluent-up.sh,
# and remove the credentials secret from the kind cluster.
# Reads resource IDs from .confluent-state written by confluent-up.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${SCRIPT_DIR}/.confluent-state"

K8S_NAMESPACE="${K8S_NAMESPACE:-crossplane-system}"

# ── Prerequisites ────────────────────────────────────────────────────────────

check_cmd() { command -v "$1" &>/dev/null || { echo "ERROR: '$1' not found."; exit 1; }; }

check_cmd confluent
check_cmd kubectl

if ! confluent environment list --output json &>/dev/null; then
  echo "ERROR: Not logged in to Confluent Cloud. Run: confluent login"
  exit 1
fi

if [[ ! -f "$STATE_FILE" ]]; then
  echo "ERROR: State file not found at $STATE_FILE."
  echo "       Nothing to destroy (or confluent-up.sh was never run)."
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

# ── Destroy in reverse creation order ────────────────────────────────────────

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
echo ""
echo "  To tear down the S3 state bucket (if used):"
echo "    cd terraform/bootstrap && terraform destroy"
