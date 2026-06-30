#!/usr/bin/env bash
# Provision Confluent Cloud POC resources and seed credentials into the kind cluster.
# Run once before starting the Crossplane POC. Saves resource IDs to .confluent-state
# for use by confluent-down.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${SCRIPT_DIR}/.confluent-state"

ENV_NAME="${CONFLUENT_ENV_NAME:-kafka-claim-poc}"
CLUSTER_NAME="${CONFLUENT_CLUSTER_NAME:-kafka-claim-poc-cluster}"
SA_NAME="${CONFLUENT_SA_NAME:-crossplane-poc}"
AWS_REGION="${AWS_REGION:-us-east-1}"
K8S_NAMESPACE="${K8S_NAMESPACE:-crossplane-system}"

# ── Prerequisites ────────────────────────────────────────────────────────────

check_cmd() { command -v "$1" &>/dev/null || { echo "ERROR: '$1' not found. Install it and retry."; exit 1; }; }

check_cmd confluent
check_cmd kubectl
check_cmd jq

if ! confluent environment list --output json &>/dev/null; then
  echo "ERROR: Not logged in to Confluent Cloud. Run: confluent login"
  exit 1
fi

if kubectl config current-context &>/dev/null | grep -q "kafka-claim-poc" 2>/dev/null || \
   kubectl cluster-info &>/dev/null; then
  : # cluster reachable
else
  echo "ERROR: kubectl cannot reach a cluster. Run: kind create cluster --name kafka-claim-poc"
  exit 1
fi

if [[ -f "$STATE_FILE" ]]; then
  echo "ERROR: State file already exists at $STATE_FILE."
  echo "       Run confluent-down.sh first, or delete the state file if it is stale."
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
CREDENTIALS_JSON="{\"cloud_api_key\":\"${API_KEY}\",\"cloud_api_secret\":\"${API_SECRET}\"}"
kubectl create namespace "$K8S_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic confluent-credentials \
  --namespace "$K8S_NAMESPACE" \
  --from-literal=credentials="${CREDENTIALS_JSON}" \
  --dry-run=client -o yaml | kubectl apply -f -

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
