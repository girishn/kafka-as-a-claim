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
#   docker                                       (for building consumer/producer images)
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
PRODUCER_SA_NAME="${PRODUCER_SA_NAME:-shipments-producer}"
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
check_cmd docker
check_cmd kind

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

  # ── 0. Build Docker images and load into kind ─────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 0/8  Docker images"
  echo "════════════════════════════════════════════════"

  echo "==> Building kafka-producer..."
  docker build -t kafka-producer:latest "${REPO_DIR}/apps/producer/"
  kind load docker-image kafka-producer:latest --name kafka-claim-poc

  echo "==> Building kafka-tracking-consumer..."
  docker build -t kafka-tracking-consumer:latest "${REPO_DIR}/apps/tracking-consumer/"
  kind load docker-image kafka-tracking-consumer:latest --name kafka-claim-poc

  echo "==> Building kafka-alerts-consumer..."
  docker build -t kafka-alerts-consumer:latest "${REPO_DIR}/apps/alerts-consumer/"
  kind load docker-image kafka-alerts-consumer:latest --name kafka-claim-poc

  # ── 1. Confluent Cloud ────────────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 1/8  Confluent Cloud"
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
      --output json 2>/dev/null) || \
    ENV_OUTPUT=$(confluent environment list --output json | \
      jq -r --arg n "$ENV_NAME" '[.[] | select(.name == $n)] | .[0]')
    ENV_ID=$(echo "$ENV_OUTPUT" | jq -r '.id')
    echo "    environment_id = $ENV_ID"

    echo "==> Creating Kafka cluster: $CLUSTER_NAME (Basic, SINGLE_ZONE, $AWS_REGION)"
    CLUSTER_OUTPUT=$(confluent kafka cluster create "$CLUSTER_NAME" \
      --environment "$ENV_ID" \
      --cloud aws \
      --region "$AWS_REGION" \
      --type basic \
      --output json 2>/dev/null) || \
    CLUSTER_OUTPUT=$(confluent kafka cluster list \
      --environment "$ENV_ID" --output json | \
      jq -r --arg n "$CLUSTER_NAME" '[.[] | select(.name == $n)] | .[0]')
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

    echo "==> Creating Crossplane service account: $SA_NAME"
    SA_OUTPUT=$(confluent iam service-account create "$SA_NAME" \
      --description "Crossplane provider-confluent POC — do not use manually" \
      --output json 2>/dev/null) || \
    SA_OUTPUT=$(confluent iam service-account list --output json | \
      jq -r --arg n "$SA_NAME" '[.[] | select(.display_name == $n or .name == $n)] | .[0]')
    SA_ID=$(echo "$SA_OUTPUT" | jq -r '.id')
    echo "    service_account_id = $SA_ID"

    echo "==> Assigning EnvironmentAdmin role to Crossplane SA (idempotent)"
    confluent iam rbac role-binding create \
      --principal "User:${SA_ID}" \
      --role EnvironmentAdmin \
      --environment "$ENV_ID" 2>/dev/null || true

    echo "==> Creating Cloud API key for Crossplane SA"
    KEY_OUTPUT=$(confluent api-key create \
      --resource cloud \
      --service-account "$SA_ID" \
      --description "crossplane-poc-cloud-key" \
      --output json)
    API_KEY=$(echo "$KEY_OUTPUT" | jq -r '.api_key // .key')
    API_SECRET=$(echo "$KEY_OUTPUT" | jq -r '.api_secret // .secret')
    API_KEY_ID="$API_KEY"

    echo "==> Getting Schema Registry cluster details..."
    # ESSENTIALS governance package includes Schema Registry at the environment level.
    _sr_describe() {
      confluent schema-registry cluster describe --environment "$ENV_ID" --output json 2>&1
    }
    _sr_extract_id() {
      # .cluster is the field in current Confluent CLI; keep fallbacks for older versions
      echo "$1" | jq -r '
        .cluster           // .cluster_id      // .id              //
        .official_cluster_id // .resource_id   //
        .schema_registry_cluster_id            //
        (if type == "array" then .[0].id else null end) //
        empty' 2>/dev/null
    }
    _sr_extract_endpoint() {
      echo "$1" | jq -r '.endpoint_url // .rest_endpoint // .url // empty' 2>/dev/null
    }

    SR_OUTPUT=$(_sr_describe)
    SR_ID=$(_sr_extract_id "$SR_OUTPUT")
    SR_REST_ENDPOINT=$(_sr_extract_endpoint "$SR_OUTPUT")

    if [[ -z "$SR_ID" || "$SR_ID" == "null" ]]; then
      echo "    WARN: Could not get SR cluster ID — raw response:"
      echo "$SR_OUTPUT" | jq '.' 2>/dev/null || echo "$SR_OUTPUT"
      echo "    Retrying in 30 seconds..."
      sleep 30
      SR_OUTPUT=$(_sr_describe)
      SR_ID=$(_sr_extract_id "$SR_OUTPUT")
      SR_REST_ENDPOINT=$(_sr_extract_endpoint "$SR_OUTPUT")
    fi

    if [[ -z "$SR_ID" || "$SR_ID" == "null" ]]; then
      echo "ERROR: Schema Registry cluster ID could not be determined after retry."
      echo "  Raw response:"
      echo "$SR_OUTPUT" | jq '.' 2>/dev/null || echo "$SR_OUTPUT"
      echo "  Hint: run 'confluent schema-registry cluster describe --environment $ENV_ID --output json'"
      echo "  and check which field holds the lsrc-... ID, then update _sr_extract_id in poc.sh."
      exit 1
    fi
    echo "    sr_cluster_id    = $SR_ID"
    echo "    sr_rest_endpoint = $SR_REST_ENDPOINT"

    echo "==> Creating Schema Registry API key for Crossplane SA"
    SR_KEY_OUTPUT=$(confluent api-key create \
      --resource "$SR_ID" \
      --environment "$ENV_ID" \
      --service-account "$SA_ID" \
      --description "crossplane-poc-sr-key" \
      --output json)
    SR_API_KEY=$(echo "$SR_KEY_OUTPUT" | jq -r '.api_key // .key')
    SR_API_SECRET=$(echo "$SR_KEY_OUTPUT" | jq -r '.api_secret // .secret')
    SR_API_KEY_ID="$SR_API_KEY"

    echo "==> Creating producer service account: $PRODUCER_SA_NAME"
    PRODUCER_SA_OUTPUT=$(confluent iam service-account create "$PRODUCER_SA_NAME" \
      --description "Shipments event producer — demo only" \
      --output json 2>/dev/null) || \
    PRODUCER_SA_OUTPUT=$(confluent iam service-account list --output json | \
      jq -r --arg n "$PRODUCER_SA_NAME" '[.[] | select(.display_name == $n or .name == $n)] | .[0]')
    PRODUCER_SA_ID=$(echo "$PRODUCER_SA_OUTPUT" | jq -r '.id')
    echo "    producer_sa_id = $PRODUCER_SA_ID"

    echo "==> Assigning CloudClusterAdmin to producer SA (idempotent)"
    confluent iam rbac role-binding create \
      --principal "User:${PRODUCER_SA_ID}" \
      --role CloudClusterAdmin \
      --environment "$ENV_ID" \
      --cloud-cluster "$CLUSTER_ID" 2>/dev/null || true

    echo "==> Creating Kafka API key for producer SA"
    PRODUCER_KEY_OUTPUT=$(confluent api-key create \
      --resource "$CLUSTER_ID" \
      --service-account "$PRODUCER_SA_ID" \
      --environment "$ENV_ID" \
      --description "shipments-producer-key" \
      --output json)
    PRODUCER_API_KEY=$(echo "$PRODUCER_KEY_OUTPUT" | jq -r '.api_key // .key')
    PRODUCER_API_SECRET=$(echo "$PRODUCER_KEY_OUTPUT" | jq -r '.api_secret // .secret')
    PRODUCER_API_KEY_ID="$PRODUCER_API_KEY"

    echo "==> Saving state to $STATE_FILE"
    cat > "$STATE_FILE" <<STATE
ENV_ID=${ENV_ID}
CLUSTER_ID=${CLUSTER_ID}
SA_ID=${SA_ID}
API_KEY_ID=${API_KEY_ID}
SR_ID=${SR_ID}
SR_REST_ENDPOINT=${SR_REST_ENDPOINT}
SR_API_KEY_ID=${SR_API_KEY_ID}
PRODUCER_SA_ID=${PRODUCER_SA_ID}
PRODUCER_API_KEY_ID=${PRODUCER_API_KEY_ID}
STATE

    echo "==> Seeding Confluent credentials secret (namespace: $K8S_NAMESPACE)"
    # Use base64 + kubectl apply — --from-literal strips inner quotes in Git Bash on Windows.
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

    echo "==> Seeding Schema Registry credentials secret (namespace: $K8S_NAMESPACE)"
    SR_KEY_B64=$(printf '%s' "${SR_API_KEY}" | base64 | tr -d '\n')
    SR_SECRET_B64=$(printf '%s' "${SR_API_SECRET}" | base64 | tr -d '\n')
    kubectl apply -f - <<YAML
apiVersion: v1
kind: Secret
metadata:
  name: confluent-sr-credentials
  namespace: ${K8S_NAMESPACE}
type: Opaque
data:
  sr_api_key: ${SR_KEY_B64}
  sr_api_secret: ${SR_SECRET_B64}
YAML

    echo "==> Creating team namespaces and seeding producer + kafka-connection..."
    kubectl create namespace team-logistics --dry-run=client -o yaml | kubectl apply -f -
    kubectl create namespace team-operations --dry-run=client -o yaml | kubectl apply -f -

    PROD_KEY_B64=$(printf '%s' "${PRODUCER_API_KEY}" | base64 | tr -d '\n')
    PROD_SECRET_B64=$(printf '%s' "${PRODUCER_API_SECRET}" | base64 | tr -d '\n')
    kubectl apply -f - <<YAML
apiVersion: v1
kind: Secret
metadata:
  name: producer-kafka-creds
  namespace: team-logistics
type: Opaque
data:
  api_key_id: ${PROD_KEY_B64}
  api_key_secret: ${PROD_SECRET_B64}
YAML

  fi

  # ── Patch Composition with current Confluent Cloud IDs ───────────────────
  # Runs whether this was a fresh provision or a state-file reload.
  # Keeps composition.yaml in sync so IDs never go stale after a reprovision.
  echo ""
  echo "==> Patching composition.yaml with current Confluent Cloud IDs..."
  CLUSTER_INFO=$(confluent kafka cluster describe "$CLUSTER_ID" \
    --environment "$ENV_ID" --output json)
  REST_ENDPOINT=$(echo "$CLUSTER_INFO" | jq -r '.rest_endpoint')
  BOOTSTRAP=$(echo "$CLUSTER_INFO" | jq -r '.endpoint' | sed 's|SASL_SSL://||')

  # Re-read SR info from state if we loaded from state file
  if [[ -z "${SR_REST_ENDPOINT:-}" ]]; then
    SR_OUTPUT=$(confluent schema-registry cluster describe \
      --environment "$ENV_ID" --output json)
    SR_ID=$(echo "$SR_OUTPUT" | jq -r '.cluster_id // .id')
    SR_REST_ENDPOINT=$(echo "$SR_OUTPUT" | jq -r '.endpoint_url // .rest_endpoint')
  fi

  COMPOSITION_FILE="${REPO_DIR}/crossplane/composition/composition.yaml"
  sed -i "s|env-[a-z0-9]*|${ENV_ID}|g"                                        "$COMPOSITION_FILE"
  sed -i "s|lkc-[a-z0-9]*|${CLUSTER_ID}|g"                                    "$COMPOSITION_FILE"
  sed -i 's|https://pkc-[^"]*\.confluent\.cloud:443|'"${REST_ENDPOINT}"'|g'   "$COMPOSITION_FILE"
  sed -i 's|pkc-[^"]*\.confluent\.cloud:9092|'"${BOOTSTRAP}"'|g'              "$COMPOSITION_FILE"
  sed -i "s|lsrc-[a-zA-Z0-9_-]*|${SR_ID}|g"                                  "$COMPOSITION_FILE"
  sed -i 's|https://psrc-[^"]*\.confluent\.cloud|'"${SR_REST_ENDPOINT}"'|g'   "$COMPOSITION_FILE"

  echo "    ENV_ID           = $ENV_ID"
  echo "    CLUSTER_ID       = $CLUSTER_ID"
  echo "    REST_ENDPOINT    = $REST_ENDPOINT"
  echo "    BOOTSTRAP        = $BOOTSTRAP"
  echo "    SR_ID            = $SR_ID"
  echo "    SR_REST_ENDPOINT = $SR_REST_ENDPOINT"

  # ── kafka-connection ConfigMaps ───────────────────────────────────────────
  # These ConfigMaps are created imperatively (not in git) so the consumer/producer
  # Deployments don't have to hardcode the bootstrap server.
  echo ""
  echo "==> Creating kafka-connection ConfigMaps in team namespaces..."
  kubectl create namespace team-logistics --dry-run=client -o yaml | kubectl apply -f -
  kubectl create namespace team-operations --dry-run=client -o yaml | kubectl apply -f -

  kubectl apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-connection
  namespace: team-logistics
data:
  BOOTSTRAP_SERVERS: "${BOOTSTRAP}"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-connection
  namespace: team-operations
data:
  BOOTSTRAP_SERVERS: "${BOOTSTRAP}"
YAML

  # ── 2. Crossplane + Providers ─────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 2/8  Crossplane + Providers"
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
  echo " 3/8  KEDA"
  echo "════════════════════════════════════════════════"

  echo "==> Installing KEDA in namespace keda..."
  helm upgrade --install keda kedacore/keda \
    --namespace keda --create-namespace \
    --wait

  # ── 4. Vault (dev mode) ───────────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 4/8  Vault (dev mode)"
  echo "════════════════════════════════════════════════"

  echo "==> Installing Vault in namespace vault..."
  helm upgrade --install vault hashicorp/vault \
    --set server.dev.enabled=true \
    --namespace vault --create-namespace \
    --wait

  # ── 5. External Secrets Operator ──────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 5/8  External Secrets Operator"
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

  # ── 6. Prometheus + Grafana ───────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 6/8  Prometheus + Grafana"
  echo "════════════════════════════════════════════════"

  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
  helm repo update

  echo "==> Installing kube-prometheus-stack in namespace monitoring..."
  helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
    --namespace monitoring --create-namespace \
    --set grafana.adminPassword=admin \
    --set "grafana.grafana\\.ini.security.allow_embedding=true" \
    --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
    --wait --timeout 10m

  echo "==> Installing KEDA Prometheus adapter for consumer lag metrics..."
  # KEDA exposes /metrics on port 8080; scrape via ServiceMonitor
  kubectl apply -f - <<'YAML'
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: keda-operator
  namespace: monitoring
  labels:
    release: prometheus
spec:
  namespaceSelector:
    matchNames:
      - keda
  selector:
    matchLabels:
      app: keda-operator
  endpoints:
    - port: metrics
      path: /metrics
      interval: 15s
YAML

  # ── 7. XRD + Composition ──────────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 7/8  XRD + Composition"
  echo "════════════════════════════════════════════════"

  echo "==> Applying XRD..."
  kubectl apply -f "${REPO_DIR}/crossplane/composition/xrd.yaml"

  echo "==> Waiting for XRD to become Established..."
  kubectl wait xrd/xkafkatopics.platform.girishn.cloud \
    --for=condition=Established --timeout=60s

  echo "==> Applying Composition..."
  kubectl apply -f "${REPO_DIR}/crossplane/composition/composition.yaml"

  # ── 8. Argo CD ────────────────────────────────────────────────────────────
  echo ""
  echo "════════════════════════════════════════════════"
  echo " 8/8  Argo CD"
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
  echo "  Next — push to git so Argo CD syncs the claims:"
  echo "    git push origin master"
  echo ""
  echo "  Argo CD UI (port-forward):"
  echo "    kubectl port-forward svc/argocd-server -n argocd 8080:443"
  echo "    open https://localhost:8080"
  echo "    kubectl get secret argocd-initial-admin-secret -n argocd \\"
  echo "      -o jsonpath='{.data.password}' | base64 -d"
  echo ""
  echo "  Grafana UI (port-forward):"
  echo "    kubectl port-forward svc/prometheus-grafana -n monitoring 3000:80"
  echo "    open http://localhost:3000  (admin/admin)"
  echo ""
  echo "  Watch reconciliation:"
  echo "    kubectl get kafkatopiclaims -A"
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
  echo "    Confluent SR    : sr=$SR_ID  sr_key=${SR_API_KEY_ID:-n/a}"
  echo "    Producer SA     : sa=${PRODUCER_SA_ID:-n/a}  key=${PRODUCER_API_KEY_ID:-n/a}"
  if [[ "$K8S_AVAILABLE" == "true" ]]; then
    echo "    Kubernetes      : Argo CD, Crossplane, KEDA, Vault, ESO, Prometheus + all managed namespaces"
  else
    echo "    Kubernetes      : SKIPPED (cluster unreachable)"
  fi
  echo ""
  read -r -p "Confirm full teardown? [y/N] " CONFIRM
  [[ "$CONFIRM" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }

  if [[ "$K8S_AVAILABLE" == "true" ]]; then
    # ── 1. Stop Argo CD from re-syncing during teardown ───────────────────────
    echo ""
    echo "==> [1/7] Removing Argo CD Application (prevents re-sync during teardown)..."
    kubectl delete application kafka-as-a-claim -n argocd \
      --ignore-not-found 2>/dev/null || true

    # ── 2. Delete claims and wait for full Crossplane cascade ─────────────────
    # --wait blocks until the claim finalizer clears, which requires the XR to be
    # gone, which requires all composed MRs and Objects to be deleted.
    echo ""
    echo "==> [2/7] Deleting KafkaTopicClaims and waiting for full cascade (up to 5 min)..."
    kubectl delete kafkatopiclaims.platform.girishn.cloud shipment-tracking \
      -n team-logistics --ignore-not-found --wait --timeout=5m 2>/dev/null || true
    kubectl delete kafkatopiclaims.platform.girishn.cloud delivery-alerts \
      -n team-operations --ignore-not-found --wait --timeout=5m 2>/dev/null || true

    # Belt-and-suspenders: strip finalizers then delete any stuck Confluent MRs.
    # Patch-first is required because the KafkaTopic needs APIKey credentials to
    # call Confluent's REST API on deletion — but the APIKey is deleted first in
    # the cascade, leaving the KafkaTopic finalizer permanently stuck.
    for CRD in kafkatopics serviceaccounts apikeys rolebindings schemas; do
      FULL="${CRD}.confluent.crossplane.io"
      REMAINING=$(kubectl get "$FULL" --all-namespaces --no-headers 2>/dev/null \
        | grep -c '[a-z]' || true)
      if [ "${REMAINING:-0}" -gt 0 ]; then
        echo "    stripping finalizers and deleting remaining $FULL..."
        kubectl get "$FULL" --all-namespaces -o name 2>/dev/null \
          | xargs -I{} kubectl patch {} --type=merge \
              -p '{"metadata":{"finalizers":[]}}' 2>/dev/null || true
        kubectl delete "$FULL" --all --all-namespaces \
          --ignore-not-found 2>/dev/null || true
      fi
    done

    # ── 3. Remove Crossplane composition layer ────────────────────────────────
    echo ""
    echo "==> [3/7] Removing Composition, XRD, ProviderConfigs, ClusterSecretStore..."
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
    kubectl delete secret confluent-sr-credentials -n "$K8S_NAMESPACE" \
      --ignore-not-found 2>/dev/null || true

    # ── 4. Uninstall Helm releases ────────────────────────────────────────────
    echo ""
    echo "==> [4/7] Uninstalling Helm releases..."
    helm uninstall prometheus -n monitoring 2>/dev/null \
      || echo "    Prometheus: not installed, skipping"
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
    echo "==> [5/7] Deleting namespaces..."
    for NS in team-logistics team-operations external-secrets vault keda argocd monitoring "$K8S_NAMESPACE"; do
      kubectl delete namespace "$NS" --ignore-not-found 2>/dev/null &
    done
    wait
    echo "    namespace deletions dispatched (finalizers may add a few more seconds)"
  else
    echo ""
    echo "==> [1-5/7] K8s teardown skipped (cluster unreachable)."
  fi

  # ── 6. Destroy Confluent Cloud ────────────────────────────────────────────
  echo ""
  echo "==> [6/7] Destroying Confluent Cloud resources..."

  echo "    deleting SR API key: ${SR_API_KEY_ID:-n/a}"
  confluent api-key delete "${SR_API_KEY_ID:-}" --force 2>/dev/null \
    || echo "    WARN: SR API key may already be deleted — continuing"

  echo "    deleting producer Kafka API key: ${PRODUCER_API_KEY_ID:-n/a}"
  confluent api-key delete "${PRODUCER_API_KEY_ID:-}" --force 2>/dev/null \
    || echo "    WARN: Producer API key may already be deleted — continuing"

  echo "    deleting Crossplane Cloud API key: $API_KEY_ID"
  confluent api-key delete "$API_KEY_ID" --force 2>/dev/null \
    || echo "    WARN: API key may already be deleted — continuing"

  echo "    removing EnvironmentAdmin role binding (Crossplane SA: $SA_ID)"
  confluent iam rbac role-binding delete \
    --principal "User:${SA_ID}" \
    --role EnvironmentAdmin \
    --environment "$ENV_ID" \
    --force 2>/dev/null \
    || echo "    WARN: Role binding may already be removed — continuing"

  echo "    deleting producer service account: ${PRODUCER_SA_ID:-n/a}"
  confluent iam service-account delete "${PRODUCER_SA_ID:-}" --force 2>/dev/null \
    || echo "    WARN: Producer SA may already be deleted — continuing"

  echo "    deleting Crossplane service account: $SA_ID"
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

  # ── 7. Cleanup ────────────────────────────────────────────────────────────
  echo ""
  echo "==> [7/7] Removing state file..."
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
