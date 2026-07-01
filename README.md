# Kafka as a Claim

A Platform Engineering POC demonstrating declarative, GitOps-driven Kafka topic provisioning on Confluent Cloud — with zero Terraform or Confluent console access required by app teams.

---

## The Problem

App teams that need a Kafka topic today must either:
- Open a ticket → wait for a platform engineer to run Terraform
- Get direct Confluent Cloud access → risk misconfiguration, no governance

Neither scales. Neither is self-service.

---

## The Solution

An app team submits a short YAML claim via git PR:

```yaml
apiVersion: platform.girishn.cloud/v1alpha1
kind: KafkaTopicClaim
metadata:
  name: shipment-tracking
  namespace: team-logistics
spec:
  topicName: shipments.events
  partitions: 6
  consumerName: tracking-consumer
  valueSchema: shipments.events-value
  schemaBody: >-
    {"type":"record","name":"ShipmentEvent","namespace":"com.logistics.events","fields":[{"name":"shipment_id","type":"string"},{"name":"status","type":"string"},{"name":"origin","type":"string"},{"name":"destination","type":"string"},{"name":"carrier","type":"string"},{"name":"timestamp","type":"long"}]}
```

A Kubernetes control loop — driven by Crossplane + Argo CD — turns that claim into:

| What | Where |
|------|-------|
| Kafka topic (`shipments.events`, 6 partitions) | Confluent Cloud |
| Consumer service account + scoped RBAC | Confluent Cloud |
| Kafka API key for the consumer | Confluent Cloud |
| Avro schema registered in Schema Registry | Confluent Cloud |
| KEDA `ScaledObject` (max replicas = partition count) | Kubernetes |
| `NetworkPolicy` for egress to Confluent | Kubernetes |
| `PodDisruptionBudget` for the consumer | Kubernetes |
| Kafka credentials in the app namespace | Kubernetes (via ESO + Vault) |

**Commit = provision. Revert = full teardown.** No `kubectl apply`, no Terraform, no Confluent console.

---

## Before vs After

| Metric | Before | After |
|--------|--------|-------|
| **Onboarding time** — time to first working workload | 3–5 days (Terraform, RBAC, YAML, reviews) | < 10 minutes (submit claim, git push, done) |
| **RBAC coverage** — % workloads with scoped bindings | Inconsistent — some topics wide-open | 100% — enforced by Composition |
| **Config drift** — time to detect and correct | Silent until incident — manual console edits linger | Next reconcile loop — corrected within seconds |
| **MTTR for Kafka issues** — mean time to resolve | Hours — no trace context, log hunting | Minutes — lag trend + distributed trace |
| **Platform team toil** — tickets per new workload | 3–7 tickets (topic, RBAC, secrets, review) | 0 tickets — fully self-service |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  App Team                                                    │
│                                                             │
│   git PR  →  KafkaTopicClaim YAML  →  merge to master      │
└──────────────────────────┬──────────────────────────────────┘
                           │ git push
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Argo CD  (crossplane/claims/ → syncs to cluster)           │
└──────────────────────────┬──────────────────────────────────┘
                           │ kubectl apply
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Crossplane (Composition)                                    │
│                                                             │
│   provider-confluent          provider-kubernetes           │
│   ├── KafkaTopic              ├── KEDA ScaledObject         │
│   ├── ServiceAccount          │     maxReplicas ← partition │
│   ├── RoleBinding             │     count (live patch)      │
│   └── APIKey ──────────┐      ├── NetworkPolicy             │
│                        │      ├── PodDisruptionBudget        │
│                        │      ├── ESO PushSecret            │
│                        │      └── ESO ExternalSecret        │
└────────────────────────│─────────────────────────────────────┘
                         │ connection secret
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  ESO + Vault                                                │
│                                                             │
│  crossplane-system secret → Vault KV → team-logistics secret │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  App Deployment (team-logistics namespace)                   │
│                                                             │
│  env:                                                       │
│    - name: KAFKA_API_KEY                                    │
│      valueFrom:                                             │
│        secretKeyRef:                                        │
│          name: tracking-consumer-kafka-creds                │
│          key: api_key_id                                    │
└─────────────────────────────────────────────────────────────┘
```

---

## Stack

| Component | Role |
|-----------|------|
| [Crossplane v2](https://crossplane.io) | Control plane — reconciles claims into cloud + K8s resources |
| [provider-confluent v1.0.0](https://github.com/crossplane-contrib/provider-confluent) | Manages Confluent Cloud resources (topic, SA, RBAC, API key) |
| [provider-kubernetes v0.17.0](https://github.com/crossplane-contrib/provider-kubernetes) | Creates K8s resources (KEDA, NetworkPolicy, PDB, ESO objects) |
| [function-patch-and-transform](https://github.com/crossplane-contrib/function-patch-and-transform) | Cross-provider field patching (partition count → ScaledObject) |
| [KEDA](https://keda.sh) | Consumer lag-based autoscaling |
| [Vault](https://vaultproject.io) (dev mode) | Secret backend |
| [External Secrets Operator](https://external-secrets.io) | Syncs Kafka credentials into app namespace |
| [Argo CD](https://argoproj.github.io/cd) | GitOps delivery — watches `crossplane/claims/` |

---

## Prerequisites

- [`kind`](https://kind.sigs.k8s.io) (or any local K8s cluster)
- [`confluent` CLI](https://docs.confluent.io/confluent-cli/current/install.html) — logged in (`confluent login`)
- `kubectl`, `helm`, `jq`
- A sandbox Confluent Cloud account

---

## Quick Start

```bash
# 1. Create a local cluster
kind create cluster --name kafka-claim-poc

# 2. Provision everything (Confluent Cloud + full K8s stack)
confluent login
bash scripts/poc.sh up
```

The script provisions in order:
0. Docker images built + loaded into kind (producer, tracking-consumer, alerts-consumer)
1. Confluent Cloud: environment, Basic cluster, Crossplane SA, Schema Registry API key, producer SA + API key
2. Crossplane v2 (pinned to latest stable v2.x)
3. provider-confluent + provider-kubernetes + function-patch-and-transform
4. KEDA
5. Vault (dev mode) + External Secrets Operator
6. Prometheus + Grafana (kube-prometheus-stack) + KEDA ServiceMonitor
7. XRD + Composition
8. Argo CD (annotation tracking + ProviderConfigUsage exclusions baked in)

Re-running `up` is safe — Helm uses `upgrade --install` and Confluent Cloud is skipped if already provisioned.

---

## Deploy a Claim

```bash
# Trigger GitOps sync (recommended)
git push origin master

# Or apply directly for debugging (bypasses Argo CD)
kubectl apply -f crossplane/claims/
```

Argo CD watches `crossplane/claims/` on branch `master`. A merged PR with a new `KafkaTopicClaim` is the production workflow.

---

## Verify

**Argo CD Application health:**
```bash
kubectl get application kafka-as-a-claim -n argocd
```

**Claim status:**
```bash
kubectl get kafkatopiclaims -A
```

**All managed resources:**
```bash
kubectl get managed -o wide
```

**Cross-provider patch — partition count flows from Confluent to KEDA:**
```bash
# Observed partition count on the XR (sourced from Confluent Cloud)
kubectl get xkafkatopic -o jsonpath='{.items[0].status.provisioned.partitionsCount}'

# ScaledObject maxReplicaCount (must match the above — live from Confluent, not hardcoded)
kubectl get scaledobject -n team-logistics tracking-consumer \
  -o jsonpath='{.spec.maxReplicaCount}'
```

**Secrets delivery — Kafka creds in app namespace:**
```bash
kubectl get secret tracking-consumer-kafka-creds -n team-logistics
```

**Consumer lag (KEDA scaling in action):**
```bash
# Watch KEDA scale the tracking-consumer up as producer outpaces it
kubectl get hpa -n team-logistics -w
# Or watch replicas directly
kubectl get deployment tracking-consumer -n team-logistics -w
```

**Grafana — consumer lag dashboard:**
```bash
kubectl port-forward svc/prometheus-grafana -n monitoring 3000:80
# open http://localhost:3000  (admin/admin)
# Query: keda_scaler_metrics_value{scaler="kafkaScaler"}
```

> **Note:** The KafkaTopic will fail transiently with `credentials.0.key is empty`
> for ~3–5 minutes while the APIKey provisions on Confluent Cloud. This is expected
> eventual consistency — Crossplane retries automatically.

---

## GitOps Teardown

Revert the claim commit → Argo CD prunes the `KafkaTopicClaim` → Crossplane
cascades and deletes all Confluent Cloud resources and Kubernetes objects.

To test this:
```bash
git revert HEAD --no-edit
git push origin master
# Watch Argo CD prune the claim and Crossplane tear down downstream resources
kubectl get managed -o wide -w
```

---

## Full Teardown

```bash
bash scripts/poc.sh down
```

Destroys in reverse order: claim cascade → Crossplane composition → Helm releases → namespaces → Confluent Cloud environment.

Works even if the local cluster is no longer reachable — Confluent Cloud resources are always cleaned up.

```bash
# Remove the kind cluster
kind delete cluster --name kafka-claim-poc
```

---

## Key Findings

See [`docs/FINDINGS.md`](docs/FINDINGS.md) for a full go/no-go assessment covering:

- **Confluent Cloud (provider-confluent):** Core resources work. Stream Governance absent. Basic tier blocks scoped RBAC — Standard/Dedicated required for production.
- **Cross-provider patching:** Confirmed. `KafkaTopic.status.atProvider.partitionsCount` → `XR.status` → `ScaledObject.spec.maxReplicaCount`. The claim is the single source of truth.
- **provider-kubernetes Object wrapping:** All 6 K8s resource types created and managed correctly.
- **ESO/Vault secrets handoff:** Confirmed end-to-end. App Deployment mounts a plain Secret with no Crossplane or Vault knowledge.
- **GitOps delivery (Argo CD):** Annotation-based tracking + ProviderConfigUsage exclusions prevent spurious OutOfSync.

---

## Production Evolution

| Concern | Solution |
|---------|----------|
| App teams writing raw YAML | Backstage Software Template → form → auto-generated PR |
| Basic tier RBAC | Standard/Dedicated tier → scoped `DeveloperRead`/`DeveloperWrite` per topic |
| XRD v1 deprecation | Migrate to v2 when Crossplane adds claim support |
| Cross-cutting platform concerns | Annotation-based controller (separate layer) |

---

## Demo Script

Five beats. Each beat has a before.

### 1. Show the old world
Open the repo — count the files an app team had to understand and touch before this platform existed: Terraform modules, Confluent provider config, RBAC rules, secret management, reviewer back-and-forth.

### 2. Show the claim
Open [`crossplane/claims/shipment-tracking.yaml`](crossplane/claims/shipment-tracking.yaml). A handful of fields: topic name, partitions, consumer name, Avro schema. Everything else — Confluent SA, RBAC, API key, Schema Registry registration, KEDA scaling, Vault push, secret delivery — is below the abstraction line. The app team never sees it.

### 3. Git push — show it land *(this is the beat that makes the room go quiet)*
```bash
git push origin master
```
Switch to the Argo CD UI — watch it sync. Switch to the Confluent Cloud console — the topic and RBAC appear live. No `kubectl apply`. No Terraform run. No ticket.

### 4. Break it manually — show it self-heal
In the Confluent Cloud console, manually edit the RoleBinding or delete the topic. Watch Crossplane detect the drift and correct it within seconds — without any human intervention.

### 5. Show the observability proof
Pull up the KEDA ScaledObject:
```bash
kubectl get scaledobject tracking-consumer -n team-logistics -o jsonpath='{.spec.maxReplicaCount}'
```
The value came from the live partition count on Confluent Cloud — not a hardcoded number. The claim is the single source of truth: change `spec.partitions`, and both the Confluent topic and the KEDA ceiling update together.

Then open Grafana at `http://localhost:3000`. The `keda_scaler_metrics_value` metric shows consumer lag in real time. The producer writes 10 events/sec; the tracking consumer processes 1/sec — lag builds, KEDA scales replicas up to the partition ceiling.
