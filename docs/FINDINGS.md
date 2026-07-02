# POC Findings — Kafka-as-a-Claim

## Status: COMPLETE — all definition-of-done items validated

---

## Step 0: CRD Discovery

**provider-confluent v1.0.0** — 12 CRDs installed. Confirmed resources:

| CRD | Kind | Present |
|-----|------|---------|
| `apikeys.confluent.crossplane.io` | `APIKey` | ✅ |
| `kafkatopics.confluent.crossplane.io` | `KafkaTopic` | ✅ |
| `rolebindings.confluent.crossplane.io` | `RoleBinding` | ✅ |
| `serviceaccounts.confluent.crossplane.io` | `ServiceAccount` | ✅ |
| `schemas.confluent.crossplane.io` | `Schema` | ✅ |
| `environments.confluent.crossplane.io` | `Environment` | ✅ |
| `clusters.confluent.crossplane.io` | `Cluster` | ✅ |

**Stream Governance (tags, tag bindings, business metadata): ABSENT.** No CRDs for Confluent Stream Governance in provider-confluent v1.0.0. If Stream Governance tagging is needed, it requires a separate out-of-band integration (Confluent REST API via a custom controller, or Terraform). This is a gap; document it before recommending production adoption.

**Schema Registry (`schemas.confluent.crossplane.io`): EXISTS and IMPLEMENTED.** The CRD is present and wired up in the prototype.

| Concern | Solution |
|---------|---------|
| SR cluster ID (`lsrc-…`) | Auto-patched by `poc.sh up` via `sed` |
| SR-scoped API key | Created by `poc.sh up` for the `crossplane-poc` SA; seeded as `confluent-sr-credentials` in `crossplane-system` |
| SR credentials in Composition | `credentials` block in the `Schema` resource — same pattern as `credentials` in `KafkaTopic` |
| Schema body | `spec.schemaBody` field in the claim (Avro JSON string) |
| Subject name | `spec.valueSchema` field in the claim (e.g., `shipments.events-value`) |

Confirmed CRD field names: `subjectName`, `format`, `schema`, `schemaRegistryCluster[].id`, `restEndpoint`, `credentials[].keySecretRef` / `secretSecretRef`.

---

## Confluent-Side Findings

### RBAC on Basic Tier: Resource Roles Not Supported

**Finding (HIGH):** `DeveloperRead` and `DeveloperWrite` are resource-level roles and are **not supported on Basic tier Kafka clusters**. Attempt:
```
Error: Forbidden - Basic Clusters can not use resource roles
```
The Composition uses `CloudClusterAdmin` (cluster-scoped role) as a workaround. This is overly permissive — it grants full cluster admin to the consumer service account.

**Production implication:** Scoped per-topic RBAC requires Standard or Dedicated tier. The Composition's `roleName` field is the only change needed — the RoleBinding's `crnPattern` for `DeveloperRead` on a specific topic:
```
crn://confluent.cloud/organization=<org-id>/environment=<env-id>/cloud-cluster=<cluster-id>/kafka=<cluster-id>/topic=<topicName>
```
with a second RoleBinding for the consumer group:
```
crn://confluent.cloud/organization=<org-id>/environment=<env-id>/cloud-cluster=<cluster-id>/kafka=<cluster-id>/group=<consumerName>-*
```

### Service Accounts Are Organization-Scoped, Not Environment-Scoped

**Finding (OPERATIONAL):** Confluent Cloud ServiceAccount display names must be unique within an **org**, not an environment. On `poc.sh down` + `poc.sh up` cycles, if the prior run's SAs were not cleanly deleted from Confluent Cloud, the new Composition fails with:
```
409 Conflict: Unable to create service account for organization: Service name is already in use.
```
This blocks the APIKey, which blocks the KafkaTopic, which blocks everything downstream.

**Root cause:** `poc.sh down`'s belt-and-suspenders step strips Kubernetes finalizers from stuck Crossplane managed resources. Stripping a finalizer prevents the provider from calling Confluent's delete API — the K8s object disappears but the Confluent Cloud SA remains orphaned.

**Fix implemented:** `poc.sh down` now captures SA display names from K8s managed resources *before* stripping finalizers and explicitly deletes them from Confluent Cloud afterward via `confluent iam service-account delete`. Same pattern applied to topic names.

**Production implication:** In production, cluster IDs and claim names are stable — you don't reprovision the same environment repeatedly. This is a POC lifecycle concern, not a production concern. Use unique `consumerName` values per environment if running multiple non-prod clusters against the same Confluent org.

### KafkaTopic Cascade Delete Bug

**Finding (HIGH — teardown reliability):** The KafkaTopic managed resource uses the APIKey's connection secret (`<xr-name>-kafka-apikey`) to authenticate to Confluent's REST API when deleting the topic. In the Crossplane cascade delete sequence, the APIKey is deleted before the KafkaTopic. Once the APIKey is gone, its connection secret is also gone. The KafkaTopic's deletion call fails (no credentials), its finalizer gets permanently stuck, and the claim never finishes deleting.

**Workaround in `poc.sh down`:** After the 5-minute claim deletion wait, a belt-and-suspenders loop strips finalizers from any remaining Confluent CRD resources and deletes the K8s objects. The Confluent Cloud topic would remain orphaned — so `poc.sh down` also explicitly deletes orphaned topics via `confluent kafka topic delete`.

**Production implication:** This is a provider-confluent ordering/dependency bug. In production, consider using the ProviderConfig Cloud API key for topic deletion instead of the per-topic Kafka API key — that would not have a chicken-and-egg problem. File with provider-confluent upstream. As a workaround, explicitly delete topics during cluster teardown before deleting the environment.

### APIKey Credential Chicken-and-Egg (Transient)

The KafkaTopic requires the APIKey connection secret before it can authenticate to Confluent. Since both are created by the same Composition simultaneously, the KafkaTopic reconcile loop transiently fails with `credentials.0.key is empty` for ~3–5 minutes while the APIKey provisions. Crossplane retries automatically. This is expected eventual consistency, not a bug.

**APIKey connection secret keys confirmed:** `api_key_id`, `api_key_secret`, `attribute.secret`. KafkaTopic credentials reference `api_key_id` (key) and `api_key_secret` (secret).

### SR API Key Requires `--environment` Flag

`confluent api-key create --resource <sr-cluster-id>` requires `--environment <env-id>` for Schema Registry resources (SR resources are environment-scoped). Omitting it returns `Resource Not Found`. Fixed in `poc.sh`.

### SR Cluster ID Field Name

Current Confluent CLI returns the SR cluster ID as `.cluster` in `confluent schema-registry cluster describe --output json`. Earlier versions used `.cluster_id` or `.id`. The `_sr_extract_id()` function in `poc.sh` tries `.cluster` first with fallbacks for older CLI versions.

### Hardcoded Confluent IDs in Composition

`composition.yaml` embeds ENV_ID, CLUSTER_ID, REST endpoint, bootstrap server, SR cluster ID, and SR REST endpoint as literal strings. These go stale after every `poc.sh down` + `poc.sh up` cycle. `poc.sh up` now automatically patches all values into `composition.yaml` via `sed` before `kubectl apply`.

**Production implication:** Cluster IDs are stable in production — this is a POC-only concern.

### Cross-Resource References via `matchControllerRef`

Both `principalSelector.matchControllerRef: true` (RoleBinding → SA) and `idSelector.matchControllerRef: true` (APIKey → SA) work correctly. Crossplane sets a shared controller reference on all composed resources owned by the same XR, making selector-based cross-resource resolution reliable without hardcoding names.

---

## Cross-Provider Patch: Confirmed Working ✅

This is the POC's primary validation target.

**Patch path (confirmed end-to-end):**
```
KafkaTopic.status.atProvider.partitionsCount: 6
  → ToCompositeFieldPath (policy: Optional)
  → XKafkaTopic.status.provisioned.partitionsCount: 6
  → FromCompositeFieldPath (policy: Optional)
  → ScaledObject.spec.maxReplicaCount: 6
```

Confirmed: `kubectl get scaledobject tracking-consumer -n team-logistics -o jsonpath='{.spec.maxReplicaCount}'` returned `6`. The tracking-consumer Deployment scaled from 1 → 6/6 replicas under KEDA. The claim is the single source of truth — if `spec.partitions` changes in the claim, both the Confluent topic partition count and the KEDA ScaledObject's ceiling update together.

**Implementation detail:** The ScaledObject receives two passes from the same function invocation:
1. Pass A — immediate patch from `spec.partitions` (claim) so the ScaledObject is created with a correct value before the Topic is ready
2. Pass B — live patch from `status.provisioned.partitionsCount` (Optional) once the Topic reconciles; overwrites Pass A with the observed value

Both passes produce the same value under normal conditions. They would diverge if Confluent rejected or adjusted the partition count — in which case the observed value wins.

---

## provider-kubernetes Object Wrapping: Confirmed Working ✅

All six Kubernetes-side resources created and confirmed Ready across two claims (two namespaces):

| Resource | Status | Notes |
|----------|--------|-------|
| KEDA ScaledObject | ✅ Ready | `maxReplicaCount` patched from live Confluent partition count |
| KEDA TriggerAuthentication | ✅ Ready | References ESO-vended `<consumerName>-kafka-creds` Secret |
| NetworkPolicy | ✅ Ready | Placeholder — see DNS finding below |
| PodDisruptionBudget | ✅ Ready | `minAvailable: 1` enforced |
| ESO PushSecret | ✅ Synced | Confluent APIKey connection secret → Vault |
| ESO ExternalSecret | ✅ Synced | Vault → `<consumerName>-kafka-creds` in team namespace |

**Object CRD version:** `kubernetes.crossplane.io/v1alpha2` (provider-kubernetes v0.17.0).

### NetworkPolicy Placeholder Blocked Consumer DNS

**Finding:** The placeholder NetworkPolicy only allows TCP/9092 egress. Kubernetes DNS runs on UDP/53. Consumer pods (selected by `app: <consumerName>`) had all DNS resolution blocked, causing `Temporary failure in name resolution` on the Confluent bootstrap host. The producer pod used a different label and was unaffected.

**Fix:** Added UDP/53 egress rule to the placeholder NetworkPolicy in the Composition.

**Production implication:** This is a placeholder-specific bug, not a fundamental concern. A real PSC NetworkPolicy would be authored with proper DNS egress from the start. The fix demonstrates that the Object wrapping pattern correctly propagates NetworkPolicy changes — after `kubectl apply -f composition.yaml`, the live NetworkPolicy updated within seconds.

### KEDA ScaledObject Stale Error State

KEDA caches reconcile errors. When the ScaledObject initially failed (consumer secret not yet present), the error was not cleared automatically after the secret became available. Fix: annotate the ScaledObject to trigger a fresh reconcile:
```bash
kubectl annotate scaledobject <name> -n <ns> reconcile=$(date +%s) --overwrite
```

### KEDA Auth for Confluent Cloud SASL_SSL

Correct values for Confluent Cloud SASL/PLAIN over TLS (port 9092):
```yaml
sasl: plaintext   # SASL mechanism = PLAIN (not the protocol string)
tls: enable       # TLS is mandatory on Confluent Cloud
```
Not `sasl: sasl_ssl` + `tls: none` — that combination is wrong and silently fails.

---

## ESO/Vault Secrets Handoff: Confirmed Working ✅

### Validated end-to-end path (two claims, two namespaces)

```
Crossplane APIKey connection secret (crossplane-system/<xr-name>-kafka-apikey)
  → PushSecret (crossplane-system) → Vault (dev mode) at kafka/<xr-name>
  → ExternalSecret (<team-namespace>) → K8s Secret <consumerName>-kafka-creds
     keys: api_key_id, api_key_secret
```

App team Deployment mounts `<consumerName>-kafka-creds` as a plain Secret. No Crossplane, Vault, or ESO knowledge required.

### Timing: PushSecret Takes ~5–10 Minutes on First Provision

PushSecret initially errors with "could not get source secret" then "secret key api_key_id does not exist" — both transient while provider-confluent is provisioning the APIKey in Confluent Cloud. Once the APIKey connection secret is fully populated, PushSecret retries and succeeds. Total time from claim apply to secrets available: ~10–15 minutes on first provision (dominated by APIKey provisioning time).

### ExternalSecret 1-Hour Refresh Interval

The ExternalSecret refresh interval is set to `1h` in the Composition. If the PushSecret syncs to Vault while the ExternalSecret is between polls, the K8s Secret won't appear until the next poll — up to 1 hour later. Force a resync:
```bash
kubectl annotate externalsecret <name> -n <ns> \
  reconcile.external-secrets.io/force-sync=$(date +%s) --overwrite
```
For production, consider a shorter refresh interval (e.g., `5m`) or a triggered resync via webhook after the PushSecret syncs.

### ESO API Version Changes

| Resource | Version Used | Notes |
|----------|-------------|-------|
| ExternalSecret | `external-secrets.io/v1` | `v1beta1` served=false in current ESO |
| PushSecret | `external-secrets.io/v1alpha1` | `secretStoreRef` → `secretStoreRefs` (array) |
| ClusterSecretStore | `external-secrets.io/v1` | |

### Secrets Format: `confluent-credentials`

The Confluent ProviderConfig expects:
```json
{"cloud_api_key":"<key>","cloud_api_secret":"<secret>"}
```
as base64 in a single `credentials` key. `poc.sh` uses base64 + `kubectl apply -f -` — `--from-literal` on Windows (Git Bash) strips inner quotes from JSON strings.

---

## GitOps Delivery: Confirmed Working ✅

**Commit → apply:** `KafkaTopicClaim` applied via Argo CD from `git push` with no manual `kubectl apply`. Crossplane Composition cascaded to all Confluent Cloud + Kubernetes resources.

**Revert → teardown:** Removing claim files from git + push caused Argo CD to prune the claims. Crossplane cascaded deletion through all composed managed resources. All Confluent Cloud resources (topics, SAs, APIKeys, RoleBindings) deleted. All Kubernetes-side resources (ScaledObjects, NetworkPolicies, PDBs, ESO objects) deleted.

**Boundary: Composition-owned vs. app team resources.** The Composition owns Confluent Cloud resources + platform K8s resources (KEDA, NetworkPolicy, PDB, ESO). The app team's Deployment manifests are separate gitops artifacts in `crossplane/claims/` — they are **not** owned by the Composition and are not pruned when the claim is removed. This is the correct and intended boundary: platform resources cascade with the claim; app workloads have their own lifecycle. In production, app team Deployments would live in a separate path, making this separation explicit.

**Argo CD configuration confirmed:**
- `application.resourceTrackingMethod: annotation` ✅
- `ProviderConfigUsage` exclusion — no spurious OutOfSync from Crossplane-generated objects ✅
- Drift detection scoped to `crossplane/claims/` only — managed resources in-cluster, never in git ✅
- `argocd-cm` settings baked into Helm at install time — no post-install patch or restart ✅

---

## Crossplane v2 Changes

### XRD v1 Deprecated; v2 Does Not Support Claims

`apiextensions.crossplane.io/v2` rejects `claimNames`:
```
The CompositeResourceDefinition "xkafkatopics.platform.girishn.cloud" is invalid:
spec: Invalid value: Claims aren't supported in apiextensions.crossplane.io/v2
```
The POC uses XRD `v1` (deprecated but functional). The claim model in v2 has changed — investigate the v2 migration path before production adoption.

### Composition Must Use `spec.mode: Pipeline`

Crossplane v2 removed inline `patches:` from Compositions. `spec.mode: Pipeline` + `function-patch-and-transform` is the only supported patching mechanism.

---

## XRD Versioning and Production Evolution

Current XRD version is `v1alpha1`. Any field changes require version bumps with conversion webhooks. Plan versioning policy before production:
- Keep `v1alpha1` for the POC
- Define `v1beta1` when the schema stabilises
- Use Crossplane's built-in conversion (or a webhook) for field renaming

## Backstage Integration Path

The app team claim is a 5-line YAML file. At scale, generating these via git PR is cumbersome. The natural production evolution:
- Backstage Software Template (form → auto-generated claim PR via scaffolder)
- Template renders `KafkaTopicClaim` YAML and opens a PR to `crossplane/claims/`
- Argo CD auto-syncs on merge
- Full flow: form submit → PR review → merge → topic provisioned — zero kubectl

## Annotation-Based Platform Controller (Out of Scope)

Cross-cutting platform behaviors (OIDC sidecar, Prometheus scraping, liveness probes, network egress policies, resource quotas) are better expressed as annotations on the app team's existing Kubernetes resources than as additional claim fields. A lightweight mutating webhook or controller reads these and injects the corresponding platform config. This is the natural layer above Claims — Claims own resource lifecycle, annotations own runtime configuration injection. Out of scope for this POC; the boundary is here.

---

## Go/No-Go Assessment

| Area | Verdict | Notes |
|------|---------|-------|
| **Confluent Cloud (provider-confluent)** | ✅ GO | Topic, SA, APIKey, RoleBinding, Schema all work. Cascade delete bug requires teardown workaround (fixed in `poc.sh`). SA org-scoping requires unique naming per org — not per environment. |
| **Cross-provider patching** | ✅ GO | Confirmed: `KafkaTopic.status.atProvider.partitionsCount` → XR status → `ScaledObject.spec.maxReplicaCount`. Tracking-consumer scaled 1→6 under live KEDA lag. Claim is the single source of truth. |
| **provider-kubernetes Object wrapping** | ✅ GO | All 6 K8s resource types created and managed correctly. NetworkPolicy DNS gap was a placeholder bug, not a framework limitation. |
| **ESO/Vault secrets handoff** | ✅ GO | Full chain confirmed across two claims and two namespaces. App team mounts a plain Secret with no Crossplane or Vault knowledge. 10–15 min first-provision lag is a Confluent APIKey provisioning delay, not an ESO limitation. |
| **GitOps delivery (Argo CD)** | ✅ GO | Commit → sync → resources and revert → prune → cascade delete both confirmed. Annotation tracking + ProviderConfigUsage exclusions confirmed. |
| **RBAC scoping** | ⚠️ CONDITION | Basic tier blocks resource-level roles. CloudClusterAdmin is overly permissive. Standard/Dedicated tier required for production scoped RBAC. |
| **Crossplane v2 XRD claims** | ⚠️ RISK | v2 drops claims; must use deprecated v1. Monitor Crossplane v2 roadmap for the migration path before production adoption. |
| **Stream Governance** | ❌ NO-GO | No CRDs in provider-confluent v1.0.0. Requires out-of-band integration. Not suitable for teams that need tags or business metadata on topics. |

**Overall recommendation: GO — with conditions.** The core platform pattern (claim → Crossplane Composition → Confluent Cloud + Kubernetes resources → GitOps delivery → ESO/Vault secrets) is validated end-to-end. Two conditions before production: (1) upgrade to Standard/Dedicated tier for scoped RBAC, (2) resolve the XRD v1 deprecation path with the Crossplane v2 roadmap. Stream Governance is a separate conversation — if required, provider-confluent is not sufficient today.
