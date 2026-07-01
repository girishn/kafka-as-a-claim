# POC Findings — Kafka-as-a-Claim

## Status: Prototype (logistics domain, real producer/consumer, Schema Registry)

---

## Step 0: CRD Discovery

**provider-confluent v1.0.0** — 12 CRDs installed. Confirmed resources:

| CRD | Kind | Present |
|-----|------|---------|
| `apikeys.confluent.crossplane.io` | `APIKey` | ✅ |
| `kafkatopics.confluent.crossplane.io` | `KafkaTopic` | ✅ |
| `rolebindings.confluent.crossplane.io` | `RoleBinding` | ✅ |
| `serviceaccounts.confluent.crossplane.io` | `ServiceAccount` | ✅ |
| `schemas.confluent.crossplane.io` | `Schema` | ✅ (deferred — see below) |
| `environments.confluent.crossplane.io` | `Environment` | ✅ |
| `clusters.confluent.crossplane.io` | `Cluster` | ✅ |

**Stream Governance (tags, tag bindings, business metadata): ABSENT.** There are no CRDs for Confluent Stream Governance in provider-confluent v1.0.0. If Stream Governance tagging is needed, it would require a separate out-of-band integration (Confluent REST API via a custom controller, or Terraform). This is a gap; document it before recommending production adoption.

**Schema Registry (`schemas.confluent.crossplane.io`): EXISTS and IMPLEMENTED.** The CRD is present and wired up in the prototype. Approach taken:

| Concern | Solution |
|---------|---------|
| SR cluster ID (`lsrc-…`) | Auto-patched by `poc.sh up` via `sed` (same pattern as Kafka cluster ID) |
| SR-scoped API key | Created by `poc.sh up` for the `crossplane-poc` SA; seeded as `confluent-sr-credentials` in `crossplane-system` |
| SR credentials in Composition | `credentials` block in the `Schema` resource — same pattern as `credentials` in `KafkaTopic` |
| Schema body | `spec.schemaBody` field in the claim (Avro JSON string, folded scalar in YAML) |
| Subject name | `spec.valueSchema` field in the claim (e.g., `shipments.events-value`) |

The `Schema` composed resource uses `providerConfigRef: name: default` (cloud credentials for management plane) and the `credentials` block for SR data plane auth. The XRD now requires both `valueSchema` and `schemaBody`.

**Unresolved:** Field naming in the `Schema` CRD spec (`subjectName`, `schemaRegistryCluster`, `restEndpoint`, `credentials`) was inferred from the Confluent Terraform provider. Confirm against actual CRD schema on first run (`kubectl explain schemas.confluent.crossplane.io --recursive`) — field names may differ from the Upjet-generated output.

---

## Confluent-Side Findings

### RBAC on Basic Tier: Resource Roles Not Supported

**Finding (HIGH):** `DeveloperRead` and `DeveloperWrite` are resource-level roles and are **not supported on Basic tier Kafka clusters**. Attempt:
```
Error: Forbidden - Basic Clusters can not use resource roles
```
The Composition uses `CloudClusterAdmin` (cluster-scoped role) as a workaround. This is overly permissive — it grants full cluster admin to the consumer service account.

**Production implication:** Scoped per-topic RBAC requires Standard or Dedicated tier. The POC cannot validate fine-grained RBAC on Basic tier. If the team's real workload is on a Basic cluster, this is a hard blocker for the "scoped consumer access" story.

**Recommendation:** Use Standard or Dedicated tier in production. The Composition's `roleName` field is the only change needed — the RoleBinding's `crnPattern` format for `DeveloperRead` on a specific topic would be:
```
crn://confluent.cloud/organization=<org-id>/environment=<env-id>/cloud-cluster=<cluster-id>/kafka=<cluster-id>/topic=<topicName>
```
with a second RoleBinding for the consumer group:
```
crn://confluent.cloud/organization=<org-id>/environment=<env-id>/cloud-cluster=<cluster-id>/kafka=<cluster-id>/group=<consumerName>-*
```

### Service Account Display Name Uniqueness

**Finding:** Confluent enforces unique SA display names within an org. If a `poc.sh down` run does not cleanly delete the SA (or if it's deleted manually but Crossplane still holds the MR), a subsequent `poc.sh up` with the same claim will hit `409 Conflict: Service name is already in use`.

**Workaround:** Delete the stale SA via `confluent iam service-account delete <id> --force`, then trigger re-reconcile of the Crossplane MR via pause/unpause annotation. `poc.sh down` deletes the SA as part of its Confluent teardown sequence, so this only occurs if `down` was incomplete or skipped.

**Production implication:** SA names are claim-scoped (derived from `consumerName`). This is correct behavior. The 409 is a safety guard, not a bug. Ensure `poc.sh down` always fully completes before reprovisioning the same claim name.

### APIKey Credential Chicken-and-Egg

The KafkaTopic requires the APIKey's connection secret to exist before it can authenticate to Confluent. Since both are created by the same Composition, the KafkaTopic reconcile loop fails with `credentials.0.key is empty` until the APIKey finishes provisioning (~3–5 min for Confluent Cloud). This is expected eventual consistency, not a bug. Crossplane retries automatically and the KafkaTopic recovers once the secret lands.

**Key confirmed:** The APIKey connection secret contains keys `api_key_id`, `api_key_secret`, and `attribute.secret`. KafkaTopic credentials reference `api_key_id` (key) and `api_key_secret` (secret).

### Hardcoded Confluent IDs in Composition

**Finding:** `composition.yaml` embeds ENV_ID, CLUSTER_ID, REST endpoint, and bootstrap server as literal strings. These go stale after every `poc.sh down` + `poc.sh up` cycle (each creates a new Confluent environment and cluster with new IDs). Stale IDs cause the APIKey to return 403 Forbidden and the KafkaTopic to be unreachable.

**Fix implemented:** `poc.sh up` now automatically patches all four values into `composition.yaml` via `sed` after the Confluent Cloud section resolves (whether from fresh provision or state file). The file on disk is always up to date before `kubectl apply -f composition.yaml` runs.

**Production implication:** In production, cluster IDs are stable (you don't reprovision). This is a POC-only operational concern. The fix is adequate for the POC lifecycle.

### Cross-Resource References via `matchControllerRef`

Both `principalSelector.matchControllerRef: true` (RoleBinding → SA) and `idSelector.matchControllerRef: true` (APIKey → SA) work correctly within the Composition. Crossplane sets a shared controller reference on all composed resources owned by the same XR, making selector-based cross-resource resolution reliable without hardcoding K8s resource names.

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

The ScaledObject `maxReplicaCount` came from the live observed partition count on Confluent, not a hardcoded value. The claim is the single source of truth — if you change `spec.partitions` in the claim, both the Confluent topic partition count and the KEDA ScaledObject's ceiling update together.

**Implementation detail:** The ScaledObject receives two passes:
1. Pass A — immediate patch from `spec.partitions` (claim) so the ScaledObject is created with a correct value before the Topic is ready
2. Pass B — live patch from `status.provisioned.partitionsCount` (Optional) once the Topic reconciles; overwrites Pass A with the observed value

Both passes produce the same value for a normal claim. They would diverge if Confluent rejected the requested partition count or adjusted it — in which case the observed value wins, which is the correct behavior.

---

## provider-kubernetes Object Wrapping: Confirmed Working ✅

All six Kubernetes-side resources created and confirmed Ready:

| Resource | Status | Notes |
|----------|--------|-------|
| KEDA ScaledObject | ✅ Ready | `maxReplicaCount` patched from live Confluent partition count |
| KEDA TriggerAuthentication | ✅ Ready | References ESO-vended `payments-consumer-kafka-creds` |
| NetworkPolicy | ✅ Ready | Placeholder — PSC requires Dedicated tier |
| PodDisruptionBudget | ✅ Ready | `minAvailable: 1` enforced |
| ESO PushSecret | ✅ Synced | Confluent APIKey → Vault |
| ESO ExternalSecret | ✅ Synced | Vault → `team-payments` Secret |

**NetworkPolicy placeholder:** PSC Private Link requires Dedicated tier. The NetworkPolicy wrapping pattern is proven — the resource is created and managed by Crossplane. In production (Dedicated tier + PSC), replace the permissive `egress: any:9092` with a proper destination CIDR to the PSC endpoint. Flag this for the annotation-based platform controller layer.

**Object CRD version:** Both `v1alpha1` and `v1alpha2` are available in provider-kubernetes v0.17.0. The Composition uses `v1alpha2`.

---

## Crossplane v2 Changes

### XRD v1 Deprecated; v2 Does Not Support Claims

`apiextensions.crossplane.io/v2` is available but rejects `claimNames`:
```
The CompositeResourceDefinition "xkafkatopics.platform.girishn.cloud" is invalid:
spec: Invalid value: Claims aren't supported in apiextensions.crossplane.io/v2
```

The POC uses XRD `v1` (deprecated but functional). The claim model in v2 has changed — investigate the v2 migration path before production. If v2 removes claims entirely, the app-team-facing interface becomes a direct XR submission (no namespace scoping), which changes the security posture.

### Composition Must Use `spec.mode: Pipeline`

Crossplane v2 removed inline `patches:` from Compositions. The `spec.mode: Pipeline` + `function-patch-and-transform` is the only supported patching mechanism. The function is installed as a `Function` package and invoked from the pipeline step.

---

## Secrets Credential Format

**Critical:** The Confluent ProviderConfig expects a single `credentials` key in the Secret containing a JSON blob:
```json
{"cloud_api_key":"<key>","cloud_api_secret":"<secret>"}
```
The `--from-literal` flag in `kubectl create secret` is unreliable on Windows (Git Bash strips inner quotes). `poc.sh` uses base64 + `kubectl apply -f -` to guarantee verbatim JSON.

---

## ESO/Vault Secrets Handoff: Confirmed Working ✅

### ESO API Version Changes

The Composition was initially authored against `external-secrets.io/v1beta1` for ExternalSecret and `v1alpha1` for PushSecret. The installed ESO version uses different APIs:

| Resource | Expected | Actual |
|----------|----------|--------|
| ExternalSecret | `v1beta1` | `v1` (`v1beta1` served=false) |
| PushSecret | `v1alpha1` | `v1alpha1` (correct, but `secretStoreRef` → `secretStoreRefs`) |
| ClusterSecretStore | `v1beta1` | `v1` |

**PushSecret schema change:** `spec.secretStoreRef` (singular) was replaced by `spec.secretStoreRefs` (array). Update:
```yaml
secretStoreRefs:
  - name: vault-cluster-store
    kind: ClusterSecretStore
```

### Validated end-to-end path

```
Crossplane APIKey connection secret (crossplane-system)
  → PushSecret → Vault (dev mode, http://vault.vault.svc.cluster.local:8200)
  → ExternalSecret → K8s Secret (payments-consumer-kafka-creds in team-payments)
     keys: api_key_id, api_key_secret
```

App team Deployment mounts `payments-consumer-kafka-creds` as a plain Secret. No Crossplane, Vault, or ESO knowledge required by the app team.

---

## GitOps Delivery: Confirmed Working ✅

**Argo CD Application:** Synced + Healthy. Annotation-based resource tracking and ProviderConfigUsage exclusions confirmed active.

- `application.resourceTrackingMethod: annotation` — verified in `argocd-cm`
- `ProviderConfigUsage` exclusion — no spurious OutOfSync from Crossplane-generated objects
- `KafkaTopicClaim` applied via Argo CD from `git push` with no manual `kubectl apply`
- Drift detection scoped to `crossplane/claims/` manifests only — managed resources live in-cluster and are not tracked in git, so Argo CD correctly ignores them

**argocd-cm settings baked into Helm at install time** — no post-install patch or controller restart required. Both exclusion rules (`Endpoints` and `ProviderConfigUsage`) are embedded in the `helm upgrade --install` values heredoc in `poc.sh up`.

**GitOps teardown (revert commit → cascade delete): NOT YET EXERCISED.** This is the last remaining gate. Risk area: claim deletion finalizer must clear through Crossplane before Argo CD considers the resource pruned. Validate before the demo.

---

## XRD Versioning and Production Evolution

The current XRD version is `v1alpha1`. Any field changes (adding `partitionsCount` to status, changing `consumerName` semantics) require XRD version bumps with conversion webhooks. Plan XRD versioning policy before production:
- Keep `v1alpha1` as-is for the POC
- Define `v1beta1` when the schema stabilises
- Use Crossplane's built-in conversion (or a webhook) for field renaming

## Backstage Integration Path

The app team claim is a 5-line YAML file. At scale, generating these via git PR is cumbersome. The natural production evolution:
- Backstage Software Template (form → auto-generated claim PR via scaffolder)
- The template renders the `KafkaTopicClaim` YAML and opens a PR to `crossplane/claims/`
- Argo CD auto-syncs the PR once merged
- The entire flow: form submit → PR review → merge → topic provisioned → zero kubectl

This is out of scope for the POC but is the correct production path.

---

## Go/No-Go Assessment

| Area | Verdict | Notes |
|------|---------|-------|
| **Confluent Cloud (provider-confluent)** | ✅ GO | Topic, SA, APIKey, RoleBinding all work. Stream Governance absent — flag separately. Schema deferred (credential chain complexity). |
| **Cross-provider patching** | ✅ GO | Confirmed: `KafkaTopic.status.atProvider.partitionsCount` → XR status → `ScaledObject.spec.maxReplicaCount`. Claim is the single source of truth. |
| **provider-kubernetes Object wrapping** | ✅ GO | All 6 K8s resource types created and managed correctly. No limitations found. |
| **ESO/Vault secrets handoff** | ✅ GO | Full chain confirmed. App team mounts a plain Secret with no Crossplane or Vault knowledge. |
| **GitOps delivery (Argo CD)** | ✅ GO | Synced + Healthy. Annotation tracking + ProviderConfigUsage exclusions confirmed. Teardown not yet exercised — validate before demo. |
| **RBAC scoping** | ⚠️ CONDITION | Basic tier blocks resource-level roles. CloudClusterAdmin is overly permissive. Standard/Dedicated tier required for production. |
| **Crossplane v2 XRD claims** | ⚠️ RISK | v2 drops claims; must use deprecated v1. Monitor Crossplane v2 roadmap for the claims migration path before production adoption. |
| **Stream Governance** | ❌ NO-GO | No CRDs in provider-confluent v1.0.0. Requires out-of-band integration. Not suitable for teams that need tags or business metadata on topics. |

**Prototype additions (logistics domain):**
- Consumer is a real Python `confluent_kafka` app mounting ESO-vended `tracking-consumer-kafka-creds`; deliberately slow (1 msg/sec) so KEDA lag builds visibly
- Producer is a real Python app writing to both `shipments.events` and `delivery.alerts` (FAILED events only) at 10 msg/sec, using a dedicated `shipments-producer` SA + API key seeded by `poc.sh`
- Two claims: `shipment-tracking` (team-logistics, 6 partitions) and `delivery-alerts` (team-operations, 3 partitions) — multi-tenancy confirmed
- KEDA trigger auth fixed: `sasl: plaintext` + `tls: enable` (was `sasl: sasl_ssl` + `tls: none` — wrong values, masked because no real Kafka traffic previously hit KEDA)
- Namespace hardcoding fixed: all 5 Object manifests now patch `spec.claimRef.namespace` so each claim's resources land in the correct team namespace
- Prometheus + Grafana (kube-prometheus-stack) + KEDA ServiceMonitor added to stack

**Outstanding:** GitOps teardown (revert commit → cascade delete) and beat 4 self-heal not yet exercised. Both require a live cluster with claims applied.

**Overall recommendation: GO — with conditions.** The core platform pattern (claim → Crossplane Composition → Confluent Cloud + Kubernetes resources → GitOps delivery) is validated end-to-end. Two conditions before production: (1) upgrade to Standard/Dedicated tier for scoped RBAC, (2) resolve the XRD v1 deprecation path. Stream Governance is a separate conversation — if required, provider-confluent is not sufficient today.
