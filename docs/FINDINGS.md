# POC Findings — Kafka-as-a-Claim

## Status: In Progress (core loop + ESO/Vault green; GitOps pending)

---

## Step 0: CRD Discovery

**provider-confluent v1.0.0** — 12 CRDs installed. Confirmed resources:

| CRD | Kind | Present |
|-----|------|---------|
| `apikeys.confluent.crossplane.io` | `APIKey` | ✅ |
| `kafkatopics.confluent.crossplane.io` | `KafkaTopic` | ✅ |
| `rolebindings.confluent.crossplane.io` | `RoleBinding` | ✅ |
| `serviceaccounts.confluent.crossplane.io` | `ServiceAccount` | ✅ |
| `schemas.confluent.crossplane.io` | `Schema` | ✅ (but deferred — see below) |
| `environments.confluent.crossplane.io` | `Environment` | ✅ |
| `clusters.confluent.crossplane.io` | `Cluster` | ✅ |

**Stream Governance (tags, tag bindings, business metadata): ABSENT.** There are no CRDs for Confluent Stream Governance in provider-confluent v1.0.0. If Stream Governance tagging is needed, it would require a separate out-of-band integration (Confluent REST API via a custom controller, or Terraform). This is a gap; document it before recommending production adoption.

**Schema Registry (`schemas.confluent.crossplane.io`): EXISTS but DEFERRED.** The CRD is present, but registering an Avro schema requires:
1. A separate Schema Registry cluster ID (`lsrc-…`) — not the Kafka cluster ID
2. A separate SR-scoped API key (`srcm.v2.Cluster` managedResource)
3. An additional ProviderConfig entry for SR credentials
4. The actual Avro schema body as a field — the claim carries `valueSchema: orders-value-v1` (a *subject name*, not a schema body)

The `valueSchema` field in the claim has no corresponding schema body anywhere in the claim design. Schema registration is a second-pass deliverable. Document the credential chain complexity and the claim field design question (where does the schema body come from?) before the next iteration.

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

### APIKey Credential Chicken-and-Egg

The KafkaTopic requires the APIKey's connection secret to exist before it can authenticate to Confluent. Since both are created by the same Composition, the KafkaTopic reconcile loop fails with `credentials.0.key is empty` until the APIKey finishes provisioning (~3–5 min for Confluent Cloud). This is expected eventual consistency, not a bug. Crossplane retries automatically and the KafkaTopic recovers once the secret lands.

**Key confirmed:** The APIKey connection secret contains keys `api_key_id`, `api_key_secret`, and `attribute.secret`. KafkaTopic credentials reference `api_key_id` (key) and `api_key_secret` (secret).

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

All six Kubernetes-side resources created correctly:

| Resource | Status | Notes |
|----------|--------|-------|
| KEDA ScaledObject | ✅ Ready | `maxReplicaCount` patched from partition count |
| KEDA TriggerAuthentication | ✅ Ready | References ESO-vended secret (not yet available) |
| NetworkPolicy | ✅ Ready | Placeholder — PSC requires Dedicated tier |
| PodDisruptionBudget | ✅ Ready | `minAvailable: 1` enforced |
| ESO PushSecret | ❌ Not synced | ESO not yet installed |
| ESO ExternalSecret | ❌ Not synced | ESO not yet installed |

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
The `--from-literal` flag in `kubectl create secret` is unreliable on Windows (Git Bash strips inner quotes). The `confluent-up.sh` script was updated to use base64 + `kubectl apply -f -` to guarantee verbatim JSON.

---

## ESO/Vault Integration: Confirmed Working ✅

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
  → PushSecret (Synced: orders-events-f9lvr-push)
  → Vault (dev mode, http://vault.vault.svc.cluster.local:8200)
  → ExternalSecret (SecretSynced: payments-consumer-kafka-creds)
  → K8s Secret (payments-consumer-kafka-creds in team-payments)
     keys: api_key_id, api_key_secret
```

App team Deployment mounts `payments-consumer-kafka-creds` as a plain Secret. No Crossplane, Vault, or ESO knowledge required by the app team.

### ClusterSecretStore configuration

```yaml
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
```

---

## GitOps Delivery: Not Yet Validated

The Argo CD Application (`gitops/argocd-app.yaml`) is written but not yet applied. The `argocd-cm` annotation-based tracking and `ProviderConfigUsage` exclusions are not yet configured.

**Next steps:**
1. Patch `argocd-cm` with annotation tracking + ProviderConfigUsage exclusions
2. Apply `gitops/argocd-app.yaml`
3. Delete manually-applied claim and Deployment — let Argo CD resync from git
4. Validate prune behavior (revert commit → cascade delete)

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

This is out of scope for the POC but is the correct production path. Document this as the "how do app teams actually use this" story for the go/no-go recommendation.

---

## Go/No-Go Assessment (Preliminary)

| Area | Assessment | Blocker? |
|------|-----------|---------|
| **Confluent Cloud (provider-confluent)** | Core resources (Topic, SA, APIKey, RoleBinding) work. Schema deferred. Stream Governance absent. | No (for basic use case); Yes (for stream governance) |
| **provider-kubernetes Object wrapping** | Pattern proven for all 6 resource types. Cross-provider patch confirmed. | No |
| **GitOps delivery (Argo CD)** | Not yet validated (structural artifacts written). | Pending |
| **ESO/Vault secrets handoff** | Objects structurally correct; not yet reconciling (ESO not installed). | Pending |
| **RBAC scoping** | Basic tier blocks resource-level roles. CloudClusterAdmin is overly permissive. | Yes (for Standard/Dedicated); No for Basic with broad access |
| **Crossplane v2 XRD claims** | v2 drops claims; must use v1 (deprecated). Migration path unclear. | Risk (future) |
