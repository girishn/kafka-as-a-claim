# Kafka-as-a-Claim POC — Crossplane + provider-confluent

## What this POC is for

We want app teams to submit a short, declarative "I need a Kafka topic"
claim and have a Kubernetes control loop turn that into real, continuously
reconciled resources — Confluent Cloud resources *and* the Kubernetes-side
workload resources that go with them — all delivered through GitOps, with
no app team Terraform or kubectl required.

This POC now answers three questions:

1. Can `crossplane-contrib/provider-confluent` model the Confluent Cloud
   side (topic, RBAC, schema) well enough to build on? *(community
   project, ~13 stars, latest tag v1.0.0 / Jan 2026, 12 CRDs as of that
   tag — not an official Confluent product.)*
2. Can a single Composition cleanly produce Kubernetes-side resources
   (KEDA ScaledObject, NetworkPolicy, PodDisruptionBudget) *alongside*
   the Confluent Cloud resources, with values patched between them?
3. Does the whole thing survive being delivered through GitOps — applied
   by Argo CD from a git commit, not by a human running `kubectl apply`?

Do not generalize, polish, or add abstractions beyond what's listed in
"Definition of done." If something is awkward or missing, write it down in
`FINDINGS.md` instead of working around it — the friction itself is the
deliverable.

## Stack

- **Crossplane v2.x** (latest stable v2.x via Helm) — the control plane
  runtime. v2 removes inline native patch-and-transform; all Compositions
  must use a Function pipeline (`spec.mode: Pipeline`). Do **not** install
  v1.x — the inline `patches:` syntax we previously referenced does not
  exist in v2.
- **function-patch-and-transform** — https://github.com/crossplane-contrib/function-patch-and-transform
  — the v2 replacement for inline patches; install as a `Function` package
  alongside the providers. Patch types (`FromCompositeFieldPath`,
  `ToCompositeFieldPath`, etc.) are identical to v1 syntax, just invoked
  from the pipeline step instead of embedded in the Composition directly.
- **provider-confluent** — https://github.com/crossplane-contrib/provider-confluent
  — Confluent Cloud resources, Upjet-generated from `confluentinc/confluent`
  (the official Terraform provider)
- **provider-kubernetes** — https://github.com/crossplane-contrib/provider-kubernetes
  — official Crossplane provider; exposes one generic `Object` CRD
  (`kubernetes.crossplane.io/v1alpha2` as of recent releases — **confirm
  the exact version installed**, some older docs/examples still show
  `v1alpha1`) that wraps an arbitrary raw manifest in
  `spec.forProvider.manifest`. This is how we'll create the KEDA
  ScaledObject, NetworkPolicy, and PodDisruptionBudget — there's no
  typed CRD for any of those, just this wrapper.
- **Argo CD** — GitOps delivery; one `Application` watching
  `https://github.com/girishn/kafka-as-a-claim` at path `crossplane/claims/`
  on branch `master`, with `syncPolicy.automated: { prune: true, selfHeal: true }`.
  Must be configured with annotation-based resource tracking and
  `ProviderConfigUsage` exclusions — see Setup.
- **HashiCorp Vault** (dev mode, in-cluster via Helm) — secret backend for
  the POC. Dev mode needs no persistent storage, no unsealing, and starts
  with a fixed root token — appropriate for kind.
- **External Secrets Operator (ESO)** — syncs Crossplane-generated
  connection detail Secrets (Confluent Kafka API key) from `crossplane-system`
  into app team namespaces. The `PushSecret` and `ExternalSecret` are created
  **per claim by the Composition** (via provider-kubernetes `Object` wrappers),
  not as static manifests. App team Deployments mount a plain K8s Secret;
  they never interact with Crossplane or Vault directly.
- **KEDA** (via Helm, `kedacore/keda`) — must be installed before the
  Composition runs; the `ScaledObject` CRD must exist for provider-kubernetes
  to create it. Uses KEDA's built-in `kafka` trigger type with SASL_SSL for
  Confluent Cloud consumer lag scaling.
- **kind** (or any disposable local cluster) — do not point this at a
  production GKE cluster
- A **sandbox Confluent Cloud environment** — never the team's real
  environment, even for a POC

## Repo layout

```
scripts/
  confluent-up.sh       # Creates Confluent Cloud env + cluster + SA + API key,
                        # waits for RUNNING, seeds kubectl secret. Saves IDs to
                        # .confluent-state for teardown.
  confluent-down.sh     # Reads .confluent-state, destroys all resources in reverse
                        # order (key → SA → cluster → env), cleans kubectl secret.
  .confluent-state      # Written by confluent-up.sh — gitignored
  .confluent-state.example  # Example of the state file format
crossplane/
  provider/             # Provider + ProviderConfig manifests (both providers)
  composition/          # The XRD + Composition (the only thing we "build")
  claims/               # Example KafkaTopicClaim manifests — this is what
                        # Argo CD watches and syncs
gitops/
  argocd-app.yaml       # The Argo CD Application pointing at crossplane/claims/
docs/
  FINDINGS.md           # Running log — what worked, what didn't, gaps found
```

## Setup

1. `kind create cluster --name kafka-claim-poc`
2. Provision Confluent Cloud resources and seed the cluster secret:
   ```bash
   confluent login
   bash scripts/confluent-up.sh
   # Creates: environment, Basic cluster, service account, EnvironmentAdmin
   # role binding, Cloud API key. Writes IDs to scripts/.confluent-state.
   # Seeds: kubectl secret confluent-credentials -n crossplane-system
   ```
   Teardown when done: `bash scripts/confluent-down.sh`
3. Install Crossplane via Helm into `crossplane-system`
4. Install both providers and the patch-and-transform function declaratively:
   ```yaml
   apiVersion: pkg.crossplane.io/v1
   kind: Provider
   metadata:
     name: provider-confluent
   spec:
     package: xpkg.upbound.io/crossplane-contrib/provider-confluent:v1.0.0
   ---
   apiVersion: pkg.crossplane.io/v1
   kind: Provider
   metadata:
     name: provider-kubernetes
   spec:
     package: xpkg.upbound.io/crossplane-contrib/provider-kubernetes:<latest>
   ---
   apiVersion: pkg.crossplane.io/v1beta1
   kind: Function
   metadata:
     name: function-patch-and-transform
   spec:
     package: xpkg.upbound.io/crossplane-contrib/function-patch-and-transform:<latest>
   ```
5. `kubectl get providers` until both report `INSTALLED=True HEALTHY=True`;
   `kubectl get functions` until `function-patch-and-transform` also reports healthy.
6. Install KEDA — must exist before the Composition runs so the `ScaledObject`
   CRD is present when provider-kubernetes tries to create it:
   ```bash
   helm repo add kedacore https://kedacore.github.io/charts
   helm repo update
   helm install keda kedacore/keda --namespace keda --create-namespace
   kubectl wait --for=condition=ready pod -l app=keda-operator -n keda --timeout=120s
   ```
7. Apply the provider-kubernetes RBAC extension so it can manage ScaledObjects,
   NetworkPolicies, PodDisruptionBudgets, and ESO ExternalSecrets/PushSecrets:
   ```bash
   kubectl apply -f crossplane/provider/provider-kubernetes-rbac.yaml
   ```
   The ClusterRoleBinding targets `system:serviceaccounts:crossplane-system`
   (all SAs in that namespace) so it survives provider upgrades that rotate
   the SA name suffix.
8. Create a `ProviderConfig` for `provider-confluent` referencing the
   `confluent-credentials` secret (Cloud API key) seeded by `confluent-up.sh`
   in step 2. The Kafka-scoped API key for topic/schema operations is created
   by the Composition itself — confirm the exact ProviderConfig shape via Step 0
   (`kubectl explain providerconfig.confluent.crossplane.io --recursive`).
9. Create a `ProviderConfig` for `provider-kubernetes` pointing at the same
   in-cluster kubeconfig (we're applying K8s resources to the same kind
   cluster the Composition runs in, not a remote one — keep this simple).
10. Install Vault in dev mode and ESO:
   ```bash
   helm install vault hashicorp/vault --set server.dev.enabled=true -n vault --create-namespace
   helm install external-secrets external-secrets/external-secrets -n external-secrets --create-namespace
   ```
   Configure a `ClusterSecretStore` pointing at `vault.vault.svc.cluster.local:8200`
   with the dev root token. The `PushSecret` and `ExternalSecret` for each
   claim are generated by the Composition (not static manifests) — the
   Deployment in `team-payments` mounts the resulting Secret with no
   Crossplane or Vault knowledge required.
11. Install Argo CD and configure `argocd-cm` **before** applying any Application:
   ```yaml
   # argocd-cm ConfigMap patch
   application.resourceTrackingMethod: annotation
   resource.exclusions: |
     - apiGroups:
       - "*.crossplane.io"
       - "*.upbound.io"
       kinds:
       - ProviderConfigUsage
       clusters:
       - "*"
   ```
   Then apply `gitops/argocd-app.yaml`, pointing at this repo's
   `crossplane/claims/` path with
   `syncPolicy.automated: { prune: true, selfHeal: true }`.

## Step 0 — discover before you build anything

**Do not assume specific CRD names or fields.** Before writing the
Composition:

```
kubectl get crds | grep confluent
kubectl explain <discovered-kind> --recursive
kubectl explain object.kubernetes.crossplane.io --recursive
```

Read everything in `examples/` and `examples-generated/confluent/` in the
provider-confluent repo — that's ground truth, not this file.

Specifically confirm whether provider-confluent exposes:
- A topic resource (partitions, retention config)
- A service-account-equivalent resource
- A role-binding resource scoped by CRN pattern
- An API key resource
- **A schema resource** (registering a value/key schema against a subject)
- **Anything for Stream Governance** (tags, tag bindings, business
  metadata) — this is the least likely of the set to exist; if it
  doesn't, say so plainly in `FINDINGS.md` rather than working around it
  with a `kubernetes.crossplane.io` Object hack against Confluent Cloud's
  REST API (that's a different kind of risk than wrapping native K8s
  resources, and worth a separate conversation, not a quiet substitution)

If any are missing or broken, that's a headline finding — it changes the
recommendation.

## Platform interaction model

Two distinct mechanisms, each for a different kind of intent:

**Claims (XRC) — for resources with their own lifecycle:**
App teams submit a `KafkaTopicClaim` YAML (5–6 fields) via git PR. Crossplane
Composition translates it into Confluent Cloud resources + K8s workload
resources. The claim is the source of truth; app teams never touch Terraform,
the Confluent console, or Crossplane internals.

**Annotations — for decorating existing resources:**
Cross-cutting platform behaviors are expressed as annotations on the app team's
existing Kubernetes resources (Deployments, Namespaces). A lightweight
controller or mutating webhook reads these and injects the corresponding
platform config. Examples:

| Annotation | Effect |
|---|---|
| `auth.platform.girishn.cloud/provider: google` | OIDC sidecar / identity wiring |
| `observability.platform.girishn.cloud/metrics: true` | Prometheus scrape config injection |
| `platform.girishn.cloud/healthcheck: http` | Liveness/readiness probe injection |
| `network.platform.girishn.cloud/egress: confluent-psc` | NetworkPolicy applied by controller |
| `platform.girishn.cloud/tier: standard` | Resource quota / limit range applied |

Annotations are **not** in scope for this POC — the POC validates the Claim
path. The annotation controller is the natural next layer. Document the
boundary in `FINDINGS.md`.

**Secrets delivery:**
Crossplane writes the Confluent API key as a connection detail Secret in
`crossplane-system`. ESO's `PushSecret` syncs it into Vault; an
`ExternalSecret` in the app namespace pulls it back as a standard K8s Secret.
App teams mount `secretKeyRef` as they would any other secret — no Crossplane
or Vault knowledge required.

**One claim per topic:**
Each `KafkaTopicClaim` provisions exactly one topic. Multiple topics = multiple
claims. This is the correct POC scope. The verbosity concern at scale is solved
by a Backstage Software Template (form → auto-generated claim PR), not by
changing the Composition. Note this in `FINDINGS.md` as the production
evolution path.

## What to build

**One XRD + Composition**, mirroring the fields from our Terraform module
plus the new additions:

```yaml
apiVersion: platform.girishn.cloud/v1alpha1
kind: KafkaTopicClaim
metadata:
  name: orders-events
  namespace: team-payments
spec:
  topicName: orders.events
  partitions: 6
  consumerName: payments-consumer       # used for both Confluent SA and K8s Deployment name
  valueSchema: orders-value-v1           # Avro subject name; omit if Schema CRD absent
```

The Composition should produce, via **provider-confluent**:
- The topic
- The consumer service account + scoped RBAC role binding(s)
- A Kafka-scoped API key for the consumer service account
- The schema registration (Avro), if Step 0 confirms the Schema CRD exists

And, via **provider-kubernetes** `Object` wrapping raw manifests:
- A **KEDA ScaledObject** targeting `consumerName` Deployment, scaled on
  consumer lag, with `maxReplicaCount` set from the topic's partition
  count — **patch this from the provider-confluent Topic resource's
  observed status into the Object's embedded manifest using
  `function-patch-and-transform` pipeline steps (`FromCompositeFieldPath`
  / `ToCompositeFieldPath`), don't hardcode it twice.** The Composition
  must use `spec.mode: Pipeline` and call the function; there is no
  inline `patches:` in Crossplane v2. This one cross-provider patch is
  the single most valuable thing to validate in this whole POC — it's the
  difference between "the claim is the source of truth" and "two places
  that can silently disagree."
- A **KEDA TriggerAuthentication** referencing the Kafka API key Secret
  in `team-payments`
- A **NetworkPolicy** (placeholder — PSC requires Dedicated tier; proves
  the Object wrapping pattern; note in FINDINGS.md)
- A **PodDisruptionBudget** for `consumerName` Deployment
- An ESO **PushSecret** — pushes the Kafka API key connection detail
  Secret from `crossplane-system` into Vault
- An ESO **ExternalSecret** — pulls from Vault into a Secret in
  `team-payments`; the consumer Deployment mounts it as a plain Secret

Applying the claim (via Argo CD, from a git commit — not `kubectl apply`
run by hand) should result in all of the above existing. Reverting that
commit should result in all of it being torn down.

## GitOps-specific things to test

- Commit a new claim → push → confirm Argo CD picks it up and applies it
  with **no manual kubectl** at any point
- Revert the commit → confirm Argo CD prunes the claim → confirm
  Crossplane actually deletes the downstream managed resources (claim
  deletion cascading through Crossplane *and* surviving Argo CD's prune
  behavior is a real risk area, not a formality)
- Confirm Argo CD's drift detection is scoped to the claim manifest only —
  it should not also be trying to reconcile the managed resources
  Crossplane generates underneath (those live only in-cluster, never in
  git, so this should be fine by construction — confirm it actually is)
- Confirm the `ProviderConfigUsage` exclusion is working — Argo CD should
  not show thousands of unknown resources or flag the Application as
  OutOfSync due to Crossplane-generated objects
- Confirm annotation-based resource tracking is active — verify with
  `kubectl get application -n argocd -o yaml | grep resourceTrackingMethod`

## Explicitly out of scope (still)

- Workload Identity Federation wiring (cloud IAM ↔ K8s ServiceAccount)
- Annotation-based platform controller (cross-cutting concerns layer)
- Backstage Software Templates / developer portal front-end
- Multiple environments (dev/staging/prod) or environment promotion
- Making the Composition "nice" — minimal, ugly, working is the goal

## Definition of done

- [ ] Both providers install cleanly and report healthy
- [ ] Step 0 discovery is documented, including whether schema and
      Stream Governance resources exist in provider-confluent
- [ ] A claim applied via Argo CD produces a real topic + scoped consumer
      access in the sandbox Confluent Cloud environment
- [ ] The same claim produces a working KEDA ScaledObject, NetworkPolicy,
      and PodDisruptionBudget in-cluster
- [ ] The ScaledObject's `maxReplicaCount` is confirmed to come from a
      live patch off the Topic's partition count, not a hardcoded value
- [ ] Confluent API key is delivered to the app namespace via ESO + Vault —
      app team Deployment mounts it as a plain Secret with no Crossplane
      or Vault knowledge
- [ ] Argo CD Application is confirmed healthy with annotation-based
      tracking and ProviderConfigUsage exclusions — no spurious OutOfSync
- [ ] Reverting the claim's git commit fully tears down every resource,
      Confluent Cloud and Kubernetes side
- [ ] `FINDINGS.md` has a clear go/no-go recommendation covering: the
      Confluent Cloud side, the `provider-kubernetes` wrapping pattern,
      the GitOps delivery path, and the ESO/Vault secrets hand-off — four
      separable conclusions; note XRD versioning and Backstage as the
      production evolution path

## References

- https://github.com/crossplane-contrib/provider-confluent
- https://github.com/crossplane-contrib/provider-kubernetes
- https://github.com/crossplane-contrib/function-patch-and-transform
- https://registry.terraform.io/providers/confluentinc/confluent/latest/docs
- https://docs.crossplane.io/latest/whats-new/
- https://docs.crossplane.io/latest/guides/function-patch-and-transform/
- https://docs.crossplane.io/latest/get-started/get-started-with-composition/
- https://docs.crossplane.io/latest/guides/crossplane-with-argo-cd/
- https://external-secrets.io/latest/provider/hashicorp-vault/
- https://developer.hashicorp.com/vault/docs/deploy/kubernetes/vso/sources/vault
- https://argo-cd.readthedocs.io/
