# 4lock Platform IaC Integration

This document defines how `pulld` should be wired into the 4lock platform
Terraform/Terragrunt layer after the public repo and chart are available.

## Current Platform Fit

- `4lock-platform-modules` already provides `modules/k8s/utility/release`, which
  renders Helm charts with `data.helm_template` and applies the generated
  manifests through `kubectl_manifest`.
- `4lock-platform-infra` has migrated away from in-cluster Helm state for live
  stacks and should consume `pulld` through that release module.
- `pulld` does not need Kubernetes API permissions. Its chart disables service
  account token mounting by default.
- The cache should run as one replica with a `ReadWriteOnce` PVC unless the
  platform explicitly provisions a shared `ReadWriteMany` cache.

## Module Design

Preferred first implementation: use the existing generic module directly.

```hcl
module "pulld" {
  source = "git::ssh://git@github.com/mkoyan44/4lock-platform-modules.git//modules/k8s/utility/release?ref=v3.0.0"

  release_name  = "pulld"
  namespace     = "pulld"
  chart         = "pulld"
  chart_version = "0.1.1"
  repository    = "oci://ghcr.io/mkoyan44/charts"

  values_yaml = yamlencode({
    image = {
      repository = "ghcr.io/mkoyan44/pulld"
      tag        = "0.1.1"
    }

    persistence = {
      enabled = true
      size    = "50Gi"
    }

    resources = {
      requests = {
        cpu    = "250m"
        memory = "256Mi"
      }
      limits = {
        cpu    = "2"
        memory = "2Gi"
      }
    }
  })
}
```

If `pulld` becomes a shared platform primitive, add a thin wrapper module under
`4lock-platform-modules/modules/k8s/apps/pulld` that validates platform defaults
and delegates to `modules/k8s/utility/release`. Keep the wrapper small; do not
copy Helm rendering logic.

## Infra Design

Add live config under the relevant environment, for example:

```text
4lock-platform-infra/live/production-onprem-shared/k8s/apps/pulld/terragrunt.hcl
```

The live unit should:

- depend on the namespace/bootstrap layer that creates `pulld` or creates the
  namespace explicitly through the established platform namespace module
- source the generic release module or the future `modules/k8s/apps/pulld`
  wrapper
- pin `chart_version` and image `tag` to the same released semver
- choose a storage class and PVC size based on registry cache retention
- expose the service internally as
  `pulld.pulld.svc.cluster.local:5050`

Recommended live values:

```hcl
inputs = {
  release_name  = "pulld"
  namespace     = "pulld"
  chart         = "pulld"
  chart_version = "0.1.1"
  repository    = "oci://ghcr.io/mkoyan44/charts"

  values_yaml = yamlencode({
    image = {
      repository = "ghcr.io/mkoyan44/pulld"
      tag        = "0.1.1"
    }

    persistence = {
      enabled          = true
      size             = "50Gi"
      storageClassName = "longhorn"
    }

    service = {
      type = "ClusterIP"
      port = 5050
    }
  })
}
```

## Verification

Before merging platform IaC:

```bash
helm lint charts/pulld
helm template pulld charts/pulld --namespace pulld
tofu fmt -check -recursive
terragrunt hclfmt --check
terragrunt plan
```

Run the Terraform/Terragrunt commands from the touched platform repo, scoped to
the `pulld` unit and any wrapper module added for it.
