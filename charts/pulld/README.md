# Pulld Helm Chart

This chart deploys `pulld` as a single-replica registry pull-through proxy with
an optional persistent cache volume.

## Install

```bash
helm install pulld oci://ghcr.io/mkoyan44/charts/pulld \
  --version 0.1.1 \
  --namespace pulld \
  --create-namespace
```

For local chart development:

```bash
helm lint charts/pulld
helm template pulld charts/pulld --namespace pulld
```

## Key Values

| Value | Default | Description |
| --- | --- | --- |
| `image.repository` | `ghcr.io/mkoyan44/pulld` | Container image repository. |
| `image.tag` | `""` | Image tag. Defaults to the chart app version. |
| `service.port` | `5050` | ClusterIP service port. |
| `cache.dir` | `/var/lib/pulld/cache` | Cache directory passed to the `pulld` binary. |
| `persistence.enabled` | `true` | Use a PVC for the registry and Helm cache. |
| `persistence.size` | `20Gi` | PVC size. |
| `test.enabled` | `false` | Render the Helm test pod. Keep disabled for Terraform `helm_template` consumers. |

Pulld exposes `/health`, `/v2/`, `/helm/<repo>/index.yaml`, and the pre-pull
API on the same HTTP port.
