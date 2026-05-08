# Pulld Helm Chart

This chart deploys `pulld` as a single-replica registry pull-through proxy with
either a filesystem cache volume or an S3-compatible cache backend.

## Install

```bash
helm install pulld oci://ghcr.io/mkoyan44/charts/pulld \
  --version 0.2.0 \
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
| `cache.backend` | `filesystem` | Cache backend. Use `s3` for S3-compatible object storage. |
| `cache.dir` | `/var/lib/pulld/cache` | Cache directory passed to the `pulld` binary. |
| `persistence.enabled` | `true` | Use a PVC for the registry and Helm cache. |
| `persistence.size` | `20Gi` | PVC size. |
| `s3.endpoint` | `""` | S3-compatible endpoint. Required when `cache.backend=s3`. |
| `s3.bucket` | `pulld-cache` | S3 bucket for blobs, manifests, tag mappings, and Helm charts. |
| `s3.region` | `us-east-1` | S3 region passed to the client. |
| `s3.forcePathStyle` | `true` | Force path-style S3 requests for MinIO. |
| `s3.scratchDir` | `/tmp/pulld-cache` | Local scratch directory used before verified uploads. |
| `s3.existingSecret` | `""` | Secret containing S3 access keys. Required when `cache.backend=s3`. |
| `test.enabled` | `false` | Render the Helm test pod. Keep disabled for Terraform `helm_template` consumers. |

Pulld exposes `/health`, `/v2/`, `/helm/<repo>/index.yaml`, and the pre-pull
API on the same HTTP port.
