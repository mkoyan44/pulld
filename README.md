# Pulld

Pulld is a Rust pull-through proxy for container registries. It caches manifests,
blobs, and Helm artifacts locally, supports multiple upstream registries and
mirrors, and exposes a Docker Registry HTTP API compatible with containerd and
other OCI clients.

## Features

- Multi-registry proxying for Docker Hub, Quay, GHCR, registry.k8s.io, and custom registries
- Local cache for manifests, blobs, Helm indexes, and Helm chart archives
- Mirror strategies: failover, hedged, striped, and adaptive
- Anonymous and bearer-token registry pulls, including per-mirror token exchange
- Containerd-friendly tag, digest, GET, and HEAD behavior
- Optional HTTPS/TLS server support
- Pre-pull API and library workflow for images and Helm charts
- Embedded registry paths, such as `pulld.internal:5050/quay.io/cilium/cilium:v1.17.7`

## Install

```bash
cargo install pulld
```

For local development:

```bash
cargo run --bin pulld -- ./cache/pulld
```

Build the container image:

```bash
docker build -t ghcr.io/mkoyan44/pulld:0.1.0 .
```

Install with Helm:

```bash
helm install pulld charts/pulld \
  --namespace pulld \
  --create-namespace
```

The server listens on `0.0.0.0:5050` by default and exposes:

- `GET /health`
- `GET /v2/`
- `GET|HEAD /v2/<name>/manifests/<reference>`
- `GET|HEAD /v2/<name>/blobs/<digest>`
- `GET /helm/<repo>/index.yaml`
- `GET /helm/<repo>/charts/<chart>`
- `POST /api/v1/pre-pull`
- `GET /api/v1/cache/stats`
- `GET /api/v1/mirror/stats`

## Library Usage

```rust
use pulld::{start_server, Config};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> pulld::Result<()> {
    let cache_dir = PathBuf::from("./cache/pulld");
    let config = Config::default();
    let _server = start_server(cache_dir, config, None, None).await?;

    tokio::signal::ctrl_c().await.unwrap();
    Ok(())
}
```

## Examples

```bash
cargo run --example basic_server
cargo run --example custom_config
cargo run --example https_server
```

## Tests

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo package --allow-dirty
helm lint charts/pulld
helm template pulld charts/pulld --namespace pulld
```

Run all local verification:

```bash
make verify
```

## Source Lineage

Pulld was extracted from the historical `4lock-agent` docker-proxy implementation
at commit `3a0cfc3a261f686abf57abd4229d5f6a54dc729c` and made standalone for
public use.
