# Pulld Design

## Goal

Create `pulld`, a standalone public Rust repository that preserves the historical `4lock-agent` pulld implementation as a publishable crate and CLI.

## Source

Use the latest complete implementation before deletion:

- source repo: `/Users/mkoyan/projects/4lock/4lock-ai-rules/platform/4lock-agent`
- source commit: `3a0cfc3a261f686abf57abd4229d5f6a54dc729c`
- source path: `crates/controller/blob`

This version includes the registry proxy, local blob and manifest cache, Helm chart cache, mirror strategies, pre-pull, TLS support, per-mirror auth tests, examples, and a server binary.

## Public Shape

- repository directory: `platform/pulld`
- crate name: `pulld`
- binary name: `pulld`
- Rust edition: 2021
- license: MIT

The public API should expose the original capabilities with `pulld` naming:

- `start_server`
- `run_pre_pull`
- `CacheStorage`
- `Config`
- `MirrorStrategy`
- `RegistryConfig`
- `PulldService`

## Architecture

Keep the implementation as a single crate with modules matching the extracted pulld implementation:

- `cache`: metadata and filesystem storage for manifests, blobs, indexes, and charts
- `registry`: upstream registry client, blob and manifest proxy handlers, mirror selection
- `helm`: Helm index and chart cache/proxy
- `config`: defaults, registry mirror configuration, TLS and pre-pull configuration
- `prepull`: image and chart prefetch workflow
- `tls` and `certs`: TLS server configuration and development certificate generation
- `server`: Axum HTTP routes and server startup
- `service`: host-level lifecycle helper, renamed from PulldService to PulldService

The CLI should start the server with default configuration and an optional cache directory argument. It should also support environment overrides for cache directory, bind address, port, and log filter where practical.

## Testing

Port the historical tests and rebrand imports from `blob` to `pulld`. Add focused regression coverage for:

- crate public API compiles under `pulld`
- default config exposes the expected docker.io mirrors
- path parsing handles embedded registries and double `/v2/`
- per-mirror auth fallback behavior remains covered

Verification must include:

- `cargo fmt --check`
- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo package --allow-dirty`

