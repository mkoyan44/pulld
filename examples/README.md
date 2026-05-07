# Pulld Examples

This directory contains example code demonstrating how to use the pulld library.

## Examples

### basic_server

Basic server startup with default configuration.

```bash
cargo run --example basic_server
```

Shows:
- Default configuration usage
- Server startup
- Health endpoint

### custom_config

Custom configuration with multiple registries and mirror strategies.

```bash
cargo run --example custom_config
```

Shows:
- Custom registry configuration
- Multiple mirror strategies (Failover, Hedged)
- Custom cache size

### https_server

HTTPS/TLS-enabled server with certificate generation.

```bash
cargo run --example https_server
```

Shows:
- TLS certificate generation
- HTTPS server startup
- Certificate bundle usage

## Usage

All examples can be run with:

```bash
cargo run --example <example_name>
```

Examples use temporary directories (`/tmp/pulld-*`) and can be stopped with Ctrl+C.

