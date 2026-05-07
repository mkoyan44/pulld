# Pulld Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build `pulld`, a standalone publishable Rust crate and CLI from the historical 4lock-agent pulld implementation.

**Architecture:** Extract the latest complete `crates/controller/blob` implementation from commit `3a0cfc3a261f686abf57abd4229d5f6a54dc729c`, rebrand crate and public names to `pulld`, and keep the existing module boundaries. Add repository metadata, docs, and publish dry-run verification.

**Tech Stack:** Rust 2021, Axum 0.7, Tokio 1, Reqwest 0.12, Rustls 0.23, Serde, Tracing, Cargo package tooling.

---

### Task 1: Add Failing Public API Smoke Test

**Files:**
- Create: `tests/public_api_test.rs`
- Create: `Cargo.toml`
- Create: `src/lib.rs`

**Step 1: Write the failing test**

```rust
use pulld::{Config, MirrorStrategy, RegistryConfig};

#[test]
fn public_api_uses_pulld_crate_name() {
    let config = Config::default();
    assert!(config.upstream.registries.contains_key("docker.io"));
    assert_eq!(MirrorStrategy::default(), MirrorStrategy::Adaptive);

    let registry = RegistryConfig {
        mirrors: vec!["https://registry-1.docker.io".to_string()],
        strategy: MirrorStrategy::Adaptive,
        max_parallel: 4,
        chunk_size: 16_777_216,
        hedge_delay_ms: 100,
        timeout_secs: 30,
        auth: None,
        ca_cert_path: None,
        insecure: false,
    };
    assert!(registry.validate().is_ok());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test public_api_test`

Expected: fail because `Config`, `MirrorStrategy`, and `RegistryConfig` are not implemented.

### Task 2: Extract Historical Implementation

**Files:**
- Create/modify: `src/**`
- Create/modify: `tests/**`
- Create/modify: `examples/**`
- Modify: `Cargo.toml`

**Step 1: Restore source from history**

Extract `crates/controller/blob` from commit `3a0cfc3a261f686abf57abd4229d5f6a54dc729c` into the repo root.

**Step 2: Rebrand crate names**

Change:

- package `blob` -> `pulld`
- binary `blob-server` -> `pulld`
- imports `pulld::` -> `pulld::`
- service `PulldService` -> `PulldService`
- log/user-facing names from `blob`/`pulld` to `pulld` where public

**Step 3: Run public API test**

Run: `cargo test --test public_api_test`

Expected: pass.

### Task 3: Add Repository Metadata and Public Docs

**Files:**
- Create: `README.md`
- Create: `LICENSE`
- Create: `.gitignore`
- Modify: `Cargo.toml`

**Step 1: Add publishable package metadata**

Set package metadata for `pulld`, including description, license, repository placeholder, keywords, categories, readme, and binary target.

**Step 2: Add public README**

Document features, quick start, CLI usage, library usage, container runtime configuration hints, testing, and publishing dry-run.

### Task 4: Fix Compile and Test Failures

**Files:**
- Modify: files surfaced by compiler/test failures

**Step 1: Run full tests**

Run: `cargo test`

Expected initially: may fail from extracted crate rebrand, dependency/API drift, or network-bound tests.

**Step 2: Fix failures without removing feature coverage**

Prefer small compatibility fixes. Do not delete historical tests unless they are replaced with equivalent coverage.

**Step 3: Re-run tests**

Run: `cargo test`

Expected: pass.

### Task 5: Format, Lint, and Package

**Files:**
- Modify: formatting and lint locations

**Step 1: Run formatting check**

Run: `cargo fmt --check`

Expected: pass after formatting if needed.

**Step 2: Run clippy**

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: pass.

**Step 3: Run package dry-run**

Run: `cargo package --allow-dirty`

Expected: package builds successfully.

