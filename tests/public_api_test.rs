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
