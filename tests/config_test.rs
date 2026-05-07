//! Unit tests for configuration
//!
//! Tests for mirror strategy parsing, registry config validation, and defaults.

use pulld::config::{MirrorStrategy, RegistryConfig};
use std::str::FromStr;

#[test]
fn test_mirror_strategy_from_str() {
    assert_eq!(
        MirrorStrategy::from_str("failover").unwrap(),
        MirrorStrategy::Failover
    );
    assert_eq!(
        MirrorStrategy::from_str("hedged").unwrap(),
        MirrorStrategy::Hedged
    );
    assert_eq!(
        MirrorStrategy::from_str("striped").unwrap(),
        MirrorStrategy::Striped
    );
    assert_eq!(
        MirrorStrategy::from_str("adaptive").unwrap(),
        MirrorStrategy::Adaptive
    );
    assert!(MirrorStrategy::from_str("fastest").is_err());
    assert!(MirrorStrategy::from_str("FASTEST").is_err());
    assert!(MirrorStrategy::from_str("invalid").is_err());
}

#[test]
fn test_mirror_strategy_deserialize() {
    // Test all valid strategies
    for (strategy_str, expected) in [
        ("failover", MirrorStrategy::Failover),
        ("hedged", MirrorStrategy::Hedged),
        ("striped", MirrorStrategy::Striped),
        ("adaptive", MirrorStrategy::Adaptive),
    ] {
        let toml_str = format!(r#"strategy = "{}""#, strategy_str);
        let config: RegistryConfig = toml::from_str(&format!(
            r#"
mirrors = ["https://registry.example.com"]
{}
"#,
            toml_str
        ))
        .unwrap();
        assert_eq!(
            config.strategy, expected,
            "Failed for strategy: {}",
            strategy_str
        );
    }

    // Test that "fastest" is rejected
    let toml_str = r#"strategy = "fastest""#;
    let result: Result<RegistryConfig, _> = toml::from_str(&format!(
        r#"
mirrors = ["https://registry.example.com"]
{}
"#,
        toml_str
    ));
    assert!(result.is_err(), "fastest strategy should be rejected");
}

#[test]
fn test_registry_config_validation() {
    // Valid config
    let valid = RegistryConfig {
        mirrors: vec!["https://registry.example.com".to_string()],
        strategy: MirrorStrategy::Adaptive,
        max_parallel: 4,
        chunk_size: 16_777_216,
        hedge_delay_ms: 100,
        timeout_secs: 30,
        auth: None,
        ca_cert_path: None,
        insecure: false,
    };
    assert!(valid.validate().is_ok());

    // Invalid config (empty mirrors)
    let invalid = RegistryConfig {
        mirrors: vec![],
        strategy: MirrorStrategy::Adaptive,
        max_parallel: 4,
        chunk_size: 16_777_216,
        hedge_delay_ms: 100,
        timeout_secs: 30,
        auth: None,
        ca_cert_path: None,
        insecure: false,
    };
    assert!(invalid.validate().is_err());
}

#[test]
fn test_registry_config_defaults() {
    let toml_str = r#"
mirrors = ["https://registry.example.com"]
"#;
    let config: RegistryConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.strategy, MirrorStrategy::Adaptive);
    assert_eq!(config.max_parallel, 4);
    assert_eq!(config.chunk_size, 16_777_216);
    assert_eq!(config.hedge_delay_ms, 100);
    assert_eq!(config.timeout_secs, 30);
}

#[test]
fn test_mirror_strategy_default() {
    assert_eq!(MirrorStrategy::default(), MirrorStrategy::Adaptive);
}
