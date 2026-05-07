//! Unit tests for mirror racer
//!
//! Tests for mirror statistics, selection strategies, and adaptive mirror selection.

use pulld::registry::mirror_racer::{MirrorSelector, MirrorStats};
use pulld::MirrorStrategy;
use std::time::Duration;

#[test]
fn test_mirror_stats_default() {
    let stats = MirrorStats::default();
    assert_eq!(stats.rtt_ewma, 1000.0);
    assert_eq!(stats.throughput_ewma, 0.0);
    assert_eq!(stats.error_count, 0);
    assert_eq!(stats.success_count, 0);
}

#[test]
fn test_mirror_stats_record_success() {
    let mut stats = MirrorStats::default();
    stats.record_success(100.0, 1024 * 1024, Duration::from_secs(1));

    // RTT should be updated (EWMA)
    assert!(stats.rtt_ewma < 1000.0);
    assert!(stats.rtt_ewma > 0.0);

    // Throughput should be updated
    assert!(stats.throughput_ewma > 0.0);

    // Success count should increment
    assert_eq!(stats.success_count, 1);
    assert!(stats.last_success.is_some());
}

#[test]
fn test_mirror_stats_record_error() {
    let mut stats = MirrorStats::default();
    stats.record_error();

    assert_eq!(stats.error_count, 1);
    assert!(stats.last_error.is_some());
}

#[test]
fn test_mirror_stats_score() {
    let mut stats = MirrorStats::default();

    // Record successful fast request
    stats.record_success(50.0, 10_000_000, Duration::from_millis(100));
    let fast_score = stats.score();

    // Record slower request
    let mut slow_stats = MirrorStats::default();
    slow_stats.record_success(500.0, 10_000_000, Duration::from_millis(1000));
    let slow_score = slow_stats.score();

    // Fast mirror should have higher score
    assert!(fast_score > slow_score);
}

#[test]
fn test_mirror_stats_error_penalty() {
    let mut stats = MirrorStats::default();
    stats.record_success(100.0, 10_000_000, Duration::from_millis(100));
    let good_score = stats.score();

    // Add errors
    stats.record_error();
    stats.record_error();
    let bad_score = stats.score();

    // Score should decrease with errors
    assert!(bad_score < good_score);
}

#[test]
fn test_mirror_stats_success_rate() {
    let mut stats = MirrorStats::default();

    // No data - should return neutral
    assert_eq!(stats.success_rate(), 0.5);

    // All successes
    stats.record_success(100.0, 1000, Duration::from_millis(100));
    stats.record_success(100.0, 1000, Duration::from_millis(100));
    assert_eq!(stats.success_rate(), 1.0);

    // Mix of success and error
    stats.record_error();
    assert_eq!(stats.success_rate(), 2.0 / 3.0);
}

#[tokio::test]
async fn test_mirror_selector_select_mirrors_failover() {
    let selector = MirrorSelector::new(MirrorStrategy::Failover);
    let mirrors = vec![
        "https://mirror1.example.com".to_string(),
        "https://mirror2.example.com".to_string(),
        "https://mirror3.example.com".to_string(),
    ];

    let selected = selector.select_mirrors(&mirrors).await;
    assert_eq!(selected, vec![0, 1, 2]);
}

#[tokio::test]
async fn test_mirror_selector_select_mirrors_hedged() {
    let selector = MirrorSelector::new(MirrorStrategy::Hedged);
    let mirrors = vec![
        "https://mirror1.example.com".to_string(),
        "https://mirror2.example.com".to_string(),
    ];

    let selected = selector.select_mirrors(&mirrors).await;
    assert_eq!(selected, vec![0, 1]);

    // Single mirror
    let single = vec!["https://mirror1.example.com".to_string()];
    let selected = selector.select_mirrors(&single).await;
    assert_eq!(selected, vec![0]);
}

#[tokio::test]
async fn test_mirror_selector_select_mirrors_striped() {
    let selector = MirrorSelector::new(MirrorStrategy::Striped);
    let mirrors = vec![
        "https://mirror1.example.com".to_string(),
        "https://mirror2.example.com".to_string(),
        "https://mirror3.example.com".to_string(),
    ];

    let selected = selector.select_mirrors(&mirrors).await;
    assert_eq!(selected, vec![0, 1, 2]);
}

#[tokio::test]
async fn test_mirror_selector_update_stats() {
    let selector = MirrorSelector::new(MirrorStrategy::Adaptive);

    // Update with success
    selector
        .update_stats(
            "https://mirror1.example.com",
            true,
            Some(100.0),
            Some(1024 * 1024),
            Some(Duration::from_secs(1)),
        )
        .await;

    let stats = selector.get_stats("https://mirror1.example.com").await;
    assert!(stats.is_some());
    let stats = stats.unwrap();
    assert_eq!(stats.success_count, 1);
    assert!(stats.rtt_ewma < 1000.0);

    // Update with error
    selector
        .update_stats("https://mirror1.example.com", false, None, None, None)
        .await;

    let stats = selector
        .get_stats("https://mirror1.example.com")
        .await
        .unwrap();
    assert_eq!(stats.error_count, 1);
}

#[tokio::test]
async fn test_mirror_selector_adaptive_selection() {
    let selector = MirrorSelector::new(MirrorStrategy::Adaptive);
    let mirrors = vec![
        "https://mirror1.example.com".to_string(),
        "https://mirror2.example.com".to_string(),
        "https://mirror3.example.com".to_string(),
        "https://mirror4.example.com".to_string(),
    ];

    // Without stats, should return top 3 (or all if less)
    let selected = selector.select_mirrors(&mirrors).await;
    assert_eq!(selected.len(), 3);

    // With single mirror
    let single = vec!["https://mirror1.example.com".to_string()];
    let selected = selector.select_mirrors(&single).await;
    assert_eq!(selected, vec![0]);
}
