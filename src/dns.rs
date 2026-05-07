//! IPv4-preferred DNS resolver for upstream registry connections.
//!
//! When the system resolver returns IPv6-only (e.g. for registry-1.docker.io) but the host
//! has no IPv6 path to the internet, reqwest fails with "error sending request". This resolver
//! returns only IPv4 addresses when available so connections succeed on IPv4-only networks.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

fn box_err(
    e: impl std::error::Error + Send + Sync + 'static,
) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(e)
}

/// Resolver that returns IPv4 addresses first (or only), then falls back to all addresses.
/// Use for upstream registry clients so VMs with IPv6-only DNS but no IPv6 route can still pull.
#[derive(Debug, Default, Clone)]
pub struct Ipv4PreferResolver;

impl Resolve for Ipv4PreferResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        let host_fallback = host.clone();
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = tokio::task::spawn_blocking(move || {
                (host.as_str(), 0u16)
                    .to_socket_addrs()
                    .map(|it| it.collect::<Vec<_>>())
            })
            .await
            .map_err(box_err)?
            .map_err(box_err)?;

            let v4: Vec<SocketAddr> = addrs.into_iter().filter(|a| a.is_ipv4()).collect();
            let addrs = if v4.is_empty() {
                // No IPv4: re-resolve and return all (fallback for IPv6-only hosts)
                tokio::task::spawn_blocking(move || {
                    (host_fallback.as_str(), 0u16)
                        .to_socket_addrs()
                        .map(|it| it.collect::<Vec<_>>())
                })
                .await
                .map_err(box_err)?
                .map_err(box_err)?
            } else {
                v4
            };

            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

/// Shared IPv4-prefer resolver for use with Client::builder().dns_resolver(...).
pub fn ipv4_prefer_resolver() -> Arc<Ipv4PreferResolver> {
    Arc::new(Ipv4PreferResolver)
}
