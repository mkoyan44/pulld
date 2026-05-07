use pulld::admission::{build_pod_image_patches, rewrite_image};
use serde_json::json;

#[test]
fn rewrites_unqualified_docker_hub_image_to_internal_registry() {
    assert_eq!(
        rewrite_image("alpine:3.20", "pulld.4lock.net").as_deref(),
        Some("pulld.4lock.net/docker.io/library/alpine:3.20")
    );
}

#[test]
fn rewrites_explicit_registry_image_to_embedded_registry_path() {
    assert_eq!(
        rewrite_image("quay.io/cilium/cilium:v1.17.7", "pulld.4lock.net").as_deref(),
        Some("pulld.4lock.net/quay.io/cilium/cilium:v1.17.7")
    );
}

#[test]
fn skips_image_already_pointing_at_internal_registry() {
    assert_eq!(
        rewrite_image(
            "pulld.4lock.net/docker.io/library/alpine:3.20",
            "pulld.4lock.net"
        ),
        None
    );
}

#[test]
fn builds_json_patches_for_all_pod_container_lists() {
    let pod = json!({
        "spec": {
            "initContainers": [
                {"name": "init", "image": "busybox:1.36"}
            ],
            "containers": [
                {"name": "app", "image": "registry.k8s.io/pause:3.10"},
                {"name": "sidecar", "image": "pulld.4lock.net/docker.io/library/alpine:3.20"}
            ],
            "ephemeralContainers": [
                {"name": "debug", "image": "ghcr.io/example/debug:latest"}
            ]
        }
    });

    let patches = build_pod_image_patches(&pod, "pulld.4lock.net");
    let patch_values = serde_json::to_value(patches).unwrap();

    assert_eq!(
        patch_values,
        json!([
            {
                "op": "replace",
                "path": "/spec/initContainers/0/image",
                "value": "pulld.4lock.net/docker.io/library/busybox:1.36"
            },
            {
                "op": "replace",
                "path": "/spec/containers/0/image",
                "value": "pulld.4lock.net/registry.k8s.io/pause:3.10"
            },
            {
                "op": "replace",
                "path": "/spec/ephemeralContainers/0/image",
                "value": "pulld.4lock.net/ghcr.io/example/debug:latest"
            }
        ])
    );
}
