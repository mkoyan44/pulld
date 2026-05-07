use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionReview {
    pub api_version: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<AdmissionRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<AdmissionResponse>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionRequest {
    pub uid: String,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub operation: String,
    pub object: Value,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionResponse {
    pub uid: String,
    pub allowed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<AdmissionStatus>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AdmissionStatus {
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JsonPatchOperation {
    pub op: &'static str,
    pub path: String,
    pub value: String,
}

pub fn mutate_pod_review(mut review: AdmissionReview, registry_endpoint: &str) -> AdmissionReview {
    let Some(request) = review.request.take() else {
        review.response = Some(AdmissionResponse {
            uid: String::new(),
            allowed: false,
            patch: None,
            patch_type: None,
            status: Some(AdmissionStatus {
                message: "AdmissionReview request is missing".to_string(),
            }),
        });
        return review;
    };

    let patches = build_pod_image_patches(&request.object, registry_endpoint);
    let patch = if patches.is_empty() {
        None
    } else {
        match serde_json::to_vec(&patches) {
            Ok(bytes) => Some(STANDARD.encode(bytes)),
            Err(error) => {
                review.response = Some(AdmissionResponse {
                    uid: request.uid,
                    allowed: false,
                    patch: None,
                    patch_type: None,
                    status: Some(AdmissionStatus {
                        message: format!("failed to encode image rewrite patch: {error}"),
                    }),
                });
                return review;
            }
        }
    };

    if patches.is_empty() {
        tracing::info!(
            uid = %request.uid,
            namespace = request.namespace.as_deref().unwrap_or(""),
            pod = request.name.as_deref().unwrap_or(""),
            "Admission image rewrite request allowed without changes"
        );
    } else {
        tracing::info!(
            uid = %request.uid,
            namespace = request.namespace.as_deref().unwrap_or(""),
            pod = request.name.as_deref().unwrap_or(""),
            patch_count = patches.len(),
            registry_endpoint = %normalize_registry_endpoint(registry_endpoint),
            "Admission image rewrite patch generated"
        );
    }

    review.response = Some(AdmissionResponse {
        uid: request.uid,
        allowed: true,
        patch,
        patch_type: if patches.is_empty() {
            None
        } else {
            Some("JSONPatch".to_string())
        },
        status: None,
    });
    review
}

pub fn build_pod_image_patches(pod: &Value, registry_endpoint: &str) -> Vec<JsonPatchOperation> {
    let mut patches = Vec::new();
    collect_container_patches(pod, "/spec/initContainers", &mut patches, registry_endpoint);
    collect_container_patches(pod, "/spec/containers", &mut patches, registry_endpoint);
    collect_container_patches(
        pod,
        "/spec/ephemeralContainers",
        &mut patches,
        registry_endpoint,
    );
    patches
}

fn collect_container_patches(
    pod: &Value,
    pointer: &str,
    patches: &mut Vec<JsonPatchOperation>,
    registry_endpoint: &str,
) {
    let Some(containers) = pod.pointer(pointer).and_then(Value::as_array) else {
        return;
    };

    for (index, container) in containers.iter().enumerate() {
        let Some(image) = container.get("image").and_then(Value::as_str) else {
            continue;
        };

        if let Some(value) = rewrite_image(image, registry_endpoint) {
            patches.push(JsonPatchOperation {
                op: "replace",
                path: format!("{pointer}/{index}/image"),
                value,
            });
        }
    }
}

pub fn rewrite_image(image: &str, registry_endpoint: &str) -> Option<String> {
    let endpoint = normalize_registry_endpoint(registry_endpoint);
    if endpoint.is_empty() || image.is_empty() {
        return None;
    }

    if image == endpoint || image.starts_with(&format!("{endpoint}/")) {
        return None;
    }

    let normalized_image = normalize_upstream_image(image);
    Some(format!("{endpoint}/{normalized_image}"))
}

fn normalize_registry_endpoint(endpoint: &str) -> String {
    endpoint
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string()
}

fn normalize_upstream_image(image: &str) -> String {
    let Some((first, remainder)) = image.split_once('/') else {
        return format!("docker.io/library/{image}");
    };

    if is_explicit_registry(first) {
        if is_docker_hub_registry(first) {
            return normalize_docker_hub_image(remainder);
        }
        return image.to_string();
    }

    format!("docker.io/{image}")
}

fn normalize_docker_hub_image(repository: &str) -> String {
    if repository.contains('/') {
        format!("docker.io/{repository}")
    } else {
        format!("docker.io/library/{repository}")
    }
}

fn is_explicit_registry(first_component: &str) -> bool {
    first_component.contains('.') || first_component.contains(':') || first_component == "localhost"
}

fn is_docker_hub_registry(registry: &str) -> bool {
    matches!(
        registry,
        "docker.io" | "index.docker.io" | "registry-1.docker.io"
    )
}
