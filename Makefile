HELM_CHART ?= charts/pulld
CHART_DEST ?= target/charts

.PHONY: fmt test clippy helm-lint helm-template helm-template-no-persistence helm-package verify

fmt:
	cargo fmt --check

test:
	cargo test

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

helm-lint:
	helm lint $(HELM_CHART)

helm-template:
	helm template pulld $(HELM_CHART) --namespace pulld >/dev/null

helm-template-no-persistence:
	helm template pulld $(HELM_CHART) --namespace pulld --set persistence.enabled=false >/dev/null

helm-package:
	mkdir -p $(CHART_DEST)
	helm package $(HELM_CHART) --destination $(CHART_DEST)

verify: fmt test clippy helm-lint helm-template helm-template-no-persistence helm-package
