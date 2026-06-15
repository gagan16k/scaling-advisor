# SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

MODULE_ROOT     := $(shell dirname "$(realpath $(lastword $(MAKEFILE_LIST)))")
REPO_ROOT       := $(shell dirname "$(shell dirname "$(MODULE_ROOT)")")
REPO_HACK_DIR   := $(REPO_ROOT)/hack

main_pkg_path := ./cmd/fakeactuator
binary_name   := fakeactuator

# include shared tools targets ($(GOLANGCI_LINT), $(GOIMPORTS_REVISER), ...)
include $(REPO_HACK_DIR)/tools.mk

# Defaults for `make start`. Override on the command line, e.g.:
#   make start CONFIG=testdata/config-backoff.yaml
#   make start CONFIG=testdata/config-success.yaml CONTROL_PLANE_KUBECONFIG=/path/to/cp.yaml
#
# Path is relative to $(MODULE_ROOT) (the directory `make start` cd's into).
CONFIG                    ?= testdata/config-success.yaml
CONTROL_PLANE_KUBECONFIG  ?=
DATA_PLANE_KUBECONFIG     ?=

.PHONY: tidy
tidy:
	@env GO111MODULE=on go mod tidy

.PHONY: build
build:
	mkdir -p bin
	go build -buildvcs -o=bin/$(binary_name) $(main_pkg_path)

.PHONY: format
format: $(GOIMPORTS_REVISER)
	@$(REPO_HACK_DIR)/format.sh ./...

.PHONY: check
check: $(GOLANGCI_LINT) format
	@$(REPO_HACK_DIR)/check.sh --golangci-lint-config=../../.golangci.yaml ./...

## test-unit: run all unit tests with coverage
.PHONY: test-unit
test-unit:
	go test -v -race -buildvcs -cover -coverprofile=$(MODULE_ROOT)/coverage.out ./...
	go tool cover -html=$(MODULE_ROOT)/coverage.out -o $(MODULE_ROOT)/coverage.html

## start: build and run the actuator.
##   CONFIG                   path to YAML config (default: testdata/config-success.yaml)
##   CONTROL_PLANE_KUBECONFIG optional override for control-plane kubeconfig
##   DATA_PLANE_KUBECONFIG    optional override for data-plane kubeconfig
.PHONY: start
start: build cleanup
	@cd $(MODULE_ROOT) && ./bin/$(binary_name) \
		--config=$(CONFIG) \
		$(if $(CONTROL_PLANE_KUBECONFIG),--control-plane-kubeconfig=$(CONTROL_PLANE_KUBECONFIG)) \
		$(if $(DATA_PLANE_KUBECONFIG),--data-plane-kubeconfig=$(DATA_PLANE_KUBECONFIG))

## cleanup: delete all fake nodes (name prefix "upcoming-") and their kube-node-lease leases.
##   DATA_PLANE_KUBECONFIG optional kubeconfig for the data-plane cluster (defaults to config-success.yaml value)
.PHONY: cleanup
cleanup:
	@KUBECONFIG=$${DATA_PLANE_KUBECONFIG:-$$(cd $(MODULE_ROOT) && \
		grep -m1 'dataPlaneKubeconfig:' $(CONFIG) | awk '{print $$2}' | tr -d '"')} && \
	KUBECONFIG=$${KUBECONFIG:-$$(cd $(MODULE_ROOT) && \
		grep -m1 'controlPlaneKubeconfig:' $(CONFIG) | awk '{print $$2}' | tr -d '"')} && \
	echo "Deleting fake nodes and leases (kubeconfig: $$KUBECONFIG)..." && \
	kubectl --kubeconfig="$$KUBECONFIG" get nodes \
		--no-headers -o custom-columns=NAME:.metadata.name \
		| grep '^upcoming-' \
		| xargs -r kubectl --kubeconfig="$$KUBECONFIG" delete node && \
	kubectl --kubeconfig="$$KUBECONFIG" get lease -n kube-node-lease \
		--no-headers -o custom-columns=NAME:.metadata.name \
		| grep '^upcoming-' \
		| xargs -r kubectl --kubeconfig="$$KUBECONFIG" delete lease -n kube-node-lease && \
	echo "Done."
