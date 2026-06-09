# SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

MODULE_ROOT     := $(shell dirname "$(realpath $(lastword $(MAKEFILE_LIST)))")
REPO_ROOT                := $(shell dirname "$(MODULE_ROOT)")
REPO_HACK_DIR            := $(REPO_ROOT)/hack

main_pkg_path = ./cmd/scalingadvisor
binary_name = scalingadvisor

# include tools targets
include $(REPO_HACK_DIR)/tools.mk

.PHONY: tidy
tidy:
	@env GO111MODULE=on go mod tidy

.PHONY: build
build:
	mkdir -p bin
	go build -buildvcs -o=bin/${binary_name} ${main_pkg_path}

.PHONY: format
format: $(GOIMPORTS_REVISER)
	@$(REPO_HACK_DIR)/format.sh ./...

.PHONY: check
check: $(GOLANGCI_LINT) format
	@$(REPO_HACK_DIR)/check.sh --golangci-lint-config=../.golangci.yaml ./...

## test-unit: run all unit tests with coverage
.PHONY: test-unit
test-unit:
	go test -v -race -buildvcs -cover -coverprofile=${MODULE_ROOT}/coverage.out ./...
	go tool cover -html=${MODULE_ROOT}/coverage.out -o ${MODULE_ROOT}/coverage.html

GARDEN   ?= sap-landscape-dev
PROJECT  ?= i765230
SHOOT    ?= scad-demo

KUBECONFIG_OUT_DIR ?= $(MODULE_ROOT)/testdata
CRD_DIR    := $(REPO_ROOT)/api/core/v1alpha1/crds
CRDS       := \
	$(CRD_DIR)/sa.gardener.cloud_scalingconstraints.yaml \
	$(CRD_DIR)/sa.gardener.cloud_scalingadvices.yaml

## get-kubeconfig: download shoot kubeconfig to $(KUBECONFIG_OUT_DIR) (use MODE=cp or MODE=both for controlplane)
.PHONY: get-kubeconfig
get-kubeconfig:
	GARDEN=$(GARDEN) PROJECT=$(PROJECT) SHOOT=$(SHOOT) \
		OUT_DIR=$(KUBECONFIG_OUT_DIR) \
		$(MODULE_ROOT)/testdata/scripts/get-kubeconfig.sh $(or $(MODE),shoot)

KUBECONFIG ?= $(KUBECONFIG_OUT_DIR)/kubeconfig.yaml

.PHONY: apply-crds
apply-crds:
	@for f in $(CRDS); do KUBECONFIG=$(KUBECONFIG) kubectl apply -f $$f; done

.PHONY: delete-crds
delete-crds:
	@for f in $(CRDS); do KUBECONFIG=$(KUBECONFIG) kubectl delete --ignore-not-found -f $$f; done

.PHONY: clear-advices
clear-advice:
	KUBECONFIG=$(KUBECONFIG) kubectl delete scalingadvices.sa.gardener.cloud --all --all-namespaces
