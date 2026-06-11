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

# Defaults for `make start`. Override on the command line, e.g.
#   make start KUBECONFIG=/path/to/kc.yaml CREATION_DELAY=5s
# Path is relative to $(MODULE_ROOT) (the directory `make start` cd's into).
KUBECONFIG     ?= ../../operator/testdata/kubeconfig.yaml
CREATION_DELAY ?= 30s

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

## start: build and run the actuator. Override KUBECONFIG / CREATION_DELAY to
##        point at a different cluster or use a different ack delay. A YAML
##        config + parser can be re-introduced later if more knobs land.
.PHONY: start
start: build
	@cd $(MODULE_ROOT) && ./bin/$(binary_name) \
		--kubeconfig=$(KUBECONFIG) \
		--creation-delay=$(CREATION_DELAY)
