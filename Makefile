.DEFAULT_GOAL := help

GO ?= go
GO_VERSION ?= 1.26.5
TOOLS_DIR ?= $(CURDIR)/.bin
GOLANGCI_LINT_VERSION ?= v2.12.2
GOVULNCHECK_VERSION ?= v1.6.0

GO_CMD = GOTOOLCHAIN=go$(GO_VERSION) $(GO)
GOLANGCI_LINT = $(TOOLS_DIR)/golangci-lint/$(GOLANGCI_LINT_VERSION)/golangci-lint
GOVULNCHECK = $(TOOLS_DIR)/govulncheck/$(GOVULNCHECK_VERSION)/govulncheck

.PHONY: help setup tools mod-download fmt lint test test-race coverage vuln verify check

help:
	@printf '%s\n' \
		'make setup      Download modules and install development tools' \
		'make fmt        Format Go source files' \
		'make lint       Run golangci-lint' \
		'make test       Run unit tests' \
		'make test-race  Run unit tests with the race detector' \
		'make coverage   Generate coverage.out with the race detector' \
		'make vuln       Scan dependencies with govulncheck' \
		'make verify     Verify and tidy-check Go modules' \
		'make check      Run all CI checks'

setup: mod-download tools

tools: $(GOLANGCI_LINT) $(GOVULNCHECK)

mod-download:
	$(GO_CMD) mod download

$(GOLANGCI_LINT): Makefile
	mkdir -p "$(dir $@)"
	GOBIN="$(dir $@)" $(GO_CMD) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

$(GOVULNCHECK): Makefile
	mkdir -p "$(dir $@)"
	GOBIN="$(dir $@)" $(GO_CMD) install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

fmt: $(GOLANGCI_LINT)
	"$(GOLANGCI_LINT)" fmt

lint: $(GOLANGCI_LINT)
	"$(GOLANGCI_LINT)" run

test:
	$(GO_CMD) test ./...

test-race:
	$(GO_CMD) test -race ./...

coverage:
	$(GO_CMD) test -race -coverprofile=coverage.out -covermode=atomic ./...

vuln: $(GOVULNCHECK)
	"$(GOVULNCHECK)" ./...

verify:
	$(GO_CMD) mod verify
	$(GO_CMD) mod tidy -diff

check: verify lint test-race vuln
