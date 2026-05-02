.PHONY: help install format lint test test-all coverage mutation-test check-mutants integration-test ci local-up local-down

help: ## Show command list
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install dependencies and tools, setup git hooks
	@./scripts/install.sh

format: ## Auto-format code
	@./scripts/format.sh

lint: ## Run linter checks
	@./scripts/lint.sh

test: ## Run unit tests
	@./scripts/test.sh $(ARGS)

test-all: ## Run unit and integration tests
	@./scripts/test-all.sh $(ARGS)

coverage: ## Run tests and check minimum coverage
	@./scripts/coverage.sh $(ARGS)

mutation-test: ## Run mutation tests
	@./scripts/mutation-test.sh

check-mutants: ## Run mutation tests and check mutator coverage
	@./scripts/check-mutants.sh

integration-test: ## Run integration tests (godog + testcontainers)
	@./scripts/integration-test.sh $(ARGS)

ci: install lint coverage check-mutants ## Execute all project checks

local-up: ## Start docker-compose (Kafka + LocalStack + mock services)
	docker compose up -d

local-down: ## Stop docker-compose
	docker compose down
