COMPOSE = docker compose -f docker/compose.dev.yml
RUN     = $(COMPOSE) run --rm dev

.PHONY: help deps compile test coverage format build clean shell

help: ## Show available targets
	@grep -E '^[a-zA-Z_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

deps: ## Install dependencies
	$(RUN) mix deps.get && $(RUN) mix deps.compile

compile: ## Compile the project
	$(RUN) mix compile

test: ## Run the test suite
	$(RUN) mix test

coverage: ## Run tests with coverage report
	$(RUN) mix coveralls

format: ## Format source code
	$(RUN) mix format

build: ## Build the release binary (requires Zig; sets BURRITO_WRAP=true)
	MIX_ENV=prod BURRITO_WRAP=true mix release

shell: ## Drop into an interactive shell inside the dev container
	$(RUN) sh

clean: ## Remove build artifacts
	$(RUN) mix clean
