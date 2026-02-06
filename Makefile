.PHONY: check check-force check-only lint format typecheck

CONTAINER := cdc-generator-dev
RUN := docker exec $(CONTAINER)

# Default target
check: typecheck lint format  ## Run all checks with safe auto-fix

check-force: typecheck lint-force format  ## Run all checks with unsafe auto-fix

check-only: typecheck-only lint-only format-only  ## Check only, no modifications

# Individual targets
typecheck:  ## Run pyright type checker
	$(RUN) pyright cdc_generator/

typecheck-only: typecheck

lint:  ## Run ruff with safe auto-fix
	$(RUN) ruff check --fix cdc_generator/

lint-force:  ## Run ruff with unsafe auto-fix
	$(RUN) ruff check --fix --unsafe-fixes cdc_generator/

lint-only:  ## Run ruff without auto-fix
	$(RUN) ruff check cdc_generator/

format:  ## Run black formatter
	$(RUN) black cdc_generator/

format-only:  ## Check formatting without modifying
	$(RUN) black --check cdc_generator/

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
