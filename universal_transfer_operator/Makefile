.PHONY: help

.DEFAULT_GOAL:= help

target = help

ifdef "$(target)"
    target = $(target)
endif

container:  ## Set up Airflow in container
	@$(MAKE) -C mk -f container.mk $(target)

local: ## Set up local dev env
	@$(MAKE) -C mk -f local.mk $(target)

tilt-up: ## Set up local dev env with Tilt
	tilt up

tilt-down: ## Tear down local dev env with Tilt
	tilt down

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-41s\033[0m %s\n", $$1, $$2}'
