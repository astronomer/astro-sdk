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

run-local-lineage-server: ## Run flask based local Lineage server
	FLASK_APP=dev/local_flask_lineage_server.py flask run --host 0.0.0.0 --port 5050

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-41s\033[0m %s\n", $$1, $$2}'
