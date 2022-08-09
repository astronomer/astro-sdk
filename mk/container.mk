PHONY: build-run clean logs stop help

.DEFAULT_GOAL:= help

logs: ## View logs of the all the containers
	docker-compose -f ../dev/docker-compose.yaml logs --follow

stop: ## Stop all the containers
	docker-compose -f ../dev/docker-compose.yaml down

clean: ## Remove all the containers along with volumes
	docker-compose -f ../dev/docker-compose.yaml down  --volumes --remove-orphans
	rm -rf dev/logs

build-run:     ## Build the Docker Image & then run the containers
	docker-compose -f ../dev/docker-compose.yaml up --build -d

help:  ## Prints this message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-41s\033[0m %s\n", $$1, $$2}'
