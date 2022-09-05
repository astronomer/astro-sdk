PHONY: build-run clean docs logs stop shell restart restart-all help

.DEFAULT_GOAL:= help

logs: ## View logs of the all the containers
	docker compose -f ../dev/docker-compose.yaml logs --follow

stop: ## Stop all the containers
    docker compose -f ../dev/docker-compose.yaml down

clean: ## Remove all the containers along with volumes
    docker compose -f ../dev/docker-compose.yaml down  --volumes --remove-orphans
    rm -rf dev/logs

build-run:     ## Build the Docker Image & then run the containers
    docker compose -f ../dev/docker-compose.yaml up --build -d

docs:  ## Build the docs using Sphinx
    docker compose -f ../dev/docker-compose.yaml build
    docker compose -f ../dev/docker-compose.yaml run --entrypoint /bin/bash airflow-init -c "cd astro_sdk/docs && make clean html"
    @echo "Documentation built in $(shell cd .. && pwd)/docs/_build/html/index.html"

restart: ## Restart Triggerer, Scheduler and Worker containers
    docker compose -f dev/docker-compose.yaml restart airflow-triggerer airflow-scheduler airflow-worker

restart-all: ## Restart all the containers
    docker compose -f dev/docker-compose.yaml restart

shell:  ## Runs a shell within a container (Allows interactive session)
    docker compose -f ../dev/docker-compose.yaml run --rm airflow-scheduler bash

help:  ## Prints this message
    @grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-41s\033[0m %s\n", $$1, $$2}'
