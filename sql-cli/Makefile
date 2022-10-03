.PHONY: help
.DEFAULT_GOAL:= help
SHELL := /bin/bash
SYSTEM_PIP := pip


clean:  ## Remove temporary files
	@echo "Removing cached and temporary files from current directory"
	@rm -rf logs
	@find . -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -name "*.sw[a-z]" -delete
	@find . -type d -name "*.egg-info" -exec rm -rf {} +

system:  ## Install system dependencies
	@echo "Installing system dependencies using default pip"
	@$(SYSTEM_PIP) install poetry
	@$(SYSTEM_PIP) install pre-commit

install:  ## Create Python virtualenv and install python dependencies
	@poetry install

virtualenv:  ## Activate project virtualenv
	@source $(poetry env info --path)/bin/activate

setup: system install  ## Setup a local development environment

pre-commit:  # Run pre-commit
	@git ls-files -- . | xargs pre-commit run --files

test:
	@poetry run pytest -s --cov --cov-branch --cov-report=term-missing

help:
	@echo "Makefile for the SQL CLI project"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
