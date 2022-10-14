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
	@echo "The virtual env is:" $(shell poetry env info -p)

setup: system install  ## Setup a local development environment

pre-commit:  # Run pre-commit
	@git ls-files -- . | xargs pre-commit run --files

flow:  # Build and run the latest version of flow

test:  # Run tests
	@PYTHONWARNINGS="ignore" poetry run pytest -s --cov --cov-branch --cov-report=term-missing

release:  # Build a package
	@echo -e "If this is your first time, please, manually follow these two steps: \n" \
		"1. Create a token at: https://pypi.org/manage/account/token/ \n" \
		"2. Set your token using: $ poetry config pypi-token.pypi <your-token>"
	@poetry publish --build

help:
	@echo "Makefile for the SQL CLI project"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
