.PHONY: help
.DEFAULT_GOAL:= help
SHELL := /bin/bash
PROJECT_NAME := astro-sdk
SYSTEM_PYTHON := python3.9

# Set default virtualenv path, if not defined
ifndef VIRTUALENV_PATH
$(shell mkdir -p ~/.virtualenvs/)
override VIRTUALENV_PATH = ~/.virtualenvs/$(PROJECT_NAME)
endif

PYTHON = $(VIRTUALENV_PATH)/bin/python
PIP = $(VIRTUALENV_PATH)/bin/pip
PYTEST = $(VIRTUALENV_PATH)/bin/pytest
PRECOMMIT = $(VIRTUALENV_PATH)/bin/pre-commit


clean: ## Remove temporary files
	@echo "Removing cached and temporary files from current directory"
	@rm -rf logs
	@find . -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -name "*.sw[a-z]" -delete
	@find . -type d -name "*.egg-info" -exec rm -rf {} +

virtualenv:  ## Create Python virtualenv
	@test -d $(VIRTUALENV_PATH) && \
	(echo "The virtualenv $(VIRTUALENV_PATH) already exists. Skipping.") || \
	(echo "Creating the virtualenv $(VIRTUALENV_PATH) using $(SYSTEM_PYTHON)" & \
	$(SYSTEM_PYTHON) -m venv $(VIRTUALENV_PATH))

install: virtualenv  ## Install python dependencies in existing virtualenv
	@echo "Installing Python dependencies using $(PIP)"
	@$(PIP) install --upgrade pip
	@$(PIP) install nox
	@$(PIP) install pre-commit
	@$(PIP) install -e .[all]
	@$(PIP) install .[tests]

config:  ## Create sample configuration files related to Snowflake, Amazon and Google
	@test -e .env && \
		(echo "The file .env already exist. Skipping.") || \
		(echo "Creating .env..." && \
		cat .env-template > .env && \
		echo "Please, update .env with your credentials")
	@test -e test-connections.yaml && \
		(echo "The file test-connections.yaml already exist. Skipping.") || \
		(echo "Creating test-connections.yaml..." && \
		cat .github/ci-test-connections.yaml > test-connections.yaml && \
		echo "Please, update test-connections.yaml with your credentials")

setup: config virtualenv install ## Setup a local development environment

quality:
	@$(PRECOMMIT) run --all-files

test: virtualenv config ## Run all tests (use option: db=[db] run only run database-specific ones)
ifdef db
	@$(PYTEST) -s --cov --cov-branch --cov-report=term-missing -m "$(db)"
else
	@$(PYTEST) -s --cov --cov-branch --cov-report=term-missing
endif

unit: virtualenv config ## Run unit tests
	@$(PYTEST) -s --cov --cov-branch --cov-report=term-missing -m "not integration"

integration: virtualenv config  ## Run integration tests
	@$(PYTEST) -s --cov --cov-branch --cov-report=term-missing -m integration

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'