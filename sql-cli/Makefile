.PHONY: help
.DEFAULT_GOAL:= help
SHELL := /bin/bash

PROJECT_NAME := sql-cli
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


clean:  ## Remove temporary files
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


system:  ## Install system dependencies
	@echo "Installing system dependencies using default pip"
	@$(PIP) install nox
	@$(PIP) install pre-commit


install: virtualenv  ## Install python dependencies in existing virtualenv
	@echo "Installing Python dependencies using $(PIP)"
	@$(PIP) install --upgrade pip
	@$(PIP) install nox
	@$(PIP) install pre-commit
	@$(PIP) install -e .[all]
	@$(PIP) install .[tests]

setup: system install  ## Setup a local development environment

pre-commit:  # Run pre-commit
	@git ls-files -- . | xargs pre-commit run --files

test:  # Run tests
	@nox -s "test-3.8"

help:
	@echo "Makefile for the SQL CLI project"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
