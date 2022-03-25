.DEFAULT_GOAL := help
SHELL := /bin/bash
VENV = venv

OK := $(shell if [ -d $(VENV) ]; then echo "ok"; fi)

ifdef venv
	VENV = $(venv)
endif

ifeq ($(OK),)
	path := $(shell which python3)
	PYTHON := $(path)
	PIP := pip
else
	PYTHON = $(VENV)/bin/python3
	PIP = $(VENV)/bin/pip
endif

install:
	$(PIP) install --upgrade pip
	$(PIP) install pipx nox
	$(PIP) install '.[$(if $(collection),$(collection),all)]'
	$(PIP) install .[tests]

activate:
	. $(VENV)/bin/activate

init_venv:
	test -d venv || $(PYTHON) -m venv $(VENV)
	. $(VENV)/bin/activate

setup: init_venv activate install
	cat .env-template > .env
	cat .github/ci-test-connections.yaml > test-connections.yaml

test:
ifdef db
	pytest -s --cov-report term-missing --cov-branch -m "$(db)"
else
	pytest -s --cov-report term-missing --cov-branch
endif

unit_test:
	pytest -s --cov-report term-missing --cov-branch -m "not integration"

integration_test:
	pytest -s --cov-report term-missing --cov-branch -m integration


clean:
	rm -rf __pycache__
	rm -rf $(VENV)

help:
	@echo -e "Usage: make [command] [option]=[value]"
	@echo -e "Commands:"
	@echo -e "\t install \t install dependencies"
	@echo -e "\t activate \t activate virtual environment"
	@echo -e "\t init_venv \t initialize virtual environment \n\t\t\t Options: venv=[path] - path to virtual environment"
	@echo -e "\t setup \t initialize virtual environment and install dependencies. \n\t\t\t Options: collection=[collection] - run tests for specific collection"
	@echo -e "\t test \t run tests \n\t\t\t Options: db=[db] - run tests for specific database"
	@echo -e "\t unit_test \t run unit tests"
	@echo -e "\t integration_test \t run integration tests"
	@echo -e "\t clean \t remove virtual environment"
	@echo -e "\t help \t show this help"

quality:
	pre-commit run --all-files
