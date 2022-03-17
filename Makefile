.DEFAULT_GOAL := help
SHELL := /bin/bash
VENV = venv

OK := $(shell if [ -d $(VENV) ]; then echo "ok"; fi)



ifeq ($(OK),)
	path := $(shell which python3)
	PYTHON := $(path)
	PIP := pip
else
	PYTHON = $(VENV)/bin/python3
	PIP = $(VENV)/bin/pip
endif

b:
	@echo $(OK)
	@echo $(PYTHON)
	@echo $(PIP)

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
	pytest -s --cov-report term --cov-branch -m "$(db)"
else
	pytest -s --cov-report term --cov-branch
endif

unit_test:
	pytest -s --cov-report term --cov-branch -m "not integration"

integration_test:
	pytest -s --cov-report term --cov-branch -m integration
		

clean:
	rm -rf __pycache__
	rm -rf $(VENV)

help:
	@echo "Use commands install, setup, init_venv, clean"

quality:
	pre-commit run --all-files