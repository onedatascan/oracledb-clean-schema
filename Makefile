.ONESHELL:

.PHONY: create-venv build docker-build docker-push test clean-build clean-pyc clean all

include .env
export

VENV?=.venv

create-venv:
	python -m venv $(VENV)

build:
	$(VENV)/bin/pip install -r dev-requirements.txt
	$(VENV)/bin/python -m build
	$(VENV)/bin/pip install --editable .

docker-build:
	docker build -t ${LAMBDA_DOCKER_REPO}:latest -f lambda/Dockerfile .
	docker build -t ${CLI_DOCKER_REPO}:latest -f cli/Dockerfile .

docker-push:
	docker push ${LAMBDA_DOCKER_REPO}:latest
	docker push ${CLI_DOCKER_REPO}:latest

# Expects an Oracle database
test:
	docker-compose -f tests/docker-compose.yml up -d --wait
	$(VENV)/bin/pytest

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean: clean-build clean-pyc
	docker-compose -f tests/docker-compose.yml down

all: build test docker-build docker-push clean