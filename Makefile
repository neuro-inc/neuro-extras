NEURO_EXTRAS_VERSION=$(shell python setup.py --version)
NEURO_EXTRAS_PACKAGE=
PYTEST_FLAGS=

.PHONY: setup
setup:
	pip install -r requirements/test.txt

.PHONY: lint
lint:
	isort --check-only --diff neuro_extras tests setup.py
	black --check neuro_extras tests setup.py
	flake8 neuro_extras tests setup.py
	mypy neuro_extras tests setup.py

.PHONY: format
format:
	isort neuro_extras tests setup.py
	black neuro_extras tests setup.py

.PHONY: test_e2e
test_e2e:
	pytest -vv -n 10 ${PYTEST_FLAGS} tests/e2e

.PHONY: test
test: lint test_e2e

.PHONY: build
build:
	docker build -t neuromation/neuro-extras:latest \
	    --build-arg NEURO_EXTRAS_VERSION="$(NEURO_EXTRAS_VERSION)" \
	    --build-arg NEURO_EXTRAS_PACKAGE="$(NEURO_EXTRAS_PACKAGE)" \
	    .

.PHONY: changelog-draft
changelog-draft:
	towncrier --draft

.PHONY: changelog
changelog:
	towncrier