PYTEST_FLAGS=

.PHONY: setup
setup:
	pip install -r requirements/test.txt
	pre-commit install

.PHONY: lint
lint: format
	mypy neuro_extras tests setup.py

.PHONY: format
format:
	pre-commit run --all-files --show-diff-on-failure

.PHONY: test_e2e
test_e2e:
	pytest -vv -n 5 ${PYTEST_FLAGS} tests/e2e -m "not serial"
	pytest -vv -n 0 ${PYTEST_FLAGS} tests/e2e -m "serial"

.PHONY: test
test: lint test_e2e

.PHONY: changelog-draft
changelog-draft:
	towncrier --draft

.PHONY: changelog
changelog:
	towncrier
