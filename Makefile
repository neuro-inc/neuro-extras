PYTEST_FLAGS = -vv
PYTEST_PARALLEL = 5

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
	pytest -n ${PYTEST_PARALLEL} ${PYTEST_FLAGS} -m "not serial" tests/e2e
	pytest -n 0                  ${PYTEST_FLAGS} -m "serial"     tests/e2e

.PHONY: test
test: lint test_e2e

.PHONY: changelog-draft
changelog-draft:
	towncrier --draft

.PHONY: changelog
changelog:
	towncrier
