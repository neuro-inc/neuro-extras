COLOR ?= auto
PYTEST_FLAGS = -v
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
	pytest -n ${PYTEST_PARALLEL} ${PYTEST_FLAGS} -m "not serial" \
		--color=$(COLOR) tests/e2e
	pytest -n 0                  ${PYTEST_FLAGS} -m "serial" \
		--color=$(COLOR) tests/e2e

.PHONY: test
test: test_unit test_e2e

.PHONY: test_unit
test_unit:
	pytest ${PYTEST_FLAGS} --color=$(COLOR) tests/unit

.PHONY: changelog-draft
changelog-draft:
	towncrier --draft --name `python setup.py --name` --version v`python setup.py --version`

.PHONY: changelog
changelog:
	towncrier --name `python setup.py --name` --version v`python setup.py --version`

.PHONY: docs
docs:
	build-tools/cli-help-generator.py CLI.in.md docs/cli.md
