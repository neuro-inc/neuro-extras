COLOR ?= auto
PYTEST_FLAGS ?= -v
PYTEST_PARALLEL ?= auto # overwritten in CI

.PHONY: setup
setup:
	pip install -r requirements/test.txt
	pre-commit install

.PHONY: lint
lint: format
	mypy apolo_extras tests setup.py

.PHONY: format
format:
	pre-commit run --all-files --show-diff-on-failure

.PHONY: test_e2e
test_e2e:
	pytest -n $(PYTEST_PARALLEL) $(PYTEST_FLAGS) -m "(not serial) and (not smoke_only)" \
		--color=$(COLOR) tests/e2e
	pytest -n 0                  $(PYTEST_FLAGS) -m "serial and (not smoke_only)" \
		--color=$(COLOR) tests/e2e

.PHONY: test_smoke
test_smoke: test_unit test_e2e_smoke

.PHONY: test_e2e_smoke
test_e2e_smoke:
	pytest -n $(PYTEST_PARALLEL) \
		$(PYTEST_FLAGS) \
		-m "(not serial) and smoke" \
		--color=$(COLOR) \
		--runxfail \
		tests/e2e
	pytest -n 0 \
		$(PYTEST_FLAGS) \
		-m "serial and smoke" \
		--color=$(COLOR) \
		--runxfail \
		tests/e2e

.PHONY: test_data
test_data:
	pytest -n $(PYTEST_PARALLEL) \
		$(PYTEST_FLAGS) \
		-k "not serial and not smoke_only" \
		--color=$(COLOR) \
		tests/e2e/data
	pytest -n 0 \
		$(PYTEST_FLAGS) \
		-k "serial and not smoke_only" \
		--color=$(COLOR) \
		tests/e2e/data

.PHONY: test_image
test_image:
	pytest -n $(PYTEST_PARALLEL) \
		$(PYTEST_FLAGS) \
		-k "not serial and not smoke_only" \
		--color=$(COLOR) \
		tests/e2e/test_image.py
	pytest -n 0 \
		$(PYTEST_FLAGS) \
		-k "serial and not smoke_only" \
		--color=$(COLOR) \
		tests/e2e/test_image.py

.PHONY: test
test: test_unit test_e2e

.PHONY: test_unit
test_unit:
	pytest $(PYTEST_FLAGS) --color=$(COLOR) tests/unit

.PHONY: changelog-draft
changelog-draft:
	towncrier --draft --name `python setup.py --name` --version v`python setup.py --version`

.PHONY: changelog
changelog:
	towncrier --name `python setup.py --name` --version v`python setup.py --version`

.PHONY: docs
docs:
	build-tools/cli-help-generator.py CLI.in.md docs/cli.md
