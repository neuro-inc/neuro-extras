PYTEST_FLAGS=

export PIP_EXTRA_INDEX_URL ?= $(shell python pip_extra_index_url.py)

setup:
	pip install -r requirements/test.txt -c requirements/constraints.txt

lint:
	black --check neuro_extras tests setup.py
	flake8 neuro_extras tests setup.py
	mypy neuro_extras tests setup.py

format:
	isort -rc neuro_extras tests setup.py
	black neuro_extras tests setup.py

test_e2e:
	pytest -vv --maxfail=3 ${PYTEST_FLAGS} tests/e2e

test: lint test_e2e

devpi_setup:
	pip install --user -U devpi-client wheel setuptools
	@devpi use $(PIP_EXTRA_INDEX_URL)/$(DEVPI_INDEX)

devpi_login:
	@devpi login $(DEVPI_USER) --password=$(DEVPI_PASS)

devpi_upload: devpi_login
	devpi upload --formats bdist_wheel

build:
	docker build -t neuromation/neuro-extras:latest \
	    --build-arg PIP_EXTRA_INDEX_URL \
	    --build-arg NEURO_EXTRAS_VERSION="$(shell python setup.py --version)" \
	    .
