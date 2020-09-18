PYTEST_FLAGS=

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
	pytest -vv --maxfail=3 ${PYTEST_FLAGS} tests/e2e -s -k test_image

test: lint test_e2e

build:
	docker build -t neuromation/neuro-extras:latest \
	    --build-arg NEURO_EXTRAS_VERSION="$(shell python setup.py --version)" \
	    .
