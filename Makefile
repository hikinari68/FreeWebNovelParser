install:
	pip install -Ur requirements.txt

install-dev: install install-nuclei
	pip install -Ur requirements-dev.txt

black:
	black .

black-check:
	black --check --diff .

isort:
	isort --profile black .

isort-check:
	isort --profile black --check-only --diff .

mypy-check:
	mypy --config-file mypy.ini .

lint: mypy-check isort-check black-check

test:
	python -m pytest -x -vv --disable-pytest-warnings

test-ci: lint test

style: isort black
