.PHONY: clean \
	clean-dev update-dev-req install-dev-req install touch-dev \
	check format check-format lint check-typing clean-test test-wo-install test \
	clean-doc doc \
	clean-build build-release check-release release

VIRTUAL_ENV = .venv

DEV_BUILD_FLAG = $(VIRTUAL_ENV)/DEV_BUILD_FLAG



clean: clean-dev clean-test clean-doc clean-build



# init

clean-dev:
	rm -rf $(VIRTUAL_ENV)

update-dev-req: $(DEV_BUILD_FLAG)
	$(VIRTUAL_ENV)/bin/pip-compile --upgrade dev-requirements.in

install-dev-req:
	python3 -m venv $(VIRTUAL_ENV)
	$(VIRTUAL_ENV)/bin/python -m pip install --upgrade pip
	$(VIRTUAL_ENV)/bin/pip install -r dev-requirements.txt
	$(VIRTUAL_ENV)/bin/pre-commit install --hook-type pre-commit --hook-type pre-push

install:
	$(VIRTUAL_ENV)/bin/python setup.py install

touch-dev:
	touch $(DEV_BUILD_FLAG)

dev: $(DEV_BUILD_FLAG)

$(DEV_BUILD_FLAG):
	$(MAKE) -f Makefile install-dev-req
	$(MAKE) -f Makefile install
	$(MAKE) -f Makefile touch-dev



# ci

check: check-format lint check-typing test

format: $(DEV_BUILD_FLAG)
	$(VIRTUAL_ENV)/bin/black src tests setup.py

check-format: $(DEV_BUILD_FLAG)
	$(VIRTUAL_ENV)/bin/black --check src tests setup.py

lint: $(DEV_BUILD_FLAG)
	$(VIRTUAL_ENV)/bin/pylint src tests setup.py

check-typing: $(DEV_BUILD_FLAG)
	$(VIRTUAL_ENV)/bin/mypy src tests setup.py

clean-test:
	rm -rf tests/.unittest
	rm -rf docs/coverage

test-wo-install: $(DEV_BUILD_FLAG)
	rm -rf tests/.unittest
	mkdir -p docs/coverage
	$(VIRTUAL_ENV)/bin/coverage run --data-file docs/coverage/.coverage --branch --source writecondastat -m unittest discover tests
	$(VIRTUAL_ENV)/bin/coverage html --data-file docs/coverage/.coverage -d docs/coverage
	$(VIRTUAL_ENV)/bin/coverage report --data-file docs/coverage/.coverage -m --fail-under=0

test: $(DEV_BUILD_FLAG) install test-wo-install



# doc

clean-doc:
	rm -rf docs

doc: $(DEV_BUILD_FLAG)
	$(VIRTUAL_ENV)/bin/pdoc --docformat google src/writecondastat -o docs



# release

clean-build:
	rm -rf build
	rm -rf dist
	rm -rf `find . -name '*.egg-info'`
	rm -rf `find . -name '__pycache__'`

build-release: $(DEV_BUILD_FLAG)
	$(VIRTUAL_ENV)/bin/python -m build

check-release: $(DEV_BUILD_FLAG)
	$(VIRTUAL_ENV)/bin/python -m twine check dist/*.tar.gz dist/*.whl

release: clean-build build-release check-release