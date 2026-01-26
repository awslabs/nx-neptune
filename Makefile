.ONESHELL:
SHELL := /bin/bash
ENV_PREFIX=$(shell python -c "if __import__('pathlib').Path('.venv/bin/pip').exists(): print('.venv/bin/')")
USING_POETRY=$(shell grep "tool.poetry" pyproject.toml && echo "yes")

.PHONY: help
help:             ## Show the help.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@fgrep "##" Makefile | fgrep -v fgrep


.PHONY: show
show:             ## Show the current environment.
	@echo "Current environment:"
	@if [ "$(USING_POETRY)" ]; then poetry env info && exit; fi
	@echo "Running using $(ENV_PREFIX)"
	@$(ENV_PREFIX)python -V
	@$(ENV_PREFIX)python -m site

.PHONY: install
install:          ## Install the project for examples.
	@if [ "$(USING_POETRY)" ]; then poetry install && exit; fi
	@echo "Don't forget to run 'make virtualenv' if you got errors."
	$(ENV_PREFIX)pip install -e .[jupyter]

.PHONY: install-dev
install-dev: install          ## Install the project in dev mode.
	@if [ "$(USING_POETRY)" ]; then poetry install && exit; fi
	@echo "Don't forget to run 'make virtualenv' if you got errors."
	$(ENV_PREFIX)pip install -e .[test,developer]


.PHONY: dist
dist: ## install the distribution
	@echo "Building distribution:"
	$(ENV_PREFIX)pip install -e .
	$(ENV_PREFIX)python -m pip wheel -w dist . --no-deps

.PHONY: fmt
fmt:              ## Format code using black & isort.
	$(ENV_PREFIX)isort nx_neptune/ nx_plugin/
	$(ENV_PREFIX)black nx_neptune/ nx_plugin/ tests/

.PHONY: lint
lint:             ## Run flake8, black, mypy linters.
    ## import imports: plugin imports are available for external use
	$(ENV_PREFIX)flake8 nx_neptune/ nx_plugin/
	$(ENV_PREFIX)black --check nx_neptune/ nx_plugin/
	$(ENV_PREFIX)black --check tests/
	$(ENV_PREFIX)mypy --ignore-missing-imports nx_neptune/ nx_plugin/

.PHONY: test
test: lint        ## Run tests and generate coverage report.
	$(ENV_PREFIX)pytest -v --cov-config=.coveragerc --cov=nx_neptune -l --tb=short --maxfail=1 --cov-fail-under=70 tests/
	$(ENV_PREFIX)coverage xml
	$(ENV_PREFIX)coverage html

.PHONY: integ-test
integ-test:
	$(ENV_PREFIX)pytest -v -l --tb=short --maxfail=1 integ_test/

.PHONY: watch
watch:            ## Run tests on every change.
	ls **/**.py | entr $(ENV_PREFIX)pytest -s -vvv -l --tb=long --maxfail=1 tests/

.PHONY: clean
clean:            ## Clean unused files.
	@find ./ -name '*.pyc' -exec rm -f {} \;
	@rm -rf .cache
	@rm -rf .pytest_cache
	@rm -rf .mypy_cache
	@rm -rf build
	@rm -rf dist
	@rm -rf *.egg-info
	@rm -rf htmlcov
	@rm -rf .tox/
	$(ENV_PREFIX)pip freeze -l | grep . && $(ENV_PREFIX)pip uninstall -y -r <($(ENV_PREFIX)pip freeze -l)

.PHONY: virtualenv
virtualenv:       ## Create a virtual environment.
	@if [ "$(USING_POETRY)" ]; then poetry install && exit; fi
	@echo "creating virtualenv ..."
	@rm -rf .venv
	@python3 -m venv .venv
	@./.venv/bin/pip install -U pip
	@echo
	@echo "!!! Please run 'source .venv/bin/activate' to enable the environment !!!"

.PHONY: release
release:          ## Create a new tag for release.
	@echo "WARNING: This operation will create s version tag and push to github"
	@read -p "Version? (provide the next x.y.z semver) : " TAG
	@echo "$${TAG}" > nx_neptune/VERSION
	@$(ENV_PREFIX)gitchangelog > HISTORY.md
	@git add nx_neptune/VERSION HISTORY.md
	@git commit -m "release: version $${TAG} 🚀"
	@echo "creating git tag : $${TAG}"
	@git tag $${TAG}
	@git push -u origin HEAD --tags
	@echo "Github Actions will detect the new tag and release the new version."


.PHONY: doc-sphinx
doc-sphinx:             ## Build the documentation.
	@echo "building Sphinx documentation ..."
	@$(ENV_PREFIX)sphinx-build -M html ./doc ./sphinx_output


.PHONY: license-check
license-check:             ## Perform license check on core dependencies
	@echo "license check ..."
	$(ENV_PREFIX)pip install -e . pip-licenses
	$(ENV_PREFIX)pip-licenses

.PHONY: clear-notebook-output
clear-notebook-output:             ## Clear notebook output cells
	@echo "Clear notebook output cells ..."
	$(ENV_PREFIX)nbstripout notebooks/*

.PHONY: switch-to-poetry
switch-to-poetry: ## Switch to poetry package manager.
	@echo "Switching to poetry ..."
	@if ! poetry --version > /dev/null; then echo 'poetry is required, install from https://python-poetry.org/'; exit 1; fi
	@rm -rf .venv
	@poetry init --no-interaction --name=a_flask_test --author=rochacbruno
	@echo "" >> pyproject.toml
	@echo "[tool.poetry.scripts]" >> pyproject.toml
	@echo "nx_neptune = 'nx_neptune.__main__:main'" >> pyproject.toml
	@cat requirements/default.txt | while read in; do poetry add --no-interaction "$${in}"; done
	@cat requirements/test.txt | while read in; do poetry add --no-interaction "$${in}" --dev; done
	@poetry install --no-interaction
	@mkdir -p .github/backup
	@mv requirements* .github/backup
	@mv setup.py .github/backup
	@echo "You have switched to https://python-poetry.org/ package manager."
	@echo "Please run 'poetry shell' or 'poetry run nx_neptune'"

.PHONY: init
init:             ## Initialize the project based on an application template.
	@./.github/init.sh
