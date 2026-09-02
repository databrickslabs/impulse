all: clean lint fmt test coverage

# Ensure that all uv commands don't automatically update the lock file. If UV_FROZEN=1 (from the environment)
# then UV_LOCKED should _not_ be set, but otherwise it needs to be set to ensure the lock-file is only ever
# deliberately updated.
ifneq ($(UV_FROZEN),1)
export UV_LOCKED := 1
endif

# Ensure that build-system requires are hash-verified when building.
export UV_BUILD_CONSTRAINT := .build-constraints.txt

UV_RUN := uv run --exact --all-extras

# Path(s) passed to pytest. Defaults to the whole suite; CI overrides this to run a
# single component's tests in parallel, e.g. `make test TEST_PATH=tests/impulse_query_engine`.
TEST_PATH ?= tests/

clean:
	rm -fr .venv htmlcov .pytest_cache .ruff_cache .coverage coverage.xml test-results.xml
	find . -name '__pycache__' -print0 | xargs -0 rm -fr

dev:
	uv sync --all-extras

lint:
	$(UV_RUN) black --check src/ tests/
	$(UV_RUN) ruff check src/ tests/

fmt:
	$(UV_RUN) black src/ tests/
	$(UV_RUN) ruff check src/ tests/ --fix

test:
	$(UV_RUN) pytest $(TEST_PATH) --cov=src --cov-branch --cov-report=xml

coverage:
	$(UV_RUN) pytest tests/ --cov=src --cov-branch --cov-report=html
	open htmlcov/index.html

build:
	uv build --require-hashes --build-constraints=.build-constraints.txt

update-api-docs:
	cd docs/impulse && uv run pydoc-markdown

lock-dependencies: UV_LOCKED := 0
lock-dependencies:
	uv lock
	printf 'setuptools>=61.0\nwheel\n' | uv pip compile --generate-hashes --universal --no-header --quiet - > .build-constraints.txt
	@perl -pi -e 's|registry = "https://[^"]*"|registry = "https://pypi.org/simple"|g' uv.lock
	@printf 'Stripped registry references from uv.lock.\n'

# Mirror a fork PR onto a fork-test/pr-<N> branch in the main repo and open a test PR,
# so CI (which is skipped for fork PRs) runs with JFrog/OIDC. Review the fork code first.
# Usage: make fork-sync PR=<number>
fork-sync:
	@test -n "$(PR)" || (echo "Usage: make fork-sync PR=<number>"; exit 1)
	./.github/scripts/fork-sync-pr.sh $(PR)

.DEFAULT: all
.PHONY: all clean dev lint fmt test coverage build update-api-docs lock-dependencies fork-sync
