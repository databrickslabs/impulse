# Contributing to Impulse

Thanks for your interest in contributing! Impulse is a [Databricks Labs](https://github.com/databrickslabs)
project and we welcome bug reports, feature requests, and pull requests from the community.

## Filing issues and feature requests

We use [GitHub Issues](https://github.com/databrickslabs/impulse/issues) to track bugs and
feature requests. Issues labelled `help wanted` or `good first issue` are a great place to start.

For **major changes** such as a new solver, a silver/gold-layer schema change, or substantial
API changes, please open an issue first so we can discuss the approach. For **smaller changes**
(bug fixes, new aggregations or events, additional tests, doc improvements), feel free to send
a pull request directly.

## Development setup

### Prerequisites

- **Python** `>= 3.12, < 3.13` (the project pins this range in [pyproject.toml](pyproject.toml))
- **[uv](https://docs.astral.sh/uv/)** for dependency management
- **Java 17+** on your `PATH` (required by PySpark 4.0)
- **Git**

### Install

```bash
git clone https://github.com/databrickslabs/impulse.git
cd impulse
uv sync
```

`uv sync` creates a `.venv/` in the repo with all runtime and development dependencies
(pytest, ruff, black, etc.).

## Running tests

```bash
uv run pytest                         # full suite
uv run pytest --cov                   # with coverage report
uv run pytest tests/path/to/test.py   # a single test file
```

Notes specific to Impulse:

- [tests/conftest.py](tests/conftest.py) provisions a session-scoped local Spark session with
  Delta enabled and creates the `silver`, `silver_narrow_db`, `silver_key_value_store`, and
  `gold` schemas. You don't need a Databricks workspace to run the unit tests.
- Spark warehouse output is written to `spark-warehouse/` (gitignored).

## Code style

We use **black** for formatting and **ruff** for linting. Both are configured in
[pyproject.toml](pyproject.toml) (line length 99, target Python 3.12).

The repo includes a helper script that runs both:

```bash
./format_code.sh
```

Or run them manually:

```bash
uv run black src/ tests/
uv run ruff check src/ tests/ --fix
```

CI ([`.github/workflows/acceptance.yml`](.github/workflows/acceptance.yml)) runs `black --check`
and `ruff check` on every pull request, so please run `./format_code.sh` before pushing to avoid
CI failures.

## Submitting a pull request

1. Fork the repository and create a feature branch.
2. Make your changes; add or update tests covering the new behavior.
3. Run `./format_code.sh` and `uv run pytest` locally.
4. Open a pull request. The [PR template](.github/pull_request_template.md) walks you through
   the summary, changes, and test plan.
5. CI must pass (format, lint, tests). A maintainer will review the PR.

## Definition of done

- [ ] New or changed behavior is covered by tests
- [ ] `./format_code.sh` and `uv run pytest` pass locally
- [ ] Public APIs have NumPy-style docstrings (see ruff config in [pyproject.toml](pyproject.toml))
- [ ] User-facing changes are reflected in [README.md](README.md) and/or `docs/impulse/`
- [ ] PR description follows the [template](.github/pull_request_template.md)

## Project support

Impulse is a Databricks Labs project provided AS-IS, without SLAs. See the
[Project Support](README.md#project-support) section of the README for details. File issues on
GitHub and we'll review them as time permits.
